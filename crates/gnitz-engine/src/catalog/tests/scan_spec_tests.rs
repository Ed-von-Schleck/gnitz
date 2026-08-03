//! `scan_spec_family` — the worker-side parameterized bounded read. The first
//! group drives the bound walks (None / PkRange / PkSet) and the sink shapes
//! (top-k / materialize / early-stop) with an **identity** spec (empty predicate
//! and projection, so `reply_schema == source schema`), isolating the cursor +
//! reduction mechanics from the expression VM (which the e2e suite covers). The
//! second group drives the projection onto the keeper — gather, compute, and
//! output-slot coverage.

use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::storage::Batch;
use gnitz_wire::{Cut, OrderKey, RangeDescriptor, ReadBound, ReadSpec};

/// The `(id U64 PK | val I64)` schema both bases below use.
fn id_val_cols() -> Vec<ColumnDef> {
    vec![col_def("id", type_code::U64), col_def("val", type_code::I64)]
}

/// An `id_val_cols` base of `n` rows, `val = val_of(id)`, each at weight 1.
fn fixture(name: &str, n: u64, val_of: impl Fn(u64) -> i64) -> (CatalogEngine, i64) {
    ingest_fixture(name, &id_val_cols(), n, 1, |bb, id| bb.put_u64(val_of(id) as u64))
}

/// An `id_val_cols` **view output store** built from explicit `(id, val, weight)`
/// triples. A view runs no `enforce_unique_pk`, so weights above 1 are admitted
/// verbatim — a base table would clamp every accumulated PK weight to 1.
/// Returns the view id; the scan verb reads a view exactly like a base table.
fn weighted_fixture(name: &str, rows: impl Iterator<Item = (u64, i64, i64)>) -> (CatalogEngine, i64) {
    let cols = id_val_cols();
    let (mut engine, tid, _dir) = table_fixture(name, &cols);

    let vid = engine.allocate_table_id();
    write_identity_circuit(&mut engine, vid, tid, None);
    engine.write_column_records(vid, OWNER_KIND_VIEW, &cols).unwrap();
    engine.write_view_deps(vid, &[tid]).unwrap();
    let batch = build_view_tab_row(vid, "v_weighted", "SELECT * FROM t");
    engine.ingest_to_family(VIEW_TAB_ID, &batch).unwrap();

    let mut bb = BatchBuilder::new(engine.get_schema(vid).unwrap());
    for (id, val, w) in rows {
        bb.begin_row(id as u128, w);
        bb.put_u64(val as u64);
        bb.end_row();
    }
    engine.ingest_to_family(vid, &bb.finish()).unwrap();
    (engine, vid)
}

/// Extract `(id, val, weight)` triples from a `(id U64 PK | val I64)` reply batch.
fn triples(b: &Batch) -> Vec<(u128, i64, i64)> {
    (0..b.count)
        .map(|i| {
            let val = i64::from_le_bytes(b.get_col_ptr(i, 0, 8).try_into().unwrap());
            (b.get_pk(i), val, b.get_weight(i))
        })
        .collect()
}

/// An identity `ReadSpec` (no predicate, no projection) with the given bound,
/// order, and limit — [`rows_spec`] for the shape the bound-walk tests drive.
fn identity_spec(bound: ReadBound, order: Vec<OrderKey>, limit_k: u64) -> ReadSpec {
    ReadSpec {
        bound,
        ..rows_spec(vec![], vec![], order, limit_k)
    }
}

fn run(engine: &mut CatalogEngine, tid: i64, spec: &ReadSpec) -> Vec<(u128, i64, i64)> {
    let reply_schema = engine.get_schema(tid).unwrap();
    let keeper = engine.scan_spec_family(tid, spec, &reply_schema).unwrap();
    triples(&keeper)
}

#[test]
fn full_scan_returns_all_rows_with_weights() {
    let (mut e, tid) = fixture("ss_full", 10, |i| (i * 10) as i64);
    let mut got = run(&mut e, tid, &identity_spec(ReadBound::None, vec![], 0));
    got.sort();
    let want: Vec<_> = (0..10u128).map(|i| (i, (i * 10) as i64, 1)).collect();
    assert_eq!(got, want);
}

#[test]
fn pk_range_ge_subsets() {
    let (mut e, tid) = fixture("ss_pkrange", 10, |i| i as i64);
    // pk >= 5
    let bound = ReadBound::PkRange(RangeDescriptor::new(&[], Cut::Before(5), Cut::After(u64::MAX as u128)));
    let mut got = run(&mut e, tid, &identity_spec(bound, vec![], 0));
    got.sort();
    let want: Vec<_> = (5..10u128).map(|i| (i, i as i64, 1)).collect();
    assert_eq!(got, want);
}

#[test]
fn pk_range_point_lookup() {
    let (mut e, tid) = fixture("ss_point", 10, |i| i as i64);
    // pk = 5  →  [Before(5), After(5)]
    let bound = ReadBound::PkRange(RangeDescriptor::new(&[], Cut::Before(5), Cut::After(5)));
    let got = run(&mut e, tid, &identity_spec(bound, vec![], 0));
    assert_eq!(got, vec![(5u128, 5i64, 1)]);
}

#[test]
fn pk_range_bounded_above_truncates_at_end() {
    let (mut e, tid) = fixture("ss_between", 20, |i| i as i64);
    // 5 <= pk < 8  →  [Before(5), Before(8))
    let bound = ReadBound::PkRange(RangeDescriptor::new(&[], Cut::Before(5), Cut::Before(8)));
    let mut got = run(&mut e, tid, &identity_spec(bound, vec![], 0));
    got.sort();
    assert_eq!(got, vec![(5u128, 5, 1), (6u128, 6, 1), (7u128, 7, 1)]);
}

#[test]
fn pk_set_gathers_named_keys() {
    let (mut e, tid) = fixture("ss_pkset", 10, |i| (i * 100) as i64);
    let bound = ReadBound::PkSet(vec![2, 5, 7]);
    let mut got = run(&mut e, tid, &identity_spec(bound, vec![], 0));
    got.sort();
    assert_eq!(got, vec![(2u128, 200, 1), (5u128, 500, 1), (7u128, 700, 1)]);
}

/// A system table's store is one unpartitioned `Table` holding every key, which
/// the cursor reads whole — so the gather's ownership filter must keep every key
/// there. Treating it like a hashed store would answer zero rows.
#[test]
fn pk_set_over_a_system_table_keeps_every_key() {
    let (mut e, tid, _dir) = table_fixture("ss_pkset_sys", &id_val_cols());
    let sys_schema = e.get_schema(TABLE_TAB_ID).unwrap();
    let spec = identity_spec(ReadBound::PkSet(vec![tid as u128]), vec![], 0);
    let keeper = e.scan_spec_family(TABLE_TAB_ID, &spec, &sys_schema).unwrap();
    assert_eq!(
        (0..keeper.count).map(|i| keeper.get_pk(i)).collect::<Vec<_>>(),
        vec![tid as u128],
        "the created table's TABLE_TAB row must come back"
    );
}

/// Two distinct wire keys can share one OPK image — `opk_key` truncates to
/// `pk_stride`, so `5` and `5 + 2^64` are the same U64 PK. The decoder's
/// schema-free view cannot see that; the gather's post-sort scan must, or the
/// duplicate key re-walks the same PK group and emits it twice. The reject must
/// not depend on which worker owns the key, so it runs before the gather's
/// ownership filter — a fixture whose store owns everything cannot show that, so
/// the assertion is on the error alone.
#[test]
fn pk_set_rejects_keys_colliding_after_truncation() {
    let (mut e, tid) = fixture("ss_pkset_trunc", 10, |i| i as i64);
    let spec = identity_spec(ReadBound::PkSet(vec![5, 5 + (1u128 << 64)]), vec![], 0);
    let reply_schema = e.get_schema(tid).unwrap();
    let Err(err) = e.scan_spec_family(tid, &spec, &reply_schema) else {
        panic!("colliding PkSet keys must be rejected, not gathered twice");
    };
    assert!(err.contains("duplicate"), "{err}");
}

/// A REPLICATED base table is an **unhashed** store: one child holding the whole
/// local dataset, its rows not keyed by `partition_for_pk` at all. Every keyed
/// read has to resolve through `slot_for_key`, which maps any key to that one
/// child — routing by the key's partition id alone finds no local slot and would
/// silently answer zero rows.
#[test]
fn keyed_reads_over_a_replicated_table_find_every_key() {
    const N: u128 = 20;
    let dir = temp_dir("ss_replicated");
    let mut e = CatalogEngine::open(&dir).unwrap();
    let cols = id_val_cols();
    let tid = create_flagged_table(&mut e, &dir, "rep", &cols, &[0], gnitz_wire::pack_table_flags(true, 0));
    assert!(
        e.dag.tables.get(&tid).unwrap().handle.is_unhashed(),
        "REPLICATED must build an unhashed store"
    );

    let schema = e.get_schema(tid).unwrap();
    let mut bb = BatchBuilder::new(schema);
    for id in 0..N {
        bb.begin_row(id, 1);
        bb.put_u64(id as u64 * 10);
        bb.end_row();
    }
    e.ingest_to_family(tid, &bb.finish()).unwrap();
    let want: Vec<(u128, i64, i64)> = (0..N).map(|i| (i, i as i64 * 10, 1)).collect();

    // The point seek (FLAG_SEEK) and the point PK range (what `WHERE pk = k`
    // compiles to) each find every key.
    for (id, val, _) in &want {
        let hit = e.seek_family(tid, *id, &[]).unwrap().0;
        assert_eq!(
            triples(&hit.expect("seek_family must find the row")),
            vec![(*id, *val, 1)]
        );
        let point = RangeDescriptor::new(&[], Cut::Before(*id), Cut::After(*id));
        assert_eq!(
            run(&mut e, tid, &identity_spec(ReadBound::PkRange(point), vec![], 0)),
            vec![(*id, *val, 1)]
        );
    }

    // The FK dereference gather and the `pk IN (…)` set gather.
    let pks: Vec<_> = (0..N)
        .map(|id| crate::schema::key::opk_key(&schema, &id.to_le_bytes()))
        .collect();
    let gathered = e
        .gather_family_bytes(tid, pks.iter().map(|p| p.pk_bytes()), &[1])
        .unwrap();
    assert_eq!(gathered.count, N as usize, "every parent key must dereference");

    let mut got = run(
        &mut e,
        tid,
        &identity_spec(ReadBound::PkSet((0..N).collect()), vec![], 0),
    );
    got.sort();
    assert_eq!(got, want);

    e.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A view output store runs no `enforce_unique_pk`, and a synthetic view key
/// (`_join_pk`) is the join key — it names one row per row the join produced for
/// it. Every keyed reader must return that whole group: the full scan is ground
/// truth and the point PK range already walked it, so a seek or `pk IN (…)` that
/// copies the first row alone silently answers a fraction of the key.
///
/// A ghost member (net weight 0 across two ingests) is dropped by all four.
#[test]
fn keyed_reads_over_a_view_return_the_whole_pk_group() {
    let rows = [(7u64, 50i64, 1i64), (7, 100, 1), (7, 200, 1), (7, 300, 1), (9, 900, 1)];
    let (mut e, vid) = weighted_fixture("ss_view_group", rows.into_iter());
    // Retract (7, 50) in a second ingest: it folds to net zero at read time.
    let mut bb = BatchBuilder::new(e.get_schema(vid).unwrap());
    bb.begin_row(7, -1);
    bb.put_u64(50);
    bb.end_row();
    e.ingest_to_family(vid, &bb.finish()).unwrap();
    let want = vec![(7u128, 100i64, 1i64), (7, 200, 1), (7, 300, 1)];

    let mut full = run(&mut e, vid, &identity_spec(ReadBound::None, vec![], 0));
    full.sort();
    assert_eq!(full, [want.clone(), vec![(9u128, 900, 1)]].concat(), "ground truth");

    // The point PK range — what `WHERE pk = 7` compiles to.
    let point = RangeDescriptor::new(&[], Cut::Before(7), Cut::After(7));
    assert_eq!(
        run(&mut e, vid, &identity_spec(ReadBound::PkRange(point), vec![], 0)),
        want
    );

    // The point seek (FLAG_SEEK).
    let hit = e.seek_family(vid, 7, &[]).unwrap().0.expect("seek must find the group");
    assert_eq!(triples(&hit), want);

    // The `pk IN (…)` set gather.
    assert_eq!(
        run(&mut e, vid, &identity_spec(ReadBound::PkSet(vec![7]), vec![], 0)),
        want
    );

    // A key the store does not name stays a miss on both.
    assert!(e.seek_family(vid, 8, &[]).unwrap().0.is_none());
    assert_eq!(
        run(&mut e, vid, &identity_spec(ReadBound::PkSet(vec![8]), vec![], 0)),
        vec![]
    );
}

/// The two keyed readers gate each row on a positive weight. That gate must be
/// applied **per row of the group**, not as a presence test for the key: a
/// retracted member an uncompacted source still holds sorts at the group head
/// (payloads order within a PK), and testing the key by its head alone answers
/// "no such row" for a key whose live rows sit right behind it.
#[test]
fn keyed_reads_skip_a_retracted_group_head_and_keep_the_live_rows() {
    let rows = [(5u64, 10i64, -1i64), (5, 20, 1), (5, 30, 1)];
    let (mut e, vid) = weighted_fixture("ss_view_head_ghost", rows.into_iter());
    let want = vec![(5u128, 20i64, 1i64), (5, 30, 1)];

    let hit = e.seek_family(vid, 5, &[]).unwrap().0.expect("seek must find the group");
    assert_eq!(triples(&hit), want);
    assert_eq!(
        run(&mut e, vid, &identity_spec(ReadBound::PkSet(vec![5]), vec![], 0)),
        want
    );
}

/// A `PkSet` chunk tests the row budget before each key and then drains that
/// key's whole group, so a group larger than the budget crosses it in one piece
/// and the chunk overshoots. The sink must take that chunk whole and resume at
/// the next key — a group cut at the budget would lose its tail.
#[test]
fn pk_set_drains_a_group_larger_than_the_chunk_budget() {
    let (mut e, vid) = weighted_fixture(
        "ss_view_chunk",
        (0..10u64)
            .map(|i| (1u64, i as i64, 1i64))
            .chain(std::iter::once((2u64, 99i64, 1i64))),
    );
    e.ddl_scan_chunk_rows = 4;
    let got = run(&mut e, vid, &identity_spec(ReadBound::PkSet(vec![1, 2]), vec![], 0));
    let want: Vec<_> = (0..10i64)
        .map(|v| (1u128, v, 1i64))
        .chain(std::iter::once((2u128, 99, 1)))
        .collect();
    assert_eq!(got, want);
}

/// A `CLUSTER BY` table hashes only the leading PK column, so a range that pins
/// it is confined to one partition and the scan opens only that one. It must
/// still return the whole group — and a range that spans the leading column is
/// not confinable (partition ids are `mix(pk) >> 56`, not monotone in key order)
/// and must stay on the merged cursor and return every row.
#[test]
fn pk_range_over_a_clustered_table_routes_when_confined_and_spans_when_not() {
    const NA: u64 = 4;
    const NB: u64 = 5;
    let dir = temp_dir("ss_clustered");
    let mut e = CatalogEngine::open(&dir).unwrap();
    let cols = vec![
        col_def("a", type_code::U64),
        col_def("b", type_code::U64),
        col_def("val", type_code::I64),
    ];
    // CLUSTER BY a: distribution prefix = the first of the two PK columns.
    let tid = create_flagged_table(
        &mut e,
        &dir,
        "clus",
        &cols,
        &[0, 1],
        gnitz_wire::pack_table_flags(false, 1),
    );
    let schema = e.get_schema(tid).unwrap();
    assert_eq!(schema.dist_stride(), 8, "CLUSTER BY one U64 column ⇒ an 8-byte prefix");

    let mut bb = BatchBuilder::new(schema);
    for a in 0..NA {
        for b in 0..NB {
            bb.begin_row_opk(&[a as u128, b as u128], 1);
            bb.put_u64(a * 100 + b);
            bb.end_row();
        }
    }
    e.ingest_to_family(tid, &bb.finish()).unwrap();

    // `(a, b, val)` triples of a reply batch, decoded off the 16-byte OPK PK.
    let decode = |batch: &Batch| -> Vec<(u64, u64, i64)> {
        let mut out: Vec<_> = (0..batch.count)
            .map(|i| {
                let pk = batch.get_pk_bytes(i);
                (
                    u64::from_be_bytes(pk[..8].try_into().unwrap()),
                    u64::from_be_bytes(pk[8..].try_into().unwrap()),
                    i64::from_le_bytes(batch.get_col_ptr(i, 0, 8).try_into().unwrap()),
                )
            })
            .collect();
        out.sort();
        out
    };
    let scan = |e: &mut CatalogEngine, desc: RangeDescriptor| {
        let spec = identity_spec(ReadBound::PkRange(desc), vec![], 0);
        decode(&e.scan_spec_family(tid, &spec, &schema).unwrap())
    };

    // `a = 2 AND b >= 0` — a whole trailing-column range inside one `a` group,
    // which shares the distribution prefix and so routes to one partition.
    let confined = RangeDescriptor::new(&[2], Cut::Before(0), Cut::After(u64::MAX as u128));
    assert!(
        scan_spec_partition(&schema, &confined).is_some(),
        "the fixture's confined range must be the routed shape"
    );
    assert_eq!(
        scan(&mut e, confined),
        (0..NB).map(|b| (2, b, (200 + b) as i64)).collect::<Vec<_>>(),
        "a confined range still returns its whole group"
    );

    // `1 <= a < 3` spans two `a` groups, so it is not confinable and keeps the
    // merged cursor.
    let spanning = RangeDescriptor::new(&[], Cut::Before(1), Cut::Before(3));
    assert!(scan_spec_partition(&schema, &spanning).is_none());
    let mut want: Vec<(u64, u64, i64)> = Vec::new();
    for a in 1..3 {
        for b in 0..NB {
            want.push((a, b, (a * 100 + b) as i64));
        }
    }
    assert_eq!(scan(&mut e, spanning), want, "a range spanning partitions loses no row");

    e.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn pk_set_missing_keys_miss_silently() {
    let (mut e, tid) = fixture("ss_pkset_miss", 5, |i| i as i64);
    // 3 present (0,4), 99 absent.
    let bound = ReadBound::PkSet(vec![0, 99, 4]);
    let mut got = run(&mut e, tid, &identity_spec(bound, vec![], 0));
    got.sort();
    assert_eq!(got, vec![(0u128, 0, 1), (4u128, 4, 1)]);
}

/// `OrderKey.col` is a raw client `u16` that reaches `reply_schema.locate`,
/// whose bound is a release-active `assert!`. The locators are built before the
/// top-k decision and before the chunk loop, so neither `limit_k` nor an empty
/// table would have contained it.
#[test]
fn order_key_column_out_of_range_is_rejected() {
    let (mut e, tid) = fixture("ss_order_oob", 4, |id| id as i64);
    let spec = identity_spec(
        ReadBound::None,
        vec![OrderKey {
            col: 99,
            desc: false,
            nulls_first: false,
        }],
        0,
    );
    let reply_schema = e.get_schema(tid).unwrap();
    assert!(e.scan_spec_family(tid, &spec, &reply_schema).is_err());
}

#[test]
fn order_by_desc_limit_keeps_top_k() {
    let (mut e, tid) = fixture("ss_topk", 20, |i| i as i64); // val == id
                                                             // ORDER BY val DESC LIMIT 3  →  the 3 largest vals (ids 19, 18, 17).
    let order = vec![OrderKey {
        col: 1,
        desc: true,
        nulls_first: false,
    }];
    let mut got = run(&mut e, tid, &identity_spec(ReadBound::None, order, 3));
    got.sort();
    assert_eq!(got, vec![(17u128, 17, 1), (18u128, 18, 1), (19u128, 19, 1)]);
}

#[test]
fn order_by_asc_limit_keeps_smallest() {
    let (mut e, tid) = fixture("ss_topk_asc", 20, |i| i as i64);
    let order = vec![OrderKey {
        col: 1,
        desc: false,
        nulls_first: false,
    }];
    let mut got = run(&mut e, tid, &identity_spec(ReadBound::None, order, 3));
    got.sort();
    assert_eq!(got, vec![(0u128, 0, 1), (1u128, 1, 1), (2u128, 2, 1)]);
}

#[test]
fn top_k_keeps_boundary_row_whole() {
    // The top row by val carries weight 3; LIMIT 2 covers its weight and it must
    // be kept whole (weight 3), not clipped to 2 — the client window does the
    // exact split.
    let (mut e, tid) = weighted_fixture(
        "ss_topk_weight",
        [(1u64, 100i64, 3i64), (2, 50, 1), (3, 10, 1)].into_iter(),
    );
    let order = vec![OrderKey {
        col: 1,
        desc: true,
        nulls_first: false,
    }];
    let got = run(&mut e, tid, &identity_spec(ReadBound::None, order, 2));
    assert_eq!(got, vec![(1u128, 100, 3)], "boundary row kept whole at its true weight");
}

#[test]
fn no_order_limit_early_stops_on_summed_weight() {
    // No ORDER BY + LIMIT 5, all weight-1: the worker keeps a >=5-weight prefix in
    // cursor order — some legal subset, all weight 1, count 5.
    let (mut e, tid) = fixture("ss_earlystop", 100, |i| i as i64);
    let got = run(&mut e, tid, &identity_spec(ReadBound::None, vec![], 5));
    let total: i64 = got.iter().map(|(_, _, w)| w).sum();
    assert!(total >= 5, "early-stop must cover the window weight, got {total}");
    assert!(
        got.len() <= 6,
        "early-stop should not materialize the whole relation, got {}",
        got.len()
    );
}

// ---------------------------------------------------------------------------
// Projections: survivor ranges driven straight onto the keeper
// ---------------------------------------------------------------------------

/// [`ingest_fixture`] in one round, with `chunk_rows` pinning the sink's chunk
/// size so a test can force the keeper to be reused across ≥2 chunks — the only
/// way a stale-byte leak in the projection's tail writes would show, since the
/// keeper arena grows `Fill::Uninit`.
fn proj_fixture(
    name: &str,
    cols: &[ColumnDef],
    n: u64,
    chunk_rows: usize,
    put_row: impl FnMut(&mut BatchBuilder, u64),
) -> (CatalogEngine, i64) {
    let (mut engine, tid) = ingest_fixture(name, cols, n, 1, put_row);
    engine.ddl_scan_chunk_rows = chunk_rows;
    (engine, tid)
}

/// `SELECT c0..c63` off a legal 65-column table (1 PK + 64 payload) — the widest
/// schema the row-major null word admits, so the output-coverage mask is exercised
/// at its top bit.
#[test]
fn gather_of_64_payload_columns_covers_every_slot() {
    const P: usize = 64;
    let mut cols = vec![col_def("id", type_code::U64)];
    cols.extend((0..P).map(|k| col_def(&format!("c{k}"), type_code::I64)));
    let (mut e, tid) = proj_fixture("ss_gather64", &cols, 40, 16, |bb, id| {
        for k in 0..P {
            bb.put_u64(id * 100 + k as u64);
        }
    });
    // Identity-order gather of every payload column: src col k+1 → out slot k.
    let copies: Vec<(u32, u32)> = (0..P as u32).map(|k| (k + 1, k)).collect();
    let reply = e.get_schema(tid).unwrap();
    let spec = rows_spec(vec![], proj_blob(&copies), vec![], 0);
    let got = e.scan_spec_family(tid, &spec, &reply).unwrap();
    assert_eq!(got.count, 40);
    for r in 0..got.count {
        let id = got.get_pk(r) as u64;
        for k in 0..P {
            assert_eq!(got.read_payload_u64(r, k), id * 100 + k as u64, "row {r} slot {k}");
        }
    }
}

/// An all-PK-sourced projection has an empty `NullPerm`. Run it across ≥2 chunks
/// so the keeper arena is grown and reused, and assert every result null word is
/// 0: were the empty permutation to skip the write (assuming a zeroed
/// destination, as a freshly allocated output could), uninitialized keeper bytes
/// would surface here and nowhere else.
#[test]
fn all_pk_sourced_projection_zeroes_null_words_across_chunks() {
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let (mut e, tid) = proj_fixture("ss_pkonly_proj", &cols, 300, 32, |bb, id| {
        bb.put_u64(id * 7);
    });
    // Reply: id U64 PK + one U64 payload copied FROM the PK column.
    let reply = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ],
        &[0],
    );
    let spec = rows_spec(vec![], proj_blob(&[(0, 0)]), vec![], 0);
    let got = e.scan_spec_family(tid, &spec, &reply).unwrap();
    assert_eq!(got.count, 300, "10 chunks of 32 rows minus the short tail");
    for r in 0..got.count {
        assert_eq!(
            got.get_null_word(r),
            0,
            "row {r}: a PK-sourced column is never NULL, so the whole word must be 0"
        );
        let pk = got.get_pk(r) as u64;
        assert_eq!(got.read_payload_u64(r, 0), pk, "row {r}: PK-decoded payload");
    }
}

/// A permuted gather over a nullable payload column, a long (out-of-line) German
/// string, and a PK-sourced column, with the keeper reused across ≥2 chunks. One
/// integration case for the projection's src-start/dst-base arithmetic, per-cell
/// blob relocation into `keeper.blob` (the keeper owns its blob after the source
/// chunk drops), and null permutation together.
#[test]
fn permuted_gather_with_string_and_nullable_across_chunks() {
    let cols = vec![
        col_def("id", type_code::U64),
        ColumnDef {
            name: "nv".into(),
            type_code: type_code::I64,
            is_nullable: true,
            fk_table_id: 0,
            fk_col_idx: 0,
            is_hidden: false,
        },
        col_def("s", type_code::STRING),
    ];
    const N: u64 = 200;
    let (mut e, tid) = proj_fixture("ss_permuted_str", &cols, N, 24, |bb, id| {
        match id.is_multiple_of(5) {
            true => bb.put_null(),
            false => bb.put_u64(id * 3),
        }
        // Every third row shares one long value; the rest are distinct long
        // values. All are > 12 bytes, so they live out-of-line in the blob heap
        // and the shared span exercises the relocator's dedup cache.
        match id.is_multiple_of(3) {
            true => bb.put_string("a-shared-out-of-line-value"),
            false => bb.put_string(&format!("a-distinct-out-of-line-value-{id}")),
        }
    });
    // Reply: id U64 PK | s STRING (slot 0) | id-from-PK U64 (slot 1) | nv I64
    // (slot 2, nullable) — a permutation that also relocates a PK column.
    let reply = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::STRING, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );
    let spec = rows_spec(vec![], proj_blob(&[(2, 0), (0, 1), (1, 2)]), vec![], 0);
    let got = e.scan_spec_family(tid, &spec, &reply).unwrap();
    assert_eq!(got.count as u64, N);

    // Decoded rows: (pk, s, pk_copy, Option<nv>).
    let mut decoded: Vec<(u128, String, u64, Option<i64>)> = (0..got.count)
        .map(|r| {
            let nv = match gnitz_wire::null_word_get(got.get_null_word(r), 2) {
                true => None,
                false => Some(i64::from_le_bytes(got.get_col_ptr(r, 2, 8).try_into().unwrap())),
            };
            (
                got.get_pk(r),
                got.read_payload_string(r, 0),
                got.read_payload_u64(r, 1),
                nv,
            )
        })
        .collect();
    decoded.sort();
    let want: Vec<_> = (0..N)
        .map(|id| {
            let s = match id.is_multiple_of(3) {
                true => "a-shared-out-of-line-value".to_string(),
                false => format!("a-distinct-out-of-line-value-{id}"),
            };
            let nv = (!id.is_multiple_of(5)).then_some(id as i64 * 3);
            (id as u128, s, id, nv)
        })
        .collect();
    assert_eq!(decoded, want);
}

/// A compute-bearing projection (`val * 2` alongside a copied column) with a
/// predicate, across ≥2 chunks: the survivors are compacted first, then the
/// compute kernel writes onto the keeper's tail at a non-zero destination base.
/// The nullable source makes the EMIT null-merge run, whose read-modify-write
/// `|=` depends on the permuted word already being written for those rows.
#[test]
fn compute_projection_writes_at_keeper_tail_across_chunks() {
    let cols = vec![
        col_def("id", type_code::U64),
        ColumnDef {
            name: "nv".into(),
            type_code: type_code::I64,
            is_nullable: true,
            fk_table_id: 0,
            fk_col_idx: 0,
            is_hidden: false,
        },
        col_def("keep", type_code::I64),
    ];
    const N: u64 = 300;
    let (mut e, tid) = proj_fixture("ss_compute_proj", &cols, N, 32, |bb, id| {
        match id.is_multiple_of(4) {
            true => bb.put_null(),
            false => bb.put_u64(id),
        }
        bb.put_u64(id * 10);
    });
    // Reply: id U64 PK | nv*2 I64 (slot 0, nullable) | keep I64 (slot 1).
    let reply = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let projection = {
        let mut eb = gnitz_core::ExprBuilder::new();
        let (v, two) = (eb.load_col_int(1), eb.load_const(2));
        let doubled = eb.mul(v, two);
        eb.emit_col(doubled, 0);
        eb.copy_col(2, 1);
        eb.build(0).encode()
    };
    // `keep = id * 10 < 1000` keeps ids 0..99, interleaved with the chunking.
    let spec = rows_spec(pred_lt_blob(2, 1000), projection, vec![], 0);
    let got = e.scan_spec_family(tid, &spec, &reply).unwrap();

    let mut decoded: Vec<(u128, Option<i64>, i64)> = (0..got.count)
        .map(|r| {
            let doubled = match gnitz_wire::null_word_get(got.get_null_word(r), 0) {
                true => None,
                false => Some(i64::from_le_bytes(got.get_col_ptr(r, 0, 8).try_into().unwrap())),
            };
            let keep = i64::from_le_bytes(got.get_col_ptr(r, 1, 8).try_into().unwrap());
            (got.get_pk(r), doubled, keep)
        })
        .collect();
    decoded.sort();
    let want: Vec<_> = (0..100u64)
        .map(|id| {
            let doubled = (!id.is_multiple_of(4)).then_some(id as i64 * 2);
            (id as u128, doubled, id as i64 * 10)
        })
        .collect();
    assert_eq!(decoded, want);
}

/// A projection that leaves a reply payload slot unwritten is a malformed frame:
/// the rows sink projects onto the keeper's recycled tail, where an unwritten
/// slot would ship a previous request's bytes. Hard reject, not a zero-fill.
#[test]
fn projection_missing_an_output_slot_errs() {
    let (mut e, tid) = fixture("ss_proj_gap", 4, |i| i as i64);
    // Reply wants two payload slots; the projection writes only slot 0.
    let reply = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let spec = rows_spec(vec![], proj_blob(&[(1, 0)]), vec![], 0);
    let err = e.scan_spec_family(tid, &spec, &reply).err().unwrap();
    assert!(err.contains("OutputSlotUnwritten"), "{err}");
}

/// A gather + ORDER BY … LIMIT k over weight-3 rows, sized so
/// `summed > 2 · limit_k` fires `topk_keep` mid-scan: `from_indexed_rows` must
/// compact a gather-produced keeper and still keep the boundary row whole.
#[test]
fn gather_top_k_keeps_boundary_row_whole() {
    let (mut e, tid) = weighted_fixture("ss_gather_topk", (0..40u64).map(|id| (id, id as i64, 3i64)));
    e.ddl_scan_chunk_rows = 8;
    // Reply: id U64 PK | val I64 — a gather of the single payload column.
    let reply = e.get_schema(tid).unwrap();
    let order = vec![OrderKey {
        col: 1,
        desc: false,
        nulls_first: false,
    }];
    let spec = rows_spec(vec![], proj_blob(&[(1, 0)]), order, 2);
    let got = e.scan_spec_family(tid, &spec, &reply).unwrap();
    // LIMIT 2 is covered by the single smallest row's weight 3 — kept whole.
    assert_eq!(triples(&got), vec![(0u128, 0, 3)]);
}

/// A gather + no-ORDER-BY `LIMIT k` with a predicate passing far more than `k`
/// rows: the range list must be cut once the summed weight covers the window, so
/// the keeper stays a small `≥ k`-weight superset instead of the whole chunk's
/// survivors.
///
/// The cut falls **between** survivor ranges (a range is never split — its rows
/// are whole), so the fixture interleaves: `val = id % 2` with `val < 1` passes
/// every even id, giving one single-row range per pair and a cut after ~`k` of
/// them. A single all-passing range would legitimately append the whole chunk.
#[test]
fn gather_limit_cuts_the_range_list_mid_chunk() {
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let (mut e, tid) = proj_fixture("ss_gather_earlystop", &cols, 500, 256, |bb, id| {
        bb.put_u64(id % 2);
    });
    let reply = e.get_schema(tid).unwrap();
    let spec = rows_spec(pred_lt_blob(1, 1), proj_blob(&[(1, 0)]), vec![], 5);
    let got = e.scan_spec_family(tid, &spec, &reply).unwrap();
    let total: i64 = (0..got.count).map(|r| got.get_weight(r)).sum();
    assert!(total >= 5, "early-stop must cover the window weight, got {total}");
    assert!(
        got.count <= 6,
        "the range-list cut must not gather the whole chunk's survivors, got {}",
        got.count
    );
}

#[test]
fn reply_pk_stride_mismatch_errs() {
    let (mut e, tid) = fixture("ss_stride", 4, |i| i as i64);
    // A reply schema with a narrower (U32) PK than the source's U64.
    let bad = crate::schema::SchemaDescriptor::new(
        &[
            crate::schema::SchemaColumn::new(type_code::U32, 0),
            crate::schema::SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let spec = identity_spec(ReadBound::None, vec![], 0);
    assert!(e.scan_spec_family(tid, &spec, &bad).is_err());
}
