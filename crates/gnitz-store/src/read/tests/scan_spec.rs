use super::*;
use crate::relation::{IndexClaim, RelationKind, RelationSpec, StoreConfig};
use crate::schema::Slot;
use crate::schema::{SchemaColumn, TypeCode};
use crate::storage::{BatchBuilder, StoreError};
use crate::test_support::payload0_i64;
use gnitz_wire::ViewProps;
use gnitz_wire::{key_image, AggDescriptor, AggReadSpec, Cut, KeyRange, OrderKey, PkColList, ReadSink};

// ── The executor — `scan_spec` over a registry built in-crate ────────
//
// `hydrator: None`, as `gnitz-mirror` calls it: nothing here holds a skeleton
// row. These drive the sink reduction, which needs no expression compiler; the
// projection and predicate paths are covered where one exists.

/// The relation every executor test below reads.
const TID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;

/// `(id U64 PK | val I64)`, `val == id`.
fn id_val_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}

/// A registry holding `TID` with `n` rows of `val == id`, each at `weight`.
///
/// A **view**, whose store runs no `enforce_unique_pk`, so a weight above 1 is
/// admitted verbatim — the window below is a summed *weight*, which few rows at
/// a large weight reach far more cheaply than many rows.
fn rows_fixture(name: &str, n: u64, weight: i64) -> RelationRegistry {
    let schema = id_val_schema();
    let mut registry = RelationRegistry::new(
        &crate::test_support::scratch_dir("read", name),
        Slot::SOLO,
        StoreConfig::default(),
    );
    registry
        .register(RelationSpec {
            id: TID,
            kind: RelationKind::View(ViewProps::Plain),
            schema,
        })
        .unwrap();
    let mut bb = BatchBuilder::new(schema);
    for id in 0..n {
        bb.begin_row(id as u128, weight);
        bb.put_u64(id);
        bb.end_row();
    }
    registry.ingest(TID, bb.finish()).unwrap();
    registry
}

/// An identity rows spec: no predicate, no projection, so the reply schema is
/// the source schema.
fn rows_spec(order: Vec<OrderKey>, limit_k: u64) -> ReadSpec {
    ReadSpec {
        bound: ReadBound::None,
        predicate: Vec::new(),
        sink: ReadSink {
            map: None,
            kind: SinkKind::Rows { order, limit_k },
        },
    }
}

/// `ORDER BY val DESC`.
fn val_desc() -> Vec<OrderKey> {
    vec![OrderKey { col: 1, desc: true, nulls_first: false }]
}

/// `(id, weight)` pairs of a reply batch, in reply order.
fn ids(b: &Batch) -> Vec<(u128, i64)> {
    (0..b.count).map(|i| (b.get_pk(i), b.get_weight(i))).collect()
}

fn run(registry: &mut RelationRegistry, spec: &ReadSpec) -> Result<Rc<Batch>, StoreError> {
    let schema = id_val_schema();
    registry.scan_spec(TID, spec.clone(), schema.layout_digest(), None)
}

/// The reply is trimmed to the window, not to the mid-scan residency cap: five
/// weight-1 rows against `LIMIT 3` sit in the band the mid-scan trim skips, so
/// only the terminal trim can shed them, and it must. The reply's order is not a
/// contract — the client re-sorts it — so the rows are compared as a set.
#[test]
fn top_k_trims_to_the_window_before_replying() {
    let mut r = rows_fixture("topk_band", 5, 1);
    let mut got = ids(&run(&mut r, &rows_spec(val_desc(), 3)).unwrap());
    got.sort();
    assert_eq!(got, vec![(2u128, 1), (3, 1), (4, 1)], "the three largest");
}

/// No window is too large for the top-k sink. The window counts summed *weight*,
/// so five rows at weight 100_000 overshoot a 65_537-row window and the trim
/// sheds all but the one row that already covers it — where a sink that gave up
/// past some window size would ship all five unsorted.
#[test]
fn top_k_runs_at_an_arbitrarily_large_window() {
    let mut r = rows_fixture("topk_past_cap", 5, 100_000);
    let got = run(&mut r, &rows_spec(val_desc(), 65_537)).unwrap();
    assert_eq!(
        ids(&got),
        vec![(4u128, 100_000)],
        "the one comparator-smallest row already covers the window"
    );
}

/// `limit_k = u64::MAX` saturates to an `i64::MAX` window, whose residency cap
/// must saturate too: an unsaturated `2 * window` is a multiply overflow, and a
/// debug build panics on one. Nothing reaches either threshold, so all five ship.
#[test]
fn a_maximal_limit_k_neither_overflows_nor_trims() {
    let mut r = rows_fixture("topk_max_limit", 5, 100_000);
    let got = run(&mut r, &rows_spec(val_desc(), u64::MAX)).unwrap();
    assert_eq!(got.count, 5);
}

/// A whole-relation spec is the store's cached snapshot, and still refuses a
/// foreign reply layout.
#[test]
fn a_whole_relation_spec_is_served_off_the_cached_snapshot() {
    let mut r = rows_fixture("whole_relation", 4, 1);
    let snapshot = r.relation(TID).unwrap().full_scan();
    assert!(Rc::ptr_eq(&run(&mut r, &rows_spec(Vec::new(), 0)).unwrap(), &snapshot));
    let mismatched = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::F64, false),
        ],
        &[0],
    );
    let Err(err) = r.scan_spec(TID, rows_spec(Vec::new(), 0), mismatched.layout_digest(), None) else {
        panic!("a reply layout off the relation's must be refused on the snapshot path");
    };
    assert!(err.to_string().contains("reply schema does not match"), "{err}");
}

/// A malformed ORDER BY key is rejected before the cursor is opened, so the walk
/// the bound names never runs and the message names the offending column.
#[test]
fn an_out_of_range_order_key_is_rejected() {
    let mut r = rows_fixture("order_oob", 4, 1);
    let order = vec![OrderKey { col: 99, desc: false, nulls_first: false }];
    let Err(err) = run(&mut r, &rows_spec(order, 1)) else {
        panic!("an out-of-range order key must be rejected");
    };
    assert!(err.to_string().contains("order key column 99"), "{err}");
}

/// A packed column word naming a column the table has not got is a corrupt
/// frame, and is rejected even with no index to walk: degrading to a full scan
/// would answer the corrupt frame instead of refusing it.
#[test]
fn an_out_of_range_index_column_is_rejected_without_an_index() {
    let mut r = rows_fixture("index_oob", 4, 1);
    let spec = ReadSpec {
        bound: ReadBound::Range(KeyRange::new(
            PkColList::from_slice(&[99]),
            &[],
            Cut::before(0),
            Cut::after(u64::MAX as u128),
        )),
        ..rows_spec(Vec::new(), 0)
    };
    let Err(err) = run(&mut r, &spec) else {
        panic!("an out-of-range index column must be rejected");
    };
    assert!(err.to_string().contains("invalid column list"), "{err}");
}

// ── Index walks are exact: the index, a full scan standing in for it ─

/// `(id U64 PK | val I64 NULL | big U128 NULL)`.
fn walk_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::U128, true),
        ],
        &[0],
    )
}

const WALK_ROWS: u64 = 64;

/// Row `id`'s `val` (`id − 32`, straddling zero) and `big` (`id << 70`); every fifth `val`
/// and every seventh `big` is NULL.
fn walk_row(id: u64) -> (Option<i64>, Option<u128>) {
    let val = (!id.is_multiple_of(5)).then_some(id as i64 - 32);
    let big = (!id.is_multiple_of(7)).then_some((id as u128) << 70);
    (val, big)
}

/// `WALK_ROWS` rows of [`walk_row`], an index on `val` and on `big` when `indexed`.
fn walk_fixture(name: &str, indexed: bool) -> RelationRegistry {
    let schema = walk_schema();
    let mut registry = RelationRegistry::new(
        &crate::test_support::scratch_dir("read", name),
        Slot::SOLO,
        StoreConfig::default(),
    );
    registry
        .register(RelationSpec {
            id: TID,
            kind: RelationKind::BaseTable,
            schema,
        })
        .unwrap();
    if indexed {
        registry
            .add_index(TID, IndexClaim::Index { id: TID + 1, unique: false }, &[1])
            .unwrap();
        registry
            .add_index(TID, IndexClaim::Index { id: TID + 2, unique: false }, &[2])
            .unwrap();
    }
    let mut bb = BatchBuilder::new(schema);
    for id in 0..WALK_ROWS {
        let (val, big) = walk_row(id);
        bb.begin_row(id as u128, 1);
        match val {
            Some(v) => bb.put_int(v as u128),
            None => bb.put_null(),
        }
        match big {
            Some(b) => bb.put_int(b),
            None => bb.put_null(),
        }
        bb.end_row();
    }
    registry.ingest(TID, bb.finish()).unwrap();
    registry
}

fn index_walk(col: u32, start: Cut, end: Cut, order: Vec<OrderKey>, limit_k: u64) -> ReadSpec {
    ReadSpec {
        bound: ReadBound::Range(KeyRange::new(PkColList::from_slice(&[col]), &[], start, end)),
        ..rows_spec(order, limit_k)
    }
}

fn walk_ids(registry: &mut RelationRegistry, spec: &ReadSpec) -> Vec<u128> {
    let mut got: Vec<u128> = ids(&registry
        .scan_spec(TID, spec.clone(), walk_schema().layout_digest(), None)
        .unwrap())
    .into_iter()
    .map(|(id, w)| {
        assert_eq!(w, 1);
        id
    })
    .collect();
    got.sort_unstable();
    got
}

/// The ids whose value in the walk's column passes `keep`, NULLs excluded.
fn walk_expect(keep: impl Fn(u64) -> bool) -> Vec<u128> {
    (0..WALK_ROWS).filter(|&id| keep(id)).map(u128::from).collect()
}

/// An unselective index walk, and one with no index, return exactly the walk's rows:
/// no NULL, a signed range across zero, a U128 range over its full width.
#[test]
fn an_index_walk_traded_for_a_scan_returns_exactly_the_walk() {
    let p = |v: i64| key_image(TypeCode::I64, v as u64 as u128);
    let signed = index_walk(1, Cut::after(p(-10)), Cut::after(p(12)), Vec::new(), 0);
    let want_signed = walk_expect(|id| walk_row(id).0.is_some_and(|v| v > -10 && v <= 12));
    let (lo, hi) = (3u128 << 70, 40u128 << 70);
    let wide = index_walk(2, Cut::before(lo), Cut::before(hi), Vec::new(), 0);
    let want_wide = walk_expect(|id| walk_row(id).1.is_some_and(|b| b >= lo && b < hi));
    let (all_lo, all_hi) = (Cut::before(0), Cut::after(gnitz_wire::image_mask(8)));
    let everything = index_walk(1, all_lo, all_hi, Vec::new(), 0);
    let want_everything = walk_expect(|id| walk_row(id).0.is_some());
    // A point is selective enough that the indexed registry really walks.
    let point = index_walk(1, Cut::before(p(-31)), Cut::after(p(-31)), Vec::new(), 0);
    for indexed in [true, false] {
        let mut r = walk_fixture(&format!("walk_exact_{indexed}"), indexed);
        assert_eq!(walk_ids(&mut r, &signed), want_signed, "indexed {indexed}");
        assert_eq!(walk_ids(&mut r, &wide), want_wide, "indexed {indexed}");
        assert_eq!(walk_ids(&mut r, &everything), want_everything, "indexed {indexed}");
        assert_eq!(walk_ids(&mut r, &point), vec![1], "indexed {indexed}");
    }
}

/// A LIMIT cuts inside the walk's rows, never outside them, with or without an ORDER BY.
#[test]
fn a_limit_over_a_traded_walk_stays_inside_it() {
    let p = |v: i64| key_image(TypeCode::I64, v as u64 as u128);
    let inside = walk_expect(|id| walk_row(id).0.is_some_and(|v| (-20..20).contains(&v)));
    let by_val_desc = vec![OrderKey { col: 1, desc: true, nulls_first: false }];
    for indexed in [true, false] {
        let mut r = walk_fixture(&format!("walk_limit_{indexed}"), indexed);
        let got = walk_ids(
            &mut r,
            &index_walk(1, Cut::before(p(-20)), Cut::before(p(20)), Vec::new(), 3),
        );
        assert_eq!(got.len(), 3, "indexed {indexed}");
        assert!(got.iter().all(|id| inside.contains(id)), "indexed {indexed}: {got:?}");
        let got = walk_ids(
            &mut r,
            &index_walk(1, Cut::before(p(-20)), Cut::before(p(20)), by_val_desc.clone(), 3),
        );
        // The three largest non-NULL `val`s: 19, 17, 16 (id 50's `val` is NULL).
        assert_eq!(got, vec![48, 49, 51], "indexed {indexed}");
    }
}

/// A walk over a column with no key order is refused rather than scanned.
#[test]
fn a_walk_over_an_unindexable_column_is_refused() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::F64, false),
        ],
        &[0],
    );
    let mut r = RelationRegistry::new(
        &crate::test_support::scratch_dir("read", "walk_float"),
        Slot::SOLO,
        StoreConfig::default(),
    );
    r.register(RelationSpec {
        id: TID,
        kind: RelationKind::View(ViewProps::Plain),
        schema,
    })
    .unwrap();
    let spec = index_walk(1, Cut::before(0), Cut::after(9), Vec::new(), 0);
    let Err(err) = r.scan_spec(TID, spec, schema.layout_digest(), None) else {
        panic!("a walk over a float column must be refused");
    };
    assert!(err.to_string().contains("no key order"), "{err}");
}

/// A range over a PK prefix walks the table's own store even when an index names the
/// same columns: `INDEX(a)` under `PRIMARY KEY (a, b)` is never walked for `a`.
#[test]
fn a_pk_prefix_range_takes_the_pk_walk_over_a_matching_index() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[0, 1],
    );
    let mut r = RelationRegistry::new(
        &crate::test_support::scratch_dir("read", "pk_prefix_walk"),
        Slot::SOLO,
        StoreConfig::default(),
    );
    r.register(RelationSpec {
        id: TID,
        kind: RelationKind::BaseTable,
        schema,
    })
    .unwrap();
    r.add_index(TID, IndexClaim::Index { id: TID + 1, unique: false }, &[0])
        .unwrap();
    let mut bb = BatchBuilder::new(schema);
    for a in 0..8u64 {
        for b in 0..3u64 {
            bb.begin_row_opk(&[a as u128, b as u128], 1);
            bb.end_row();
        }
    }
    r.ingest(TID, bb.finish()).unwrap();

    let range = KeyRange::point(PkColList::from_slice(&[0]), &[], 5);
    let (cursor, unapplied) = r.open_bound(TID, ReadBound::Range(range)).unwrap();
    assert!(
        matches!(cursor, crate::read::SourceCursor::Full(_)),
        "a PK walk, not the index"
    );
    assert_eq!(unapplied, ReadBound::None, "a PK walk applies the whole range");
    let got = ids(&r
        .scan_spec(
            TID,
            ReadSpec::all_rows(ReadBound::Range(range)),
            schema.layout_digest(),
            None,
        )
        .unwrap());
    assert_eq!(got.len(), 3);
}

/// A bounded view whose sweep has dehydrated everything on disk, plus fresh rows
/// still in the RAM tier. `hydrator: None` is the point: a walk that meets a
/// skeleton row here can only fail, so a read that succeeds proves it met none.
fn dehydrated_fixture(name: &str, on_disk: std::ops::Range<u64>, in_ram: std::ops::Range<u64>) -> RelationRegistry {
    let schema = id_val_schema();
    let mut registry = RelationRegistry::new(
        &crate::test_support::scratch_dir("read", name),
        Slot::SOLO,
        StoreConfig::default(),
    );
    registry
        .register(RelationSpec {
            id: TID,
            kind: RelationKind::View(ViewProps::Bounded { capacity_bytes: 1 }),
            schema,
        })
        .unwrap();
    ingest(&mut registry, on_disk);
    registry.set_resume_generation(1);
    registry.checkpoint_ephemeral([]).unwrap();
    assert!(
        registry
            .relation_or_err(TID)
            .unwrap()
            .store()
            .held()
            .has_skeleton_rows(),
        "premise: the capacity sweep must have dehydrated the flushed shard",
    );
    ingest(&mut registry, in_ram);
    registry
}

fn ingest(registry: &mut RelationRegistry, ids: std::ops::Range<u64>) {
    let mut bb = BatchBuilder::new(id_val_schema());
    for id in ids {
        bb.begin_row(id as u128, 1);
        bb.put_u64(id);
        bb.end_row();
    }
    registry.ingest(TID, bb.finish()).unwrap();
}

fn pk_range(lo: u64, hi: u64) -> ReadSpec {
    ReadSpec {
        bound: ReadBound::Range(KeyRange::new(
            PkColList::from_slice(&[0]),
            &[],
            Cut::before(lo as u128),
            Cut::after(hi as u128),
        )),
        ..rows_spec(Vec::new(), 0)
    }
}

/// Whether a read hydrates is the **opened cursor's** question, not the store's:
/// a range that no skeleton shard's key band overlaps streams, even though the
/// store as a whole holds skeleton rows. The control read below meets one and
/// has nothing to recompute it with, which is what makes the first read's
/// success pruning rather than an empty store.
#[test]
fn a_bound_that_prunes_every_skeleton_shard_streams() {
    let mut r = dehydrated_fixture("skeleton_prune", 0..5, 100..105);

    let got = run(&mut r, &pk_range(100, 104)).unwrap();
    assert_eq!(ids(&got), (100..105).map(|i| (i as u128, 1)).collect::<Vec<_>>());

    let Err(err) = run(&mut r, &pk_range(0, 4)) else {
        panic!("a range over the dehydrated band must reach the hydrator");
    };
    assert!(err.to_string().contains("skeleton rows"), "{err}");
}

/// [`dehydrated_fixture`] over keys `0..5` on disk and `100..105` in RAM, plus a
/// second row `(2, val = 7)` under skeleton key 2, read two merge groups a chunk.
fn skeleton_fixture(name: &str) -> RelationRegistry {
    let mut registry = dehydrated_fixture(name, 0..5, 100..105);
    let mut bb = BatchBuilder::new(id_val_schema());
    bb.begin_row(2, 1);
    bb.put_u64(7);
    bb.end_row();
    registry.ingest(TID, bb.finish()).unwrap();
    registry.set_scan_chunk_rows(2);
    registry
}

/// The `(id, val, weight)` rows [`skeleton_fixture`] ingested whose id `keep` admits,
/// sorted.
fn ingested(keep: impl Fn(u64) -> bool) -> Vec<(u128, i64, i64)> {
    let mut rows: Vec<_> = (0..5)
        .chain(100..105)
        .filter(|&k| keep(k))
        .map(|k| (k as u128, k as i64, 1))
        .collect();
    if keep(2) {
        rows.push((2, 7, 1));
    }
    rows.sort();
    rows
}

/// A reply's `(id, val, weight)` rows, sorted.
fn rows_of(b: &Batch) -> Vec<(u128, i64, i64)> {
    let mut rows: Vec<_> = (0..b.count)
        .map(|i| {
            let val = payload0_i64(b, i);
            (b.get_pk(i), val, b.get_weight(i))
        })
        .collect();
    rows.sort();
    rows
}

/// Recomputes each skeleton key `k` as `(k, val = k)`, plus [`skeleton_fixture`]'s
/// `(2, 7)` for key 2, and records each call's key list.
#[derive(Default)]
struct Recompute {
    calls: Vec<Vec<u64>>,
}

impl SkeletonHydrator for Recompute {
    fn hydrate_keys(&mut self, _: &RelationRegistry, _: u64, keys: Vec<u8>) -> Result<Batch, StoreError> {
        let schema = id_val_schema();
        let ks: Vec<u64> = keys
            .chunks_exact(schema.pk_stride())
            .map(|k| u64::from_be_bytes(k.try_into().unwrap()))
            .collect();
        let mut bb = BatchBuilder::new(schema);
        for &k in &ks {
            for val in std::iter::once(k).chain((k == 2).then_some(7)) {
                bb.begin_row(k as u128, 1);
                bb.put_u64(val);
                bb.end_row();
            }
        }
        self.calls.push(ks);
        Ok(bb.finish().into_consolidated(&schema))
    }
}

fn pk_set(ids: &[u64]) -> ReadBound {
    let keys: Vec<[u8; 8]> = ids.iter().map(|k| k.to_be_bytes()).collect();
    ReadBound::PkSet(gnitz_wire::PkKeys::from_keys(8, keys.iter().map(|k| &k[..])))
}

/// Every read verb over a skeleton store answers exactly the ingested rows, and a
/// chunked walk hydrates once per chunk, only the skeleton keys that chunk met.
#[test]
fn a_skeleton_store_hydrates_chunk_by_chunk() {
    let r = skeleton_fixture("skeleton_chunks");
    let schema = id_val_schema();
    let mut h = Recompute::default();

    assert_eq!(
        rows_of(
            &r.scan_spec(TID, rows_spec(Vec::new(), 0), schema.layout_digest(), Some(&mut h))
                .unwrap()
        ),
        ingested(|_| true)
    );
    for k in [2u64, 101] {
        let spec = ReadSpec {
            bound: pk_set(&[k]),
            ..rows_spec(Vec::new(), 0)
        };
        let hit = r.scan_spec(TID, spec, schema.layout_digest(), Some(&mut h)).unwrap();
        assert_eq!(rows_of(&hit), ingested(|i| i == k));
    }

    h.calls.clear();
    let got = r
        .scan_spec(TID, pk_range(0, 104), schema.layout_digest(), Some(&mut h))
        .unwrap();
    assert_eq!(rows_of(&got), ingested(|i| i <= 104));
    assert_eq!(
        h.calls,
        vec![vec![0, 1], vec![2, 3], vec![4]],
        "one hydration per chunk"
    );

    let spec = ReadSpec {
        bound: pk_set(&[1, 4, 102]),
        ..rows_spec(Vec::new(), 0)
    };
    let got = r.scan_spec(TID, spec, schema.layout_digest(), Some(&mut h)).unwrap();
    assert_eq!(rows_of(&got), ingested(|i| [1, 4, 102].contains(&i)));
}

/// A bounded join view's PK is not unique, so a scan chunk can end inside a PK group
/// of live rows; the chunks still concatenate.
#[test]
fn a_scan_chunk_boundary_inside_a_pk_group() {
    let mut r = dehydrated_fixture("skeleton_split_group", 0..2, 100..102);
    let mut bb = BatchBuilder::new(id_val_schema());
    bb.begin_row(100, 1);
    bb.put_u64(7);
    bb.end_row();
    r.ingest(TID, bb.finish()).unwrap();
    // Groups: skeleton 0, skeleton 1, (100, 7) | (100, 100), (101, 101).
    r.set_scan_chunk_rows(3);
    let got = r
        .scan_spec(
            TID,
            rows_spec(Vec::new(), 0),
            id_val_schema().layout_digest(),
            Some(&mut Recompute::default()),
        )
        .unwrap();
    assert_eq!(
        rows_of(&got),
        vec![(0, 0, 1), (1, 1, 1), (100, 7, 1), (100, 100, 1), (101, 101, 1)]
    );
}

/// A LIMIT cuts at the row, not at the hydrated group: key 2 hydrates to two rows,
/// and `LIMIT 1` ships one.
#[test]
fn a_limit_stops_at_the_row_inside_a_hydrated_group() {
    let r = skeleton_fixture("skeleton_limit");
    let mut h = Recompute::default();
    let spec = ReadSpec {
        bound: pk_set(&[2]),
        ..rows_spec(Vec::new(), 1)
    };
    let got = r
        .scan_spec(TID, spec, id_val_schema().layout_digest(), Some(&mut h))
        .unwrap();
    assert_eq!(got.count, 1);
}

/// A raw drain has no hydrator to hand a skeleton row to, so meeting one is a bug.
#[test]
#[should_panic(expected = "a raw drain met a skeleton row")]
fn a_raw_drain_over_a_skeleton_run_panics() {
    let r = dehydrated_fixture("skeleton_raw_drain", 0..5, 100..105);
    r.relation_or_err(TID).unwrap().cursor().drain_chunk(usize::MAX);
}

/// A fold reply schema off the partial layout the fold derives — missing, then
/// mistyping, its aggregate column — is rejected.
#[test]
fn a_fold_reply_schema_not_matching_its_partial_layout_is_rejected() {
    let r = rows_fixture("fold_reply", 4, 1);
    let spec = ReadSpec {
        bound: ReadBound::None,
        predicate: Vec::new(),
        sink: ReadSink {
            map: None,
            kind: SinkKind::Fold(AggReadSpec {
                group_cols: vec![1],
                aggs: vec![AggDescriptor::COUNT_STAR],
            }),
        },
    };
    let partial = |last: Option<TypeCode>| {
        let mut cols = vec![
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, false),
        ];
        cols.extend(last.map(|tc| SchemaColumn::new(tc, false)));
        SchemaDescriptor::new(&cols, &[0])
    };
    // The derived partial itself is accepted, which is what makes the two
    // refusals below about the layout rather than about the spec.
    assert!(r
        .scan_spec(TID, spec.clone(), partial(Some(TypeCode::I64)).layout_digest(), None)
        .is_ok());
    for bad in [partial(None), partial(Some(TypeCode::F64))] {
        let Err(err) = r.scan_spec(TID, spec.clone(), bad.layout_digest(), None) else {
            panic!("a reply schema off the partial layout must be rejected");
        };
        assert!(err.to_string().contains("reply schema does not match"), "{err}");
    }
}

#[test]
fn a_delta_cursor_expires_below_the_floor_and_not_at_it() {
    assert!(!delta_cursor_expired(0, 0), "nothing dropped");
    assert!(!delta_cursor_expired(5, 5), "at the floor");
    assert!(!delta_cursor_expired(6, 5), "above the floor");
    assert!(delta_cursor_expired(4, 5), "round 5 was dropped");
}

/// 1M rows narrowed to `val ∈ [0, sel% · N)` by an index walk's membership
/// (`GNITZ_BENCH_SHAPE=membership`) or by the VM predicate `val >= lo AND val < hi`
/// (`predicate`), or by both (`both`, the predicate keeping the walk's upper half).
/// Difference two pass counts, one shape per process:
///
///   cargo build -p gnitz-store --release --tests
///   for s in membership predicate both; do for p in 1 21; do \
///     GNITZ_BENCH_SHAPE=$s GNITZ_BENCH_SEL=10 GNITZ_BENCH_PASSES=$p perf stat -e instructions:u \
///     cargo test -p gnitz-store --release survivors_membership_bench -- --ignored --nocapture
///   done; done
#[test]
#[ignore]
fn survivors_membership_bench() {
    use gnitz_expr::{CmpOp, ExprBuilder, LogicalInstr};
    use std::hint::black_box;

    const N: u64 = 1_000_000;
    let shape = std::env::var("GNITZ_BENCH_SHAPE").unwrap_or_else(|_| "membership".to_string());
    let sel: u64 = std::env::var("GNITZ_BENCH_SEL").map_or(10, |s| s.parse().unwrap());
    let passes: usize = std::env::var("GNITZ_BENCH_PASSES").map_or(1, |s| s.parse().unwrap());
    let hi = (N * sel / 100) as i64;
    let mut r = rows_fixture("survivors_bench", N, 1);
    let img = |v: i64| key_image(TypeCode::I64, v as u64 as u128);
    let walk = ReadBound::Range(KeyRange::new(
        PkColList::from_slice(&[1]),
        &[],
        Cut::before(img(0)),
        Cut::before(img(hi)),
    ));
    // `lo <= val < hi` as the wire predicate; `hi = None` leaves the top open.
    let between = |lo: i64, hi: Option<i64>| {
        let mut eb = ExprBuilder::new();
        let v = eb.emit(LogicalInstr::LoadColInt { col: 1 });
        let lo_c = eb.emit(LogicalInstr::LoadConst { val: lo, unsigned: false });
        let mut keep = eb.emit(LogicalInstr::Cmp { op: CmpOp::Ge, a: v, b: lo_c });
        if let Some(hi) = hi {
            let hi_c = eb.emit(LogicalInstr::LoadConst { val: hi, unsigned: false });
            let lt = eb.emit(LogicalInstr::Cmp { op: CmpOp::Lt, a: v, b: hi_c });
            keep = eb.emit(LogicalInstr::BoolBinary { a: keep, b: lt, is_or: false });
        }
        eb.build(Some(keep)).unwrap().to_blob_bytes()
    };
    let (spec, want) = match shape.as_str() {
        "membership" => (ReadSpec { bound: walk, ..rows_spec(Vec::new(), 0) }, hi),
        "predicate" => (
            ReadSpec {
                predicate: between(0, Some(hi)),
                ..rows_spec(Vec::new(), 0)
            },
            hi,
        ),
        "both" => (
            ReadSpec {
                bound: walk,
                predicate: between(hi / 2, None),
                ..rows_spec(Vec::new(), 0)
            },
            hi - hi / 2,
        ),
        other => panic!("GNITZ_BENCH_SHAPE={other}: membership, predicate or both"),
    };
    for _ in 0..passes {
        let got = black_box(run(&mut r, &spec).unwrap());
        assert_eq!(got.count as i64, want);
    }
    println!("survivors {shape} sel {sel}% passes {passes}");
}

/// A read whose window lies in its first `m` rows drains fewer than `2m + first`
/// of them, and never more than a flat `chunk_rows` drain would.
#[test]
fn drain_ramp_doubles_onto_the_chunk_grid() {
    let chunk = 1000;
    let sizes: Vec<usize> = drain_ramp(1, chunk).take(14).collect();
    assert_eq!(sizes, [1, 2, 4, 8, 16, 32, 64, 128, 256, 489, 1000, 1000, 1000, 1000]);
    for m in 1..5 * chunk {
        let mut drained = 0;
        for rows in drain_ramp(1, chunk) {
            drained += rows;
            if drained >= m {
                break;
            }
        }
        assert!(drained < 2 * m + 1, "m = {m}: drained {drained}");
        assert!(drained <= m.next_multiple_of(chunk), "m = {m}: drained {drained}");
    }
    assert!(
        drain_ramp(chunk, chunk).take(4).all(|rows| rows == chunk),
        "an unwindowed drain stays flat"
    );
}
