use super::*;
use crate::relation::{RelationKind, RelationSpec};
use crate::test_support::{
    make_batch_raw, make_schema_u64_i64, map_of, opk_pk, payload0_i64, relation_fixture, rows_spec, RelationFixture,
    TID,
};
use gnitz_expr::{
    payload_is_null, payload_string, payload_u64, CmpOp, ExprBuilder, LogicalInstr, LogicalProgram, Sink,
};
use gnitz_wire::TypeCode;
use gnitz_wire::{key_image, AggDescriptor, AggReadSpec, Cut, KeyRange, OrderKey, PkColList, ReadSink};
use gnitz_wire::{PkKeys, ViewProps};
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::{Placement, SchemaColumn};

/// [`relation_fixture`] over a plain `(id U64 PK | val I64)` view holding the
/// `(id, weight, val)` `rows`. A view's store runs no `enforce_unique_pk`, so a PK
/// may repeat and a weight above 1 is admitted verbatim.
fn view(rows: &[(u64, i64, i64)]) -> RelationFixture {
    let schema = make_schema_u64_i64();
    let kind = RelationKind::View(ViewProps::Plain);
    relation_fixture(kind, schema, &[], make_batch_raw(&schema, rows))
}

/// A `(id U64 PK | val I64)` reply's `(id, weight, val)` rows, sorted: a reply's
/// order is not a contract, the client re-sorts it.
fn rows_of(b: &Batch) -> Vec<(u64, i64, i64)> {
    let mut rows: Vec<_> = (0..b.len())
        .map(|i| (b.get_pk(i) as u64, b.get_weight(i), payload0_i64(b, i)))
        .collect();
    rows.sort_unstable();
    rows
}

/// `spec` over `TID`, replying in the relation's own layout.
fn run(r: &RelationRegistry, spec: ReadSpec) -> Result<Rc<Batch>, String> {
    let layout = r.relation(TID).unwrap().schema().layout_digest();
    r.scan_spec(TID, spec, layout, None)
}

/// The layout `sink` replies in over `schema`.
fn fold_layout(schema: &SchemaDescriptor, sink: &ReadSink) -> u64 {
    SinkPlan::from_wire(schema, sink, usize::MAX)
        .unwrap()
        .output_schema()
        .layout_digest()
}

fn order_by(col: u16, desc: bool) -> Vec<OrderKey> {
    vec![OrderKey { col, desc, nulls_first: false }]
}

/// `lo <= col`, and `col < hi` under `Some(hi)`, as a wire predicate.
fn between(col: u32, lo: i64, hi: Option<i64>) -> Vec<u8> {
    let mut eb = ExprBuilder::new();
    let v = eb.emit(LogicalInstr::LoadCol { col });
    let lo_c = eb.emit(LogicalInstr::LoadConst { val: lo, unsigned: false });
    let mut keep = eb.emit(LogicalInstr::Cmp { op: CmpOp::Ge, a: v, b: lo_c });
    if let Some(hi) = hi {
        let hi_c = eb.emit(LogicalInstr::LoadConst { val: hi, unsigned: false });
        let lt = eb.emit(LogicalInstr::Cmp { op: CmpOp::Lt, a: v, b: hi_c });
        keep = eb.emit(LogicalInstr::BoolBinary { a: keep, b: lt, is_or: false });
    }
    eb.build(vec![Sink::Reg(keep)]).unwrap().to_blob_bytes()
}

/// `v`'s key image in an I64 column.
fn img(v: i64) -> u128 {
    key_image(TypeCode::I64, v as u64 as u128)
}

/// A `pk IN (…)` bound over a U64 PK.
fn pk_set(ids: &[u64]) -> ReadBound {
    let schema = make_schema_u64_i64();
    let keys: Vec<_> = ids.iter().map(|&k| opk_pk(&schema, &[k as u128])).collect();
    ReadBound::PkSet(PkKeys::from_keys(8, keys.iter().map(Vec::as_slice)))
}

/// A range over the U64 PK.
fn pk_range(start: Cut, end: Cut) -> ReadBound {
    ReadBound::Range(KeyRange::new(PkColList::from_slice(&[0]), &[], start, end))
}

// ── Sinks ────────────────────────────────────────────────────────────

/// Each sink replies only in the layout it produces, and a read of the whole
/// relation is the store's cached snapshot itself.
#[test]
fn every_sink_answers_only_in_its_own_layout() {
    let r = view(&[(1, 1, 10), (2, 1, 20)]);
    let schema = make_schema_u64_i64();
    let whole = rows_spec(None, vec![], 0);
    let snapshot = r.relation(TID).unwrap().full_scan();
    assert!(Rc::ptr_eq(&run(&r, whole.clone()).unwrap(), &snapshot));

    let agg = AggReadSpec {
        group_cols: vec![1],
        aggs: vec![AggDescriptor::COUNT_STAR],
    };
    let fold = ReadSpec {
        sink: ReadSink { map: None, kind: SinkKind::Fold(agg) },
        ..whole.clone()
    };
    let fold_layout = fold_layout(&schema, &fold.sink);
    let own = schema.layout_digest();
    let sinks = [
        (whole, own),
        (rows_spec(None, vec![], 1), own),
        (rows_spec(None, order_by(1, false), 1), own),
        (fold, fold_layout),
    ];
    for (spec, layout) in sinks {
        assert!(r.scan_spec(TID, spec.clone(), layout, None).is_ok(), "{spec:?}");
        assert!(r.scan_spec(TID, spec.clone(), layout ^ 1, None).is_err(), "{spec:?}");
    }
}

/// An ORDER BY … LIMIT reply is the comparator-smallest rows whose summed weight
/// covers the window, the boundary row kept whole — only the client, which sees
/// every worker's rows, may clip it — at any window size, mapped or not.
#[test]
fn top_k_keeps_the_smallest_rows_covering_the_window() {
    let ids = |n: u64, w: i64| (0..n).map(|id| (id, w, id as i64)).collect::<Vec<_>>();
    // (rows, descending, limit_k, reply).
    let cases = [
        // Under the mid-scan residency cap: only the terminal trim sheds.
        (ids(5, 1), true, 3, vec![(2, 1, 2), (3, 1, 3), (4, 1, 4)]),
        // Past it, chunk after chunk.
        (ids(20, 1), true, 3, vec![(17, 1, 17), (18, 1, 18), (19, 1, 19)]),
        (ids(20, 1), false, 3, ids(3, 1)),
        // The window counts weight, so one row covers more than the row count.
        (ids(5, 100_000), true, 65_537, vec![(4, 100_000, 4)]),
        // An unbounded window saturates and trims nothing.
        (ids(5, 100_000), true, u64::MAX, ids(5, 100_000)),
        (vec![(1, 3, 100), (2, 1, 50), (3, 1, 10)], true, 2, vec![(1, 3, 100)]),
        (ids(40, 3), false, 2, vec![(0, 3, 0)]),
    ];
    let schema = make_schema_u64_i64();
    for (i, (rows, desc, limit_k, want)) in cases.into_iter().enumerate() {
        let mut r = view(&rows);
        r.set_scan_chunk_rows(8);
        for map in [None, map_of(LogicalProgram::copy_cols(&[1]), &schema)] {
            let mapped = map.is_some();
            let got = run(&r, rows_spec(map, order_by(1, desc), limit_k)).unwrap();
            assert_eq!(rows_of(&got), want, "case {i}, mapped {mapped}");
        }
    }
}

/// With no ORDER BY, a LIMIT stops at the row whose weight reaches the window —
/// never short of it, never a row past it, even where each survivor is a range of
/// its own — and a `limit_k` past `i64::MAX` saturates rather than wrapping to a
/// window the first chunk covers.
#[test]
fn a_limit_without_order_stops_at_the_row_covering_the_window() {
    // (row weight, limit_k) → (rows shipped, summed weight), over 40 rows.
    for (w, limit_k, want) in [
        (1, 5, (5, 5)),
        (3, 5, (2, 6)),
        (1, 1 << 63, (40, 40)),
        (1, u64::MAX, (40, 40)),
    ] {
        let rows: Vec<_> = (0..40).map(|id| (id, w, id as i64)).collect();
        let mut r = view(&rows);
        r.set_scan_chunk_rows(8);
        let got = run(&r, rows_spec(None, vec![], limit_k)).unwrap();
        let summed: i64 = (0..got.len()).map(|i| got.get_weight(i)).sum();
        assert_eq!((got.len(), summed), want, "weight {w}, limit_k {limit_k}");
    }

    // `val = id % 2`, and `val < 1` keeps every other row: one range per survivor.
    let rows: Vec<_> = (0..500).map(|id| (id, 1, id as i64 % 2)).collect();
    let r = view(&rows);
    let spec = ReadSpec {
        predicate: between(1, 0, Some(1)),
        ..rows_spec(
            map_of(LogicalProgram::copy_cols(&[1]), &make_schema_u64_i64()),
            vec![],
            5,
        )
    };
    let got = rows_of(&run(&r, spec).unwrap());
    assert_eq!(got.len(), 5, "{got:?}");
    assert!(got.iter().all(|&(_, w, val)| w == 1 && val == 0), "{got:?}");
}

/// The fold sink folds exactly the predicate's survivors of every chunk, mapped or
/// not: one partial per group, its count the survivors' summed weight.
#[test]
fn a_fold_counts_the_survivors_of_every_chunk() {
    let schema = make_schema_u64_i64();
    let rows: Vec<_> = (0..20).map(|id| (id, 2, id as i64 % 3)).collect();
    let mut r = view(&rows);
    r.set_scan_chunk_rows(4);
    let agg = AggReadSpec {
        group_cols: vec![1],
        aggs: vec![AggDescriptor::COUNT_STAR],
    };
    let layout = fold_layout(
        &schema,
        &ReadSink {
            map: None,
            kind: SinkKind::Fold(agg.clone()),
        },
    );
    for map in [None, map_of(LogicalProgram::copy_cols(&[1]), &schema)] {
        let spec = ReadSpec {
            bound: ReadBound::None,
            predicate: between(1, 0, Some(2)),
            sink: ReadSink { map, kind: SinkKind::Fold(agg.clone()) },
        };
        let got = r.scan_spec(TID, spec, layout, None).unwrap();
        let mut groups: Vec<(i64, i64, i64)> = (0..got.len())
            .map(|i| {
                (
                    gnitz_wire::decode_opk_i64(got.get_pk_bytes(i), gnitz_wire::FixedInt::I64),
                    got.get_weight(i),
                    payload0_i64(&*got, i),
                )
            })
            .collect();
        groups.sort_unstable();
        // Seven ids per surviving `val`, each at weight 2; `val = 2` fails the predicate.
        assert_eq!(groups, [(0, 1, 14), (1, 1, 14)]);
    }
}

/// A malformed request is refused, never degraded to a scan that answers it: an
/// ORDER BY column, a walk column or a map output slot the relation has not got, a
/// walk over a column with no key order, a key list at a foreign stride.
#[test]
fn a_malformed_request_is_refused() {
    let col = |tc| SchemaColumn::new(tc, false);
    let schema = SchemaDescriptor::new(&[col(TypeCode::U64), col(TypeCode::I64), col(TypeCode::F64)], &[0]);
    let kind = RelationKind::View(ViewProps::Plain);
    let r = relation_fixture(kind, schema, &[], Batch::empty_with_schema(&schema));
    let walk = |c: u32| {
        let r = KeyRange::new(PkColList::from_slice(&[c]), &[], Cut::before(0), Cut::after(9));
        ReadSpec::all_rows(ReadBound::Range(r))
    };
    let own = schema.layout_digest();
    let two_slots = SchemaDescriptor::new(&[col(TypeCode::U64), col(TypeCode::I64), col(TypeCode::I64)], &[0]);
    let cases = [
        (rows_spec(None, order_by(99, false), 1), own),
        (walk(99), own),
        (walk(2), own),
        (
            ReadSpec::all_rows(ReadBound::PkSet(PkKeys::from_keys(16, [&[0u8; 16][..]]))),
            own,
        ),
        (
            rows_spec(map_of(LogicalProgram::copy_cols(&[1]), &two_slots), vec![], 0),
            two_slots.layout_digest(),
        ),
    ];
    for (spec, layout) in cases {
        assert!(r.scan_spec(TID, spec.clone(), layout, None).is_err(), "{spec:?}");
    }
}

/// Every keyed read returns each named key's whole PK group at its folded weights —
/// a view's PK is not unique, and a group larger than the chunk budget crosses it
/// in one piece — and a key the store does not hold misses.
#[test]
fn keyed_reads_return_each_named_keys_whole_group() {
    let schema = make_schema_u64_i64();
    let mut rows: Vec<_> = (0..10).map(|id| (id, 1, id as i64 * 10)).collect();
    rows.extend([(7, 1, 71), (7, 1, 72), (7, 1, 73)]);
    let mut r = view(&rows);
    // Retracted in a later ingest, (7, 70) is a ghost every read must drop.
    r.ingest(TID, make_batch_raw(&schema, &[(7, -1, 70)])).unwrap();
    r.set_scan_chunk_rows(2);
    let reads = [
        (ReadBound::None, (0..10).collect()),
        (pk_set(&[2, 5, 7, 99]), vec![2, 5, 7]),
        (pk_set(&[7]), vec![7]),
        (
            pk_range(Cut::before(5), Cut::after(u64::MAX as u128)),
            (5..10).collect(),
        ),
        (pk_range(Cut::before(5), Cut::before(8)), vec![5, 6, 7]),
        (
            ReadBound::Range(KeyRange::point(PkColList::from_slice(&[0]), &[], 7)),
            vec![7],
        ),
    ];
    for (bound, ids) in reads {
        let mut want: Vec<_> = rows
            .iter()
            .copied()
            .filter(|&(id, _, val)| ids.contains(&id) && val != 70)
            .collect();
        want.sort_unstable();
        assert_eq!(
            rows_of(&run(&r, ReadSpec::all_rows(bound.clone())).unwrap()),
            want,
            "{bound:?}"
        );
    }
}

// ── Index walks ──────────────────────────────────────────────────────

const WALK_ROWS: u64 = 64;

/// Row `id`'s `val` (`id − 32`, straddling zero) and `big` (`id << 70`); every fifth `val`
/// and every seventh `big` is NULL.
fn walk_row(id: u64) -> (Option<i64>, Option<u128>) {
    let val = (!id.is_multiple_of(5)).then_some(id as i64 - 32);
    let big = (!id.is_multiple_of(7)).then_some((id as u128) << 70);
    (val, big)
}

/// An index walk returns exactly its range — no NULL, a signed range across zero, a
/// U128 range — whether it is selective enough to walk the index (at most 1/16 of
/// the rows) or is traded for a scan filtered by it; a LIMIT cuts inside the range,
/// with or without an ORDER BY.
#[test]
fn an_index_walk_returns_exactly_its_range() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::U128, true),
        ],
        &[0],
    );
    let mut bb = BatchBuilder::new(&schema);
    for id in 0..WALK_ROWS {
        let (val, big) = walk_row(id);
        bb.begin_row(id as u128, 1);
        bb.put_opt_int(val.map(|v| v as u128));
        bb.put_opt_int(big);
        bb.end_row();
    }
    let r = relation_fixture(RelationKind::BaseTable, schema, &[1, 2], bb.finish());

    let vals = |keep: &dyn Fn(i64) -> bool| -> Vec<u64> {
        (0..WALK_ROWS).filter(|&id| walk_row(id).0.is_some_and(keep)).collect()
    };
    let bigs = |lo: u128, hi: u128| -> Vec<u64> {
        (0..WALK_ROWS)
            .filter(|&id| walk_row(id).1.is_some_and(|b| b >= lo && b < hi))
            .collect()
    };
    let (all_lo, all_hi) = (Cut::before(0), Cut::after(gnitz_wire::image_mask(8)));
    // (column, start, end, walks the index, ids).
    let walks = [
        (
            1,
            Cut::after(img(-10)),
            Cut::after(img(12)),
            false,
            vals(&|v| v > -10 && v <= 12),
        ),
        (1, Cut::after(img(-3)), Cut::after(img(1)), true, vec![31, 32, 33]),
        (1, Cut::before(img(-31)), Cut::after(img(-31)), true, vec![1]),
        (1, all_lo, all_hi, false, vals(&|_| true)),
        (
            2,
            Cut::before(3 << 70),
            Cut::before(40 << 70),
            false,
            bigs(3 << 70, 40 << 70),
        ),
        (2, Cut::before(3 << 70), Cut::before(6 << 70), true, vec![3, 4, 5]),
    ];
    let ids = |spec: ReadSpec| rows_of_walk(&r.scan_spec(TID, spec, schema.layout_digest(), None).unwrap());
    for (col, start, end, walks, want) in walks {
        let bound = ReadBound::Range(KeyRange::new(PkColList::from_slice(&[col]), &[], start, end));
        let (cursor, _) = r.open_bound(TID, bound.clone()).unwrap();
        assert_eq!(matches!(cursor, SourceCursor::Bounded(_)), walks, "{bound:?}");
        let spec = |order, limit_k| ReadSpec {
            bound: bound.clone(),
            ..rows_spec(None, order, limit_k)
        };
        assert_eq!(ids(spec(vec![], 0)), want, "{bound:?}");
        let some = ids(spec(vec![], 2));
        assert!(
            some.len() == want.len().min(2) && some.iter().all(|id| want.contains(id)),
            "{bound:?}: {some:?}"
        );
        // Both columns rise with the id, so the two largest are the last two ids.
        let top = ids(spec(order_by(col as u16, true), 2));
        assert_eq!(top, want[want.len().saturating_sub(2)..], "{bound:?}");
    }
}

/// A walk reply's ids, sorted, each at weight 1.
fn rows_of_walk(b: &Batch) -> Vec<u64> {
    let mut ids: Vec<u64> = (0..b.len())
        .map(|i| {
            assert_eq!(b.get_weight(i), 1);
            b.get_pk(i) as u64
        })
        .collect();
    ids.sort_unstable();
    ids
}

// ── Maps onto the keeper ─────────────────────────────────────────────

/// A base table over `cols` (PK = column 0) of `n` rows, `put_row` writing each
/// row's payload, scanned `chunk_rows` rows at a time so the keeper is grown and
/// reused across chunks — the only way a stale-byte leak in a map's tail writes
/// would show.
fn map_fixture(
    cols: &[SchemaColumn],
    n: u64,
    chunk_rows: usize,
    mut put_row: impl FnMut(&mut BatchBuilder, u64),
) -> RelationFixture {
    let schema = SchemaDescriptor::new(cols, &[0]);
    let mut bb = BatchBuilder::new(&schema);
    for id in 0..n {
        bb.begin_row(id as u128, 1);
        put_row(&mut bb, id);
        bb.end_row();
    }
    let mut r = relation_fixture(RelationKind::BaseTable, schema, &[], bb.finish());
    r.set_scan_chunk_rows(chunk_rows);
    r
}

/// A copy of every payload column of a 65-column table (1 PK + 64 payload) — the
/// widest schema the row-major null word admits, so the output-coverage mask is
/// exercised at its top bit.
#[test]
fn a_copy_of_64_payload_columns_covers_every_slot() {
    const P: usize = 64;
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend((0..P).map(|_| SchemaColumn::new(TypeCode::I64, false)));
    let r = map_fixture(&cols, 40, 16, |bb, id| {
        for k in 0..P {
            bb.put_u64(id * 100 + k as u64);
        }
    });
    let reply = SchemaDescriptor::new(&cols, &[0]);
    let copies: Vec<u32> = (1..=P as u32).collect();
    let spec = rows_spec(map_of(LogicalProgram::copy_cols(&copies), &reply), vec![], 0);
    let got = r.scan_spec(TID, spec, reply.layout_digest(), None).unwrap();
    assert_eq!(got.len(), 40);
    for row in 0..got.len() {
        let id = got.get_pk(row) as u64;
        for k in 0..P {
            assert_eq!(payload_u64(&*got, row, k), id * 100 + k as u64, "row {row} slot {k}");
        }
    }
}

/// A map sourcing only the PK has an empty null permutation, and must still write
/// every null word: an unwritten word would surface the recycled keeper's bytes.
#[test]
fn a_pk_sourced_map_zeroes_every_null_word() {
    let mut r = view(&(0..300).map(|id| (id, 1, id as i64 * 7)).collect::<Vec<_>>());
    r.set_scan_chunk_rows(32);
    let reply = crate::test_support::u64_pk_schema(SchemaColumn::new(TypeCode::U64, false));
    let spec = rows_spec(map_of(LogicalProgram::copy_cols(&[0]), &reply), vec![], 0);
    let got = r.scan_spec(TID, spec, reply.layout_digest(), None).unwrap();
    assert_eq!(got.len(), 300);
    for row in 0..got.len() {
        assert_eq!(got.get_null_word(row), 0, "row {row}");
        assert_eq!(payload_u64(&*got, row, 0), got.get_pk(row) as u64, "row {row}");
    }
}

/// A permuted copy of a nullable column, an out-of-line string and the PK: the
/// source-start and destination-base arithmetic, each cell's string relocated into
/// the keeper's own blob (the source chunk drops), and the null permutation together.
#[test]
fn a_permuted_copy_relocates_strings_and_nulls() {
    const N: u64 = 200;
    let string = |id: u64| match id.is_multiple_of(3) {
        // A span shared by every third row exercises the relocator's dedup cache.
        true => "a-shared-out-of-line-value".to_string(),
        false => format!("a-distinct-out-of-line-value-{id}"),
    };
    let cols = [
        SchemaColumn::new(TypeCode::U64, false),
        SchemaColumn::new(TypeCode::I64, true),
        SchemaColumn::new(TypeCode::String, false),
    ];
    let r = map_fixture(&cols, N, 24, |bb, id| {
        bb.put_opt_int((!id.is_multiple_of(5)).then_some(id as u128 * 3));
        bb.put_string(&string(id));
    });
    // id U64 PK | s STRING | id U64 (from the PK) | nv I64 NULL.
    let reply = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let spec = rows_spec(map_of(LogicalProgram::copy_cols(&[2, 0, 1]), &reply), vec![], 0);
    let got = r.scan_spec(TID, spec, reply.layout_digest(), None).unwrap();
    let mut decoded: Vec<_> = (0..got.len())
        .map(|row| {
            let nv = (!payload_is_null(&*got, row, 2)).then(|| payload_u64(&*got, row, 2) as i64);
            let s = payload_string(&*got, row, 0);
            (got.get_pk(row) as u64, s, payload_u64(&*got, row, 1), nv)
        })
        .collect();
    decoded.sort();
    let want: Vec<_> = (0..N)
        .map(|id| (id, string(id), id, (!id.is_multiple_of(5)).then_some(id as i64 * 3)))
        .collect();
    assert_eq!(decoded, want);
}

/// A computed column (`nv * 2`) beside a copied one, under a predicate: the compute
/// kernel writes the survivors onto the keeper's tail at a non-zero base, and the
/// nullable source runs the emit's null merge over the already-permuted word.
#[test]
fn a_computed_map_writes_at_the_keepers_tail() {
    const N: u64 = 300;
    let cols = [
        SchemaColumn::new(TypeCode::U64, false),
        SchemaColumn::new(TypeCode::I64, true),
        SchemaColumn::new(TypeCode::I64, false),
    ];
    let r = map_fixture(&cols, N, 32, |bb, id| {
        bb.put_opt_int((!id.is_multiple_of(4)).then_some(id as u128));
        bb.put_u64(id * 10);
    });
    // id U64 PK | nv * 2 I64 NULL | keep I64.
    let reply = SchemaDescriptor::new(&cols, &[0]);
    let mut eb = ExprBuilder::new();
    let (v, two) = (
        eb.emit(LogicalInstr::LoadCol { col: 1 }),
        eb.emit(LogicalInstr::LoadConst { val: 2, unsigned: false }),
    );
    let doubled = eb.emit(LogicalInstr::IntArith {
        op: gnitz_expr::IntArithOp::Mul,
        a: v,
        b: two,
    });
    let program = eb.build(vec![Sink::Reg(doubled), Sink::Col(2)]).unwrap();
    // `keep = id * 10 < 1000` keeps ids 0..100, across several chunks.
    let spec = ReadSpec {
        predicate: between(2, 0, Some(1000)),
        ..rows_spec(map_of(program, &reply), vec![], 0)
    };
    let got = r.scan_spec(TID, spec, reply.layout_digest(), None).unwrap();
    let mut decoded: Vec<_> = (0..got.len())
        .map(|row| {
            let doubled = (!payload_is_null(&*got, row, 0)).then(|| payload_u64(&*got, row, 0) as i64);
            (got.get_pk(row) as u64, doubled, payload_u64(&*got, row, 1) as i64)
        })
        .collect();
    decoded.sort();
    let want: Vec<_> = (0..100u64)
        .map(|id| (id, (!id.is_multiple_of(4)).then_some(id as i64 * 2), id as i64 * 10))
        .collect();
    assert_eq!(decoded, want);
}

// ── Skeleton rows ────────────────────────────────────────────────────

/// A bounded view whose sweep has dehydrated every row of `on_disk`, plus the rows of
/// `in_ram` still in the RAM tier, each `(id, 1, id)`.
fn dehydrated_fixture(on_disk: std::ops::Range<u64>, in_ram: std::ops::Range<u64>) -> RelationFixture {
    let schema = make_schema_u64_i64();
    let rows =
        |ids: std::ops::Range<u64>| make_batch_raw(&schema, &ids.map(|id| (id, 1, id as i64)).collect::<Vec<_>>());
    let kind = RelationKind::View(ViewProps::Bounded { capacity_bytes: 1 });
    let mut registry = relation_fixture(kind, schema, &[], rows(on_disk));
    registry.checkpoint_ephemeral([], 1).unwrap();
    assert!(
        registry.relation(TID).unwrap().table().has_skeleton_rows(),
        "premise: the capacity sweep must have dehydrated the flushed shard",
    );
    registry.ingest(TID, rows(in_ram)).unwrap();
    registry
}

/// Whether a read hydrates is the **opened cursor's** question, not the store's: a
/// range that no skeleton shard's key band overlaps streams with no hydrator, even
/// though the store as a whole holds skeleton rows; one that meets a skeleton row
/// has nothing to recompute it with.
#[test]
fn a_bound_that_prunes_every_skeleton_shard_streams() {
    let r = dehydrated_fixture(0..5, 100..105);
    let got = run(&r, ReadSpec::all_rows(pk_range(Cut::before(100), Cut::after(104)))).unwrap();
    assert_eq!(
        rows_of(&got),
        (100..105).map(|id| (id, 1, id as i64)).collect::<Vec<_>>()
    );

    let Err(err) = run(&r, ReadSpec::all_rows(pk_range(Cut::before(0), Cut::after(4)))) else {
        panic!("a range over the dehydrated band must reach the hydrator");
    };
    assert!(err.contains("skeleton rows"), "{err}");
}

/// [`dehydrated_fixture`] over keys `0..5` on disk and `100..105` in RAM, plus a second
/// row `(2, val = 7)` under skeleton key 2, read two merge groups a chunk.
fn skeleton_fixture() -> RelationFixture {
    let mut registry = dehydrated_fixture(0..5, 100..105);
    registry
        .ingest(TID, make_batch_raw(&make_schema_u64_i64(), &[(2, 1, 7)]))
        .unwrap();
    registry.set_scan_chunk_rows(2);
    registry
}

/// The `(id, weight, val)` rows [`skeleton_fixture`] ingested under `ids`, sorted.
fn ingested(ids: &[u64]) -> Vec<(u64, i64, i64)> {
    let mut rows: Vec<_> = (0..5)
        .chain(100..105)
        .map(|id| (id, 1, id as i64))
        .chain([(2, 1, 7)])
        .filter(|(id, ..)| ids.contains(id))
        .collect();
    rows.sort_unstable();
    rows
}

/// Recomputes each skeleton key `k` as `(k, val = k)`, plus [`skeleton_fixture`]'s
/// `(2, 7)` for key 2 unless `short`, and records each call's key list.
#[derive(Default)]
struct Recompute {
    calls: Vec<Vec<u64>>,
    short: bool,
}

impl SkeletonHydrator for Recompute {
    fn hydrate_keys(&mut self, _: &RelationRegistry, _: u64, keys: PkKeys) -> Result<Batch, String> {
        let ks: Vec<u64> = keys.iter().map(|k| gnitz_wire::widen_pk_be(k) as u64).collect();
        let mut rows: Vec<_> = ks.iter().map(|&k| (k, 1, k as i64)).collect();
        if ks.contains(&2) && !self.short {
            rows.push((2, 1, 7));
        }
        rows.sort_unstable();
        self.calls.push(ks);
        Ok(make_batch_raw(&make_schema_u64_i64(), &rows).into_consolidated())
    }
}

/// Every read verb over a skeleton store answers exactly the ingested rows, and a
/// chunked walk hydrates once per chunk, only the skeleton keys that chunk met.
#[test]
fn a_skeleton_store_hydrates_chunk_by_chunk() {
    let r = skeleton_fixture();
    let layout = make_schema_u64_i64().layout_digest();
    let mut h = Recompute::default();
    let every: Vec<u64> = (0..5).chain(100..105).collect();
    let reads = [
        (ReadBound::None, every),
        (pk_set(&[2]), vec![2]),
        (pk_set(&[101]), vec![101]),
        (pk_set(&[1, 4, 102]), vec![1, 4, 102]),
    ];
    for (bound, ids) in reads {
        let got = r.scan_spec(TID, ReadSpec::all_rows(bound.clone()), layout, Some(&mut h));
        assert_eq!(rows_of(&got.unwrap()), ingested(&ids), "{bound:?}");
    }

    h.calls.clear();
    let range = ReadSpec::all_rows(pk_range(Cut::before(0), Cut::after(104)));
    let got = r.scan_spec(TID, range, layout, Some(&mut h)).unwrap();
    assert_eq!(rows_of(&got), ingested(&(0..105).collect::<Vec<_>>()));
    assert_eq!(h.calls, [vec![0, 1], vec![2, 3], vec![4]], "one hydration per chunk");
}

/// A bounded join view's PK is not unique, so a scan chunk can end inside a PK group
/// of live rows; the chunks still concatenate.
#[test]
fn a_scan_chunk_boundary_inside_a_pk_group() {
    let mut r = dehydrated_fixture(0..2, 100..102);
    r.ingest(TID, make_batch_raw(&make_schema_u64_i64(), &[(100, 1, 7)]))
        .unwrap();
    // Groups: skeleton 0, skeleton 1, (100, 7) | (100, 100), (101, 101).
    r.set_scan_chunk_rows(3);
    let layout = make_schema_u64_i64().layout_digest();
    let got = r
        .scan_spec(
            TID,
            ReadSpec::all_rows(ReadBound::None),
            layout,
            Some(&mut Recompute::default()),
        )
        .unwrap();
    assert_eq!(
        rows_of(&got),
        [(0, 1, 0), (1, 1, 1), (100, 1, 7), (100, 1, 100), (101, 1, 101)]
    );
}

/// A LIMIT cuts at the row, not at the hydrated group: key 2 hydrates to two rows,
/// and `LIMIT 1` ships one of them.
#[test]
fn a_limit_stops_at_the_row_inside_a_hydrated_group() {
    let r = skeleton_fixture();
    let spec = ReadSpec {
        bound: pk_set(&[2]),
        ..rows_spec(None, vec![], 1)
    };
    let layout = make_schema_u64_i64().layout_digest();
    let got = rows_of(&r.scan_spec(TID, spec, layout, Some(&mut Recompute::default())).unwrap());
    assert!(got.len() == 1 && ingested(&[2]).contains(&got[0]), "{got:?}");
}

/// A raw drain has no hydrator to hand a skeleton row to, so meeting one is a bug.
#[test]
#[should_panic(expected = "a raw drain met a skeleton row")]
fn a_raw_drain_over_a_skeleton_run_panics() {
    let r = dehydrated_fixture(0..5, 100..105);
    r.open_bound(TID, ReadBound::None).unwrap().0.drain_chunk(usize::MAX);
}

/// A hydration whose rows under a key do not sum to the skeleton row's weight is a
/// broken hydrator, caught where the rows come back.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "hydration weight mismatch")]
fn a_hydration_off_the_skeleton_weight_panics() {
    let r = skeleton_fixture();
    let mut h = Recompute { short: true, ..Recompute::default() };
    let layout = make_schema_u64_i64().layout_digest();
    let _ = r.scan_spec(TID, ReadSpec::all_rows(pk_set(&[2])), layout, Some(&mut h));
}

// ── Delta reads ──────────────────────────────────────────────────────

/// A fed view's delta read answers the rounds `(after_tick, cut_tick]` at each
/// round's own weights — both sides of a pair the output store folded away — in the
/// view's layout; `after_tick = 0` reads the view whole. Any other layout, or a view
/// with no feed, is refused.
#[test]
fn a_delta_read_answers_the_rounds_past_its_cursor() {
    let schema = make_schema_u64_i64();
    let kind = RelationKind::View(ViewProps::Fed { delta_bytes: 1 << 20 });
    let mut r = relation_fixture(kind, schema, &[], Batch::empty_with_schema(&schema));
    r.ingest_at(TID, make_batch_raw(&schema, &[(7, 1, 70)]), Some(4), false)
        .unwrap();
    r.ingest_at(TID, make_batch_raw(&schema, &[(7, -1, 70), (8, 1, 80)]), Some(5), false)
        .unwrap();
    let own = schema.layout_digest();
    let read = |after_tick| rows_of(&r.delta_read(TID, after_tick, 5, own).unwrap());
    assert_eq!(read(3), [(7, -1, 70), (7, 1, 70), (8, 1, 80)]);
    assert_eq!(read(4), [(7, -1, 70), (8, 1, 80)]);
    assert_eq!(read(5), []);
    assert_eq!(read(0), [(8, 1, 80)], "the output store folded the pair away");

    r.register(RelationSpec {
        id: TID + 1,
        kind: RelationKind::View(ViewProps::Plain),
        schema,
        placement: Placement::full_pk(&schema),
    })
    .unwrap();
    for (id, after_tick, layout) in [(TID, 0, own ^ 1), (TID, 3, own ^ 1), (TID + 1, 0, own)] {
        let Err(err) = r.delta_read(id, after_tick, 5, layout) else {
            panic!("relation {id} at {after_tick} must be refused");
        };
        assert!(matches!(err.status, WireStatus::Error), "{err:?}");
    }
}

// ── The drain ramp ───────────────────────────────────────────────────

/// A read whose window lies in its first `m` rows drains fewer than `2m + first`
/// of them, and never more than a flat `chunk_rows` drain would.
#[test]
fn drain_ramp_doubles_onto_the_chunk_grid() {
    let chunk = 1000;
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
    use std::hint::black_box;

    const N: u64 = 1_000_000;
    let shape = std::env::var("GNITZ_BENCH_SHAPE").unwrap_or_else(|_| "membership".to_string());
    let sel: u64 = std::env::var("GNITZ_BENCH_SEL").map_or(10, |s| s.parse().unwrap());
    let passes: usize = std::env::var("GNITZ_BENCH_PASSES").map_or(1, |s| s.parse().unwrap());
    let hi = (N * sel / 100) as i64;
    let r = view(&(0..N).map(|id| (id, 1, id as i64)).collect::<Vec<_>>());
    let walk = ReadBound::Range(KeyRange::new(
        PkColList::from_slice(&[1]),
        &[],
        Cut::before(img(0)),
        Cut::before(img(hi)),
    ));
    let (spec, want) = match shape.as_str() {
        "membership" => (ReadSpec::all_rows(walk), hi),
        "predicate" => (
            ReadSpec {
                predicate: between(1, 0, Some(hi)),
                ..ReadSpec::all_rows(ReadBound::None)
            },
            hi,
        ),
        "both" => (
            ReadSpec {
                predicate: between(1, hi / 2, None),
                ..ReadSpec::all_rows(walk)
            },
            hi - hi / 2,
        ),
        other => panic!("GNITZ_BENCH_SHAPE={other}: membership, predicate or both"),
    };
    for _ in 0..passes {
        let got = black_box(run(&r, spec.clone()).unwrap());
        assert_eq!(got.len() as i64, want);
    }
    println!("survivors {shape} sel {sel}% passes {passes}");
}
