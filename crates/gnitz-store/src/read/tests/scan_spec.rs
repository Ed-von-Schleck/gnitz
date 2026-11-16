use super::*;
use crate::read::SourceCursor;
use crate::relation::{RelationKind, RelationSpec, StoreConfig};
use crate::test_support::{
    RelationFixture, TID, batch_rows, between, chunked, col_range, cut, id_rows, img, make_batch_raw,
    make_schema_u64_i64, map_of, order_by, relation_fixture, relation_fixture_with, rows_spec, spec, spilling, zset_of,
};
use gnitz_expr::{ExprBuilder, LogicalInstr, LogicalProgram, Sink};
use gnitz_foundation::rng::Rng;
use gnitz_wire::{AggDescriptor, GroupAggs, KeyRange, PkColList, ReadBound, ReadSink, SinkKind};
use gnitz_wire::{PkKeys, TypeCode, ViewBudget};
use gnitz_zset::algebra::Slot;
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

/// [`relation_fixture`] over a plain `(id U64 PK | val I64)` view holding the
/// `(id, weight, val)` `rows`, scanned `scan_chunk_rows` rows at a time. A view's
/// store runs no `enforce_unique_pk`, so a PK may repeat and a weight above 1 is
/// admitted verbatim.
fn view(rows: &[(u64, i64, i64)], scan_chunk_rows: usize) -> RelationFixture {
    let schema = make_schema_u64_i64();
    let batch = make_batch_raw(&schema, rows);
    view_with(chunked(scan_chunk_rows), schema, [batch])
}

/// A plain view over `schema` under `config`, holding `rounds`: a view whose
/// PKs repeat.
fn view_with(
    config: StoreConfig,
    schema: SchemaDescriptor,
    rounds: impl IntoIterator<Item = Batch>,
) -> RelationFixture {
    let spec = RelationSpec {
        pk_repeats: true,
        ..spec(TID, RelationKind::View(ViewBudget::Plain), schema)
    };
    relation_fixture_with(config, Slot::SOLO, spec, schema, &[], rounds)
}

/// `spec` over `TID`, replying in the layout its sink produces.
fn run(r: &RelationRegistry, spec: ReadSpec) -> Result<ReadRows, String> {
    let layout = layout_of(&r.relation(TID).unwrap().schema(), &spec.sink);
    r.scan_spec(TID, spec, layout, None)
}

/// The layout `sink` replies in over `schema`.
pub(super) fn layout_of(schema: &SchemaDescriptor, sink: &ReadSink) -> u64 {
    SinkPlan::from_wire(schema, sink, usize::MAX)
        .unwrap()
        .output_schema()
        .layout_digest()
}

/// A `pk IN (…)` bound over a U64 PK.
fn pk_set(ids: &[u64]) -> ReadBound {
    ReadBound::PkSet(PkKeys::from_images(8, ids.iter().map(|&id| id as u128)))
}

// ── Sinks ────────────────────────────────────────────────────────────

/// A cut keeps the first rows whose summed weight covers its window, the boundary
/// row whole — only the client, which sees every worker's rows, may clip it: the
/// smallest by `(val, id)` under an ORDER BY, else the first the walk yields, in id
/// order. A window past `i64::MAX` saturates rather than wrapping to one the first
/// chunk covers, and under a predicate a survivor may be a range of its own.
#[test]
fn a_cut_keeps_the_first_rows_covering_its_window() {
    let mut rng = Rng::new(0x9E3779B97F4A7C15);
    let schema = make_schema_u64_i64();
    for round in 0..400 {
        let n = 1 + rng.gen_range(200);
        let shape = rng.gen_range(6);
        let heavy = rng.gen_range(8) == 0;
        let rows: Vec<(u64, i64, i64)> = (0..n)
            .map(|id| {
                let val = match shape {
                    0 => id as i64,
                    1 => -(id as i64),
                    2 => rng.gen_range(4) as i64,
                    3 => rng.gen_range(1000) as i64 - 500,
                    4 => id as i64 % 2,
                    _ => 7,
                };
                let w = if heavy { 100_000 } else { 1 + rng.gen_range(3) as i64 };
                (id, w, val)
            })
            .collect();
        let r = view(&rows, 1 + rng.gen_range(16) as usize);
        let desc = [None, Some(false), Some(true)][rng.gen_range(3) as usize];
        let k = match rng.gen_range(8) {
            0 => 65_537,
            1 => 1 << 63,
            2 => u64::MAX,
            _ => 1 + rng.gen_range(40),
        };
        let filtered = rng.gen_range(3) == 0;

        let mut kept: Vec<_> = rows
            .into_iter()
            .filter(|&(_, _, val)| !filtered || (-2..1).contains(&val))
            .collect();
        kept.sort_by_key(|&(id, _, val)| match desc {
            None => (0, id),
            Some(desc) => (if desc { -val } else { val }, id),
        });
        let mut covered = 0i128;
        let mut want: Vec<_> = kept
            .into_iter()
            .take_while(|&(_, w, _)| {
                let take = covered < k as i128;
                covered += w as i128;
                take
            })
            .collect();
        want.sort_unstable();
        let cut = cut(k, desc.map_or(vec![], |desc| order_by(1, desc)));
        for map in [None, map_of(LogicalProgram::copy_cols(&[1]), &schema)] {
            let spec = ReadSpec {
                predicate: if filtered { between(1, -2, 1) } else { vec![] },
                ..rows_spec(map, cut.clone())
            };
            assert_eq!(
                batch_rows(&run(&r, spec).unwrap()),
                want,
                "round {round} shape {shape} desc {desc:?} k {k} filtered {filtered}"
            );
        }
    }
}

/// A filtered read over a store of several runs, in memory and spilled to shards,
/// answers the predicate's survivors of the runs' consolidated sum: a row the
/// predicate keeps at its net weight, one whose weights cancel across runs not at all.
#[test]
fn a_filtered_read_over_many_runs_keeps_the_survivors_net_weights() {
    let schema = make_schema_u64_i64();
    let mut rng = Rng::new(0xC0FFEE);
    for round in 0..60 {
        let rounds: Vec<Vec<(u64, i64, i64)>> = (0..1 + rng.gen_range(6))
            .map(|_| {
                let mut rows: Vec<_> = (0..rng.gen_range(80))
                    .map(|_| {
                        let w = [1, 1, 2, -1][rng.gen_range(4) as usize];
                        (rng.gen_range(40), w, rng.gen_range(6) as i64)
                    })
                    .collect();
                rows.sort_unstable_by_key(|&(id, _, val)| (id, val));
                rows
            })
            .collect();
        let mut net = std::collections::BTreeMap::new();
        for &(id, w, val) in rounds.iter().flatten() {
            *net.entry((id, val)).or_insert(0i64) += w;
        }
        let (lo, hi) = (rng.gen_range(4) as i64, 2 + rng.gen_range(5) as i64);
        let mut want: Vec<_> = (net.into_iter())
            .filter(|&((_, val), w)| w != 0 && (lo..hi).contains(&val))
            .map(|((id, val), w)| (id, w, val))
            .collect();
        want.sort_unstable();
        for config in [StoreConfig::default(), spilling()] {
            let batches = rounds
                .iter()
                .map(|rows| make_batch_raw(&schema, rows).into_consolidated());
            let config = StoreConfig {
                scan_chunk_rows: 1 + rng.gen_range(24) as usize,
                ..config
            };
            let r = view_with(config, schema, batches);
            let spec = ReadSpec {
                predicate: between(1, lo, hi),
                ..rows_spec(None, None)
            };
            assert_eq!(batch_rows(&run(&r, spec).unwrap()), want, "round {round} {lo}..{hi}");
        }
    }
}

/// The fold sink folds exactly the predicate's survivors of every chunk, mapped or
/// not: one partial per group, its count the survivors' summed weight.
#[test]
fn a_fold_counts_the_survivors_of_every_chunk() {
    let schema = make_schema_u64_i64();
    let rows: Vec<_> = (0..20).map(|id| (id, 2, id as i64 % 3)).collect();
    let r = view(&rows, 4);
    let agg = GroupAggs {
        group_cols: vec![1],
        aggs: vec![AggDescriptor::COUNT_STAR],
    };
    for map in [None, map_of(LogicalProgram::copy_cols(&[1]), &schema)] {
        let spec = ReadSpec {
            bound: ReadBound::None,
            predicate: between(1, 0, 2),
            sink: ReadSink { map, kind: SinkKind::Fold(agg.clone()) },
        };
        let got = run(&r, spec).unwrap();
        let mut groups: Vec<(i64, i64, i64)> = (0..got.len())
            .map(|i| {
                (
                    gnitz_wire::decode_opk_i64(got.get_pk_bytes(i), gnitz_wire::FixedInt::I64),
                    got.get_weight(i),
                    crate::test_support::payload0_i64(&*got, i),
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
/// walk over a column with no key order, a key list at a foreign stride — and every
/// sink replies only in the layout it produces.
#[test]
fn a_malformed_request_is_refused() {
    let col = |tc| SchemaColumn::new(tc, false);
    let schema = SchemaDescriptor::new(&[col(TypeCode::U64), col(TypeCode::I64), col(TypeCode::F64)], &[0]);
    let kind = RelationKind::View(ViewBudget::Plain);
    let r = relation_fixture(kind, schema, &[], []);
    let walk = |c: u32| ReadSpec::all_rows(col_range(c, 0, 9));
    let own = schema.layout_digest();
    let two_slots = SchemaDescriptor::new(&[col(TypeCode::U64), col(TypeCode::I64), col(TypeCode::I64)], &[0]);
    let fold = ReadSpec {
        sink: ReadSink {
            map: None,
            kind: SinkKind::Fold(GroupAggs {
                group_cols: vec![1],
                aggs: vec![AggDescriptor::COUNT_STAR],
            }),
        },
        ..rows_spec(None, None)
    };
    let other_layout = "reply schema does not match";
    let cases = [
        (rows_spec(None, cut(1, order_by(99, false))), own, "order"),
        (walk(64), own, "column"),
        (walk(2), own, "F64"),
        (
            ReadSpec::all_rows(ReadBound::PkSet(PkKeys::from_sorted(16, vec![0; 16]))),
            own,
            "stride",
        ),
        (
            rows_spec(map_of(LogicalProgram::copy_cols(&[1]), &two_slots), None),
            two_slots.layout_digest(),
            "map",
        ),
        (rows_spec(None, None), own ^ 1, other_layout),
        (rows_spec(None, cut(1, vec![])), own ^ 1, other_layout),
        (rows_spec(None, cut(1, order_by(1, false))), own ^ 1, other_layout),
        (fold.clone(), layout_of(&schema, &fold.sink) ^ 1, other_layout),
    ];
    for (spec, layout, why) in cases {
        let Err(err) = r.scan_spec(TID, spec.clone(), layout, None) else {
            panic!("{spec:?} must be refused");
        };
        assert!(err.contains(why), "{spec:?}: {err}");
    }
}

/// Every keyed read returns each named key's whole PK group at its folded weights —
/// a view's PK is not unique, and a group larger than the chunk budget crosses it
/// in one piece — and a key the store does not hold misses, whether it drains its
/// source once, unfiltered, or in chunks under a predicate that keeps every row.
/// A read of the whole relation is the store's cached snapshot itself.
#[test]
fn keyed_reads_return_each_named_keys_whole_group() {
    let schema = make_schema_u64_i64();
    let mut rows: Vec<_> = (0..10).map(|id| (id, 1, id as i64 * 10)).collect();
    rows.extend([(7, 1, 71), (7, 1, 72), (7, 1, 73)]);
    let mut r = view(&rows, 2);
    // Retracted in a later ingest, (7, 70) is a ghost every read must drop.
    r.ingest(TID, make_batch_raw(&schema, &[(7, -1, 70)])).unwrap();
    let pk_range = |start, end| col_range(0, start, end);
    let reads = [
        (ReadBound::None, (0..10).collect()),
        (pk_set(&[2, 5, 7, 99]), vec![2, 5, 7]),
        (pk_set(&[7]), vec![7]),
        (pk_range(5, u64::MAX as u128), (5..10).collect()),
        (pk_range(5, 7), vec![5, 6, 7]),
        (
            ReadBound::Range(KeyRange::point(PkColList::from_slice(&[0]), &[7])),
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
        for predicate in [vec![], between(1, i64::MIN, i64::MAX)] {
            let spec = ReadSpec {
                predicate,
                ..ReadSpec::all_rows(bound.clone())
            };
            let got = run(&r, spec).unwrap();
            assert_eq!(batch_rows(&got), want, "{bound:?}");
        }
    }
    let ReadRows::Shared(whole) = run(&r, rows_spec(None, None)).unwrap() else {
        panic!("a whole read shares the cached snapshot");
    };
    assert!(std::rc::Rc::ptr_eq(&whole, &r.relation(TID).unwrap().full_scan()));
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
    let rows = id_rows(&schema, (0..WALK_ROWS).map(|id| (id, 1)), |bb, id| {
        let (val, big) = walk_row(id);
        bb.put_opt_int(val.map(|v| v as u128));
        bb.put_opt_int(big);
    });
    let r = relation_fixture(RelationKind::BaseTable, schema, &[&[1], &[2]], [rows]);

    let vals = |keep: &dyn Fn(i64) -> bool| -> Vec<u64> {
        (0..WALK_ROWS).filter(|&id| walk_row(id).0.is_some_and(keep)).collect()
    };
    let bigs = |lo: u128, hi: u128| -> Vec<u64> {
        (0..WALK_ROWS)
            .filter(|&id| walk_row(id).1.is_some_and(|b| b >= lo && b < hi))
            .collect()
    };
    let (all_lo, all_hi) = (0, gnitz_wire::image_mask(8));
    // (column, first, last, walks the index, ids).
    let walks = [
        (1, img(-9), img(12), false, vals(&|v| v > -10 && v <= 12)),
        (1, img(-2), img(1), true, vec![31, 32, 33]),
        (1, img(-31), img(-31), true, vec![1]),
        (1, all_lo, all_hi, false, vals(&|_| true)),
        (2, 3 << 70, (40 << 70) - 1, false, bigs(3 << 70, 40 << 70)),
        (2, 3 << 70, (6 << 70) - 1, true, vec![3, 4, 5]),
    ];
    // A walk reply's ids, sorted, each at weight 1.
    let ids = |spec: ReadSpec| -> Vec<u64> {
        let b = run(&r, spec).unwrap();
        assert!((0..b.len()).all(|i| b.get_weight(i) == 1));
        let mut ids: Vec<u64> = (0..b.len())
            .map(|i| gnitz_wire::widen_pk_be(b.get_pk_bytes(i)) as u64)
            .collect();
        ids.sort_unstable();
        ids
    };
    for (col, start, end, walks, want) in walks {
        let bound = col_range(col, start, end);
        let (cursor, _) = r.relation(TID).unwrap().open_read(bound.clone(), u64::MAX).unwrap();
        assert_eq!(
            matches!(cursor, SourceCursor::Keyed { walk: Some(_), .. }),
            walks,
            "{bound:?}"
        );
        let spec = |cut| ReadSpec {
            bound: bound.clone(),
            ..rows_spec(None, cut)
        };
        assert_eq!(ids(spec(None)), want, "{bound:?}");
        // Both walks yield in id order, so an unordered LIMIT keeps the first two ids;
        // both columns rise with the id, so a descending one keeps the last two.
        assert_eq!(ids(spec(cut(2, vec![]))), want[..want.len().min(2)], "{bound:?}");
        let top = ids(spec(cut(2, order_by(col, true))));
        assert_eq!(top, want[want.len().saturating_sub(2)..], "{bound:?}");
    }
}

// ── Maps onto the keeper ─────────────────────────────────────────────

/// `ids` at weights cycling through 1, 2 and 3.
fn weighted(ids: std::ops::Range<u64>) -> impl Iterator<Item = (u64, i64)> {
    ids.map(|id| (id, 1 + id as i64 % 3))
}

/// A view over `cols` (PK = column 0) of rows `0..n` at [`weighted`] weights,
/// `put_row` writing each row's payload, scanned `chunk_rows` rows at a time so the
/// keeper is grown and reused across chunks — the only way a stale-byte leak in a
/// map's tail writes would show.
fn map_fixture(
    cols: &[SchemaColumn],
    n: u64,
    chunk_rows: usize,
    put_row: impl FnMut(&mut BatchBuilder, u64),
) -> RelationFixture {
    let schema = SchemaDescriptor::new(cols, &[0]);
    let kind = RelationKind::View(ViewBudget::Plain);
    let rows = id_rows(&schema, weighted(0..n), put_row);
    relation_fixture_with(
        chunked(chunk_rows),
        Slot::SOLO,
        spec(TID, kind, schema),
        schema,
        &[],
        [rows],
    )
}

/// `spec`'s reply over `r` is `want`, weight for weight.
fn assert_reply(r: &RelationRegistry, spec: ReadSpec, want: Batch) {
    let got = run(r, spec).unwrap();
    assert_eq!(zset_of(&got, want.schema()), zset_of(&want, want.schema()));
}

/// A copy of every payload column of a 65-column table (1 PK + 64 payload) — the
/// widest schema the row-major null word admits, so the output-coverage mask is
/// exercised at its top bit.
#[test]
fn a_copy_of_64_payload_columns_covers_every_slot() {
    const P: u64 = 64;
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend((0..P).map(|_| SchemaColumn::new(TypeCode::I64, false)));
    let put_row = |bb: &mut BatchBuilder, id: u64| (0..P).for_each(|k| bb.put_u64(id * 100 + k));
    let r = map_fixture(&cols, 40, 16, put_row);
    let reply = SchemaDescriptor::new(&cols, &[0]);
    let copies: Vec<u32> = (1..=P as u32).collect();
    let spec = rows_spec(map_of(LogicalProgram::copy_cols(&copies), &reply), None);
    assert_reply(&r, spec, id_rows(&reply, weighted(0..40), put_row));
}

/// A map sourcing only the PK has an empty null permutation, and must still write
/// every null word: an unwritten word would surface the recycled keeper's bytes.
#[test]
fn a_pk_sourced_map_zeroes_every_null_word() {
    let cols = [
        SchemaColumn::new(TypeCode::U64, false),
        SchemaColumn::new(TypeCode::I64, false),
    ];
    let r = map_fixture(&cols, 300, 32, |bb, id| bb.put_u64(id * 7));
    let reply = crate::test_support::u64_pk_schema(&[SchemaColumn::new(TypeCode::U64, false)]);
    let spec = rows_spec(map_of(LogicalProgram::copy_cols(&[0]), &reply), None);
    let got = run(&r, spec.clone()).unwrap();
    assert!((0..got.len()).all(|row| got.get_null_word(row) == 0));
    assert_reply(&r, spec, id_rows(&reply, weighted(0..300), |bb, id| bb.put_u64(id)));
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
    let nv = |id: u64| (!id.is_multiple_of(5)).then_some(id as u128 * 3);
    let cols = [
        SchemaColumn::new(TypeCode::U64, false),
        SchemaColumn::new(TypeCode::I64, true),
        SchemaColumn::new(TypeCode::String, false),
    ];
    let r = map_fixture(&cols, N, 24, |bb, id| {
        bb.put_opt_int(nv(id));
        bb.put_string(&string(id));
    });
    // id U64 PK | s STRING | id U64 (from the PK) | nv I64 NULL.
    let reply = SchemaDescriptor::new(&[cols[0], cols[2], cols[0], cols[1]], &[0]);
    let spec = rows_spec(map_of(LogicalProgram::copy_cols(&[2, 0, 1]), &reply), None);
    let want = id_rows(&reply, weighted(0..N), |bb, id| {
        bb.put_string(&string(id));
        bb.put_u64(id);
        bb.put_opt_int(nv(id));
    });
    assert_reply(&r, spec, want);
}

/// A computed column (`nv * 2`) beside a copied one, under a predicate: the compute
/// kernel writes the survivors onto the keeper's tail at a non-zero base, and the
/// nullable source runs the emit's null merge over the already-permuted word.
#[test]
fn a_computed_map_writes_at_the_keepers_tail() {
    let nv = |id: u64| (!id.is_multiple_of(4)).then_some(id as u128);
    let cols = [
        SchemaColumn::new(TypeCode::U64, false),
        SchemaColumn::new(TypeCode::I64, true),
        SchemaColumn::new(TypeCode::I64, false),
    ];
    let r = map_fixture(&cols, 300, 32, |bb, id| {
        bb.put_opt_int(nv(id));
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
        predicate: between(2, 0, 1000),
        ..rows_spec(map_of(program, &reply), None)
    };
    let want = id_rows(&reply, weighted(0..100), |bb, id| {
        bb.put_opt_int(nv(id).map(|v| v * 2));
        bb.put_u64(id * 10);
    });
    assert_reply(&r, spec, want);
}

// ── Skeleton rows ────────────────────────────────────────────────────

/// A bounded view whose sweep has dehydrated every row of keys `0..5`, plus the rows
/// of `100..105` still in the RAM tier, each `(id, 1, id)`; then the live rows
/// `(2, val = 7)` under skeleton key 2 and `(100, val = 7)`, read two merge groups a
/// chunk, so a chunk ends inside the live group 100.
fn skeleton_fixture() -> RelationFixture {
    let schema = make_schema_u64_i64();
    let rows = |ids: std::ops::Range<u64>| {
        let rows: Vec<_> = ids.map(|id| (id, 1, id as i64)).collect();
        make_batch_raw(&schema, &rows)
    };
    let kind = RelationKind::View(ViewBudget::Bounded {
        capacity_bytes: std::num::NonZeroU64::MIN,
    });
    let mut r = relation_fixture_with(
        chunked(2),
        Slot::SOLO,
        spec(TID, kind, schema),
        schema,
        &[],
        [rows(0..5)],
    );
    r.checkpoint_ephemeral([], 1, |_| true).unwrap();
    r.settle(TID).unwrap();
    assert!(
        r.relation(TID).unwrap().table().has_skeleton_rows(),
        "premise: the capacity sweep must have dehydrated the flushed shard",
    );
    r.ingest(TID, rows(100..105)).unwrap();
    r.ingest(TID, make_batch_raw(&schema, &[(2, 1, 7), (100, 1, 7)]))
        .unwrap();
    r
}

/// The `(id, weight, val)` rows [`skeleton_fixture`] ingested under `ids`, sorted.
fn ingested(ids: impl IntoIterator<Item = u64>) -> Vec<(u64, i64, i64)> {
    let ids: Vec<u64> = ids.into_iter().collect();
    let mut rows: Vec<_> = (0..5)
        .chain(100..105)
        .map(|id| (id, 1, id as i64))
        .chain([(2, 1, 7), (100, 1, 7)])
        .filter(|(id, ..)| ids.contains(id))
        .collect();
    rows.sort_unstable();
    rows
}

/// Recomputes each skeleton key `k` as `(k, val = k)`, plus [`skeleton_fixture`]'s
/// `(2, 7)` for key 2, then applies `edit`; records each call's key list.
struct Recompute {
    calls: Vec<Vec<u64>>,
    edit: fn(&mut Vec<(u64, i64, i64)>),
}

impl Default for Recompute {
    fn default() -> Self {
        Recompute { calls: Vec::new(), edit: |_| {} }
    }
}

impl Recompute {
    fn call(&mut self, keys: PkKeys) -> Result<Batch, String> {
        let ks: Vec<u64> = keys.iter().map(|k| gnitz_wire::widen_pk_be(k) as u64).collect();
        let mut rows: Vec<_> = ks.iter().map(|&k| (k, 1, k as i64)).collect();
        if ks.contains(&2) {
            rows.push((2, 1, 7));
        }
        (self.edit)(&mut rows);
        rows.sort_unstable();
        self.calls.push(ks);
        Ok(make_batch_raw(&make_schema_u64_i64(), &rows).into_consolidated())
    }
}

/// Every read over a skeleton store answers exactly the ingested rows, and a chunked
/// walk hydrates once per chunk, only the skeleton keys that chunk met. Whether a
/// read hydrates is the **opened cursor's** question, not the store's: a range that
/// no skeleton shard's key band overlaps streams with no hydrator, and one that meets
/// a skeleton row has nothing to recompute it with. A LIMIT cuts at the row, not at
/// the hydrated group.
#[test]
fn every_read_over_a_skeleton_store_answers_the_ingested_rows() {
    let r = skeleton_fixture();
    let layout = make_schema_u64_i64().layout_digest();
    let pk_range = |first, last| col_range(0, first, last);
    let mut h = Recompute::default();
    let reads = [
        (ReadBound::None, (0..5).chain(100..105).collect()),
        (pk_set(&[2]), vec![2]),
        (pk_set(&[101]), vec![101]),
        (pk_set(&[1, 4, 102]), vec![1, 4, 102]),
    ];
    for (bound, ids) in reads {
        let got = r.scan_spec(TID, ReadSpec::all_rows(bound.clone()), layout, Some(&mut |k| h.call(k)));
        assert_eq!(batch_rows(&got.unwrap()), ingested(ids), "{bound:?}");
    }

    let filtered = ReadSpec {
        predicate: between(1, 0, 5),
        ..rows_spec(None, None)
    };
    let got = r.scan_spec(TID, filtered, layout, Some(&mut |k| h.call(k))).unwrap();
    let want: Vec<_> = ingested(0..5).into_iter().filter(|&(_, _, val)| val < 5).collect();
    assert_eq!(batch_rows(&got), want, "a predicate over hydrated and live rows");

    h.calls.clear();
    let range = ReadSpec::all_rows(pk_range(0, 104));
    let got = r.scan_spec(TID, range, layout, Some(&mut |k| h.call(k))).unwrap();
    assert_eq!(batch_rows(&got), ingested(0..105));
    assert_eq!(h.calls, [vec![0, 1], vec![2, 3], vec![4]], "one hydration per chunk");

    let streamed = run(&r, ReadSpec::all_rows(pk_range(100, 104))).unwrap();
    assert_eq!(batch_rows(&streamed), ingested(100..105));
    let Err(err) = run(&r, ReadSpec::all_rows(pk_range(0, 4))) else {
        panic!("a range over the dehydrated band must reach the hydrator");
    };
    assert!(err.contains("skeleton rows"), "{err}");

    let first = ReadSpec {
        bound: pk_set(&[2]),
        ..rows_spec(None, cut(1, vec![]))
    };
    let got = r.scan_spec(TID, first, layout, Some(&mut |k| h.call(k))).unwrap();
    assert_eq!(batch_rows(&got), [(2, 1, 2)]);
}

/// A raw drain has no hydrator to hand a skeleton row to, so meeting one is a bug.
#[test]
#[should_panic(expected = "a raw drain met a skeleton row")]
fn a_raw_drain_over_a_skeleton_run_panics() {
    let r = skeleton_fixture();
    r.relation(TID)
        .unwrap()
        .open_sealed(ReadBound::None, u64::MAX)
        .unwrap()
        .drain_chunk(usize::MAX);
}

/// A hydration that misses a named key, names a key no skeleton row did, misses the
/// last key, or whose rows under a key do not sum to the skeleton row's weight is a
/// broken hydrator, caught where the rows come back.
#[cfg(debug_assertions)]
#[test]
fn a_hydration_off_the_skeleton_rows_panics() {
    type Edit = fn(&mut Vec<(u64, i64, i64)>);
    let r = skeleton_fixture();
    let layout = make_schema_u64_i64().layout_digest();
    let faults: [(&[u64], Edit); 4] = [
        (&[1, 4], |rows| rows.retain(|r| r.0 != 1)),
        (&[1, 4], |rows| rows.push((3, 1, 3))),
        (&[1, 4], |rows| rows.retain(|r| r.0 != 4)),
        (&[2], |rows| rows.retain(|r| *r != (2, 1, 7))),
    ];
    for (i, (keys, edit)) in faults.into_iter().enumerate() {
        let mut h = Recompute { edit, ..Recompute::default() };
        let read = || drop(r.scan_spec(TID, ReadSpec::all_rows(pk_set(keys)), layout, Some(&mut |k| h.call(k))));
        let panic =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(read)).expect_err("a broken hydration panics");
        let msg = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or_default();
        assert!(msg.contains("hydration off its skeleton rows"), "fault {i}: {msg}");
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

/// A stream holds no rows: a read of one is refused whatever its spec.
#[test]
fn a_stream_is_refused() {
    let schema = make_schema_u64_i64();
    let r = relation_fixture(RelationKind::Stream, schema, &[], []);
    let Err(err) = r.scan_spec(TID, rows_spec(None, None), schema.layout_digest(), None) else {
        panic!("a stream read must be refused");
    };
    assert!(err.contains("is a stream"), "{err}");
}
