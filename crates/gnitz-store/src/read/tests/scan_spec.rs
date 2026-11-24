use super::*;
use crate::relation::{RelationKind, RelationSpec, StoreConfig};
use crate::test_support::{
    RelationFixture, TID, batch_rows, between, chunked, col_range, cut, img, make_batch, make_schema_u64_i64, map_of,
    order_by, registered_fixture, relation_fixture, relation_fixture_with, rows_spec, spec, spilling,
};
use gnitz_expr::LogicalProgram;
use gnitz_foundation::rng::Rng;
use gnitz_wire::{AggDescriptor, GroupAggs, KeyRange, PkColList, ReadBound, ReadSink, SinkKind};
use gnitz_wire::{PkKeys, TypeCode, ViewBudget};
use gnitz_zset::algebra::Slot;
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor, pk_keys_of_images};

/// [`relation_fixture`] over a plain `(id U64 PK | val I64)` view holding the
/// `(id, weight, val)` `rows`, scanned `scan_chunk_rows` rows at a time. A view's
/// store runs no `enforce_unique_pk`, so a PK may repeat and a weight above 1 is
/// admitted verbatim.
fn view(rows: &[(u64, i64, i64)], scan_chunk_rows: usize) -> RelationFixture {
    let schema = make_schema_u64_i64();
    let batch = make_batch(&schema, rows);
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
    registered_fixture(config, Slot::SOLO, spec, schema, &[], rounds)
}

/// `spec` over `TID`, replying in the layout its sink produces.
fn run(r: &RelationRegistry, spec: ReadSpec) -> Result<Rc<Batch>, String> {
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
    ReadBound::PkSet(pk_keys_of_images(8, ids.iter().map(|&id| id as u128)))
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
            let batches = rounds.iter().map(|rows| make_batch(&schema, rows).into_consolidated());
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
        (walk(2), own, "DOUBLE"),
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
#[test]
fn keyed_reads_return_each_named_keys_whole_group() {
    let schema = make_schema_u64_i64();
    let mut rows: Vec<_> = (0..10).map(|id| (id, 1, id as i64 * 10)).collect();
    rows.extend([(7, 1, 71), (7, 1, 72), (7, 1, 73)]);
    let mut r = view(&rows, 2);
    // Retracted in a later ingest, (7, 70) is a ghost every read must drop.
    r.ingest(TID, make_batch(&schema, &[(7, -1, 70)])).unwrap();
    let reads = [
        (ReadBound::None, (0..10).collect()),
        (pk_set(&[2, 5, 7, 99]), vec![2, 5, 7]),
        (pk_set(&[7]), vec![7]),
        (col_range(0, 5, u64::MAX as u128), (5..10).collect()),
        (col_range(0, 5, 7), vec![5, 6, 7]),
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
}

// ── Index walks ──────────────────────────────────────────────────────

/// A LIMIT cuts inside an index range, with or without an ORDER BY, whether
/// the range is selective enough to walk the index or is traded for a scan
/// filtered by it.
#[test]
fn a_cut_keeps_the_first_rows_of_an_index_range() {
    let schema = make_schema_u64_i64();
    let rows: Vec<_> = (0..64).map(|id| (id, 1, id as i64 - 32)).collect();
    let r = relation_fixture(RelationKind::BaseTable, schema, &[&[1]], [make_batch(&schema, &rows)]);
    for (lo, hi, walks) in [(-2, 0, true), (-9, 12, false)] {
        let bound = col_range(1, img(lo), img(hi));
        let (cursor, _) = r.relation(TID).unwrap().open_read(bound.clone(), u64::MAX).unwrap();
        assert_eq!(cursor.walks_index(), walks, "premise: {bound:?}");
        let want: Vec<_> = rows
            .iter()
            .copied()
            .filter(|&(_, _, v)| (lo..=hi).contains(&v))
            .collect();
        let read = |cut| {
            let spec = ReadSpec {
                bound: bound.clone(),
                ..rows_spec(None, cut)
            };
            batch_rows(&run(&r, spec).unwrap())
        };
        assert_eq!(read(None), want, "{bound:?}");
        // Both reads yield in id order, so an unordered LIMIT keeps the first two ids;
        // `val` rises with the id, so a descending one keeps the last two.
        assert_eq!(read(cut(2, vec![])), want[..2], "{bound:?}");
        assert_eq!(read(cut(2, order_by(1, true))), want[want.len() - 2..], "{bound:?}");
    }
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
        make_batch(&schema, &rows)
    };
    let kind = RelationKind::View(ViewBudget::Bounded {
        capacity_bytes: std::num::NonZeroU64::MIN,
    });
    let mut r = relation_fixture_with(chunked(2), kind, schema, &[], [rows(0..5)]);
    r.checkpoint_ephemeral([], 1, |_| true).unwrap();
    r.settle(TID).unwrap();
    assert!(
        r.relation(TID).unwrap().table().has_skeleton_rows(),
        "premise: the capacity sweep must have dehydrated the flushed shard",
    );
    r.ingest(TID, rows(100..105)).unwrap();
    r.ingest(TID, make_batch(&schema, &[(2, 1, 7), (100, 1, 7)])).unwrap();
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
/// `(2, 7)` for key 2; records each call's key list.
#[derive(Default)]
struct Recompute {
    calls: Vec<Vec<u64>>,
}

impl Recompute {
    fn call(&mut self, keys: PkKeys) -> Result<Batch, String> {
        let ks: Vec<u64> = keys.iter().map(|k| gnitz_wire::widen_pk_be(k) as u64).collect();
        let mut rows: Vec<_> = ks.iter().map(|&k| (k, 1, k as i64)).collect();
        if ks.contains(&2) {
            rows.push((2, 1, 7));
        }
        rows.sort_unstable();
        self.calls.push(ks);
        Ok(make_batch(&make_schema_u64_i64(), &rows).into_consolidated())
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
    let range = ReadSpec::all_rows(col_range(0, 0, 104));
    let got = r.scan_spec(TID, range, layout, Some(&mut |k| h.call(k))).unwrap();
    assert_eq!(batch_rows(&got), ingested(0..105));
    assert_eq!(h.calls, [vec![0, 1], vec![2, 3], vec![4]], "one hydration per chunk");

    let streamed = run(&r, ReadSpec::all_rows(col_range(0, 100, 104))).unwrap();
    assert_eq!(batch_rows(&streamed), ingested(100..105));
    let Err(err) = run(&r, ReadSpec::all_rows(col_range(0, 0, 4))) else {
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
