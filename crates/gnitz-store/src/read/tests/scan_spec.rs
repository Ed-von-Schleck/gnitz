use super::*;
use crate::relation::{OnRegister, RelationKind, RelationSpec, StoreConfig, ViewBudgets};
use crate::schema::{type_code, SchemaColumn};
use crate::storage::{BatchBuilder, Slot, StoreError};
use gnitz_wire::{AggDescriptor, AggReadSpec, IndexWalk, ReadSink};

// ── The executor — `scan_spec` over a registry built in-crate ────────
//
// `hydrator: None`, as `gnitz-mirror` calls it: nothing here holds a skeleton
// row. These drive the sink reduction, which needs no expression compiler; the
// projection and predicate paths are covered where one exists.

/// The relation every executor test below reads.
const TID: i64 = gnitz_wire::FIRST_USER_TABLE_ID as i64;

/// `(id U64 PK | val I64)`, `val == id`.
fn id_val_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
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
    let mut registry = RelationRegistry::new(Slot::SOLO, StoreConfig::default());
    registry
        .register(
            RelationSpec {
                id: TID,
                kind: RelationKind::View,
                schema,
                directory: crate::test_support::scratch_dir("read", name),
                budgets: ViewBudgets { capacity_bytes: None, delta_bytes: None },
            },
            OnRegister::Live,
        )
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
    registry.scan_spec(TID, spec.clone(), &schema, None)
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

/// A whole-relation spec, ordered or not, is the store's cached snapshot, and still
/// refuses a forged order column or a foreign reply layout.
#[test]
fn a_whole_relation_spec_is_served_off_the_cached_snapshot() {
    let mut r = rows_fixture("whole_relation", 4, 1);
    let snapshot = r.scan(TID, None).unwrap();
    for spec in [rows_spec(Vec::new(), 0), rows_spec(val_desc(), 0)] {
        assert!(Rc::ptr_eq(&run(&mut r, &spec).unwrap(), &snapshot));
    }
    let forged = vec![OrderKey { col: 99, desc: false, nulls_first: false }];
    let Err(err) = run(&mut r, &rows_spec(forged, 0)) else {
        panic!("a forged order column must be refused on the snapshot path");
    };
    assert!(err.to_string().contains("order key column 99"), "{err}");
    let mismatched = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::F64, 0),
        ],
        &[0],
    );
    let Err(err) = r.scan_spec(TID, rows_spec(Vec::new(), 0), &mismatched, None) else {
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
    let Err(err) = run(&mut r, &rows_spec(order, 0)) else {
        panic!("an out-of-range order key must be rejected");
    };
    assert!(err.to_string().contains("order key column 99"), "{err}");
}

/// A packed column word naming a column the table has not got is a corrupt
/// frame, and is rejected on both walk kinds. An `Optional` walk is the
/// one a residual predicate could cover for, so it is the one where degrading to
/// a full scan would answer the corrupt frame instead of refusing it.
#[test]
fn an_out_of_range_index_column_is_rejected_even_when_inexact() {
    let mut r = rows_fixture("index_oob", 4, 1);
    let spec = ReadSpec {
        bound: ReadBound::IndexRange {
            bound: gnitz_wire::IndexBound {
                idx_cols: gnitz_wire::PkColList::from_slice(&[99]),
                desc: RangeDescriptor::new(&[], Cut::Before(0), Cut::After(u64::MAX as u128)),
            },
            walk: IndexWalk::Optional,
        },
        ..rows_spec(Vec::new(), 0)
    };
    let Err(err) = run(&mut r, &spec) else {
        panic!("an out-of-range index column must be rejected");
    };
    assert!(err.to_string().contains("invalid column list"), "{err}");
}

/// An `Optional` walk may trade a declined index for a full scan, but a malformed
/// range is not a decline: it names no range column, so it is refused.
#[test]
fn a_malformed_range_on_an_optional_index_walk_is_rejected() {
    let mut r = rows_fixture("index_malformed", 4, 1);
    r.add_index(TID, TID + 1, &[1], false).unwrap();
    let spec = ReadSpec {
        bound: ReadBound::IndexRange {
            bound: gnitz_wire::IndexBound {
                idx_cols: gnitz_wire::PkColList::from_slice(&[1]),
                desc: RangeDescriptor::new(&[3], Cut::Before(0), Cut::After(9)),
            },
            walk: IndexWalk::Optional,
        },
        ..rows_spec(Vec::new(), 0)
    };
    let Err(err) = run(&mut r, &spec) else {
        panic!("a range with no range column must be refused, not scanned");
    };
    assert!(err.to_string().contains("has no range column"), "{err}");
}

/// A bounded view whose sweep has dehydrated everything on disk, plus fresh rows
/// still in the RAM tier. `hydrator: None` is the point: a walk that meets a
/// skeleton row here can only fail, so a read that succeeds proves it met none.
fn dehydrated_fixture(name: &str, on_disk: std::ops::Range<u64>, in_ram: std::ops::Range<u64>) -> RelationRegistry {
    let schema = id_val_schema();
    let mut registry = RelationRegistry::new(Slot::SOLO, StoreConfig::default());
    registry
        .register(
            RelationSpec {
                id: TID,
                kind: RelationKind::View,
                schema,
                directory: crate::test_support::scratch_dir("read", name),
                budgets: ViewBudgets {
                    capacity_bytes: Some(1),
                    delta_bytes: None,
                },
            },
            OnRegister::Live,
        )
        .unwrap();
    ingest(&mut registry, on_disk);
    registry.checkpoint_ephemeral(1, []).unwrap();
    assert!(
        registry.relation_or_err(TID).unwrap().store().has_skeleton_rows(),
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
        bound: ReadBound::PkRange(RangeDescriptor::new(
            &[],
            Cut::Before(lo as u128),
            Cut::After(hi as u128),
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
            let val = i64::from_le_bytes(b.get_col_ptr(i, 0, 8).try_into().unwrap());
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
    fn hydrate_keys(&mut self, _: &RelationRegistry, _: i64, keys: Vec<u8>) -> Result<Batch, StoreError> {
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

    assert_eq!(rows_of(&r.scan(TID, Some(&mut h)).unwrap()), ingested(|_| true));
    for k in [2u64, 101] {
        let hit = r.seek(TID, &k.to_be_bytes(), Some(&mut h)).unwrap();
        assert_eq!(rows_of(&hit.expect("a present key")), ingested(|i| i == k));
    }

    h.calls.clear();
    let got = r.scan_spec(TID, pk_range(0, 104), &schema, Some(&mut h)).unwrap();
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
    let got = r.scan_spec(TID, spec, &schema, Some(&mut h)).unwrap();
    assert_eq!(rows_of(&got), ingested(|i| [1, 4, 102].contains(&i)));
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
    let got = r.scan_spec(TID, spec, &id_val_schema(), Some(&mut h)).unwrap();
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
    let partial = |last: Option<u8>| {
        let mut cols = vec![
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
        ];
        cols.extend(last.map(|tc| SchemaColumn::new(tc, 0)));
        SchemaDescriptor::new(&cols, &[0])
    };
    // The derived partial itself is accepted, which is what makes the two
    // refusals below about the layout rather than about the spec.
    assert!(r
        .scan_spec(TID, spec.clone(), &partial(Some(type_code::I64)), None)
        .is_ok());
    for bad in [partial(None), partial(Some(type_code::F64))] {
        let Err(err) = r.scan_spec(TID, spec.clone(), &bad, None) else {
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
