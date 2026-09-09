use super::*;
use crate::relation::{OnRegister, RelationKind, RelationSpec, StoreConfig, ViewBudgets};
use crate::schema::{type_code, SchemaColumn};
use crate::storage::{BatchBuilder, Slot, StoreError};
use gnitz_wire::Cut;

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
        sink: ReadSink::Rows { projection: Vec::new(), order, limit_k },
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

fn run(registry: &mut RelationRegistry, spec: &ReadSpec) -> Result<Batch, StoreError> {
    let schema = id_val_schema();
    registry.scan_spec(TID, spec, &schema, 0, None)
}

/// The reply is trimmed to the window, not to the mid-scan residency cap: five
/// weight-1 rows against `LIMIT 3` sit in the band the mid-scan trim skips, so
/// only the terminal trim can shed them, and it must.
#[test]
fn top_k_trims_to_the_window_before_replying() {
    let mut r = rows_fixture("topk_band", 5, 1);
    let got = run(&mut r, &rows_spec(val_desc(), 3)).unwrap();
    assert_eq!(ids(&got), vec![(4u128, 1), (3, 1), (2, 1)], "the three largest, DESC");
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
/// frame, and is rejected on both walk kinds. An `exact == false` walk is the
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
            exact: false,
        },
        ..rows_spec(Vec::new(), 0)
    };
    let Err(err) = run(&mut r, &spec) else {
        panic!("an out-of-range index column must be rejected");
    };
    assert!(err.to_string().contains("invalid column list"), "{err}");
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

// ---------------------------------------------------------------------------
// Fold pre-map — the trust boundary
//
// `AggReadSpec.pre_map`/`pre_payload` are the client's, and the schema they
// describe is *derived* here rather than shipped. These pin that a frame no
// planner would send is refused rather than aborting the worker: the derivation
// runs through `DerivedSchema`, whose `push`/`push_pk` reject exactly what
// `SchemaDescriptor::new` asserts on (and its asserts are release-active).
//
// The fused map->fold loop over real rows is covered end-to-end by the Python
// aggregate suite, which is the only place a *valid* pre-map program exists —
// building one here would mean reimplementing the planner's expression compiler.
// ---------------------------------------------------------------------------

/// `(id U64 PK, v I64)` — a one-column key so the derived reduce input is the PK
/// plus whatever `pre_payload` declares.
fn premap_src() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

fn premap_spec(program: Vec<u8>, out_cols: Vec<(u8, bool)>) -> AggReadSpec {
    AggReadSpec {
        group_cols: vec![1],
        aggs: vec![],
        pre: Some(gnitz_wire::ComputeMap { program, out_cols }),
    }
}

/// The rejection message, or a panic naming the shape that was accepted.
/// `MapPlan` is not `Debug`, so the `Ok` half cannot go through `unwrap_err`.
fn premap_err(spec: &AggReadSpec) -> String {
    match compile_fold_pre_map(spec, &premap_src()) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("the pre-map derivation accepted a frame it must refuse"),
    }
}

/// No pre-map is the ordinary fold: the source *is* the reduce input, and
/// nothing is compiled.
#[test]
fn fold_pre_map_is_absent_without_a_program() {
    let spec = AggReadSpec::direct(vec![1], vec![]);
    assert!(compile_fold_pre_map(&spec, &premap_src()).unwrap().is_none());
}

/// A declaration wider than one schema can hold. `MAX_COLUMNS` payload columns
/// on top of a 1-column key is one past the limit — the case that would reach
/// `SchemaDescriptor::new`'s release-active `assert!` and abort the worker if the
/// derivation did not go through `DerivedSchema` first.
#[test]
fn fold_pre_map_refuses_an_over_wide_declaration() {
    let wide: Vec<(u8, bool)> = (0..crate::schema::MAX_COLUMNS)
        .map(|_| (type_code::I64, false))
        .collect();
    let err = premap_err(&premap_spec(vec![1, 2, 3], wide));
    assert!(
        err.contains("compute map: output exceeds MAX_COLUMNS"),
        "an over-wide pre-map must be refused by the derivation, got: {err}"
    );
}

/// A corrupt program blob is refused by the shared map compiler, not decoded
/// into a plan that folds over garbage.
#[test]
fn fold_pre_map_refuses_a_corrupt_program() {
    let err = premap_err(&premap_spec(vec![0xff; 8], vec![(type_code::I64, false)]));
    assert!(
        err.contains("map: invalid program"),
        "a corrupt pre-map blob must be refused by the program decoder, got: {err}"
    );
}
