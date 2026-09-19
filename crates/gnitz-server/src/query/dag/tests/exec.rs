//! The tick schedule and the one driver over it.

use super::*;
use crate::catalog::{CatalogEngine, ColumnDef};
use crate::test_support::{col_def, make_batch, register_identity_view, scratch_dir, sum_weights, try_register_view};
use gnitz_store::relation::Relation;
use gnitz_wire::type_code;

/// The exchange transport of the driver tests below. They register only identity
/// circuits, which repartition nothing — a call here means the premise changed,
/// and the new case needs a double that applies the view's `RelayRoute`.
struct NoExchange;

impl ExchangeCallback for NoExchange {
    fn do_exchange(&mut self, view_id: i64, _batch: Batch, _source_id: i64) -> Batch {
        panic!("view {view_id} relayed: these tests cover the non-exchanged path only");
    }
}

fn view_cols() -> Vec<ColumnDef> {
    vec![col_def("id", type_code::U64), col_def("v", type_code::I64)]
}

/// A base table `(id U64 PK, v I64)` in a fresh catalog.
fn engine_with_base(name: &str) -> (CatalogEngine, i64) {
    let mut engine = CatalogEngine::open(&scratch_dir("dag_exec", name), 1).unwrap();
    let base = engine.create_table("public.base", &view_cols(), &[0]).unwrap();
    (engine, base)
}

/// [`engine_with_base`] plus `base → {va, vb}` and `va → vdeep`: a two-wide
/// fan-out and a second rung over one of its arms.
fn engine_with_fanout(name: &str) -> (CatalogEngine, i64, i64, i64, i64) {
    let (mut engine, base) = engine_with_base(name);
    let cols = view_cols();
    let a = register_identity_view(&mut engine, base, "va", &cols);
    let b = register_identity_view(&mut engine, base, "vb", &cols);
    let deep = register_identity_view(&mut engine, a, "vdeep", &cols);
    (engine, base, a, b, deep)
}

/// `(pk, weight, payload)` rows in `tid`'s own registered schema.
fn delta_for(engine: &CatalogEngine, tid: i64, rows: &[(u64, i64, i64)]) -> Batch {
    let schema = engine
        .registry()
        .relation(tid)
        .map(Relation::schema)
        .expect("a registered relation");
    make_batch(&schema, rows)
}

/// The net weight a relation's own store holds.
fn live_weight(engine: &CatalogEngine, tid: i64) -> i64 {
    sum_weights(
        engine
            .registry()
            .relation(tid)
            .map(|r| r.cursor())
            .expect("an owned store"),
    )
}

// ── The schedule ────────────────────────────────────────────────────────────

/// The schedule names one step per dependency edge out of the source's forward
/// closure, ordered by view id — so a producer always precedes the steps it
/// feeds, and a tick of an intermediate view runs only what that view reaches.
#[test]
fn the_schedule_names_every_edge_of_the_closure_in_id_order() {
    let (engine, base, a, b, deep) = engine_with_fanout("schedule");
    let step = |view, producer| Step { view, producer };

    let dag = engine.dag();
    assert_eq!(
        dag.tick_schedule(base),
        vec![step(a, base), step(b, base), step(deep, a)],
        "every edge of the closure, producers first",
    );
    assert_eq!(
        dag.tick_schedule(a),
        vec![step(deep, a)],
        "a tick of an intermediate view runs only what it reaches",
    );
    assert!(dag.tick_schedule(deep).is_empty(), "a terminal view reaches nothing");
}

// ── The drivers ─────────────────────────────────────────────────────────────

/// One tick of the base drives the whole dependent closure through real compiled
/// plans: each view ingests what its own epoch produced, and a view over a view
/// sees its producer's output rather than the base delta.
#[test]
fn a_tick_drives_the_whole_closure() {
    let (mut engine, base, a, b, deep) = engine_with_fanout("evaluate");

    let delta = delta_for(&engine, base, &[(1, 1, 10), (2, 1, 20)]);
    let (dag, registry) = engine.dag_and_registry_mut();
    dag.drive(registry, Drive::Tick { source: base, round: 1 }, delta, &mut NoExchange)
        .unwrap();

    assert_eq!(live_weight(&engine, a), 2);
    assert_eq!(live_weight(&engine, b), 2);
    assert_eq!(
        live_weight(&engine, deep),
        2,
        "the second rung sees its producer's output, exactly once"
    );
}

/// A tick carrying no rows reaches the view's epoch and leaves it empty. Not a
/// test that every step ran: with identity circuits and no relay, an early
/// return would be indistinguishable here.
#[test]
fn an_empty_tick_produces_nothing() {
    let (mut engine, base) = engine_with_base("empty_tick");
    let a = register_identity_view(&mut engine, base, "va", &view_cols());

    let schema = engine.registry().relation(base).map(Relation::schema).unwrap();
    let (dag, registry) = engine.dag_and_registry_mut();
    let tick = Drive::Tick { source: base, round: 1 };
    dag.drive(registry, tick, Batch::empty_with_schema(&schema), &mut NoExchange)
        .unwrap();

    assert_eq!(live_weight(&engine, a), 0);
}

/// A view two producers feed runs one epoch per producer, and the view reading
/// it sees the union of the two — not the last one alone.
#[test]
fn a_two_producer_view_feeds_its_reader_the_union() {
    let (mut engine, base) = engine_with_base("two_producers");
    let cols = view_cols();
    let a = register_identity_view(&mut engine, base, "va", &cols);
    let b = register_identity_view(&mut engine, base, "vb", &cols);
    let mut circuit = gnitz_wire::Circuit::default();
    let left = circuit.input_delta(a as u64, gnitz_wire::ReadBound::None);
    let right = circuit.input_delta(b as u64, gnitz_wire::ReadBound::None);
    let merged = circuit.union(left, right);
    circuit.sink(merged);
    let u = try_register_view(&mut engine, circuit, "vu", &cols, 0, 0).unwrap();
    let d = register_identity_view(&mut engine, u, "vd", &cols);

    let delta = delta_for(&engine, base, &[(1, 1, 10), (2, 1, 20)]);
    let (dag, registry) = engine.dag_and_registry_mut();
    dag.drive(registry, Drive::Tick { source: base, round: 1 }, delta, &mut NoExchange)
        .unwrap();

    assert_eq!(live_weight(&engine, u), 4, "both producers' epochs land");
    assert_eq!(live_weight(&engine, d), 4, "its reader sees both, once each");
}

/// A backfill drive runs the named view alone — not the source's closure, which
/// would double-count into the dependents the source already populated.
#[test]
fn backfill_chunk_runs_only_the_named_view() {
    let (mut engine, base) = engine_with_base("backfill");
    let cols = view_cols();
    let a = register_identity_view(&mut engine, base, "va", &cols);
    let b = register_identity_view(&mut engine, base, "vb", &cols);
    let backfill = |view| Drive::Backfill { view, source: base };

    let chunk = delta_for(&engine, base, &[(1, 1, 10)]);
    let (dag, registry) = engine.dag_and_registry_mut();
    dag.drive(registry, backfill(a), chunk, &mut NoExchange).unwrap();
    assert_eq!(live_weight(&engine, a), 1);
    assert_eq!(
        live_weight(&engine, b),
        0,
        "the source's other dependents are untouched"
    );

    let empty = Batch::empty_with_schema(&engine.registry().relation(base).map(Relation::schema).unwrap());
    let (dag, registry) = engine.dag_and_registry_mut();
    dag.drive(registry, backfill(a), empty, &mut NoExchange).unwrap();
    assert_eq!(live_weight(&engine, a), 1, "a chunk with no rows adds none");

    // A view the registry no longer holds is a fail-stop, not a silent skip.
    let (dag, registry) = engine.dag_and_registry_mut();
    let empty = Batch::empty_with_schema(&registry.relation(base).map(Relation::schema).unwrap());
    assert!(dag.drive(registry, backfill(999_999), empty, &mut NoExchange).is_err());
}

// ── The relay's single-sourcing ─────────────────────────────────────────────

/// Records the row count of every batch a relay sends, and relays it back.
struct Recorder(Vec<usize>);

impl ExchangeCallback for Recorder {
    fn do_exchange(&mut self, _view_id: i64, batch: Batch, _source_id: i64) -> Batch {
        self.0.push(batch.len());
        batch
    }
}

/// Every other rank still takes part in the round, with an empty batch.
#[test]
fn a_replicated_sources_relay_is_sent_by_worker_0_alone() {
    let cols = view_cols();
    for rank in [0u32, 1] {
        let mut engine =
            CatalogEngine::open_master(&scratch_dir("dag_exec", &format!("relay_trim_{rank}")), 2).unwrap();
        let replicated = engine.allocate_ids(1).unwrap();
        engine.write_column_records(replicated, &cols).unwrap();
        let mut bb = gnitz_store::storage::BatchBuilder::new(*crate::catalog::SysFamily::Table.schema());
        let flags = gnitz_wire::TableProps { replicated: true, ..Default::default() }.pack();
        crate::test_support::push_table_tab_row(
            &mut bb,
            replicated,
            crate::catalog::PUBLIC_SCHEMA_ID,
            "rt",
            gnitz_wire::pack_pk_cols(&[0]),
            flags,
            1,
        );
        engine.submit(crate::catalog::SysFamily::Table, bb.finish()).unwrap();
        let keyed = engine.create_table("public.kt", &cols, &[0]).unwrap();
        engine.registry_mut().reconcile_child_dirs().unwrap();
        engine
            .open_stores(rank, gnitz_store::relation::Residency::Worker)
            .unwrap();

        let delta = delta_for(&engine, replicated, &[(1, 1, 10), (2, 1, 20)]);
        let registry = engine.registry();
        let mut sent = Recorder(Vec::new());
        // One relay per source: muting is bound at construction.
        Relay::new(&mut sent, registry, 99, replicated, false).send(delta.clone_batch(), replicated, true);
        Relay::new(&mut sent, registry, 99, keyed, false).send(delta.clone_batch(), keyed, true);
        // A single-side round is not single-sourced, so it is sent by every rank.
        Relay::new(&mut sent, registry, 99, replicated, false).round(delta, false);

        let replicated_rows = if rank == 0 { 2 } else { 0 };
        assert_eq!(sent.0, vec![replicated_rows, 2, 2], "rank {rank}");
    }
}
