use super::*;
use crate::catalog::CatalogEngine;
use crate::test_support::{
    RowKey, certified, chunked, col_def, empty_catalog, make_batch, make_schema_u64_i64, push_and_tick,
    reindexed_on_col1, scan_all, table_engine_with, try_register_identity_view, try_register_view,
    two_term_join_circuit, u64_i64_cols, zset_of,
};
use gnitz_store::relation::PkRule;
use gnitz_wire::{Circuit, JoinKind, TypeCode};
use std::collections::HashMap;

/// `view`'s rows recomputed for the U64 keys `ids`, as a Z-set.
fn hydrate(engine: &mut CatalogEngine, view: u64, ids: &[u64]) -> HashMap<RowKey, i64> {
    let keys = PkKeys::from_sorted(8, ids.iter().flat_map(|k| k.to_be_bytes()).collect::<Vec<u8>>());
    let out = engine.dag.hydrate_keys(&engine.registry, view, keys).unwrap();
    zset_of(&out, out.schema())
}

/// A linear bounded view hydrates from its source as of the source's last tick,
/// one gathered row per chunk.
#[test]
fn a_relation_seed_reads_the_source_as_of_its_last_tick() {
    let cols = u64_i64_cols();
    let (_dir, mut engine, tid) = table_engine_with(1, &cols, chunked(1));
    let view = try_register_identity_view(&mut engine, tid, "bounded", &cols, 1 << 20, 0).unwrap();
    let rows: Vec<(u64, i64, i64)> = (1..=8).map(|id| (id, 1, id as i64 * 10)).collect();
    let schema = make_schema_u64_i64();
    push_and_tick(&mut engine, tid, certified(make_batch(&schema, &rows)));

    // Row 8 lies outside every gathered key below.
    let pushed = certified(make_batch(
        &schema,
        &[(3, -1, 30), (5, -1, 50), (5, 1, 55), (8, -1, 80)],
    ));
    engine.ingest_unticked(tid, pushed, PkRule::Enforce).unwrap();
    let expect = |rows: &[(u64, i64, i64)]| zset_of(&certified(make_batch(&schema, rows)), &schema);
    assert_eq!(hydrate(&mut engine, view, &[3, 5]), expect(&[(3, 1, 30), (5, 1, 50)]));
    assert_eq!(hydrate(&mut engine, view, &[5]), expect(&[(5, 1, 50)]));

    crate::query::tick(&mut engine, tid, 1).unwrap();
    assert_eq!(hydrate(&mut engine, view, &[3, 5]), expect(&[(5, 1, 55)]));
}

/// A bounded join hydrates from its own operator trace: every key it holds
/// recomputes to exactly the rows it holds, each at weight 1.
#[test]
fn a_trace_seed_recomputes_a_joins_rows() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
    let a = engine.create_table("public.a", &cols, &[0]).unwrap();
    let b = engine.create_table("public.b", &cols, &[0]).unwrap();
    let join_cols = ["k", "a_id", "b_id"].map(|name| col_def(name, TypeCode::U64));
    let circuit = two_term_join_circuit(a, b, TypeCode::U64);
    let view = try_register_view(&mut engine, circuit, "bounded_join", &join_cols, 1 << 20, 0).unwrap();
    let schema = engine.registry.relation_or_err(a).unwrap().schema();
    push_and_tick(
        &mut engine,
        a,
        certified(make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 10)])),
    );
    push_and_tick(
        &mut engine,
        b,
        certified(make_batch(&schema, &[(7, 1, 10), (8, 1, 30), (9, 1, 20)])),
    );

    let rows = scan_all(&mut engine, view);
    let held = zset_of(&rows, rows.schema());
    assert_eq!(held.len(), 3, "k=10 twice, k=20 once, k=30 unmatched");
    assert!(held.values().all(|&w| w == 1), "each match once");
    assert_eq!(hydrate(&mut engine, view, &[10, 20, 30]), held);
}

/// A bounded join whose `a` side is a `DISTINCT` hydrates from the clamp's own
/// history, read through the clamp: an `a` row the history holds twice joins
/// once, as the view holds it.
#[test]
fn a_clamp_history_seed_is_read_through_the_clamp() {
    let (_dir, mut engine) = empty_catalog(1);
    let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
    let a = engine.create_table("public.a", &cols, &[0]).unwrap();
    let b = engine.create_table("public.b", &cols, &[0]).unwrap();
    let mut circuit = Circuit::default();
    let ra = reindexed_on_col1(&mut circuit, a, TypeCode::U64);
    let twice = circuit.union(ra, ra);
    let da = circuit.distinct(twice);
    let db = reindexed_on_col1(&mut circuit, b, TypeCode::U64);
    circuit.join([da, db], [da, db], JoinKind::Equi);
    let join_cols = ["k", "a_id", "b_id"].map(|name| col_def(name, TypeCode::U64));
    let view = try_register_view(&mut engine, circuit, "bounded_distinct_join", &join_cols, 1 << 20, 0).unwrap();
    let schema = engine.registry.relation_or_err(a).unwrap().schema();
    push_and_tick(
        &mut engine,
        a,
        certified(make_batch(&schema, &[(1, 1, 10), (2, 1, 20)])),
    );
    push_and_tick(
        &mut engine,
        b,
        certified(make_batch(&schema, &[(7, 1, 10), (9, 1, 20)])),
    );

    let rows = scan_all(&mut engine, view);
    let held = zset_of(&rows, rows.schema());
    assert_eq!(held.len(), 2, "k=10 and k=20, once each");
    assert!(held.values().all(|&w| w == 1), "each match once, through the clamp");
    assert_eq!(hydrate(&mut engine, view, &[10, 20]), held);
}
