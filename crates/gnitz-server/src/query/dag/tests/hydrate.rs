use super::*;
use crate::catalog::CatalogEngine;
use crate::test_support::{
    col_def, make_batch, make_schema_u64_i64, push_and_tick, scan_all, scratch_dir, try_register_identity_view,
    try_register_view, two_term_join_circuit, zset_of, RowKey,
};
use gnitz_wire::TypeCode;
use std::collections::HashMap;

/// `view`'s rows recomputed for the U64 keys `ids`, as a Z-set.
fn hydrate(engine: &mut CatalogEngine, view: u64, ids: &[u64]) -> HashMap<RowKey, i64> {
    let keys = PkKeys::from_sorted(8, ids.iter().flat_map(|k| k.to_be_bytes()).collect());
    let out = engine.dag.hydrate_keys(&engine.registry, view, keys).unwrap();
    zset_of(&out, out.schema())
}

/// A linear bounded view hydrates from its source as of the source's last tick,
/// one gathered row per chunk.
#[test]
fn a_relation_seed_reads_the_source_as_of_its_last_tick() {
    let mut engine = CatalogEngine::open(&scratch_dir("dag_hydrate", "relation_seed"), 1).unwrap();
    let cols = [col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let view = try_register_identity_view(&mut engine, tid, "bounded", &cols, 1 << 20, 0).unwrap();
    let rows: Vec<(u64, i64, i64)> = (1..=8).map(|id| (id, 1, id as i64 * 10)).collect();
    let schema = make_schema_u64_i64();
    push_and_tick(&mut engine, tid, make_batch(&schema, &rows));
    engine.registry.set_scan_chunk_rows(1);

    // Row 8 lies outside every gathered key below.
    let pushed = make_batch(&schema, &[(3, -1, 30), (5, -1, 50), (5, 1, 55), (8, -1, 80)]);
    engine.ingest_unticked(tid, pushed).unwrap();
    let expect = |rows: &[(u64, i64, i64)]| zset_of(&make_batch(&schema, rows), &schema);
    assert_eq!(hydrate(&mut engine, view, &[3, 5]), expect(&[(3, 1, 30), (5, 1, 50)]));
    assert_eq!(hydrate(&mut engine, view, &[5]), expect(&[(5, 1, 50)]));

    crate::query::tick(&mut engine, tid, 1).unwrap();
    assert_eq!(hydrate(&mut engine, view, &[3, 5]), expect(&[(5, 1, 55)]));
}

/// A bounded join hydrates from its own operator trace: every key it holds
/// recomputes to exactly the rows it holds, each at weight 1.
#[test]
fn a_trace_seed_recomputes_a_joins_rows() {
    let mut engine = CatalogEngine::open(&scratch_dir("dag_hydrate", "trace_seed"), 1).unwrap();
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
        make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 10)]),
    );
    push_and_tick(
        &mut engine,
        b,
        make_batch(&schema, &[(7, 1, 10), (8, 1, 30), (9, 1, 20)]),
    );

    let rows = scan_all(&mut engine, view);
    let held = zset_of(&rows, rows.schema());
    assert_eq!(held.len(), 3, "k=10 twice, k=20 once, k=30 unmatched");
    assert!(held.values().all(|&w| w == 1), "each match once");
    assert_eq!(hydrate(&mut engine, view, &[10, 20, 30]), held);
}
