use super::*;
use crate::catalog::CatalogEngine;
use crate::query::{drive, Drive};
use crate::test_support::{
    col_def, make_batch, scan_all, scratch_dir, try_register_identity_view, try_register_view, two_term_join_circuit,
    zset_of, LocalDrive, RowKey,
};
use gnitz_wire::TypeCode;
use std::collections::HashMap;

/// Ingest `rows` into `tid` as a push does, leaving them unticked.
fn push(engine: &mut CatalogEngine, tid: u64, rows: &[(u64, i64, i64)]) {
    let schema = engine.registry.relation_or_err(tid).unwrap().schema();
    engine.ingest_unticked(tid, make_batch(&schema, rows)).unwrap();
}

/// Tick `tid` over everything pushed into it since its last tick.
fn tick(engine: &mut CatalogEngine, tid: u64) {
    let delta = engine
        .registry
        .seal(tid)
        .unwrap()
        .expect("a view scans the pushed table");
    drive(
        &mut LocalDrive(engine),
        Drive::Tick { source: tid, round: 1 },
        Some(delta),
    )
    .unwrap();
}

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
    push(&mut engine, tid, &rows);
    tick(&mut engine, tid);
    engine.registry.set_scan_chunk_rows(1);

    // Row 8 lies outside every gathered key below.
    push(&mut engine, tid, &[(3, -1, 30), (5, -1, 50), (5, 1, 55), (8, -1, 80)]);
    let schema = engine.registry.relation_or_err(view).unwrap().schema();
    let expect = |rows: &[(u64, i64, i64)]| zset_of(&make_batch(&schema, rows), &schema);
    assert_eq!(hydrate(&mut engine, view, &[3, 5]), expect(&[(3, 1, 30), (5, 1, 50)]));
    assert_eq!(hydrate(&mut engine, view, &[5]), expect(&[(5, 1, 50)]));

    tick(&mut engine, tid);
    assert_eq!(hydrate(&mut engine, view, &[3, 5]), expect(&[(5, 1, 55)]));
}

/// A bounded join hydrates from its own operator trace: every key it holds
/// recomputes to exactly the rows it holds.
#[test]
fn a_trace_seed_recomputes_a_joins_rows() {
    let mut engine = CatalogEngine::open(&scratch_dir("dag_hydrate", "trace_seed"), 1).unwrap();
    let cols = [col_def("id", TypeCode::U64), col_def("k", TypeCode::U64)];
    let a = engine.create_table("public.a", &cols, &[0]).unwrap();
    let b = engine.create_table("public.b", &cols, &[0]).unwrap();
    let join_cols = [
        col_def("k", TypeCode::U64),
        col_def("a_id", TypeCode::U64),
        col_def("b_id", TypeCode::U64),
    ];
    let circuit = two_term_join_circuit(a, b, TypeCode::U64);
    let view = try_register_view(&mut engine, circuit, "bounded_join", &join_cols, 1 << 20, 0).unwrap();
    push(&mut engine, a, &[(1, 1, 10), (2, 1, 20), (3, 1, 10)]);
    tick(&mut engine, a);
    push(&mut engine, b, &[(7, 1, 10), (8, 1, 30), (9, 1, 20)]);
    tick(&mut engine, b);

    let rows = scan_all(&mut engine, view);
    assert_eq!(rows.len(), 3, "k=10 twice, k=20 once, k=30 unmatched");
    assert_eq!(hydrate(&mut engine, view, &[10, 20, 30]), zset_of(&rows, rows.schema()));
}
