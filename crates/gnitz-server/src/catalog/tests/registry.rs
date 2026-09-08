//! `validated_run_last` is what stands between an untrusted `count` and the
//! durable id counters in `allocate_table_ids` / `allocate_index_ids`.

use super::*;
use crate::test_support::scratch_dir;
use std::fs;

fn open(name: &str) -> (CatalogEngine, String) {
    let dir = scratch_dir("catalog", name);
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    (engine, dir)
}

#[test]
fn a_huge_run_length_is_rejected_and_leaves_the_table_id_counter_untouched() {
    let (mut engine, dir) = open("registry_alloc_table_ids_overflow");

    let before = engine.allocate_table_id().unwrap();
    let err = engine.allocate_table_ids(u64::MAX).unwrap_err();
    assert!(err.contains("invalid"), "{err}");
    let after = engine.allocate_table_id().unwrap();

    assert_eq!(after, before + 1, "a rejected run must not move the id counter");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_huge_run_length_is_rejected_and_leaves_the_index_id_counter_untouched() {
    let (mut engine, dir) = open("registry_alloc_index_ids_overflow");

    let before = engine.allocate_index_id().unwrap();
    let err = engine.allocate_index_ids(u64::MAX).unwrap_err();
    assert!(err.contains("invalid"), "{err}");
    let after = engine.allocate_index_id().unwrap();

    assert_eq!(after, before + 1, "a rejected run must not move the id counter");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_zero_run_length_is_rejected_for_both_id_spaces() {
    let (mut engine, dir) = open("registry_alloc_zero_run");

    let before_table = engine.allocate_table_id().unwrap();
    engine.allocate_table_ids(0).unwrap_err();
    assert_eq!(engine.allocate_table_id().unwrap(), before_table + 1);

    let before_index = engine.allocate_index_id().unwrap();
    engine.allocate_index_ids(0).unwrap_err();
    assert_eq!(engine.allocate_index_id().unwrap(), before_index + 1);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// Distinct from overflow: a count that fits `i64` cleanly but still reaches
/// the ceiling.
#[test]
fn a_run_length_reaching_the_relation_id_ceiling_is_rejected() {
    let (mut engine, dir) = open("registry_alloc_table_ids_ceiling");

    let err = engine
        .allocate_table_ids(sys_tables::RELATION_ID_CEILING as u64)
        .unwrap_err();
    assert!(err.contains("invalid"), "{err}");

    let after = engine.allocate_table_id().unwrap();
    assert!(after < sys_tables::RELATION_ID_CEILING, "{after}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
