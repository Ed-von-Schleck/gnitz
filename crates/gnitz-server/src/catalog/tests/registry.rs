//! `validated_run_last` is what stands between an untrusted `count` and the
//! catalog object-id counter in `allocate_ids`.

use super::*;
use crate::test_support::scratch_dir;
use std::fs;

fn open(name: &str) -> (CatalogEngine, String) {
    let dir = scratch_dir("catalog", name);
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    (engine, dir)
}

#[test]
fn a_huge_run_length_is_rejected_and_leaves_the_id_counter_untouched() {
    let (mut engine, dir) = open("registry_alloc_ids_overflow");

    let before = engine.allocate_ids(1).unwrap();
    let err = engine.allocate_ids(u64::MAX).unwrap_err();
    assert!(err.contains("invalid"), "{err}");
    let after = engine.allocate_ids(1).unwrap();

    assert_eq!(after, before + 1, "a rejected run must not move the id counter");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_zero_run_length_is_rejected() {
    let (mut engine, dir) = open("registry_alloc_zero_run");

    let before = engine.allocate_ids(1).unwrap();
    engine.allocate_ids(0).unwrap_err();
    assert_eq!(engine.allocate_ids(1).unwrap(), before + 1);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// Distinct from overflow: a count that fits `i64` cleanly but still reaches
/// the ceiling.
#[test]
fn a_run_length_reaching_the_id_ceiling_is_rejected() {
    let (mut engine, dir) = open("registry_alloc_ids_ceiling");

    let err = engine.allocate_ids(sys_tables::CATALOG_ID_CEILING as u64).unwrap_err();
    assert!(err.contains("invalid"), "{err}");

    let after = engine.allocate_ids(1).unwrap();
    assert!(after < sys_tables::CATALOG_ID_CEILING, "{after}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
