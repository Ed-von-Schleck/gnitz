use super::*;
use crate::storage::manifest_path;
use crate::test_support::{make_schema_u64_i64, pk_only_schema, solo, spec};
use gnitz_wire::TypeCode;

/// A master creates each relation's directory, so it exists once the DDL is
/// acknowledged, and opens no child store under it.
#[test]
fn a_master_creates_the_relation_directory_and_no_child() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = RelationRegistry::master(tmp.path().to_str().unwrap(), 1, StoreConfig::default());
    registry
        .register(spec(50, RelationKind::BaseTable, make_schema_u64_i64()))
        .unwrap();
    registry
        .add_index(
            50,
            IndexClaim::Index { id: 999, unique: false },
            PkColList::from_slice(&[1]),
        )
        .unwrap();
    let dir = relation_dir(registry.base_dir(), 50);
    assert!(std::path::Path::new(&dir).is_dir());
    assert_eq!(super::super::dirs::subdir_names(&dir).unwrap(), Vec::<String>::new());
}

/// A view told to resume opens only from its manifests: its store from the
/// rows', whichever opener asks, its operator state from each declared trace's.
/// Told to rebuild, a worker opens it regardless.
#[test]
fn a_resumed_view_refuses_a_missing_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_str().unwrap();
    let spec = spec(
        7,
        RelationKind::View(ViewBudget::Plain),
        pk_only_schema(&[TypeCode::U64]),
    );
    let layout = || {
        let mut l = StateLayout::default();
        l.declare("t".to_string(), spec.schema);
        l
    };
    {
        let mut origin = solo(tmp.path());
        origin.register(spec).unwrap();
        let mut state = CircuitState::open(&origin, 7, &layout()).unwrap();
        origin.checkpoint_ephemeral([(7, &mut state)], 0, |_| true).unwrap();
    }
    let reopen = || {
        let mut r = solo(tmp.path());
        r.reopen_view(spec, 0).map(|()| r)
    };
    let trace = |r: &RelationRegistry| CircuitState::open(r, 7, &layout()).map(drop);
    let worker = |resume: bool| {
        let mut master = RelationRegistry::master(base, 1, StoreConfig::default());
        master.register(spec).unwrap();
        master.declare_traces(7, &layout()).unwrap();
        master.reconcile_child_dirs().unwrap();
        master
            .open_stores(0, Residency::Worker, |_| resume.then_some(0))
            .map(drop)
    };
    let child = |kind| manifest_path(&solo(tmp.path()).child_dir(7, kind));
    trace(&reopen().unwrap()).expect("a published trace resumes");
    worker(true).expect("a published view resumes on a worker");

    std::fs::remove_file(child(ChildKind::Scratch("t"))).unwrap();
    assert!(trace(&reopen().expect("the rows resume without the trace")).is_err());

    std::fs::remove_file(child(ChildKind::Rows)).unwrap();
    let mut r = solo(tmp.path());
    assert!(r.reopen_view(spec, 0).is_err());
    assert!(!r.has_id(7), "a refused view is not entered");
    assert!(worker(true).is_err());
    worker(false).expect("told to rebuild, a worker opens the view");
}
