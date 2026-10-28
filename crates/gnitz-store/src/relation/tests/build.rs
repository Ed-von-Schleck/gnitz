use super::*;
use crate::storage::manifest_path;
use crate::test_support::{make_batch_raw, make_schema_u64_i64, pk_only_schema, solo, spec, zset_of};
use gnitz_wire::TypeCode;

/// A master creates each relation's directory, so it exists once the DDL is
/// acknowledged, and opens no child store under it.
#[test]
fn a_master_creates_the_relation_directory_and_no_child() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = RelationRegistry::master(tmp.path().to_str().unwrap(), 1, StoreConfig::default());
    registry
        .register(
            spec(50, RelationKind::BaseTable, make_schema_u64_i64()),
            make_schema_u64_i64(),
        )
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
    assert_eq!(dirs::subdir_names(&dir).unwrap(), Vec::<String>::new());
}

/// A view told to resume opens only from its manifests: its store from the
/// rows', whichever opener asks, its operator state from each declared trace's.
/// Told to rebuild, a worker opens it regardless.
#[test]
fn a_resumed_view_refuses_a_missing_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_str().unwrap();
    let schema = pk_only_schema(&[TypeCode::U64]);
    let spec = spec(7, RelationKind::View(ViewBudget::Plain), schema);
    let mut layout = StateLayout::default();
    layout.declare("t".to_string(), schema);
    {
        let mut origin = solo(tmp.path());
        origin.register(spec, schema).unwrap();
        let mut state = CircuitState::open(&origin, 7, &layout).unwrap();
        origin.checkpoint_ephemeral([&mut state], 0, |_| true).unwrap();
    }
    let reopen = || {
        let mut r = solo(tmp.path());
        r.register_at(spec, schema, &[], Some(0)).map(|_| r)
    };
    let trace = |r: &RelationRegistry| CircuitState::open(r, 7, &layout).map(drop);
    let worker = |resume: bool| {
        let mut master = RelationRegistry::master(base, 1, StoreConfig::default());
        master.register(spec, schema).unwrap();
        master.reconcile_child_dirs(|id| (id == 7).then_some(&layout)).unwrap();
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
    assert!(r.register_at(spec, schema, &[], Some(0)).is_err());
    assert!(r.relation(7).is_none(), "a refused view is not entered");
    assert!(worker(true).is_err());
    worker(false).expect("told to rebuild, a worker opens the view");
}

/// A second registration of a held id is refused before any store opens, and
/// the relation already registered keeps its rows.
#[test]
fn a_held_id_is_not_registered_again() {
    let tmp = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut registry = solo(tmp.path());
    registry
        .register(spec(9, RelationKind::BaseTable, schema), schema)
        .unwrap();
    let rows = make_batch_raw(&schema, &[(1, 1, 10)]);
    registry.ingest(9, rows.clone()).unwrap();

    let err = registry
        .register(spec(9, RelationKind::BaseTable, schema), schema)
        .unwrap_err();
    assert!(err.contains("already registered"), "{err}");
    assert_eq!(
        zset_of(&registry.relation(9).unwrap().full_scan(), &schema),
        zset_of(&rows, &schema)
    );
}

/// A registration whose index cannot be entered enters nothing, so the same
/// relation registers again without it.
#[test]
fn a_refused_index_rolls_its_registration_back() {
    let tmp = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut registry = solo(tmp.path());
    // A UNIQUE index on a view is refused: its circuit cannot refuse a duplicate.
    let view = spec(9, RelationKind::View(ViewBudget::Plain), schema);
    let unique = (IndexClaim::Index { id: 10, unique: true }, PkColList::from_slice(&[1]));
    assert!(registry.register_at(view, schema, &[unique], None).is_err());
    assert!(registry.relation(9).is_none(), "a refused registration enters nothing");
    registry
        .register(view, schema)
        .expect("the relation registers once its index is gone");
}
