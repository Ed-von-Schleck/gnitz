use super::*;
use crate::storage::manifest_path;
use crate::test_support::{
    TID, assert_index_holds, batch_rows, make_batch, make_schema_u64_i64, pk_only_schema, relation_fixture, solo, spec,
};
use gnitz_wire::TypeCode;
use gnitz_zset::schema::SchemaColumn;
use std::path::Path;

fn index(id: u64, unique: bool) -> IndexClaim {
    IndexClaim::Index { id, unique }
}

/// A master creates each relation's directory, so it exists once the DDL is
/// acknowledged, and opens no child store under it.
#[test]
fn a_master_creates_the_relation_directory_and_no_child() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = RelationRegistry::master(tmp.path().to_str().unwrap(), 1, StoreConfig::default());
    let schema = make_schema_u64_i64();
    let indexes = [(index(999, false), PkColList::from_slice(&[1]))];
    registry
        .register_at(
            spec(50, RelationKind::BaseTable, schema),
            schema,
            &indexes,
            StateLayout::default(),
            None,
        )
        .unwrap();
    let dir = relation_dir(registry.base_dir(), 50);
    assert!(Path::new(&dir).is_dir());
    assert_eq!(dirs::subdir_names(&dir).unwrap(), Vec::<String>::new());
}

/// An index whose columns lead the owner's PK reads its entries off the owner's
/// rows, so it opens no store of its own; any other index does.
#[test]
fn an_index_leading_the_pk_keeps_no_store() {
    let schema = gnitz_zset_testkit::keyed_schema(
        &[TypeCode::I64, TypeCode::U64],
        &[SchemaColumn::new(TypeCode::I64, true)],
    );
    let r = relation_fixture(RelationKind::BaseTable, schema, &[&[0], &[0, 1], &[2]], []);
    let rel = r.relation(TID).unwrap();
    for (cols, leads) in [(&[0u32][..], true), (&[0, 1], true), (&[2], false)] {
        assert_eq!(rel.index_on(cols).unwrap().reads_owner_pk(), leads, "{cols:?}");
        let dir = r.child_dir(TID, ChildKind::Index(PkColList::from_slice(cols)));
        assert_eq!(Path::new(&dir).exists(), !leads, "{cols:?}");
    }
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
    let layout = || {
        let mut layout = StateLayout::default();
        layout.declare("t".to_string(), schema);
        layout
    };
    {
        let mut origin = solo(tmp.path());
        origin.register_at(spec, schema, &[], layout(), None).unwrap();
        let mut state = CircuitState::open(&origin, 7).unwrap();
        origin.checkpoint_ephemeral([&mut state], 0, |_| true).unwrap();
    }
    let reopen = || {
        let mut r = solo(tmp.path());
        r.register_at(spec, schema, &[], layout(), Some(0)).map(|_| r)
    };
    let trace = |r: &RelationRegistry| CircuitState::open(r, 7).map(drop);
    let worker = |resume: bool| {
        let mut master = RelationRegistry::master(base, 1, StoreConfig::default());
        master.register_at(spec, schema, &[], layout(), None).unwrap();
        master.reconcile_child_dirs().unwrap();
        master
            .open_stores(0, Residency::Worker, |_| resume.then_some(0))
            .map(drop)
    };
    let child = |kind| manifest_path(&solo(tmp.path()).child_dir(7, kind));
    trace(&reopen().unwrap()).expect("a published trace resumes");
    worker(true).expect("a published view resumes on a worker");

    let declared = layout();
    std::fs::remove_file(child(ChildKind::Scratch(declared.names().next().unwrap()))).unwrap();
    assert!(trace(&reopen().expect("the rows resume without the trace")).is_err());

    std::fs::remove_file(child(ChildKind::Rows)).unwrap();
    let mut r = solo(tmp.path());
    assert!(r.register_at(spec, schema, &[], layout(), Some(0)).is_err());
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
    registry.ingest(9, make_batch(&schema, &[(1, 1, 10)])).unwrap();

    let err = registry
        .register(spec(9, RelationKind::BaseTable, schema), schema)
        .unwrap_err();
    assert!(err.contains("already registered"), "{err}");
    assert_eq!(batch_rows(&registry.relation(9).unwrap().full_scan()), [(1, 1, 10)]);
}

/// A registration or an index addition whose index cannot be entered enters
/// nothing, so the same relation registers again without it.
#[test]
fn a_refused_index_rolls_its_registration_back() {
    let tmp = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let mut registry = solo(tmp.path());
    // A UNIQUE index on a view is refused: its circuit cannot refuse a duplicate.
    let view = spec(9, RelationKind::View(ViewBudget::Plain), schema);
    let (plain, unique) = (
        (index(10, false), PkColList::from_slice(&[1])),
        (index(11, true), PkColList::from_slice(&[0])),
    );
    assert!(
        registry
            .register_at(view, schema, &[plain, unique], StateLayout::default(), None)
            .is_err()
    );
    assert!(registry.relation(9).is_none(), "a refused registration enters nothing");
    registry
        .register(view, schema)
        .expect("the relation registers once its index is gone");
    assert!(registry.add_indexes(9, &[plain, unique]).is_err());
    assert!(
        registry.relation(9).unwrap().indexes().is_empty(),
        "a refused addition enters nothing"
    );
}

/// An index reopened beside its owner resumes from the manifest the owner's
/// checkpoint published it with, and is filled from the owner when its own
/// manifest is gone or of another generation — one a release left behind.
/// Either way it holds the owner's entries.
#[test]
fn a_reopened_index_resumes_only_at_its_owners_generation() {
    let schema = make_schema_u64_i64();
    let spec = spec(9, RelationKind::View(ViewBudget::Plain), schema);
    let indexes = [(index(1, false), PkColList::from_slice(&[1]))];
    let rows = make_batch(&schema, &[(1, 1, 30), (2, 1, 10), (3, 1, 20)]);
    // A registry over `base` at `generation`, and how many indexes it filled.
    let reopen = |base: &Path, generation: u64| {
        let mut r = solo(base);
        let filled = r
            .register_at(spec, schema, &indexes, StateLayout::default(), Some(generation))
            .unwrap();
        assert_index_holds(
            r.relation(9).unwrap(),
            &[1],
            &rows,
            format_args!("generation {generation}"),
        );
        (r, filled)
    };
    for gone in [true, false] {
        let tmp = tempfile::tempdir().unwrap();
        let mut origin = solo(tmp.path());
        origin
            .register_at(spec, schema, &indexes, StateLayout::default(), None)
            .unwrap();
        origin.ingest(9, rows.clone()).unwrap();
        origin.checkpoint_ephemeral([], 4, |_| true).unwrap();
        drop(origin);
        assert_eq!(reopen(tmp.path(), 4).1, 0, "published with its owner");

        let generation = match gone {
            true => {
                let dir = solo(tmp.path()).child_dir(9, ChildKind::Index(indexes[0].1));
                std::fs::remove_file(manifest_path(&dir)).unwrap();
                4
            }
            // The owner alone publishes at 5: the released index keeps its manifest at 4.
            false => {
                let (mut r, _) = reopen(tmp.path(), 4);
                r.release_index(9, indexes[0].0);
                r.checkpoint_ephemeral([], 5, |_| true).unwrap();
                5
            }
        };
        assert_eq!(
            reopen(tmp.path(), generation).1,
            1,
            "gone={gone}: filled from the owner"
        );
    }
}
