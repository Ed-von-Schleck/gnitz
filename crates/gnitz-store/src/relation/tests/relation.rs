use super::*;
use crate::storage::{manifest_path, read_at, read_intact};
use crate::test_support::{
    flip_last_byte_in_place, make_batch_raw, make_schema_u64_i64, pk_only_schema, pk_u64_two_i64_schema, zset_of,
};
use gnitz_wire::TypeCode;

/// A one-worker registry over `base`.
fn solo(base: &std::path::Path) -> RelationRegistry {
    RelationRegistry::new(base.to_str().unwrap(), Slot::SOLO, StoreConfig::default())
}

fn table(id: u64, schema: SchemaDescriptor) -> RelationSpec {
    RelationSpec {
        id,
        kind: RelationKind::BaseTable,
        schema,
    }
}

fn view(id: u64) -> RelationSpec {
    RelationSpec {
        id,
        kind: RelationKind::View(ViewProps::Plain),
        schema: pk_only_schema(&[TypeCode::U64]),
    }
}

fn index(id: u64, unique: bool) -> IndexClaim {
    IndexClaim::Index { id, unique }
}

#[test]
fn a_circuit_lives_while_one_claim_remains() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    registry.register(table(50, pk_u64_two_i64_schema())).unwrap();
    registry.add_index(50, index(70, false), &[2]).unwrap();
    registry.add_index(50, index(71, true), &[2]).unwrap();
    let circuit = |r: &RelationRegistry| r.relation(50).unwrap().index_on(&[2]).map(|ix| ix.is_unique());
    assert_eq!(
        registry.relation(50).unwrap().indexes().len(),
        1,
        "one circuit per column list"
    );
    assert_eq!(circuit(&registry), Some(true));

    registry.release_index(50, 71);
    assert_eq!(
        circuit(&registry),
        Some(false),
        "the non-unique claim keeps the circuit"
    );

    registry.release_index(50, 12345);
    registry.release_index(51, 70);
    assert_eq!(circuit(&registry), Some(false), "an unknown claim or owner is a no-op");

    registry.release_index(50, 70);
    assert!(
        registry.relation(50).unwrap().indexes().is_empty(),
        "the last release drops the circuit"
    );
}

/// A master creates each relation's directory, so it exists once the DDL is
/// acknowledged, and opens no child store under it.
#[test]
fn a_master_creates_the_relation_directory_and_no_child() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = RelationRegistry::master(tmp.path().to_str().unwrap(), 1, StoreConfig::default());
    registry.register(table(50, make_schema_u64_i64())).unwrap();
    registry.add_index(50, index(999, false), &[1]).unwrap();
    let dir = relation_dir(registry.base_dir(), 50);
    assert!(std::path::Path::new(&dir).is_dir());
    assert_eq!(super::dirs::subdir_names(&dir).unwrap(), Vec::<String>::new());
}

#[test]
fn bound_cols_admits_only_the_relations_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    registry.register(table(50, pk_u64_two_i64_schema())).unwrap();
    let r = registry.relation(50).unwrap();
    let bound = |cols: &[u32]| {
        r.bound_cols(PkColList::from_slice(cols), "op")
            .map(|c| c.as_slice().to_vec())
    };
    assert_eq!(bound(&[2, 0]), Ok(vec![2, 0]));
    for cols in [&[3][..], &[0, 3]] {
        assert!(bound(cols).is_err(), "{cols:?}");
    }
}

/// Not unique, or covering the PK: either one excludes an index.
#[test]
fn a_unique_index_covering_the_pk_has_nothing_left_to_check() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    registry.register(table(60, pk_u64_two_i64_schema())).unwrap();
    registry.add_index(60, index(901, true), &[0]).unwrap(); // exactly the PK
    registry.add_index(60, index(902, true), &[2, 0]).unwrap(); // the PK plus a payload column
    registry.add_index(60, index(903, true), &[1]).unwrap(); // the only real check
    registry.add_index(60, index(904, false), &[2]).unwrap(); // not unique at all

    let cols: Vec<Vec<u32>> = registry
        .relation(60)
        .unwrap()
        .unique_indexes_to_check()
        .map(|ic| ic.cols().as_slice().to_vec())
        .collect();
    assert_eq!(cols, vec![vec![1u32]]);
}

/// The append path sizes by the store's region count, so a batch of another
/// payload arity is refused before anything is written.
#[test]
fn ingest_refuses_a_batch_of_another_arity() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    registry.register(table(50, make_schema_u64_i64())).unwrap();
    let mut wide = gnitz_zset::repr::BatchBuilder::new(pk_u64_two_i64_schema());
    wide.begin_row(1, 1);
    wide.put_int(1);
    wide.put_int(2);
    wide.end_row();
    assert!(registry.ingest(50, wide.finish()).is_err());
    assert_eq!(
        registry.relation(50).unwrap().full_scan().len(),
        0,
        "nothing was written"
    );
}

/// An index is fed the batch as the PK rule left it, so an upsert moves the
/// entry: only the live row's entry remains, at weight 1.
#[test]
fn an_upsert_moves_the_index_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    let schema = make_schema_u64_i64();
    registry.register(table(50, schema)).unwrap();
    registry.add_index(50, index(51, false), &[1]).unwrap();
    registry.ingest(50, make_batch_raw(&schema, &[(1, 1, 10)])).unwrap();
    registry.ingest(50, make_batch_raw(&schema, &[(1, 1, 20)])).unwrap();

    let ix = registry.relation(50).unwrap().index_on(&[1]).unwrap();
    let want =
        gnitz_zset::algebra::index_entries(&make_batch_raw(&schema, &[(1, 1, 20)]), &ix.key_spec(), &ix.schema());
    assert_eq!(
        zset_of(&ix.cursor().materialize(), &ix.schema()),
        zset_of(&want, &ix.schema())
    );
}

/// An index store is rederived: the base round leaves it, the ephemeral round
/// publishes it at the resume generation.
#[test]
fn only_the_ephemeral_round_publishes_an_index() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    registry.register(table(70, make_schema_u64_i64())).unwrap();
    registry.add_index(70, index(999, false), &[1]).unwrap();
    let dir = registry.child_dir(70, ChildKind::Index(PkColList::from_slice(&[1])));

    registry.set_resume_generation(3);
    registry.checkpoint_base().unwrap();
    assert!(read_intact(&dir).unwrap().is_none(), "the base round skips it");
    registry.checkpoint_ephemeral([]).unwrap();
    assert!(read_at(&dir, 3).unwrap().is_some());
}

/// Only a directory named exactly as a relation's is read, and a damaged
/// manifest holds no record.
#[test]
fn persisted_records_reads_well_formed_relation_manifests() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    let (good, damaged) = (123, 124);
    registry.register(view(good)).unwrap();
    registry.register(view(damaged)).unwrap();
    registry.set_caller_record(good, b"good".to_vec()).unwrap();
    registry.set_caller_record(damaged, b"damaged".to_vec()).unwrap();
    registry.checkpoint_ephemeral([]).unwrap();

    flip_last_byte_in_place(manifest_path(&registry.child_dir(damaged, ChildKind::Rows)));
    // A name that parses to `good`'s id but is not the name its directory has.
    let rows = ChildAddr { kind: ChildKind::Rows, slot: Slot::SOLO };
    let alias = format!("{}/0{good}", relations_dir(registry.base_dir()));
    std::fs::create_dir_all(rows.dir(&alias)).unwrap();
    std::fs::copy(
        manifest_path(&registry.child_dir(good, ChildKind::Rows)),
        rows.manifest(&alias),
    )
    .unwrap();

    let records: Vec<(u64, Vec<u8>)> = registry
        .persisted_records()
        .unwrap()
        .into_iter()
        .map(|(id, r)| (id, r.unwrap()))
        .collect();
    assert_eq!(records, vec![(good, b"good".to_vec())]);
}

/// A view told to resume opens only from its manifests: its store from the
/// rows', whichever opener asks, its operator state from each declared trace's.
/// Told to rebuild, a worker opens it regardless.
#[test]
fn a_resumed_view_refuses_a_missing_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_str().unwrap();
    let spec = view(7);
    let layout = || {
        let mut l = StateLayout::default();
        l.declare("t".to_string(), spec.schema);
        l
    };
    {
        let mut origin = solo(tmp.path());
        origin.register(spec).unwrap();
        let mut state = CircuitState::open(&origin, 7, layout()).unwrap();
        origin.checkpoint_ephemeral([&mut state]).unwrap();
    }
    let reopen = || {
        let mut r = solo(tmp.path());
        r.reopen_view(spec).map(|()| r)
    };
    let trace = |r: &RelationRegistry| CircuitState::open(r, 7, layout()).map(drop);
    let worker = |resume: bool| {
        let mut master = RelationRegistry::master(base, 1, StoreConfig::default());
        master.register(spec).unwrap();
        master.reconcile_child_dirs().unwrap();
        master.open_stores(0, Residency::Worker, |_| resume).map(drop)
    };
    let child = |kind| manifest_path(&solo(tmp.path()).child_dir(7, kind));
    trace(&reopen().unwrap()).expect("a published trace resumes");
    worker(true).expect("a published view resumes on a worker");

    std::fs::remove_file(child(ChildKind::Scratch("t"))).unwrap();
    assert!(trace(&reopen().expect("the rows resume without the trace")).is_err());

    std::fs::remove_file(child(ChildKind::Rows)).unwrap();
    let mut r = solo(tmp.path());
    assert!(r.reopen_view(spec).is_err());
    assert!(!r.has_id(7), "a refused view is not entered");
    assert!(worker(true).is_err());
    // Last: a rebuild erases the traces.
    worker(false).expect("told to rebuild, a worker opens the view");
}
