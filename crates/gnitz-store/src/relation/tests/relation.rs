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
        placement: Placement::full_pk(&schema),
        pk_repeats: false,
    }
}

fn view(id: u64) -> RelationSpec {
    let schema = pk_only_schema(&[TypeCode::U64]);
    RelationSpec {
        id,
        kind: RelationKind::View(ViewProps::Plain),
        schema,
        placement: Placement::full_pk(&schema),
        pk_repeats: false,
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
    registry
        .add_index(50, index(70, false), PkColList::from_slice(&[2]))
        .unwrap();
    registry
        .add_index(50, index(71, true), PkColList::from_slice(&[2]))
        .unwrap();
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
/// A store holding each of its keys once, in full, takes a non-unique index, and
/// only a base table a unique one.
#[test]
fn only_a_store_naming_one_row_per_key_admits_an_index() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    let schema = make_schema_u64_i64();
    let plain = RelationKind::View(ViewProps::Plain);
    let mb = std::num::NonZeroU64::new(1 << 20).unwrap();
    let fed = RelationKind::View(ViewProps::Fed { delta_bytes: mb });
    let bounded = RelationKind::View(ViewProps::Bounded { capacity_bytes: mb });
    let owners = [
        (RelationKind::BaseTable, false, Ok(true)),
        (plain, false, Ok(false)),
        (fed, false, Ok(false)),
        (plain, true, Err("repeats its primary key")),
        (bounded, false, Err("without a capacity")),
        (RelationKind::Stream, true, Err("without a capacity")),
    ];
    for (id, (kind, pk_repeats, want)) in (70..).zip(owners) {
        let spec = RelationSpec { kind, pk_repeats, ..table(id, schema) };
        registry.register(spec).unwrap();
        let verdict = |unique| registry.index_owner(id, unique).map(drop);
        match want {
            Ok(unique_too) => {
                assert!(verdict(false).is_ok(), "{kind:?}");
                assert_eq!(verdict(true).is_ok(), unique_too, "{kind:?}");
            }
            Err(needle) => {
                let err = verdict(false).unwrap_err();
                assert!(err.contains(needle), "{kind:?}: {err}");
                assert!(verdict(true).is_err(), "{kind:?}");
            }
        }
    }
}

#[test]
fn a_master_creates_the_relation_directory_and_no_child() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = RelationRegistry::master(tmp.path().to_str().unwrap(), 1, StoreConfig::default());
    registry.register(table(50, make_schema_u64_i64())).unwrap();
    registry
        .add_index(50, index(999, false), PkColList::from_slice(&[1]))
        .unwrap();
    let dir = relation_dir(registry.base_dir(), 50);
    assert!(std::path::Path::new(&dir).is_dir());
    assert_eq!(super::dirs::subdir_names(&dir).unwrap(), Vec::<String>::new());
}

/// Not unique, or covering the PK: either one excludes an index.
#[test]
fn a_unique_index_covering_the_pk_has_nothing_left_to_check() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    registry.register(table(60, pk_u64_two_i64_schema())).unwrap();
    registry
        .add_index(60, index(901, true), PkColList::from_slice(&[0]))
        .unwrap(); // exactly the PK
    registry
        .add_index(60, index(902, true), PkColList::from_slice(&[2, 0]))
        .unwrap(); // the PK plus a payload column
    registry
        .add_index(60, index(903, true), PkColList::from_slice(&[1]))
        .unwrap(); // the only real check
    registry
        .add_index(60, index(904, false), PkColList::from_slice(&[2]))
        .unwrap(); // not unique at all

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
    let mut wide = gnitz_zset::repr::BatchBuilder::new(&pk_u64_two_i64_schema());
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
    registry
        .add_index(50, index(51, false), PkColList::from_slice(&[1]))
        .unwrap();
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

/// An upsert of the row a key already holds retracts and re-inserts one entry;
/// the pair cancels before the index store takes it.
#[test]
fn an_identical_upsert_writes_no_index_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    let schema = make_schema_u64_i64();
    registry.register(table(50, schema)).unwrap();
    registry
        .add_index(50, index(51, false), PkColList::from_slice(&[1]))
        .unwrap();
    for _ in 0..3 {
        registry.ingest(50, make_batch_raw(&schema, &[(1, 1, 10)])).unwrap();
    }
    let ix = registry.relation(50).unwrap().index_on(&[1]).unwrap();
    assert_eq!(ix.store.held().estimated_rows(), 1);
}

/// An index store is rederived: the base round leaves it, the ephemeral round
/// publishes it at the generation it is handed.
#[test]
fn only_the_ephemeral_round_publishes_an_index() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    registry.register(table(70, make_schema_u64_i64())).unwrap();
    registry
        .add_index(70, index(999, false), PkColList::from_slice(&[1]))
        .unwrap();
    let dir = registry.child_dir(70, ChildKind::Index(PkColList::from_slice(&[1])));

    registry.checkpoint_base().unwrap();
    assert!(read_intact(&dir).unwrap().is_none(), "the base round skips it");
    registry.checkpoint_ephemeral([], 3, |_| true).unwrap();
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
    registry.checkpoint_ephemeral([], 3, |_| true).unwrap();

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

    let records: Vec<(u64, (u64, Vec<u8>))> = registry
        .persisted_records()
        .unwrap()
        .into_iter()
        .map(|(id, r)| (id, r.unwrap()))
        .collect();
    assert_eq!(records, vec![(good, (3, b"good".to_vec()))]);
}

/// An index reopened beside its owner resumes from the manifest the owner's
/// checkpoint published it with, and is filled from the owner when its own
/// manifest is of another generation or gone. Either way it holds the owner's
/// entries.
#[test]
fn a_reopened_index_resumes_only_at_its_owners_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let spec = RelationSpec {
        kind: RelationKind::View(ViewProps::Plain),
        ..table(9, schema)
    };
    let cols = PkColList::from_slice(&[1]);
    let rows = make_batch_raw(&schema, &[(1, 1, 30), (2, 1, 10), (3, 1, 20)]);
    {
        let mut origin = solo(tmp.path());
        origin.register(spec).unwrap();
        origin.add_index(9, index(1, false), cols).unwrap();
        origin.ingest(9, rows.clone()).unwrap();
        origin.checkpoint_ephemeral([], 5, |_| true).unwrap();
    }
    let reopen = |index_generation: u64| {
        let mut r = solo(tmp.path());
        r.reopen_view(spec, 5).unwrap();
        r.reopen_index(9, index(1, false), cols, index_generation).unwrap();
        let ix = r.relation(9).unwrap().index_on(&[1]).unwrap();
        let want = gnitz_zset::algebra::index_entries(&rows, &ix.key_spec(), &ix.schema());
        assert_eq!(
            zset_of(&ix.cursor().materialize(), &ix.schema()),
            zset_of(&want, &ix.schema()),
            "generation {index_generation}"
        );
        ix.resumed()
    };
    assert!(reopen(5), "published with its owner");
    assert!(!reopen(4), "of another generation, so filled from the owner");
    assert!(!reopen(5), "the refused manifest is gone");
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
        origin.checkpoint_ephemeral([(7, &mut state)], 0, |_| true).unwrap();
    }
    let reopen = || {
        let mut r = solo(tmp.path());
        r.reopen_view(spec, 0).map(|()| r)
    };
    let trace = |r: &RelationRegistry| CircuitState::open(r, 7, layout()).map(drop);
    let worker = |resume: bool| {
        let mut master = RelationRegistry::master(base, 1, StoreConfig::default());
        master.register(spec).unwrap();
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
    // Last: a rebuild erases the traces.
    worker(false).expect("told to rebuild, a worker opens the view");
}

/// A `(a, b | v)` table read by its leading key column: a gather and a cursor
/// both take `a` alone and answer every row under each key they are given, and
/// with the un-ticked ingests handed in, the rows as they stood before those —
/// a replaced row at its old value, a removed one present, a new one absent.
#[test]
fn a_table_is_read_by_a_key_prefix_as_it_stood_before_its_unticked_ingests() {
    use gnitz_zset::repr::BatchBuilder;
    use gnitz_zset::schema::SchemaColumn;
    let col = SchemaColumn::new(TypeCode::U64, false);
    let schema = SchemaDescriptor::new(&[col, col, SchemaColumn::new(TypeCode::I64, false)], &[0, 1]);
    let rows = |rows: &[(u64, u64, i64, i64)]| {
        let mut b = BatchBuilder::new(&schema);
        for &(a, k, w, v) in rows {
            b.begin_row_natives(&[a as u128, k as u128], w);
            b.put_int(v as u128);
            b.end_row();
        }
        b.finish()
    };
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    registry.register(table(50, schema)).unwrap();
    let before = [
        (1, 1, 1, 10),
        (1, 2, 1, 20),
        (2, 1, 1, 30),
        (3, 7, 1, 40),
        (5, 1, 1, 50),
    ];
    registry.ingest(50, rows(&before)).unwrap();
    // (1, 2) replaced, (3, 7) removed, (2, 9) and (4, 4) new.
    let pushed = rows(&[(1, 2, 1, 21), (3, 7, -1, 40), (2, 9, 1, 60), (4, 4, 1, 70)]);
    registry.ingest_pending(50, pushed).unwrap();
    let now = [
        (1, 1, 1, 10),
        (1, 2, 1, 21),
        (2, 1, 1, 30),
        (2, 9, 1, 60),
        (4, 4, 1, 70),
        (5, 1, 1, 50),
    ];
    let under = |all: &[(u64, u64, i64, i64)], keys: &[u64]| {
        let kept: Vec<_> = all.iter().copied().filter(|r| keys.contains(&r.0)).collect();
        zset_of(&rows(&kept), &schema)
    };

    let check = |registry: &RelationRegistry, state: &[(u64, u64, i64, i64)], cut: Cut, what: &str| {
        let relation = registry.relation(50).unwrap();
        let keys = [1u64, 3, 4];
        let pk_keys = PkKeys::from_keys(
            8,
            keys.iter()
                .map(|k| k.to_be_bytes())
                .collect::<Vec<_>>()
                .iter()
                .map(|k| &k[..]),
        );
        let mut gather = relation.gather(pk_keys, cut);
        let mut gathered = std::collections::HashMap::new();
        while let Some(chunk) = gather.drain_chunk(2) {
            gathered.extend(zset_of(&chunk, &schema));
        }
        assert_eq!(gathered, under(state, &keys), "gather, {cut:?}, {what}");

        // Positioned on the first key's first row and exact at each key: the
        // rows of every other key are a walk's to skip.
        let mut probe = BatchBuilder::new(&SchemaDescriptor::new(&[col], &[0]));
        for k in keys {
            probe.begin_row_natives(&[k as u128], 1);
            probe.end_row();
        }
        let cursor = relation.cursor_for_keys(&probe.finish().into_consolidated(), cut);
        let mut read = zset_of(&cursor.materialize(), &schema);
        read.retain(|row, _| keys.iter().any(|k| row.0[..8] == k.to_be_bytes()));
        assert_eq!(read, under(state, &keys), "cursor, {cut:?}, {what}");
        assert_eq!(
            zset_of(&relation.full_scan(), &schema),
            zset_of(&rows(&now), &schema),
            "{what}"
        );
    };
    check(&registry, &now, Cut::Now, "pending in RAM");
    check(&registry, &before, Cut::Sealed, "pending in RAM");

    // A flush with rows above the cut makes them durable and leaves the cut.
    registry.checkpoint_base().unwrap();
    check(&registry, &now, Cut::Now, "pending in a shard");
    check(&registry, &before, Cut::Sealed, "pending in a shard");

    // A reopen finds the flushed rows whole, with nothing above the cut.
    {
        let mut reopened = solo(tmp.path());
        reopened.register(table(50, schema)).unwrap();
        check(&reopened, &now, Cut::Now, "reopened");
        check(&reopened, &now, Cut::Sealed, "reopened");
        assert!(reopened.seal(50).unwrap().is_none());
    }

    // More above the cut, beside the flushed ones: (5, 1) removed.
    registry.ingest_pending(50, rows(&[(5, 1, -1, 50)])).unwrap();
    let now = [
        (1, 1, 1, 10),
        (1, 2, 1, 21),
        (2, 1, 1, 30),
        (2, 9, 1, 60),
        (4, 4, 1, 70),
    ];
    let delta = registry.seal(50).unwrap().expect("rows sat above the cut");
    let want = [
        (1, 2, -1, 20),
        (1, 2, 1, 21),
        (3, 7, -1, 40),
        (2, 9, 1, 60),
        (4, 4, 1, 70),
        (5, 1, -1, 50),
    ];
    assert_eq!(zset_of(&delta, &schema), zset_of(&rows(&want), &schema));
    assert!(delta.is_consolidated());
    assert!(registry.seal(50).unwrap().is_none(), "a seal drains the pending rows");
    let relation = registry.relation(50).unwrap();
    for cut in [Cut::Now, Cut::Sealed] {
        let pk_keys = PkKeys::from_keys(8, [&1u64.to_be_bytes()[..], &5u64.to_be_bytes()[..]]);
        let mut gathered = std::collections::HashMap::new();
        let mut gather = relation.gather(pk_keys, cut);
        while let Some(chunk) = gather.drain_chunk(2) {
            gathered.extend(zset_of(&chunk, &schema));
        }
        assert_eq!(gathered, under(&now, &[1, 5]), "after the seal, {cut:?}");
    }
    assert_eq!(zset_of(&relation.full_scan(), &schema), zset_of(&rows(&now), &schema));
}
