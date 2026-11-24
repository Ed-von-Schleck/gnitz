use super::*;
use crate::relation::{IndexClaim, StateLayout, StoreConfig};
use crate::storage::flush_barrier;
use crate::test_support::{
    TID, batch_rows, flip_last_byte_in_place, make_batch, make_schema_u64_i64, pk_only_schema, relation_fixture_with,
    scratch_table, solo, spec, spilling,
};
use gnitz_wire::{TypeCode, ViewBudget};

#[test]
fn parse_inverts_name_for_every_grammar() {
    for slot in [Slot::SOLO, Slot::new(2, 3), Slot::new(3, 64)] {
        for kind in [
            ChildKind::Rows,
            ChildKind::Scratch("_reduce_9_3"),
            ChildKind::Scratch("agg_w3"),
            ChildKind::Index(PkColList::from_slice(&[7])),
            ChildKind::Index(PkColList::from_slice(&[1, 3])),
        ] {
            let addr = ChildAddr { kind, slot };
            let name = addr.name();
            assert_eq!(ChildAddr::parse(&name), Some(addr), "round-trip of {name}");
        }
    }
}

#[test]
fn parse_rejects_names_in_no_grammar() {
    for name in [
        "foo",
        "foo_w0of1",
        "w3",
        "w3of",
        "w+1of2",
        "w01of2",
        "w0of0",
        "scratch__w0of1x",
        "idx_x_w0of1",
        "idx_07_w0of1",
        "idx_1-_w0of1",
        "idx_1-1_w0of1",
        "manifest.bin",
    ] {
        assert_eq!(ChildAddr::parse(name), None, "{name} is not a child dir");
    }
}

/// Publish an empty store at `dir` under checkpoint mark `generation`.
fn stamp(dir: &str, generation: u64) {
    flush_barrier([&mut scratch_table(dir, make_schema_u64_i64())], generation).unwrap();
}

/// A view's children are resumable once every rank's rows child and every
/// declared trace child carry a manifest at the generation, whatever else the
/// directory holds; an unregistered id is not.
#[test]
fn a_view_resumes_when_every_rank_published_its_declared_children() {
    const G: u64 = 5;
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_str().unwrap();
    let rel = relation_dir(base, TID);
    let layout = |schema| {
        let mut traces = StateLayout::default();
        traces.declare("agg".to_string(), schema);
        traces
    };
    // A master at 2 workers holding the view, its trace declared under `schema`.
    let resumable = |schema| {
        let mut master = RelationRegistry::master(base, 2, StoreConfig::default());
        let view = spec(TID, RelationKind::View(ViewBudget::Plain), make_schema_u64_i64());
        master
            .register_at(view, make_schema_u64_i64(), &[], layout(schema), None)
            .unwrap();
        assert!(!master.view_children_resumable(TID + 1, G), "an unregistered id");
        master.view_children_resumable(TID, G)
    };
    let traced = make_schema_u64_i64();
    assert!(!resumable(traced), "an absent rank is a mismatch");

    let agg = layout(traced).names().next().unwrap().to_string();
    let (agg0, agg1) = (format!("scratch_{agg}_w0of2"), format!("scratch_{agg}_w1of2"));
    for name in ["w0of2", "w1of2", &agg0, &agg1] {
        stamp(&format!("{rel}/{name}"), G);
    }
    assert!(resumable(traced));
    assert!(
        !resumable(pk_only_schema(&[TypeCode::U64])),
        "a trace published under another schema is a mismatch"
    );

    for name in ["idx_7_w0of2", "w0of4", "scratch_old_w0of2", "not_a_child"] {
        std::fs::create_dir_all(format!("{rel}/{name}")).unwrap();
    }
    assert!(resumable(traced), "undeclared children are not read");

    stamp(&format!("{rel}/{agg1}"), G - 1);
    assert!(!resumable(traced), "a stale scratch child is a mismatch");
}

/// What a store published reaches a second directory whole: a registry opened
/// over it reads every row the first held at its last checkpoint, and none
/// pushed after. A store with no manifest, as an index after a base round, is
/// not put.
#[test]
fn a_published_store_put_elsewhere_opens_with_the_checkpointed_rows() {
    let schema = make_schema_u64_i64();
    let rows: Vec<_> = (0..50).map(|k| (k, 1, k as i64)).collect();
    // Spilling, so the store put is several shards.
    let mut r = relation_fixture_with(
        spilling(),
        RelationKind::BaseTable,
        schema,
        &[&[1]],
        [make_batch(&schema, &rows)],
    );
    r.checkpoint_base().unwrap();
    // After the checkpoint, so only the SAL would carry it: not in the copy.
    r.ingest(TID, make_batch(&schema, &[(99, 1, 0)])).unwrap();

    let dest = tempfile::tempdir().unwrap();
    let dest = dest.path().to_str().unwrap();
    r.link_published_into(dest).unwrap();
    let index = solo(dest).child_dir(TID, ChildKind::Index(PkColList::from_slice(&[1])));
    assert!(!std::path::Path::new(&index).exists());

    let mut copy = RelationRegistry::new(dest, Slot::SOLO, spilling());
    copy.register(spec(TID, RelationKind::BaseTable, schema), schema)
        .unwrap();
    assert_eq!(batch_rows(&copy.relation(TID).unwrap().full_scan()), rows);
}

/// Only a directory named exactly as a relation's is read, and a damaged
/// manifest holds no record.
#[test]
fn persisted_records_reads_well_formed_relation_manifests() {
    let tmp = tempfile::tempdir().unwrap();
    let mut registry = solo(tmp.path());
    let (good, damaged) = (123, 124);
    let view = |id| spec(id, RelationKind::View(ViewBudget::Plain), make_schema_u64_i64());
    registry.register(view(good), make_schema_u64_i64()).unwrap();
    registry.register(view(damaged), make_schema_u64_i64()).unwrap();
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

/// A sweep removes every relation directory no relation holds, and every child
/// of a held one laid out for another worker count, an index no circuit covers
/// or one that reads its owner's PK, or a trace none declares; it leaves the
/// rest, a name in no child grammar included, and a second sweep finds nothing
/// to remove.
#[test]
fn a_sweep_reclaims_exactly_what_no_relation_owns() {
    let schema = make_schema_u64_i64();
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_str().unwrap();
    // Rank 0 of 3, holding a view indexed on its payload column and on its PK,
    // with one trace.
    let mut traces = StateLayout::default();
    traces.declare("agg".to_string(), schema);
    let agg = traces.names().next().unwrap().to_string();
    let mut r = RelationRegistry::new(base, Slot::new(0, 3), StoreConfig::default());
    let view = spec(TID, RelationKind::View(ViewBudget::Plain), schema);
    let indexes = [1, 0].map(|col| {
        let claim = IndexClaim::Index { id: TID + 1 + col as u64, unique: false };
        (claim, PkColList::from_slice(&[col]))
    });
    r.register_at(view, schema, &indexes, traces, None).unwrap();

    let rel = relation_dir(base, TID);
    let unowned = [
        format!("{}/ghost_{TID}", relations_dir(base)),
        relation_dir(base, TID + 50),
        format!("{rel}/w0of2"),
        format!("{rel}/idx_0_w0of3"),
        format!("{rel}/idx_0-1_w0of3"),
        format!("{rel}/scratch_old_w0of3"),
    ];
    let kept = [
        format!("{rel}/w0of3"),
        format!("{rel}/idx_1_w0of3"),
        format!("{rel}/scratch_{agg}_w2of3"),
        format!("{rel}/data_keep"),
    ];
    for dir in unowned.iter().chain(&kept) {
        std::fs::create_dir_all(dir).unwrap();
    }
    for _ in 0..2 {
        r.reclaim_orphan_relation_dirs().unwrap();
        for dir in &unowned {
            assert!(!std::path::Path::new(dir).exists(), "{dir} is reclaimed");
        }
        for dir in &kept {
            assert!(std::path::Path::new(dir).exists(), "{dir} is kept");
        }
    }
}

/// Erasing a relation drops its entry and its whole directory; an unknown id
/// erases nothing.
#[test]
fn unregister_and_erase_removes_the_relation_directory() {
    let schema = make_schema_u64_i64();
    let mut r = relation_fixture_with(
        StoreConfig::default(),
        RelationKind::BaseTable,
        schema,
        &[&[1]],
        [make_batch(&schema, &[(1, 1, 7)])],
    );
    r.checkpoint_base().unwrap();
    r.checkpoint_ephemeral([], 1, |_| true).unwrap();
    let dir = relation_dir(r.base_dir(), TID);
    r.unregister_and_erase(TID + 1).unwrap();
    assert!(std::path::Path::new(&dir).is_dir());
    r.unregister_and_erase(TID).unwrap();
    assert!(r.relation(TID).is_none());
    assert!(!std::path::Path::new(&dir).exists());
}

/// A data directory whose ancestors are missing is created whole.
#[test]
fn lock_data_dir_creates_missing_ancestors() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("a/b/data");
    let base = base.to_str().unwrap();
    let _lock = lock_data_dir(base).unwrap();
    assert!(std::path::Path::new(&relations_dir(base)).is_dir());
}
