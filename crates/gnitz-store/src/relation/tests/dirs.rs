use super::*;
use crate::relation::{IndexClaim, StateLayout, StoreConfig};
use crate::storage::flush_barrier;
use crate::test_support::{
    flip_last_byte_in_place, make_batch_raw, make_schema_u64_i64, pk_only_schema, relation_fixture_with, scratch_table,
    solo, spec, spilling, zset_of, TID,
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
        "t_12",
        "w3",
        "wof",
        "w3of",
        "wxof4",
        "w3ofx",
        "scratch_x_wq",
        "scratch_x_w3",
        "scratch__w0of1x",
        "idx_",
        "idx_7",
        "idx_x_w0of1",
        "idx_-3_w0of1",
        "idx_07_w0of1",
        "idx_1-_w0of1",
        "idx_1-1_w0of1",
        "idx_-1_w0of1",
        "delta_w",
        "delta_wx",
        "delta_w0",
        "delta_0",
        "w0of0",
        "w3of3",
        "w01of2",
        "manifest.bin",
    ] {
        assert_eq!(ChildAddr::parse(name), None, "{name} is not a child dir");
    }
}

/// Publish an empty store at `dir` under checkpoint mark `generation`.
fn stamp(dir: &str, generation: u64) {
    flush_barrier([&mut scratch_table(dir, make_schema_u64_i64())], generation).unwrap();
}

/// The verdict reads every rank's rows child and every declared trace child,
/// whatever else the directory holds.
#[test]
fn children_at_generation_reads_the_rows_and_the_declared_scratch() {
    const G: u64 = 5;
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap().to_string();
    let mut traces = StateLayout::default();
    traces.declare("agg".to_string(), make_schema_u64_i64());
    let agg = traces.names().next().unwrap().to_string();
    assert!(
        !children_at_generation(&dir, 2, G, &traces),
        "an absent rank is a mismatch"
    );

    let (agg0, agg1) = (format!("scratch_{agg}_w0of2"), format!("scratch_{agg}_w1of2"));
    for name in ["w0of2", "w1of2", &agg0, &agg1] {
        stamp(&format!("{dir}/{name}"), G);
    }
    assert!(children_at_generation(&dir, 2, G, &traces));

    let mut reshaped = StateLayout::default();
    reshaped.declare("agg".to_string(), pk_only_schema(&[TypeCode::U64]));
    assert!(
        !children_at_generation(&dir, 2, G, &reshaped),
        "a trace published under another schema is a mismatch"
    );

    for name in ["idx_7_w0of2", "w0of4", "scratch_old_w0of2", "not_a_child"] {
        std::fs::create_dir_all(format!("{dir}/{name}")).unwrap();
    }
    assert!(
        children_at_generation(&dir, 2, G, &traces),
        "undeclared children are not read"
    );

    stamp(&format!("{dir}/{agg1}"), G - 1);
    assert!(
        !children_at_generation(&dir, 2, G, &traces),
        "a stale scratch child is a mismatch"
    );

    std::fs::remove_dir_all(format!("{dir}/{agg1}")).unwrap();
    assert!(
        !children_at_generation(&dir, 2, G, &traces),
        "a declared scratch child with no directory is a mismatch"
    );
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
        Slot::SOLO,
        RelationKind::BaseTable,
        schema,
        &[&[1]],
        [make_batch_raw(&schema, &rows)],
    );
    r.checkpoint_base().unwrap();
    // After the checkpoint, so only the SAL would carry it: not in the copy.
    r.ingest(TID, make_batch_raw(&schema, &[(99, 1, 0)])).unwrap();

    let dest = tempfile::tempdir().unwrap();
    let dest = dest.path().to_str().unwrap();
    r.link_published_into(dest).unwrap();
    let index = ChildAddr {
        kind: ChildKind::Index(PkColList::from_slice(&[1])),
        slot: Slot::SOLO,
    };
    assert!(!std::path::Path::new(&index.dir(&relation_dir(dest, TID))).exists());

    let mut copy = RelationRegistry::new(dest, Slot::SOLO, spilling());
    copy.register(spec(TID, RelationKind::BaseTable, schema), schema)
        .unwrap();
    assert_eq!(
        zset_of(&copy.relation(TID).unwrap().full_scan(), &schema),
        zset_of(&make_batch_raw(&schema, &rows), &schema)
    );
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
/// or a trace none declares; it leaves the rest, a name in no child grammar
/// included, and a second sweep finds nothing to remove.
#[test]
fn a_sweep_reclaims_exactly_what_no_relation_owns() {
    let schema = make_schema_u64_i64();
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_str().unwrap();
    // Rank 0 of 3, holding a table indexed on its payload column.
    let mut r = RelationRegistry::new(base, Slot::new(0, 3), StoreConfig::default());
    r.register(spec(TID, RelationKind::BaseTable, schema), schema).unwrap();
    r.add_indexes(
        TID,
        &[(
            IndexClaim::Index { id: TID + 1, unique: false },
            PkColList::from_slice(&[1]),
        )],
    )
    .unwrap();
    let mut traces = StateLayout::default();
    traces.declare("agg".to_string(), schema);
    let agg = traces.names().next().unwrap().to_string();
    r.ingest(TID, make_batch_raw(&schema, &[(1, 1, 7)])).unwrap();
    r.checkpoint_base().unwrap();

    let rel = relation_dir(base, TID);
    let unowned = [
        format!("{}/ghost_{TID}", relations_dir(base)),
        relation_dir(base, TID + 50),
        format!("{rel}/w0of2"),
        format!("{rel}/w1of2"),
        format!("{rel}/scratch_{agg}_w1of2"),
        format!("{rel}/idx_7_w0of2"),
        format!("{rel}/idx_0-1_w0of3"),
        format!("{rel}/scratch_old_w0of3"),
        format!("{rel}/scratch_agg_w0of3"),
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
        r.reclaim_orphan_relation_dirs(|id| (id == TID).then_some(&traces))
            .unwrap();
        for dir in &unowned {
            assert!(!std::path::Path::new(dir).exists(), "{dir} is reclaimed");
        }
        for dir in &kept {
            assert!(std::path::Path::new(dir).exists(), "{dir} is kept");
        }
    }
    assert_eq!(
        zset_of(&r.relation(TID).unwrap().full_scan(), &schema),
        zset_of(&make_batch_raw(&schema, &[(1, 1, 7)]), &schema)
    );
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
