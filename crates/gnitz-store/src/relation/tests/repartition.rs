//! A relayout: which child set a boot relays onto the launched worker count,
//! what the target holds and how its shards are cut, and what is left behind.

use std::collections::HashMap;
use std::path::Path;

use super::*;
use crate::relation::{relation_dir, RelationKind, RelationRegistry, RelationSpec, StoreConfig};
use crate::storage::{read_intact, shard_files, DEFAULT_RAM_TIER_BYTES};
use crate::test_support::{
    flip_last_byte_in_place, make_batch_opk, make_schema_u64_i64, new_table, opk_pk, pk_payload_schema, spec,
    table_shards, zset_of, RowKey,
};
use gnitz_wire::TypeCode;

/// The base table every boot below holds.
const ID: u64 = 7;

/// `(OPK, payload)`, each row at weight 1.
type Rows = [(Vec<u8>, i64)];

fn child(k: u32, of: u32) -> ChildAddr<'static> {
    ChildAddr {
        kind: ChildKind::Rows,
        slot: Slot::new(k, of),
    }
}

/// `rel`'s `w{k}of{of}` child, opened as the durable ingest path opens one.
fn open_child(rel: &str, schema: SchemaDescriptor, k: u32, of: u32) -> Table {
    new_table(
        child(k, of).dir(rel),
        schema,
        RecoverySource::SalReplay,
        DEFAULT_RAM_TIER_BYTES,
    )
}

/// Keys `0..n` of [`make_schema_u64_i64`], payload `-pk`.
fn ids(n: u64) -> Vec<(Vec<u8>, i64)> {
    let schema = make_schema_u64_i64();
    (0..n)
        .map(|pk| (opk_pk(&schema, &[pk as u128]), -(pk as i64)))
        .collect()
}

/// The rows of `rows` worker `k` of `of` holds under `placement`.
fn owned(schema: SchemaDescriptor, placement: Placement, rows: &Rows, k: u32, of: u32) -> Batch {
    let kept: Vec<(&[u8], i64, i64)> = rows
        .iter()
        .filter(|(pk, _)| placement.owner(pk, of as usize).is_none_or(|o| o == k as usize))
        .map(|(pk, v)| (&pk[..], 1, *v))
        .collect();
    make_batch_opk(&schema, &kept)
}

/// Write child `k` of `rel`'s `of`-worker set afresh, holding its share of
/// `rows`, and publish it — empty too, as a checkpoint's base round does.
fn seed_child(rel: &str, schema: SchemaDescriptor, placement: Placement, rows: &Rows, k: u32, of: u32) {
    std::fs::remove_dir_all(child(k, of).dir(rel)).ok();
    let mut t = open_child(rel, schema, k, of);
    t.ingest(owned(schema, placement, rows, k, of)).unwrap();
    flush_barrier([&mut t], 0).unwrap();
}

/// [`seed_child`] for every child of the `of`-worker set.
fn seed_set(rel: &str, schema: SchemaDescriptor, placement: Placement, rows: &Rows, of: u32) {
    (0..of).for_each(|k| seed_child(rel, schema, placement, rows, k, of));
}

/// Each child's Z-set across `rel`'s `of`-worker set.
fn set_of(rel: &str, schema: SchemaDescriptor, of: u32) -> Vec<HashMap<RowKey, i64>> {
    (0..of)
        .map(|k| zset_of(&open_child(rel, schema, k, of).full_scan(), &schema))
        .collect()
}

/// What `rows` reads back as across an `of`-worker set.
fn expected(schema: SchemaDescriptor, placement: Placement, rows: &Rows, of: u32) -> Vec<HashMap<RowKey, i64>> {
    (0..of)
        .map(|k| zset_of(&owned(schema, placement, rows, k, of), &schema))
        .collect()
}

/// A boot at `of` workers over `base` holding [`ID`]: the master relays its set
/// onto `of`, then sweeps.
fn boot(base: &str, of: u32, schema: SchemaDescriptor, placement: Placement) -> Result<(), String> {
    let mut master = RelationRegistry::master(base, of, StoreConfig::default());
    master
        .register(
            RelationSpec {
                placement,
                ..spec(ID, RelationKind::BaseTable, schema)
            },
            schema,
        )
        .unwrap();
    master.reconcile_child_dirs()
}

/// Whether any child of `rel`'s `of`-worker set holds a manifest.
fn any_published(rel: &str, of: u32) -> bool {
    (0..of).any(|k| Path::new(&child(k, of).manifest(rel)).exists())
}

/// A fresh data directory, and [`ID`]'s relation directory under it.
fn scratch_relation() -> (tempfile::TempDir, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_str().unwrap().to_string();
    let rel = relation_dir(&base, ID);
    (dir, base, rel)
}

/// A relay config whose 4 KiB RAM tier each target passes only after several
/// 64-row chunks.
fn relay_config() -> StoreConfig {
    StoreConfig {
        ram_tier_bytes: 4096,
        scan_chunk_rows: 64,
        ..StoreConfig::default()
    }
}

/// A relayout cuts each target into filtered terminal runs and moves every row
/// once, onto its owner, payload intact.
#[test]
fn a_relayout_writes_filtered_terminal_runs() {
    let (_dir, _, rel) = scratch_relation();
    let rel = rel.as_str();
    let schema = make_schema_u64_i64();
    let placement = Placement::full_pk(&schema);

    let rows = ids(400);
    seed_set(rel, schema, placement, &rows, 1);
    repartition_relation(rel, &schema, placement, 2, &relay_config()).unwrap();

    for k in 0..2 {
        let t = open_child(rel, schema, k, 2);
        let shards = table_shards(&t);
        assert!(shards.len() > 1, "child {k} cut {} run(s)", shards.len());
        assert!(
            shards.iter().all(|s| s.has_pk_filter()),
            "child {k}: every run carries a PK filter"
        );
        assert_eq!(t.level_shape(), (0, [0, shards.len()]), "child {k}: terminal placement");
    }
    assert_eq!(set_of(rel, schema, 2), expected(schema, placement, &rows, 2));
    assert!(!Path::new(&child(0, 1).dir(rel)).exists(), "the source set is removed");
}

#[test]
fn a_corrupt_source_body_fails_the_relayout_and_keeps_the_source_set() {
    let (_dir, _, rel) = scratch_relation();
    let rel = rel.as_str();
    let schema = make_schema_u64_i64();
    let placement = Placement::full_pk(&schema);
    seed_set(rel, schema, placement, &ids(100), 1);

    let source = child(0, 1).dir(rel);
    let manifest = read_intact(&source).unwrap().unwrap();
    let [(shard, Some(_))] = &shard_files(&source).unwrap()[..] else {
        panic!("the seeded set flushed one published shard")
    };
    flip_last_byte_in_place(shard);

    assert!(matches!(
        repartition_relation(rel, &schema, placement, 2, &relay_config()),
        Err(e) if e.contains("corrupt: body checksum")
    ));
    assert!(Path::new(shard).exists(), "the source shard survives");
    assert_eq!(
        read_intact(&source).unwrap(),
        Some(manifest),
        "the source manifest survives"
    );
    assert!(!any_published(rel, 2), "no target is published");
}

/// Over a grid of `(old, new)` worker counts, every row comes back at its exact
/// weight on its owner at `new`, and the old set is gone.
#[test]
fn a_keyed_table_round_trips_across_worker_counts() {
    let schema = make_schema_u64_i64();
    let placement = Placement::full_pk(&schema);
    let rows = ids(300);
    for (old, new) in [(1, 1), (1, 4), (4, 1), (2, 3), (3, 2), (4, 5), (5, 8), (8, 4)] {
        let (_dir, base, rel) = scratch_relation();
        seed_set(&rel, schema, placement, &rows, old);
        boot(&base, new, schema, placement).unwrap();
        assert_eq!(
            set_of(&rel, schema, new),
            expected(schema, placement, &rows, new),
            "{old}->{new}"
        );
        if old != new {
            assert!(
                !Path::new(&child(0, old).dir(&rel)).exists(),
                "{old}->{new}: the source set is removed"
            );
        }
    }
}

/// A replicated table's children are copies of one another, so a relayout
/// hard-links rank 0's files into every launched rank and removes the old set.
#[test]
fn a_replicated_table_is_relinked_at_a_new_worker_count() {
    let (_dir, base, rel) = scratch_relation();
    let schema = make_schema_u64_i64();
    let rows = ids(10);
    seed_set(&rel, schema, Placement::Replicated, &rows, 1);
    let files = |k, of| {
        let mut v: Vec<_> = std::fs::read_dir(child(k, of).dir(&rel))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        v.sort();
        v
    };
    let source_files = files(0, 1);

    boot(&base, 3, schema, Placement::Replicated).unwrap();
    for k in 0..3 {
        assert_eq!(files(k, 3), source_files, "rank {k} holds a copy");
    }
    assert_eq!(
        set_of(&rel, schema, 3),
        expected(schema, Placement::Replicated, &rows, 3)
    );
    assert!(!Path::new(&child(0, 1).dir(&rel)).exists(), "the source set is removed");
}

/// Rows that all share a table's distribution prefix sit on one worker, so the
/// other children are empty: published anyway, they complete the set, and it
/// relays.
#[test]
fn a_set_of_empty_children_relays() {
    let (_dir, base, rel) = scratch_relation();
    let schema = pk_payload_schema(&[TypeCode::U64, TypeCode::U64]);
    let placement = Placement::keyed(&schema, 1);
    let rows: Vec<_> = (0..20).map(|b| (opk_pk(&schema, &[1, b]), b as i64)).collect();
    seed_set(&rel, schema, placement, &rows, 3);
    let occupied = set_of(&rel, schema, 3).iter().filter(|z| !z.is_empty()).count();
    assert_eq!(occupied, 1, "one prefix, one worker");

    boot(&base, 2, schema, placement).unwrap();
    assert_eq!(set_of(&rel, schema, 2), expected(schema, placement, &rows, 2));
}

/// Two complete sets, as a crash between a relay's target publish and its
/// source's removal leaves, hold the same rows: either relays, and both go.
#[test]
fn either_of_two_complete_sets_relays() {
    let (_dir, base, rel) = scratch_relation();
    let schema = make_schema_u64_i64();
    let placement = Placement::full_pk(&schema);
    let rows = ids(40);
    seed_set(&rel, schema, placement, &rows, 2);
    seed_set(&rel, schema, placement, &rows, 4);

    boot(&base, 3, schema, placement).unwrap();
    assert_eq!(set_of(&rel, schema, 3), expected(schema, placement, &rows, 3));
    assert!(!any_published(&rel, 2) && !any_published(&rel, 4));
}

/// A partial target at the launched count beside a complete source is what a
/// relay that crashed part-way leaves: it is relaid over, not accepted as
/// current nor merged into — merged, every row it holds would reach weight 2.
#[test]
fn a_partial_target_is_relaid_over() {
    let schema = make_schema_u64_i64();
    let rows = ids(40);
    for placement in [Placement::full_pk(&schema), Placement::Replicated] {
        let (_dir, base, rel) = scratch_relation();
        seed_set(&rel, schema, placement, &rows, 2);
        seed_child(&rel, schema, placement, &rows, 0, 4);

        boot(&base, 4, schema, placement).unwrap();
        assert_eq!(
            set_of(&rel, schema, 4),
            expected(schema, placement, &rows, 4),
            "{placement:?}"
        );
        assert!(!any_published(&rel, 2), "{placement:?}: the source set is removed");
    }
}

/// A set at the launched count missing a rank's manifest is what a boot that
/// aborted between two workers' publishes leaves: nothing moved, and it stays
/// exactly as it was.
#[test]
fn a_torn_set_at_the_launched_count_is_left_as_it_is() {
    let (_dir, base, rel) = scratch_relation();
    let schema = make_schema_u64_i64();
    let placement = Placement::full_pk(&schema);
    seed_set(&rel, schema, placement, &ids(60), 3);
    std::fs::remove_file(child(2, 3).manifest(&rel)).unwrap();
    let published = set_of(&rel, schema, 3);

    boot(&base, 3, schema, placement).unwrap();
    assert_eq!(set_of(&rel, schema, 3), published);
}

/// A foreign-count set missing a rank's manifest — torn, or never checkpointed
/// at all — still has its rows in the SAL tail: nothing moves, and the sweep
/// removes it.
#[test]
fn a_torn_foreign_set_moves_nothing() {
    let schema = make_schema_u64_i64();
    for placement in [Placement::full_pk(&schema), Placement::Replicated] {
        let (_dir, base, rel) = scratch_relation();
        seed_set(&rel, schema, placement, &ids(40), 4);
        std::fs::remove_file(child(3, 4).manifest(&rel)).unwrap();

        boot(&base, 2, schema, placement).unwrap();
        assert!(!any_published(&rel, 2), "{placement:?}: nothing is relaid");
        assert!(
            !Path::new(&child(0, 4).dir(&rel)).exists(),
            "{placement:?}: the sweep removes it"
        );
    }
}

/// A complete launched set with a leftover source set beside it, as a crash
/// after a relay's target publish leaves: the boot keeps the target as it
/// stands and removes the source.
#[test]
fn a_complete_launched_set_is_kept_and_a_leftover_source_set_removed() {
    let (_dir, base, rel) = scratch_relation();
    let schema = make_schema_u64_i64();
    let placement = Placement::full_pk(&schema);
    seed_set(&rel, schema, placement, &ids(40), 1);
    boot(&base, 2, schema, placement).unwrap();
    // One row the target does not hold: relaying the set again would add it.
    seed_set(&rel, schema, placement, &ids(41), 1);

    boot(&base, 2, schema, placement).unwrap();
    assert_eq!(set_of(&rel, schema, 2), expected(schema, placement, &ids(40), 2));
    assert!(
        !Path::new(&child(0, 1).dir(&rel)).exists(),
        "the leftover source set is removed"
    );
}

/// A boot at the count the set is laid out for rewrites no file.
#[test]
fn an_unchanged_worker_count_moves_nothing() {
    let (_dir, base, rel) = scratch_relation();
    let schema = make_schema_u64_i64();
    let placement = Placement::full_pk(&schema);
    seed_set(&rel, schema, placement, &ids(50), 2);
    let stamps = || -> Vec<_> {
        (0..2)
            .flat_map(|k| std::fs::read_dir(child(k, 2).dir(&rel)).unwrap())
            .map(|e| {
                let e = e.unwrap();
                (e.path(), e.metadata().unwrap().modified().unwrap())
            })
            .collect()
    };
    let before = stamps();
    assert!(!before.is_empty());

    boot(&base, 2, schema, placement).unwrap();
    assert_eq!(stamps(), before);
}

/// A child directory in no grammar this build knows may hold rows nothing would
/// read or reclaim, so the boot refuses rather than read it as never
/// checkpointed.
#[test]
fn an_unknown_child_grammar_refuses_the_boot() {
    let (_dir, base, rel) = scratch_relation();
    std::fs::create_dir_all(format!("{rel}/part_7")).unwrap();
    std::fs::write(format!("{rel}/part_7/manifest.bin"), b"x").unwrap();
    let schema = make_schema_u64_i64();
    let err = boot(&base, 1, schema, Placement::full_pk(&schema)).unwrap_err();
    assert!(err.contains("part_7"), "{err}");
}
