//! A relayout: which child set a boot relays onto the launched worker count,
//! what the target holds and how its shards are cut, and what is left behind.

use std::collections::HashMap;
use std::path::Path;

use super::*;
use crate::relation::{RelationKind, RelationRegistry, RelationSpec, StoreConfig, relation_dir};
use crate::storage::{DEFAULT_RAM_TIER_BYTES, read_intact, shard_files};
use crate::test_support::{
    RowKey, flip_last_byte_in_place, make_batch_opk, make_schema_u64_i64, new_table, opk_pk, pk_payload_schema, spec,
    table_shards, zset_of,
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

/// What a boot leaves at the launched worker count.
enum Outcome {
    /// Every row of these, at its exact weight, on its owner.
    Relaid(Vec<(Vec<u8>, i64)>),
    /// Every file as it was.
    Unchanged,
    /// No child holds a manifest.
    NothingPublished,
}

/// One child set seeded before the boot: its worker count, its rows, a rank
/// whose manifest is then removed, and the one rank seeded if not all.
type Seed = (u32, Vec<(Vec<u8>, i64)>, Option<u32>, Option<u32>);

/// A boot's case: what it shows, the table, the sets seeded, the worker count
/// launched, what that leaves, and the worker counts whose sets must be gone.
type Case = (
    &'static str,
    SchemaDescriptor,
    Placement,
    Vec<Seed>,
    u32,
    Outcome,
    &'static [u32],
);

/// Which set a boot relays onto the launched count, and what it leaves: a
/// complete foreign set is relaid, a torn or partial one is not trusted, and a
/// set already at the launched count is kept as it stands.
#[test]
fn a_boot_relays_only_a_complete_set() {
    use Outcome::{NothingPublished, Relaid, Unchanged};
    let schema = make_schema_u64_i64();
    let full_pk = Placement::full_pk(&schema);
    let prefixed = pk_payload_schema(&[TypeCode::U64, TypeCode::U64]);
    let one_prefix: Vec<_> = (0..20).map(|b| (opk_pk(&prefixed, &[1, b]), b as i64)).collect();
    let set = |of, rows: &Vec<_>| (of, rows.clone(), None, None);
    let mut cases: Vec<Case> = vec![
        // One prefix, one worker: the other children are empty, published anyway.
        (
            "a set of empty children relays",
            prefixed,
            Placement::keyed(&prefixed, 1),
            vec![set(3, &one_prefix)],
            2,
            Relaid(one_prefix.clone()),
            &[3],
        ),
        // As a crash between a relay's target publish and its source's removal leaves.
        (
            "either of two complete sets relays",
            schema,
            full_pk,
            vec![set(2, &ids(40)), set(4, &ids(40))],
            3,
            Relaid(ids(40)),
            &[2, 4],
        ),
        // As a crash after a relay's target publish leaves; one row the target
        // does not hold, which relaying the source again would add.
        (
            "a complete launched set is kept",
            schema,
            full_pk,
            vec![set(2, &ids(40)), set(1, &ids(41))],
            2,
            Relaid(ids(40)),
            &[1],
        ),
        // As a boot that aborted between two workers' publishes leaves.
        (
            "a torn set at the launched count is left as it is",
            schema,
            full_pk,
            vec![(3, ids(60), Some(2), None)],
            3,
            Unchanged,
            &[],
        ),
        (
            "an unchanged worker count moves nothing",
            schema,
            full_pk,
            vec![set(2, &ids(50))],
            2,
            Unchanged,
            &[],
        ),
    ];
    for placement in [full_pk, Placement::Replicated] {
        // As a relay that crashed part-way leaves: merged into, every row it
        // holds would reach weight 2.
        cases.push((
            "a partial target is relaid over",
            schema,
            placement,
            vec![set(2, &ids(40)), (4, ids(40), None, Some(0))],
            4,
            Relaid(ids(40)),
            &[2],
        ));
        // Torn, or never checkpointed at all: its rows are still in the SAL tail.
        cases.push((
            "a torn foreign set moves nothing and is swept",
            schema,
            placement,
            vec![(4, ids(40), Some(3), None)],
            2,
            NothingPublished,
            &[4],
        ));
    }

    for (what, schema, placement, seeds, launched, outcome, gone) in cases {
        let what = format!("{what} ({placement:?})");
        let (_dir, base, rel) = scratch_relation();
        for (of, rows, torn, only) in &seeds {
            match only {
                Some(k) => seed_child(&rel, schema, placement, rows, *k, *of),
                None => seed_set(&rel, schema, placement, rows, *of),
            }
            if let Some(k) = torn {
                std::fs::remove_file(child(*k, *of).manifest(&rel)).unwrap();
            }
        }
        let stamps = || -> Vec<_> {
            (0..launched)
                .filter_map(|k| std::fs::read_dir(child(k, launched).dir(&rel)).ok())
                .flatten()
                .map(|e| {
                    let e = e.unwrap();
                    (e.path(), e.metadata().unwrap().modified().unwrap())
                })
                .collect()
        };
        let before = stamps();

        boot(&base, launched, schema, placement).unwrap();
        match outcome {
            Relaid(rows) => assert_eq!(
                set_of(&rel, schema, launched),
                expected(schema, placement, &rows, launched),
                "{what}"
            ),
            Unchanged => {
                assert!(!before.is_empty(), "{what}");
                assert_eq!(stamps(), before, "{what}");
            }
            NothingPublished => assert!(!any_published(&rel, launched), "{what}"),
        }
        for &of in gone {
            assert!(
                !Path::new(&child(0, of).dir(&rel)).exists(),
                "{what}: the {of}-worker set is removed"
            );
        }
    }
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
