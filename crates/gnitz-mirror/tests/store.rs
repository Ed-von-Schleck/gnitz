//! The store on its own: no server, no client, no connection.
//!
//! Every method is a statement about the copy it holds, so the store's lifecycle
//! is tested here: the registration and what it retracts, the teardown ladder,
//! the apply's ordering, and what a checkpoint makes durable. The acceptance
//! suite beside it (`mirror.rs`) drives a real client against a real server.
//!
//! Faults are real I/O faults, not seams: [`block_copy`] puts a regular file
//! where a copy's child directory belongs, so whatever writes there fails.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use gnitz_core::{DeltaCursor, Invalidate, MirrorError, MirrorStore, Schema, ZSetBatch};
use gnitz_mirror::Mirror;
use gnitz_store_testkit::{
    assert_child_ok, in_child_test, make_batch, make_schema_u64_i64, run_test_in_child, scratch_dir, CHILD_OK,
};
use gnitz_wire::{ColumnDef, TypeCode};
use gnitz_wire::{ReadBound, ReadSpec};

#[path = "support/common.rs"]
mod common;
use common::{has_copy, has_manifest, manifest_path};
use gnitz_store::relation::{relation_dir, ChildAddr, ChildKind};
use gnitz_store::schema::Slot;

const SCHEMA: &str = "s";
const TID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;
const OTHER_TID: u64 = gnitz_wire::FIRST_USER_TABLE_ID + 1;

/// The client-side shape every copy here is registered under: a `U64` PK and one
/// `I64` payload — the same layout [`make_schema_u64_i64`] describes on the
/// engine side, which is what lets a batch built there decode against a copy
/// registered from here.
fn view_schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef::new("id", TypeCode::U64, false),
            ColumnDef::new("v", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    }
}

fn cursor(tick: u64) -> DeltaCursor {
    DeltaCursor {
        tag: 0xA11CE,
        tick: NonZeroU64::new(tick).unwrap(),
    }
}

/// One wire block of the view's own rows, in the view's own schema — the shape
/// a bootstrap and a poll both reply in.
fn plain(rows: &[(u64, i64, i64)]) -> Vec<u8> {
    gnitz_store_testkit::encode_to_wire_vec(&make_batch(&make_schema_u64_i64(), rows))
}

/// A store on a fresh directory with `TID` registered under `s.v`.
fn registered(name: &str) -> (Mirror, String) {
    let dir = scratch_dir("mirror_store", name);
    let mut store = Mirror::open(&dir).expect("a fresh store opens");
    store
        .register(TID, SCHEMA, "v", &view_schema())
        .expect("a first registration");
    (store, dir)
}

/// A store holding two copies, `TID` under `s.v` and `OTHER_TID` under `s.w`,
/// one row each at round 4.
fn two_copies(name: &str) -> (Mirror, String) {
    let (mut store, dir) = registered(name);
    store.register(OTHER_TID, SCHEMA, "w", &view_schema()).unwrap();
    store.reseed(TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();
    store.reseed(OTHER_TID, &[&plain(&[(9, 1, 90)])], cursor(4)).unwrap();
    (store, dir)
}

/// [`two_copies`], checkpointed and closed. Returns the directory.
fn two_checkpointed_copies(name: &str) -> String {
    let (mut store, dir) = two_copies(name);
    store.checkpoint().unwrap();
    dir
}

/// `(pk, payload) → summed weight` for every row the copy holds.
///
/// **Weight-exact, because that is what correctness means here**: a row-set
/// comparison would accept a delta applied twice, which leaves the row set
/// identical and doubles every weight in the interval.
///
/// **Keyed by the payload as well as the PK**, which is the element identity
/// every store path uses. Folding on the PK alone would sum two rows that differ
/// only in payload onto one entry, and would let a payload the round-stamp strip
/// mangled pass unnoticed — the keys and their weights would still line up.
fn held(store: &mut Mirror, tid: u64) -> BTreeMap<(u64, i64), i64> {
    let batch = whole_copy(store, tid).expect("a scan of a held copy");
    let vals = &batch.payload[0].bytes;
    let mut out = BTreeMap::new();
    for row in 0..batch.weights.len() {
        let pk = batch.pks.get(&view_schema(), row) as u64;
        let val = i64::from_le_bytes(vals[row * 8..row * 8 + 8].try_into().unwrap());
        *out.entry((pk, val)).or_insert(0) += batch.weights[row];
    }
    out.retain(|_, w| *w != 0);
    out
}

/// Every row of the copy, through the unbounded spec `scan_local` sends.
fn whole_copy(store: &mut Mirror, tid: u64) -> Result<ZSetBatch, MirrorError> {
    store.scan_spec(tid, ReadSpec::all_rows(ReadBound::None), &view_schema())
}

/// Put a regular file where `tid`'s per-worker child directory belongs, moving
/// the directory aside: an open of the copy fails, and so does anything that
/// writes into it. [`unblock_copy`] undoes it.
fn block_copy(base_dir: &str, tid: u64) {
    let child = rows_dir(base_dir, tid);
    std::fs::rename(&child, aside(base_dir, tid)).expect("the copy's child directory");
    std::fs::write(&child, b"not a directory").expect("block the child path");
}

fn unblock_copy(base_dir: &str, tid: u64) {
    let child = rows_dir(base_dir, tid);
    std::fs::remove_file(&child).unwrap();
    std::fs::rename(aside(base_dir, tid), &child).unwrap();
}

fn rows_dir(base_dir: &str, tid: u64) -> String {
    ChildAddr { kind: ChildKind::Rows, slot: Slot::SOLO }.dir(&relation_dir(base_dir, tid))
}

fn aside(base_dir: &str, tid: u64) -> String {
    format!("{base_dir}/aside_{tid}")
}

/// A registration stands while the id and the layout do; a *different* id under
/// the same qualified name retracts the incumbent.
#[test]
fn a_registration_stands_and_a_moved_name_retracts_the_incumbent() {
    let (mut store, _dir) = registered("registration");

    store.reseed(TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();
    store
        .register(TID, SCHEMA, "v", &view_schema())
        .expect("a second registration");
    assert_eq!(
        store.cursor_of(TID),
        Some(cursor(4)),
        "the same id at the same layout stands, cursor and all",
    );

    store
        .register(OTHER_TID, SCHEMA, "v", &view_schema())
        .expect("the name moves to a fresh id");
    assert_eq!(
        store.cursor_of(TID),
        None,
        "a retracted registration takes its cursor with it, or the next poll \
         delivers onto an erased copy and loses everything below its round",
    );
    assert!(whole_copy(&mut store, TID).is_err(), "and its copy is gone");
}

/// The ladder, level by level: each does everything the level above it does, and
/// then more. Every level drops the cursor, so no delta continues it, and is
/// idempotent.
#[test]
fn the_invalidate_ladder_stops_where_it_is_asked() {
    for level in [Invalidate::Cursor, Invalidate::Copy, Invalidate::Registration] {
        let (mut store, dir) = registered(&format!("ladder_{level:?}"));
        store
            .reseed(TID, &[&plain(&[(1, 1, 10), (2, 1, 20)])], cursor(4))
            .unwrap();
        store.checkpoint().unwrap();
        store.invalidate(TID, level).unwrap();
        store.invalidate(TID, level).expect("a second teardown is a no-op");

        assert_eq!(store.cursor_of(TID), None, "{level:?} drops the cursor");
        assert!(
            store.advance(TID, &[&plain(&[(3, 1, 30)])], cursor(5)).is_err(),
            "{level:?}: no cursor, so no delta continues it",
        );
        let reseeded = store.reseed(TID, &[&plain(&[(1, 1, 10)])], cursor(5));
        match level {
            Invalidate::Cursor => {
                assert!(reseeded.is_err(), "a whole value never lands on the rows it replaces");
                assert_eq!(
                    held(&mut store, TID),
                    BTreeMap::from([((1, 10), 1), ((2, 20), 1)]),
                    "Cursor keeps the rows, and neither refusal touched one",
                );
            }
            Invalidate::Copy => {
                reseeded.expect("an erased copy takes a reseed");
                assert_eq!(held(&mut store, TID), BTreeMap::from([((1, 10), 1)]));
            }
            Invalidate::Registration => {
                assert!(reseeded.is_err(), "the registration that named its shape is gone");
                assert!(whole_copy(&mut store, TID).is_err(), "and so is the copy");
                assert!(!has_copy(&dir, TID), "directory and all, checkpointed or not");
                // Dropped without another checkpoint: the retraction is durable
                // on its own.
                drop(store);
                let mut store = Mirror::open(&dir).expect("the store reopens");
                assert_eq!(store.cursor_of(TID), None, "a forgotten copy never comes back");
                store
                    .register(TID, SCHEMA, "v", &view_schema())
                    .expect("the id can be registered again");
            }
        }
    }
}

/// An advance moves the cursor only after its blocks are applied, and its train
/// folds onto the rows a reseed left.
#[test]
fn an_advance_applies_before_it_moves_the_cursor() {
    let (mut store, _dir) = registered("advance");
    store
        .reseed(TID, &[&plain(&[(1, 1, 10), (2, 1, 20)])], cursor(4))
        .unwrap();
    assert_eq!(store.cursor_of(TID), Some(cursor(4)));

    store
        .advance(TID, &[&plain(&[(1, 1, 10), (3, 1, 30)])], cursor(5))
        .unwrap();
    assert_eq!(store.cursor_of(TID), Some(cursor(5)));
    assert_eq!(
        held(&mut store, TID),
        BTreeMap::from([((1, 10), 2), ((2, 20), 1), ((3, 30), 1)]),
        "the repeated key folds onto its own element",
    );
}

/// A checkpoint keeps the position of a copy this session never re-registered.
#[test]
fn a_checkpoint_covers_a_cursor_this_session_never_claimed() {
    let dir = two_checkpointed_copies("unclaimed");
    {
        // Claim only one, move it, and checkpoint again.
        let mut store = Mirror::open(&dir).expect("the checkpointed store reopens");
        assert!(
            store.cursor_of(TID).is_some() && store.cursor_of(OTHER_TID).is_some(),
            "both positions came back",
        );
        store.register(TID, SCHEMA, "v", &view_schema()).unwrap();
        store.advance(TID, &[&plain(&[(1, 1, 10)])], cursor(5)).unwrap();
        store.checkpoint().unwrap();
    }

    let mut store = Mirror::open(&dir).expect("the store reopens again");
    assert_eq!(
        store.cursor_of(OTHER_TID),
        Some(cursor(4)),
        "the unclaimed copy's position survived the checkpoint that republished it",
    );
    store.register(OTHER_TID, SCHEMA, "w", &view_schema()).unwrap();
    assert_eq!(held(&mut store, OTHER_TID), BTreeMap::from([((9, 90), 1)]));
}

/// A failed apply erases that copy and leaves the others alone, the store still
/// readable and publishable.
///
/// Weight-exact: a re-applied interval leaves the row set identical.
#[test]
fn a_failed_apply_erases_that_copy_and_spares_the_rest() {
    let (mut store, _dir) = two_copies("failed_apply");

    let err = store
        .advance(TID, &[&[0xFF; 64]], cursor(5))
        .expect_err("an undecodable block must fail the advance");
    assert!(
        !matches!(err, MirrorError::Poisoned(_)),
        "one copy's fault must not poison the store: {err}",
    );
    assert!(store.poisoned().is_none(), "and the store must not be poisoned");

    assert_eq!(store.cursor_of(TID), None, "the erased copy keeps no cursor");
    assert!(held(&mut store, TID).is_empty(), "and holds no rows");
    assert_eq!(
        store.cursor_of(OTHER_TID),
        Some(cursor(4)),
        "the sibling copy keeps its position",
    );
    assert_eq!(
        held(&mut store, OTHER_TID),
        BTreeMap::from([((9, 90), 1)]),
        "and its rows",
    );
    store
        .checkpoint()
        .expect("a store holding one erased copy is still safe to publish");

    // With no cursor the next poll bootstraps, onto the erased copy.
    store
        .reseed(TID, &[&plain(&[(1, 1, 10), (2, 1, 20)])], cursor(9))
        .unwrap();
    assert_eq!(
        held(&mut store, TID),
        BTreeMap::from([((1, 10), 1), ((2, 20), 1)]),
        "the re-bootstrapped copy holds exactly one materialisation",
    );
}

/// What a copy on disk can suffer between two opens.
#[derive(Debug, Clone, Copy)]
enum Damage {
    /// Its child directory cannot be opened — possibly a transient fault.
    Unopenable,
    /// Its manifest no longer decodes — the copy is gone for good.
    Manifest,
}

/// Damage costs the copies it touches, and the open degrades rather than fails.
/// An unreadable manifest is swept; an unopenable copy is kept, since the fault
/// may be transient — and the sweep runs only when nothing failed to open, as it
/// deletes every directory it does not recognise.
#[test]
fn a_damaged_copy_costs_that_copy_alone() {
    let cases: &[(&str, &[(u64, Damage)])] = &[
        ("unopenable", &[(TID, Damage::Unopenable)]),
        ("bad_manifest", &[(TID, Damage::Manifest)]),
        (
            "all_unopenable",
            &[(TID, Damage::Unopenable), (OTHER_TID, Damage::Unopenable)],
        ),
    ];
    for &(name, damage) in cases {
        let dir = two_checkpointed_copies(&format!("damaged_{name}"));
        for &(tid, d) in damage {
            match d {
                Damage::Unopenable => block_copy(&dir, tid),
                Damage::Manifest => {
                    let manifest = manifest_path(&dir, tid);
                    let mut bytes = std::fs::read(&manifest).unwrap();
                    bytes[20] ^= 1;
                    std::fs::write(&manifest, bytes).unwrap();
                }
            }
        }

        let mut store = Mirror::open(&dir).unwrap_or_else(|e| panic!("{name}: the open must degrade: {e}"));
        for (tid, row) in [(TID, (1, 10)), (OTHER_TID, (9, 90))] {
            match damage.iter().find(|(t, _)| *t == tid) {
                Some(&(_, d)) => {
                    assert_eq!(store.cursor_of(tid), None, "{name}: a damaged copy bootstraps");
                    assert_eq!(
                        has_copy(&dir, tid),
                        matches!(d, Damage::Unopenable),
                        "{name}: {d:?} decides whether the directory is swept",
                    );
                }
                None => {
                    assert_eq!(
                        store.cursor_of(tid),
                        Some(cursor(4)),
                        "{name}: an intact sibling resumes"
                    );
                    store.register(tid, SCHEMA, "w", &view_schema()).unwrap();
                    assert_eq!(
                        held(&mut store, tid),
                        BTreeMap::from([(row, 1)]),
                        "{name}: with its rows"
                    );
                }
            }
        }

        if name == "unopenable" {
            // A registration after the open starts empty, even over rows the
            // open left on disk.
            unblock_copy(&dir, TID);
            store.register(TID, SCHEMA, "v", &view_schema()).unwrap();
            assert_eq!(store.cursor_of(TID), None, "a fresh registration has no position");
            assert!(held(&mut store, TID).is_empty(), "and holds none of the old rows");
        }
    }
}

/// A copy erased and refilled to its checkpointed cursor is published again.
#[test]
fn a_copy_erased_and_refilled_to_the_same_cursor_is_published() {
    let (mut store, dir) = registered("erased_and_refilled");
    store.reseed(TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();
    store.checkpoint().unwrap();
    store.invalidate(TID, Invalidate::Copy).unwrap();
    store.reseed(TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();
    store.checkpoint().unwrap();
    drop(store);

    let mut store = Mirror::open(&dir).expect("the store reopens");
    assert_eq!(store.cursor_of(TID), Some(cursor(4)), "the refilled copy resumes");
    assert_eq!(held(&mut store, TID), BTreeMap::from([((1, 10), 1)]));
}

/// A second store on a held directory is refused. `flock` conflicts on a fresh
/// open file description whichever process holds the first, so a second open in
/// this process stands for one in any other.
#[test]
fn a_second_store_on_one_directory_is_refused() {
    let (store, dir) = registered("second_store");
    let err = Mirror::open(&dir).err().expect("a held directory must be refused");
    assert!(err.to_string().contains("is already held"), "{err}");
    drop(store);
    Mirror::open(&dir).expect("the directory is free once the first store is dropped");
}

/// A teardown that cannot erase poisons the store: the copy may be half gone,
/// so it must be neither read nor published. The cursor goes first, and a reopen
/// finds the last checkpoint, which no failed erase published over.
#[test]
fn a_teardown_that_cannot_erase_poisons_the_store() {
    let (mut store, dir) = registered("failed_teardown");
    store.reseed(TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();
    store.checkpoint().unwrap();

    block_copy(&dir, TID);
    let err = store
        .invalidate(TID, Invalidate::Copy)
        .expect_err("an erase over a blocked copy must fail");
    assert!(matches!(err, MirrorError::Poisoned(_)), "{err}");
    assert_eq!(
        store.cursor_of(TID),
        None,
        "the cursor went before the erase could fail"
    );
    assert!(matches!(whole_copy(&mut store, TID), Err(MirrorError::Poisoned(_))));
    assert!(matches!(store.checkpoint(), Err(MirrorError::Poisoned(_))));
    drop(store);

    unblock_copy(&dir, TID);
    let mut store = Mirror::open(&dir).expect("the store reopens");
    assert_eq!(store.cursor_of(TID), Some(cursor(4)), "the last checkpoint stands");
    store.register(TID, SCHEMA, "v", &view_schema()).unwrap();
    assert_eq!(held(&mut store, TID), BTreeMap::from([((1, 10), 1)]));
}

/// A failed checkpoint is reported, not poisoned: the flush leaves every copy
/// holding what it held, so reads go on and a retry publishes exactly that.
#[test]
fn a_failed_checkpoint_leaves_the_store_usable_and_the_retry_sound() {
    let (mut store, dir) = registered("failed_checkpoint");
    store.reseed(TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();
    store.checkpoint().unwrap();
    store
        .advance(TID, &[&plain(&[(1, 1, 10), (2, 1, 20)])], cursor(5))
        .unwrap();

    block_copy(&dir, TID);
    let err = store
        .checkpoint()
        .expect_err("a checkpoint into a blocked copy must fail");
    assert!(
        !matches!(err, MirrorError::Poisoned(_)),
        "a failed checkpoint is not a poisoning: {err}"
    );
    assert!(store.poisoned().is_none());
    let rows = BTreeMap::from([((1, 10), 2), ((2, 20), 1)]);
    assert_eq!(held(&mut store, TID), rows, "the copy is untouched");

    unblock_copy(&dir, TID);
    store.checkpoint().expect("the retry publishes");
    drop(store);
    let mut store = Mirror::open(&dir).expect("the store reopens");
    assert_eq!(store.cursor_of(TID), Some(cursor(5)));
    assert_eq!(
        held(&mut store, TID),
        rows,
        "nothing the failed attempt wrote is applied a second time",
    );
}

/// A failing auto-checkpoint does not fail the apply it follows, and leaves no
/// debt behind: the byte counter restarts, so a sub-threshold apply after it
/// publishes nothing, and the next crossing publishes the cursor its rows
/// reached.
///
/// In a child, because the threshold is read from the environment at open.
#[test]
fn a_failed_auto_checkpoint_costs_nothing_but_durability() {
    let out = run_test_in_child(
        module_path!(),
        "auto_checkpoint_child",
        &[("GNITZ_MIRROR_CHECKPOINT_BYTES", "512")],
    );
    assert_child_ok(&out, "a failed auto-checkpoint must not cost the round it followed");
}

/// Runs only in the child the test above spawns.
#[test]
fn auto_checkpoint_child() {
    if !in_child_test() {
        return;
    }
    /// The threshold the parent arms.
    const THRESHOLD: usize = 512;
    let big = |base: u64| plain(&(base..base + 40).map(|i| (i, 1, i as i64 * 10)).collect::<Vec<_>>());
    let small = plain(&[(1, 1, 10)]);
    assert!(
        big(0).len() > THRESHOLD && small.len() < THRESHOLD,
        "the trains straddle the threshold"
    );

    let (mut store, dir) = registered("auto_checkpoint");
    block_copy(&dir, TID);
    store
        .reseed(TID, &[&big(0)], cursor(4))
        .expect("the threshold drives a checkpoint, and its failure is not the apply's");
    assert!(store.poisoned().is_none(), "a failed checkpoint is not a poisoning");
    assert_eq!(
        store.cursor_of(TID),
        Some(cursor(4)),
        "the cursor advanced with its rows"
    );
    unblock_copy(&dir, TID);
    assert!(!has_manifest(&dir, TID), "the failed checkpoint published nothing");

    store.advance(TID, &[&small], cursor(5)).unwrap();
    assert!(
        !has_manifest(&dir, TID),
        "a sub-threshold apply drives no checkpoint; the counter was left over threshold",
    );

    store.advance(TID, &[&big(100)], cursor(6)).unwrap();
    assert!(has_manifest(&dir, TID), "the next crossing publishes");
    let mut want: BTreeMap<(u64, i64), i64> = (0..40).chain(100..140).map(|i| ((i, i as i64 * 10), 1)).collect();
    *want.get_mut(&(1, 10)).unwrap() += 1;
    drop(store);
    let mut store = Mirror::open(&dir).expect("the store reopens");
    assert_eq!(
        store.cursor_of(TID),
        Some(cursor(6)),
        "the published cursor is the one its rows reached"
    );
    store.register(TID, SCHEMA, "v", &view_schema()).unwrap();
    assert_eq!(held(&mut store, TID), want, "every round applied exactly once");
    println!("{CHILD_OK}");
}
