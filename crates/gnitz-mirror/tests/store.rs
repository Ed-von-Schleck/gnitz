//! The store on its own: no server, no client, no connection.
//!
//! It is testable in isolation because it no longer owns one — every method is a
//! statement about the copy it holds. What lives here is the store's lifecycle:
//! the registration and what it retracts, the teardown ladder, the ingest's
//! ordering, and what a checkpoint makes durable. The acceptance suite beside it
//! (`mirror.rs`) drives a real client against a real server.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use gnitz_core::{ColumnDef, DeltaCursor, Invalidate, MirrorError, MirrorStore, RawBlock, Schema, TypeCode, ZSetBatch};
use gnitz_mirror::Mirror;
use gnitz_store::relation::{relation_dir, RelationKind};
use gnitz_store::schema::make_delta_schema;
use gnitz_store::storage::{Batch, ChildAddr, Slot};
use gnitz_store_testkit::{
    assert_child_ok, in_child_test, make_batch, make_schema_u64_i64, run_test_in_child, scratch_dir, CHILD_OK,
};
use gnitz_wire::{ReadBound, ReadSpec};

/// Every test in this binary takes this lock: `cargo test` runs a target's tests
/// as threads of one process, and a store open touches process-wide state — the
/// one-shot fault seams, the `io_uring` verdict latched once per process, and the
/// environment variables an open re-reads.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

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
    DeltaCursor { tag: 0xA11CE, tick }
}

/// One wire block holding `batch`, as a reply frame would have carried it.
fn block_of(batch: &Batch) -> RawBlock {
    RawBlock::from_block(batch.encode_to_wire_vec(TID as u32, false))
}

/// A bootstrap's train: the view's own rows, in the view's own schema.
fn plain(rows: &[(u64, i64, i64)]) -> Vec<RawBlock> {
    let desc = make_schema_u64_i64();
    vec![block_of(&make_batch(&desc, rows))]
}

/// A poll's train: the same rows keyed by a round number prepended to the view's
/// key, stamped by the very call the server's feed stamps with — so what the
/// store strips is tested against the real producer and not a second spelling of
/// the OPK rule.
fn stamped(tick: u64, rows: &[(u64, i64, i64)]) -> Vec<RawBlock> {
    let view = make_schema_u64_i64();
    let delta = make_delta_schema(&view).expect("the stamped shape fits");
    let batch = make_batch(&view, rows).stamped_with_pk_prefix(&delta, tick);
    vec![block_of(&batch)]
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

/// One copy's directory, through the engine's own path grammar.
fn copy_dir(base_dir: &str, tid: u64) -> String {
    relation_dir(base_dir, RelationKind::View, tid as i64)
}

/// Whether `tid`'s copy currently has a published manifest — the on-disk
/// difference between a checkpointed store and one that never published.
fn has_manifest(base_dir: &str, tid: u64) -> bool {
    std::path::Path::new(&ChildAddr::worker(Slot::SOLO).manifest(&copy_dir(base_dir, tid))).exists()
}

/// Two registered copies, one row each at round 4, checkpointed and closed.
/// Returns the directory.
fn two_checkpointed_copies(name: &str) -> String {
    let dir = scratch_dir("mirror_store", name);
    let mut store = Mirror::open(&dir).expect("a fresh store");
    store.register(TID, SCHEMA, "v", &view_schema()).unwrap();
    store.register(OTHER_TID, SCHEMA, "w", &view_schema()).unwrap();
    store.ingest(TID, plain(&[(1, 1, 10)]), cursor(4)).unwrap();
    store.ingest(OTHER_TID, plain(&[(9, 1, 90)]), cursor(4)).unwrap();
    store.checkpoint().unwrap();
    dir
}

/// Make `tid`'s copy refuse to open: a regular file where its per-worker child
/// directory belongs. The copy's own directory stays a directory, so a sweep can
/// still remove it.
fn block_copy(base_dir: &str, tid: u64) {
    let child = ChildAddr::worker(Slot::SOLO).dir(&copy_dir(base_dir, tid));
    std::fs::remove_dir_all(&child).expect("the copy's child directory");
    std::fs::write(&child, b"not a directory").expect("block the child path");
}

/// The `MirrorStore` bound the client's `Box<dyn MirrorStore>` needs.
///
/// Passes while the `unsafe impl` exists, whatever is behind it: a later
/// `pub fn` returning an `Rc` breaks soundness and still compiles here.
#[test]
fn the_store_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<Mirror>();
}

/// A registration stands while the id and the layout do; a *different* id under
/// the same qualified name retracts the incumbent.
#[test]
fn a_registration_stands_and_a_moved_name_retracts_the_incumbent() {
    let _g = serial();
    let (mut store, _dir) = registered("registration");

    store.ingest(TID, plain(&[(1, 1, 10)]), cursor(4)).unwrap();
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
/// then more — and all three leave no cursor.
///
/// One table-driven case, because the property that used to be three separate
/// contracts is now one function's control flow. The empty ingest after each
/// teardown reinstates the read gate without touching a row, which is what makes
/// "the rows survived" observable at all.
#[test]
fn the_invalidate_ladder_stops_where_it_is_asked() {
    let _g = serial();
    for (level, want) in [(Invalidate::Cursor, 2usize), (Invalidate::Copy, 0)] {
        let (mut store, _dir) = registered(&format!("ladder_{level:?}"));
        store.ingest(TID, plain(&[(1, 1, 10), (2, 1, 20)]), cursor(4)).unwrap();
        assert_eq!(held(&mut store, TID).len(), 2);

        store.invalidate(TID, level).unwrap();
        assert_eq!(store.cursor_of(TID), None, "{level:?} drops the cursor");

        store.ingest(TID, Vec::new(), cursor(4)).unwrap();
        assert_eq!(
            held(&mut store, TID).len(),
            want,
            "{level:?} must leave exactly {want} rows",
        );
    }

    let (mut store, _dir) = registered("ladder_registration");
    store.ingest(TID, plain(&[(1, 1, 10), (2, 1, 20)]), cursor(4)).unwrap();
    store.invalidate(TID, Invalidate::Registration).unwrap();
    assert_eq!(store.cursor_of(TID), None);
    assert!(whole_copy(&mut store, TID).is_err(), "the copy is gone");
    assert!(
        store.ingest(TID, plain(&[(1, 1, 10)]), cursor(5)).is_err(),
        "and so is the registration that named its shape",
    );
    store
        .invalidate(TID, Invalidate::Registration)
        .expect("an id the store does not hold tears down to Ok");
    store
        .register(TID, SCHEMA, "v", &view_schema())
        .expect("the id can be registered again");
}

/// An ingest advances the cursor only after its blocks are applied, and a
/// stamped train folds onto the rows a bootstrap left.
#[test]
fn an_ingest_applies_before_it_advances() {
    let _g = serial();
    let (mut store, _dir) = registered("ingest");
    store.ingest(TID, plain(&[(1, 1, 10), (2, 1, 20)]), cursor(4)).unwrap();
    assert_eq!(store.cursor_of(TID), Some(cursor(4)));

    store
        .ingest(TID, stamped(5, &[(1, 1, 10), (3, 1, 30)]), cursor(5))
        .unwrap();
    assert_eq!(store.cursor_of(TID), Some(cursor(5)));
    assert_eq!(
        held(&mut store, TID),
        BTreeMap::from([((1, 10), 2), ((2, 20), 1), ((3, 30), 1)]),
        "the round stamp is stripped and the repeated key folds onto its own element",
    );
}

/// A checkpoint covers every copy the store holds a position for — including one
/// this session never re-registered.
///
/// The flush round republishes every copy the store holds, so a cursor set
/// gathered from this session's registrations alone would strand the unclaimed
/// one: published at the new generation with no cursor, and bootstrapped next
/// session though it was intact.
#[test]
fn a_checkpoint_covers_a_cursor_this_session_never_claimed() {
    let _g = serial();
    let dir = scratch_dir("mirror_store", "unclaimed");
    {
        let mut store = Mirror::open(&dir).expect("a fresh store");
        store.register(TID, SCHEMA, "v", &view_schema()).unwrap();
        store.register(OTHER_TID, SCHEMA, "w", &view_schema()).unwrap();
        store.ingest(TID, plain(&[(1, 1, 10)]), cursor(4)).unwrap();
        store.ingest(OTHER_TID, plain(&[(9, 1, 90)]), cursor(4)).unwrap();
        store.checkpoint().unwrap();
    }
    {
        // Claim only one, move it, and checkpoint again.
        let mut store = Mirror::open(&dir).expect("the checkpointed store reopens");
        assert!(
            store.cursor_of(TID).is_some() && store.cursor_of(OTHER_TID).is_some(),
            "both positions came back",
        );
        store.register(TID, SCHEMA, "v", &view_schema()).unwrap();
        store.ingest(TID, stamped(5, &[(1, 1, 10)]), cursor(5)).unwrap();
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

/// A failed ingest erases that copy and leaves the others alone, the store still
/// readable and publishable.
///
/// Weight-exact: a re-applied interval leaves the row set identical.
#[test]
fn a_failed_ingest_erases_that_copy_and_spares_the_rest() {
    let _g = serial();
    let dir = scratch_dir("mirror_store", "failed_ingest");
    let mut store = Mirror::open(&dir).expect("a fresh store");
    store.register(TID, SCHEMA, "v", &view_schema()).unwrap();
    store.register(OTHER_TID, SCHEMA, "w", &view_schema()).unwrap();
    store.ingest(TID, plain(&[(1, 1, 10), (2, 1, 20)]), cursor(4)).unwrap();
    store.ingest(OTHER_TID, plain(&[(9, 1, 90)]), cursor(4)).unwrap();

    let err = store
        .ingest(TID, vec![RawBlock::from_block(vec![0xFF; 64])], cursor(5))
        .expect_err("an undecodable block must fail the ingest");
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

    // With no cursor the next poll bootstraps, and a bootstrap's train is plain.
    store.ingest(TID, plain(&[(1, 1, 10), (2, 1, 20)]), cursor(9)).unwrap();
    assert_eq!(
        held(&mut store, TID),
        BTreeMap::from([((1, 10), 1), ((2, 20), 1)]),
        "the re-bootstrapped copy holds exactly one materialisation",
    );
}

/// One copy that will not open costs that copy; the store still opens.
#[test]
fn an_unopenable_copy_bootstraps_and_its_siblings_resume() {
    let _g = serial();
    let dir = two_checkpointed_copies("unopenable_copy");
    block_copy(&dir, TID);

    let mut store = Mirror::open(&dir).expect("one unopenable copy must not fail the open");
    assert_eq!(
        store.cursor_of(TID),
        None,
        "the copy that did not open keeps no cursor, so its view bootstraps",
    );
    assert_eq!(
        store.cursor_of(OTHER_TID),
        Some(cursor(4)),
        "and its sibling resumes at the position its checkpoint recorded",
    );
    store.register(OTHER_TID, SCHEMA, "w", &view_schema()).unwrap();
    assert_eq!(
        held(&mut store, OTHER_TID),
        BTreeMap::from([((9, 90), 1)]),
        "with the rows that went with it",
    );
}

/// When every copy fails to open, the orphan sweep is skipped: the fault may be
/// transient, and the sweep deletes every copy on disk.
#[test]
fn a_wholly_failed_open_sweeps_nothing() {
    let _g = serial();
    let dir = two_checkpointed_copies("wholly_failed_open");
    block_copy(&dir, TID);
    block_copy(&dir, OTHER_TID);

    let store = Mirror::open(&dir).expect("the open must degrade rather than fail");
    assert_eq!(store.cursor_of(TID), None);
    assert_eq!(store.cursor_of(OTHER_TID), None);
    drop(store);
    for tid in [TID, OTHER_TID] {
        assert!(
            std::path::Path::new(&copy_dir(&dir, tid)).exists(),
            "a transient fault must not cost the copy on disk",
        );
    }
}

// ---------------------------------------------------------------------------
// Fault seams — each in a child, because a seam is a per-process latch
// ---------------------------------------------------------------------------

/// A `Copy` teardown that fails after erasing leaves **no** cursor, in memory
/// and durably: a surviving one would have the next poll deliver `(T, …]` onto
/// an empty copy and lose everything at or below `T` in silence.
#[test]
fn a_failed_copy_teardown_leaves_no_cursor() {
    let _g = serial();
    if !cfg!(debug_assertions) {
        return; // the seam folds away in a release build
    }
    let out = run_test_in_child(
        module_path!(),
        "failed_copy_teardown_child",
        &[("GNITZ_INJECT_MIRROR_BOOTSTRAP_ERROR", "1")],
    );
    assert_child_ok(&out, "an erased copy must never keep its cursor");
}

/// Runs only in the child the test above spawns.
#[test]
fn failed_copy_teardown_child() {
    if !in_child_test() {
        return;
    }
    let dir = scratch_dir("mirror_store", "failed_copy_teardown");
    {
        let mut store = Mirror::open(&dir).expect("a fresh store");
        store.register(TID, SCHEMA, "v", &view_schema()).unwrap();
        store.ingest(TID, plain(&[(1, 1, 10)]), cursor(4)).unwrap();
        store.checkpoint().expect("the cursor is durable before the failure");

        store
            .invalidate(TID, Invalidate::Copy)
            .expect_err("the armed seam must fail the teardown");
        assert_eq!(
            store.cursor_of(TID),
            None,
            "the cursor went before the erase could fail"
        );
        assert!(store.poisoned().is_none(), "a failed teardown is not a poisoning");
        store.checkpoint().expect("the checkpoint writes the cursor set below");
    }

    let store = Mirror::open(&dir).expect("the store reopens");
    assert_eq!(
        store.cursor_of(TID),
        None,
        "the reopen must bootstrap rather than continue a cursor over an erased copy",
    );
    println!("{CHILD_OK}");
}

/// A failing auto-checkpoint inside an ingest is reported, leaves the delta
/// applied and the cursor advanced, and does not poison — so the next poll
/// continues from the advanced cursor rather than re-applying its interval.
///
/// Asserted on **weights**: a re-applied interval leaves the row set identical
/// and doubles every weight in it, which a row-set comparison passes.
#[test]
fn an_auto_checkpoint_failure_leaves_the_cursor_advanced() {
    let _g = serial();
    if !cfg!(debug_assertions) {
        return; // the seam folds away in a release build
    }
    let out = run_test_in_child(
        module_path!(),
        "auto_checkpoint_failure_child",
        &[
            ("GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR", "1"),
            ("GNITZ_MIRROR_CHECKPOINT_BYTES", "1"),
        ],
    );
    assert_child_ok(&out, "a failed auto-checkpoint must not cost the round it followed");
}

/// Runs only in the child the test above spawns.
#[test]
fn auto_checkpoint_failure_child() {
    if !in_child_test() {
        return;
    }
    let dir = scratch_dir("mirror_store", "auto_checkpoint_failure");
    let mut store = Mirror::open(&dir).expect("a fresh store");
    store.register(TID, SCHEMA, "v", &view_schema()).unwrap();

    store
        .ingest(TID, plain(&[(1, 1, 10), (2, 1, 20)]), cursor(4))
        .expect_err("the byte threshold drives a checkpoint, and the armed seam fails it");
    assert!(store.poisoned().is_none(), "a failed checkpoint is not a poisoning");
    assert_eq!(
        store.cursor_of(TID),
        Some(cursor(4)),
        "the cursor advanced with the delta it followed; what failed was durability",
    );
    assert_eq!(held(&mut store, TID), BTreeMap::from([((1, 10), 1), ((2, 20), 1)]));

    // The seam is one-shot, so this round's own checkpoint is the real thing.
    store
        .ingest(TID, stamped(5, &[(2, 1, 20)]), cursor(5))
        .expect("the next round applies");
    assert_eq!(
        held(&mut store, TID),
        BTreeMap::from([((1, 10), 1), ((2, 20), 2)]),
        "the interval the failed checkpoint covered must not be applied a second time",
    );
    println!("{CHILD_OK}");
}

/// A failed checkpoint must not leave the applied-byte counter over its
/// threshold, or every later ingest retries one and re-fails.
///
/// Observed through the manifest: a stranded counter makes the sub-threshold
/// apply below checkpoint, and the one-shot is already spent, so it publishes.
#[test]
fn a_failed_checkpoint_does_not_strand_the_byte_counter() {
    let _g = serial();
    if !cfg!(debug_assertions) {
        return; // the seam folds away in a release build
    }
    let out = run_test_in_child(
        module_path!(),
        "failed_checkpoint_byte_counter_child",
        &[
            ("GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR", "1"),
            ("GNITZ_MIRROR_CHECKPOINT_BYTES", "512"),
        ],
    );
    assert_child_ok(
        &out,
        "a failed checkpoint must not leave every later ingest re-attempting one",
    );
}

/// Runs only in the child the test above spawns.
#[test]
fn failed_checkpoint_byte_counter_child() {
    if !in_child_test() {
        return;
    }
    /// The threshold the parent arms.
    const THRESHOLD: usize = 512;

    let dir = scratch_dir("mirror_store", "failed_checkpoint_counter");
    let mut store = Mirror::open(&dir).expect("a fresh store");
    store.register(TID, SCHEMA, "v", &view_schema()).unwrap();

    let rows: Vec<(u64, i64, i64)> = (0..40).map(|i| (i as u64, 1, i as i64 * 10)).collect();
    let big = plain(&rows);
    let big_bytes: usize = big.iter().map(|b| b.block().len()).sum();
    assert!(
        big_bytes > THRESHOLD,
        "the first train must cross the threshold: {big_bytes}"
    );
    store
        .ingest(TID, big, cursor(4))
        .expect_err("crossing the threshold drives a checkpoint, and the armed seam fails it");
    assert!(store.poisoned().is_none(), "a failed checkpoint is not a poisoning");
    assert!(!has_manifest(&dir, TID), "the failed checkpoint published nothing");

    let small = stamped(5, &[(1, 1, 10)]);
    let small_bytes: usize = small.iter().map(|b| b.block().len()).sum();
    assert!(
        small_bytes < THRESHOLD,
        "the second train must stay under it: {small_bytes}"
    );
    store.ingest(TID, small, cursor(5)).expect("a sub-threshold apply");
    assert!(
        !has_manifest(&dir, TID),
        "a sub-threshold apply must drive no checkpoint; the counter was left over threshold",
    );
    println!("{CHILD_OK}");
}
