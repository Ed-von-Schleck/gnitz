//! The store on its own: no server, no client, no connection.
//!
//! It is testable in isolation because it no longer owns one — every method is a
//! statement about the copy it holds. What lives here is the store's lifecycle:
//! the registration and its retraction set, the teardown ladder, the readability
//! gate, the ingest's ordering, and what a checkpoint makes durable. The
//! acceptance suite beside it (`mirror.rs`) drives a real client against a real
//! server.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use gnitz_core::{ColumnDef, DeltaCursor, Invalidate, MirrorStore, RawBlock, Schema, Shape, StoreRead, TypeCode};
use gnitz_engine::schema::make_delta_schema;
use gnitz_engine::storage::Batch;
use gnitz_engine_testkit::{
    assert_child_ok, in_child_test, make_batch, make_schema_u64_i64, run_test_in_child, scratch_dir, CHILD_OK,
};
use gnitz_mirror::Mirror;

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
const SID: u64 = gnitz_wire::FIRST_USER_SCHEMA_ID;

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
    let batch = make_batch(&view, rows).stamped_with_pk_prefix(&view, &delta, tick);
    vec![block_of(&batch)]
}

/// A store on a fresh directory with `TID` registered under `s.v`.
fn registered(name: &str) -> (Mirror, String) {
    let dir = scratch_dir("mirror_store", name);
    let mut store = Mirror::open(&dir).expect("a fresh store opens");
    let retracted = store
        .register(TID, SID, SCHEMA, "v", &view_schema())
        .expect("a first registration");
    assert!(retracted.is_empty(), "a first registration retracts nothing");
    (store, dir)
}

/// `pk → summed weight` for every row the copy holds, or `None` when the store
/// does not answer for it.
///
/// **Weight-exact, because that is what correctness means here**: a row-set
/// comparison would accept a delta applied twice, which leaves the row set
/// identical and doubles every weight in the interval.
fn held(store: &mut Mirror, tid: u64) -> Option<BTreeMap<u64, i64>> {
    let (_, batch) = store.scan(tid).expect("a scan of a held copy")?;
    let mut out = BTreeMap::new();
    for row in 0..batch.weights.len() {
        let pk = u64::from_le_bytes(batch.pks.buf[row * 8..row * 8 + 8].try_into().unwrap());
        *out.entry(pk).or_insert(0) += batch.weights[row];
    }
    out.retain(|_, w| *w != 0);
    Some(out)
}

/// The `MirrorStore` bound the client's `Box<dyn MirrorStore>` needs, and the
/// whole reason `handle.rs` carries an `unsafe impl`.
#[test]
fn the_store_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<Mirror>();
}

/// A registration stands while the id and the layout do; a *different* id under
/// the same qualified name retracts the incumbent, and says which.
#[test]
fn a_registration_stands_and_a_moved_name_retracts_the_incumbent() {
    let _g = serial();
    let (mut store, _dir) = registered("registration");

    assert_eq!(
        store.schema_id(SCHEMA),
        Some(SID),
        "the schema row the registration wrote is what a later one reads back",
    );
    let again = store
        .register(TID, SID, SCHEMA, "v", &view_schema())
        .expect("a second registration");
    assert!(
        again.is_empty(),
        "the same id at the same layout stands, so nothing is retracted",
    );

    store
        .ingest(TID, plain(&[(1, 1, 10)]), Shape::Plain, cursor(4))
        .unwrap();
    let moved = store
        .register(OTHER_TID, SID, SCHEMA, "v", &view_schema())
        .expect("the name moves to a fresh id");
    assert_eq!(moved, vec![TID], "the incumbent under that name is reported retracted");
    assert_eq!(
        store.cursor_of(TID),
        None,
        "a retracted registration takes its cursor with it, or the next poll \
         delivers onto an erased copy and loses everything below its round",
    );
    assert!(store.scan(TID).unwrap().is_none(), "and its copy is gone");
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
        store
            .ingest(TID, plain(&[(1, 1, 10), (2, 1, 20)]), Shape::Plain, cursor(4))
            .unwrap();
        assert_eq!(held(&mut store, TID).unwrap().len(), 2);

        store.invalidate(TID, level).unwrap();
        assert_eq!(store.cursor_of(TID), None, "{level:?} drops the cursor");

        store.ingest(TID, Vec::new(), Shape::Plain, cursor(4)).unwrap();
        assert_eq!(
            held(&mut store, TID).unwrap().len(),
            want,
            "{level:?} must leave exactly {want} rows",
        );
    }

    let (mut store, _dir) = registered("ladder_registration");
    store
        .ingest(TID, plain(&[(1, 1, 10), (2, 1, 20)]), Shape::Plain, cursor(4))
        .unwrap();
    store.invalidate(TID, Invalidate::Registration).unwrap();
    assert_eq!(store.cursor_of(TID), None);
    assert!(store.scan(TID).unwrap().is_none(), "the copy is gone");
    assert!(
        store
            .ingest(TID, plain(&[(1, 1, 10)]), Shape::Plain, cursor(5))
            .is_err(),
        "and so is the registration that named its shape",
    );
    store
        .invalidate(TID, Invalidate::Registration)
        .expect("an id the catalog does not hold tears down to Ok");
    store
        .register(TID, SID, SCHEMA, "v", &view_schema())
        .expect("the id can be registered again");
}

/// The gate is the cursor, on both reads.
///
/// A registration is written before the copy behind it exists, so a read gated on
/// the registration alone would answer an erased copy with zero rows, no request
/// and no error. `NotHeld` is what sends it upstream instead — and it stays
/// distinct from an empty answer off a readable copy.
#[test]
fn the_cursor_is_the_read_gate() {
    let _g = serial();
    let (mut store, _dir) = registered("gate");
    let spec = gnitz_wire::ReadSpec::encode_parts(&gnitz_wire::ReadBound::None, &[], &gnitz_wire::ReadSink::all_rows());

    assert!(store.scan(TID).unwrap().is_none(), "no cursor, no local scan");
    assert!(
        matches!(store.scan_spec(TID, &spec, &view_schema()).unwrap(), StoreRead::NotHeld),
        "no cursor, no local spec read",
    );

    store
        .ingest(TID, plain(&[(1, 1, 10)]), Shape::Plain, cursor(4))
        .unwrap();
    assert_eq!(held(&mut store, TID).unwrap(), BTreeMap::from([(1, 1)]));
    let StoreRead::Held(rows) = store.scan_spec(TID, &spec, &view_schema()).unwrap() else {
        panic!("a copy with a cursor answers its own reads")
    };
    assert_eq!(rows.map_or(0, |b| b.weights.len()), 1);
}

/// An ingest advances the cursor only after its blocks are applied, and a
/// stamped train folds onto the rows a bootstrap left.
#[test]
fn an_ingest_applies_before_it_advances() {
    let _g = serial();
    let (mut store, _dir) = registered("ingest");
    store
        .ingest(TID, plain(&[(1, 1, 10), (2, 1, 20)]), Shape::Plain, cursor(4))
        .unwrap();
    assert_eq!(store.cursor_of(TID), Some(cursor(4)));

    store
        .ingest(TID, stamped(5, &[(1, 1, 10), (3, 1, 30)]), Shape::Stamped, cursor(5))
        .unwrap();
    assert_eq!(store.cursor_of(TID), Some(cursor(5)));
    assert_eq!(
        held(&mut store, TID).unwrap(),
        BTreeMap::from([(1, 2), (2, 1), (3, 1)]),
        "the round stamp is stripped and the repeated key folds onto its own element",
    );
}

/// A checkpoint covers every copy the store holds a position for — including one
/// this session never re-registered.
///
/// The flush round republishes every copy in the local catalog, so a cursor set
/// gathered from this session's registrations alone would strand the unclaimed
/// one: published at the new generation with no cursor, and bootstrapped next
/// session though it was intact.
#[test]
fn a_checkpoint_covers_a_cursor_this_session_never_claimed() {
    let _g = serial();
    let dir = scratch_dir("mirror_store", "unclaimed");
    {
        let mut store = Mirror::open(&dir).expect("a fresh store");
        store.register(TID, SID, SCHEMA, "v", &view_schema()).unwrap();
        store.register(OTHER_TID, SID, SCHEMA, "w", &view_schema()).unwrap();
        store
            .ingest(TID, plain(&[(1, 1, 10)]), Shape::Plain, cursor(4))
            .unwrap();
        store
            .ingest(OTHER_TID, plain(&[(9, 1, 90)]), Shape::Plain, cursor(4))
            .unwrap();
        store.checkpoint().unwrap();
    }
    {
        // Claim only one, and checkpoint again.
        let mut store = Mirror::open(&dir).expect("the checkpointed store reopens");
        assert!(
            store.cursor_of(TID).is_some() && store.cursor_of(OTHER_TID).is_some(),
            "both positions came back",
        );
        store.register(TID, SID, SCHEMA, "v", &view_schema()).unwrap();
        store.checkpoint().unwrap();
    }

    let mut store = Mirror::open(&dir).expect("the store reopens again");
    assert_eq!(
        store.cursor_of(OTHER_TID),
        Some(cursor(4)),
        "the unclaimed copy's position survived the checkpoint that republished it",
    );
    store.register(OTHER_TID, SID, SCHEMA, "w", &view_schema()).unwrap();
    assert_eq!(held(&mut store, OTHER_TID).unwrap(), BTreeMap::from([(9, 1)]));
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
        store.register(TID, SID, SCHEMA, "v", &view_schema()).unwrap();
        store
            .ingest(TID, plain(&[(1, 1, 10)]), Shape::Plain, cursor(4))
            .unwrap();
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
        // The drop checkpoints, which is what writes the cursor set below.
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
    store.register(TID, SID, SCHEMA, "v", &view_schema()).unwrap();

    store
        .ingest(TID, plain(&[(1, 1, 10), (2, 1, 20)]), Shape::Plain, cursor(4))
        .expect_err("the byte threshold drives a checkpoint, and the armed seam fails it");
    assert!(store.poisoned().is_none(), "a failed checkpoint is not a poisoning");
    assert_eq!(
        store.cursor_of(TID),
        Some(cursor(4)),
        "the cursor advanced with the delta it followed; what failed was durability",
    );
    assert_eq!(held(&mut store, TID).unwrap(), BTreeMap::from([(1, 1), (2, 1)]));

    // The seam is one-shot, so this round's own checkpoint is the real thing.
    store
        .ingest(TID, stamped(5, &[(2, 1, 20)]), Shape::Stamped, cursor(5))
        .expect("the next round applies");
    assert_eq!(
        held(&mut store, TID).unwrap(),
        BTreeMap::from([(1, 1), (2, 2)]),
        "the interval the failed checkpoint covered must not be applied a second time",
    );
    println!("{CHILD_OK}");
}
