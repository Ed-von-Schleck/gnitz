//! The store on its own: no server, no client, no connection.
//!
//! Every method is a statement about the copy it holds, so the store's lifecycle
//! is tested here: the registration and what it retracts, the teardown ladder,
//! what a drop forfeits, and what a checkpoint makes durable. The acceptance
//! suite beside it (`mirror.rs`) drives a real client against a real server.
//!
//! Faults are real I/O faults, not seams: [`block_copy`] puts a regular file
//! where a copy's child directory belongs, so whatever writes there fails.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use gnitz_core::{DeltaCursor, Invalidate, MirrorError, MirrorStore, Schema, ZSetBatch};
use gnitz_mirror::{Mirror, MirrorConfig};
use gnitz_wire::{ReadBound, ReadSpec};
use gnitz_zset::schema::{encode_schema_block, SchemaDescriptor};
use gnitz_zset_testkit::{make_batch, make_schema_u128_i64, make_schema_u64_i64};
use tempfile::TempDir;

#[path = "support/common.rs"]
mod common;
use common::{block_copy, has_copy, has_manifest, manifest_path, unblock_copy};

const SCHEMA: &str = "s";
const TID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;
const OTHER_TID: u64 = gnitz_wire::FIRST_USER_TABLE_ID + 1;

/// The client-side schema whose record denotes `layout`.
fn schema_of(layout: &SchemaDescriptor) -> Schema {
    Schema::from_block(&encode_schema_block(layout)).unwrap()
}

/// The shape every copy here is registered under: [`make_schema_u64_i64`]'s, so
/// a batch built on the engine side decodes against a copy registered from here.
fn view_schema() -> Schema {
    schema_of(&make_schema_u64_i64())
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
    gnitz_zset_testkit::encode_to_wire_vec(&make_batch(&make_schema_u64_i64(), rows))
}

fn path(dir: &TempDir) -> &str {
    dir.path().to_str().unwrap()
}

fn open(dir: &TempDir) -> Mirror {
    Mirror::open(path(dir), MirrorConfig::default()).expect("the store opens")
}

/// A store on a fresh directory with `TID` registered under `s.v`.
fn registered() -> (Mirror, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(&dir);
    store
        .register(TID, SCHEMA, "v", &view_schema())
        .expect("a first registration");
    (store, dir)
}

/// A store holding two copies, `TID` under `s.v` and `OTHER_TID` under `s.w`,
/// one row each at round 4.
fn two_copies() -> (Mirror, TempDir) {
    let (mut store, dir) = registered();
    store.register(OTHER_TID, SCHEMA, "w", &view_schema()).unwrap();
    store.reseed(TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();
    store.reseed(OTHER_TID, &[&plain(&[(9, 1, 90)])], cursor(4)).unwrap();
    (store, dir)
}

/// [`two_copies`], checkpointed and closed. Returns the directory.
fn two_checkpointed_copies() -> TempDir {
    let (mut store, dir) = two_copies();
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
        *out.entry((pk, gnitz_wire::read_i64_le(vals, row * 8))).or_insert(0) += batch.weights[row];
    }
    out.retain(|_, w| *w != 0);
    out
}

/// Every row of the copy.
fn whole_copy(store: &mut Mirror, tid: u64) -> Result<ZSetBatch, MirrorError> {
    store.scan_spec(tid, ReadSpec::all_rows(ReadBound::None), &view_schema())
}

/// What a registration keeps and what it retracts. The same id at the same
/// layout stands, rows and cursor, and takes the upstream name; another id under
/// a held name retracts the incumbent and names it; the same id at another
/// layout is registered afresh.
#[test]
fn a_registration_stands_while_the_id_and_the_layout_do() {
    let (mut store, dir) = registered();
    store.reseed(TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();

    assert_eq!(store.register(TID, SCHEMA, "moved", &view_schema()).unwrap(), None);
    assert_eq!(store.cursor_of(TID), Some(cursor(4)), "a renamed copy keeps its cursor");
    assert_eq!(held(&mut store, TID), BTreeMap::from([((1, 10), 1)]), "and its rows");
    assert_eq!(store.register(OTHER_TID, SCHEMA, "v", &view_schema()).unwrap(), None);
    assert_eq!(
        store.cursor_of(TID),
        Some(cursor(4)),
        "the name it left retracts nothing"
    );

    assert_eq!(
        store.register(OTHER_TID + 1, SCHEMA, "moved", &view_schema()).unwrap(),
        Some(TID),
        "a held name at another id names the incumbent it retracted",
    );
    assert_eq!(
        store.cursor_of(TID),
        None,
        "a retracted registration takes its cursor with it, or the next poll \
         delivers onto an erased copy and loses everything below its round",
    );
    assert!(!has_copy(path(&dir), TID), "and its copy is gone");

    store.reseed(OTHER_TID, &[&plain(&[(9, 1, 90)])], cursor(4)).unwrap();
    let wide = schema_of(&make_schema_u128_i64());
    assert_eq!(store.register(OTHER_TID, SCHEMA, "v", &wide).unwrap(), None);
    assert_eq!(store.cursor_of(OTHER_TID), None, "another layout is another copy");
    let rows = store.scan_spec(OTHER_TID, ReadSpec::all_rows(ReadBound::None), &wide);
    assert_eq!(rows.unwrap().weights.len(), 0, "which starts empty");
}

/// The ladder, level by level: each does everything the level above it does, and
/// then more. Every level drops the cursor, so no delta continues it, and is
/// idempotent.
#[test]
fn the_invalidate_ladder_stops_where_it_is_asked() {
    for level in [Invalidate::Cursor, Invalidate::Copy, Invalidate::Registration] {
        let (mut store, dir) = registered();
        store
            .reseed(TID, &[&plain(&[(1, 1, 10), (2, 1, 20)])], cursor(4))
            .unwrap();
        store.checkpoint().unwrap();
        store.invalidate(TID, level).unwrap();
        store.invalidate(TID, level).expect("a second teardown is a no-op");

        assert_eq!(store.cursor_of(TID), None, "{level:?} drops the cursor");
        assert!(
            matches!(
                store.advance(TID, &[&plain(&[(3, 1, 30)])], cursor(5)),
                Err(MirrorError::Engine(_))
            ),
            "{level:?}: no cursor, so no delta continues it",
        );
        let reseeded = store.reseed(TID, &[&plain(&[(7, 1, 70)])], cursor(4));
        match level {
            Invalidate::Cursor => {
                assert!(
                    matches!(reseeded, Err(MirrorError::Engine(_))),
                    "a whole value never lands on the rows it replaces",
                );
                assert_eq!(
                    held(&mut store, TID),
                    BTreeMap::from([((1, 10), 1), ((2, 20), 1)]),
                    "Cursor keeps the rows, and neither refusal touched one",
                );
            }
            Invalidate::Copy => {
                reseeded.expect("an erased copy takes a reseed");
                // Refilled to the cursor the last checkpoint published, so only
                // the rows say the copy changed.
                store.checkpoint().unwrap();
                drop(store);
                let mut store = open(&dir);
                assert_eq!(store.cursor_of(TID), Some(cursor(4)));
                assert_eq!(
                    held(&mut store, TID),
                    BTreeMap::from([((7, 70), 1)]),
                    "the refill is what was published, over the rows it replaced",
                );
            }
            Invalidate::Registration => {
                assert!(
                    matches!(reseeded, Err(MirrorError::Engine(_))),
                    "the registration that named its shape is gone",
                );
                assert!(whole_copy(&mut store, TID).is_err(), "and so is the copy");
                assert!(!has_copy(path(&dir), TID), "directory and all, checkpointed or not");
                // Dropped without another checkpoint: the retraction is durable
                // on its own.
                drop(store);
                let mut store = open(&dir);
                assert_eq!(store.cursor_of(TID), None, "a forgotten copy never comes back");
                store
                    .register(TID, SCHEMA, "v", &view_schema())
                    .expect("the id can be registered again");
            }
        }
    }
}

/// An empty view's whole value fills a copy: the cursor says so where no row
/// can, and a second whole value is refused on it.
#[test]
fn a_copy_filled_with_no_rows_takes_no_second_reseed() {
    let (mut store, _dir) = registered();
    store.reseed(TID, &[], cursor(4)).unwrap();
    let again = store.reseed(TID, &[&plain(&[(1, 1, 10)])], cursor(5));
    assert!(matches!(again, Err(MirrorError::Engine(_))), "{again:?}");
    assert_eq!(store.cursor_of(TID), Some(cursor(4)));
    assert!(held(&mut store, TID).is_empty());
}

/// A round folds onto the rows the copy holds — a retraction, a second payload
/// under a held PK, a fresh key — and a drop forfeits the rounds since the last
/// checkpoint together with their cursor, so the feed delivers them once more
/// and they land once.
#[test]
fn a_drop_forfeits_the_rounds_since_the_last_checkpoint() {
    let (mut store, dir) = registered();
    store
        .reseed(TID, &[&plain(&[(1, 1, 10), (2, 1, 20)])], cursor(4))
        .unwrap();
    store.checkpoint().unwrap();
    let round = plain(&[(1, -1, 10), (1, 1, 11), (3, 1, 30)]);
    let after = BTreeMap::from([((1, 11), 1), ((2, 20), 1), ((3, 30), 1)]);
    store.advance(TID, &[&round], cursor(5)).unwrap();
    assert_eq!(store.cursor_of(TID), Some(cursor(5)));
    assert_eq!(held(&mut store, TID), after);
    drop(store);

    let mut store = open(&dir);
    assert_eq!(
        store.cursor_of(TID),
        Some(cursor(4)),
        "the cursor falls back with its rows"
    );
    assert_eq!(held(&mut store, TID), BTreeMap::from([((1, 10), 1), ((2, 20), 1)]));
    store.advance(TID, &[&round], cursor(5)).unwrap();
    assert_eq!(held(&mut store, TID), after, "re-applied once, not doubled");
}

/// A checkpoint republishes a copy the session never touched, position and
/// rows, beside the one it moved.
#[test]
fn a_checkpoint_keeps_a_copy_the_session_never_touched() {
    let dir = two_checkpointed_copies();
    {
        let mut store = open(&dir);
        assert_eq!(
            [store.cursor_of(TID), store.cursor_of(OTHER_TID)],
            [Some(cursor(4)); 2],
            "both positions came back",
        );
        store.advance(TID, &[&plain(&[(1, 1, 10)])], cursor(5)).unwrap();
        store.checkpoint().unwrap();
    }

    let mut store = open(&dir);
    assert_eq!(store.cursor_of(TID), Some(cursor(5)));
    assert_eq!(held(&mut store, TID), BTreeMap::from([((1, 10), 2)]));
    assert_eq!(
        store.cursor_of(OTHER_TID),
        Some(cursor(4)),
        "the untouched copy's position survived the checkpoint that republished it",
    );
    assert_eq!(held(&mut store, OTHER_TID), BTreeMap::from([((9, 90), 1)]));
}

/// A failed apply erases that copy — the blocks of its train that did apply
/// included — and leaves the others alone, the store still readable and
/// publishable.
#[test]
fn a_failed_apply_erases_that_copy_and_spares_the_rest() {
    let (mut store, _dir) = two_copies();

    let err = store
        .advance(TID, &[&plain(&[(2, 1, 20)]), &[0xFF; 64]], cursor(5))
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
    /// Its manifest no longer decodes.
    Manifest,
    /// Its manifest is intact and carries a record this store did not write.
    Record,
}

/// Damage costs the copy it touches, and the open degrades rather than fails.
/// A copy with no manifest or no record of its own is swept; an unopenable one
/// is kept, since the fault may be transient. Either way a registration after
/// the open starts empty.
#[test]
fn a_damaged_copy_costs_that_copy_alone() {
    for damage in [Damage::Unopenable, Damage::Manifest, Damage::Record] {
        let dir = two_checkpointed_copies();
        match damage {
            Damage::Unopenable => block_copy(path(&dir), TID),
            Damage::Manifest => {
                let manifest = manifest_path(path(&dir), TID);
                let mut bytes = std::fs::read(&manifest).unwrap();
                *bytes.last_mut().unwrap() ^= 1;
                std::fs::write(&manifest, bytes).unwrap();
            }
            Damage::Record => {
                use gnitz_store::relation::{RelationKind, RelationRegistry, RelationSpec};
                let mut raw = RelationRegistry::new(path(&dir), gnitz_zset::schema::Slot::SOLO, Default::default());
                raw.reopen_view(RelationSpec {
                    id: TID,
                    kind: RelationKind::View(gnitz_wire::ViewProps::Plain),
                    schema: make_schema_u64_i64(),
                })
                .unwrap();
                raw.set_caller_record(TID, b"not a mirror record".to_vec()).unwrap();
                raw.checkpoint_ephemeral([]).unwrap();
            }
        }

        let mut store = Mirror::open(path(&dir), MirrorConfig::default())
            .unwrap_or_else(|e| panic!("{damage:?}: the open must degrade: {e}"));
        assert_eq!(store.cursor_of(TID), None, "{damage:?}: a damaged copy bootstraps");
        assert_eq!(
            has_copy(path(&dir), TID),
            matches!(damage, Damage::Unopenable),
            "{damage:?} decides whether the directory is swept",
        );
        assert_eq!(
            store.cursor_of(OTHER_TID),
            Some(cursor(4)),
            "{damage:?}: the intact sibling resumes"
        );
        assert_eq!(
            held(&mut store, OTHER_TID),
            BTreeMap::from([((9, 90), 1)]),
            "{damage:?}: with its rows"
        );

        if matches!(damage, Damage::Unopenable) {
            unblock_copy(path(&dir), TID);
        }
        store.register(TID, SCHEMA, "v", &view_schema()).unwrap();
        assert_eq!(
            store.cursor_of(TID),
            None,
            "{damage:?}: a fresh registration has no position"
        );
        assert!(
            held(&mut store, TID).is_empty(),
            "{damage:?}: and holds none of the rows the open left on disk"
        );
    }
}

/// A second store on a held directory is refused. `flock` conflicts on a fresh
/// open file description whichever process holds the first, so a second open in
/// this process stands for one in any other.
#[test]
fn a_second_store_on_one_directory_is_refused() {
    let (store, dir) = registered();
    let err = Mirror::open(path(&dir), MirrorConfig::default())
        .err()
        .expect("a held directory must be refused");
    assert!(err.to_string().contains("is already held"), "{err}");
    drop(store);
    open(&dir);
}

/// A teardown that cannot erase poisons the store: the copy may be half gone,
/// so every verb that touches a copy is refused — the sibling's included — and
/// nothing is published. The accessors still answer and the read gate still
/// shuts. A reopen finds the last checkpoint, which no failed erase published
/// over.
#[test]
fn a_teardown_that_cannot_erase_poisons_the_store() {
    let (mut store, dir) = two_copies();
    store.checkpoint().unwrap();

    block_copy(path(&dir), TID);
    let err = store
        .invalidate(TID, Invalidate::Copy)
        .expect_err("an erase over a blocked copy must fail");
    assert!(matches!(err, MirrorError::Poisoned(_)), "{err}");
    assert!(store.poisoned().is_some());
    assert_eq!(
        store.cursor_of(TID),
        None,
        "the cursor went before the erase could fail"
    );

    let block = plain(&[(1, 1, 10)]);
    let refused: [(&str, Result<(), MirrorError>); 6] = [
        (
            "register",
            store.register(OTHER_TID, SCHEMA, "w", &view_schema()).map(drop),
        ),
        ("invalidate", store.invalidate(OTHER_TID, Invalidate::Cursor)),
        ("reseed", store.reseed(TID, &[&block], cursor(5))),
        ("advance", store.advance(OTHER_TID, &[&block], cursor(5))),
        ("scan_spec", whole_copy(&mut store, OTHER_TID).map(drop)),
        ("checkpoint", store.checkpoint()),
    ];
    for (verb, r) in refused {
        assert!(matches!(r, Err(MirrorError::Poisoned(_))), "{verb}: {r:?}");
    }
    assert_eq!(
        store.cursor_of(OTHER_TID),
        Some(cursor(4)),
        "no refused verb moved the sibling"
    );
    store.clear_cursors();
    assert_eq!(
        store.cursor_of(OTHER_TID),
        None,
        "a poisoned store still shuts the read gate"
    );
    drop(store);

    unblock_copy(path(&dir), TID);
    let mut store = open(&dir);
    for (tid, row) in [(TID, (1, 10)), (OTHER_TID, (9, 90))] {
        assert_eq!(store.cursor_of(tid), Some(cursor(4)), "the last checkpoint stands");
        assert_eq!(held(&mut store, tid), BTreeMap::from([(row, 1)]));
    }
}

/// A failed checkpoint is reported, not poisoned: the flush leaves every copy
/// holding what it held, so reads go on and a retry publishes exactly that.
#[test]
fn a_failed_checkpoint_leaves_the_store_usable_and_the_retry_sound() {
    let (mut store, dir) = registered();
    store.reseed(TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();
    store.checkpoint().unwrap();
    store
        .advance(TID, &[&plain(&[(1, 1, 10), (2, 1, 20)])], cursor(5))
        .unwrap();

    block_copy(path(&dir), TID);
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

    unblock_copy(path(&dir), TID);
    store.checkpoint().expect("the retry publishes");
    drop(store);
    let mut store = open(&dir);
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
#[test]
fn a_failed_auto_checkpoint_costs_nothing_but_durability() {
    const THRESHOLD: usize = 512;
    let big = |base: u64| plain(&(base..base + 40).map(|i| (i, 1, i as i64 * 10)).collect::<Vec<_>>());
    let small = plain(&[(1, 1, 10)]);
    assert!(
        big(0).len() > THRESHOLD && small.len() < THRESHOLD,
        "the trains straddle the threshold"
    );

    let dir = tempfile::tempdir().unwrap();
    let config = MirrorConfig {
        checkpoint_bytes: THRESHOLD,
        ..MirrorConfig::default()
    };
    let mut store = Mirror::open(path(&dir), config).unwrap();
    store.register(TID, SCHEMA, "v", &view_schema()).unwrap();
    block_copy(path(&dir), TID);
    store
        .reseed(TID, &[&big(0)], cursor(4))
        .expect("the threshold drives a checkpoint, and its failure is not the apply's");
    assert!(store.poisoned().is_none(), "a failed checkpoint is not a poisoning");
    assert_eq!(
        store.cursor_of(TID),
        Some(cursor(4)),
        "the cursor advanced with its rows"
    );
    unblock_copy(path(&dir), TID);
    assert!(
        !has_manifest(path(&dir), TID),
        "the failed checkpoint published nothing"
    );

    store.advance(TID, &[&small], cursor(5)).unwrap();
    assert!(
        !has_manifest(path(&dir), TID),
        "a sub-threshold apply drives no checkpoint; the counter was left over threshold",
    );

    store.advance(TID, &[&big(100)], cursor(6)).unwrap();
    assert!(has_manifest(path(&dir), TID), "the next crossing publishes");
    let mut want: BTreeMap<(u64, i64), i64> = (0..40).chain(100..140).map(|i| ((i, i as i64 * 10), 1)).collect();
    *want.get_mut(&(1, 10)).unwrap() += 1;
    drop(store);
    let mut store = open(&dir);
    assert_eq!(
        store.cursor_of(TID),
        Some(cursor(6)),
        "the published cursor is the one its rows reached"
    );
    assert_eq!(held(&mut store, TID), want, "every round applied exactly once");
}
