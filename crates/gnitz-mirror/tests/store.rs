//! The store on its own: no server, no client, no connection.
//!
//! Every method is a statement about the copy it holds, so the store's lifecycle
//! is tested here: the registration and what it retracts, the two teardowns,
//! what a drop forfeits, and what a checkpoint makes durable. The acceptance
//! suite beside it (`mirror.rs`) drives a real client against a real server.
//!
//! Faults are real I/O faults, not seams: [`block_copy`] puts a regular file
//! where a copy's child directory belongs, so whatever writes there fails.

use std::collections::BTreeMap;

use gnitz_core::{DeltaCursor, MirrorError, MirrorStore, RelDescriptor, RelName, Schema, ZSetBatch};
use gnitz_mirror::{Mirror, MirrorConfig};
use gnitz_wire::{Cut, KeyRange, PkColList, ReadBound, ReadSpec, RelClass, RelIndex};
use gnitz_zset::schema::{encode_schema_block, SchemaDescriptor};
use gnitz_zset_testkit::{make_batch, make_schema_u128_i64, make_schema_u64_i64};
use tempfile::TempDir;

#[path = "support/common.rs"]
mod common;
use common::{block_copy, has_copy, has_manifest, index_manifest_path, manifest_path, root, unblock_copy};

/// `name` in the one schema these tests register under.
fn rel(name: &str) -> RelName {
    RelName::new("s", name).unwrap()
}
const TID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;
const OTHER_TID: u64 = gnitz_wire::FIRST_USER_TABLE_ID + 1;
/// The generation a store's first checkpoint publishes at.
const FIRST_GENERATION: u64 = 1;

/// The client-side schema whose record denotes `layout`.
fn schema_of(layout: &SchemaDescriptor) -> Schema {
    Schema::from_block(&encode_schema_block(layout)).unwrap()
}

/// The shape every copy here is registered under: [`make_schema_u64_i64`]'s, so
/// a batch built on the engine side decodes against a copy registered from here.
fn view_schema() -> Schema {
    schema_of(&make_schema_u64_i64())
}

/// The descriptor a resolve of fed view `tid` answers: `schema`, with an index
/// on each of `indexes`.
fn desc_of(tid: u64, schema: &Schema, indexes: &[&[u32]]) -> RelDescriptor {
    RelDescriptor {
        tid,
        class: RelClass::FedView,
        pk_repeats: false,
        serial: false,
        schema: std::sync::Arc::new(schema.clone()),
        indexes: indexes
            .iter()
            .map(|cols| RelIndex {
                cols: PkColList::from_slice(cols),
                is_unique: false,
            })
            .collect(),
        token: 1,
    }
}

/// [`desc_of`] an unindexed view of [`view_schema`]'s shape.
fn desc(tid: u64) -> RelDescriptor {
    desc_of(tid, &view_schema(), &[])
}

fn cursor(tick: u64) -> DeltaCursor {
    DeltaCursor::from_pair(0xA11CE, tick).unwrap()
}

/// One wire block of the view's own rows, in the view's own schema — the shape
/// a bootstrap and a poll both reply in.
fn plain(rows: &[(u64, i64, i64)]) -> Vec<u8> {
    gnitz_zset_testkit::encode_to_wire_vec(&make_batch(&make_schema_u64_i64(), rows))
}

/// Replace `tid`'s copy with `blocks`, the view's whole value at `cursor`.
fn seed(store: &mut Mirror, tid: u64, blocks: &[&[u8]], cursor: DeltaCursor) -> Result<(), MirrorError> {
    store.refill(tid)?;
    store.fill(tid, blocks)?;
    store.seal(tid, cursor)
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
    store.register(&rel("v"), &desc(TID)).expect("a first registration");
    (store, dir)
}

/// A store holding two copies, `TID` under `s.v` and `OTHER_TID` under `s.w`,
/// one row each at round 4.
fn two_copies() -> (Mirror, TempDir) {
    let (mut store, dir) = registered();
    store.register(&rel("w"), &desc(OTHER_TID)).unwrap();
    seed(&mut store, TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();
    seed(&mut store, OTHER_TID, &[&plain(&[(9, 1, 90)])], cursor(4)).unwrap();
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
/// only in payload onto one entry, and would let a mangled payload pass
/// unnoticed — the keys and their weights would still line up.
fn held(store: &mut Mirror, tid: u64) -> BTreeMap<(u64, i64), i64> {
    rows_of(&whole_copy(store, tid).expect("a scan of a held copy"))
}

/// [`held`]'s fold over one reply batch.
fn rows_of(batch: &ZSetBatch) -> BTreeMap<(u64, i64), i64> {
    let vals = &batch.payload[0].bytes;
    let mut out = BTreeMap::new();
    for row in 0..batch.weights.len() {
        let pk = batch.pks.get(row) as u64;
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
/// a held name retracts the incumbent; the same id at another layout is
/// registered afresh.
#[test]
fn a_registration_stands_while_the_id_and_the_layout_do() {
    let (mut store, dir) = registered();
    seed(&mut store, TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();

    store.register(&rel("moved"), &desc(TID)).unwrap();
    assert_eq!(store.cursor_of(TID), Some(cursor(4)), "a renamed copy keeps its cursor");
    assert_eq!(held(&mut store, TID), BTreeMap::from([((1, 10), 1)]), "and its rows");
    store.register(&rel("v"), &desc(OTHER_TID)).unwrap();
    assert_eq!(
        store.cursor_of(TID),
        Some(cursor(4)),
        "the name it left retracts nothing"
    );

    store.register(&rel("moved"), &desc(OTHER_TID + 1)).unwrap();
    assert_eq!(
        store.cursor_of(TID),
        None,
        "a retracted registration takes its cursor with it, or the next poll \
         delivers onto an erased copy and loses everything below its round",
    );
    assert!(!has_copy(path(&dir), TID), "and its copy is gone");

    seed(&mut store, OTHER_TID, &[&plain(&[(9, 1, 90)])], cursor(4)).unwrap();
    let wide = schema_of(&make_schema_u128_i64());
    store.register(&rel("v"), &desc_of(OTHER_TID, &wide, &[])).unwrap();
    assert_eq!(store.cursor_of(OTHER_TID), None, "another layout is another copy");
    let rows = store.scan_spec(OTHER_TID, ReadSpec::all_rows(ReadBound::None), &wide);
    assert_eq!(rows.unwrap().weights.len(), 0, "which starts empty");
}

/// A forgotten copy is gone — cursor, rows, record and directory — so no delta
/// continues it, and forgetting it again is a no-op.
#[test]
fn a_forgotten_copy_is_gone() {
    let (mut store, dir) = registered();
    seed(&mut store, TID, &[&plain(&[(1, 1, 10), (2, 1, 20)])], cursor(4)).unwrap();
    store.checkpoint().unwrap();
    for _ in 0..2 {
        store.forget(TID).unwrap();
    }

    assert_eq!(store.cursor_of(TID), None);
    assert!(
        matches!(
            store.advance(TID, &[&plain(&[(3, 1, 30)])], cursor(5)),
            Err(MirrorError::Engine(_))
        ),
        "no cursor, so no delta continues it",
    );
    let refilled = seed(&mut store, TID, &[&plain(&[(7, 1, 70)])], cursor(4));
    assert!(
        matches!(refilled, Err(MirrorError::Engine(_))),
        "the registration that named its shape is gone",
    );
    assert!(whole_copy(&mut store, TID).is_err(), "and so is the copy");
    assert!(!has_copy(path(&dir), TID), "directory and all, checkpointed or not");
    // Dropped without another checkpoint: the retraction is durable on its own.
    drop(store);
    let mut store = open(&dir);
    assert_eq!(store.cursor_of(TID), None, "a forgotten copy never comes back");
    store
        .register(&rel("v"), &desc(TID))
        .expect("the id can be registered again");
}

/// A refill replaces what the copy held, rows and cursor, and answers no read
/// until it is sealed — at its cursor even when the value has no rows.
#[test]
fn a_refill_replaces_the_copy_once_sealed() {
    let (mut store, dir) = registered();
    seed(&mut store, TID, &[&plain(&[(1, 1, 10), (2, 1, 20)])], cursor(4)).unwrap();
    store.checkpoint().unwrap();

    store.refill(TID).unwrap();
    store.fill(TID, &[&plain(&[(7, 1, 70)])]).unwrap();
    store.fill(TID, &[&plain(&[(8, 1, 80)])]).unwrap();
    assert_eq!(store.cursor_of(TID), None, "an unsealed refill answers no read");

    seed(
        &mut store,
        TID,
        &[&plain(&[(7, 1, 70)]), &plain(&[(8, 1, 80)])],
        cursor(4),
    )
    .unwrap();
    // Sealed at the cursor the last checkpoint published, so only the rows say
    // the copy changed.
    store.checkpoint().unwrap();
    drop(store);
    let mut store = open(&dir);
    assert_eq!(store.cursor_of(TID), Some(cursor(4)));
    assert_eq!(held(&mut store, TID), BTreeMap::from([((7, 70), 1), ((8, 80), 1)]));

    seed(&mut store, TID, &[], cursor(9)).unwrap();
    assert_eq!(store.cursor_of(TID), Some(cursor(9)));
    assert!(held(&mut store, TID).is_empty());
}

/// A block that fails takes the blocks before it along, and the refill takes
/// neither another block nor a seal. The failure is that copy's alone.
#[test]
fn a_failed_block_ends_its_refill() {
    let (mut store, _dir) = registered();
    store.refill(TID).unwrap();
    store.fill(TID, &[&plain(&[(5, 1, 50)])]).unwrap();
    let torn = store.fill(TID, &[&[0xFF; 64]]);
    assert!(matches!(torn, Err(MirrorError::Engine(_))), "{torn:?}");
    let next = store.fill(TID, &[&plain(&[(6, 1, 60)])]);
    assert!(matches!(next, Err(MirrorError::Engine(_))), "{next:?}");
    let sealed = store.seal(TID, cursor(5));
    assert!(matches!(sealed, Err(MirrorError::Engine(_))), "{sealed:?}");
    assert_eq!(store.cursor_of(TID), None);
    assert!(store.poisoned().is_none());

    seed(&mut store, TID, &[&plain(&[(7, 1, 70)])], cursor(6)).unwrap();
    assert_eq!(held(&mut store, TID), BTreeMap::from([((7, 70), 1)]));
}

/// A round folds onto the rows the copy holds — a retraction, a second payload
/// under a held PK, a fresh key — and a drop forfeits the rounds since the last
/// checkpoint together with their cursor, so the feed delivers them once more
/// and they land once.
#[test]
fn a_drop_forfeits_the_rounds_since_the_last_checkpoint() {
    let (mut store, dir) = registered();
    seed(&mut store, TID, &[&plain(&[(1, 1, 10), (2, 1, 20)])], cursor(4)).unwrap();
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
    seed(&mut store, TID, &[&plain(&[(1, 1, 10), (2, 1, 20)])], cursor(9)).unwrap();
    assert_eq!(
        held(&mut store, TID),
        BTreeMap::from([((1, 10), 1), ((2, 20), 1)]),
        "the re-bootstrapped copy holds exactly one materialisation",
    );
}

/// The rows of `tid`'s copy whose `v` lies in `[lo, hi]`, read by a range on
/// `v`: an index walk where the copy holds that index.
fn v_range(store: &mut Mirror, tid: u64, lo: i64, hi: i64) -> BTreeMap<(u64, i64), i64> {
    let image = |v: i64| gnitz_wire::key_image(gnitz_wire::TypeCode::I64, v as u64 as u128);
    let range = KeyRange::new(
        PkColList::from_slice(&[1]),
        &[],
        Cut::before(image(lo)),
        Cut::after(image(hi)),
    );
    let batch = store
        .scan_spec(tid, ReadSpec::all_rows(ReadBound::Range(range)), &view_schema())
        .expect("a range read of a held copy");
    rows_of(&batch)
}

/// [`held`], narrowed to the rows [`v_range`] selects.
fn held_in(store: &mut Mirror, tid: u64, lo: i64, hi: i64) -> BTreeMap<(u64, i64), i64> {
    let mut rows = held(store, tid);
    rows.retain(|&(_, v), _| (lo..=hi).contains(&v));
    rows
}

/// 128 rows at `v = 10 * id`, so a range of a few values stays an index walk.
fn spread_rows() -> Vec<u8> {
    plain(&(0..128).map(|i| (i, 1, i as i64 * 10)).collect::<Vec<_>>())
}

/// A walk through a copy's index answers exactly the rows a scan selects,
/// weight-exact, after a bootstrap and applied rounds and across a checkpoint
/// and reopen, where the index comes back from its own manifest.
#[test]
fn a_copys_index_is_maintained_and_resumes_with_it() {
    let dir = tempfile::tempdir().unwrap();
    let (lo, hi) = (100, 130);
    let indexed = desc_of(TID, &view_schema(), &[&[1]]);
    let want = {
        let mut store = open(&dir);
        store.register(&rel("v"), &indexed).unwrap();
        seed(&mut store, TID, &[&spread_rows()], cursor(4)).unwrap();
        // 11 leaves the range, 12 doubles and 200 enters it.
        let round = plain(&[(11, -1, 110), (12, 1, 120), (200, 1, 125)]);
        store.advance(TID, &[&round], cursor(5)).unwrap();
        let want = held_in(&mut store, TID, lo, hi);
        assert_eq!(
            want,
            BTreeMap::from([((10, 100), 1), ((12, 120), 2), ((13, 130), 1), ((200, 125), 1)]),
        );
        assert_eq!(
            v_range(&mut store, TID, lo, hi),
            want,
            "the walk answers what a scan selects"
        );
        store.checkpoint().unwrap();
        want
    };
    let index_manifest = index_manifest_path(path(&dir), TID, &[1]);
    let published = std::fs::read(&index_manifest).expect("the checkpoint published the index");

    let mut store = open(&dir);
    assert_eq!(store.cursor_of(TID), Some(cursor(5)), "the copy resumed");
    assert_eq!(v_range(&mut store, TID, lo, hi), want, "and answers the same rows");
    assert_eq!(
        std::fs::read(&index_manifest).unwrap(),
        published,
        "the index resumed from its manifest rather than being erased"
    );
}

/// A crash between a checkpoint's two renames leaves a copy published and its
/// index a checkpoint behind. The reopened index is refilled from the copy, so
/// a walk never answers from entries the copy has moved past.
#[test]
fn an_index_a_checkpoint_behind_its_copy_is_refilled() {
    let dir = tempfile::tempdir().unwrap();
    let (lo, hi) = (100, 130);
    let index_manifest = index_manifest_path(path(&dir), TID, &[1]);
    let want = {
        let mut store = open(&dir);
        store
            .register(&rel("v"), &desc_of(TID, &view_schema(), &[&[1]]))
            .unwrap();
        seed(&mut store, TID, &[&spread_rows()], cursor(4)).unwrap();
        store.checkpoint().unwrap();
        let behind = std::fs::read(&index_manifest).unwrap();
        let round = plain(&[(11, -1, 110), (11, 1, 5000), (90, 1, 115), (90, -1, 900)]);
        store.advance(TID, &[&round], cursor(5)).unwrap();
        store.checkpoint().unwrap();
        let want = held_in(&mut store, TID, lo, hi);
        drop(store);
        std::fs::write(&index_manifest, behind).unwrap();
        want
    };
    assert_eq!(
        want,
        BTreeMap::from([((10, 100), 1), ((12, 120), 1), ((13, 130), 1), ((90, 115), 1)]),
    );

    let mut store = open(&dir);
    assert_eq!(store.cursor_of(TID), Some(cursor(5)), "the copy resumed");
    assert_eq!(v_range(&mut store, TID, lo, hi), want);
}

/// An index created or dropped upstream reaches a copy that holds a cursor in
/// place, at the next registration: the copy keeps its rows and its position,
/// and a range read answers the same rows either way.
#[test]
fn a_registration_brings_a_live_copys_indexes_to_the_views() {
    let dir = tempfile::tempdir().unwrap();
    let (lo, hi) = (100, 130);
    let index_manifest = index_manifest_path(path(&dir), TID, &[1]);
    let mut store = open(&dir);
    store.register(&rel("v"), &desc(TID)).unwrap();
    seed(&mut store, TID, &[&spread_rows()], cursor(4)).unwrap();
    let want = held_in(&mut store, TID, lo, hi);
    assert_eq!(want.len(), 4);
    assert_eq!(v_range(&mut store, TID, lo, hi), want, "narrowed from a scan");

    for (indexes, indexed) in [(&[&[1u32][..]][..], true), (&[][..], false)] {
        store
            .register(&rel("v"), &desc_of(TID, &view_schema(), indexes))
            .unwrap();
        assert_eq!(store.cursor_of(TID), Some(cursor(4)), "the copy stands");
        assert_eq!(v_range(&mut store, TID, lo, hi), want, "indexed: {indexed}");
        store.checkpoint().unwrap();
        drop(store);
        store = open(&dir);
        assert_eq!(std::path::Path::new(&index_manifest).exists(), indexed);
        assert_eq!(v_range(&mut store, TID, lo, hi), want, "reopened, indexed: {indexed}");
    }
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
                let mut raw =
                    RelationRegistry::new(&root(path(&dir)), gnitz_zset::schema::Slot::SOLO, Default::default());
                raw.reopen_view(
                    RelationSpec {
                        id: TID,
                        kind: RelationKind::View(gnitz_wire::ViewProps::Plain),
                        schema: make_schema_u64_i64(),
                        placement: gnitz_zset::schema::Placement::Local,
                        pk_repeats: false,
                    },
                    FIRST_GENERATION,
                )
                .unwrap();
                raw.set_caller_record(TID, b"not a mirror record".to_vec()).unwrap();
                raw.checkpoint_ephemeral([], FIRST_GENERATION, |_| true).unwrap();
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
        store.register(&rel("v"), &desc(TID)).unwrap();
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

/// A store leaves another host's relation directories and lock, in the
/// directory it is opened at, as they were.
#[test]
fn a_store_leaves_whatever_else_its_directory_holds() {
    use gnitz_store::relation::{lock_data_dir, relation_dir};
    let dir = tempfile::tempdir().unwrap();
    // A one-worker server's rows for the id the store is about to register, and
    // a relation the store never hears of.
    let theirs = [
        format!("{}/w0of1/manifest.bin", relation_dir(path(&dir), TID)),
        format!("{}/w0of1/shard_1.db", relation_dir(path(&dir), TID)),
        format!("{}/manifest.bin", relation_dir(path(&dir), 1)),
    ];
    for file in &theirs {
        std::fs::create_dir_all(std::path::Path::new(file).parent().unwrap()).unwrap();
        std::fs::write(file, b"theirs").unwrap();
    }

    let mut store = open(&dir);
    store.register(&rel("v"), &desc(TID)).unwrap();
    seed(&mut store, TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();
    store.checkpoint().unwrap();
    let _server = lock_data_dir(path(&dir)).expect("the directory's own lock is free");
    drop(store);
    let mut store = open(&dir);
    store.forget(TID).unwrap();

    for file in &theirs {
        assert_eq!(std::fs::read(file).ok().as_deref(), Some(&b"theirs"[..]), "{file}");
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
    let err = store.refill(TID).expect_err("an erase over a blocked copy must fail");
    assert!(matches!(err, MirrorError::Poisoned(_)), "{err}");
    assert!(store.poisoned().is_some());
    assert_eq!(
        store.cursor_of(TID),
        None,
        "the cursor went before the erase could fail"
    );

    let block = plain(&[(1, 1, 10)]);
    let refused: [(&str, Result<(), MirrorError>); 6] = [
        ("register", store.register(&rel("w"), &desc(OTHER_TID)).map(drop)),
        ("forget", store.forget(OTHER_TID)),
        ("refill", seed(&mut store, TID, &[&block], cursor(5))),
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
    seed(&mut store, TID, &[&plain(&[(1, 1, 10)])], cursor(4)).unwrap();
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
    store.register(&rel("v"), &desc(TID)).unwrap();
    store.refill(TID).unwrap();
    block_copy(path(&dir), TID);
    store.fill(TID, &[&big(0)]).unwrap();
    store
        .seal(TID, cursor(4))
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
