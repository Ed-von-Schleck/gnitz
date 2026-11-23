//! The store: what a host opens, and the `MirrorStore` surface a client drives
//! it through.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use gnitz_core::{MirrorStore, RelDescriptor, RelName, Schema, ZSetBatch};
use gnitz_foundation::env::env_num;
use gnitz_foundation::fault::Switch;
use gnitz_foundation::{gnitz_debug, gnitz_error};
use gnitz_store::relation::{
    DirLock, IndexClaim, RelationKind, RelationRegistry, RelationSpec, StateLayout, StoreConfig, lock_data_dir,
};
use gnitz_wire::multi_frame::DeltaCursor;
use gnitz_wire::{PkColList, ViewBudget};
use gnitz_zset::algebra::{Placement, Slot};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;
use std::rc::Rc;

use crate::record::{Feed, MirrorRecord};

/// What a store is sized by. `Default` is the production value of every field.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MirrorConfig {
    /// The tuning of the engine stores behind the copies.
    pub store: StoreConfig,
    /// Applied block bytes — a bootstrap's value or a feed's deltas — after
    /// which a sealing fill or an advance drives a checkpoint of its own.
    pub checkpoint_bytes: usize,
}

impl Default for MirrorConfig {
    fn default() -> Self {
        MirrorConfig {
            store: StoreConfig::default(),
            checkpoint_bytes: 64 * 1024 * 1024,
        }
    }
}

impl MirrorConfig {
    /// Every field from its `GNITZ_MIRROR_*` override — [`StoreConfig::from_env`]'s
    /// three under that prefix, and `GNITZ_MIRROR_CHECKPOINT_BYTES` — each falling
    /// back to [`Default`].
    pub fn from_env() -> Self {
        MirrorConfig {
            store: StoreConfig::from_env("GNITZ_MIRROR_"),
            checkpoint_bytes: env_num("GNITZ_MIRROR_CHECKPOINT_BYTES", Self::default().checkpoint_bytes),
        }
    }
}

/// The store's directory under the one a host names.
const ROOT: &str = "_mirror";

/// `GNITZ_INJECT_MIRROR_INGEST_PANIC`: panic once on an advance or a cursor
/// move. A panic rather than an `Err`: nothing else poisons the client's
/// lock on the store.
static INGEST_PANIC: Switch = Switch::new("GNITZ_INJECT_MIRROR_INGEST_PANIC");

/// A maintained local copy of one or more views.
///
/// One store holds many, in one data directory, under one lock and one
/// checkpoint; each carries its own cursor and is advanced independently.
///
/// Each copy lives under its descriptor's id — the server's relation id for a
/// view, the client's alias id for an alias; the store mints none of its own.
/// Every copy is registered as a plain view with no capacity budget, so no copy
/// holds a skeleton row, and owns an index on each column list its descriptor
/// named when it was last registered.
pub struct Mirror {
    registry: RelationRegistry,
    /// One per relation in `registry`, whose indexes are the record's.
    records: HashMap<u64, MirrorRecord>,
    /// The highest generation any copy's manifest carries; a checkpoint
    /// publishes the copies that moved at one above it. A reopened index
    /// resumes only at its own copy's, so one a crash left behind its copy is
    /// refilled.
    generation: u64,
    /// The copies whose record or stores moved since they were last
    /// published; each is in `records`.
    unpublished: HashSet<u64>,
    applied_bytes: usize,
    checkpoint_bytes: usize,
    _dir_lock: DirLock,
}

// SAFETY: nothing public hands out an `Rc`. `open` and `impl MirrorStore` are
// all there is, and their returns are `gnitz-core` types and primitives.
// Condition: `RelationRegistry`'s "Thread contract".
unsafe impl Send for Mirror {}

impl Mirror {
    /// Open (or create) a store at `base_dir`. Fails if another store, in this
    /// process or another, holds it.
    pub fn open(base_dir: &str, config: MirrorConfig) -> Result<Self, String> {
        let root = format!("{base_dir}/{ROOT}");
        let dir_lock = lock_data_dir(&root)?;
        let registry = RelationRegistry::new(&root, Slot::SOLO, config.store);
        let persisted = registry.persisted_records()?;
        let mut store = Mirror {
            registry,
            records: HashMap::new(),
            generation: 0,
            unpublished: HashSet::new(),
            applied_bytes: 0,
            checkpoint_bytes: config.checkpoint_bytes,
            _dir_lock: dir_lock,
        };
        let mut all_reopened = true;
        for (tid, persisted) in persisted {
            let reopened = persisted.and_then(|(generation, bytes)| {
                // Not a record this store wrote; the directory is the orphan sweep's.
                let Some((rec, schema)) = MirrorRecord::decode(&bytes) else {
                    return Ok(());
                };
                store.generation = store.generation.max(generation);
                // Reopened from the manifest `rec` was read from, at its generation.
                store.enter(tid, rec, schema, Some(generation))
            });
            if let Err(e) = reopened {
                gnitz_debug!("mirror: relation {} did not reopen, so it bootstraps: {}", tid, e);
                all_reopened = false;
            }
        }
        // A failure may be transient, and the sweep would delete that copy.
        if all_reopened && let Err(e) = store.registry.reclaim_orphan_relation_dirs() {
            gnitz_debug!("mirror: orphan directory sweep failed: {}", e);
        }
        Ok(store)
    }

    /// `tid`'s record, to change: the next checkpoint publishes it.
    fn touch(&mut self, tid: u64) -> &mut MirrorRecord {
        self.unpublished.insert(tid);
        self.records.get_mut(&tid).expect("a mirrored relation is recorded")
    }

    /// Open `tid`'s copy and the indexes `rec` lists, and record it: from the
    /// manifests at `resume`, or empty. On `Err` the copy is not entered.
    fn enter(
        &mut self,
        tid: u64,
        rec: MirrorRecord,
        schema: SchemaDescriptor,
        resume: Option<u64>,
    ) -> Result<(), String> {
        let spec = RelationSpec {
            id: tid,
            kind: RelationKind::View(ViewBudget::Plain),
            placement: Placement::Local,
            pk_repeats: rec.pk_repeats,
            compress: false,
        };
        let indexes: Vec<_> = rec.indexes.iter().map(|&cols| (index_claim(cols), cols)).collect();
        // A copy keeps no operator traces. On `Err` nothing is entered, and
        // nothing erased: the fault may be transient, and the stores resumable.
        let filled = self
            .registry
            .register_at(spec, schema, &indexes, StateLayout::default(), resume)?;
        self.records.insert(tid, rec);
        // A fresh copy has no manifest, and an index that did not resume holds
        // rows no manifest does.
        if resume.is_none() || filled > 0 {
            self.unpublished.insert(tid);
        }
        Ok(())
    }

    /// Drop `tid`'s rows, its indexes' entries and its value, keeping its
    /// registration; the next checkpoint publishes it empty.
    fn erase(&mut self, tid: u64) {
        self.touch(tid).feed = Feed::Empty;
        self.registry
            .clear_rows(tid)
            .expect("a mirrored relation's copy is registered");
    }

    /// Apply a train's blocks to `tid`'s copy as one batch: every block is
    /// validated before any is applied. A failed train erases the copy.
    fn ingest(&mut self, tid: u64, blocks: &[&[u8]]) -> Result<(), String> {
        if blocks.is_empty() {
            return Ok(());
        }
        self.applied_bytes += blocks.iter().map(|b| b.len()).sum::<usize>();
        self.unpublished.insert(tid);
        let schema = self
            .registry
            .relation(tid)
            .expect("a mirrored relation's copy is registered")
            .schema();
        let applied = Batch::decode_foreign_wal_blocks(blocks, &schema)
            .map_err(|e| format!("decoding a block for {tid}: {e}"))
            .and_then(|b| {
                self.registry
                    .ingest(tid, b)
                    .map_err(|e| format!("applying the blocks to {tid}: {e}"))
            });
        applied.inspect_err(|_| self.erase(tid))
    }

    /// Set `tid`'s value to its view's at `at`.
    fn settle(&mut self, tid: u64, at: DeltaCursor) {
        if self.records[&tid].feed != Feed::At(at) {
            self.touch(tid).feed = Feed::At(at);
        }
    }

    /// Checkpoint once enough bytes were applied since the last. A failed
    /// auto-checkpoint is logged, and the next one retries.
    fn checkpoint_if_due(&mut self) {
        if self.applied_bytes >= self.checkpoint_bytes
            && let Err(e) = self.checkpoint()
        {
            gnitz_error!(
                "mirror: auto-checkpoint failed: {} — the copies stand; the next checkpoint retries",
                e
            );
        }
    }
}

impl MirrorStore for Mirror {
    /// Bring `desc.tid`'s copy to `desc`: kept in place, cursor and all, while
    /// it is the same relation, its indexes brought to the ones `desc` names;
    /// entered empty otherwise.
    fn register(&mut self, name: &RelName, desc: &RelDescriptor) -> Result<(), String> {
        let tid = desc.tid;
        let record = desc.schema.shape_record();
        // Ascending, none twice: canonical, as the record codec requires.
        let mut indexes: Vec<PkColList> = desc.indexes.iter().map(|ix| ix.cols).collect();
        indexes.sort_unstable();
        indexes.dedup();
        // This name at another id was renamed or recreated upstream.
        let renamed = self
            .records
            .iter()
            .find(|&(&t, r)| t != tid && r.name == *name)
            .map(|(&t, _)| t);
        if let Some(old) = renamed {
            self.forget(old)?;
        }
        let same = |r: &&mut MirrorRecord| *r.schema == *record && r.pk_repeats == desc.pk_repeats;
        match self.records.get_mut(&tid).filter(same) {
            // A rename upstream keeps the id; a stale name here would match a
            // later view created under it.
            Some(r) => {
                if r.name != *name {
                    r.name = name.clone();
                    self.unpublished.insert(tid);
                }
                if r.indexes != indexes {
                    self.unpublished.insert(tid);
                    for &cols in r.indexes.iter().filter(|c| !indexes.contains(c)) {
                        self.registry.release_index(tid, index_claim(cols));
                    }
                    // Kept in order, so canonical as the record codec requires.
                    r.indexes.retain(|c| indexes.contains(c));
                    let missing: Vec<_> = indexes
                        .iter()
                        .filter(|c| !r.indexes.contains(c))
                        .map(|&cols| (index_claim(cols), cols))
                        .collect();
                    // Filled from the rows the copy holds. If they cannot be
                    // added the copy is erased and the registration fails, so
                    // the next one retries them.
                    if let Err(e) = self.registry.add_indexes(tid, &missing) {
                        self.erase(tid);
                        return Err(format!("indexing the copy of {tid}: {e}"));
                    }
                    r.indexes = indexes;
                }
            }
            None => {
                self.forget(tid)?;
                let rec = MirrorRecord {
                    name: name.clone(),
                    pk_repeats: desc.pk_repeats,
                    indexes,
                    feed: Feed::Empty,
                    schema: record.to_vec(),
                };
                self.enter(tid, rec, *desc.schema.layout(), None)?;
            }
        }
        Ok(())
    }

    /// Drop `tid`'s record, and erase its copy with its directory. On `Err` the
    /// copy is gone from this store; a reopen may find it at its last
    /// checkpoint.
    fn forget(&mut self, tid: u64) -> Result<(), String> {
        if self.records.remove(&tid).is_none() {
            return Ok(());
        }
        self.unpublished.remove(&tid);
        self.registry
            .unregister_and_erase(tid)
            .map_err(|e| format!("erasing the copy of {tid} failed: {e}"))
    }

    fn refill(&mut self, name: &RelName, desc: &RelDescriptor) -> Result<(), String> {
        self.register(name, desc)?;
        self.erase(desc.tid);
        self.touch(desc.tid).feed = Feed::Filling;
        Ok(())
    }

    fn fill(&mut self, tid: u64, blocks: &[&[u8]], sealed_at: Option<DeltaCursor>) -> Result<(), String> {
        if self.records.get(&tid).map(|r| r.feed) != Some(Feed::Filling) {
            return Err(format!("relation {tid}'s copy is not being refilled"));
        }
        // Block by block: a refill's blocks are large and each arrives in
        // order, so each is certified in place, where one batch would merge
        // them. A failed block erases the copy, which ends the refill.
        for block in blocks {
            self.ingest(tid, std::slice::from_ref(block))?;
        }
        if let Some(cursor) = sealed_at {
            self.settle(tid, cursor);
            self.checkpoint_if_due();
        }
        Ok(())
    }

    fn advance(&mut self, tid: u64, blocks: &[&[u8]], next: DeltaCursor) -> Result<(), String> {
        if self.cursor_of(tid).is_none() {
            return Err(format!("relation {tid}'s copy holds no cursor for deltas to continue"));
        }
        if INGEST_PANIC.take_once() {
            panic!("injected mirror ingest panic");
        }
        self.ingest(tid, blocks)?;
        self.settle(tid, next);
        // A cursor move applies nothing and runs on the client's thread.
        if !blocks.is_empty() {
            self.checkpoint_if_due();
        }
        Ok(())
    }

    fn scan_spec(
        &mut self,
        tid: u64,
        spec: gnitz_wire::ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> Result<ZSetBatch, String> {
        // No copy holds a skeleton row, so no hydrator is needed.
        // The reply is the copy's own batch, in the regions `reply_schema` lays out: its
        // cells were checked when the copy ingested them.
        let batch = self
            .registry
            .scan_spec(tid, spec, reply_schema.layout().layout_digest(), None)?;
        Ok(ZSetBatch::of(reply_schema, Rc::unwrap_or_clone(batch)))
    }

    fn cursor_of(&self, tid: u64) -> Option<DeltaCursor> {
        self.records.get(&tid)?.feed.cursor()
    }

    /// Publish every copy that moved since it was last published, with its
    /// record and its indexes, at one new generation; nothing when none moved.
    /// A failure leaves every store holding what it held.
    fn checkpoint(&mut self) -> Result<(), String> {
        self.applied_bytes = 0;
        if self.unpublished.is_empty() {
            return Ok(());
        }
        for &tid in &self.unpublished {
            self.registry.set_caller_record(tid, self.records[&tid].encode())?;
        }
        self.generation += 1;
        let moved = &self.unpublished;
        self.registry
            .checkpoint_ephemeral([], self.generation, |tid| moved.contains(&tid))?;
        self.unpublished.clear();
        Ok(())
    }
}

/// How a copy claims its index on `cols`: a copy's index is identified by its
/// column list, so the list's packing is its id.
fn index_claim(cols: PkColList) -> IndexClaim {
    IndexClaim::Index { id: cols.pack(), unique: false }
}

#[cfg(test)]
#[path = "benches/handle.rs"]
mod bench;
