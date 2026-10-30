//! The store: what a host opens, and the `MirrorStore` surface a client drives
//! it through.
use std::collections::{HashMap, HashSet};

use gnitz_core::append_own_regions;
use gnitz_core::{DeltaCursor, MirrorError, MirrorStore, RelDescriptor, RelName, Schema, ZSetBatch};
use gnitz_foundation::env::env_num;
use gnitz_foundation::fault::Seam;
use gnitz_foundation::{gnitz_debug, gnitz_error};
use gnitz_store::relation::{
    lock_data_dir, DirLock, IndexClaim, RelationKind, RelationRegistry, RelationSpec, StoreConfig,
};
use gnitz_wire::{PkColList, ViewBudget};
use gnitz_zset::algebra::{Placement, Slot};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

use crate::record::{index_lists, Feed, MirrorRecord};

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
static INGEST_PANIC: Seam = Seam::new("GNITZ_INJECT_MIRROR_INGEST_PANIC");

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
    pub fn open(base_dir: &str, config: MirrorConfig) -> Result<Self, MirrorError> {
        let root = format!("{base_dir}/{ROOT}");
        let dir_lock = lock_data_dir(&root).map_err(MirrorError::Engine)?;
        let registry = RelationRegistry::new(&root, Slot::SOLO, config.store);
        let persisted = registry.persisted_records().map_err(MirrorError::Engine)?;
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
        if all_reopened {
            // A copy carries no operator state, so no trace child is its own.
            if let Err(e) = store.registry.reclaim_orphan_relation_dirs(|_| None) {
                gnitz_debug!("mirror: orphan directory sweep failed: {}", e);
            }
        }
        Ok(store)
    }

    /// `tid`'s record.
    fn record(&mut self, tid: u64) -> &mut MirrorRecord {
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
        // On `Err` nothing is entered, and nothing erased: the fault may be
        // transient, and the stores resumable.
        let filled = self.registry.register_at(spec, schema, &indexes, resume)?;
        self.records.insert(tid, rec);
        // A fresh copy has no manifest, and an index that did not resume holds
        // rows no manifest does.
        if resume.is_none() || filled > 0 {
            self.unpublished.insert(tid);
        }
        Ok(())
    }

    /// Bring `tid`'s copy's indexes to `listed`, the new ones filled from the
    /// rows the copy holds. If they cannot be added the copy is erased and the
    /// registration fails, so the next one retries them.
    fn sync_indexes(&mut self, tid: u64, listed: Vec<PkColList>) -> Result<(), MirrorError> {
        let rec = self.records.get_mut(&tid).expect("checked by the caller");
        for &cols in rec.indexes.iter().filter(|c| !listed.contains(c)) {
            self.registry.release_index(tid, index_claim(cols));
        }
        // Kept in order, so canonical as the record codec requires.
        rec.indexes.retain(|c| listed.contains(c));
        let missing: Vec<_> = listed
            .iter()
            .filter(|c| !rec.indexes.contains(c))
            .map(|&cols| (index_claim(cols), cols))
            .collect();
        if let Err(e) = self.registry.add_indexes(tid, &missing) {
            self.erase(tid);
            return Err(MirrorError::Engine(format!("indexing the copy of {tid}: {e}")));
        }
        rec.indexes = listed;
        Ok(())
    }

    /// Drop `tid`'s rows, its indexes' entries and its value, keeping its
    /// registration; the next checkpoint publishes it empty.
    fn erase(&mut self, tid: u64) {
        self.record(tid).feed = Feed::Empty;
        self.unpublished.insert(tid);
        self.registry
            .clear_rows(tid)
            .expect("a mirrored relation's copy is registered");
    }

    /// Apply one block to `tid`'s copy. A failed apply erases the copy.
    fn ingest(&mut self, tid: u64, block: &[u8]) -> Result<(), MirrorError> {
        self.applied_bytes += block.len();
        self.unpublished.insert(tid);
        let schema = self
            .registry
            .relation(tid)
            .expect("a mirrored relation's copy is registered")
            .schema();
        let applied = Batch::decode_foreign_wal_block(block, &schema)
            .map_err(|e| format!("decoding a block for {tid}: {e}"))
            .and_then(|b| {
                self.registry
                    .ingest(tid, b)
                    .map_err(|e| format!("applying a block to {tid}: {e}"))
            });
        applied.map_err(|why| {
            self.erase(tid);
            MirrorError::Engine(why)
        })
    }

    /// Refuse deltas for a copy of `tid` that holds no cursor to continue. Where
    /// [`INGEST_PANIC`] fires.
    fn continuing(&self, tid: u64) -> Result<(), MirrorError> {
        if self.cursor_of(tid).is_none() {
            return Err(MirrorError::Engine(format!(
                "relation {tid}'s copy holds no cursor for deltas to continue"
            )));
        }
        if INGEST_PANIC.take_once() {
            panic!("injected mirror ingest panic");
        }
        Ok(())
    }

    /// Set `tid`'s value to its view's at `at`.
    fn settle(&mut self, tid: u64, at: DeltaCursor) {
        let rec = self.record(tid);
        if rec.feed != Feed::At(at) {
            rec.feed = Feed::At(at);
            self.unpublished.insert(tid);
        }
    }

    /// Checkpoint once enough bytes were applied since the last. A failed
    /// auto-checkpoint is logged, and the next one retries.
    fn checkpoint_if_due(&mut self) {
        if self.applied_bytes >= self.checkpoint_bytes {
            if let Err(e) = self.checkpoint() {
                gnitz_error!(
                    "mirror: auto-checkpoint failed: {} — the copies stand; the next checkpoint retries",
                    e
                );
            }
        }
    }

    /// `tid`'s feed; [`Feed::Empty`] for a relation the store does not hold.
    fn feed_of(&self, tid: u64) -> Feed {
        self.records.get(&tid).map_or(Feed::Empty, |r| r.feed)
    }
}

impl MirrorStore for Mirror {
    /// Bring `desc.tid`'s copy to `desc`: kept in place, cursor and all, while
    /// it is the same relation, its indexes brought to the ones `desc` names;
    /// entered empty otherwise.
    fn register(&mut self, name: &RelName, desc: &RelDescriptor) -> Result<(), MirrorError> {
        let tid = desc.tid;
        let block = desc.schema.to_block();
        let indexes = index_lists(desc);
        // This name at another id was renamed or recreated upstream.
        let renamed = self
            .records
            .iter()
            .find(|(&t, r)| t != tid && r.name == *name)
            .map(|(&t, _)| t);
        if let Some(old) = renamed {
            self.forget(old)?;
        }
        let same = |r: &&mut MirrorRecord| r.block == block && r.pk_repeats == desc.pk_repeats;
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
                    self.sync_indexes(tid, indexes)?;
                }
            }
            None => {
                self.forget(tid)?;
                let rec = MirrorRecord {
                    name: name.clone(),
                    pk_repeats: desc.pk_repeats,
                    indexes,
                    feed: Feed::Empty,
                    block,
                };
                self.enter(tid, rec, *desc.schema.layout(), None)
                    .map_err(MirrorError::Engine)?;
            }
        }
        Ok(())
    }

    /// Drop `tid`'s record, and erase its copy with its directory. On `Err` the
    /// copy is gone from this store; a reopen may find it at its last
    /// checkpoint.
    fn forget(&mut self, tid: u64) -> Result<(), MirrorError> {
        if self.records.remove(&tid).is_none() {
            return Ok(());
        }
        self.unpublished.remove(&tid);
        self.registry
            .unregister_and_erase(tid)
            .map_err(|e| MirrorError::Engine(format!("erasing the copy of {tid} failed: {e}")))
    }

    fn refill(&mut self, name: &RelName, desc: &RelDescriptor) -> Result<(), MirrorError> {
        self.register(name, desc)?;
        self.erase(desc.tid);
        self.record(desc.tid).feed = Feed::Filling;
        Ok(())
    }

    fn fill(&mut self, tid: u64, blocks: &[&[u8]], sealed_at: Option<DeltaCursor>) -> Result<(), MirrorError> {
        if self.feed_of(tid) != Feed::Filling {
            return Err(MirrorError::Engine(format!(
                "relation {tid}'s copy is not being refilled"
            )));
        }
        // A block that fails erases the copy, which ends the refill.
        for &block in blocks {
            self.ingest(tid, block)?;
        }
        if let Some(cursor) = sealed_at {
            self.settle(tid, cursor);
            self.checkpoint_if_due();
        }
        Ok(())
    }

    fn advance(&mut self, tid: u64, blocks: &[&[u8]], next: DeltaCursor) -> Result<(), MirrorError> {
        self.continuing(tid)?;
        for &block in blocks {
            self.ingest(tid, block)?;
        }
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
        reply_schema: &Schema,
    ) -> Result<ZSetBatch, MirrorError> {
        // No copy holds a skeleton row, so no hydrator is needed.
        let batch = self
            .registry
            .scan_spec(tid, spec, reply_schema.layout().layout_digest(), None)
            .map_err(MirrorError::Engine)?;
        let regions = batch.wire_regions();
        let mut rows = ZSetBatch::new(reply_schema);
        // The client's own region append, so a local and a remote reply land in
        // one row layout; the cells are this copy's, checked when it ingested them.
        append_own_regions(&mut rows, &regions, reply_schema).map_err(|e| MirrorError::Engine(e.to_string()))?;
        Ok(rows)
    }

    fn cursor_of(&self, tid: u64) -> Option<DeltaCursor> {
        self.feed_of(tid).cursor()
    }

    /// Publish every copy that moved since it was last published, with its
    /// record and its indexes, at one new generation; nothing when none moved.
    /// A failure leaves every store holding what it held.
    fn checkpoint(&mut self) -> Result<(), MirrorError> {
        self.applied_bytes = 0;
        if self.unpublished.is_empty() {
            return Ok(());
        }
        for &tid in &self.unpublished {
            self.registry
                .set_caller_record(tid, self.records[&tid].encode())
                .map_err(MirrorError::Engine)?;
        }
        self.generation += 1;
        let moved = &self.unpublished;
        self.registry
            .checkpoint_ephemeral([], self.generation, |tid| moved.contains(&tid))
            .map_err(MirrorError::Engine)?;
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
