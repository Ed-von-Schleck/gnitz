//! The store: what a host opens, and the `MirrorStore` surface a client drives
//! it through.

use gnitz_zset::schema::SchemaFacts;
use std::collections::HashMap;

use gnitz_core::append_own_regions;
use gnitz_core::{DeltaCursor, MirrorError, MirrorStore, RelDescriptor, RelName, Schema, ZSetBatch};
use gnitz_foundation::env::env_num;
use gnitz_foundation::fault::Seam;
use gnitz_foundation::{gnitz_debug, gnitz_error};
use gnitz_store::relation::{
    lock_data_dir, DirLock, IndexClaim, Relation, RelationKind, RelationRegistry, RelationSpec, StoreConfig,
};
use gnitz_wire::{PkColList, ViewProps};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{Placement, SchemaDescriptor, Slot};

use crate::guard::Guarded;
use crate::record::{descriptor_of_block, index_lists, MirrorRecord};

/// What a store is sized by. `Default` is the production value of every field.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MirrorConfig {
    /// The tuning of the engine stores behind the copies.
    pub store: StoreConfig,
    /// Applied delta bytes after which an apply drives a checkpoint of its own.
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

/// `GNITZ_INJECT_MIRROR_INGEST_PANIC`: panic once on an advance, idle polls
/// included. A panic rather than an `Err`: nothing else reaches
/// [`Guarded::touching`]'s panic arm.
static INGEST_PANIC: Seam = Seam::new("GNITZ_INJECT_MIRROR_INGEST_PANIC");

/// A maintained local copy of one or more views.
///
/// One store holds many, in one data directory, under one lock and one
/// checkpoint; each carries its own cursor and is advanced independently.
///
/// Each copy lives under the **server's** relation id; the store mints none of
/// its own. Every copy is registered as a plain view with no capacity budget,
/// so no copy holds a skeleton row, and owns an index on each column list its
/// view's descriptor named when it was last registered.
pub struct Mirror {
    /// The directory the host named; the copies' is [`ROOT`] under it.
    base_dir: String,
    copies: Guarded<Copies>,
    /// The copy between its erase and its seal, every block of its value so
    /// far landed.
    filling: Option<u64>,
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
        let mut copies = Copies {
            registry,
            records: HashMap::new(),
            generation: 0,
            unpublished: false,
            applied_bytes: 0,
            checkpoint_bytes: config.checkpoint_bytes,
        };
        let mut all_reopened = true;
        for (tid, record) in persisted {
            let reopened = match record.map(|(generation, bytes)| (generation, MirrorRecord::decode(&bytes))) {
                // Reopened from the manifest `rec` was read from, at its generation.
                Ok((generation, Some((rec, schema)))) => {
                    copies.generation = copies.generation.max(generation);
                    copies.enter(tid, rec, schema, Some(generation))
                }
                // Not a record this store wrote; the directory is the orphan sweep's.
                Ok((_, None)) => continue,
                Err(e) => Err(e),
            };
            if let Err(e) = reopened {
                gnitz_debug!("mirror: relation {} did not reopen, so it bootstraps: {}", tid, e);
                all_reopened = false;
            }
        }
        // A failure may be transient, and the sweep would delete that copy.
        if all_reopened {
            if let Err(e) = copies.registry.reclaim_orphan_relation_dirs() {
                gnitz_debug!("mirror: orphan directory sweep failed: {}", e);
            }
        }
        Ok(Mirror {
            base_dir: base_dir.to_string(),
            copies: Guarded::new(copies),
            filling: None,
            _dir_lock: dir_lock,
        })
    }
}

/// The copies and their records.
struct Copies {
    registry: RelationRegistry,
    /// One per relation in `registry`, whose indexes are the record's.
    records: HashMap<u64, MirrorRecord>,
    /// The generation the last checkpoint published every copy and index at. A
    /// reopened index resumes only at its copy's, so one a crash left a
    /// checkpoint behind its copy is refilled.
    generation: u64,
    /// Whether a copy or a record moved since the last checkpoint.
    unpublished: bool,
    applied_bytes: usize,
    checkpoint_bytes: usize,
}

impl Copies {
    /// `tid`'s open copy.
    fn copy(&self, tid: u64) -> &Relation {
        self.registry
            .relation(tid)
            .expect("a mirrored relation's copy is registered")
    }

    fn cursor_of(&self, tid: u64) -> Option<DeltaCursor> {
        self.records.get(&tid).and_then(|r| r.cursor)
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
            kind: RelationKind::View(ViewProps::Plain),
            schema,
            placement: Placement::Local,
            pk_repeats: rec.pk_repeats,
        };
        match resume {
            Some(generation) => self.registry.reopen_view(spec, generation)?,
            None => self.registry.register(spec)?,
        }
        for &cols in &rec.indexes {
            let entered = match resume {
                Some(generation) => self.registry.reopen_index(tid, index_claim(cols), cols, generation),
                None => self.registry.add_index(tid, index_claim(cols), cols),
            };
            if let Err(e) = entered {
                // Not erased: the fault may be transient, and the stores resumable.
                self.registry.unregister(tid);
                return Err(e);
            }
        }
        self.records.insert(tid, rec);
        // An index that did not resume holds rows no manifest does.
        if resume.is_some() {
            self.unpublished |= self.copy(tid).indexes().iter().any(|ix| !ix.resumed());
        }
        Ok(())
    }

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
                    self.unpublished = true;
                }
                if r.indexes != indexes {
                    self.unpublished = true;
                    self.sync_indexes(tid, indexes)?;
                }
            }
            None => {
                self.forget(tid)?;
                self.unpublished = true;
                let rec = MirrorRecord {
                    name: name.clone(),
                    pk_repeats: desc.pk_repeats,
                    indexes,
                    cursor: None,
                    block,
                };
                let schema = descriptor_of_block(&rec.block)?;
                self.enter(tid, rec, schema, None).map_err(MirrorError::Engine)?;
            }
        }
        Ok(())
    }

    /// Bring `tid`'s copy's indexes to `listed`. A copy with a cursor keeps its
    /// rows, a new index filled from them; one without is entered empty, since
    /// its bootstrap erases it, as is one an index could not be added to.
    fn sync_indexes(&mut self, tid: u64, listed: Vec<PkColList>) -> Result<(), MirrorError> {
        let rec = self.records.get_mut(&tid).expect("checked by the caller");
        let held = std::mem::replace(&mut rec.indexes, listed.clone());
        if rec.cursor.is_none() {
            return self.erase(tid);
        }
        for cols in held.iter().filter(|c| !listed.contains(c)) {
            self.registry.release_index(tid, cols.pack());
        }
        for &cols in listed.iter().filter(|c| !held.contains(c)) {
            if let Err(e) = self.registry.add_index(tid, index_claim(cols), cols) {
                gnitz_debug!("mirror: indexing the copy of {} failed, so it reseeds: {}", tid, e);
                return self.erase(tid);
            }
        }
        Ok(())
    }

    /// Drop `tid`'s record, and erase its copy with its directory.
    fn forget(&mut self, tid: u64) -> Result<(), MirrorError> {
        if self.records.remove(&tid).is_none() {
            return Ok(());
        }
        self.unpublished = true;
        self.registry
            .unregister_and_erase(tid)
            .map_err(|e| erase_failed(tid, e))
    }

    /// Drop `tid`'s rows and cursor, keeping its registration.
    fn erase(&mut self, tid: u64) -> Result<(), MirrorError> {
        let Some(mut rec) = self.records.remove(&tid) else {
            return Err(MirrorError::Engine(format!("relation {tid} is not mirrored")));
        };
        rec.cursor = None;
        self.unpublished = true;
        // A rederived open with no resume point erases the copy and its indexes.
        let schema = self.copy(tid).schema();
        self.registry.unregister(tid);
        self.enter(tid, rec, schema, None).map_err(|e| erase_failed(tid, e))
    }

    /// Apply one block to `tid`'s copy. A failed apply erases the copy.
    fn ingest(&mut self, tid: u64, block: &[u8]) -> Result<(), MirrorError> {
        self.applied_bytes += block.len();
        self.unpublished = true;
        let schema = self.copy(tid).schema();
        let applied = Batch::decode_foreign_wal_block(block, &schema)
            .map_err(|e| format!("decoding a block for {tid}: {e}"))
            .and_then(|b| {
                self.registry
                    .ingest(tid, b)
                    .map_err(|e| format!("applying a block to {tid}: {e}"))
            });
        if let Err(why) = applied {
            self.erase(tid)?;
            return Err(MirrorError::Engine(why));
        }
        Ok(())
    }

    /// Set `tid`'s cursor to `at`.
    fn settle(&mut self, tid: u64, at: DeltaCursor) {
        let rec = self.records.get_mut(&tid).expect("checked by the caller");
        if rec.cursor != Some(at) {
            rec.cursor = Some(at);
            self.unpublished = true;
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

    fn advance(&mut self, tid: u64, blocks: &[&[u8]], next: DeltaCursor) -> Result<(), MirrorError> {
        if self.cursor_of(tid).is_none() {
            return Err(MirrorError::Engine(format!(
                "relation {tid}'s copy holds no cursor for deltas to continue"
            )));
        }
        if INGEST_PANIC.take_once() {
            panic!("injected mirror ingest panic");
        }
        for &block in blocks {
            self.ingest(tid, block)?;
        }
        self.settle(tid, next);
        // Not for an advance that carried nothing, which touches no disk.
        if !blocks.is_empty() {
            self.checkpoint_if_due();
        }
        Ok(())
    }

    fn scan_spec(&self, tid: u64, spec: gnitz_wire::ReadSpec, reply_schema: &Schema) -> Result<ZSetBatch, MirrorError> {
        // No copy holds a skeleton row, so no hydrator is needed.
        let batch = self
            .registry
            .scan_spec(tid, spec, reply_schema.layout_digest(), None)
            .map_err(MirrorError::Engine)?;
        let regions = batch.wire_regions();
        let mut rows = ZSetBatch::new(reply_schema);
        // The client's own region append, so a local and a remote reply land in
        // one row layout; the cells are this copy's, checked when it ingested them.
        append_own_regions(&mut rows, &regions, reply_schema).map_err(|e| MirrorError::Engine(e.to_string()))?;
        Ok(rows)
    }

    /// Publish every copy with its record, and its indexes, at one new
    /// generation; nothing when no copy or record moved since the last one. A
    /// failure is not a poisoning: a flush leaves every store holding what it
    /// held.
    fn checkpoint(&mut self) -> Result<(), MirrorError> {
        self.applied_bytes = 0;
        if !self.unpublished {
            return Ok(());
        }
        for (&tid, rec) in &self.records {
            self.registry
                .set_caller_record(tid, rec.encode())
                .map_err(MirrorError::Engine)?;
        }
        self.generation += 1;
        self.registry
            .checkpoint_ephemeral([], self.generation, |_| true)
            .map_err(MirrorError::Engine)?;
        self.unpublished = false;
        Ok(())
    }
}

/// A copy that could not be erased may be half gone.
fn erase_failed(tid: u64, why: String) -> MirrorError {
    MirrorError::Poisoned(format!("erasing the copy of {tid} failed: {why}"))
}

impl MirrorStore for Mirror {
    fn base_dir(&self) -> &str {
        &self.base_dir
    }

    fn register(&mut self, name: &RelName, desc: &RelDescriptor) -> Result<(), MirrorError> {
        self.copies.touching("registering a view", |c| c.register(name, desc))
    }

    fn forget(&mut self, tid: u64) -> Result<(), MirrorError> {
        self.copies.touching("forgetting a copy", |c| c.forget(tid))
    }

    fn refill(&mut self, tid: u64) -> Result<(), MirrorError> {
        self.filling = None;
        self.copies.touching("erasing a copy", |c| c.erase(tid))?;
        self.filling = Some(tid);
        Ok(())
    }

    fn fill(&mut self, tid: u64, blocks: &[&[u8]]) -> Result<(), MirrorError> {
        self.being_filled(tid)?;
        let taken = self.copies.touching("filling a copy", |c| {
            blocks.iter().try_for_each(|block| c.ingest(tid, block))
        });
        // A block that failed took the blocks before it along.
        if taken.is_err() {
            self.filling = None;
        }
        taken
    }

    fn seal(&mut self, tid: u64, cursor: DeltaCursor) -> Result<(), MirrorError> {
        self.being_filled(tid)?;
        self.filling = None;
        self.copies.touching("sealing a copy", |c| {
            c.settle(tid, cursor);
            c.checkpoint_if_due();
            Ok(())
        })
    }

    fn advance(&mut self, tid: u64, blocks: &[&[u8]], next: DeltaCursor) -> Result<(), MirrorError> {
        self.copies
            .touching("advancing a copy", |c| c.advance(tid, blocks, next))
    }

    fn scan_spec(
        &mut self,
        tid: u64,
        spec: gnitz_wire::ReadSpec,
        reply_schema: &Schema,
    ) -> Result<ZSetBatch, MirrorError> {
        self.copies
            .touching("running a read spec", |c| c.scan_spec(tid, spec, reply_schema))
    }

    fn cursor_of(&self, tid: u64) -> Option<DeltaCursor> {
        self.copies.even_if_poisoned().cursor_of(tid)
    }

    fn checkpoint(&mut self) -> Result<(), MirrorError> {
        self.copies.touching("checkpointing", Copies::checkpoint)
    }

    fn poisoned(&self) -> Option<&str> {
        self.copies.poisoned()
    }
}

impl Mirror {
    /// `tid` is the copy being filled.
    fn being_filled(&self, tid: u64) -> Result<(), MirrorError> {
        match self.filling == Some(tid) {
            true => Ok(()),
            false => Err(MirrorError::Engine(format!(
                "relation {tid}'s copy is not being refilled"
            ))),
        }
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
