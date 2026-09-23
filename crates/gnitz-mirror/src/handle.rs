//! The store: what a host opens, and the `MirrorStore` surface a client drives
//! it through.

use gnitz_zset::schema::SchemaFacts;
use std::collections::HashMap;

use gnitz_core::append_own_regions;
use gnitz_core::{DeltaCursor, Invalidate, MirrorError, MirrorStore, Refill, Schema, ZSetBatch};
use gnitz_foundation::env::env_num;
use gnitz_foundation::fault::Seam;
use gnitz_foundation::{gnitz_debug, gnitz_error};
use gnitz_store::relation::{
    lock_data_dir, DirLock, Relation, RelationKind, RelationRegistry, RelationSpec, StoreConfig,
};
use gnitz_wire::ViewProps;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{Placement, SchemaDescriptor, Slot};

use crate::guard::Guarded;
use crate::record::{descriptor_of_block, MirrorRecord};

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

/// The one generation a mirror's manifests carry.
const GENERATION: u64 = 0;

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
/// its own. Every copy is registered as a plain view — no capacity budget, no
/// index — so no copy holds a skeleton row, and a `Range` bound over non-PK
/// columns is served by the full scan narrowed to the walk's rows.
pub struct Mirror {
    /// The directory the host named; the copies' is [`ROOT`] under it.
    base_dir: String,
    copies: Guarded<Copies>,
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
            applied_bytes: 0,
            checkpoint_bytes: config.checkpoint_bytes,
        };
        let mut all_reopened = true;
        for (tid, record) in persisted {
            let reopened = match record.map(|bytes| MirrorRecord::decode(&bytes)) {
                // Reopened from the manifest `rec` was read from.
                Ok(Some((rec, schema))) => copies
                    .registry
                    .reopen_view(copy_spec(tid, schema), GENERATION)
                    .map(|()| {
                        copies.records.insert(tid, rec);
                    }),
                // Not a record this store wrote; the directory is the orphan sweep's.
                Ok(None) => continue,
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
            _dir_lock: dir_lock,
        })
    }
}

/// The copies and their records.
struct Copies {
    registry: RelationRegistry,
    /// One per relation in `registry`.
    records: HashMap<u64, MirrorRecord>,
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

    fn register(
        &mut self,
        tid: u64,
        schema_name: &str,
        name: &str,
        schema: &Schema,
    ) -> Result<Option<u64>, MirrorError> {
        let block = schema.to_block();
        // This name at another id was renamed or recreated upstream.
        let renamed = self
            .records
            .iter()
            .find(|(&t, r)| t != tid && r.schema_name == schema_name && r.name == name)
            .map(|(&t, _)| t);
        if let Some(old) = renamed {
            self.invalidate(old, Invalidate::Registration)?;
        }
        match self.records.get_mut(&tid).filter(|r| r.block == block) {
            // A rename upstream keeps the id; a stale name here would match a
            // later view created under it.
            Some(r) => {
                r.schema_name = schema_name.to_string();
                r.name = name.to_string();
            }
            None => {
                self.invalidate(tid, Invalidate::Registration)?;
                self.registry
                    .register(copy_spec(tid, descriptor_of_block(&block)?))
                    .map_err(MirrorError::Engine)?;
                let rec = MirrorRecord {
                    schema_name: schema_name.to_string(),
                    name: name.to_string(),
                    block,
                    cursor: None,
                };
                self.records.insert(tid, rec);
            }
        }
        Ok(renamed)
    }

    fn invalidate(&mut self, tid: u64, level: Invalidate) -> Result<(), MirrorError> {
        let Some(rec) = self.records.get_mut(&tid) else {
            return Ok(());
        };
        rec.cursor = None;
        if level == Invalidate::Registration {
            self.records.remove(&tid);
            self.registry
                .unregister_and_erase(tid)
                .map_err(|e| erase_failed(tid, e))?;
        }
        Ok(())
    }

    /// Drop `tid`'s rows and cursor, keeping its registration.
    fn erase(&mut self, tid: u64) -> Result<(), MirrorError> {
        let Some(rec) = self.records.get_mut(&tid) else {
            return Err(MirrorError::Engine(format!("relation {tid} is not mirrored")));
        };
        rec.cursor = None;
        // A rederived open with no resume point erases the copy.
        let spec = copy_spec(tid, self.copy(tid).schema());
        self.registry.unregister(tid);
        self.registry.register(spec).map_err(|e| erase_failed(tid, e))
    }

    /// Apply one block to `tid`'s copy. A failed apply erases the copy.
    fn ingest(&mut self, tid: u64, block: &[u8]) -> Result<(), MirrorError> {
        self.applied_bytes += block.len();
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

    /// Set `tid`'s cursor to `at`, then checkpoint if due. A failed
    /// auto-checkpoint is logged, and the next one retries.
    fn settle(&mut self, tid: u64, at: DeltaCursor) {
        self.records.get_mut(&tid).expect("checked by the caller").cursor = Some(at);
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

    /// Publish every copy with its record. A failure is not a poisoning: a
    /// flush leaves every store holding what it held.
    fn checkpoint(&mut self) -> Result<(), MirrorError> {
        self.applied_bytes = 0;
        for (&tid, rec) in &self.records {
            self.registry
                .set_caller_record(tid, rec.encode())
                .map_err(MirrorError::Engine)?;
        }
        self.registry
            .checkpoint_ephemeral([], GENERATION)
            .map_err(MirrorError::Engine)
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

    fn register(
        &mut self,
        tid: u64,
        schema_name: &str,
        name: &str,
        schema: &Schema,
    ) -> Result<Option<u64>, MirrorError> {
        self.copies
            .touching("registering a view", |c| c.register(tid, schema_name, name, schema))
    }

    fn invalidate(&mut self, tid: u64, level: Invalidate) -> Result<(), MirrorError> {
        self.copies
            .touching("invalidating a copy", |c| c.invalidate(tid, level))
    }

    fn refill(&mut self, tid: u64) -> Result<Box<dyn Refill + '_>, MirrorError> {
        self.copies.touching("erasing a copy", |c| c.erase(tid))?;
        Ok(Box::new(Filling {
            copies: &mut self.copies,
            tid,
            torn: false,
        }))
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

    fn clear_cursors(&mut self) {
        for r in self.copies.even_if_poisoned_mut().records.values_mut() {
            r.cursor = None;
        }
    }

    fn checkpoint(&mut self) -> Result<(), MirrorError> {
        self.copies.touching("checkpointing", Copies::checkpoint)
    }

    fn poisoned(&self) -> Option<&str> {
        self.copies.poisoned()
    }
}

/// One copy between its erase and its seal.
struct Filling<'a> {
    copies: &'a mut Guarded<Copies>,
    tid: u64,
    /// A block failed and took the blocks before it along.
    torn: bool,
}

impl Filling<'_> {
    fn intact(&self) -> Result<(), MirrorError> {
        if self.torn {
            return Err(MirrorError::Engine(format!(
                "a block of relation {}'s whole value failed, so the copy holds none of it",
                self.tid
            )));
        }
        Ok(())
    }
}

impl Refill for Filling<'_> {
    fn block(&mut self, block: &[u8]) -> Result<(), MirrorError> {
        self.intact()?;
        let tid = self.tid;
        let taken = self.copies.touching("filling a copy", |c| c.ingest(tid, block));
        self.torn = taken.is_err();
        taken
    }

    fn seal(self: Box<Self>, cursor: DeltaCursor) -> Result<(), MirrorError> {
        self.intact()?;
        let tid = self.tid;
        self.copies.touching("sealing a copy", |c| {
            c.settle(tid, cursor);
            Ok(())
        })
    }
}

/// The registration of `tid`'s copy: a plain view in `schema`'s layout.
fn copy_spec(tid: u64, schema: SchemaDescriptor) -> RelationSpec {
    RelationSpec {
        id: tid,
        kind: RelationKind::View(ViewProps::Plain),
        schema,
        placement: Placement::Local,
    }
}
