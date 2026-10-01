//! The store: what a host opens, and the `MirrorStore` surface a client drives
//! it through.

use gnitz_store::schema::SchemaFacts;
use std::collections::HashMap;

use gnitz_core::decode_regions_into;
use gnitz_core::{DeltaCursor, Invalidate, MirrorError, MirrorStore, Schema, ZSetBatch};
use gnitz_foundation::env::env_num;
use gnitz_foundation::fault::Seam;
use gnitz_foundation::{gnitz_debug, gnitz_error};
use gnitz_store::relation::{
    lock_data_dir, DirLock, Relation, RelationKind, RelationRegistry, RelationSpec, StoreConfig,
};
use gnitz_store::schema::{SchemaDescriptor, Slot};
use gnitz_store::storage::Batch;
use gnitz_wire::ViewProps;

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

/// `GNITZ_INJECT_MIRROR_INGEST_PANIC`: panic once on an advance, idle polls
/// included. A panic rather than an `Err`: nothing else reaches
/// [`Mirror::touching`]'s guard arm.
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
    registry: RelationRegistry,
    /// One per relation in `registry`.
    records: HashMap<u64, MirrorRecord>,
    _dir_lock: DirLock,
    poison: Option<String>,
    applied_bytes: usize,
    checkpoint_bytes: usize,
}

// SAFETY: nothing public hands out an `Rc`. `open` and `impl MirrorStore` are
// all there is, and their returns are `gnitz-core` types and primitives.
// Condition: `RelationRegistry`'s "Thread contract".
unsafe impl Send for Mirror {}

impl Mirror {
    /// Open (or create) a store at `base_dir`. Fails if another store, in this
    /// process or another, holds it.
    pub fn open(base_dir: &str, config: MirrorConfig) -> Result<Self, MirrorError> {
        let dir_lock = lock_data_dir(base_dir).map_err(MirrorError::Engine)?;
        let registry = RelationRegistry::new(base_dir, Slot::SOLO, config.store);
        let persisted = registry.persisted_records().map_err(MirrorError::Engine)?;
        let mut mirror = Mirror {
            registry,
            records: HashMap::new(),
            _dir_lock: dir_lock,
            poison: None,
            applied_bytes: 0,
            checkpoint_bytes: config.checkpoint_bytes,
        };
        let mut all_reopened = true;
        for (tid, record) in persisted {
            let reopened = match record.map(|bytes| MirrorRecord::decode(&bytes)) {
                // Reopened from the manifest `rec` was read from.
                Ok(Some((rec, schema))) => mirror.registry.reopen_view(copy_spec(tid, schema)).map(|()| {
                    mirror.records.insert(tid, rec);
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
            if let Err(e) = mirror.registry.reclaim_orphan_relation_dirs() {
                gnitz_debug!("mirror: orphan directory sweep failed: {}", e);
            }
        }
        Ok(mirror)
    }

    /// `tid`'s open copy.
    fn copy(&self, tid: u64) -> &Relation {
        self.registry
            .relation(tid)
            .expect("a mirrored relation's copy is registered unless the store is poisoned")
    }

    /// Apply `blocks`, set the cursor to `next`, then checkpoint if due. A failed
    /// apply erases the copy; a failed auto-checkpoint is logged, and the next
    /// one retries.
    fn apply(&mut self, tid: u64, blocks: &[&[u8]], next: DeltaCursor) -> Result<(), MirrorError> {
        let schema = self.copy(tid).schema();
        for &block in blocks {
            self.applied_bytes += block.len();
            let applied = Batch::decode_foreign_wal_block(block, &schema)
                .map_err(|e| format!("decoding a delta for {tid}: {e}"))
                .and_then(|b| {
                    self.registry
                        .ingest(tid, b)
                        .map_err(|e| format!("applying a delta to {tid}: {e}"))
                });
            if let Err(why) = applied {
                self.invalidate(tid, Invalidate::Copy)?;
                return Err(MirrorError::Engine(why));
            }
        }
        self.records.get_mut(&tid).expect("checked by the caller").cursor = Some(next);
        if self.applied_bytes >= self.checkpoint_bytes {
            if let Err(e) = self.checkpoint() {
                gnitz_error!(
                    "mirror: auto-checkpoint failed: {} — the copies stand; the next checkpoint retries",
                    e
                );
            }
        }
        Ok(())
    }

    /// Poison the store and report why.
    ///
    /// Raised for a teardown that itself failed, and for a panic caught by
    /// [`Self::touching`].
    fn poison(&mut self, why: String) -> MirrorError {
        if self.poison.is_none() {
            self.poison = Some(why.clone());
        }
        MirrorError::Poisoned(why)
    }

    /// **The one way to touch a copy**: refuse a poisoned store, run `f`, and
    /// poison on a panic before letting the unwind continue. A verb reused
    /// inside another is called through its `MirrorStore` method, so it passes
    /// the guard too.
    ///
    /// The panic arm covers bugs; a poisoned store then refuses to publish a torn
    /// copy at the next checkpoint or `close_mirror`.
    fn touching<T>(
        &mut self,
        what: &str,
        f: impl FnOnce(&mut Self) -> Result<T, MirrorError>,
    ) -> Result<T, MirrorError> {
        if let Some(m) = &self.poison {
            return Err(MirrorError::Poisoned(m.clone()));
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(self))) {
            Ok(v) => v,
            Err(payload) => {
                self.poison(format!("panic while {what}; a store may be torn"));
                std::panic::resume_unwind(payload)
            }
        }
    }
}

/// Every method that touches a copy runs inside [`Mirror::touching`]. The state
/// accessors do not, nor does `clear_cursors`: it shuts the read gate, which a
/// poisoned store must still do.
impl MirrorStore for Mirror {
    fn base_dir(&self) -> &str {
        self.registry.base_dir()
    }

    /// Reconcile the local registry against `tid`'s upstream layout: a record
    /// holding `tid` under the same schema record stands, cursor and all, and
    /// takes the upstream name; anything else at this id or this name is a
    /// relation that changed identity, whose copy is retracted. Returns the id
    /// whose registration that retracted, so the caller can drop its own
    /// binding under the same name.
    fn register(
        &mut self,
        tid: u64,
        schema_name: &str,
        name: &str,
        schema: &Schema,
    ) -> Result<Option<u64>, MirrorError> {
        self.touching("registering a view", |m| {
            let block = schema.to_block();
            // This name at another id was renamed or recreated upstream.
            let renamed = m
                .records
                .iter()
                .find(|(&t, r)| t != tid && r.schema_name == schema_name && r.name == name)
                .map(|(&t, _)| t);
            if let Some(old) = renamed {
                m.invalidate(old, Invalidate::Registration)?;
            }
            match m.records.get_mut(&tid).filter(|r| r.block == block) {
                // A rename upstream keeps the id; a stale name here would match a
                // later view created under it.
                Some(r) => {
                    r.schema_name = schema_name.to_string();
                    r.name = name.to_string();
                }
                None => {
                    m.invalidate(tid, Invalidate::Registration)?;
                    m.registry
                        .register(copy_spec(tid, descriptor_of_block(&block)?))
                        .map_err(MirrorError::Engine)?;
                    let rec = MirrorRecord {
                        schema_name: schema_name.to_string(),
                        name: name.to_string(),
                        block,
                        cursor: None,
                    };
                    m.records.insert(tid, rec);
                }
            }
            Ok(renamed)
        })
    }

    /// [`Invalidate`]'s ladder, stopping where the caller asked. The cursor goes
    /// first at every level.
    fn invalidate(&mut self, tid: u64, level: Invalidate) -> Result<(), MirrorError> {
        self.touching("invalidating a copy", |m| {
            let Some(rec) = m.records.get_mut(&tid) else {
                return Ok(());
            };
            rec.cursor = None;
            let erased = match level {
                Invalidate::Cursor => return Ok(()),
                Invalidate::Copy => {
                    // A rederived open with no resume point erases the copy.
                    let spec = copy_spec(tid, m.copy(tid).schema());
                    m.registry.unregister(tid);
                    m.registry.register(spec)
                }
                Invalidate::Registration => {
                    m.records.remove(&tid);
                    m.registry.unregister_and_erase(tid)
                }
            };
            erased.map_err(|e| m.poison(format!("erasing the copy of {tid} failed: {e}")))?;
            Ok(())
        })
    }

    fn reseed(&mut self, tid: u64, blocks: &[&[u8]], cursor: DeltaCursor) -> Result<(), MirrorError> {
        self.touching("reseeding a copy", |m| {
            let Some(rec) = m.records.get(&tid) else {
                return Err(MirrorError::Engine(format!("relation {tid} is not mirrored")));
            };
            if rec.cursor.is_some() || m.copy(tid).cursor().valid {
                return Err(MirrorError::Engine(format!(
                    "relation {tid}'s copy is not erased; a whole value lands only on an empty copy"
                )));
            }
            m.apply(tid, blocks, cursor)
        })
    }

    fn advance(&mut self, tid: u64, blocks: &[&[u8]], next: DeltaCursor) -> Result<(), MirrorError> {
        self.touching("advancing a copy", |m| {
            if m.cursor_of(tid).is_none() {
                return Err(MirrorError::Engine(format!(
                    "relation {tid}'s copy holds no cursor for deltas to continue"
                )));
            }
            if INGEST_PANIC.take_once() {
                panic!("injected mirror ingest panic");
            }
            m.apply(tid, blocks, next)
        })
    }

    fn scan_spec(
        &mut self,
        tid: u64,
        spec: gnitz_wire::ReadSpec,
        reply_schema: &Schema,
    ) -> Result<ZSetBatch, MirrorError> {
        self.touching("running a read spec", |m| {
            // No copy holds a skeleton row, so no hydrator is needed.
            let batch = m
                .registry
                .scan_spec(tid, spec, reply_schema.layout_digest(), None)
                .map_err(MirrorError::Engine)?;
            let regions = batch.wire_regions();
            let mut rows = ZSetBatch::new(reply_schema);
            // The client's own block decoder: a local and a remote reply decode by one rule.
            decode_regions_into(&mut rows, &regions, batch.len(), reply_schema)
                .map_err(|e| MirrorError::Engine(e.to_string()))?;
            Ok(rows)
        })
    }

    fn cursor_of(&self, tid: u64) -> Option<DeltaCursor> {
        self.records.get(&tid).and_then(|r| r.cursor)
    }

    fn clear_cursors(&mut self) {
        for r in self.records.values_mut() {
            r.cursor = None;
        }
    }

    /// Publish every copy with its record. A failure is reported, not poisoned:
    /// a flush leaves every store holding what it held.
    fn checkpoint(&mut self) -> Result<(), MirrorError> {
        self.touching("checkpointing", |m| {
            m.applied_bytes = 0;
            for (&tid, rec) in &m.records {
                m.registry
                    .set_caller_record(tid, rec.encode())
                    .map_err(MirrorError::Engine)?;
            }
            m.registry.checkpoint_ephemeral([]).map_err(MirrorError::Engine)
        })
    }

    fn poisoned(&self) -> Option<&str> {
        self.poison.as_deref()
    }
}

/// The registration of `tid`'s copy: a plain view in `schema`'s layout.
fn copy_spec(tid: u64, schema: SchemaDescriptor) -> RelationSpec {
    RelationSpec {
        id: tid,
        kind: RelationKind::View(ViewProps::Plain),
        schema,
    }
}
