//! The store: what a host opens, and the `MirrorStore` surface a client drives
//! it through.

use std::collections::HashMap;

use gnitz_core::{DeltaCursor, Invalidate, MirrorError, MirrorStore, RawBlock, Schema, ZSetBatch};
use gnitz_foundation::env::env_num;
use gnitz_foundation::fault::Seam;
use gnitz_foundation::gnitz_debug;
use gnitz_store::relation::{lock_data_dir, RelationKind, RelationRegistry, RelationSpec, StoreConfig};
use gnitz_store::storage::{Slot, StoreError};
use gnitz_wire::ViewProps;

use crate::record::MirrorRecord;

/// Applied delta bytes after which an apply drives a checkpoint of its own.
/// `GNITZ_MIRROR_CHECKPOINT_BYTES` overrides it.
const DEFAULT_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;

/// A store error as the host sees it. `gnitz-core` does not depend on
/// `gnitz-store`, so this cannot be a `From` impl.
pub(crate) fn engine(e: StoreError) -> MirrorError {
    MirrorError::Engine(e.to_string())
}

/// `GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR`: fail the next checkpoint once, before
/// anything durable moved. Debug-only.
static CHECKPOINT_ERROR: Seam = Seam::new("GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR");

/// `GNITZ_INJECT_MIRROR_BOOTSTRAP_ERROR`: fail the next `Invalidate::Copy` once,
/// after the cursor is dropped and the copy erased. Debug-only.
static BOOTSTRAP_ERROR: Seam = Seam::new("GNITZ_INJECT_MIRROR_BOOTSTRAP_ERROR");

/// `GNITZ_INJECT_MIRROR_INGEST_PANIC`: panic once on a poll's apply, idle polls
/// included. A panic rather than an `Err`: nothing else reaches
/// [`Mirror::touching`]'s guard arm.
static INGEST_PANIC: Seam = Seam::new("GNITZ_INJECT_MIRROR_INGEST_PANIC");

/// A maintained local copy of one or more views.
///
/// One store holds many: the registry holds relations, so each lives under its
/// own server id in one data directory, under one lock and one checkpoint. Each
/// carries its own cursor and is advanced independently.
pub struct Mirror {
    pub(crate) registry: RelationRegistry,
    /// One per relation in `registry`.
    pub(crate) records: HashMap<u64, MirrorRecord>,
    base_dir: String,
    /// Declared after `registry`, so it is released once every store has closed.
    _dir_lock: std::fs::File,
    poison: Option<String>,
    applied_bytes: usize,
    checkpoint_bytes: usize,
}

// SAFETY: nothing public hands out an `Rc`. `open` and `impl MirrorStore` are
// all there is, and their returns are `gnitz-core` types and primitives.
// Condition: `RelationRegistry`'s "Thread contract".
unsafe impl Send for Mirror {}

impl Mirror {
    /// Open (or create) a store at `base_dir`.
    ///
    /// Fails if `base_dir` is already held — by another process, or by another
    /// store in this one. The store homes at `w0of1` whatever process it runs
    /// in, under a directory it locks itself.
    pub fn open(base_dir: &str) -> Result<Self, MirrorError> {
        // No retry: no forked child inherits a mirror's lock, so a holder is live.
        let dir_lock = lock_data_dir(base_dir, std::time::Duration::ZERO).map_err(engine)?;
        let registry = RelationRegistry::new(base_dir, Slot::SOLO, StoreConfig::from_env("GNITZ_MIRROR_"));
        let persisted = registry.persisted_view_records().map_err(engine)?;
        let mut mirror = Mirror {
            registry,
            records: HashMap::new(),
            base_dir: base_dir.to_string(),
            _dir_lock: dir_lock,
            poison: None,
            applied_bytes: 0,
            checkpoint_bytes: env_num("GNITZ_MIRROR_CHECKPOINT_BYTES", DEFAULT_CHECKPOINT_BYTES),
        };
        let mut all_reopened = true;
        for (id, record) in persisted {
            let tid = id as u64;
            let reopened = match record.map(|bytes| MirrorRecord::decode(&bytes)) {
                Ok(Some(rec)) => mirror.reopen(tid, rec),
                // Damage: the sweep below removes the directory.
                Ok(None) => continue,
                Err(e) => Err(engine(e)),
            };
            if let Err(e) = reopened {
                gnitz_debug!("mirror: relation {} did not reopen, so it bootstraps: {}", tid, e);
                all_reopened = false;
            }
        }
        // A failure may be transient, and the sweep would delete that copy.
        if all_reopened {
            mirror.registry.reclaim_orphan_relation_dirs();
        }
        Ok(mirror)
    }

    /// Open an empty copy of `tid`, registered with no feed position.
    pub(crate) fn enter(&mut self, tid: u64, schema_name: &str, name: &str, block: Vec<u8>) -> Result<(), MirrorError> {
        self.registry.register(copy_spec(tid, &block)?).map_err(engine)?;
        let rec = MirrorRecord {
            schema_name: schema_name.to_string(),
            name: name.to_string(),
            block,
            cursor: None,
        };
        self.records.insert(tid, rec);
        Ok(())
    }

    /// Reopen `tid`'s copy from the manifest `rec` was read from.
    fn reopen(&mut self, tid: u64, rec: MirrorRecord) -> Result<(), MirrorError> {
        self.registry.reopen_view(copy_spec(tid, &rec.block)?).map_err(engine)?;
        self.records.insert(tid, rec);
        Ok(())
    }

    // -- Poison ------------------------------------------------------------

    /// Erase `tid`'s copy and report why. Leaves it registered, empty and
    /// cursorless, so the next poll bootstraps it. A failed erase poisons, in
    /// [`Self::invalidate_inner`].
    pub(crate) fn erase_copy(&mut self, tid: u64, why: String) -> MirrorError {
        match self.invalidate_inner(tid, Invalidate::Copy) {
            Ok(()) => MirrorError::Engine(why),
            Err(e) => e,
        }
    }

    /// Poison the store and report why.
    ///
    /// Raised for an erase that itself failed, and for a panic caught by
    /// [`Self::touching`].
    pub(crate) fn poison(&mut self, why: String) -> MirrorError {
        if self.poison.is_none() {
            self.poison = Some(why.clone());
        }
        MirrorError::Poisoned(why)
    }

    /// **The one way to touch a copy**: refuse a poisoned store, run `f`, and
    /// poison on a panic before letting the unwind continue. Every `MirrorStore`
    /// method below goes through it, so neither half can be forgotten at a call
    /// site.
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

    // -- Checkpoint --------------------------------------------------------

    /// Publish every copy with its record. A failure is reported, not poisoned:
    /// a flush leaves every store holding what it held.
    fn checkpoint_inner(&mut self) -> Result<(), MirrorError> {
        self.applied_bytes = 0;
        if CHECKPOINT_ERROR.take_once() {
            return Err(MirrorError::Engine("injected checkpoint failure".to_string()));
        }
        for (&tid, rec) in &self.records {
            self.registry
                .set_caller_record(tid as i64, rec.encode())
                .map_err(engine)?;
        }
        self.registry.checkpoint_ephemeral([]).map_err(engine)
    }

    // -- Teardown ----------------------------------------------------------

    /// [`Invalidate`]'s ladder, stopping where the caller asked. The cursor goes
    /// first at every level.
    pub(crate) fn invalidate_inner(&mut self, tid: u64, level: Invalidate) -> Result<(), MirrorError> {
        let Some(rec) = self.records.get_mut(&tid) else {
            return Ok(());
        };
        rec.cursor = None;
        match level {
            Invalidate::Cursor => Ok(()),
            Invalidate::Copy => {
                self.registry
                    .reset_view(tid as i64)
                    .map_err(|e| self.poison(format!("erasing the copy of {tid} failed: {e}")))?;
                if BOOTSTRAP_ERROR.take_once() {
                    return Err(MirrorError::Engine("injected bootstrap failure".to_string()));
                }
                Ok(())
            }
            Invalidate::Registration => {
                self.records.remove(&tid);
                self.registry
                    .unregister_and_erase(tid as i64)
                    .map_err(|e| self.poison(format!("erasing the copy of {tid} failed: {e}")))
            }
        }
    }
}

/// Every method that touches a copy runs inside [`Mirror::touching`]. The state
/// accessors do not, nor does `clear_cursors`: it shuts the read gate, which a
/// poisoned store must still do.
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
        self.touching("registering a view", |m| {
            m.register_inner(tid, schema_name, name, schema)
        })
    }

    fn invalidate(&mut self, tid: u64, level: Invalidate) -> Result<(), MirrorError> {
        self.touching("invalidating a copy", |m| m.invalidate_inner(tid, level))
    }

    /// Apply, then advance the cursor, then maybe checkpoint. A checkpoint writes
    /// the live records, so checkpointing first would record `prev` beside copies
    /// that already absorbed through `next`, and the reopen would re-apply that
    /// interval.
    ///
    /// Neither failure poisons: an apply failure erases that copy, and an
    /// auto-checkpoint failure leaves the cursor advanced and the store usable.
    fn ingest(&mut self, tid: u64, blocks: Vec<RawBlock>, next: DeltaCursor) -> Result<(), MirrorError> {
        self.touching("applying a delta", |m| {
            let Some(rec) = m.records.get(&tid) else {
                return Err(MirrorError::Engine(format!("relation {tid} is not mirrored")));
            };
            let stamped = rec.cursor.is_some();
            if stamped && INGEST_PANIC.take_once() {
                panic!("injected mirror ingest panic");
            }
            if !blocks.is_empty() {
                let desc = m
                    .registry
                    .relation(tid as i64)
                    .ok_or_else(|| MirrorError::Engine(format!("relation {tid} has no open copy")))?
                    .schema();
                let applied = m.ingest_blocks(tid, blocks, stamped, desc)?;
                m.applied_bytes += applied;
            }
            m.records.get_mut(&tid).expect("resolved above").cursor = Some(next);
            if m.applied_bytes >= m.checkpoint_bytes {
                m.checkpoint_inner()?;
            }
            Ok(())
        })
    }

    fn scan_spec(
        &mut self,
        tid: u64,
        spec: gnitz_wire::ReadSpec,
        reply_schema: &Schema,
    ) -> Result<ZSetBatch, MirrorError> {
        self.touching("running a read spec", |m| m.scan_spec_inner(tid, spec, reply_schema))
    }

    fn cursor_of(&self, tid: u64) -> Option<DeltaCursor> {
        self.records.get(&tid).and_then(|r| r.cursor)
    }

    fn clear_cursors(&mut self) {
        for r in self.records.values_mut() {
            r.cursor = None;
        }
    }

    fn checkpoint(&mut self) -> Result<(), MirrorError> {
        self.touching("checkpointing", |m| m.checkpoint_inner())
    }

    fn poisoned(&self) -> Option<&str> {
        self.poison.as_deref()
    }
}

/// The registration of `tid`'s copy, in the layout `block` describes.
fn copy_spec(tid: u64, block: &[u8]) -> Result<RelationSpec, MirrorError> {
    Ok(RelationSpec {
        id: tid as i64,
        kind: RelationKind::View(ViewProps::Plain),
        schema: crate::register::descriptor_of_block(block)?,
    })
}
