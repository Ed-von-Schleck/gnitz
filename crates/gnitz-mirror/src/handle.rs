//! The store: what a host opens, and the `MirrorStore` surface a client drives
//! it through.

use std::collections::HashMap;

use gnitz_core::{DeltaCursor, Invalidate, MirrorError, MirrorStore, RawBlock, Schema, ZSetBatch};
use gnitz_foundation::env::env_num;
use gnitz_foundation::fault::Seam;
use gnitz_foundation::gnitz_debug;
use gnitz_store::relation::{
    lock_data_dir, relation_dir, OnRegister, Relation, RelationKind, RelationRegistry, RelationSpec, StoreConfig,
    ViewBudgets,
};
use gnitz_store::storage::{Slot, StoreError};

use crate::state::{encode_records, read_state, write_state, MirrorRecord};

/// Applied delta bytes after which an apply drives a checkpoint of its own.
/// `GNITZ_MIRROR_CHECKPOINT_BYTES` overrides it.
const DEFAULT_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;

/// A store error as the host sees it. `gnitz-core` does not depend on
/// `gnitz-store`, so this cannot be a `From` impl.
pub(crate) fn engine(e: StoreError) -> MirrorError {
    MirrorError::Engine(e.to_string())
}

/// `GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR`: fail the next checkpoint once, before
/// anything durable moved; the after-flush shape is the torn-checkpoint test's.
/// Debug-only. It is the only way to reach [`Mirror::checkpoint`]'s
/// report-rather-than-poison path without a disk fault.
static CHECKPOINT_ERROR: Seam = Seam::new("GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR");

/// `GNITZ_INJECT_MIRROR_BOOTSTRAP_ERROR`: fail the next `Invalidate::Copy` once,
/// after the cursor is dropped and the copy erased. Debug-only.
///
/// Makes the window deterministic: copy gone, no cursor, registration standing.
/// A transport failure reaches it too.
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
    /// The relations this store holds and the stores behind them — the engine's
    /// L4 rung and **nothing above it**. No compiler, no VM, no catalog: a copy
    /// is fed by direct ingest of drained deltas and never by evaluating a
    /// circuit, so the whole DBSP layer is unreachable from here.
    pub(crate) registry: RelationRegistry,
    /// Key set equals the registry's: [`Mirror::enter`] and the
    /// [`Invalidate::Registration`] arm are the two writers of both.
    pub(crate) records: HashMap<u64, MirrorRecord>,
    base_dir: String,
    /// The `flock`ed handle on `base_dir`'s lock file, held only to be dropped:
    /// declared after `registry`, so field drop order releases it once every
    /// store is gone, which is what makes the next open of this directory
    /// succeed.
    _dir_lock: std::fs::File,
    poison: Option<String>,
    /// The state file's body as it was last written; a checkpoint that would
    /// rewrite it unchanged is skipped. The bytes and not a dirty bit, so no
    /// mutation site has to remember to set one — and a bit stuck false would
    /// stop the store checkpointing for good.
    published_block: Vec<u8>,
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
        // No retry: a mirror directory is never inherited by a forked child of
        // its previous holder, so a held lock is a live holder.
        let dir_lock = lock_data_dir(base_dir, std::time::Duration::ZERO).map_err(engine)?;

        // The mirror's own knobs, under its own prefix.
        let config = StoreConfig::from_env("GNITZ_MIRROR_");
        let state = read_state(base_dir);
        let mut registry = RelationRegistry::new(Slot::SOLO, config);
        // A copy holds no operator state, so only the generation decides a resume.
        registry.set_resume_enabled(true);
        if let Some((s, _)) = &state {
            registry.set_resume_generation(s.generation);
        }

        let mut mirror = Mirror {
            registry,
            records: HashMap::new(),
            base_dir: base_dir.to_string(),
            _dir_lock: dir_lock,
            poison: None,
            published_block: Vec::new(),
            applied_bytes: 0,
            checkpoint_bytes: env_num("GNITZ_MIRROR_CHECKPOINT_BYTES", DEFAULT_CHECKPOINT_BYTES),
        };
        // An unreadable or absent file leaves the generation at 0 with no record,
        // so nothing is entered and every view bootstraps.
        let mut entry_failed = false;
        if let Some((state, body)) = state {
            // Every record's store opens here, before any `mirror_view` supplies
            // a schema — which is why the record carries the schema block.
            for (tid, rec) in state.records.iter() {
                // A failed `enter` inserts into neither map, so the record goes
                // with it and that view bootstraps.
                if let Err(e) = mirror.enter(*tid, rec.clone()) {
                    gnitz_debug!("mirror: relation {} did not open, so it bootstraps: {}", tid, e);
                    entry_failed = true;
                }
            }
            mirror.published_block = body;
            // A cursor naming a copy that did not come back is dropped, which
            // makes its view bootstrap.
            for (tid, r) in &mut mirror.records {
                if !mirror.registry.relation(*tid as i64).is_some_and(Relation::resumed) {
                    r.cursor = None;
                }
            }
        }
        // With nothing entered — a lost `mirror_state` — this covers every
        // directory, whose stale manifests the restarted generation would accept.
        // Skipped after a failed `enter`: the fault may be transient.
        if !entry_failed {
            mirror.registry.reclaim_orphan_relation_dirs(base_dir);
        }
        Ok(mirror)
    }

    /// Enter one relation: open its copy under `<base_dir>/_relations` and record
    /// it.
    pub(crate) fn enter(&mut self, tid: u64, rec: MirrorRecord) -> Result<(), MirrorError> {
        let schema = crate::register::descriptor_of_block(&rec.block)?;
        self.registry
            .register(
                RelationSpec {
                    id: tid as i64,
                    // `View` maps to `Rederive`, and gives the `v_` directory
                    // prefix and the `RelClass::View` a copy reports.
                    kind: RelationKind::View,
                    schema,
                    directory: relation_dir(&self.base_dir, RelationKind::View, tid as i64),
                    // No skeleton row is ever written, so nothing can ask this
                    // store to hydrate; and the store maintains no feed of its own.
                    budgets: ViewBudgets::default(),
                },
                OnRegister::Live,
            )
            .map_err(engine)?;
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

    /// Make every copy and the record of where its feed got to durable: raise
    /// the generation fence, flush the copies to it, then write `mirror_state`
    /// naming that generation — in that order, so a crash leaves the file
    /// naming an older generation than the manifests and every view bootstraps.
    /// A round that would rewrite the records unchanged is skipped: every
    /// manifest is already at the named generation.
    ///
    /// **A failed checkpoint is reported, not poisoned** — the one `Err` here
    /// that is not fatal. The flush writes shards and publishes manifests;
    /// neither mutates what a store holds, so every copy is intact in the RAM
    /// tier and retrying is sound.
    fn checkpoint_inner(&mut self) -> Result<(), MirrorError> {
        let block = encode_records(&self.records);
        if block == self.published_block {
            return Ok(());
        }
        // Cleared before the fallible work below: a failing checkpoint must not
        // leave it over threshold, or every later ingest retries one.
        self.applied_bytes = 0;
        if CHECKPOINT_ERROR.take_once() {
            return Err(MirrorError::Engine("injected checkpoint failure".to_string()));
        }
        let generation = self.registry.resume_generation() + 1;
        self.registry.set_resume_generation(generation);
        self.registry.checkpoint_ephemeral(generation, []).map_err(engine)?;
        write_state(&self.base_dir, generation, &block)?;
        self.published_block = block;
        Ok(())
    }

    // -- Teardown ----------------------------------------------------------

    /// [`Invalidate`]'s ladder, as one body that stops where the caller asked.
    ///
    /// The cursor goes first at every level, and that is the whole reason the
    /// three teardowns are one method: a cursor left standing over an erased copy
    /// makes the next poll deliver `(T, …]` onto an empty store and lose
    /// everything at or below `T` in silence, and no tag check can see it — a
    /// local retraction leaves the server's tag identical.
    pub(crate) fn invalidate_inner(&mut self, tid: u64, level: Invalidate) -> Result<(), MirrorError> {
        let Some(rec) = self.records.get_mut(&tid) else {
            return Ok(());
        };
        rec.cursor = None;
        match level {
            Invalidate::Cursor => Ok(()),
            Invalidate::Copy => {
                // The reset unlinks this worker's child manifest, so the rebuilt
                // store's `Rederive` open peeks `None` and *erases* the stale
                // shards rather than reloading them, then rebuilds the handle
                // empty. It is exactly the state transition a bootstrap needs.
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
                // The directory goes now: a mirror re-registers ids within one
                // session, so a later `enter` would reopen these shards.
                self.registry.unregister_and_erase(tid as i64);
                Ok(())
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
                let desc = crate::register::descriptor_of_block(&rec.block)?;
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
