//! The store: what a host opens, and the `MirrorStore` surface a client drives
//! it through.

use std::collections::HashMap;

use gnitz_core::{DeltaCursor, Invalidate, MirrorError, MirrorStore, RawBlock, Schema, Shape, ZSetBatch};
use gnitz_foundation::env::env_num;
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::{
    lock_data_dir, relation_dir, RelationKind, RelationRegistry, RelationSpec, StoreConfig, ViewBudgets,
};
use gnitz_store::storage::{remove_child, ChildAddr, Slot, StoreError, DEFAULT_RAM_TIER_BYTES};

use crate::state::{encode_records, read_state, write_state, MirrorRecord};

/// Applied delta bytes after which an apply drives a checkpoint of its own.
/// `GNITZ_MIRROR_CHECKPOINT_BYTES` overrides it.
const DEFAULT_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;

/// A store error as the host sees it. `gnitz-core` does not depend on
/// `gnitz-store`, so this cannot be a `From` impl.
pub(crate) fn engine(e: StoreError) -> MirrorError {
    MirrorError::Engine(e.to_string())
}

/// `<base_dir>/_copies` — the root every mirrored relation's directory sits
/// under, so `<base_dir>` itself holds exactly three entries this crate wrote:
/// `LOCK`, `mirror_state` and this. That is what lets [`Mirror::open`]'s sweep
/// enumerate a directory the mirror provably created, where the server's boot
/// sweep refuses to enumerate an arbitrary host-chosen root.
const COPIES_DIRNAME: &str = "_copies";

/// `GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR`: fail the next checkpoint once, before
/// anything durable moved; the after-flush shape is the torn-checkpoint test's.
/// Debug-only. It is the only way to reach [`Mirror::checkpoint`]'s
/// report-rather-than-poison path without a disk fault.
static CHECKPOINT_ERROR: Seam = Seam::new("GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR");

/// `GNITZ_INJECT_MIRROR_BOOTSTRAP_ERROR`: fail the next `Invalidate::Copy` once,
/// after the cursor is dropped and the copy erased. Debug-only. It reproduces
/// the window a bootstrap whose upstream read fails leaves behind — copy gone,
/// no cursor, registration standing — which is otherwise unreachable, the
/// server's only refusal there being an oversized reply.
static BOOTSTRAP_ERROR: Seam = Seam::new("GNITZ_INJECT_MIRROR_BOOTSTRAP_ERROR");

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
    /// Key set equals the registry's: [`Mirror::enter`] and [`Mirror::retract`]
    /// are the two writers of both. Every change to a copy moves its record —
    /// an ingest advances the cursor, a teardown drops it.
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

// SAFETY: no handle into the registry can escape. `Mirror`'s whole public
// surface is `open` plus `impl MirrorStore`, whose signatures are `gnitz-core`
// types and primitives — so every `Rc` the store mints is reached only from
// inside it and a move carries them all together. `Mirror` is `!Sync` (the
// store's `UnsafeCell`s and raw pointers make it so), so the graph is reached by
// one thread at a time; the host's own edge orders the refcounts across a move.
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

        // The mirror's own knobs, read by this entry point and not by a store
        // constructor: it honours none of the server's tuning variables.
        let config = StoreConfig {
            ram_tier_bytes: env_num("GNITZ_MIRROR_RAM_TIER_BYTES", DEFAULT_RAM_TIER_BYTES),
            ..Default::default()
        };
        let state = read_state(base_dir);
        let mut registry = RelationRegistry::new(Slot::SOLO, config);
        // A copy is Z-set rows in the view's own schema with no operator state,
        // opened at one worker: neither axis of the topology verdict can move
        // under it, so the generation alone decides a resume.
        registry.set_recorded_topology(registry.launched_topology_word());
        if let Some(s) = &state {
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
        // An unreadable or absent file leaves the generation at 0 with no
        // record, so nothing is entered, the sweep reclaims the whole tree and
        // every view bootstraps.
        if let Some(state) = state {
            // Every record's store opens here, before any `mirror_view` supplies
            // a schema — which is why the record carries the schema block.
            for (tid, rec) in state.records.iter() {
                mirror.enter(*tid, rec.clone())?;
            }
            // Through the encoder a checkpoint compares against, and before the
            // sweep below, which does change the records.
            mirror.published_block = encode_records(&state.records);
            // A cursor naming a copy that did not come back is dropped, which
            // makes its view bootstrap.
            for (tid, r) in &mut mirror.records {
                if !mirror.registry.store_resumed(*tid as i64) {
                    r.cursor = None;
                }
            }
        }
        // Before anything is entered that is every directory: the generation
        // counter restarts at 0 after a lost `mirror_state`, so a stale manifest
        // would become resumable again.
        mirror
            .registry
            .reclaim_orphan_relation_dirs([format!("{base_dir}/{COPIES_DIRNAME}")]);
        Ok(mirror)
    }

    /// Enter one relation: open its copy under `<base_dir>/_copies` and record
    /// it. Together with [`Self::retract`] the only writer of either map, which
    /// is what keeps their key sets equal without either consulting the other.
    pub(crate) fn enter(&mut self, tid: u64, rec: MirrorRecord) -> Result<(), MirrorError> {
        let schema = gnitz_store::schema::decode_schema_block(&rec.block, false)
            .map_err(|e| MirrorError::Engine(format!("mirror: schema block: {e}")))?;
        self.registry
            .register(RelationSpec {
                id: tid as i64,
                // `View` is what maps to `Rederive`: the ephemeral round is the
                // only one that publishes a copy, and the only round a mirror runs.
                kind: RelationKind::View,
                schema,
                directory: relation_dir(&self.base_dir, COPIES_DIRNAME, RelationKind::View, tid as i64),
                // No skeleton row is ever written, so nothing can ask this store
                // to hydrate; and the store maintains no feed of its own.
                budgets: ViewBudgets::default(),
            })
            .map_err(engine)?;
        self.records.insert(tid, rec);
        Ok(())
    }

    /// Drop one relation's registration, record and directory. A `tid` the
    /// store does not hold is a no-op.
    pub(crate) fn retract(&mut self, tid: u64) {
        self.records.remove(&tid);
        let Some(dir) = self.registry.entry(tid as i64).map(|e| e.directory.clone()) else {
            return;
        };
        self.registry.unregister(tid as i64);
        // The state file still names this id until the next checkpoint, so the
        // child goes manifest-first.
        remove_child(&ChildAddr::worker(self.registry.slot()).dir(&dir));
        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Poison ------------------------------------------------------------

    /// Poison the store and report why.
    ///
    /// An ingest error is the one that *must* poison: the delta it dropped
    /// leaves a hole the cursor would step over, so every later read would
    /// answer off a copy silently missing rows. A checkpoint error is the
    /// opposite case — see [`Mirror::checkpoint_inner`].
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
    /// The panic arm covers *bugs*, where the `Err` channel covers the expected
    /// faults, and only where the panic unwinds: this crate built in release
    /// inherits the workspace's `panic = "abort"`, which cannot be set per
    /// package. A panic poisons so a later [`Mirror::checkpoint`] or the
    /// client's `close_mirror` refuses to publish a torn copy.
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
        if CHECKPOINT_ERROR.take_once() {
            return Err(MirrorError::Engine("injected checkpoint failure".to_string()));
        }
        let generation = self.registry.resume_generation() + 1;
        self.registry.set_resume_generation(generation);
        self.registry.flush_ephemeral_outputs(generation).map_err(engine)?;
        write_state(&self.base_dir, generation, &block)?;
        self.published_block = block;
        self.applied_bytes = 0;
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
                    .reset_store(tid as i64)
                    .map_err(|e| self.poison(format!("erasing the copy of {tid} failed: {e}")))?;
                if BOOTSTRAP_ERROR.take_once() {
                    return Err(MirrorError::Engine("injected bootstrap failure".to_string()));
                }
                Ok(())
            }
            Invalidate::Registration => {
                self.retract(tid);
                Ok(())
            }
        }
    }
}

/// The whole surface a client drives the store through. Every method that
/// touches a copy runs inside [`Mirror::touching`]; the three accessors that
/// only report state keep answering on a poisoned store.
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

    /// Apply the blocks, **then** advance the cursor, **then** maybe checkpoint —
    /// in that order. A checkpoint writes the live records, so checkpointing
    /// before the advance would record `prev` beside copies that already absorbed
    /// through `next`, and the reopen would re-apply the interval and double every
    /// weight in it while the row set stayed identical.
    ///
    /// An auto-checkpoint failure is **not** a poisoning, and it reaches the
    /// caller with the cursor already advanced: the store stays usable and the
    /// next poll continues from there. Any other failure poisons — the apply is a
    /// loop, one store ingest per block, and a failure at block *k* leaves
    /// `0..k` applied with no way to roll back.
    fn ingest(&mut self, tid: u64, blocks: Vec<RawBlock>, shape: Shape, next: DeltaCursor) -> Result<(), MirrorError> {
        self.touching("applying a delta", |m| {
            let applied = m.ingest_blocks(tid, blocks, shape)?;
            m.applied_bytes += applied;
            // A zero-block ingest still moves the cursor.
            m.records.get_mut(&tid).expect("ingest_blocks resolved it").cursor = Some(next);
            if m.applied_bytes >= m.checkpoint_bytes {
                // Reported, not poisoned; the cursor is already advanced.
                m.checkpoint_inner()?;
            }
            Ok(())
        })
    }

    fn scan(&mut self, tid: u64, schema: &Schema) -> Result<ZSetBatch, MirrorError> {
        self.touching("scanning a copy", |m| m.scan_inner(tid, schema))
    }

    fn scan_spec(&mut self, tid: u64, spec: &[u8], reply_schema: &Schema) -> Result<ZSetBatch, MirrorError> {
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
