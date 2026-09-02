//! The store: what a host opens, and the `MirrorStore` surface a client drives
//! it through.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use gnitz_core::{DeltaCursor, Invalidate, MirrorError, MirrorStore, RawBlock, Schema, Shape, StoreRead, ZSetBatch};
use gnitz_store::foundation::env::env_num;
use gnitz_store::foundation::fault::Seam;
use gnitz_store::foundation::worker_ctx;
use gnitz_store::relation::{ensure_dir, is_table_dir_name, lock_data_dir, RelationRegistry};
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::subdir_names;

use crate::state::{read_state, write_state, MirrorRecord};

/// Applied delta bytes after which an apply drives a checkpoint of its own.
/// `GNITZ_MIRROR_CHECKPOINT_BYTES` overrides it.
const DEFAULT_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;

/// `<base_dir>/_copies` — the root every mirrored relation's directory sits
/// under, so `<base_dir>` itself holds exactly three entries this crate wrote:
/// `LOCK`, `mirror_state` and this. That is what lets [`Mirror::open`]'s sweep
/// enumerate a directory the mirror provably created, where the server's boot
/// sweep refuses to enumerate an arbitrary host-chosen root.
///
/// A user schema literally named `_copies` collides with nothing: schema
/// directories sit *inside* this root, so it would be `_copies/_copies/v_<id>`.
const COPIES_DIRNAME: &str = "_copies";

/// `GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR`: fail the next checkpoint once, before
/// it has changed anything durable. Debug-only. It is the only way to reach
/// [`Mirror::checkpoint`]'s report-rather-than-poison path without a disk fault.
static CHECKPOINT_ERROR: Seam = Seam::new("GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR");

/// `GNITZ_INJECT_MIRROR_BOOTSTRAP_ERROR`: fail the next `Invalidate::Copy` once,
/// after the cursor is dropped and the copy erased. Debug-only. It reproduces
/// the window a bootstrap whose upstream read fails leaves behind — copy gone,
/// no cursor, registration standing — which is otherwise unreachable, the
/// server's only refusal there being an oversized reply.
static BOOTSTRAP_ERROR: Seam = Seam::new("GNITZ_INJECT_MIRROR_BOOTSTRAP_ERROR");

/// The shapes one mirrored relation is read and written under, all three fixed
/// by its registration and dropped only with it.
pub(crate) struct Shapes {
    /// What a local reply decodes back to — hidden columns included, which keeps
    /// `pk_stride` right for a view whose physical PK is a synthetic hidden
    /// column.
    pub(crate) schema: Arc<Schema>,
    /// The local store's own descriptor, read back from the registry — so it is
    /// by construction the one the store was created with.
    pub(crate) view_desc: SchemaDescriptor,
    /// The derived delta-store shape, from the same builder the server derives
    /// its own with.
    pub(crate) delta_desc: SchemaDescriptor,
}

/// A maintained local copy of one or more views.
///
/// One store holds many: the registry holds relations, so each lives under its
/// own server id in one data directory, under one lock and one checkpoint. Each
/// carries its own cursor and is advanced independently.
pub struct Mirror {
    /// The relations this store holds and the stores behind them — the engine's
    /// L4 rung and **nothing above it**. No compiler, no VM, no catalog: a copy
    /// is fed by direct ingest of drained deltas and never by evaluating a
    /// circuit, so the whole DBSP layer is unreachable from here and, after the
    /// crate split, unlinked.
    pub(crate) registry: RelationRegistry,
    /// Per relation, what its registration fixed. Dropped only by
    /// `Invalidate::Registration`, alongside the record.
    pub(crate) shapes: HashMap<u64, Shapes>,
    /// What `mirror_state` holds for each relation: its qualified name and its
    /// schema block, verbatim. Written only by [`Mirror::enter`] and
    /// [`Mirror::retract`], which write the registry in the same breath.
    pub(crate) records: HashMap<u64, MirrorRecord>,
    /// Where each copy's feed got to, and by its presence that the copy is valid
    /// — the store half of the readability gate. It outlives one session's
    /// registrations on purpose: a checkpoint republishes every copy the
    /// registry holds, so the cursors beside them must cover the same set.
    pub(crate) cursors: HashMap<u64, DeltaCursor>,
    pub(crate) base_dir: String,
    /// `<base_dir>/_copies`, held rather than rebuilt per registration.
    pub(crate) copies_root: String,
    /// The `flock`ed handle on `base_dir`'s lock file, held only to be dropped:
    /// declared after `registry`, so field drop order releases it once every
    /// store is gone, which is what makes the next open of this directory
    /// succeed.
    _dir_lock: std::fs::File,
    pub(crate) poison: Option<String>,
    applied_bytes: usize,
    checkpoint_bytes: usize,
}

// SAFETY: no handle into the registry can escape. `Mirror`'s whole public
// surface is `open` plus `impl MirrorStore`, whose signatures are `gnitz-core`
// types and primitives — so every `Rc` the store mints is reached only from
// inside it and a move carries them all together. `Mirror` is `!Sync` (the
// store's `UnsafeCell`s and raw pointers make it so), so the graph is reached by
// one thread at a time. What orders the non-atomic refcounts across a handover
// is whichever edge the host holds the client behind, and there are three: a
// blocking host holds the client by value, so the move itself is the edge; the
// Python host's client never moves — it sits at a fixed address in the Python
// heap — and the edge is pyo3's borrow flag, an `AcqRel` compare-exchange on
// entry to every `&mut self` method and a `Release` store on exit; `gnitz-tokio`
// reaches the same store from `spawn_blocking` threads, where its own lock is
// the edge.
//
// Nothing under the store is thread-affine: process-wide atomics, an `flock`
// held as an open file description, an `io_uring` built per call. The
// `thread_local!`s are scratch pools that hold nothing between calls, bar a
// compaction-statistics counter a moved store splits across two copies, which
// only an `#[ignore]`d amplification bench reads.
unsafe impl Send for Mirror {}

impl Mirror {
    /// Open (or create) a store at `base_dir`.
    ///
    /// Fails if the process has taken a server role, or if `base_dir` is already
    /// held — by another process, or by another store in this one.
    pub fn open(base_dir: &str) -> Result<Self, MirrorError> {
        // Assert Standalone rather than set it. `set_worker_role` would invert
        // the index home directory and `store_lsn`'s own assertion; and a store
        // opened inside a forked worker would home its stores at `w{k}of1`,
        // which the next boot sweep deletes as unowned.
        if !worker_ctx::is_standalone() {
            return Err(MirrorError::Engine(
                "a mirror cannot be opened in a process that has taken a server role".to_string(),
            ));
        }
        // The directory before the lock: `lock_data_dir` opens `<base_dir>/LOCK`
        // with `create(true)`, which fails if the directory is absent.
        ensure_dir(base_dir)?;
        let dir_lock = lock_data_dir(base_dir)?;

        let mut registry = RelationRegistry::new(1);
        // The mirror always runs at one worker, so the topology word is a
        // constant of the process and is stamped once, here. That leaves the
        // *generation* as the only fence, which is the one thing `mirror_state`
        // carries — and on the arm below where there is no state to carry, the
        // sweep at the end of this function has removed every copy directory, so
        // there is no manifest left for any generation to match.
        registry.set_recorded_topology(registry.launched_topology_word());
        let state = read_state(base_dir, registry.launched_topology_word());

        let mut mirror = Mirror {
            registry,
            shapes: HashMap::new(),
            records: HashMap::new(),
            cursors: HashMap::new(),
            base_dir: base_dir.to_string(),
            copies_root: format!("{base_dir}/{COPIES_DIRNAME}"),
            _dir_lock: dir_lock,
            poison: None,
            applied_bytes: 0,
            checkpoint_bytes: env_num("GNITZ_MIRROR_CHECKPOINT_BYTES", DEFAULT_CHECKPOINT_BYTES),
        };
        // An unreadable, foreign-topology or absent file leaves the generation at
        // 0 with no record, so nothing is entered, the sweep reclaims the whole
        // tree and every view bootstraps.
        if let Some(state) = state {
            mirror.registry.set_resume_generation(state.generation);
            // Every record's store opens here, before any `mirror_view` supplies
            // a schema — which is why the record carries the schema block.
            // A checkpoint raises the fence for the whole store but flushes only
            // what the registry holds, so a copy left unregistered across one
            // would keep a manifest at the old generation and be erased at the
            // next open.
            for (tid, rec) in state.records {
                let desc = crate::register::descriptor_of_block(&rec.block)?;
                mirror.enter(tid, rec, desc)?;
            }
            // A cursor naming a copy that did not come back is dropped, which
            // makes its view bootstrap.
            let mut cursors = state.cursors;
            cursors.retain(|&tid, _| mirror.registry.store_resumed(tid as i64));
            mirror.cursors = cursors;
        }
        mirror.reclaim_unnamed_copies();
        Ok(mirror)
    }

    /// Enter one relation: open its copy under [`Self::copies_root`] and record
    /// it. Together with [`Self::retract`] the only writer of either map, which
    /// is what keeps their key sets equal without either consulting the other.
    ///
    /// `desc` is what `rec.block` decodes to; the caller already holds it, so the
    /// block is decoded once per entry rather than once here and once there.
    pub(crate) fn enter(&mut self, tid: u64, rec: MirrorRecord, desc: SchemaDescriptor) -> Result<(), MirrorError> {
        self.registry.register(crate::register::spec_for(
            &self.copies_root,
            tid,
            &rec.schema_name,
            desc,
        ))?;
        self.records.insert(tid, rec);
        Ok(())
    }

    /// Drop one relation's registration, its record and its directory. A `tid`
    /// the store does not hold is `Ok(())` by construction.
    ///
    /// The directory goes now rather than at the next checkpoint: no worker here
    /// can still be applying a create this drop races, so the server's gate is
    /// vacuous. One level above `remove_child`, whose manifest-first unlink is a
    /// no-op here — the caller has already reset the store, which unlinked the
    /// manifest, so a failed removal leaves nothing resumable behind.
    pub(crate) fn retract(&mut self, tid: u64) {
        self.records.remove(&tid);
        let dir = self.registry.entry(tid as i64).map(|e| e.directory.clone());
        self.registry.unregister(tid as i64);
        if let Some(dir) = dir {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// Reclaim every directory under [`Self::copies_root`] that no entered
    /// relation owns.
    ///
    /// Both gates are the server's boot sweep's — a shape test on the directory
    /// name, and membership in the set of paths the registry actually opened —
    /// kept for its reasons even though this root is one the mirror created. The
    /// live set comes from the registry rather than from re-parsing an id back
    /// out of the name, so it cannot drift from what [`relation_dir`] wrote.
    ///
    /// Before anything has been entered that is *every* directory under the
    /// root, and that is load-bearing rather than hygienic: the generation
    /// counter restarts at 0 after a lost `mirror_state`, so a manifest left
    /// behind at some later generation would be reachable again once the counter
    /// climbed back past it, and a store resumed that way is old shards read
    /// under a new schema.
    fn reclaim_unnamed_copies(&self) {
        let live: HashSet<&str> = self.registry.entries().map(|(_, e)| e.directory.as_str()).collect();
        for schema_name in subdir_names(&self.copies_root) {
            let schema_dir = format!("{}/{schema_name}", self.copies_root);
            for name in subdir_names(&schema_dir) {
                let full = format!("{schema_dir}/{name}");
                if live.contains(full.as_str()) || !is_table_dir_name(&name) {
                    continue;
                }
                let _ = std::fs::remove_dir_all(&full);
            }
        }
    }

    /// Whether a read of `tid` is answered off this store: a copy the
    /// registration described, and a cursor saying it is valid.
    fn holds(&self, tid: u64) -> bool {
        self.cursors.contains_key(&tid) && self.shapes.contains_key(&tid)
    }

    // -- Poison ------------------------------------------------------------

    fn check_poison(&self) -> Result<(), MirrorError> {
        match &self.poison {
            Some(m) => Err(MirrorError::Poisoned(m.clone())),
            None => Ok(()),
        }
    }

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
    /// package. `Drop` skips its checkpoint when poisoned, so an unguarded panic
    /// would let it publish torn state at a fresh generation with a state file
    /// blessing it. A `thread::panicking()` check in `Drop` would not do: pyo3
    /// catches the panic at the `#[pymethods]` boundary, so the later drop
    /// happens on a thread that is not panicking.
    fn touching<T>(
        &mut self,
        what: &str,
        f: impl FnOnce(&mut Self) -> Result<T, MirrorError>,
    ) -> Result<T, MirrorError> {
        self.check_poison()?;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(self))) {
            Ok(v) => v,
            Err(payload) => {
                self.poison(format!("panic while {what}; a store may be torn"));
                std::panic::resume_unwind(payload)
            }
        }
    }

    // -- Checkpoint --------------------------------------------------------

    pub(crate) fn note_applied(&mut self, bytes: usize) {
        self.applied_bytes += bytes;
    }

    pub(crate) fn checkpoint_if_due(&mut self) -> Result<(), MirrorError> {
        if self.applied_bytes >= self.checkpoint_bytes {
            // Reported after the delta it followed was applied, and the store
            // stays usable: what failed is the durability of the copy, not the
            // copy.
            return self.checkpoint_inner();
        }
        Ok(())
    }

    /// Make every copy and the record of where its feed got to durable: raise
    /// the generation fence, flush the copies to it, then write `mirror_state`
    /// naming that generation. The round publishes even for a copy that absorbed
    /// nothing, which keeps "resumed at `gen`" and "never checkpointed"
    /// distinguishable.
    ///
    /// **The order is load-bearing.** The copies must be durable at `g` before
    /// the file names `g`, so a crash anywhere leaves the file naming an older
    /// generation than the manifests carry and every view bootstraps. The
    /// reverse would resume copies the file cannot vouch for.
    ///
    /// **A failed checkpoint is reported, not poisoned** — the one `Err` here
    /// that is not fatal. The flush writes shards and publishes manifests;
    /// neither mutates what a store holds, so every copy is intact in the RAM
    /// tier and retrying is sound.
    fn checkpoint_inner(&mut self) -> Result<(), MirrorError> {
        // Before the bump, so the injected failure is the one that changed
        // nothing durably — the retryable case this method's doc promises.
        if CHECKPOINT_ERROR.take_once() {
            return Err(MirrorError::Engine("injected checkpoint failure".to_string()));
        }
        let generation = self.registry.resume_generation() + 1;
        self.registry.set_resume_generation(generation);
        self.registry.flush_ephemeral_outputs(generation)?;
        write_state(
            &self.base_dir,
            generation,
            self.registry.launched_topology_word(),
            &self.records,
            &self.cursors,
        )?;
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
        self.cursors.remove(&tid);
        if level == Invalidate::Cursor {
            return Ok(());
        }
        if !self.registry.has_id(tid as i64) {
            self.shapes.remove(&tid);
            self.retract(tid);
            return Ok(());
        }

        // The reset unlinks this worker's child manifest, so the rebuilt store's
        // `Rederive` open peeks `None` and *erases* the stale shards rather than
        // reloading them, then rebuilds the handle empty. It is exactly the state
        // transition a bootstrap needs, and it compiles nothing.
        self.registry
            .reset_store(tid as i64)
            .map_err(|e| self.poison(format!("erasing the copy of {tid} failed: {e}")))?;
        if level == Invalidate::Copy {
            if BOOTSTRAP_ERROR.take_once() {
                return Err(MirrorError::Engine("injected bootstrap failure".to_string()));
            }
            return Ok(());
        }

        self.shapes.remove(&tid);
        self.retract(tid);
        Ok(())
    }
}

/// The whole surface a client drives the store through.
///
/// Every method that touches a copy runs inside [`Mirror::touching`]. The two
/// reads check the gate *before* it, so a relation the copy does not hold is
/// delegated unaffected and a poisoned copy takes down only the reads it would
/// have answered. The three accessors that report state rather than use it keep
/// answering, so a host can diagnose the store it must discard.
impl MirrorStore for Mirror {
    fn base_dir(&self) -> &str {
        &self.base_dir
    }

    fn register(&mut self, tid: u64, schema_name: &str, name: &str, schema: &Schema) -> Result<Vec<u64>, MirrorError> {
        self.touching("registering a view", |m| {
            m.register_inner(tid, schema_name, name, schema)
        })
    }

    fn invalidate(&mut self, tid: u64, level: Invalidate) -> Result<(), MirrorError> {
        self.touching("invalidating a copy", |m| m.invalidate_inner(tid, level))
    }

    /// Apply the blocks, **then** advance the cursor, **then** maybe checkpoint —
    /// in that order. A checkpoint snapshots the live cursor map, so checkpointing
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
            m.ingest_blocks(tid, blocks, shape)?;
            m.cursors.insert(tid, next);
            m.checkpoint_if_due()
        })
    }

    fn scan(&mut self, tid: u64) -> Result<Option<(Arc<Schema>, ZSetBatch)>, MirrorError> {
        if !self.holds(tid) {
            return Ok(None);
        }
        self.touching("scanning a copy", |m| m.scan_inner(tid)).map(Some)
    }

    fn scan_spec(&mut self, tid: u64, spec: &[u8], reply_schema: &Schema) -> Result<StoreRead, MirrorError> {
        if !self.holds(tid) {
            return Ok(StoreRead::NotHeld);
        }
        self.touching("running a read spec", |m| m.scan_spec_inner(tid, spec, reply_schema))
            .map(StoreRead::Held)
    }

    fn cursor_of(&self, tid: u64) -> Option<DeltaCursor> {
        self.cursors.get(&tid).copied()
    }

    fn clear_cursors(&mut self) {
        self.cursors.clear();
    }

    fn checkpoint(&mut self) -> Result<(), MirrorError> {
        self.touching("checkpointing", |m| m.checkpoint_inner())
    }

    fn poisoned(&self) -> Option<&str> {
        self.poison.as_deref()
    }
}

impl Drop for Mirror {
    /// Check point on the way out, unless the store is poisoned. The stores and
    /// then the lock drop after this returns, in field-declaration order.
    ///
    /// A host that just drops it would otherwise lose every round since the last
    /// checkpoint. It has to be the checkpoint and not a per-store `flush()`:
    /// that is a **Base** round, on which a `Rederive` store — which every copy
    /// is — folds to RAM and publishes no manifest, so it would be pure waste
    /// after a checkpoint and actively wrong before one.
    ///
    /// A poisoned store skips the checkpoint: poisoning means a copy may be
    /// torn, and checkpointing a torn copy would publish the tear. That leaves
    /// the last checkpoint standing, which the next open resumes from.
    fn drop(&mut self) {
        // Swallowed, and the panic case with it: a `Drop` that panics while the
        // thread is already unwinding ends the process, which is the one outcome
        // this whole design exists to keep off a host.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if self.poison.is_none() {
                let _ = self.checkpoint_inner();
            }
            self.registry.close();
        }));
    }
}
