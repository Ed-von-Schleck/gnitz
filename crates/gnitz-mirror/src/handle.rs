//! The store: what a host opens, and the `MirrorStore` surface a client drives
//! it through.

use std::collections::HashMap;
use std::sync::Arc;

use gnitz_core::{DeltaCursor, Invalidate, MirrorError, MirrorStore, RawBlock, Schema, Shape, StoreRead, ZSetBatch};
use gnitz_engine::catalog::CatalogEngine;
use gnitz_engine::foundation::env::env_num;
use gnitz_engine::foundation::fault::Seam;
use gnitz_engine::foundation::worker_ctx;
use gnitz_engine::schema::SchemaDescriptor;

use crate::cursors::{read_cursors, write_cursors};

/// Applied delta bytes after which an apply drives a checkpoint of its own.
/// `GNITZ_MIRROR_CHECKPOINT_BYTES` overrides it.
const DEFAULT_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;

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
    /// The local store's own descriptor, read back from the local catalog — so
    /// it is by construction the one the store was created with.
    pub(crate) view_desc: SchemaDescriptor,
    /// The derived delta-store shape, from the same builder the server derives
    /// its own with.
    pub(crate) delta_desc: SchemaDescriptor,
}

/// A maintained local copy of one or more views.
///
/// One store holds many: the engine is a registry of relations, so each lives
/// under its own server id in one local catalog, one data directory, one lock
/// and one checkpoint. Each carries its own cursor and is advanced
/// independently.
pub struct Mirror {
    pub(crate) engine: CatalogEngine,
    /// Per relation, what its registration fixed. Dropped only by
    /// `Invalidate::Registration`, alongside the catalog rows.
    pub(crate) shapes: HashMap<u64, Shapes>,
    /// Where each copy's feed got to, and by its presence that the copy is valid
    /// — the store half of the readability gate. It outlives one session's
    /// registrations on purpose: a checkpoint republishes every copy in the local
    /// catalog, so the cursors beside them must cover the same set.
    pub(crate) cursors: HashMap<u64, DeltaCursor>,
    pub(crate) base_dir: String,
    pub(crate) poison: Option<String>,
    applied_bytes: usize,
    checkpoint_bytes: usize,
}

// SAFETY: no handle into the engine can escape. `Mirror`'s whole public surface
// is `open` plus `impl MirrorStore`, whose signatures are `gnitz-core` types and
// primitives — so every `Rc` the engine mints is reached only from inside it and
// a move carries them all together. `Mirror` is `!Sync` (the engine's
// `UnsafeCell`s and raw pointers make it so), so the graph is reached by one
// thread at a time. What orders the non-atomic refcounts across a handover is
// whichever edge the host holds the client behind, and there are three: a
// blocking host holds the client by value, so the move itself is the edge; the
// Python host's client never moves — it sits at a fixed address in the Python
// heap — and the edge is pyo3's borrow flag, an `AcqRel` compare-exchange on
// entry to every `&mut self` method and a `Release` store on exit; `gnitz-tokio`
// reaches the same store from `spawn_blocking` threads, where its own lock is
// the edge.
//
// Nothing under the engine is thread-affine: process-wide atomics, an `flock`
// held as an open file description, an `io_uring` built per call. The
// `thread_local!`s are scratch pools that hold nothing between calls, bar a
// compaction-statistics counter a moved engine splits across two copies, which
// only an `#[ignore]`d amplification bench reads.
unsafe impl Send for Mirror {}

impl Mirror {
    /// Open (or create) a store at `base_dir`.
    ///
    /// Fails if the process has taken a server role, or if `base_dir` is already
    /// held — by another process, or by another store in this one.
    pub fn open(base_dir: &str) -> Result<Self, MirrorError> {
        // Assert Standalone rather than set it. `set_worker_role` would invert
        // the local index backfill, the index home directory and `store_lsn`'s
        // own assertion; and a store opened inside a forked worker would home
        // its stores at `w{k}of1`, which the next boot sweep deletes as unowned.
        if !worker_ctx::is_standalone() {
            return Err(MirrorError::Engine(
                "a mirror cannot be opened in a process that has taken a server role".to_string(),
            ));
        }
        let mut engine = CatalogEngine::open(base_dir, 1)?;

        // The stores are already open, so the cursor read takes the whole verdict
        // they took: the same generation, the same topology word, and — per view —
        // whether that store actually came back. A cursor naming a copy that is
        // not there is dropped, which makes its view bootstrap.
        // `record_topology(1)` follows so the next open has one to compare.
        let mut cursors = read_cursors(base_dir, engine.resume_generation(), engine.topology_matches());
        cursors.retain(|&vid, _| engine.store_resumed(vid as i64));
        engine
            .record_topology(1)
            .map_err(|e| MirrorError::Engine(format!("topology record failed: {e}")))?;

        Ok(Mirror {
            engine,
            shapes: HashMap::new(),
            cursors,
            base_dir: base_dir.to_string(),
            poison: None,
            applied_bytes: 0,
            checkpoint_bytes: env_num("GNITZ_MIRROR_CHECKPOINT_BYTES", DEFAULT_CHECKPOINT_BYTES),
        })
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
    /// would let it publish torn state at a fresh generation with a cursor file
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

    /// Make every copy and its cursor durable — the committer's sequence minus
    /// the steps that only exist for a SAL: raise the generation fence, flush the
    /// copies to it, record the cursors at the generation the flush reports. The
    /// round publishes even for a copy that absorbed nothing, which is what keeps
    /// "resumed at `gen`" and "never checkpointed" distinguishable.
    ///
    /// **A failed checkpoint is reported, not poisoned** — the one `Err` here
    /// that is not fatal. The flush writes shards and publishes manifests;
    /// neither mutates what a store holds, so every copy is intact in the RAM
    /// tier and retrying is sound. What it leaves durably is a generation ahead
    /// of the manifests beside a cursor file at the last good one, which a reopen
    /// reads as bootstrap.
    fn checkpoint_inner(&mut self) -> Result<(), MirrorError> {
        // Before the bump, so the injected failure is the one that changed
        // nothing durably — the retryable case this method's doc promises.
        if CHECKPOINT_ERROR.take_once() {
            return Err(MirrorError::Engine("injected checkpoint failure".to_string()));
        }
        self.engine.bump_checkpoint_generation()?;
        let generation = self.engine.flush_ephemeral_round()?;
        let cursors: Vec<(u64, DeltaCursor)> = self.cursors.iter().map(|(&view_id, &c)| (view_id, c)).collect();
        write_cursors(&self.base_dir, generation, &cursors)?;
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
        if !self.engine.has_id(tid as i64) {
            self.shapes.remove(&tid);
            return Ok(());
        }

        // The erase unlinks this worker's child manifest, so the rebuilt store's
        // `Rederive` open peeks `None` and *erases* the stale shards rather than
        // reloading them, then rebuilds the handle empty. It is exactly the state
        // transition a bootstrap needs, and it compiles nothing.
        self.engine
            .reset_view_output_for_rebuild(tid as i64)
            .map_err(|e| self.poison(format!("erasing the copy of {tid} failed: {e}")))?;
        if level == Invalidate::Copy {
            if BOOTSTRAP_ERROR.take_once() {
                return Err(MirrorError::Engine("injected bootstrap failure".to_string()));
            }
            return Ok(());
        }

        self.shapes.remove(&tid);
        self.engine.retract_relation_registration(tid as i64)?;
        // No worker here can still be applying the create this drop races, so the
        // gate the server needs is vacuous and the directory goes now.
        self.engine.defer_pending_dir_deletions();
        self.engine.drain_checkpoint_gated_deletions();
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

    fn schema_id(&mut self, schema_name: &str) -> Option<u64> {
        self.engine.schema_id(schema_name).map(|id| id as u64)
    }

    fn register(
        &mut self,
        tid: u64,
        schema_id: u64,
        schema_name: &str,
        name: &str,
        schema: &Schema,
    ) -> Result<Vec<u64>, MirrorError> {
        self.touching("registering a view", |m| {
            m.register_inner(tid, schema_id, schema_name, name, schema)
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
    /// loop, one engine ingest per block, and a failure at block *k* leaves
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
    /// Check point on the way out — unless the store is poisoned.
    ///
    /// A host that just drops it would otherwise lose every round since the last
    /// checkpoint. It has to be the whole sequence and not `CatalogEngine::close`
    /// alone: `close` flushes each store on the **Base** round, where a
    /// `Rederive` store folds to RAM and publishes nothing, so `close` by itself
    /// would discard exactly what this is meant to save.
    ///
    /// A poisoned store skips both: poisoning means a copy may be torn, and
    /// checkpointing a torn copy would publish the tear. That leaves the last
    /// checkpoint standing, which the next open resumes from.
    fn drop(&mut self) {
        if self.poison.is_some() {
            return;
        }
        // Swallowed, and the panic case with it: a `Drop` that panics while the
        // thread is already unwinding ends the process, which is the one outcome
        // this whole design exists to keep off a host.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = self.checkpoint_inner();
            self.engine.close();
        }));
    }
}
