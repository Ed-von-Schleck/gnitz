//! The handle: what a host opens, registers views on, polls, and reads through.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use gnitz_core::{DeltaCursor, GnitzClient, RelKind, Schema};
use gnitz_engine::catalog::CatalogEngine;
use gnitz_engine::foundation::env::env_num;
use gnitz_engine::foundation::fault::Seam;
use gnitz_engine::foundation::worker_ctx;
use gnitz_engine::schema::SchemaDescriptor;

use crate::cursors::{read_cursors, write_cursors};
use crate::error::MirrorError;

/// Applied delta bytes after which an apply drives a checkpoint of its own.
/// `GNITZ_MIRROR_CHECKPOINT_BYTES` overrides it.
const DEFAULT_CHECKPOINT_BYTES: usize = 64 * 1024 * 1024;

/// `GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR`: fail the next checkpoint once, before
/// it has changed anything durable. Debug-only. It is the only way to reach
/// [`Mirror::checkpoint`]'s report-rather-than-poison path without a disk fault.
static CHECKPOINT_ERROR: Seam = Seam::new("GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR");

/// One live handle per process.
///
/// Not a `!Send` consequence but a process-global one: `worker_ctx` holds the
/// committed checkpoint generation as a process static, `CatalogEngine::open`
/// publishes its own durable generation into it, and every `Rederive` store
/// captures that global at open. A second handle on a second data directory
/// would overwrite it, and the first handle's next store would then be created
/// against a generation from someone else's checkpoint history.
///
/// A latch the handle's `Drop` clears, not a once-per-process trip switch, so a
/// host (and this crate's own tests) may open and drop repeatedly — but never
/// *concurrently*, which is the same process-global state the latch exists for.
static HANDLE_LIVE: AtomicBool = AtomicBool::new(false);

struct HandleGuard;

impl HandleGuard {
    fn take() -> Result<Self, MirrorError> {
        if HANDLE_LIVE.swap(true, Ordering::SeqCst) {
            return Err(MirrorError::Engine(
                "a mirror handle is already live in this process".to_string(),
            ));
        }
        Ok(HandleGuard)
    }
}

impl Drop for HandleGuard {
    fn drop(&mut self) {
        HANDLE_LIVE.store(false, Ordering::SeqCst);
    }
}

/// One mirrored view: what registration resolved, and where its feed got to.
pub(crate) struct MirroredView {
    pub(crate) schema_name: String,
    pub(crate) name: String,
    pub(crate) kind: RelKind,
    /// The client-side schema, as the resolve returned it — hidden columns
    /// included, which is what keeps `pk_stride` right for a view whose physical
    /// PK is a synthetic hidden column.
    pub(crate) schema: Arc<Schema>,
    /// The client-side shape a poll's *request* carries. `Arc` like `schema`
    /// above: a poll hands it to the client by clone, and a deep one would
    /// allocate per column on every poll, including the empty ones.
    pub(crate) delta_reply_schema: Arc<Schema>,
    /// The engine's own descriptor for the local store — read back from the
    /// local catalog, so it is by construction the one the store was created
    /// with.
    pub(crate) view_desc: SchemaDescriptor,
    /// The engine's derived delta-store shape, from the same builder the server
    /// derives its own delta store with.
    pub(crate) delta_desc: SchemaDescriptor,
}

/// A maintained local copy of one or more views, and the connection that feeds
/// it.
///
/// One handle mirrors many views: the engine is a registry of relations, so each
/// lives under its own server id in one local catalog, one data directory, one
/// lock and one checkpoint. Each carries its own cursor and is polled
/// independently.
pub struct Mirror {
    pub(crate) engine: CatalogEngine,
    pub(crate) client: GnitzClient,
    pub(crate) views: HashMap<u64, MirroredView>,
    /// Where each view's feed got to, and by its presence that the copy is
    /// valid — see [`Mirror::readable`].
    ///
    /// Not a field of [`MirroredView`]: it outlives one. A checkpoint
    /// republishes every copy in the local catalog, including views this session
    /// never re-registered, so the cursors beside them must cover the same set.
    pub(crate) cursors: HashMap<u64, DeltaCursor>,
    pub(crate) base_dir: String,
    pub(crate) poison: Option<String>,
    applied_bytes: usize,
    checkpoint_bytes: usize,
    _guard: HandleGuard,
}

impl Mirror {
    /// Open (or create) a mirror at `base_dir`, feeding it through `client`.
    ///
    /// The handle owns the connection: registration, every poll and every read
    /// it delegates upstream all run on this thread, so nothing but the host's
    /// own calls crosses a channel. While a poll is in flight a local read waits
    /// one round trip — which is what a read against the server pays every time.
    ///
    /// Fails if the process has taken a server role, if another handle is live
    /// in it, or if another process holds `base_dir`.
    pub fn open(base_dir: &str, client: GnitzClient) -> Result<Self, MirrorError> {
        // Assert Standalone rather than set it. `set_worker_role` would invert
        // the local index backfill, the index home directory and `store_lsn`'s
        // own assertion; and a mirror opened inside a forked worker would home
        // its stores at `w{k}of1`, which the next boot sweep deletes as unowned.
        if !worker_ctx::is_standalone() {
            return Err(MirrorError::Engine(
                "a mirror cannot be opened in a process that has taken a server role".to_string(),
            ));
        }
        let guard = HandleGuard::take()?;
        let mut engine = CatalogEngine::open(base_dir, 1)?;

        // The generation is half the reopen verdict; the topology word is the
        // other half, and nothing asks for the mirror: a view's own recovery
        // source gates on the generation alone, and the server's second gate
        // lives in a pre-fork step the mirror never runs. Re-record either way,
        // so the next open has one to compare.
        let cursors = read_cursors(base_dir, engine.resume_generation(), engine.topology_matches());
        engine
            .record_topology(1)
            .map_err(|e| MirrorError::Engine(format!("topology record failed: {e}")))?;

        Ok(Mirror {
            engine,
            client,
            views: HashMap::new(),
            cursors,
            base_dir: base_dir.to_string(),
            poison: None,
            applied_bytes: 0,
            checkpoint_bytes: env_num("GNITZ_MIRROR_CHECKPOINT_BYTES", DEFAULT_CHECKPOINT_BYTES),
            _guard: guard,
        })
    }

    /// Whether this process currently holds a live handle, and so whether
    /// [`Mirror::open`] would be refused for that reason. Not a reservation: only
    /// `open` itself takes the latch.
    pub fn any_live() -> bool {
        HANDLE_LIVE.load(Ordering::SeqCst)
    }

    /// The connection the handle owns. A host writes through it — the mirror is
    /// read-only, and a mirrored relation is a view, which is not a DML target
    /// upstream either.
    ///
    /// A host that drops a view it mirrors stops mirroring it; the crate offers
    /// [`Mirror::forget_view`] as that statement of intent rather than inferring
    /// it from a `DROP` that happened to pass through here.
    pub fn client_mut(&mut self) -> &mut GnitzClient {
        &mut self.client
    }

    /// The views this handle holds a registration for — the set [`Mirror::poll`]
    /// advances, which is wider than [`Mirror::mirrors`] by the ones whose copy
    /// the next poll has yet to make valid.
    pub fn mirrored_ids(&self) -> Vec<u64> {
        self.views.keys().copied().collect()
    }

    /// The registration a local read answers out of, and the one gate that says
    /// it may: `None` sends the read upstream.
    ///
    /// A registration is written before the copy behind it exists, and a
    /// bootstrap erases the copy before it re-reads it. The cursor is what marks
    /// the difference, so it is returned together with the registration and
    /// cannot be forgotten at a call site.
    pub(crate) fn readable(&self, table_id: u64) -> Option<&MirroredView> {
        self.views
            .get(&table_id)
            .filter(|_| self.cursors.contains_key(&table_id))
    }

    /// Whether `table_id` is answered locally — [`Self::readable`] as a bool.
    pub fn mirrors(&self, table_id: u64) -> bool {
        self.readable(table_id).is_some()
    }

    /// The round `table_id`'s copy is current as of — what [`Mirror::poll`] last
    /// carried through, and the round a local read of it answers at. `None` when
    /// there is no valid copy to read one off.
    pub fn cursor_of(&self, table_id: u64) -> Option<DeltaCursor> {
        self.cursors.get(&table_id).copied()
    }

    // -- Poison ------------------------------------------------------------

    /// The message that poisoned this handle, if any.
    pub fn poisoned(&self) -> Option<&str> {
        self.poison.as_deref()
    }

    pub(crate) fn check_poison(&self) -> Result<(), MirrorError> {
        match &self.poison {
            Some(m) => Err(MirrorError::Poisoned(m.clone())),
            None => Ok(()),
        }
    }

    /// Poison the handle and report why.
    ///
    /// An ingest error is the one that *must* poison: the delta it dropped
    /// leaves a hole the cursor would step over, so every later read would
    /// answer off a copy silently missing rows. A checkpoint error is the
    /// opposite case — see [`Mirror::checkpoint`].
    pub(crate) fn poison(&mut self, why: String) -> MirrorError {
        if self.poison.is_none() {
            self.poison = Some(why.clone());
        }
        MirrorError::Poisoned(why)
    }

    /// Run `f` with a panic guard that poisons before the unwind continues.
    ///
    /// The `Err` channel covers the expected faults — a full or failing disk —
    /// and works in every build. This covers *bugs*, and only where the panic
    /// unwinds: a mirror built inside this workspace in release inherits
    /// `panic = "abort"`, which cannot be set per package, so a bug in the
    /// engine still ends the process there. That is the ordinary contract of any
    /// `panic = "abort"` library.
    pub(crate) fn guarded<T>(&mut self, what: &str, f: impl FnOnce(&mut Self) -> T) -> T {
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

    pub(crate) fn checkpoint_is_due(&self) -> bool {
        self.applied_bytes >= self.checkpoint_bytes
    }

    /// Make every copy and its cursor durable — the committer's sequence minus
    /// the steps that only exist for a SAL: raise the generation fence, flush the
    /// copies to it, record the cursors at the generation the flush reports.
    ///
    /// The round publishes even for a copy that absorbed nothing since the last
    /// checkpoint, which is what keeps "resumed at `gen`" and "never
    /// checkpointed" distinguishable.
    ///
    /// **A failed checkpoint is reported, not poisoned** — the one `Err` here
    /// that is not fatal to the handle. The flush writes shards and publishes
    /// manifests; neither mutates what a store holds, so every copy is intact in
    /// the RAM tier and retrying is sound. What it leaves durably is a
    /// generation ahead of the manifests beside a cursor file still at the last
    /// good generation, which a reopen reads as bootstrap. Under a sustained
    /// fault the RAM tier grows and the host is told at exactly the rate it
    /// asks: every checkpoint returns the same error.
    pub fn checkpoint(&mut self) -> Result<(), MirrorError> {
        self.check_poison()?;
        self.guarded("checkpointing", |m| m.checkpoint_inner())
    }

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
}

impl Drop for Mirror {
    /// Check point on the way out — unless the handle is poisoned.
    ///
    /// A host that just drops the handle would otherwise lose every round since
    /// the last checkpoint. It has to be the whole sequence and not
    /// `CatalogEngine::close` alone: `close` flushes each store on the **Base**
    /// round, where a `Rederive` store folds to RAM and publishes nothing, so
    /// `close` by itself would discard exactly what this is meant to save.
    ///
    /// A poisoned handle skips both: poisoning means a store may be torn, and
    /// checkpointing a torn store would publish the tear. That leaves the last
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
