//! Per-process runtime identity: which worker this process is, how many there
//! are, its role in the fork, and the checkpoint generation it stamps into
//! manifests. Written once by the bootstrap, read wherever no catalog or
//! dispatcher is on the stack.
//!
//! `cargo test` shares one process across test threads, so every value here is
//! shared between concurrently running tests. A test that depends on one must
//! set it and read it back without yielding; the role is covered by the e2e
//! suite over a real fork, and unit tests must never set one.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering::Relaxed};

/// Process-local worker rank, 0 in the master and in single-worker mode. It
/// names the `w{rank}of{n}` child directory of every relation store this
/// process opens, so a stale value silently points a store at a sibling's data.
static WORKER_RANK: AtomicU32 = AtomicU32::new(0);

/// The worker count baked into this process's compiled plans, set alongside the
/// rank post-fork. Defaults to 1 for the master and for unit tests. Fixed for a
/// process's lifetime, so a topology change is a restart-and-recompile, not a
/// live plan-cache invalidation.
static NUM_WORKERS: AtomicU32 = AtomicU32::new(1);

/// The generation a manifest must carry to be resumed from — mirrored here from
/// the catalog's `resume_generation` for the `Table::new` callers that hold no
/// catalog. The master sets it at each gen-bump and once at boot, so the value
/// is COW-inherited by the forked workers; each worker sets it from the
/// `FLAG_FLUSH_EPH` group header.
static COMMITTED_GENERATION: AtomicU64 = AtomicU64::new(0);

/// This process's role in the multi-process server: `0` Standalone (the
/// default — unit tests and any in-process embedding), `1` Master (the pre-fork
/// dispatcher), `2` Worker (a forked slice owner). Set once per process:
/// `set_master_role` before `CatalogEngine::open`, `set_worker_role` in the
/// forked child before any catalog work.
static ROLE: AtomicU8 = AtomicU8::new(0);

const ROLE_MASTER: u8 = 1;
const ROLE_WORKER: u8 = 2;

/// Set the calling process's worker rank and worker count. Called post-fork
/// before any view is compiled, so the scratch tables a worker opens carry its
/// own rank and its `WorkerFilter` nodes are emitted with this process's
/// `(worker_id, num_workers)`.
pub(crate) fn set_worker_identity(rank: u32, num_workers: u32) {
    WORKER_RANK.store(rank, Relaxed);
    NUM_WORKERS.store(num_workers.max(1), Relaxed);
}

/// Latch this process's role to Worker, in the forked child before any catalog
/// work.
pub(crate) fn set_worker_role() {
    ROLE.store(ROLE_WORKER, Relaxed);
}

/// Latch this process's role to Master. Called at the top of `server_main`,
/// before `CatalogEngine::open`, so the pre-fork catalog replay hooks already
/// see Master (and skip the index backfill their forked children rebuild).
pub(crate) fn set_master_role() {
    ROLE.store(ROLE_MASTER, Relaxed);
}

pub(crate) fn is_master() -> bool {
    ROLE.load(Relaxed) == ROLE_MASTER
}

pub(crate) fn is_worker() -> bool {
    ROLE.load(Relaxed) == ROLE_WORKER
}

pub(crate) fn worker_rank() -> u32 {
    WORKER_RANK.load(Relaxed)
}

pub(crate) fn num_workers() -> u32 {
    NUM_WORKERS.load(Relaxed)
}

/// The generation a manifest must carry to be resumed from. Read this only
/// where the catalog is out of reach; it holds the same value in its
/// `resume_generation` field, and a reader that wants the *next* generation to
/// publish must take `durable_generation` from the catalog instead — the two
/// diverge across `recovery_start_generation_bump`.
pub(crate) fn committed_generation() -> u64 {
    COMMITTED_GENERATION.load(Relaxed)
}

/// Mirror the catalog's resume generation here. Called only by
/// `set_resume_generation`, which moves the field and this mirror together.
pub(crate) fn set_committed_generation(g: u64) {
    COMMITTED_GENERATION.store(g, Relaxed);
}
