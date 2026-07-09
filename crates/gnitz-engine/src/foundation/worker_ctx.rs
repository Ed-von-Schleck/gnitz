//! Per-process worker identity: the worker rank and the worker count baked into
//! this process's compiled plans. Multi-process runtime state, not a planner
//! concern — written once by the worker bootstrap post-fork, read by the
//! compiler when emitting `PartitionFilter` nodes and naming scratch tables.

/// Process-local worker rank, set once per forked worker (and left at 0 in the
/// master / single-worker mode). Scratch operator-state tables (`Distinct`
/// history, `IntegrateTrace`) are named under the view directory, which all
/// forked workers share via the same `base_dir`; embedding the rank keeps each
/// worker's ephemeral shard isolated so siblings don't clobber each other.
///
/// These scratch tables flush in-memory (`in_memory_l0`), so the rank only
/// matters when a table *spills* past `INMEM_CEILING` (which writes shard
/// files into the shared tree) and at construction-time `erase_stale_shards`.
/// Retained for those paths; a future change that removed spill entirely could
/// revisit it.
static WORKER_RANK: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// Sibling of `WORKER_RANK`: the worker count baked into this process's compiled
/// plans. Set alongside the rank post-fork. Defaults to 1 (single process /
/// unit tests), which makes `PartitionFilter` a keep-all identity and the
/// broadcast input relay degenerate. `num_workers` is fixed for a process's
/// lifetime, so a topology change is a restart-and-recompile, not a live
/// plan-cache invalidation.
static NUM_WORKERS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);

/// The committed checkpoint generation baked into every manifest this process
/// publishes. A process-global `AtomicU64` mirroring `WORKER_RANK`: the manifest
/// stamp is read deep inside `storage/lsm` (L3), far below any place a parameter
/// could be threaded, and workers are single-threaded processes. The master sets
/// it at each gen-bump (and once at boot after `recover_sequences`, so the value
/// is COW-inherited by the forked workers); each worker sets it from the
/// `FLAG_FLUSH_EPH` group header before flushing its ephemeral view state.
///
/// Test caveat (same footgun as `WORKER_RANK`): `cargo test` shares one process
/// across test threads, so this atomic is shared. A test that reads a published
/// generation must set it and read it back within one non-yielding test.
static COMMITTED_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// This process's role in the multi-process server: `0` Standalone (the
/// default — unit tests and any in-process embedding), `1` Master (the pre-fork
/// dispatcher), `2` Worker (a forked slice owner). Set once per process:
/// `set_master_role` before `CatalogEngine::open`, `set_worker_rank` in the
/// forked child before any catalog work. Role-dependent behavior (index
/// backfill homing) is covered by the e2e suite over a real fork; unit tests
/// must never set a role — `cargo test` shares one process across test threads.
static ROLE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

const ROLE_MASTER: u8 = 1;
const ROLE_WORKER: u8 = 2;

/// Set the calling process's worker rank and worker count, and latch its role
/// to Worker. Called post-fork before any view is compiled, so the scratch
/// tables a worker opens carry its own rank, its `PartitionFilter` nodes are
/// emitted with this process's `(worker_id, num_workers)`, and its index tables
/// home into a per-rank subdirectory.
pub(crate) fn set_worker_rank(rank: u32, num_workers: u32) {
    WORKER_RANK.store(rank, std::sync::atomic::Ordering::Relaxed);
    NUM_WORKERS.store(num_workers.max(1), std::sync::atomic::Ordering::Relaxed);
    ROLE.store(ROLE_WORKER, std::sync::atomic::Ordering::Relaxed);
}

/// Latch this process's role to Master. Called at the top of `server_main`,
/// before `CatalogEngine::open`, so the pre-fork catalog replay hooks already
/// see Master (and skip the index backfill their forked children rebuild).
pub(crate) fn set_master_role() {
    ROLE.store(ROLE_MASTER, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn is_master() -> bool {
    ROLE.load(std::sync::atomic::Ordering::Relaxed) == ROLE_MASTER
}

pub(crate) fn is_worker() -> bool {
    ROLE.load(std::sync::atomic::Ordering::Relaxed) == ROLE_WORKER
}

pub(crate) fn worker_rank() -> u32 {
    WORKER_RANK.load(std::sync::atomic::Ordering::Relaxed)
}

/// The committed checkpoint generation this process last latched — the value
/// the ephemeral flush round stamps into manifests and `RederiveCheckpointed`
/// opens gate on. Storage never reads this directly; the catalog/runtime
/// callers pass it down as data.
pub(crate) fn committed_generation() -> u64 {
    COMMITTED_GENERATION.load(std::sync::atomic::Ordering::Relaxed)
}

/// Set the checkpoint generation this process stamps into published manifests.
/// Called on the master at each gen-bump and once at boot, and on each worker
/// from the `FLAG_FLUSH_EPH` group header before the ephemeral flush round.
pub(crate) fn set_committed_generation(g: u64) {
    COMMITTED_GENERATION.store(g, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn num_workers() -> u32 {
    NUM_WORKERS.load(std::sync::atomic::Ordering::Relaxed)
}
