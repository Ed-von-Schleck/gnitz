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
/// These scratch tables now flush in-memory (`in_memory_l0`), so the rank only
/// matters when a table *spills* past `EPHEMERAL_INMEM_CEILING` (which still
/// writes `eph_shard_*` files into the shared tree) and at construction-time
/// `erase_stale_shards`. Retained for those paths; a future change that removed
/// spill entirely could revisit it.
static WORKER_RANK: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// Sibling of `WORKER_RANK`: the worker count baked into this process's compiled
/// plans. Set alongside the rank post-fork. Defaults to 1 (single process /
/// unit tests), which makes `PartitionFilter` a keep-all identity and the
/// broadcast input relay degenerate. `num_workers` is fixed for a process's
/// lifetime, so a topology change is a restart-and-recompile, not a live
/// plan-cache invalidation.
static NUM_WORKERS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);

/// Set the calling process's worker rank and worker count. Called post-fork
/// before any view is compiled, so the scratch tables a worker opens carry its
/// own rank and its `PartitionFilter` nodes are emitted with this process's
/// `(worker_id, num_workers)`.
pub(crate) fn set_worker_rank(rank: u32, num_workers: u32) {
    WORKER_RANK.store(rank, std::sync::atomic::Ordering::Relaxed);
    NUM_WORKERS.store(num_workers.max(1), std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn worker_rank() -> u32 {
    WORKER_RANK.load(std::sync::atomic::Ordering::Relaxed)
}

pub(crate) fn num_workers() -> u32 {
    NUM_WORKERS.load(std::sync::atomic::Ordering::Relaxed)
}
