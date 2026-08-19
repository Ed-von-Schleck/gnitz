//! Exchange repartitioning facade: partition routing (`router`) and the
//! relay/scatter operators (`relay`) that drive the master worker-exchange.

mod relay;
mod router;

pub(crate) use relay::{op_relay_broadcast, op_relay_scatter_consolidated_mode, op_repartition_batches_mode};
pub(crate) use router::{op_worker_filter, RouteMode};

/// Reset `out` to `num_workers` empty slots, keeping the pooled allocations —
/// the prologue of every per-worker row-index fan-out, here and on the master's
/// write path.
pub(crate) fn reset_slots<T>(out: &mut Vec<Vec<T>>, num_workers: usize) -> &mut [Vec<T>] {
    if out.len() < num_workers {
        out.resize_with(num_workers, Vec::new);
    }
    let slots = &mut out[..num_workers];
    slots.iter_mut().for_each(Vec::clear);
    slots
}
