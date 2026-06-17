//! Exchange repartitioning facade: partition routing (`router`) and the
//! relay/scatter operators (`relay`) that drive the master worker-exchange.

mod relay;
mod router;

pub(crate) use router::{
    PartitionRouter, RouteMode, op_partition_filter, with_broadcast_indices, with_worker_indices,
    worker_for_partition,
};
#[cfg(test)]
pub(crate) use router::compute_worker_indices;
pub(crate) use relay::{
    op_relay_broadcast, op_relay_scatter_consolidated_mode, op_repartition_batches_mode,
};
