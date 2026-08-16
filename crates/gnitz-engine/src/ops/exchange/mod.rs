//! Exchange repartitioning facade: partition routing (`router`) and the
//! relay/scatter operators (`relay`) that drive the master worker-exchange.

mod relay;
mod router;

pub(crate) use relay::{op_relay_broadcast, op_relay_scatter_consolidated_mode, op_repartition_batches_mode};
pub(crate) use router::{op_worker_filter, RouteMode};
