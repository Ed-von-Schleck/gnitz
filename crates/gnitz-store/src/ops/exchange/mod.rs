//! Exchange repartitioning facade: partition routing (`router`) and the
//! relay/scatter operators (`relay`) that drive the master worker-exchange.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod relay;
mod router;

pub use relay::{op_relay_broadcast, op_relay_scatter_consolidated, op_repartition_batches};
pub use router::op_worker_filter;
pub use router::ScatterSpec;
