//! Exchange repartitioning facade: partition routing (`router`) and the
//! relay scatter operator (`relay`) that drives the master worker-exchange.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod relay;
mod router;

pub use relay::op_relay_scatter;
pub use router::op_worker_filter;
pub use router::ScatterSpec;
