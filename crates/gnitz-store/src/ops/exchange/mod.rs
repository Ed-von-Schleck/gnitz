//! Exchange repartitioning facade: partition routing (`router`) and the route
//! and gather operators (`round`) either end of a worker exchange runs.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod round;
mod router;

pub use round::{op_exchange_gather, op_exchange_route};
pub use router::{op_worker_filter, ScatterPlan};
