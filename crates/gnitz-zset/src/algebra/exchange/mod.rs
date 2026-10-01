//! Exchange repartitioning facade: partition routing (`router`) and the gather
//! the receiving end of a worker exchange runs (`gather`).
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod gather;
mod router;

pub use gather::op_exchange_gather;
pub use router::{op_worker_filter, ScatterPlan};
