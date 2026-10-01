//! Reduce operator: the accumulators, the combined aggregate-value index,
//! `op_reduce` itself, and the ad-hoc aggregation hash-fold sink.
//!
//! `tests/reduce.rs` holds the operator, its split and the ad-hoc fold to one
//! model of their input; `tests/agg.rs` is the accumulator's own.

mod adhoc_fold;
mod agg;
mod avi;
mod emit;
mod op_reduce;
mod plan;

#[cfg(test)]
mod bench;

#[cfg(test)]
#[path = "tests/reduce.rs"]
mod tests;

pub(crate) use adhoc_fold::AdhocFold;
pub use avi::{avi_batch, AviBake};
pub use op_reduce::op_reduce;
pub use plan::ReducePlan;
