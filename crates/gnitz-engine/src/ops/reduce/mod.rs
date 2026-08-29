//! Reduce operator: the accumulators, the group argsort, the combined
//! aggregate-value index, `op_reduce` itself, and the ad-hoc aggregation
//! hash-fold sink.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod adhoc_fold;
mod agg;
mod avi;
mod emit;
mod op_reduce;
mod plan;
mod sort;

#[cfg(test)]
mod bench_secondary_index;

#[cfg(test)]
#[path = "tests/reduce.rs"]
mod tests;

pub(crate) use adhoc_fold::AdhocFold;
pub(crate) use agg::AggDescriptor;
pub(crate) use avi::op_populate_avi;
pub(crate) use op_reduce::op_reduce;
pub(crate) use plan::ReducePlan;
