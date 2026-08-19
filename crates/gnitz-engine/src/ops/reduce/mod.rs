//! Reduce operator: accumulator, group key, argsort, AVI, op_reduce, and the
//! ad-hoc aggregation hash-fold sink.

mod adhoc_fold;
mod agg;
mod emit;
mod op_reduce;
mod plan;
mod sort;

#[cfg(test)]
mod tests;

pub(crate) use adhoc_fold::AdhocFold;
pub use agg::AggDescriptor;
pub use op_reduce::{op_reduce, AviHistory};
pub(crate) use plan::build_reduce_output_schema;
pub use plan::ReducePlan;
