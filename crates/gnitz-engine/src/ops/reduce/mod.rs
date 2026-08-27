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
pub(crate) use agg::AggDescriptor;
pub(crate) use op_reduce::{op_reduce, AviHistory};
pub(crate) use plan::ReducePlan;
