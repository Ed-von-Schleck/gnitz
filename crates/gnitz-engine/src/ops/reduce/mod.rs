//! Reduce operator: accumulator, group key, argsort, AVI, op_reduce, op_gather_reduce.

mod agg;
mod emit;
mod op_gather;
mod op_reduce;
mod sort;

#[cfg(test)]
mod tests;

pub use agg::{AggDescriptor, AggOp};
pub(crate) use agg::is_single_col_natural_pk;
pub use op_gather::op_gather_reduce;
pub use op_reduce::op_reduce;
