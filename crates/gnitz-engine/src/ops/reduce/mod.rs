//! Reduce operator: accumulator, group key, argsort, AVI, op_reduce.

mod agg;
mod emit;
mod op_reduce;
mod plan;
mod sort;

#[cfg(test)]
mod tests;

pub use agg::{AggDescriptor, AggOp};
pub use op_reduce::op_reduce;
pub use plan::ReducePlan;
