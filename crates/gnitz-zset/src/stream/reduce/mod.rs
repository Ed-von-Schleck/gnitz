//! Incremental reduce: `op_reduce`, its baked plan, and the combined
//! aggregate-value index its MIN/MAX read their history from.
//!
//! `tests/reduce.rs` holds the operator, its split and the ad-hoc fold to one
//! model of their input.

mod avi;
mod op_reduce;
mod plan;

#[cfg(test)]
#[path = "benches/reduce.rs"]
mod bench;

#[cfg(test)]
#[path = "tests/reduce.rs"]
mod tests;

pub use op_reduce::op_reduce;
pub use plan::ReducePlan;
