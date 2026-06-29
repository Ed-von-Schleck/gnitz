//! Join operators: inner join (equi and non-equi).
//!
//! Split by match shape: `delta_trace` (equi Δ ⋈ trace), `range` (non-equi
//! delta-trace), and the shared `rowwrite` row builder.

mod delta_trace;
mod range;
mod rowwrite;

#[cfg(test)]
mod test_common;

pub(crate) use delta_trace::op_join_delta_trace;
pub(crate) use range::op_join_delta_trace_range;
