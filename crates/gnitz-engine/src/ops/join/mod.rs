//! Join operators: anti-join, semi-join, inner join, outer join.
//!
//! Split by match shape: `delta_trace` (Δ ⋈ trace), `delta_delta` (ΔA ⋈ ΔB),
//! `range` (non-equi delta-trace), and the shared `rowwrite` row builders.

mod delta_delta;
mod delta_trace;
mod range;
mod rowwrite;

#[cfg(test)]
mod test_common;

pub(crate) use delta_delta::{
    op_anti_join_delta_delta, op_join_delta_delta, op_semi_join_delta_delta,
};
pub(crate) use delta_trace::{
    op_anti_join_delta_trace, op_join_delta_trace, op_join_delta_trace_outer,
    op_semi_join_delta_trace,
};
pub(crate) use range::op_join_delta_trace_range;
