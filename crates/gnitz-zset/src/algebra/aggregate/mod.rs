//! Aggregation over one Z-set: the accumulators, the row shape a reduce emits,
//! and the ad-hoc hash-fold sink behind a fold `ReadSpec`.
//!
//! The circuit reduce in `stream` runs these same accumulators, group key and
//! row emitter, so a maintained aggregate and an ad-hoc one over the same group
//! set agree by construction. `tests/agg.rs` is the accumulator's own.

mod adhoc_fold;
mod agg;
mod emit;
mod shape;

pub(crate) use adhoc_fold::AdhocFold;
pub(crate) use agg::{Accumulator, ExtremeSpec, GroupedState};
pub(crate) use emit::emit_reduce_row;
pub(crate) use shape::ReduceShape;

#[cfg(test)]
mod bench;
