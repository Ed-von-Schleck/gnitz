//! Aggregation over one Z-set: the aggregates, the row shape a reduce emits,
//! and the ad-hoc hash-fold sink behind a fold `ReadSpec`.
//!
//! The circuit reduce in `gnitz-trace`'s `stream` runs these same aggregates, group key and
//! row emitter, so a maintained aggregate and an ad-hoc one over the same group
//! set agree by construction.

mod adhoc_fold;
mod agg;
mod shape;

pub(crate) use adhoc_fold::AdhocFold;
pub use agg::{Agg, AggValues, FoldGroups, RangeGroups};
pub use shape::{ReduceShape, resolve_aggs};
