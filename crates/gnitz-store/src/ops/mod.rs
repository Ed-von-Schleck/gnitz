//! The DBSP operators: the kernels a compiled circuit's instructions dispatch
//! to, each consuming and emitting deltas rather than recomputing from state.
//!
//!   - `linear`    — filter, union (Theorem 3.3: no state added)
//!   - `join`      — the bilinear operator, equi, range and cross, over one probe
//!   - `reduce`    — the aggregates and their combined value index (`avi`)
//!   - `topn`      — per-group top-N over an ordered index of every input row
//!   - `order_image` — the byte images both indexes order by
//!   - `clamp`     — the weight clamps every set operation is built from
//!   - `exchange`  — repartition and broadcast across workers
//!   - `group_key` — the shared key machinery
//!
//! Every operator kernel is `pub`, because the VM that dispatches to them is in
//! `gnitz-server` — one crate up, and it also builds each operator's artifact
//! (plan, probe, output schema) through the constructor here that owns the
//! guards a client-supplied circuit clears. What stays `pub(crate)` is the
//! machinery no instruction names: `group_key` and the `AdhocFold` the read rung
//! drives.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod clamp;
mod exchange;
mod group_key;
mod join;
mod linear;
mod order_image;
mod reduce;
mod topn;

#[cfg(test)]
mod bench_join;

pub use clamp::op_weight_clamp;
pub use exchange::op_worker_filter;
pub use exchange::{op_exchange_gather, op_exchange_route, op_exchange_share, ScatterSpec};
pub use join::{op_join_delta_trace, JoinPlan, JoinProbe};
pub use linear::{null_extend_output_schema, op_filter, op_union, union_nullability_merge};
pub(crate) use reduce::AdhocFold;
pub use reduce::{avi_batch, op_reduce, AviBake, ReducePlan};
pub use topn::{op_topn, TopNPlan};
