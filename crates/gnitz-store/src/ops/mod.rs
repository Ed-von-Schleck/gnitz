//! The DBSP operators: the kernels a compiled circuit's instructions dispatch
//! to, each consuming and emitting deltas rather than recomputing from state.
//!
//!   - `linear`    — filter, negate, union (Theorem 3.3: no state added)
//!   - `join`      — the bilinear operator, equi and range, over one probe
//!   - `reduce`    — the aggregates and their combined value index (`avi`)
//!   - `distinct`  — the weight clamps every set operation is built from
//!   - `exchange`  — repartition, relay and broadcast across workers
//!   - `cogroup` / `group_key` — the shared grouping and key machinery
//!
//! Every operator kernel is `pub`, because the VM that dispatches to them is in
//! `gnitz-server` — one crate up. What stays `pub(crate)` is the machinery no
//! instruction names: `cogroup`, `group_key`, and the `AdhocFold` the read rung
//! drives.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod cogroup;
mod distinct;
mod exchange;
mod group_key;
mod join;
mod linear;
mod reduce;

#[cfg(test)]
mod bench_join;

pub use distinct::op_weight_clamp;
pub use exchange::op_worker_filter;
pub use exchange::{
    op_relay_broadcast, op_relay_scatter_consolidated, op_repartition_batches, reset_slots, ScatterSpec,
};
pub use join::{merge_schemas_for_join, op_join_delta_trace, JoinProbe, RangeProbe};
pub use linear::op_union;
pub use linear::{op_filter, op_negate};
pub(crate) use reduce::AdhocFold;
pub use reduce::{op_populate_avi, op_reduce, ReducePlan};
