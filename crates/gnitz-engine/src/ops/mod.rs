//! The DBSP operators: the kernels a compiled circuit's instructions dispatch
//! to, each consuming and emitting deltas rather than recomputing from state.
//!
//!   - `linear`    — filter, negate, union (Theorem 3.3: no state added)
//!   - `join`      — the bilinear operator, equi and range, over one probe
//!   - `reduce`    — the aggregates and their secondary value indexes
//!   - `distinct`  — the weight clamps every set operation is built from
//!   - `exchange`  — repartition, relay and broadcast across workers
//!   - `index`     — integration into a store and its secondary indexes
//!   - `cogroup` / `util` — the shared grouping and key machinery
//!
//! What leaves the crate is the exchange surface alone: the runtime drives
//! repartition and relay directly, so those names are `pub`. Every other
//! operator is reached through a compiled circuit, never called by name from
//! outside, and stays `pub(crate)`.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod cogroup;
mod distinct;
mod exchange;
mod index;
mod join;
mod linear;
mod reduce;
mod util;

#[cfg(test)]
mod bench_join;
#[cfg(test)]
mod bench_secondary_index;

pub(crate) use distinct::op_weight_clamp;
pub(crate) use exchange::op_worker_filter;
pub use exchange::{
    op_relay_broadcast, op_relay_scatter_consolidated_mode, op_repartition_batches_mode, reset_slots, RouteMode,
};
pub(crate) use index::{op_integrate_with_indexes, AviBake, IntegrateTarget as OpsIntegrateTarget};
pub(crate) use join::{op_join_delta_trace, JoinProbe, RangeProbe};
pub(crate) use linear::{op_filter, op_negate, op_union};
pub(crate) use reduce::{op_reduce, AdhocFold, AggDescriptor, AviHistory, ReducePlan};
