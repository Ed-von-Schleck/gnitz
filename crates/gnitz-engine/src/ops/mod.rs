mod cogroup;
mod distinct;
mod exchange;
mod index;
mod join;
mod linear;
mod reduce;
mod reindex;
mod scan;
mod util;

#[cfg(test)]
mod bench_secondary_index;

#[cfg(test)]
pub(crate) use distinct::op_distinct;
pub(crate) use distinct::op_weight_clamp;
pub(crate) use exchange::{
    op_partition_filter, op_relay_broadcast, op_relay_scatter_consolidated_mode, op_repartition_batches_mode, RouteMode,
};
pub(crate) use exchange::{with_broadcast_indices, with_worker_indices, worker_for_partition};
pub(crate) use index::{op_integrate_with_indexes, AviDesc};
// Facade for the AVI schema builder + AVI key layout, so out-of-ops callers
// (compiler, master) reach `crate::ops::X`, not the internals.
pub(crate) use index::make_avi_schema;
pub(crate) use join::{op_join_delta_trace, op_join_delta_trace_range};
pub(crate) use linear::{op_filter, op_map, op_negate, op_null_extend, op_union};
pub(crate) use reduce::{op_reduce, AggDescriptor, AggOp};
pub(crate) use scan::op_scan_trace;
pub(crate) use util::{all_payload_null_mask, global_group_key, AVI_AV_BYTES};
