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

pub(crate) use distinct::op_distinct;
pub(crate) use exchange::{
    PartitionRouter, with_worker_indices, with_broadcast_indices, worker_for_partition,
};
pub(crate) use exchange::{RouteMode, op_partition_filter, op_relay_broadcast, op_repartition_batches_mode, op_relay_scatter_consolidated_mode};
// Only the test harness routes via the eager (cloning) variant now; the
// production scatter paths all borrow via `with_worker_indices`.
#[cfg(test)]
pub(crate) use exchange::compute_worker_indices;
pub(crate) use index::{AviDesc, GiDesc, op_integrate_with_indexes};
// Facade for the index/aggregate-value schema builders + AVI key layout, so
// out-of-ops callers (compiler, master) reach `crate::ops::X`, not the internals.
pub(crate) use index::{make_avi_schema, make_gi_schema};
pub(crate) use util::{AVI_AV_BYTES, all_payload_null_mask};
pub(crate) use join::{
    op_anti_join_delta_delta, op_anti_join_delta_trace, op_join_delta_delta,
    op_join_delta_trace, op_join_delta_trace_outer, op_join_delta_trace_range,
    op_semi_join_delta_delta, op_semi_join_delta_trace,
};
pub(crate) use linear::{op_filter, op_map, op_negate, op_null_extend, op_union};
pub(crate) use reduce::{AggDescriptor, AggOp, op_gather_reduce, op_reduce};
pub(crate) use reduce::is_single_col_natural_pk;
pub(crate) use scan::op_scan_trace;

