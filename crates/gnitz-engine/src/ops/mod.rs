mod distinct;
mod exchange;
pub(crate) mod index;
mod join;
mod linear;
mod reduce;
mod scan;
pub(crate) mod util;

#[cfg(test)]
mod bench_secondary_index;

pub use distinct::op_distinct;
pub use exchange::{
    PartitionRouter, with_worker_indices, worker_for_partition,
};
pub(crate) use exchange::{RouteMode, op_partition_filter, op_relay_broadcast, op_repartition_batches_mode, op_relay_scatter_consolidated_mode};
// Only the test harness routes via the eager (cloning) variant now; the
// production scatter paths all borrow via `with_worker_indices`.
#[cfg(test)]
pub use exchange::compute_worker_indices;
pub use index::{AviDesc, GiDesc, op_integrate_with_indexes};
pub use join::{
    op_anti_join_delta_delta, op_anti_join_delta_trace, op_join_delta_delta,
    op_join_delta_trace, op_join_delta_trace_outer, op_join_delta_trace_range,
    op_semi_join_delta_delta, op_semi_join_delta_trace,
};
pub use linear::{op_filter, op_map, op_negate, op_null_extend, op_union};
pub use reduce::{AggDescriptor, AggOp, op_gather_reduce, op_reduce};
pub(crate) use reduce::is_single_col_natural_pk;
pub use scan::op_scan_trace;

