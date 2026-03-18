# gnitz/dbsp/ops/exchange.py
#
# Column-based hash for repartitioning batches across workers.
# Single authority for all partition routing logic.

from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128, intmask
from gnitz.core.batch import ArenaZSetBatch, ColumnarBatchAccessor
from gnitz.storage.partitioned_table import _partition_for_key
from gnitz.dbsp.ops.group_index import _extract_group_key


class PartitionAssignment(object):
    """Maps 256 partitions to N workers. Worker w owns [start, end)."""

    _immutable_fields_ = ["num_workers", "starts[*]", "ends[*]",
                          "_partition_to_worker[*]"]

    def __init__(self, num_workers):
        self.num_workers = num_workers
        chunk = 256 // num_workers
        starts = [0] * num_workers
        ends = [0] * num_workers
        for w in range(num_workers):
            starts[w] = w * chunk
            if w == num_workers - 1:
                ends[w] = 256
            else:
                ends[w] = (w + 1) * chunk
        self.starts = starts
        self.ends = ends
        # Precompute O(1) lookup table
        lut = [0] * 256
        for w in range(num_workers):
            for p in range(starts[w], ends[w]):
                lut[p] = w
        self._partition_to_worker = lut

    def worker_for_partition(self, partition_idx):
        """Returns the worker ID that owns the given partition."""
        return self._partition_to_worker[partition_idx]

    def range_for_worker(self, worker_id):
        """Returns (start, end) partition range for the given worker."""
        return self.starts[worker_id], self.ends[worker_id]


def worker_for_pk(pk_lo, pk_hi, assignment):
    """Route a single PK to its owning worker (for fan_out_seek)."""
    return assignment.worker_for_partition(_partition_for_key(r_uint64(pk_lo), r_uint64(pk_hi)))


def hash_row_by_columns(batch, row_idx, col_indices):
    """Compute destination partition for a row using group-key semantics.

    For single-column shards on the PK column, reads pk_lo/pk_hi directly
    (col_bufs[pk_index] has stride=0 and cannot be used for routing).
    All other cases delegate to _extract_group_key for consistency with
    the reduce output PK partition routing.
    """
    schema = batch._schema
    if len(col_indices) == 1 and col_indices[0] == schema.pk_index:
        return _partition_for_key(r_uint64(batch._read_pk_lo(row_idx)),
                                  r_uint64(batch._read_pk_hi(row_idx)))
    acc = ColumnarBatchAccessor(schema)
    acc.bind(batch, row_idx)
    group_key = _extract_group_key(acc, schema, col_indices)
    return _partition_for_key(r_uint64(group_key), r_uint64(group_key >> 64))


def repartition_batch(batch, shard_col_indices, num_workers, assignment):
    """Split a batch into N sub-batches by hashing shard columns.
    Returns list[ArenaZSetBatch or None] of length num_workers."""
    sub_batches = [None] * num_workers
    schema = batch._schema
    for i in range(batch.length()):
        p = hash_row_by_columns(batch, i, shard_col_indices)
        w = assignment.worker_for_partition(p)
        if sub_batches[w] is None:
            sub_batches[w] = ArenaZSetBatch(schema)
        sub_batches[w]._direct_append_row(batch, i, batch.get_weight(i))
    return sub_batches


def relay_scatter(source_batches, shard_cols, num_workers, assignment):
    """Scatter N exchange results. Sources must remain alive during call.
    Caller is responsible for freeing source_batches after this returns."""
    schema = None
    for sb in source_batches:
        if sb is not None and sb.length() > 0:
            schema = sb._schema
            break
    if schema is None:
        return [None] * num_workers
    merged = ArenaZSetBatch(schema)
    for sb in source_batches:
        if sb is not None:
            merged.append_batch(sb)
    dest = repartition_batch(merged, shard_cols, num_workers, assignment)
    merged.free()
    return dest


def multi_scatter(batch, col_spec_list, num_workers, assignment):
    """Scatter one batch by multiple column specs in a single pass.
    Returns list[list[ArenaZSetBatch|None]], one inner list per spec.
    Used by Phase 4's combined push+preload pass."""
    n_specs = len(col_spec_list)
    results = [[None] * num_workers for _ in range(n_specs)]
    schema = batch._schema
    for i in range(batch.length()):
        weight = batch.get_weight(i)
        for si in range(n_specs):
            p = hash_row_by_columns(batch, i, col_spec_list[si])
            w = assignment.worker_for_partition(p)
            if results[si][w] is None:
                results[si][w] = ArenaZSetBatch(schema)
            results[si][w]._direct_append_row(batch, i, weight)
    # Flag propagation: PK-spec sub-batches inherit consolidation from source
    for si in range(n_specs):
        spec = col_spec_list[si]
        if len(spec) == 1 and spec[0] == schema.pk_index:
            if batch._sorted or batch._consolidated:
                for w in range(num_workers):
                    sb = results[si][w]
                    if sb is not None:
                        if batch._sorted:
                            sb.mark_sorted(True)
                        if batch._consolidated:
                            sb.mark_consolidated(True)
    return results
