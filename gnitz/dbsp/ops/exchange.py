# gnitz/dbsp/ops/exchange.py
#
# Column-based hash for repartitioning batches across workers.
# Single authority for all partition routing logic.

from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
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
    group_key_lo, group_key_hi = _extract_group_key(acc, schema, col_indices)
    return _partition_for_key(group_key_lo, group_key_hi)


def repartition_batch(batch, shard_col_indices, num_workers, assignment):
    """Split a batch into N sub-batches by hashing shard columns.
    Returns list[ArenaZSetBatch or None] of length num_workers.
    Two-pass: classify then bulk-copy to reduce alloc pressure."""
    sub_batches = [None] * num_workers
    schema = batch._schema
    n = batch.length()
    # Pass 1: classify rows into per-worker index lists
    worker_indices = newlist_hint(num_workers)
    for _w in range(num_workers):
        worker_indices.append(newlist_hint(n // num_workers + 1))
    for i in range(n):
        p = hash_row_by_columns(batch, i, shard_col_indices)
        w = assignment.worker_for_partition(p)
        worker_indices[w].append(i)
    # Pass 2: bulk-copy each worker's rows
    for w in range(num_workers):
        wl = worker_indices[w]
        if len(wl) > 0:
            sub_batches[w] = ArenaZSetBatch(schema, initial_capacity=len(wl))
            sub_batches[w]._copy_rows_indexed_src_weights(batch, wl)
    return sub_batches


def repartition_batches(source_batches, shard_col_indices, num_workers, assignment):
    """Scatter N source batches into N dest batches without an intermediate merge.
    Sources are iterated sequentially. Output is NOT marked consolidated.
    Fallback when sources are not all consolidated.
    Two-pass per source batch: classify then bulk-copy."""
    dest = [None] * num_workers
    schema = None
    total = 0
    for sb in source_batches:
        if sb is not None and sb.length() > 0:
            if schema is None:
                schema = sb._schema
            total += sb.length()
    if schema is None:
        return dest
    cap = total // num_workers
    for sb in source_batches:
        if sb is None:
            continue
        sb_len = sb.length()
        if sb_len == 0:
            continue
        # Pass 1: classify this source batch's rows
        worker_indices = newlist_hint(num_workers)
        for _w in range(num_workers):
            worker_indices.append(newlist_hint(sb_len // num_workers + 1))
        for i in range(sb_len):
            p = hash_row_by_columns(sb, i, shard_col_indices)
            w = assignment.worker_for_partition(p)
            worker_indices[w].append(i)
        # Pass 2: bulk-copy
        for w in range(num_workers):
            wl = worker_indices[w]
            if len(wl) > 0:
                if dest[w] is None:
                    dest[w] = ArenaZSetBatch(schema, initial_capacity=cap)
                dest[w]._copy_rows_indexed_src_weights(sb, wl)
    return dest


def repartition_batches_merged(source_batches, shard_col_indices, num_workers,
                                assignment):
    """K-way merge of N consolidated source batches in merged PK order.
    Produces consolidated dest batches. source_batches must all have
    _consolidated=True and have globally unique PKs."""
    dest = [None] * num_workers
    schema = None
    total = 0
    for sb in source_batches:
        if sb is not None and sb.length() > 0:
            if schema is None:
                schema = sb._schema
            total += sb.length()
    if schema is None:
        return dest
    cap = total // num_workers
    n = len(source_batches)
    cursors = [0] * n

    while True:
        # Linear scan: find source with minimum current PK
        min_w = -1
        for w in range(n):
            sb = source_batches[w]
            if sb is None:
                continue
            idx = cursors[w]
            if idx >= sb.length():
                continue
            if min_w == -1:
                min_w = w
            else:
                min_sb = source_batches[min_w]
                min_idx = cursors[min_w]
                cur_hi = sb._read_pk_hi(idx)
                min_hi = min_sb._read_pk_hi(min_idx)
                if cur_hi < min_hi:
                    min_w = w
                elif cur_hi == min_hi:
                    if sb._read_pk_lo(idx) < min_sb._read_pk_lo(min_idx):
                        min_w = w
        if min_w == -1:
            break
        src = source_batches[min_w]
        src_idx = cursors[min_w]
        p = hash_row_by_columns(src, src_idx, shard_col_indices)
        dw = assignment.worker_for_partition(p)
        if dest[dw] is None:
            dest[dw] = ArenaZSetBatch(schema, initial_capacity=cap)
        dest[dw]._direct_append_row(src, src_idx, src.get_weight(src_idx))
        cursors[min_w] += 1

    for w in range(num_workers):
        if dest[w] is not None:
            dest[w].mark_consolidated(True)
    return dest


def relay_scatter(source_batches, shard_cols, num_workers, assignment):
    """Scatter N exchange results. Sources must remain alive during call."""
    all_consolidated = True
    has_any = False
    for sb in source_batches:
        if sb is not None and sb.length() > 0:
            has_any = True
            if not sb._consolidated:
                all_consolidated = False
                break
    if not has_any:
        return [None] * num_workers
    if all_consolidated:
        return repartition_batches_merged(source_batches, shard_cols,
                                          num_workers, assignment)
    return repartition_batches(source_batches, shard_cols, num_workers, assignment)


class PartitionRouter(object):
    """Master-side cache: maps (table_id, col_idx, key_lo, key_hi) -> worker.

    Populated by fan_out_push after repartitioning unique-indexed columns.
    Lets fan_out_seek_by_index unicast instead of broadcast on cache hit.
    """

    def __init__(self, assignment):
        self.assignment = assignment
        self._index_routing = {}  # (table_id, col_idx, key_lo, key_hi) -> worker

    def worker_for_index_key(self, table_id, col_idx, key_lo, key_hi):
        """Returns worker on cache hit, -1 on miss."""
        key = (table_id, col_idx, key_lo, key_hi)
        if key in self._index_routing:
            return self._index_routing[key]
        return -1

    def record_routing(self, batch, table_id, col_idx, worker):
        """Populate or invalidate cache entries from a push sub-batch.

        All rows in batch belong to worker.  Rows with negative weight
        invalidate the entry (delete); positive weight records the mapping.
        """
        acc = ColumnarBatchAccessor(batch._schema)
        for i in range(batch.length()):
            acc.bind(batch, i)
            key_lo = intmask(r_uint64(acc.get_int(col_idx)))
            key = (table_id, col_idx, key_lo, 0)
            if batch.get_weight(i) < 0:
                if key in self._index_routing:
                    del self._index_routing[key]
            else:
                self._index_routing[key] = worker


def multi_scatter(batch, col_spec_list, num_workers, assignment):
    """Scatter one batch by multiple column specs.
    Returns list[list[ArenaZSetBatch|None]], one inner list per spec.
    Two-pass: classify then bulk-copy via _copy_rows_indexed_src_weights."""
    n = batch.length()
    n_specs = len(col_spec_list)
    schema = batch._schema
    # Pass 1: classify rows into flat_indices[si * num_workers + w]
    flat_indices = newlist_hint(n_specs * num_workers)
    for _i in range(n_specs * num_workers):
        flat_indices.append(newlist_hint(n // num_workers + 1))
    for i in range(n):
        for si in range(n_specs):
            p = hash_row_by_columns(batch, i, col_spec_list[si])
            w = assignment.worker_for_partition(p)
            flat_indices[si * num_workers + w].append(i)
    # Pass 2: bulk-copy each (spec, worker) bucket
    results = [None] * n_specs
    for si in range(n_specs):
        results[si] = [None] * num_workers
    for si in range(n_specs):
        for w in range(num_workers):
            wl = flat_indices[si * num_workers + w]
            if len(wl) > 0:
                results[si][w] = ArenaZSetBatch(schema, initial_capacity=len(wl))
                results[si][w]._copy_rows_indexed_src_weights(batch, wl)
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
