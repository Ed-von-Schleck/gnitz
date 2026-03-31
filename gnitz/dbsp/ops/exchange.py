# gnitz/dbsp/ops/exchange.py
#
# Column-based hash for repartitioning batches across workers.
# Single authority for all partition routing logic.

from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core.batch import ArenaZSetBatch, ColumnarBatchAccessor
from gnitz.storage.partitioned_table import _partition_for_key


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


def _unpack_handles(out_arr, num_workers, schema):
    """Build list[ArenaZSetBatch|None] from a filled out_handles array."""
    from gnitz.storage import engine_ffi
    result = [None] * num_workers
    for w in range(num_workers):
        h = out_arr[w]
        if h:
            is_s = intmask(engine_ffi._batch_is_sorted(h)) != 0
            is_c = intmask(engine_ffi._batch_is_consolidated(h)) != 0
            result[w] = ArenaZSetBatch._wrap_handle(schema, h, is_s, is_c)
    return result


def repartition_batch(batch, shard_col_indices, num_workers, assignment):
    """Split a batch into N sub-batches by hashing shard columns.
    Returns list[ArenaZSetBatch or None] of length num_workers."""
    from gnitz.storage import engine_ffi
    schema = batch._schema
    n = len(shard_col_indices)
    col_arr = lltype.malloc(rffi.UINTP.TO, max(n, 1), flavor="raw")
    out = lltype.malloc(rffi.VOIDPP.TO, max(num_workers, 1), flavor="raw")
    schema_buf = engine_ffi.pack_schema(schema)
    for w in range(num_workers):
        out[w] = lltype.nullptr(rffi.VOIDP.TO)
    try:
        for i in range(n):
            col_arr[i] = rffi.cast(rffi.UINT, shard_col_indices[i])
        rc = engine_ffi._repartition_batch(
            batch._handle,
            col_arr, rffi.cast(rffi.UINT, n),
            rffi.cast(rffi.VOIDP, schema_buf),
            rffi.cast(rffi.UINT, num_workers),
            out,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_repartition_batch failed: %d" % intmask(rc))
        return _unpack_handles(out, num_workers, schema)
    finally:
        lltype.free(col_arr, flavor="raw")
        lltype.free(out, flavor="raw")
        lltype.free(schema_buf, flavor="raw")


def repartition_batches(source_batches, shard_col_indices, num_workers, assignment):
    """Scatter N source batches into N dest batches. Delegates to relay_scatter."""
    return relay_scatter(source_batches, shard_col_indices, num_workers, assignment)


def repartition_batches_merged(source_batches, shard_col_indices, num_workers, assignment):
    """K-way merge of N consolidated source batches in merged PK order.
    Delegates to relay_scatter (which selects merged path when all sources
    are consolidated)."""
    return relay_scatter(source_batches, shard_col_indices, num_workers, assignment)


def relay_scatter(source_batches, shard_cols, num_workers, assignment):
    """Scatter N exchange results. Sources must remain alive during call."""
    schema = None
    for sb in source_batches:
        if sb is not None and sb.length() > 0:
            schema = sb._schema
            break
    if schema is None:
        return [None] * num_workers
    from gnitz.storage import engine_ffi
    n_src = len(source_batches)
    n_cols = len(shard_cols)
    src_ptrs = lltype.malloc(rffi.VOIDPP.TO, max(n_src, 1), flavor="raw")
    col_arr = lltype.malloc(rffi.UINTP.TO, max(n_cols, 1), flavor="raw")
    out = lltype.malloc(rffi.VOIDPP.TO, max(num_workers, 1), flavor="raw")
    schema_buf = engine_ffi.pack_schema(schema)
    for w in range(num_workers):
        out[w] = lltype.nullptr(rffi.VOIDP.TO)
    try:
        for i in range(n_src):
            sb = source_batches[i]
            if sb is not None and sb.length() > 0:
                src_ptrs[i] = sb._handle
            else:
                src_ptrs[i] = lltype.nullptr(rffi.VOIDP.TO)
        for i in range(n_cols):
            col_arr[i] = rffi.cast(rffi.UINT, shard_cols[i])
        rc = engine_ffi._relay_scatter(
            src_ptrs, rffi.cast(rffi.UINT, n_src),
            col_arr, rffi.cast(rffi.UINT, n_cols),
            rffi.cast(rffi.VOIDP, schema_buf),
            rffi.cast(rffi.UINT, num_workers),
            out,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_relay_scatter failed: %d" % intmask(rc))
        return _unpack_handles(out, num_workers, schema)
    finally:
        lltype.free(src_ptrs, flavor="raw")
        lltype.free(col_arr, flavor="raw")
        lltype.free(out, flavor="raw")
        lltype.free(schema_buf, flavor="raw")


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
    Returns list[list[ArenaZSetBatch|None]], one inner list per spec."""
    from gnitz.storage import engine_ffi
    schema = batch._schema
    n_specs = len(col_spec_list)
    flat = newlist_hint(0)
    lengths = newlist_hint(n_specs)
    for spec in col_spec_list:
        for ci in spec:
            flat.append(ci)
        lengths.append(len(spec))
    n_flat = len(flat)
    flat_arr = lltype.malloc(rffi.UINTP.TO, max(n_flat, 1), flavor="raw")
    len_arr = lltype.malloc(rffi.UINTP.TO, max(n_specs, 1), flavor="raw")
    total_out = n_specs * num_workers
    out = lltype.malloc(rffi.VOIDPP.TO, max(total_out, 1), flavor="raw")
    schema_buf = engine_ffi.pack_schema(schema)
    for i in range(total_out):
        out[i] = lltype.nullptr(rffi.VOIDP.TO)
    # Flat handle list: save pointers before freeing the C array
    handles = newlist_hint(total_out)
    for i in range(total_out):
        handles.append(lltype.nullptr(rffi.VOIDP.TO))
    try:
        for i in range(n_flat):
            flat_arr[i] = rffi.cast(rffi.UINT, flat[i])
        for i in range(n_specs):
            len_arr[i] = rffi.cast(rffi.UINT, lengths[i])
        rc = engine_ffi._multi_scatter(
            batch._handle,
            flat_arr, len_arr, rffi.cast(rffi.UINT, n_specs),
            rffi.cast(rffi.VOIDP, schema_buf),
            rffi.cast(rffi.UINT, num_workers),
            out,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_multi_scatter failed: %d" % intmask(rc))
        for i in range(total_out):
            handles[i] = out[i]
    finally:
        lltype.free(flat_arr, flavor="raw")
        lltype.free(len_arr, flavor="raw")
        lltype.free(out, flavor="raw")
        lltype.free(schema_buf, flavor="raw")
    # Build nested results outside try/finally to avoid RPython annotation
    # issues with nested list construction in exception-handling blocks
    results = newlist_hint(n_specs)
    for si in range(n_specs):
        inner = newlist_hint(num_workers)
        for w in range(num_workers):
            h = handles[si * num_workers + w]
            if h:
                is_s = intmask(engine_ffi._batch_is_sorted(h)) != 0
                is_c = intmask(engine_ffi._batch_is_consolidated(h)) != 0
                inner.append(ArenaZSetBatch._wrap_handle(schema, h, is_s, is_c))
            else:
                inner.append(None)
        results.append(inner)
    return results
