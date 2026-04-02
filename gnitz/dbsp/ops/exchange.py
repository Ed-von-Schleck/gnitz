# gnitz/dbsp/ops/exchange.py
#
# Partition routing and batch repartitioning.
# PartitionAssignment and PartitionRouter delegate to Rust.
# Repartition/scatter operators are thin FFI wrappers.

from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage.owned_batch import ArenaZSetBatch
from gnitz.storage.partitioned_table import _partition_for_key


class PartitionAssignment(object):
    """Maps 256 partitions to N workers. Worker w owns [start, end).

    The partition→worker mapping is computed via Rust (same hash as the
    repartition operators).  Only `num_workers` is stored; all queries are
    O(1) arithmetic.
    """

    _immutable_fields_ = ["num_workers"]

    def __init__(self, num_workers):
        self.num_workers = num_workers

    def worker_for_partition(self, partition_idx):
        """Returns the worker ID that owns the given partition."""
        chunk = 256 // self.num_workers
        w = partition_idx // chunk
        if w >= self.num_workers:
            w = self.num_workers - 1
        return w

    def range_for_worker(self, worker_id):
        """Returns (start, end) partition range for the given worker."""
        chunk = 256 // self.num_workers
        start = worker_id * chunk
        if worker_id == self.num_workers - 1:
            end = 256
        else:
            end = (worker_id + 1) * chunk
        return start, end


def worker_for_pk(pk_lo, pk_hi, assignment):
    """Route a single PK to its owning worker (for fan_out_seek)."""
    from gnitz.storage import engine_ffi
    return intmask(engine_ffi._worker_for_pk(
        rffi.cast(rffi.ULONGLONG, pk_lo),
        rffi.cast(rffi.ULONGLONG, pk_hi),
        rffi.cast(rffi.UINT, assignment.num_workers),
    ))


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
    """Master-side routing cache backed by a Rust HashMap.

    Maps (table_id, col_idx, key_lo) → worker.
    Populated by fan_out_push after repartitioning unique-indexed columns.
    Lets fan_out_seek_by_index unicast instead of broadcast on cache hit.
    """

    def __init__(self, assignment):
        from gnitz.storage import engine_ffi
        self.assignment = assignment
        self._handle = engine_ffi._partition_router_create()
        self._closed = False

    def close(self):
        if self._closed:
            return
        from gnitz.storage import engine_ffi
        engine_ffi._partition_router_free(self._handle)
        self._closed = True

    def worker_for_index_key(self, table_id, col_idx, key_lo, key_hi):
        """Returns worker on cache hit, -1 on miss."""
        from gnitz.storage import engine_ffi
        return intmask(engine_ffi._partition_router_worker_for_index_key(
            self._handle,
            rffi.cast(rffi.UINT, table_id),
            rffi.cast(rffi.UINT, col_idx),
            rffi.cast(rffi.ULONGLONG, key_lo),
        ))

    def record_routing(self, batch, table_id, col_idx, worker):
        """Populate or invalidate cache entries from a push sub-batch."""
        from gnitz.storage import engine_ffi
        schema_buf = engine_ffi.pack_schema(batch._schema)
        try:
            engine_ffi._partition_router_record_routing(
                self._handle,
                batch._handle,
                rffi.cast(rffi.VOIDP, schema_buf),
                rffi.cast(rffi.UINT, table_id),
                rffi.cast(rffi.UINT, col_idx),
                rffi.cast(rffi.UINT, worker),
            )
        finally:
            lltype.free(schema_buf, flavor="raw")


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
