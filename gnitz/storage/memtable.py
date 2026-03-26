# gnitz/storage/memtable.py

from rpython.rlib.rarithmetic import r_int64, r_uint64
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.core import comparator as core_comparator
from gnitz.core.batch import ArenaZSetBatch, ColumnarBatchAccessor, pk_lt, pk_eq
from gnitz.storage import shard_writer
from gnitz.storage.bloom import BloomFilter

ACCUMULATOR_THRESHOLD = 64


def _batch_bytes(batch):
    """Sum all buffer offsets for a single ArenaZSetBatch."""
    total = (batch.pk_lo_buf.offset + batch.pk_hi_buf.offset +
             batch.weight_buf.offset + batch.null_buf.offset +
             batch.blob_arena.offset)
    col_bufs = batch.col_bufs
    for i in range(len(col_bufs)):
        total += col_bufs[i].offset
    return total


class MemTable(object):
    """
    Mutable, in-memory Z-Set storage.
    Maintains a list of sorted runs plus a small unsorted accumulator.
    """

    _immutable_fields_ = ["schema", "max_bytes"]

    def __init__(self, schema, arena_size):
        self.schema = schema
        initial_capacity = arena_size // (schema.memtable_stride + 32)
        if initial_capacity < 16:
            initial_capacity = 16
        self.runs = newlist_hint(8)               # list of sorted ArenaZSetBatch
        self._accumulator = ArenaZSetBatch(schema, initial_capacity=initial_capacity)
        self._total_row_count = 0
        self.max_bytes = arena_size
        self.bloom = BloomFilter(initial_capacity)
        self._runs_bytes = 0
        self._cached_consolidated = None

    def _invalidate_runs_cache(self):
        if self._cached_consolidated is not None:
            self._cached_consolidated.release()
            self._cached_consolidated = None

    def get_consolidated_snapshot(self):
        """Return a refcounted snapshot of runs + accumulator.

        Caches the N-way merge of sorted runs.  If accumulator is non-empty,
        does a cheap 2-way merge of [cached_runs, sorted_acc].
        Caller must call release() when done (not free()).
        """
        if self._cached_consolidated is None and len(self.runs) > 0:
            self._cached_consolidated = _merge_runs_to_consolidated(
                self.runs, self.schema
            )

        has_acc = self._accumulator.length() > 0

        if self._cached_consolidated is None:
            if has_acc:
                sorted_acc = self._accumulator.to_sorted()
                result = sorted_acc.to_consolidated()
                if result is sorted_acc:
                    result = sorted_acc.clone()
                if sorted_acc is not self._accumulator:
                    sorted_acc.free()
                return result
            return ArenaZSetBatch(self.schema)

        if not has_acc:
            return self._cached_consolidated.acquire()

        sorted_acc = self._accumulator.to_sorted()
        two = [self._cached_consolidated, sorted_acc]
        result = _merge_runs_to_consolidated(two, self.schema)
        if sorted_acc is not self._accumulator:
            sorted_acc.free()
        return result

    def upsert_batch(self, batch):
        self._check_capacity()
        if batch.length() == 0:
            return
        sorted_batch = batch.to_sorted()
        # to_sorted() returns self when already sorted; we need an owned copy
        # because the caller may free the original batch after this call.
        if sorted_batch is batch:
            sorted_batch = batch.clone()
        self.runs.append(sorted_batch)
        self._runs_bytes += _batch_bytes(sorted_batch)
        self._invalidate_runs_cache()
        self._total_row_count += sorted_batch.length()
        num_records = batch.length()
        for i in range(num_records):
            self.bloom.add(batch.get_pk_lo(i), batch.get_pk_hi(i))

    def upsert_single(self, key_lo, key_hi, weight, accessor):
        self._check_capacity()
        self._accumulator.append_from_accessor(key_lo, key_hi, weight, accessor)
        self._total_row_count += 1
        self.bloom.add(key_lo, key_hi)
        if self._accumulator.length() >= ACCUMULATOR_THRESHOLD:
            self._flush_accumulator()

    def _flush_accumulator(self):
        if self._accumulator.length() == 0:
            return
        sorted_acc = self._accumulator.to_sorted()
        if sorted_acc is self._accumulator:
            # to_sorted returned self (<=1 row or already sorted)
            # clone it so we can reset the accumulator
            sorted_acc = self._accumulator.clone()
            self._accumulator.free()
        else:
            # to_sorted created a new batch; free the old accumulator
            self._accumulator.free()
        self._accumulator = ArenaZSetBatch(self.schema)
        self.runs.append(sorted_acc)
        self._runs_bytes += _batch_bytes(sorted_acc)
        self._invalidate_runs_cache()

    def _total_bytes(self):
        return self._runs_bytes + _batch_bytes(self._accumulator)

    def _check_capacity(self):
        if self._total_bytes() > self.max_bytes:
            raise errors.MemTableFullError()

    def should_flush(self):
        return self._total_bytes() > self.max_bytes * 3 // 4

    def lookup_pk(self, key_lo, key_hi):
        """Find a PK across all runs and the accumulator.

        Returns (net_weight, batch_or_None, row_idx) where batch+row_idx
        point to the first row found with this PK.
        """
        total_w = r_int64(0)
        found_batch = None
        found_idx = -1
        for ri in range(len(self.runs)):
            run = self.runs[ri]
            lo = _lower_bound(run, key_lo, key_hi)
            while lo < run.length() and pk_eq(run.get_pk_lo(lo), run.get_pk_hi(lo), key_lo, key_hi):
                total_w += run.get_weight(lo)
                if found_batch is None:
                    found_batch = run
                    found_idx = lo
                lo += 1
        n = self._accumulator.length()
        for i in range(n):
            if pk_eq(self._accumulator.get_pk_lo(i), self._accumulator.get_pk_hi(i), key_lo, key_hi):
                total_w += self._accumulator.get_weight(i)
                if found_batch is None:
                    found_batch = self._accumulator
                    found_idx = i
        return total_w, found_batch, found_idx

    def find_weight_for_row(self, key_lo, key_hi, accessor):
        total_w = r_int64(0)
        batch_acc = ColumnarBatchAccessor(self.schema)
        # Binary search each sorted run
        for ri in range(len(self.runs)):
            run = self.runs[ri]
            lo = _lower_bound(run, key_lo, key_hi)
            while lo < run.length() and pk_eq(run.get_pk_lo(lo), run.get_pk_hi(lo), key_lo, key_hi):
                batch_acc.bind(run, lo)
                if core_comparator.compare_rows(self.schema, batch_acc, accessor) == 0:
                    total_w += run.get_weight(lo)
                lo += 1
        # Linear scan accumulator
        n = self._accumulator.length()
        for i in range(n):
            if pk_eq(self._accumulator.get_pk_lo(i), self._accumulator.get_pk_hi(i), key_lo, key_hi):
                batch_acc.bind(self._accumulator, i)
                if core_comparator.compare_rows(self.schema, batch_acc, accessor) == 0:
                    total_w += self._accumulator.get_weight(i)
        return total_w

    def may_contain_pk(self, key_lo, key_hi):
        return self.bloom.may_contain(key_lo, key_hi)

    def flush(self, dirfd, basename, table_id=0, durable=True):
        self._flush_accumulator()
        consolidated = self.get_consolidated_snapshot()
        wrote = shard_writer.write_batch_to_shard(
            consolidated, dirfd, basename, table_id, durable=durable
        )
        consolidated.release()
        return wrote

    def is_empty(self):
        return self._total_row_count == 0

    def reset(self):
        """Reset for reuse after flush. Frees run data, keeps accumulator and bloom buffers."""
        for r in self.runs:
            r.free()
        self.runs = newlist_hint(8)
        self._runs_bytes = 0
        self._invalidate_runs_cache()
        self._total_row_count = 0
        self.bloom.reset()

    def free(self):
        for r in self.runs:
            r.free()
        self.runs = newlist_hint(8)
        self._runs_bytes = 0
        self._invalidate_runs_cache()
        self._accumulator.free()
        self._total_row_count = 0
        self.bloom.free()


def _merge_runs_to_consolidated(runs, schema):
    """Merge N sorted runs into one owned, consolidated ArenaZSetBatch via Rust FFI."""
    from rpython.rlib.rarithmetic import intmask
    from gnitz.storage import buffer as buffer_ops, engine_ffi

    num_batches = len(runs)
    if num_batches == 0:
        return ArenaZSetBatch(schema)

    if num_batches == 1:
        consolidated = runs[0].to_consolidated()
        if consolidated is runs[0]:
            consolidated = runs[0].clone()
        consolidated.mark_consolidated(True)
        return consolidated

    num_cols = len(schema.columns)
    pk_index = schema.pk_index
    num_payload_cols = num_cols - 1
    regions_per_batch = 4 + num_payload_cols + 1

    total_rows = 0
    total_blob = 0
    for ri in range(num_batches):
        total_rows += runs[ri].length()
        total_blob += runs[ri].blob_arena.offset

    if total_rows == 0:
        return ArenaZSetBatch(schema)

    total_regions = num_batches * regions_per_batch

    in_ptrs = lltype.malloc(rffi.VOIDPP.TO, total_regions, flavor="raw")
    in_sizes = lltype.malloc(rffi.UINTP.TO, total_regions, flavor="raw")
    in_counts = lltype.malloc(rffi.UINTP.TO, num_batches, flavor="raw")

    idx = 0
    for bi in range(num_batches):
        batch = runs[bi]
        count = batch.length()
        in_counts[bi] = rffi.cast(rffi.UINT, count)
        # pk_lo
        in_ptrs[idx] = rffi.cast(rffi.VOIDP, batch.pk_lo_buf.base_ptr)
        in_sizes[idx] = rffi.cast(rffi.UINT, count * 8)
        idx += 1
        # pk_hi
        in_ptrs[idx] = rffi.cast(rffi.VOIDP, batch.pk_hi_buf.base_ptr)
        in_sizes[idx] = rffi.cast(rffi.UINT, count * 8)
        idx += 1
        # weight
        in_ptrs[idx] = rffi.cast(rffi.VOIDP, batch.weight_buf.base_ptr)
        in_sizes[idx] = rffi.cast(rffi.UINT, count * 8)
        idx += 1
        # null
        in_ptrs[idx] = rffi.cast(rffi.VOIDP, batch.null_buf.base_ptr)
        in_sizes[idx] = rffi.cast(rffi.UINT, count * 8)
        idx += 1
        # payload columns (skip pk_index)
        for ci in range(num_cols):
            if ci == pk_index:
                continue
            col_sz = count * batch.col_strides[ci]
            in_ptrs[idx] = rffi.cast(rffi.VOIDP, batch.col_bufs[ci].base_ptr)
            in_sizes[idx] = rffi.cast(rffi.UINT, col_sz)
            idx += 1
        # blob arena
        in_ptrs[idx] = rffi.cast(rffi.VOIDP, batch.blob_arena.base_ptr)
        in_sizes[idx] = rffi.cast(rffi.UINT, batch.blob_arena.offset)
        idx += 1

    # Pre-allocate output buffers (upper bound)
    out_pk_lo = buffer_ops.Buffer(total_rows * 8)
    out_pk_hi = buffer_ops.Buffer(total_rows * 8)
    out_weight = buffer_ops.Buffer(total_rows * 8)
    out_null = buffer_ops.Buffer(total_rows * 8)

    out_cols = newlist_hint(num_payload_cols)
    for ci in range(num_cols):
        if ci == pk_index:
            continue
        stride = schema.columns[ci].field_type.size
        out_cols.append(buffer_ops.Buffer(total_rows * stride))

    blob_cap = total_blob if total_blob > 0 else 1
    out_blob = buffer_ops.Buffer(blob_cap)

    # Pack output region pointers
    out_ptrs = lltype.malloc(rffi.VOIDPP.TO, regions_per_batch, flavor="raw")
    out_sizes_arr = lltype.malloc(rffi.UINTP.TO, regions_per_batch, flavor="raw")
    out_count_arr = lltype.malloc(rffi.UINTP.TO, 1, flavor="raw")

    ori = 0
    out_ptrs[ori] = rffi.cast(rffi.VOIDP, out_pk_lo.base_ptr)
    ori += 1
    out_ptrs[ori] = rffi.cast(rffi.VOIDP, out_pk_hi.base_ptr)
    ori += 1
    out_ptrs[ori] = rffi.cast(rffi.VOIDP, out_weight.base_ptr)
    ori += 1
    out_ptrs[ori] = rffi.cast(rffi.VOIDP, out_null.base_ptr)
    ori += 1
    for col_buf in out_cols:
        out_ptrs[ori] = rffi.cast(rffi.VOIDP, col_buf.base_ptr)
        ori += 1
    out_ptrs[ori] = rffi.cast(rffi.VOIDP, out_blob.base_ptr)

    schema_buf = engine_ffi.pack_schema(schema)

    try:
        rc = engine_ffi._merge_batches(
            in_ptrs,
            in_sizes,
            in_counts,
            rffi.cast(rffi.UINT, num_batches),
            rffi.cast(rffi.UINT, regions_per_batch),
            rffi.cast(rffi.VOIDP, schema_buf),
            out_ptrs,
            out_sizes_arr,
            out_count_arr,
        )
        rc_int = intmask(rc)
        if rc_int < 0:
            raise errors.StorageError(
                "merge_batches failed (error %d)" % rc_int
            )

        result_count = intmask(out_count_arr[0])

        # Update buffer offsets to reflect actual data written
        out_pk_lo.offset = intmask(out_sizes_arr[0])
        out_pk_hi.offset = intmask(out_sizes_arr[1])
        out_weight.offset = intmask(out_sizes_arr[2])
        out_null.offset = intmask(out_sizes_arr[3])
        oci = 4
        for col_buf in out_cols:
            col_buf.offset = intmask(out_sizes_arr[oci])
            oci += 1
        out_blob.offset = intmask(out_sizes_arr[oci])

        # Build result batch: reconstruct col_bufs with PK slot
        col_bufs_full = newlist_hint(num_cols)
        col_strides_full = newlist_hint(num_cols)
        payload_idx = 0
        for ci in range(num_cols):
            if ci == pk_index:
                col_bufs_full.append(buffer_ops.Buffer(0))
                col_strides_full.append(0)
            else:
                col_bufs_full.append(out_cols[payload_idx])
                col_strides_full.append(schema.columns[ci].field_type.size)
                payload_idx += 1

        result = ArenaZSetBatch.from_buffers(
            schema,
            out_pk_lo,
            out_pk_hi,
            out_weight,
            out_null,
            col_bufs_full,
            col_strides_full,
            out_blob,
            result_count,
            is_sorted=True,
        )
        result.mark_consolidated(True)
        return result
    finally:
        lltype.free(in_ptrs, flavor="raw")
        lltype.free(in_sizes, flavor="raw")
        lltype.free(in_counts, flavor="raw")
        lltype.free(out_ptrs, flavor="raw")
        lltype.free(out_sizes_arr, flavor="raw")
        lltype.free(out_count_arr, flavor="raw")
        lltype.free(schema_buf, flavor="raw")


def _lower_bound(run, key_lo, key_hi):
    """Binary search for the first index where pk >= key."""
    lo = 0
    hi = run.length()
    while lo < hi:
        mid = (lo + hi) >> 1
        if pk_lt(run.get_pk_lo(mid), run.get_pk_hi(mid), key_lo, key_hi):
            lo = mid + 1
        else:
            hi = mid
    return lo


def _binary_search_weight(run, key_lo, key_hi):
    """Binary search a sorted run for all rows with the given PK, sum weights."""
    total_w = r_int64(0)
    lo = _lower_bound(run, key_lo, key_hi)
    while lo < run.length() and pk_eq(run.get_pk_lo(lo), run.get_pk_hi(lo), key_lo, key_hi):
        total_w += run.get_weight(lo)
        lo += 1
    return total_w
