# gnitz/storage/memtable.py

from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.core import comparator as core_comparator
from gnitz.core.batch import ArenaZSetBatch, ColumnarBatchAccessor, pk_lt, pk_eq
from gnitz.storage import buffer as buffer_mod, engine_ffi, writer_table
from gnitz.storage.buffer import c_memmove
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
        wrote = writer_table.write_batch_to_shard(
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
    """Merge N sorted runs into one owned, consolidated ArenaZSetBatch."""
    if len(runs) == 0:
        return ArenaZSetBatch(schema)

    if len(runs) == 1:
        consolidated = runs[0].to_consolidated()
        if consolidated is runs[0]:
            consolidated = runs[0].clone()
        consolidated.mark_consolidated(True)
        return consolidated

    return _rust_merge_runs(runs, schema)


def _rust_merge_runs(runs, schema):
    """N-way merge via Rust gnitz_merge_to_batch."""
    num_runs = len(runs)
    num_cols = len(schema.columns)
    pk_index = schema.pk_index
    num_payload = num_cols - 1

    desc_buf = lltype.malloc(
        rffi.CCHARP.TO, num_runs * engine_ffi.BATCH_DESC_SIZE, flavor="raw"
    )
    schema_buf = engine_ffi.pack_schema(schema)
    out_buf = lltype.malloc(
        rffi.CCHARP.TO, engine_ffi.MERGED_BATCH_SIZE, flavor="raw"
    )
    # Zero the output struct
    i = 0
    while i < engine_ffi.MERGED_BATCH_SIZE:
        out_buf[i] = '\x00'
        i += 1

    # Per-run col_ptrs/col_sizes arrays (needed for lifetime — must outlive the FFI call)
    all_col_ptrs = newlist_hint(num_runs)
    all_col_sizes = newlist_hint(num_runs)

    try:
        for ri in range(num_runs):
            run = runs[ri]
            base = rffi.ptradd(desc_buf, ri * engine_ffi.BATCH_DESC_SIZE)
            _pack_batch_desc(base, run, schema, pk_index, num_payload,
                             all_col_ptrs, all_col_sizes)

        rc = engine_ffi._merge_to_batch(
            rffi.cast(rffi.VOIDP, desc_buf),
            rffi.cast(rffi.UINT, num_runs),
            rffi.cast(rffi.VOIDP, schema_buf),
            rffi.cast(rffi.VOIDP, out_buf),
        )
        rc_int = intmask(rc)
        if rc_int < 0:
            raise errors.StorageError("merge_to_batch failed (%d)" % rc_int)

        result = _unpack_merged_batch(out_buf, schema, pk_index, num_payload)
    finally:
        engine_ffi._merged_batch_free(rffi.cast(rffi.VOIDP, out_buf))
        _free_col_arrays(all_col_ptrs, all_col_sizes)
        lltype.free(out_buf, flavor="raw")
        lltype.free(schema_buf, flavor="raw")
        lltype.free(desc_buf, flavor="raw")

    return result


def _pack_batch_desc(base, run, schema, pk_index, num_payload,
                     all_col_ptrs, all_col_sizes):
    """Write a GnitzBatchDesc struct at `base` for one ArenaZSetBatch."""
    # Allocate col_ptrs and col_sizes C arrays for this run
    col_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
    col_sizes = lltype.malloc(rffi.ULONGLONGP.TO, num_payload, flavor="raw")
    all_col_ptrs.append(col_ptrs)
    all_col_sizes.append(col_sizes)

    pi = 0
    num_cols = len(schema.columns)
    ci = 0
    while ci < num_cols:
        if ci != pk_index:
            col_ptrs[pi] = rffi.cast(rffi.VOIDP, run.col_bufs[ci].base_ptr)
            col_sizes[pi] = rffi.cast(rffi.ULONGLONG, run.col_bufs[ci].offset)
            pi += 1
        ci += 1

    # Write struct fields at known offsets (must match Rust GnitzBatchDesc #[repr(C)])
    off = 0
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, run.pk_lo_buf.base_ptr)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, run.pk_hi_buf.base_ptr)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, run.weight_buf.base_ptr)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, run.null_buf.base_ptr)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, col_ptrs)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, col_sizes)
    off += 8
    rffi.cast(rffi.UINTP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.UINT, num_payload)
    off += 4
    off += 4  # padding
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, run.blob_arena.base_ptr)
    off += 8
    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.ULONGLONG, run.blob_arena.offset)
    off += 8
    rffi.cast(rffi.UINTP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.UINT, run.length())


def _free_col_arrays(all_col_ptrs, all_col_sizes):
    for p in all_col_ptrs:
        lltype.free(p, flavor="raw")
    for s in all_col_sizes:
        lltype.free(s, flavor="raw")


def _read_merged_ptr(out_buf, off):
    p = rffi.cast(rffi.VOIDPP, rffi.ptradd(out_buf, off))[0]
    ln = intmask(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(out_buf, off + 8))[0])
    return rffi.cast(rffi.CCHARP, p), ln


def _unpack_merged_batch(out_buf, schema, pk_index, num_payload):
    """Read a GnitzMergedBatch struct and build an ArenaZSetBatch from copies."""
    count = intmask(rffi.cast(rffi.UINTP, out_buf)[0])
    off = 8  # skip count(4) + padding(4)

    pk_lo_ptr, pk_lo_len = _read_merged_ptr(out_buf, off); off += 16
    pk_hi_ptr, pk_hi_len = _read_merged_ptr(out_buf, off); off += 16
    w_ptr, w_len = _read_merged_ptr(out_buf, off); off += 16
    n_ptr, n_len = _read_merged_ptr(out_buf, off); off += 16

    off += 8  # num_cols(4) + pad(4)

    col_ptrs_off = off
    col_lens_off = off + 64 * 8
    off = col_lens_off + 64 * 8

    blob_ptr, blob_len = _read_merged_ptr(out_buf, off)

    # Copy into RPython-owned Buffers
    pk_lo_b = _copy_to_buffer(pk_lo_ptr, pk_lo_len)
    pk_hi_b = _copy_to_buffer(pk_hi_ptr, pk_hi_len)
    w_b = _copy_to_buffer(w_ptr, w_len)
    n_b = _copy_to_buffer(n_ptr, n_len)

    num_cols = len(schema.columns)
    col_bufs = newlist_hint(num_cols)
    col_strides = newlist_hint(num_cols)
    pi = 0
    ci = 0
    while ci < num_cols:
        if ci == pk_index:
            col_bufs.append(buffer_mod.Buffer(0))
            col_strides.append(0)
        else:
            sz = schema.columns[ci].field_type.size
            c_ptr = rffi.cast(rffi.CCHARP, rffi.cast(
                rffi.VOIDPP, rffi.ptradd(out_buf, col_ptrs_off + pi * 8))[0])
            c_len = intmask(rffi.cast(
                rffi.ULONGLONGP, rffi.ptradd(out_buf, col_lens_off + pi * 8))[0])
            col_bufs.append(_copy_to_buffer(c_ptr, c_len))
            col_strides.append(sz)
            pi += 1
        ci += 1

    blob_b = _copy_to_buffer(blob_ptr, blob_len)

    result = ArenaZSetBatch.from_buffers(
        schema, pk_lo_b, pk_hi_b, w_b, n_b,
        col_bufs, col_strides, blob_b, count, is_sorted=True,
    )
    result._consolidated = True
    return result


def _copy_to_buffer(src_ptr, size):
    """Copy Rust-allocated data into a new RPython-owned Buffer."""
    buf = buffer_mod.Buffer(max(size, 1))
    if size > 0 and src_ptr:
        c_memmove(
            rffi.cast(rffi.VOIDP, buf.base_ptr),
            rffi.cast(rffi.VOIDP, src_ptr),
            rffi.cast(rffi.SIZE_T, size),
        )
    buf.offset = size
    return buf


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
