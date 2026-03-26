# gnitz/storage/memtable.py
#
# Thin RPython wrapper over the Rust MemTable opaque handle.
# Manages a small RPython-side accumulator for single-row appends;
# all bulk operations (runs, sort, merge, bloom, cache, flush) are in Rust.

from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.core.batch import ArenaZSetBatch
from gnitz.storage import engine_ffi

ACCUMULATOR_THRESHOLD = 64

ERR_CAPACITY = -2


def _pack_batch_regions(batch, schema):
    """Pack an ArenaZSetBatch into FFI region arrays.

    Returns (ptrs, sizes, count, rpb) where ptrs and sizes are malloc'd
    arrays that MUST be freed by the caller.
    """
    num_cols = len(schema.columns)
    pk_index = schema.pk_index
    num_payload_cols = num_cols - 1
    regions_per_batch = 4 + num_payload_cols + 1
    count = batch.length()

    ptrs = lltype.malloc(rffi.VOIDPP.TO, regions_per_batch, flavor="raw")
    sizes = lltype.malloc(rffi.UINTP.TO, regions_per_batch, flavor="raw")

    idx = 0
    ptrs[idx] = rffi.cast(rffi.VOIDP, batch.pk_lo_buf.base_ptr)
    sizes[idx] = rffi.cast(rffi.UINT, count * 8)
    idx += 1
    ptrs[idx] = rffi.cast(rffi.VOIDP, batch.pk_hi_buf.base_ptr)
    sizes[idx] = rffi.cast(rffi.UINT, count * 8)
    idx += 1
    ptrs[idx] = rffi.cast(rffi.VOIDP, batch.weight_buf.base_ptr)
    sizes[idx] = rffi.cast(rffi.UINT, count * 8)
    idx += 1
    ptrs[idx] = rffi.cast(rffi.VOIDP, batch.null_buf.base_ptr)
    sizes[idx] = rffi.cast(rffi.UINT, count * 8)
    idx += 1
    for ci in range(num_cols):
        if ci == pk_index:
            continue
        col_sz = count * batch.col_strides[ci]
        ptrs[idx] = rffi.cast(rffi.VOIDP, batch.col_bufs[ci].base_ptr)
        sizes[idx] = rffi.cast(rffi.UINT, col_sz)
        idx += 1
    ptrs[idx] = rffi.cast(rffi.VOIDP, batch.blob_arena.base_ptr)
    sizes[idx] = rffi.cast(rffi.UINT, batch.blob_arena.offset)

    return ptrs, sizes, count, regions_per_batch


class MemTable(object):
    """Thin RPython wrapper over Rust MemTable opaque handle.

    The Rust handle owns sorted runs, the Bloom filter, the consolidation
    cache, and the capacity tracking.  The RPython side keeps a small
    accumulator for single-row appends (upsert_single) which is flushed
    to Rust before any read operation.
    """

    _immutable_fields_ = ["schema", "max_bytes"]

    def __init__(self, schema, arena_size):
        self.schema = schema
        self.max_bytes = arena_size
        initial_capacity = arena_size // (schema.memtable_stride + 32)
        if initial_capacity < 16:
            initial_capacity = 16

        schema_buf = engine_ffi.pack_schema(schema)
        try:
            self._handle = engine_ffi._memtable_create(
                rffi.cast(rffi.VOIDP, schema_buf),
                rffi.cast(rffi.ULONGLONG, arena_size),
            )
        finally:
            lltype.free(schema_buf, flavor="raw")

        self._accumulator = ArenaZSetBatch(schema, initial_capacity=initial_capacity)
        self._total_row_count = 0
        self._lookup_found_buf = lltype.malloc(rffi.INTP.TO, 1, flavor="raw")

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def upsert_batch(self, batch):
        if batch.length() == 0:
            return
        sorted_batch = batch.to_sorted()
        ptrs, sizes, count, rpb = _pack_batch_regions(sorted_batch, self.schema)
        try:
            rc = intmask(engine_ffi._memtable_upsert_batch(
                self._handle, ptrs, sizes,
                rffi.cast(rffi.UINT, count),
                rffi.cast(rffi.UINT, rpb),
            ))
        finally:
            lltype.free(ptrs, flavor="raw")
            lltype.free(sizes, flavor="raw")
            if sorted_batch is not batch:
                sorted_batch.free()
        if rc == ERR_CAPACITY:
            raise errors.MemTableFullError()
        self._total_row_count += batch.length()

    def upsert_single(self, key_lo, key_hi, weight, accessor):
        self._accumulator.append_from_accessor(key_lo, key_hi, weight, accessor)
        self._total_row_count += 1
        engine_ffi._memtable_bloom_add(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )
        if self._accumulator.length() >= ACCUMULATOR_THRESHOLD:
            self._flush_accumulator()

    def _flush_accumulator(self):
        if self._accumulator.length() == 0:
            return
        sorted_acc = self._accumulator.to_sorted()
        ptrs, sizes, count, rpb = _pack_batch_regions(sorted_acc, self.schema)
        try:
            rc = intmask(engine_ffi._memtable_upsert_batch(
                self._handle, ptrs, sizes,
                rffi.cast(rffi.UINT, count),
                rffi.cast(rffi.UINT, rpb),
            ))
        finally:
            lltype.free(ptrs, flavor="raw")
            lltype.free(sizes, flavor="raw")
        if sorted_acc is not self._accumulator:
            sorted_acc.free()
        self._accumulator.free()
        self._accumulator = ArenaZSetBatch(self.schema)
        if rc == ERR_CAPACITY:
            raise errors.MemTableFullError()

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def get_consolidated_snapshot(self):
        """Return an opaque Rust snapshot handle (void*).

        Caller must pass this to RustUnifiedCursor.from_snapshots() and
        eventually free it via engine_ffi._memtable_snapshot_free().
        """
        self._flush_accumulator()
        return engine_ffi._memtable_get_snapshot(self._handle)

    def lookup_pk(self, key_lo, key_hi):
        """Find a PK across all runs.

        Returns (net_weight, found_flag).  If found_flag is True, the
        found row's data is accessible via the found_* accessor methods
        on this MemTable (valid until the next mutation).
        """
        self._flush_accumulator()
        weight = engine_ffi._memtable_lookup_pk(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
            self._lookup_found_buf,
        )
        found = intmask(self._lookup_found_buf[0]) != 0
        return r_int64(weight), found

    def find_weight_for_row(self, key_lo, key_hi, accessor):
        """Find the net weight for rows matching PK + full payload.

        Packs the accessor's row into a 1-row scratch batch and passes
        its regions to Rust for comparison.
        """
        self._flush_accumulator()
        ref_batch = ArenaZSetBatch(self.schema, initial_capacity=1)
        ref_batch.append_from_accessor(key_lo, key_hi, r_int64(1), accessor)
        ptrs, sizes, count, rpb = _pack_batch_regions(ref_batch, self.schema)
        try:
            w = engine_ffi._memtable_find_weight_for_row(
                self._handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                ptrs, sizes,
                rffi.cast(rffi.UINT, count),
                rffi.cast(rffi.UINT, rpb),
            )
        finally:
            lltype.free(ptrs, flavor="raw")
            lltype.free(sizes, flavor="raw")
            ref_batch.free()
        return r_int64(w)

    def may_contain_pk(self, key_lo, key_hi):
        return intmask(engine_ffi._memtable_may_contain_pk(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )) != 0

    def should_flush(self):
        return intmask(engine_ffi._memtable_should_flush(self._handle)) != 0

    def is_empty(self):
        return (
            self._accumulator.length() == 0
            and intmask(engine_ffi._memtable_is_empty(self._handle)) != 0
        )

    # ------------------------------------------------------------------
    # Flush / lifecycle
    # ------------------------------------------------------------------

    def flush(self, dirfd, basename, table_id=0, durable=True):
        self._flush_accumulator()
        with rffi.scoped_str2charp(basename) as name_c:
            rc = intmask(engine_ffi._memtable_flush(
                self._handle,
                rffi.cast(rffi.INT, dirfd),
                name_c,
                rffi.cast(rffi.UINT, table_id),
                rffi.cast(rffi.INT, 1 if durable else 0),
            ))
        return rc >= 0

    def reset(self):
        engine_ffi._memtable_reset(self._handle)
        self._total_row_count = 0

    def free(self):
        engine_ffi._memtable_close(self._handle)
        self._accumulator.free()
        lltype.free(self._lookup_found_buf, flavor="raw")
