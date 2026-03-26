# gnitz/storage/shard_writer.py
#
# Shard file writing: batch-level (production) and row-at-a-time (via Rust FFI).

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import errors
from gnitz.storage import engine_ffi


# ---------------------------------------------------------------------------
# Row-at-a-time shard builder (Rust-backed via opaque handle)
# ---------------------------------------------------------------------------


class ShardWriter(object):
    """Accumulate rows one-at-a-time and finalize to a shard file.

    All column accumulation, blob deduplication, and type dispatch happens in
    Rust (shard_writer.rs). RPython extracts column pointers from the accessor
    and passes them flat.
    """

    _immutable_fields_ = ["schema", "table_id"]

    def __init__(self, schema, table_id=0):
        self.schema = schema
        self.table_id = table_id
        self._handle = lltype.nullptr(rffi.VOIDP.TO)

        schema_buf = engine_ffi.pack_schema(schema)
        try:
            h = engine_ffi._shard_writer_new(schema_buf)
        finally:
            lltype.free(schema_buf, flavor="raw")

        if not h:
            raise errors.StorageError("Failed to create shard writer")
        self._handle = h

    def add_row_from_accessor(self, key, weight, accessor):
        if not self._handle:
            raise errors.StorageError("ShardWriter already closed")

        schema = self.schema
        pk_index = schema.pk_index
        num_cols = len(schema.columns)

        # Count non-PK columns
        num_payload = 0
        for ci in range(num_cols):
            if ci != pk_index:
                num_payload += 1

        # Build null word
        null_word = r_uint64(0)
        ci = 0
        while ci < num_cols:
            if ci == pk_index:
                ci += 1
                continue
            if accessor.is_null(ci):
                payload_idx = ci if ci < pk_index else ci - 1
                null_word |= r_uint64(1) << payload_idx
            ci += 1

        # Allocate C arrays for column pointers/sizes
        col_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
        col_sizes = lltype.malloc(rffi.UINTP.TO, num_payload, flavor="raw")
        try:
            pi = 0
            ci = 0
            while ci < num_cols:
                if ci == pk_index:
                    ci += 1
                    continue
                col_ptr = accessor.get_col_ptr(ci)
                col_size = schema.columns[ci].field_type.size
                col_ptrs[pi] = rffi.cast(rffi.VOIDP, col_ptr)
                col_sizes[pi] = rffi.cast(rffi.UINT, col_size)
                pi += 1
                ci += 1

            # Get blob heap pointer from the accessor's bound batch
            blob_base = lltype.nullptr(rffi.CCHARP.TO)
            blob_len = 0
            batch = accessor._batch
            if batch is not None and batch.blob_arena is not None:
                blob_base = batch.blob_arena.base_ptr
                blob_len = batch.blob_arena.offset

            key_lo = r_uint64(intmask(key))
            key_hi = r_uint64(intmask(key >> 64))

            rc = engine_ffi._shard_writer_add_row(
                self._handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                rffi.cast(rffi.LONGLONG, weight),
                rffi.cast(rffi.ULONGLONG, null_word),
                col_ptrs,
                col_sizes,
                rffi.cast(rffi.UINT, num_payload),
                blob_base,
                rffi.cast(rffi.UINT, blob_len),
            )
            rc_int = intmask(rc)
            if rc_int < 0:
                raise errors.StorageError(
                    "shard_writer add_row failed (error %d)" % rc_int
                )
        finally:
            lltype.free(col_ptrs, flavor="raw")
            lltype.free(col_sizes, flavor="raw")

    def finalize(self, filename, durable=True):
        if not self._handle:
            raise errors.StorageError("ShardWriter already closed")
        with rffi.scoped_str2charp(filename) as p:
            rc = engine_ffi._shard_writer_finalize(
                self._handle, p,
                rffi.cast(rffi.UINT, self.table_id),
                rffi.cast(rffi.INT, 1 if durable else 0),
            )
        rc_int = intmask(rc)
        if rc_int < 0:
            raise errors.StorageError(
                "shard_writer finalize failed (error %d)" % rc_int
            )
        self.close()

    def close(self):
        if self._handle:
            engine_ffi._shard_writer_close(self._handle)
            self._handle = lltype.nullptr(rffi.VOIDP.TO)


# For backward compatibility during rename transition
TableShardWriter = ShardWriter


# ---------------------------------------------------------------------------
# Batch-level shard writing (production flush path, already Rust-backed)
# ---------------------------------------------------------------------------


def _write_shard_via_ffi(dirfd, name, table_id, count, region_list,
                         durable=True):
    """Write a shard file via Rust FFI (gnitz_write_shard)."""
    num_regions = len(region_list)
    ptrs_arr = lltype.malloc(rffi.VOIDPP.TO, num_regions, flavor="raw")
    sizes_arr = lltype.malloc(rffi.UINTP.TO, num_regions, flavor="raw")
    try:
        i = 0
        while i < num_regions:
            buf_ptr, sz = region_list[i]
            ptrs_arr[i] = rffi.cast(rffi.VOIDP, buf_ptr)
            sizes_arr[i] = rffi.cast(rffi.UINT, sz)
            i += 1
        with rffi.scoped_str2charp(name) as c_name:
            rc = engine_ffi._write_shard(
                rffi.cast(rffi.INT, dirfd),
                c_name,
                rffi.cast(rffi.UINT, table_id),
                rffi.cast(rffi.UINT, count),
                ptrs_arr,
                sizes_arr,
                rffi.cast(rffi.UINT, num_regions),
                rffi.cast(rffi.INT, 1 if durable else 0),
            )
        rc_int = intmask(rc)
        if rc_int < 0:
            raise errors.StorageError(
                "write_shard failed (error %d)" % rc_int
            )
    finally:
        lltype.free(ptrs_arr, flavor="raw")
        lltype.free(sizes_arr, flavor="raw")


def write_shard_at(dirfd, basename, table_id, count, region_list, pk_lo_ptr,
                   pk_hi_ptr, durable=True):
    """Write a shard file using openat/renameat via Rust FFI."""
    _write_shard_via_ffi(dirfd, basename, table_id, count, region_list,
                         durable=durable)


def write_batch_to_shard(batch, dirfd, basename, table_id, durable=True):
    """Write a consolidated ArenaZSetBatch directly to a shard file.
    Returns True if a file was written, False if the batch was empty."""
    count = batch.length()
    if count == 0:
        return False

    schema = batch._schema
    num_cols = len(schema.columns)
    num_regions = 3 + 1 + (num_cols - 1) + 1
    regions = newlist_hint(num_regions)
    regions.append((batch.pk_lo_buf.base_ptr, batch.pk_lo_buf.offset))
    regions.append((batch.pk_hi_buf.base_ptr, batch.pk_hi_buf.offset))
    regions.append((batch.weight_buf.base_ptr, batch.weight_buf.offset))
    regions.append((batch.null_buf.base_ptr, batch.null_buf.offset))
    for ci in range(num_cols):
        if ci != schema.pk_index:
            regions.append((batch.col_bufs[ci].base_ptr, batch.col_bufs[ci].offset))
    regions.append((batch.blob_arena.base_ptr, batch.blob_arena.offset))

    write_shard_at(
        dirfd, basename, table_id, count, regions,
        batch.pk_lo_buf.base_ptr, batch.pk_hi_buf.base_ptr,
        durable=durable,
    )
    return True
