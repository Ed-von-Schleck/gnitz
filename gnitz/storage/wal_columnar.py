# gnitz/storage/wal_columnar.py
#
# WAL block encode/decode backed by Rust (libgnitz_engine).

from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.core.batch import ArenaZSetBatch
from gnitz.storage import engine_ffi

# Maximum regions per WAL block (4 system + columns + blob).
MAX_REGIONS = 128


class WALColumnarBlock(object):
    """A decoded columnar WAL block containing an ArenaZSetBatch."""

    _immutable_fields_ = ["lsn", "tid"]

    def __init__(self, lsn, tid, batch, raw_buf):
        self.lsn = lsn
        self.tid = tid
        self.batch = batch
        self._raw_buf = raw_buf

    def free(self):
        if self._raw_buf:
            lltype.free(self._raw_buf, flavor="raw")
            self._raw_buf = lltype.nullptr(rffi.CCHARP.TO)
        self.batch.free()


def gather_batch_regions(schema, batch):
    """Allocate C arrays of region pointers/sizes from batch handle.

    Returns (region_ptrs, region_sizes, num_regions, blob_size).
    Caller MUST free region_ptrs and region_sizes via lltype.free(..., flavor="raw").
    """
    num_regions = intmask(engine_ffi._batch_num_regions(batch._handle))

    region_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_regions, flavor="raw")
    region_sizes = lltype.malloc(rffi.UINTP.TO, num_regions, flavor="raw")

    for i in range(num_regions):
        region_ptrs[i] = rffi.cast(
            rffi.VOIDP,
            engine_ffi._batch_region_ptr(batch._handle, rffi.cast(rffi.UINT, i)),
        )
        region_sizes[i] = rffi.cast(
            rffi.UINT,
            engine_ffi._batch_region_size(batch._handle, rffi.cast(rffi.UINT, i)),
        )

    # Blob is the last region
    blob_size = intmask(region_sizes[num_regions - 1])
    return region_ptrs, region_sizes, num_regions, blob_size


def encode_batch_append(block_buf, schema, lsn, table_id, batch):
    """Append one WAL block to block_buf without resetting it."""
    region_ptrs, region_sizes, num_regions, blob_size = gather_batch_regions(
        schema, batch
    )
    try:
        needed = 48 + num_regions * 8
        ri2 = 0
        while ri2 < num_regions:
            needed = (needed + 7) & ~7
            needed += intmask(region_sizes[ri2])
            ri2 += 1
        block_buf.ensure_capacity(needed)

        new_offset = engine_ffi._wal_encode(
            block_buf.base_ptr,
            rffi.cast(rffi.LONGLONG, block_buf.offset),
            rffi.cast(rffi.LONGLONG, block_buf.capacity * block_buf.item_size),
            rffi.cast(rffi.ULONGLONG, lsn),
            rffi.cast(rffi.UINT, table_id),
            rffi.cast(rffi.UINT, batch.length()),
            region_ptrs,
            region_sizes,
            rffi.cast(rffi.UINT, num_regions),
            rffi.cast(rffi.ULONGLONG, blob_size),
        )
        new_offset_int = intmask(new_offset)
        if new_offset_int < 0:
            raise errors.StorageError("WAL encode failed: buffer too small")
        block_buf.offset = new_offset_int
    finally:
        lltype.free(region_ptrs, flavor="raw")
        lltype.free(region_sizes, flavor="raw")


def encode_batch_to_buffer(block_buf, schema, lsn, table_id, batch):
    """Serializes an ArenaZSetBatch into a WAL block buffer."""
    block_buf.reset()
    encode_batch_append(block_buf, schema, lsn, table_id, batch)


def _parse_wal_block(ptr, total_size, schema):
    """Non-owning parse. ptr must remain valid while the returned batch is in use."""
    num_cols = len(schema.columns)
    num_non_pk = 0
    for i in range(num_cols):
        if i != schema.pk_index:
            num_non_pk += 1
    max_regions = 4 + num_non_pk + 1

    out_lsn = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
    out_tid = lltype.malloc(rffi.UINTP.TO, 1, flavor="raw")
    out_count = lltype.malloc(rffi.UINTP.TO, 1, flavor="raw")
    out_num_regions = lltype.malloc(rffi.UINTP.TO, 1, flavor="raw")
    out_blob_size = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
    out_offsets = lltype.malloc(rffi.UINTP.TO, max_regions, flavor="raw")
    out_sizes = lltype.malloc(rffi.UINTP.TO, max_regions, flavor="raw")

    try:
        rc = engine_ffi._wal_validate_and_parse(
            ptr,
            rffi.cast(rffi.LONGLONG, total_size),
            out_lsn, out_tid, out_count,
            out_num_regions, out_blob_size,
            out_offsets, out_sizes,
            rffi.cast(rffi.UINT, max_regions),
        )
        rc_int = intmask(rc)
        if rc_int == -1:
            raise errors.CorruptShardError("Unsupported WAL version")
        elif rc_int == -2:
            raise errors.CorruptShardError("WAL checksum mismatch")
        elif rc_int < 0:
            raise errors.CorruptShardError("WAL block truncated or corrupt")

        lsn = r_uint64(out_lsn[0])
        tid = intmask(out_tid[0])
        count = intmask(out_count[0])
        num_regions = intmask(out_num_regions[0])

        region_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_regions, flavor="raw")
        region_sizes = lltype.malloc(rffi.UINTP.TO, num_regions, flavor="raw")
        for ri in range(num_regions):
            region_ptrs[ri] = rffi.cast(
                rffi.VOIDP, rffi.ptradd(ptr, intmask(out_offsets[ri]))
            )
            region_sizes[ri] = out_sizes[ri]
        try:
            result_batch = ArenaZSetBatch.from_regions(
                schema, region_ptrs, region_sizes, count, num_regions,
                is_sorted=False,
            )
        finally:
            lltype.free(region_ptrs, flavor="raw")
            lltype.free(region_sizes, flavor="raw")

        return result_batch, lsn, tid
    finally:
        lltype.free(out_lsn, flavor="raw")
        lltype.free(out_tid, flavor="raw")
        lltype.free(out_count, flavor="raw")
        lltype.free(out_num_regions, flavor="raw")
        lltype.free(out_blob_size, flavor="raw")
        lltype.free(out_offsets, flavor="raw")
        lltype.free(out_sizes, flavor="raw")


def decode_batch_from_buffer(buf, total_size, schema):
    result_batch, lsn, tid = _parse_wal_block(buf, total_size, schema)
    return WALColumnarBlock(lsn, tid, result_batch, raw_buf=buf)


def decode_batch_from_ptr(ptr, total_size, schema):
    result_batch, _lsn, _tid = _parse_wal_block(ptr, total_size, schema)
    return result_batch
