# gnitz/storage/wal_columnar.py
#
# WAL block encode/decode backed by Rust (libgnitz_engine).

from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.core.batch import ArenaZSetBatch
from gnitz.storage import buffer as buffer_ops, engine_ffi

# Maximum regions per WAL block (4 system + columns + blob).
# 128 is generous — supports schemas with up to ~120 columns.
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
    """Allocate C arrays of region pointers/sizes from batch SoA buffers.

    Returns (region_ptrs, region_sizes, num_regions, blob_size).
    Caller MUST free region_ptrs and region_sizes via lltype.free(..., flavor="raw").
    """
    count = batch.length()
    num_cols = len(schema.columns)

    num_non_pk = 0
    for i in range(num_cols):
        if i != schema.pk_index:
            num_non_pk += 1
    num_regions = 4 + num_non_pk + 1

    region_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_regions, flavor="raw")
    region_sizes = lltype.malloc(rffi.UINTP.TO, num_regions, flavor="raw")

    ri = 0
    # pk_lo
    region_ptrs[ri] = rffi.cast(rffi.VOIDP, batch.pk_lo_buf.base_ptr)
    region_sizes[ri] = rffi.cast(rffi.UINT, count * 8)
    ri += 1
    # pk_hi
    region_ptrs[ri] = rffi.cast(rffi.VOIDP, batch.pk_hi_buf.base_ptr)
    region_sizes[ri] = rffi.cast(rffi.UINT, count * 8)
    ri += 1
    # weight
    region_ptrs[ri] = rffi.cast(rffi.VOIDP, batch.weight_buf.base_ptr)
    region_sizes[ri] = rffi.cast(rffi.UINT, count * 8)
    ri += 1
    # null
    region_ptrs[ri] = rffi.cast(rffi.VOIDP, batch.null_buf.base_ptr)
    region_sizes[ri] = rffi.cast(rffi.UINT, count * 8)
    ri += 1
    # non-PK columns
    for ci in range(num_cols):
        if ci == schema.pk_index:
            continue
        col_sz = count * batch.col_strides[ci]
        region_ptrs[ri] = rffi.cast(rffi.VOIDP, batch.col_bufs[ci].base_ptr)
        region_sizes[ri] = rffi.cast(rffi.UINT, col_sz)
        ri += 1
    # blob arena
    blob_size = batch.blob_arena.offset
    region_ptrs[ri] = rffi.cast(rffi.VOIDP, batch.blob_arena.base_ptr)
    region_sizes[ri] = rffi.cast(rffi.UINT, blob_size)

    return region_ptrs, region_sizes, num_regions, blob_size


def encode_batch_append(block_buf, schema, lsn, table_id, batch):
    """Append one WAL block to block_buf without resetting it."""
    region_ptrs, region_sizes, num_regions, blob_size = gather_batch_regions(
        schema, batch
    )
    try:
        # Compute exact block size from actual region sizes
        needed = 48 + num_regions * 8  # header + directory
        ri2 = 0
        while ri2 < num_regions:
            needed = (needed + 7) & ~7  # 8-byte alignment
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
    """Non-owning parse. ptr must remain valid while the returned batch is in use.
    Returns (batch, lsn, tid). Raises CorruptShardError on bad version/checksum."""
    num_cols = len(schema.columns)
    num_non_pk = 0
    for i in range(num_cols):
        if i != schema.pk_index:
            num_non_pk += 1
    max_regions = 4 + num_non_pk + 1

    # Allocate output arrays for header fields and directory
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

        # Reconstruct batch from region offsets (same as before but using Rust-parsed directory)
        region_idx = 0

        pk_lo_buf = buffer_ops.Buffer.from_existing_data(
            rffi.ptradd(ptr, intmask(out_offsets[region_idx])),
            intmask(out_sizes[region_idx]),
        )
        region_idx += 1

        pk_hi_buf = buffer_ops.Buffer.from_existing_data(
            rffi.ptradd(ptr, intmask(out_offsets[region_idx])),
            intmask(out_sizes[region_idx]),
        )
        region_idx += 1

        weight_buf = buffer_ops.Buffer.from_existing_data(
            rffi.ptradd(ptr, intmask(out_offsets[region_idx])),
            intmask(out_sizes[region_idx]),
        )
        region_idx += 1

        null_buf = buffer_ops.Buffer.from_existing_data(
            rffi.ptradd(ptr, intmask(out_offsets[region_idx])),
            intmask(out_sizes[region_idx]),
        )
        region_idx += 1

        col_bufs = newlist_hint(num_cols)
        col_strides = newlist_hint(num_cols)
        for ci in range(num_cols):
            if ci == schema.pk_index:
                col_bufs.append(buffer_ops.Buffer(0, is_owned=True))
                col_strides.append(0)
            else:
                stride = schema.columns[ci].field_type.size
                col_bufs.append(
                    buffer_ops.Buffer.from_existing_data(
                        rffi.ptradd(ptr, intmask(out_offsets[region_idx])),
                        intmask(out_sizes[region_idx]),
                    )
                )
                col_strides.append(stride)
                region_idx += 1

        blob_buf = buffer_ops.Buffer.from_existing_data(
            rffi.ptradd(ptr, intmask(out_offsets[region_idx])),
            intmask(out_sizes[region_idx]),
        )

        result_batch = ArenaZSetBatch.from_buffers(
            schema, pk_lo_buf, pk_hi_buf, weight_buf, null_buf,
            col_bufs, col_strides, blob_buf, count, is_sorted=False,
        )
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
    """Decodes a columnar WAL block from a raw buffer into a WALColumnarBlock.
    The returned block owns the raw buffer; call block.free() when done."""
    result_batch, lsn, tid = _parse_wal_block(buf, total_size, schema)
    return WALColumnarBlock(lsn, tid, result_batch, raw_buf=buf)


def decode_batch_from_ptr(ptr, total_size, schema):
    """Non-owning decode. ptr must remain valid while the batch is in use.
    Caller owns the memory; batch Buffers do NOT free the regions."""
    result_batch, _lsn, _tid = _parse_wal_block(ptr, total_size, schema)
    return result_batch
