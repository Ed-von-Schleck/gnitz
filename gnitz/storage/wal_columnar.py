# gnitz/storage/wal_columnar.py

from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import xxh, errors
from gnitz.core.batch import ArenaZSetBatch
from gnitz.storage import buffer as buffer_ops, wal_layout


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


def _align8(val):
    return (val + 7) & ~7


def _copy_and_record(block_buf, src_ptr, size, offsets, sizes, idx):
    """Copy a region into block_buf with 8-byte alignment and record its offset/size."""
    block_buf.alloc(0, alignment=8)
    off = block_buf.offset
    if size > 0:
        block_buf.put_bytes(src_ptr, size)
    offsets[idx] = off
    sizes[idx] = size
    return idx + 1


def encode_batch_to_buffer(block_buf, schema, lsn, table_id, batch):
    """
    Serializes an ArenaZSetBatch into a WAL block buffer.
    Layout: 48B header + column directory + 8-byte aligned data regions.
    """
    count = batch.length()

    # Count data regions: pk_lo, pk_hi, weight, null, non-PK columns, blob
    num_cols = len(schema.columns)
    num_non_pk = 0
    for i in range(num_cols):
        if i != schema.pk_index:
            num_non_pk += 1
    num_data_regions = 4 + num_non_pk + 1  # system + cols + blob

    # Reset buffer and reserve header
    block_buf.reset()
    block_buf.alloc(wal_layout.WAL_BLOCK_HEADER_SIZE)

    # Reserve directory space
    dir_start = block_buf.offset
    dir_size = num_data_regions * 8
    block_buf.alloc(dir_size, alignment=1)

    # Track region offsets and sizes
    region_offsets = newlist_hint(num_data_regions)
    region_sizes = newlist_hint(num_data_regions)
    for _ in range(num_data_regions):
        region_offsets.append(0)
        region_sizes.append(0)

    region_idx = 0

    # pk_lo
    region_idx = _copy_and_record(
        block_buf, batch.pk_lo_buf.base_ptr, count * 8,
        region_offsets, region_sizes, region_idx
    )

    # pk_hi
    region_idx = _copy_and_record(
        block_buf, batch.pk_hi_buf.base_ptr, count * 8,
        region_offsets, region_sizes, region_idx
    )

    # weight
    region_idx = _copy_and_record(
        block_buf, batch.weight_buf.base_ptr, count * 8,
        region_offsets, region_sizes, region_idx
    )

    # null
    region_idx = _copy_and_record(
        block_buf, batch.null_buf.base_ptr, count * 8,
        region_offsets, region_sizes, region_idx
    )

    # Non-PK column buffers
    for ci in range(num_cols):
        if ci == schema.pk_index:
            continue
        col_sz = count * batch.col_strides[ci]
        region_idx = _copy_and_record(
            block_buf, batch.col_bufs[ci].base_ptr, col_sz,
            region_offsets, region_sizes, region_idx
        )

    # Blob arena
    blob_size = batch.blob_arena.offset
    region_idx = _copy_and_record(
        block_buf, batch.blob_arena.base_ptr, blob_size,
        region_offsets, region_sizes, region_idx
    )

    # Write directory entries (offset, size as u32 pairs)
    for ri in range(num_data_regions):
        dir_entry_ptr = rffi.ptradd(block_buf.base_ptr, dir_start + ri * 8)
        u32p = rffi.cast(rffi.UINTP, dir_entry_ptr)
        u32p[0] = rffi.cast(rffi.UINT, region_offsets[ri])
        u32p[1] = rffi.cast(rffi.UINT, region_sizes[ri])

    # Backfill header
    total_size = block_buf.offset
    header = wal_layout.WALBlockHeaderView(block_buf.base_ptr)
    header.set_lsn(r_uint64(lsn))
    header.set_table_id(table_id)
    header.set_entry_count(count)
    header.set_total_size(total_size)
    header.set_format_version(wal_layout.WAL_FORMAT_VERSION_CURRENT)
    header.set_num_regions(num_data_regions)
    header.set_blob_size(r_uint64(blob_size))

    # Compute body checksum (everything after header)
    body_ptr = rffi.ptradd(block_buf.base_ptr, wal_layout.WAL_BLOCK_HEADER_SIZE)
    body_size = total_size - wal_layout.WAL_BLOCK_HEADER_SIZE
    if body_size > 0:
        header.set_checksum(xxh.compute_checksum(body_ptr, body_size))


def decode_batch_from_buffer(buf, total_size, schema):
    """
    Decodes a columnar WAL block from a raw buffer into a WALColumnarBlock.
    The returned block owns the raw buffer; call block.free() when done.
    """
    header = wal_layout.WALBlockHeaderView(buf)
    if header.get_format_version() != wal_layout.WAL_FORMAT_VERSION_CURRENT:
        raise errors.CorruptShardError("Unsupported WAL version")

    # Verify checksum
    expected_cs = header.get_checksum()
    if total_size > wal_layout.WAL_BLOCK_HEADER_SIZE:
        body_ptr = rffi.ptradd(buf, wal_layout.WAL_BLOCK_HEADER_SIZE)
        body_size = total_size - wal_layout.WAL_BLOCK_HEADER_SIZE
        if xxh.compute_checksum(body_ptr, body_size) != expected_cs:
            raise errors.CorruptShardError("WAL checksum mismatch")

    count = header.get_entry_count()
    lsn = header.get_lsn()
    tid = header.get_table_id()
    num_regions = header.get_num_regions()

    # Read directory
    dir_start = wal_layout.WAL_BLOCK_HEADER_SIZE
    offsets = newlist_hint(num_regions)
    sizes = newlist_hint(num_regions)
    for ri in range(num_regions):
        dir_entry_ptr = rffi.ptradd(buf, dir_start + ri * 8)
        u32p = rffi.cast(rffi.UINTP, dir_entry_ptr)
        offsets.append(intmask(u32p[0]))
        sizes.append(intmask(u32p[1]))

    # Wrap regions as non-owning Buffers
    region_idx = 0

    pk_lo_buf = buffer_ops.Buffer.from_external_ptr(
        rffi.ptradd(buf, offsets[region_idx]), sizes[region_idx]
    )
    region_idx += 1

    pk_hi_buf = buffer_ops.Buffer.from_external_ptr(
        rffi.ptradd(buf, offsets[region_idx]), sizes[region_idx]
    )
    region_idx += 1

    weight_buf = buffer_ops.Buffer.from_external_ptr(
        rffi.ptradd(buf, offsets[region_idx]), sizes[region_idx]
    )
    region_idx += 1

    null_buf = buffer_ops.Buffer.from_external_ptr(
        rffi.ptradd(buf, offsets[region_idx]), sizes[region_idx]
    )
    region_idx += 1

    # Non-PK column buffers
    num_cols = len(schema.columns)
    col_bufs = newlist_hint(num_cols)
    col_strides = newlist_hint(num_cols)
    for ci in range(num_cols):
        if ci == schema.pk_index:
            col_bufs.append(buffer_ops.Buffer(0, is_owned=True))
            col_strides.append(0)
        else:
            stride = schema.columns[ci].field_type.size
            col_bufs.append(
                buffer_ops.Buffer.from_external_ptr(
                    rffi.ptradd(buf, offsets[region_idx]), sizes[region_idx]
                )
            )
            col_strides.append(stride)
            region_idx += 1

    # Blob arena
    blob_buf = buffer_ops.Buffer.from_external_ptr(
        rffi.ptradd(buf, offsets[region_idx]), sizes[region_idx]
    )

    batch = ArenaZSetBatch.from_buffers(
        schema, pk_lo_buf, pk_hi_buf, weight_buf, null_buf,
        col_bufs, col_strides, blob_buf, count, is_sorted=False
    )

    return WALColumnarBlock(lsn, tid, batch, raw_buf=buf)
