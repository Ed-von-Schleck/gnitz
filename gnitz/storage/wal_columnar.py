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


def encode_batch_append(block_buf, schema, lsn, table_id, batch):
    """Append one WAL block to block_buf without resetting it.

    Identical to encode_batch_to_buffer except:
    - No block_buf.reset().
    - block_start = block_buf.offset at entry; all directory offsets and
      header pointers are relative to block_start, not to base_ptr.
    - total_size stored in the header is the block's own byte count,
      not block_buf.offset.

    The sequential-scan decoder advances by the exact total_size from each
    block's header, so no inter-block alignment is required.
    """
    block_start = block_buf.offset                                   # capture first
    block_buf.alloc(wal_layout.WAL_BLOCK_HEADER_SIZE, alignment=1)  # no gap before header

    count = batch.length()

    # Count data regions: pk_lo, pk_hi, weight, null, non-PK columns, blob
    num_cols = len(schema.columns)
    num_non_pk = 0
    for i in range(num_cols):
        if i != schema.pk_index:
            num_non_pk += 1
    num_data_regions = 4 + num_non_pk + 1  # system + cols + blob

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

    # Write directory entries — block-relative offsets
    for ri in range(num_data_regions):
        dir_entry_ptr = rffi.ptradd(block_buf.base_ptr, dir_start + ri * 8)
        u32p = rffi.cast(rffi.UINTP, dir_entry_ptr)
        u32p[0] = rffi.cast(rffi.UINT, region_offsets[ri] - block_start)
        u32p[1] = rffi.cast(rffi.UINT, region_sizes[ri])

    # Backfill header
    total_size = block_buf.offset - block_start
    header = wal_layout.WALBlockHeaderView(
        rffi.ptradd(block_buf.base_ptr, block_start)
    )
    header.set_lsn(r_uint64(lsn))
    header.set_table_id(table_id)
    header.set_entry_count(count)
    header.set_total_size(total_size)
    header.set_format_version(wal_layout.WAL_FORMAT_VERSION_CURRENT)
    header.set_num_regions(num_data_regions)
    header.set_blob_size(r_uint64(blob_size))

    # Compute body checksum (everything after header)
    body_ptr = rffi.ptradd(block_buf.base_ptr,
                           block_start + wal_layout.WAL_BLOCK_HEADER_SIZE)
    body_size = total_size - wal_layout.WAL_BLOCK_HEADER_SIZE
    if body_size > 0:
        header.set_checksum(xxh.compute_checksum(body_ptr, body_size))


def encode_batch_to_buffer(block_buf, schema, lsn, table_id, batch):
    """
    Serializes an ArenaZSetBatch into a WAL block buffer.
    Layout: 48B header + column directory + 8-byte aligned data regions.
    """
    block_buf.reset()
    encode_batch_append(block_buf, schema, lsn, table_id, batch)


def _parse_wal_block(ptr, total_size, schema):
    """Non-owning parse. ptr must remain valid while the returned batch is in use.
    Returns (batch, lsn, tid). Raises CorruptShardError on bad version/checksum."""
    header = wal_layout.WALBlockHeaderView(ptr)
    if header.get_format_version() != wal_layout.WAL_FORMAT_VERSION_CURRENT:
        raise errors.CorruptShardError("Unsupported WAL version")

    expected_cs = header.get_checksum()
    if total_size > wal_layout.WAL_BLOCK_HEADER_SIZE:
        body_ptr = rffi.ptradd(ptr, wal_layout.WAL_BLOCK_HEADER_SIZE)
        body_size = total_size - wal_layout.WAL_BLOCK_HEADER_SIZE
        if xxh.compute_checksum(body_ptr, body_size) != expected_cs:
            raise errors.CorruptShardError("WAL checksum mismatch")

    count = header.get_entry_count()
    lsn = header.get_lsn()
    tid = header.get_table_id()
    num_regions = header.get_num_regions()

    dir_start = wal_layout.WAL_BLOCK_HEADER_SIZE
    offsets = newlist_hint(num_regions)
    sizes = newlist_hint(num_regions)
    for ri in range(num_regions):
        dir_entry_ptr = rffi.ptradd(ptr, dir_start + ri * 8)
        u32p = rffi.cast(rffi.UINTP, dir_entry_ptr)
        offsets.append(intmask(u32p[0]))
        sizes.append(intmask(u32p[1]))

    region_idx = 0

    # All WAL region buffers wrap already-written memory.  Use from_existing_data
    # so that offset == capacity for every buffer — critical for blob_buf, whose
    # offset is read as blob_size by encode_batch_append during DDL re-broadcast.
    pk_lo_buf = buffer_ops.Buffer.from_existing_data(
        rffi.ptradd(ptr, offsets[region_idx]), sizes[region_idx]
    )
    region_idx += 1

    pk_hi_buf = buffer_ops.Buffer.from_existing_data(
        rffi.ptradd(ptr, offsets[region_idx]), sizes[region_idx]
    )
    region_idx += 1

    weight_buf = buffer_ops.Buffer.from_existing_data(
        rffi.ptradd(ptr, offsets[region_idx]), sizes[region_idx]
    )
    region_idx += 1

    null_buf = buffer_ops.Buffer.from_existing_data(
        rffi.ptradd(ptr, offsets[region_idx]), sizes[region_idx]
    )
    region_idx += 1

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
                buffer_ops.Buffer.from_existing_data(
                    rffi.ptradd(ptr, offsets[region_idx]), sizes[region_idx]
                )
            )
            col_strides.append(stride)
            region_idx += 1

    blob_buf = buffer_ops.Buffer.from_existing_data(
        rffi.ptradd(ptr, offsets[region_idx]), sizes[region_idx]
    )

    result_batch = ArenaZSetBatch.from_buffers(
        schema, pk_lo_buf, pk_hi_buf, weight_buf, null_buf,
        col_bufs, col_strides, blob_buf, count, is_sorted=False
    )

    return result_batch, lsn, tid


def decode_batch_from_buffer(buf, total_size, schema):
    """
    Decodes a columnar WAL block from a raw buffer into a WALColumnarBlock.
    The returned block owns the raw buffer; call block.free() when done.
    """
    result_batch, lsn, tid = _parse_wal_block(buf, total_size, schema)
    return WALColumnarBlock(lsn, tid, result_batch, raw_buf=buf)


def decode_batch_from_ptr(ptr, total_size, schema):
    """Non-owning decode. ptr must remain valid while the batch is in use.
    Caller owns the memory; batch Buffers do NOT free the regions."""
    result_batch, _lsn, _tid = _parse_wal_block(ptr, total_size, schema)
    return result_batch
