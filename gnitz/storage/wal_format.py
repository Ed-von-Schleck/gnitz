# gnitz/storage/wal_format.py

from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import serialize, xxh, strings as string_logic, errors, types
from gnitz.storage import mmap_posix, wal_layout
from gnitz.storage.wal_layout import (
    WALBlockHeaderView,
    WAL_BLOCK_HEADER_SIZE,
    _REC_PK_OFFSET,
    _REC_WEIGHT_OFFSET,
    _REC_NULL_OFFSET,
    _REC_PAYLOAD_BASE,
)


class WALBlobAllocator(string_logic.BlobAllocator):
    """Writes string tails into a specific offset within the WAL block buffer."""

    def __init__(self, buf, heap_base_offset):
        self.buf = buf
        self.heap_base_offset = heap_base_offset
        self.cursor = 0

    def allocate(self, string_data):
        length = len(string_data)
        dest = rffi.ptradd(self.buf, self.heap_base_offset + self.cursor)
        for k in range(length):
            dest[k] = string_data[k]
        off = self.cursor
        self.cursor += length
        return r_uint64(off)

    def allocate_from_ptr(self, src_ptr, length):
        """Zero-copy allocation from raw pointer source."""
        dest = rffi.ptradd(self.buf, self.heap_base_offset + self.cursor)
        if length > 0:
            import gnitz.storage.buffer as buffer_ops

            buffer_ops.c_memmove(
                rffi.cast(rffi.VOIDP, dest),
                rffi.cast(rffi.VOIDP, src_ptr),
                rffi.cast(rffi.SIZE_T, length),
            )
        off = self.cursor
        self.cursor += length
        return r_uint64(off)


class RawWALRecord(object):
    """Metadata for a record read from the WAL, pointing into a raw buffer."""

    _immutable_fields_ = ["pk", "weight", "null_word", "payload_ptr", "heap_ptr"]

    def __init__(self, pk, weight, null_word, payload_ptr, heap_ptr):
        self.pk = pk
        self.weight = weight
        self.null_word = null_word
        self.payload_ptr = payload_ptr
        self.heap_ptr = heap_ptr

    def get_key(self):
        return self.pk


class WALBlock(object):
    """A decoded WAL block containing a sequence of RawWALRecord pointers."""

    def __init__(self, lsn, tid, records, raw_buf=None):
        self.lsn = lsn
        self.tid = tid
        self.records = records
        self._raw_buf = raw_buf

    def free(self):
        if self._raw_buf:
            lltype.free(self._raw_buf, flavor="raw")
            self._raw_buf = lltype.nullptr(rffi.CCHARP.TO)


def compute_record_size(schema, accessor):
    """
    Calculates the physical size of a WAL record (Fixed Header + Payload + String Heap).
    """
    fixed_size = _REC_PAYLOAD_BASE + schema.memtable_stride
    heap_size = serialize.get_heap_size(schema, accessor)
    return fixed_size, heap_size


def write_wal_record(schema, pk, weight, accessor, buf, base_offset, heap_base_offset):
    """
    Serializes a single record into the WAL buffer from a RowAccessor.
    """
    # 1. Fixed Header
    wal_layout.write_u128(buf, base_offset + _REC_PK_OFFSET, pk)
    wal_layout.write_i64(buf, base_offset + _REC_WEIGHT_OFFSET, weight)

    # 2. Extract Null Word from Accessor
    null_word = r_uint64(0)
    import gnitz.storage.comparator as storage_comparator
    import gnitz.core.comparator as core_comparator

    if isinstance(accessor, storage_comparator.RawWALAccessor):
        null_word = accessor.null_word
    elif isinstance(accessor, core_comparator.PayloadRowAccessor):
        if accessor._row:
            null_word = accessor._row._null_word

    wal_layout.write_u64(buf, base_offset + _REC_NULL_OFFSET, null_word)

    # 3. Payload and Blob relocation
    allocator = WALBlobAllocator(buf, heap_base_offset)
    payload_dest = rffi.ptradd(buf, base_offset + _REC_PAYLOAD_BASE)

    serialize.serialize_row(schema, accessor, payload_dest, allocator)


def decode_wal_block(buf, total_size, schema):
    """
    Decodes a raw WAL block into RawWALRecord pointers.
    Identical to previous implementation as the on-disk format is stable.
    """
    header = WALBlockHeaderView(buf)
    if header.get_format_version() != wal_layout.WAL_FORMAT_VERSION_CURRENT:
        raise errors.CorruptShardError("Unsupported WAL version")

    entry_count = header.get_entry_count()
    expected_cs = header.get_checksum()
    body_ptr = rffi.ptradd(buf, WAL_BLOCK_HEADER_SIZE)

    if total_size > WAL_BLOCK_HEADER_SIZE:
        if (
            xxh.compute_checksum(body_ptr, total_size - WAL_BLOCK_HEADER_SIZE)
            != expected_cs
        ):
            raise errors.CorruptShardError("WAL checksum mismatch")

    records = newlist_hint(entry_count)
    current_offset = WAL_BLOCK_HEADER_SIZE
    accessor = storage_comparator.RawWALAccessor(schema)

    for i in range(entry_count):
        pk = wal_layout.read_u128(buf, current_offset + _REC_PK_OFFSET)
        weight = rffi.cast(
            rffi.LONGLONGP, rffi.ptradd(buf, current_offset + _REC_WEIGHT_OFFSET)
        )[0]
        null_word = wal_layout.read_u64(buf, current_offset + _REC_NULL_OFFSET)

        f_sz = _REC_PAYLOAD_BASE + schema.memtable_stride
        payload_ptr = rffi.ptradd(buf, current_offset + _REC_PAYLOAD_BASE)
        heap_ptr = rffi.ptradd(buf, current_offset + f_sz)

        raw_rec = RawWALRecord(pk, weight, null_word, payload_ptr, heap_ptr)
        accessor.set_record(raw_rec)
        h_sz = serialize.get_heap_size(schema, accessor)

        records.append(raw_rec)
        current_offset += f_sz + h_sz

    return WALBlock(header.get_lsn(), header.get_table_id(), records, raw_buf=buf)


def decode_wal_record_to_row(schema, buf, base, heap_base):
    null_word = wal_layout.read_u64(buf, base + _REC_NULL_OFFSET)
    payload_ptr = rffi.ptradd(buf, base + _REC_PAYLOAD_BASE)
    return serialize.deserialize_row(schema, payload_ptr, heap_base, null_word)


def write_wal_block(fd, lsn, table_id, batch, start_idx, count, schema):
    """
    Serializes a range of records from an ArenaZSetBatch into a single WAL block.
    """
    total_size = WAL_BLOCK_HEADER_SIZE
    # Parallel list of (fixed_sz, heap_sz) to avoid re-calculating during write
    sizes = newlist_hint(count)

    # 1. First Pass: Calculate Total Block Size
    for i in range(start_idx, start_idx + count):
        acc = batch.get_accessor(i)
        f_sz, h_sz = compute_record_size(schema, acc)
        sizes.append((f_sz, h_sz))
        total_size += f_sz + h_sz

    # 2. Second Pass: Materialize Block
    buf = lltype.malloc(rffi.CCHARP.TO, total_size, flavor="raw")
    try:
        header = WALBlockHeaderView(buf)
        header.set_lsn(r_uint64(lsn))
        header.set_table_id(table_id)
        header.set_entry_count(count)
        header.set_total_size(total_size)
        header.set_format_version(wal_layout.WAL_FORMAT_VERSION_CURRENT)

        current_offset = WAL_BLOCK_HEADER_SIZE
        for i in range(count):
            batch_idx = start_idx + i
            f_sz, h_sz = sizes[i]

            write_wal_record(
                schema,
                batch.get_pk(batch_idx),
                batch.get_weight(batch_idx),
                batch.get_accessor(batch_idx),
                buf,
                current_offset,
                current_offset + f_sz,
            )
            current_offset += f_sz + h_sz

        # 3. Finalize Checksum and Write to Disk
        body_ptr = rffi.ptradd(buf, WAL_BLOCK_HEADER_SIZE)
        body_size = total_size - WAL_BLOCK_HEADER_SIZE
        if body_size > 0:
            header.set_checksum(xxh.compute_checksum(body_ptr, body_size))

        mmap_posix.write_c(fd, buf, rffi.cast(rffi.SIZE_T, total_size))
    finally:
        lltype.free(buf, flavor="raw")
