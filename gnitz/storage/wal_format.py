# gnitz/storage/wal_format.py

from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import serialize, xxh, strings as string_logic, errors
from gnitz.storage import buffer as buffer_ops, wal_layout
import gnitz.storage.comparator as storage_comparator
import gnitz.core.comparator as core_comparator
from gnitz.storage.wal_layout import (
    WALBlockHeaderView,
    WAL_BLOCK_HEADER_SIZE,
    _REC_PK_OFFSET,
    _REC_WEIGHT_OFFSET,
    _REC_NULL_OFFSET,
    _REC_PAYLOAD_BASE,
)


class WALBlobAllocator(string_logic.BlobAllocator):
    """Writes string tails into a reused WAL block buffer."""

    def __init__(self, block_buf):
        self.block_buf = block_buf

    def allocate(self, string_data):
        length = len(string_data)
        off = self.block_buf.offset
        dest = self.block_buf.alloc(length, alignment=1)
        for k in range(length):
            dest[k] = string_data[k]
        return r_uint64(off)

    def allocate_from_ptr(self, src_ptr, length):
        """Zero-copy allocation from raw pointer source."""
        off = self.block_buf.offset
        dest = self.block_buf.alloc(length, alignment=1)
        if length > 0:
            buffer_ops.c_memmove(
                rffi.cast(rffi.VOIDP, dest),
                rffi.cast(rffi.VOIDP, src_ptr),
                rffi.cast(rffi.SIZE_T, length),
            )
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
    Calculates the physical size of a WAL record.
    Includes fixed metadata, stride, and variable-length heap data.
    """
    fixed_size = _REC_PAYLOAD_BASE + schema.memtable_stride
    heap_size = serialize.get_heap_size(schema, accessor)
    return fixed_size, heap_size


def append_record_to_buffer(block_buf, schema, acc, pk, weight, allocator):
    """
    Writes a single record directly into the provided Buffer.
    
    CRITICAL: The caller MUST call block_buf.ensure_capacity() with the 
    total size (fixed + heap) before calling this. This ensures the buffer 
    does not reallocate during serialization, keeping rec_ptr stable.
    """
    stride = schema.memtable_stride
    rec_start_off = block_buf.offset
    
    # 1. Reserve metadata and payload space
    fixed_sz = _REC_PAYLOAD_BASE + stride
    block_buf.alloc(fixed_sz)
    
    # Get a stable pointer for this record's fixed section
    rec_ptr = rffi.ptradd(block_buf.base_ptr, rec_start_off)
    
    # 2. Write metadata header
    wal_layout.write_u128(rec_ptr, _REC_PK_OFFSET, pk)
    wal_layout.write_i64(rec_ptr, _REC_WEIGHT_OFFSET, weight)
    
    null_word = r_uint64(0)
    if isinstance(acc, storage_comparator.RawWALAccessor):
        null_word = acc.null_word
    elif isinstance(acc, core_comparator.PayloadRowAccessor):
        if acc._row:
            null_word = acc._row._null_word
    wal_layout.write_u64(rec_ptr, _REC_NULL_OFFSET, null_word)
    
    # 3. Serialize payload and heap blobs
    # Strings will be allocated via allocator.allocate(), which advances block_buf.offset
    payload_ptr = rffi.ptradd(rec_ptr, _REC_PAYLOAD_BASE)
    serialize.serialize_row(schema, acc, payload_ptr, allocator)


def decode_wal_block(buf, total_size, schema):
    """
    Decodes a raw WAL block into RawWALRecord pointers.
    Used for recovery and WAL reading.
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

    records = [] # Resizable list
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
        heap_ptr = buf

        raw_rec = RawWALRecord(pk, weight, null_word, payload_ptr, heap_ptr)
        accessor.set_record(raw_rec)
        h_sz = serialize.get_heap_size(schema, accessor)

        records.append(raw_rec)
        current_offset += f_sz + h_sz

    return WALBlock(header.get_lsn(), header.get_table_id(), records, raw_buf=buf)
