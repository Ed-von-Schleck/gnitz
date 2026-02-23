# gnitz/storage/wal_format.py

from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import serialize, xxh, strings as string_logic, errors, types
from gnitz.storage import mmap_posix, wal_layout
from gnitz.core.comparator import PayloadRowAccessor
from gnitz.storage.comparator import RawWALAccessor
from gnitz.storage.wal_layout import (
    WALBlockHeaderView,
    WAL_BLOCK_HEADER_SIZE,
    _REC_PK_OFFSET,
    _REC_WEIGHT_OFFSET,
    _REC_NULL_OFFSET,
    _REC_PAYLOAD_BASE,
)


class WALBlobAllocator(string_logic.BlobAllocator):
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


class RawWALRecord(object):
    _immutable_fields_ = []

    def __init__(self, pk, weight, null_word, payload_ptr, heap_ptr):
        self.pk = pk
        self.weight = weight
        self.null_word = null_word
        self.payload_ptr = payload_ptr
        self.heap_ptr = heap_ptr

    def get_key(self):
        return self.pk


class WALRecord(object):
    def __init__(self, key, weight, component_data):
        self.key = key
        self.weight = weight
        self.component_data = component_data

    def get_key(self):
        return self.key


class WALBlock(object):
    def __init__(self, lsn, tid, records, raw_buf=None):
        self.lsn = lsn
        self.tid = tid
        self.records = records
        self._raw_buf = raw_buf

    def free(self):
        if self._raw_buf:
            lltype.free(self._raw_buf, flavor="raw")
            self._raw_buf = lltype.nullptr(rffi.CCHARP.TO)


def compute_record_size(schema, row, accessor=None):
    """
    Calculates the physical size of a record (fixed + heap).
    Uses the unified serialize.get_heap_size kernel.
    Optionally accepts a pre-allocated PayloadRowAccessor to reduce GC pressure.
    """
    if accessor is None:
        accessor = PayloadRowAccessor(schema)

    accessor.set_row(row)
    fixed_size = _REC_PAYLOAD_BASE + schema.memtable_stride
    heap_size = serialize.get_heap_size(schema, accessor)
    return fixed_size, heap_size


def write_wal_record(schema, pk, weight, row, buf, base_offset, heap_base_offset, accessor=None):
    """
    Writes a single WAL record using the unified serialization kernel.
    Delegates payload serialization to serialize.serialize_row.
    """
    wal_layout.write_u128(buf, base_offset + _REC_PK_OFFSET, pk)
    wal_layout.write_i64(buf, base_offset + _REC_WEIGHT_OFFSET, weight)
    wal_layout.write_u64(buf, base_offset + _REC_NULL_OFFSET, row._null_word)

    allocator = WALBlobAllocator(buf, heap_base_offset)
    payload_dest = rffi.ptradd(buf, base_offset + _REC_PAYLOAD_BASE)

    if accessor is None:
        accessor = PayloadRowAccessor(schema)

    accessor.set_row(row)
    serialize.serialize_row(schema, accessor, payload_dest, allocator)


def decode_wal_block(buf, total_size, schema):
    """
    Decodes a WAL block into a list of RawWALRecord objects.
    Uses newlist_hint to avoid RPython mr-poisoning.
    Uses unified get_heap_size kernel via RawWALAccessor to calculate offsets.
    """
    header = WALBlockHeaderView(buf)
    if header.get_format_version() != wal_layout.WAL_FORMAT_VERSION_CURRENT:
        raise errors.CorruptShardError("Unsupported WAL version")

    entry_count = header.get_entry_count()
    expected_cs = header.get_checksum()
    body_ptr = rffi.ptradd(buf, WAL_BLOCK_HEADER_SIZE)
    if total_size > WAL_BLOCK_HEADER_SIZE:
        if xxh.compute_checksum(body_ptr, total_size - WAL_BLOCK_HEADER_SIZE) != expected_cs:
            raise errors.CorruptShardError("WAL checksum mismatch")

    # Use newlist_hint to prevent ListChangeUnallowed (mr-poisoning)
    records = newlist_hint(entry_count)
    current_offset = WAL_BLOCK_HEADER_SIZE

    # Pre-allocate accessor to reuse in the loop
    accessor = RawWALAccessor(schema)

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

        # Use unified kernel to calculate heap size for offset advancement
        accessor.set_record(raw_rec)
        h_sz = serialize.get_heap_size(schema, accessor)

        records.append(raw_rec)
        current_offset += f_sz + h_sz

    return WALBlock(header.get_lsn(), header.get_table_id(), records, raw_buf=buf)


def decode_wal_record_to_row(schema, buf, base, heap_base):
    """
    Unifies test-compatibility paths by reconstructing a PayloadRow
    from a WAL record using the extended deserialize_row kernel.
    """
    null_word = wal_layout.read_u64(buf, base + _REC_NULL_OFFSET)
    payload_ptr = rffi.ptradd(buf, base + _REC_PAYLOAD_BASE)
    return serialize.deserialize_row(schema, payload_ptr, heap_base, null_word)


def write_wal_block(fd, lsn, table_id, records, schema):
    entry_count = len(records)
    total_size = WAL_BLOCK_HEADER_SIZE
    record_sizes = []

    # Pre-allocate accessor to reuse across size calculation and writing
    accessor = PayloadRowAccessor(schema)

    for rec in records:
        f_sz, h_sz = compute_record_size(schema, rec.component_data, accessor=accessor)
        record_sizes.append((f_sz, h_sz))
        total_size += f_sz + h_sz

    buf = lltype.malloc(rffi.CCHARP.TO, total_size, flavor="raw")
    try:
        header = WALBlockHeaderView(buf)
        header.set_lsn(r_uint64(lsn))
        header.set_table_id(table_id)
        header.set_entry_count(entry_count)
        header.set_total_size(total_size)
        header.set_format_version(wal_layout.WAL_FORMAT_VERSION_CURRENT)

        current_offset = WAL_BLOCK_HEADER_SIZE
        for i in range(entry_count):
            rec = records[i]
            f_sz, h_sz = record_sizes[i]
            write_wal_record(
                schema,
                rec.get_key(),
                rec.weight,
                rec.component_data,
                buf,
                current_offset,
                current_offset + f_sz,
                accessor=accessor,
            )
            current_offset += f_sz + h_sz

        body_ptr = rffi.ptradd(buf, WAL_BLOCK_HEADER_SIZE)
        body_size = total_size - WAL_BLOCK_HEADER_SIZE
        if body_size > 0:
            header.set_checksum(xxh.compute_checksum(body_ptr, body_size))

        mmap_posix.write_c(fd, buf, rffi.cast(rffi.SIZE_T, total_size))
    finally:
        lltype.free(buf, flavor="raw")
