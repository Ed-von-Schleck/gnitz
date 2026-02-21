# gnitz/storage/wal_format.py
#
# WAL (Write-Ahead Log) serialisation and deserialisation.
#
# This implementation uses a centralized Header View to prevent layout
# regressions and width mismatches.

from rpython.rlib.rarithmetic import r_int64, r_uint64, r_uint32, r_ulonglonglong as r_uint128
from rpython.rlib.rarithmetic import intmask
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit

from gnitz.core import types, xxh, strings as string_logic, errors
from gnitz.core.values import make_payload_row
from gnitz.storage import mmap_posix


# ---------------------------------------------------------------------------
# WAL Block Header Geometry (Source of Truth)
# ---------------------------------------------------------------------------

WAL_OFF_LSN       = 0   # u64: Log Sequence Number
WAL_OFF_TID       = 8   # u32: Table ID
WAL_OFF_COUNT     = 12  # u32: Entry Count
WAL_OFF_SIZE      = 16  # u32: Total Block Size (header + body)
WAL_OFF_RESERVED  = 20  # u32: Reserved padding
WAL_OFF_CHECKSUM  = 24  # u64: XXH3-64 of the body
WAL_BLOCK_HEADER_SIZE = 32


# ---------------------------------------------------------------------------
# Record header byte offsets (Internal to Block Body)
# ---------------------------------------------------------------------------

_REC_PK_OFFSET     =  0   # 16 bytes: primary key (u64 or u128)
_REC_WEIGHT_OFFSET = 16   #  8 bytes: algebraic weight
_REC_NULL_OFFSET   = 24   #  8 bytes: null bitmap (64 bits)
_REC_PAYLOAD_BASE  = 32   #  Fixed-stride payload starts here


# ---------------------------------------------------------------------------
# WAL Block Header View
# ---------------------------------------------------------------------------

class WALBlockHeaderView(object):
    """
    Encapsulates WAL header layout. Provides type-safe accessors to prevent
    the manual offset errors and width mismatches.
    """
    _immutable_fields_ = ["ptr"]

    def __init__(self, ptr):
        self.ptr = ptr

    @jit.elidable
    def get_lsn(self):
        return _read_u64(self.ptr, WAL_OFF_LSN)

    def set_lsn(self, lsn):
        _write_u64(self.ptr, WAL_OFF_LSN, lsn)

    @jit.elidable
    def get_table_id(self):
        return intmask(_read_u32(self.ptr, WAL_OFF_TID))

    def set_table_id(self, tid):
        _write_u32(self.ptr, WAL_OFF_TID, r_uint64(r_uint32(tid)))

    @jit.elidable
    def get_entry_count(self):
        return intmask(_read_u32(self.ptr, WAL_OFF_COUNT))

    def set_entry_count(self, count):
        _write_u32(self.ptr, WAL_OFF_COUNT, r_uint64(r_uint32(count)))

    @jit.elidable
    def get_total_size(self):
        return intmask(_read_u32(self.ptr, WAL_OFF_SIZE))

    def set_total_size(self, size):
        u32_size = r_uint32(size)
        _write_u32(self.ptr, WAL_OFF_SIZE, r_uint64(u32_size))
        _write_u32(self.ptr, WAL_OFF_RESERVED, r_uint64(0))

    @jit.elidable
    def get_checksum(self):
        return _read_u64(self.ptr, WAL_OFF_CHECKSUM)

    def set_checksum(self, cs):
        _write_u64(self.ptr, WAL_OFF_CHECKSUM, cs)


# ---------------------------------------------------------------------------
# Buffer primitive readers (Raw Memory Access)
# ---------------------------------------------------------------------------

def _read_i8(buf, offset):
    p = rffi.cast(rffi.CCHARP, rffi.ptradd(buf, offset))
    val = rffi.cast(rffi.SIGNEDCHAR, p[0])
    return r_int64(val)

def _read_u8(buf, offset):
    p = rffi.cast(rffi.CCHARP, rffi.ptradd(buf, offset))
    val = rffi.cast(rffi.UCHAR, p[0])
    return r_uint64(val)

def _read_i16(buf, offset):
    p = rffi.cast(rffi.SHORTP, rffi.ptradd(buf, offset))
    return r_int64(p[0])

def _read_u16(buf, offset):
    p = rffi.cast(rffi.USHORTP, rffi.ptradd(buf, offset))
    return r_uint64(p[0])

def _read_i32(buf, offset):
    p = rffi.cast(rffi.INTP, rffi.ptradd(buf, offset))
    return r_int64(p[0])

def _read_u32(buf, offset):
    p = rffi.cast(rffi.UINTP, rffi.ptradd(buf, offset))
    return r_uint64(p[0])

def _read_i64(buf, offset):
    p = rffi.cast(rffi.LONGLONGP, rffi.ptradd(buf, offset))
    return r_int64(p[0])

def _read_u64(buf, offset):
    p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, offset))
    return r_uint64(p[0])

def _read_u128(buf, offset):
    lo = r_uint128(_read_u64(buf, offset))
    hi = r_uint128(_read_u64(buf, offset + 8))
    return (hi << 64) | lo

def _read_f32(buf, offset):
    p = rffi.cast(rffi.FLOATP, rffi.ptradd(buf, offset))
    return float(p[0])

def _read_f64(buf, offset):
    p = rffi.cast(rffi.DOUBLEP, rffi.ptradd(buf, offset))
    return float(p[0])


# ---------------------------------------------------------------------------
# Buffer primitive writers
# ---------------------------------------------------------------------------

def _write_i8(buf, offset, val_i64):
    p = rffi.cast(rffi.CCHARP, rffi.ptradd(buf, offset))
    p[0] = chr(intmask(val_i64 & 0xFF))

def _write_u8(buf, offset, val_u64):
    p = rffi.cast(rffi.CCHARP, rffi.ptradd(buf, offset))
    p[0] = chr(intmask(val_u64 & 0xFF))

def _write_i16(buf, offset, val_i64):
    p = rffi.cast(rffi.SHORTP, rffi.ptradd(buf, offset))
    p[0] = rffi.cast(rffi.SHORT, val_i64)

def _write_u16(buf, offset, val_u64):
    p = rffi.cast(rffi.USHORTP, rffi.ptradd(buf, offset))
    p[0] = rffi.cast(rffi.USHORT, val_u64)

def _write_i32(buf, offset, val_i64):
    p = rffi.cast(rffi.INTP, rffi.ptradd(buf, offset))
    p[0] = rffi.cast(rffi.INT, val_i64)

def _write_u32(buf, offset, val_u64):
    p = rffi.cast(rffi.UINTP, rffi.ptradd(buf, offset))
    p[0] = rffi.cast(rffi.UINT, val_u64)

def _write_i64(buf, offset, val_i64):
    p = rffi.cast(rffi.LONGLONGP, rffi.ptradd(buf, offset))
    p[0] = rffi.cast(rffi.LONGLONG, val_i64)

def _write_u64(buf, offset, val_u64):
    p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, offset))
    p[0] = rffi.cast(rffi.ULONGLONG, val_u64)

def _write_u128(buf, offset, val_u128):
    _write_u64(buf, offset,     r_uint64(val_u128))
    _write_u64(buf, offset + 8, r_uint64(val_u128 >> 64))

def _write_f32(buf, offset, val_f64):
    p = rffi.cast(rffi.FLOATP, rffi.ptradd(buf, offset))
    p[0] = rffi.cast(rffi.FLOAT, val_f64)

def _write_f64(buf, offset, val_f64):
    p = rffi.cast(rffi.DOUBLEP, rffi.ptradd(buf, offset))
    p[0] = rffi.cast(rffi.DOUBLE, val_f64)


# ---------------------------------------------------------------------------
# String (blob) helpers
# ---------------------------------------------------------------------------

def _unpack_string(buf, col_offset, heap_base):
    blob_len = intmask(_read_u32(buf, col_offset))
    blob_off = intmask(_read_u64(buf, col_offset + 8))
    abs_off  = heap_base + blob_off
    
    chars = [None] * blob_len
    src = rffi.cast(rffi.CCHARP, rffi.ptradd(buf, abs_off))
    for k in range(blob_len):
        chars[k] = src[k]
    return "".join(chars)

def _pack_string_ref(buf, col_offset, blob_off_rel, blob_len):
    _write_u32(buf, col_offset,     r_uint64(blob_len))
    _write_u64(buf, col_offset + 8, r_uint64(blob_off_rel))

def _write_blob_bytes(buf, heap_base, blob_off_rel, val_str):
    dest = rffi.cast(rffi.CCHARP, rffi.ptradd(buf, heap_base + blob_off_rel))
    for k in range(len(val_str)):
        dest[k] = val_str[k]


# ---------------------------------------------------------------------------
# Record decoding
# ---------------------------------------------------------------------------

def decode_wal_record(schema, buf, base, heap_base):
    """
    Decodes a single row-oriented record from the WAL buffer.
    Returns: (pk: r_uint128, weight: r_int64, row: PayloadRow)
    """
    pk        = _read_u128(buf, base + _REC_PK_OFFSET)
    weight    = _read_i64 (buf, base + _REC_WEIGHT_OFFSET)
    null_word = _read_u64 (buf, base + _REC_NULL_OFFSET)

    row = make_payload_row(schema)
    payload_col = 0

    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue

        if null_word & (r_uint64(1) << payload_col):
            row.append_null(payload_col)
            payload_col += 1
            continue

        col_type   = schema.columns[i].field_type
        col_offset = base + _REC_PAYLOAD_BASE + schema.column_offsets[i]

        code = col_type.code
        if code == types.TYPE_I64.code:
            row.append_int(_read_i64(buf, col_offset))
        elif code == types.TYPE_I32.code:
            row.append_int(_read_i32(buf, col_offset))
        elif code == types.TYPE_I16.code:
            row.append_int(_read_i16(buf, col_offset))
        elif code == types.TYPE_I8.code:
            row.append_int(_read_i8(buf, col_offset))
        elif code == types.TYPE_U64.code:
            row.append_int(rffi.cast(rffi.LONGLONG, _read_u64(buf, col_offset)))
        elif code == types.TYPE_U32.code:
            row.append_int(rffi.cast(rffi.LONGLONG, _read_u32(buf, col_offset)))
        elif code == types.TYPE_U16.code:
            row.append_int(rffi.cast(rffi.LONGLONG, _read_u16(buf, col_offset)))
        elif code == types.TYPE_U8.code:
            row.append_int(rffi.cast(rffi.LONGLONG, _read_u8(buf, col_offset)))
        elif code == types.TYPE_F64.code:
            row.append_float(_read_f64(buf, col_offset))
        elif code == types.TYPE_F32.code:
            row.append_float(_read_f32(buf, col_offset))
        elif code == types.TYPE_STRING.code:
            row.append_string(_unpack_string(buf, col_offset, heap_base))
        elif code == types.TYPE_U128.code:
            row.append_u128(_read_u64(buf, col_offset), _read_u64(buf, col_offset + 8))
        else:
            row.append_int(_read_i64(buf, col_offset))

        payload_col += 1

    return pk, weight, row


# ---------------------------------------------------------------------------
# Record encoding
# ---------------------------------------------------------------------------

def write_wal_record(schema, pk, weight, row, buf, base, heap_base):
    """
    Encodes a PayloadRow into the WAL buffer at 'base'. 
    String bodies are written to 'heap_base'.
    """
    _write_u128(buf, base + _REC_PK_OFFSET,     pk)
    _write_i64 (buf, base + _REC_WEIGHT_OFFSET, weight)
    _write_u64 (buf, base + _REC_NULL_OFFSET,   row._null_word)

    heap_cursor = 0
    payload_col = 0

    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue

        if row.is_null(payload_col):
            payload_col += 1
            continue

        col_type   = schema.columns[i].field_type
        col_offset = base + _REC_PAYLOAD_BASE + schema.column_offsets[i]
        code = col_type.code

        if code == types.TYPE_STRING.code:
            s_val = row.get_str(payload_col)
            p = rffi.cast(rffi.UINT, string_logic.compute_prefix(s_val))
            _write_u32(buf, col_offset + 4, r_uint64(p))

            _pack_string_ref(buf, col_offset, heap_cursor, len(s_val))
            _write_blob_bytes(buf, heap_base, heap_cursor, s_val)
            heap_cursor += len(s_val)
        elif code == types.TYPE_F64.code:
            _write_f64(buf, col_offset, row.get_float(payload_col))
        elif code == types.TYPE_F32.code:
            _write_f32(buf, col_offset, row.get_float(payload_col))
        elif code == types.TYPE_U128.code:
            v = row.get_u128(payload_col)
            _write_u128(buf, col_offset, v)
        elif code == types.TYPE_U64.code:
            _write_u64(buf, col_offset, row.get_int(payload_col))
        elif code == types.TYPE_U32.code:
            _write_u32(buf, col_offset, row.get_int(payload_col))
        elif code == types.TYPE_U16.code:
            _write_u16(buf, col_offset, row.get_int(payload_col))
        elif code == types.TYPE_U8.code:
            _write_u8(buf, col_offset, row.get_int(payload_col))
        elif code == types.TYPE_I64.code:
            _write_i64(buf, col_offset, row.get_int_signed(payload_col))
        elif code == types.TYPE_I32.code:
            _write_i32(buf, col_offset, row.get_int_signed(payload_col))
        elif code == types.TYPE_I16.code:
            _write_i16(buf, col_offset, row.get_int_signed(payload_col))
        elif code == types.TYPE_I8.code:
            _write_i8(buf, col_offset, row.get_int_signed(payload_col))
        else:
            _write_i64(buf, col_offset, row.get_int_signed(payload_col))

        payload_col += 1


# ---------------------------------------------------------------------------
# WAL Objects & Sizing
# ---------------------------------------------------------------------------

class WALRecord(object):
    """Container for a Z-Set delta during WAL processing."""
    def __init__(self, key, weight, component_data):
        self.key = key             # r_uint128
        self.weight = weight       # r_int64
        self.component_data = component_data # PayloadRow

    def get_key(self):
        return self.key

class WALBlock(object):
    """A batch of Z-Set records sharing a single LSN."""
    def __init__(self, lsn, tid, records):
        self.lsn = lsn
        self.tid = tid
        self.records = records

def compute_record_size(schema, row):
    """Calculates the total size (fixed + variable) for a record."""
    fixed_size = _REC_PAYLOAD_BASE + schema.memtable_stride
    heap_size = 0
    payload_col = 0
    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue
        if schema.columns[i].field_type.code == types.TYPE_STRING.code:
            if not row.is_null(payload_col):
                heap_size += len(row.get_str(payload_col))
        payload_col += 1
    return fixed_size, heap_size


# ---------------------------------------------------------------------------
# High-level Block IO
# ---------------------------------------------------------------------------

def write_wal_block(fd, lsn, table_id, records, schema):
    """Serialises a list of WALRecords into a block and writes to fd."""
    entry_count = len(records)
    total_size = WAL_BLOCK_HEADER_SIZE
    
    record_sizes = []
    for rec in records:
        f_sz, h_sz = compute_record_size(schema, rec.component_data)
        record_sizes.append((f_sz, h_sz))
        total_size += f_sz + h_sz
        
    buf = lltype.malloc(rffi.CCHARP.TO, total_size, flavor='raw')
    try:
        # Header Encapsulation
        header = WALBlockHeaderView(buf)
        header.set_lsn(r_uint64(lsn))
        header.set_table_id(table_id)
        header.set_entry_count(entry_count)
        header.set_total_size(total_size)
        
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
                current_offset + f_sz
            )
            current_offset += f_sz + h_sz
            
        # Checksum calculation over block body
        body_ptr = rffi.ptradd(buf, WAL_BLOCK_HEADER_SIZE)
        body_size = total_size - WAL_BLOCK_HEADER_SIZE
        if body_size > 0:
            cs = xxh.compute_checksum(body_ptr, body_size)
            header.set_checksum(cs)
        else:
            header.set_checksum(r_uint64(0))
            
        n = mmap_posix.write_c(fd, buf, rffi.cast(rffi.SIZE_T, total_size))
        if rffi.cast(lltype.Signed, n) < total_size:
            raise IOError("WAL write syscall failed or was incomplete")
    finally:
        lltype.free(buf, flavor='raw')


def decode_wal_block(buf, total_size, schema):
    """Reconstructs a WALBlock from a raw memory buffer."""
    header = WALBlockHeaderView(buf)
    
    lsn = header.get_lsn()
    tid = header.get_table_id()
    entry_count = header.get_entry_count()
    
    # Verify Integrity
    body_ptr = rffi.ptradd(buf, WAL_BLOCK_HEADER_SIZE)
    body_size = total_size - WAL_BLOCK_HEADER_SIZE
    expected_cs = header.get_checksum()
    
    if body_size > 0:
        actual_cs = xxh.compute_checksum(body_ptr, body_size)
        if actual_cs != expected_cs:
            raise errors.CorruptShardError("WAL block checksum mismatch")
            
    records = []
    current_offset = WAL_BLOCK_HEADER_SIZE
    for i in range(entry_count):
        f_sz = _REC_PAYLOAD_BASE + schema.memtable_stride
        heap_base = current_offset + f_sz
        
        pk, weight, row = decode_wal_record(schema, buf, current_offset, heap_base)
        records.append(WALRecord(pk, weight, row))
        
        _, h_sz = compute_record_size(schema, row)
        current_offset += f_sz + h_sz
        
    return WALBlock(lsn, tid, records)
