# gnitz/storage/wal_format.py
#
# WAL (Write-Ahead Log) serialisation and deserialisation.
#
# Both directions of the codec are updated to work with ``PayloadRow``
# directly, replacing the former ``List`` representation.
# ``TaggedValue`` and all ``TAG_*`` constants are removed from the codebase;
# column-type dispatch is now exclusively on
# ``schema.columns.field_type.code``.

from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rlib.rarithmetic import intmask
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types
from gnitz.core.values import make_payload_row


# ---------------------------------------------------------------------------
# Record header byte offsets
# ---------------------------------------------------------------------------

_HDR_PK_OFFSET     =  0   # 16 bytes: primary key
_HDR_WEIGHT_OFFSET = 16   #  8 bytes: algebraic weight
_HDR_NULL_OFFSET   = 24   #  8 bytes: null bitmap
_HDR_PAYLOAD_BASE  = 32   #  fixed payload starts here

_BLOB_REF_SIZE = 8         # bytes occupied by a string column in the fixed
                           # payload: 4-byte offset + 4-byte length

WAL_BLOCK_HEADER_SIZE = 32

# ---------------------------------------------------------------------------
# Buffer primitive readers
# ---------------------------------------------------------------------------

def _read_u8_raw(buf, offset):
    return r_uint64(ord(buf))


def _read_i8(buf, offset):
    raw = _read_u8_raw(buf, offset)
    if raw & 0x80:
        return r_int64(raw | (~r_uint64(0xFF)))  # sign-extend
    return r_int64(raw)


def _read_u8(buf, offset):
    return r_uint64(_read_u8_raw(buf, offset))


def _read_i16(buf, offset):
    raw = (r_uint64(_read_u8_raw(buf, offset)) |
           (r_uint64(_read_u8_raw(buf, offset + 1)) << 8))
    if raw & r_uint64(0x8000):
        return r_int64(intmask(raw | (~r_uint64(0xFFFF))))
    return r_int64(intmask(raw))


def _read_u16(buf, offset):
    return (r_uint64(_read_u8_raw(buf, offset)) |
            (r_uint64(_read_u8_raw(buf, offset + 1)) << 8))


def _read_i32(buf, offset):
    raw = (r_uint64(_read_u8_raw(buf, offset))        |
           (r_uint64(_read_u8_raw(buf, offset + 1)) << 8)  |
           (r_uint64(_read_u8_raw(buf, offset + 2)) << 16) |
           (r_uint64(_read_u8_raw(buf, offset + 3)) << 24))
    if raw & r_uint64(0x80000000):
        return r_int64(intmask(raw | (~r_uint64(0xFFFFFFFF))))
    return r_int64(intmask(raw))


def _read_u32(buf, offset):
    return (r_uint64(_read_u8_raw(buf, offset))        |
            (r_uint64(_read_u8_raw(buf, offset + 1)) << 8)  |
            (r_uint64(_read_u8_raw(buf, offset + 2)) << 16) |
            (r_uint64(_read_u8_raw(buf, offset + 3)) << 24))


def _read_i64(buf, offset):
    acc = r_uint64(0)
    for k in range(8):
        acc |= r_uint64(_read_u8_raw(buf, offset + k)) << r_uint64(k * 8)
    return r_int64(intmask(acc))


def _read_u64(buf, offset):
    acc = r_uint64(0)
    for k in range(8):
        acc |= r_uint64(_read_u8_raw(buf, offset + k)) << r_uint64(k * 8)
    return acc


def _read_u128(buf, offset):
    lo = r_uint128(_read_u64(buf, offset))
    hi = r_uint128(_read_u64(buf, offset + 8))
    return (hi << 64) | lo


def _read_f32(buf, offset):
    return float(rffi.cast(rffi.FLOATP, rffi.ptradd(buf, offset)))


def _read_f64(buf, offset):
    return longlong2float(_read_i64(buf, offset))


# ---------------------------------------------------------------------------
# Buffer primitive writers
# ---------------------------------------------------------------------------

def _write_u8(buf, offset, val_u64):
    buf = chr(intmask(val_u64 & r_uint64(0xFF)))


def _write_i64(buf, offset, val_i64):
    v = r_uint64(intmask(val_i64))
    for k in range(8):
        buf = chr(intmask((v >> r_uint64(k * 8)) & r_uint64(0xFF)))


def _write_u64(buf, offset, val_u64):
    for k in range(8):
        buf = chr(intmask((val_u64 >> r_uint64(k * 8)) & r_uint64(0xFF)))


def _write_u128(buf, offset, val_u128):
    _write_u64(buf, offset,     r_uint64(val_u128))
    _write_u64(buf, offset + 8, r_uint64(val_u128 >> 64))


def _write_f32(buf, offset, val_f64):
    rffi.cast(rffi.FLOATP, rffi.ptradd(buf, offset)) = rffi.cast(rffi.FLOAT, val_f64)


def _write_f64(buf, offset, val_f64):
    _write_i64(buf, offset, float2longlong(val_f64))


def _write_u32(buf, offset, val_u64):
    v = val_u64 & r_uint64(0xFFFFFFFF)
    for k in range(4):
        buf = chr(intmask((v >> r_uint64(k * 8)) & r_uint64(0xFF)))


def _write_u16(buf, offset, val_u64):
    buf     = chr(intmask(val_u64 & r_uint64(0xFF)))
    buf = chr(intmask((val_u64 >> 8) & r_uint64(0xFF)))


# ---------------------------------------------------------------------------
# String (blob) helpers
# ---------------------------------------------------------------------------

def _unpack_string(buf, col_offset, heap_base):
    blob_off = intmask(_read_u32(buf, col_offset))
    blob_len = intmask(_read_u32(buf, col_offset + 4))
    abs_off  = heap_base + blob_off
    chars =[]
    for k in range(blob_len):
        chars.append(buf)
    return "".join(chars)


def _pack_string_ref(buf, col_offset, blob_off_rel, blob_len):
    _write_u32(buf, col_offset,     r_uint64(blob_off_rel))
    _write_u32(buf, col_offset + 4, r_uint64(blob_len))


def _write_blob_bytes(buf, heap_base, blob_off_rel, val_str):
    abs_off = heap_base + blob_off_rel
    for k in range(len(val_str)):
        buf = val_str


# ---------------------------------------------------------------------------
# Record decoding
# ---------------------------------------------------------------------------

def decode_wal_record(schema, buf, base, heap_base):
    pk        = _read_u128(buf, base + _HDR_PK_OFFSET)
    weight    = _read_i64 (buf, base + _HDR_WEIGHT_OFFSET)
    null_word = _read_u64 (buf, base + _HDR_NULL_OFFSET)

    row = make_payload_row(schema)
    payload_col = 0

    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue

        col_type   = schema.columns.field_type
        col_offset = base + _HDR_PAYLOAD_BASE + schema.column_offsets

        if null_word & (r_uint64(1) << payload_col):
            row.append_null(payload_col)
            payload_col += 1
            continue

        if col_type.code == types.TYPE_I64.code:
            row.append_int(_read_i64(buf, col_offset))

        elif col_type.code == types.TYPE_I32.code:
            row.append_int(_read_i32(buf, col_offset))

        elif col_type.code == types.TYPE_I16.code:
            row.append_int(_read_i16(buf, col_offset))

        elif col_type.code == types.TYPE_I8.code:
            row.append_int(_read_i8(buf, col_offset))

        elif col_type.code == types.TYPE_U64.code:
            raw = _read_u64(buf, col_offset)
            row.append_int(r_int64(intmask(raw)))

        elif col_type.code == types.TYPE_U32.code:
            raw = _read_u32(buf, col_offset)
            row.append_int(r_int64(intmask(raw)))

        elif col_type.code == types.TYPE_U16.code:
            raw = _read_u16(buf, col_offset)
            row.append_int(r_int64(intmask(raw)))

        elif col_type.code == types.TYPE_U8.code:
            raw = _read_u8(buf, col_offset)
            row.append_int(r_int64(intmask(raw)))

        elif col_type.code == types.TYPE_F64.code:
            row.append_float(_read_f64(buf, col_offset))
            
        elif col_type.code == types.TYPE_F32.code:
            row.append_float(_read_f32(buf, col_offset))

        elif col_type.code == types.TYPE_STRING.code:
            s = _unpack_string(buf, col_offset, heap_base)
            row.append_string(s)

        elif col_type.code == types.TYPE_U128.code:
            lo = _read_u64(buf, col_offset)
            hi = _read_u64(buf, col_offset + 8)
            row.append_u128(lo, hi)

        else:
            row.append_int(_read_i64(buf, col_offset))

        payload_col += 1

    return pk, weight, row


# ---------------------------------------------------------------------------
# Record encoding
# ---------------------------------------------------------------------------

def write_wal_record(schema, pk, weight, row, buf, base, heap_base):
    _write_u128(buf, base + _HDR_PK_OFFSET,     pk)
    _write_i64 (buf, base + _HDR_WEIGHT_OFFSET, weight)
    _write_u64 (buf, base + _HDR_NULL_OFFSET,   row._null_word)

    heap_cursor = 0
    payload_col = 0

    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue

        col_type   = schema.columns.field_type
        col_offset = base + _HDR_PAYLOAD_BASE + schema.column_offsets

        if row.is_null(payload_col):
            if col_type.code == types.TYPE_U128.code:
                _write_u64(buf, col_offset,     r_uint64(0))
                _write_u64(buf, col_offset + 8, r_uint64(0))
            elif col_type.code == types.TYPE_STRING.code:
                _write_u32(buf, col_offset,     r_uint64(0))
                _write_u32(buf, col_offset + 4, r_uint64(0))
            else:
                _write_u64(buf, col_offset, r_uint64(0))
            payload_col += 1
            continue

        if col_type.code == types.TYPE_STRING.code:
            s_val = row.get_str(payload_col)
            blob_off_rel = heap_cursor
            blob_len     = len(s_val)
            _pack_string_ref(buf, col_offset, blob_off_rel, blob_len)
            _write_blob_bytes(buf, heap_base, blob_off_rel, s_val)
            heap_cursor = blob_off_rel + blob_len

        elif col_type.code == types.TYPE_F64.code:
            _write_f64(buf, col_offset, row.get_float(payload_col))
            
        elif col_type.code == types.TYPE_F32.code:
            _write_f32(buf, col_offset, row.get_float(payload_col))

        elif col_type.code == types.TYPE_U128.code:
            v = row.get_u128(payload_col)
            _write_u64(buf, col_offset,     r_uint64(v))
            _write_u64(buf, col_offset + 8, r_uint64(v >> 64))

        elif col_type.code == types.TYPE_U64.code:
            _write_u64(buf, col_offset, row.get_int(payload_col))

        elif col_type.code == types.TYPE_U32.code:
            _write_u32(buf, col_offset, row.get_int(payload_col))

        elif col_type.code == types.TYPE_U16.code:
            _write_u16(buf, col_offset, row.get_int(payload_col))

        elif col_type.code == types.TYPE_U8.code:
            _write_u8(buf, col_offset, row.get_int(payload_col))

        else:
            _write_i64(buf, col_offset, row.get_int_signed(payload_col))

        payload_col += 1


# ---------------------------------------------------------------------------
# Block serialization & Domain classes
# ---------------------------------------------------------------------------

class WALRecord(object):
    def __init__(self, key, weight, component_data):
        self.key = key
        self.weight = weight
        self.component_data = component_data

    def get_key(self):
        return self.key


class WALBlock(object):
    def __init__(self, lsn, tid, records):
        self.lsn = lsn
        self.tid = tid
        self.records = records


def compute_record_size(schema, row):
    fixed_size = _HDR_PAYLOAD_BASE + schema.memtable_stride
    heap_size = 0
    payload_col = 0
    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue
        col_type = schema.columns.field_type
        if col_type.code == types.TYPE_STRING.code:
            if not row.is_null(payload_col):
                heap_size += len(row.get_str(payload_col))
        payload_col += 1
    return fixed_size, heap_size


def write_wal_block(fd, lsn, table_id, records, schema):
    import os as _os
    entry_count = len(records)
    total_size = WAL_BLOCK_HEADER_SIZE
    
    record_sizes =[]
    for rec in records:
        f_sz, h_sz = compute_record_size(schema, rec.component_data)
        record_sizes.append((f_sz, h_sz))
        total_size += f_sz + h_sz
        
    buf = lltype.malloc(rffi.CCHARP.TO, total_size, flavor='raw')
    try:
        _write_u64(buf, 0, r_uint64(lsn))
        _write_u32(buf, 8, r_uint64(table_id))
        _write_u32(buf, 12, r_uint64(entry_count))
        _write_u64(buf, 16, r_uint64(total_size))
        
        current_offset = WAL_BLOCK_HEADER_SIZE
        for i in range(entry_count):
            rec = records
            f_sz, h_sz = record_sizes
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
            
        from gnitz.core import xxh
        body_ptr = rffi.ptradd(buf, WAL_BLOCK_HEADER_SIZE)
        body_size = total_size - WAL_BLOCK_HEADER_SIZE
        if body_size > 0:
            cs = xxh.compute_checksum(body_ptr, body_size)
            _write_u64(buf, 24, cs)
        else:
            _write_u64(buf, 24, r_uint64(0))
            
        written = 0
        while written < total_size:
            ptr = rffi.ptradd(buf, written)
            chunk = rffi.cast(rffi.CCHARP, ptr)
            n = _os.write(fd, rffi.charpsize2str(chunk, total_size - written))
            if n <= 0:
                raise IOError("WAL write failed")
            written += n
    finally:
        lltype.free(buf, flavor='raw')


def decode_wal_block(buf, total_size, schema):
    lsn = _read_u64(buf, 0)
    tid = intmask(_read_u32(buf, 8))
    entry_count = intmask(_read_u32(buf, 12))
    
    from gnitz.core import xxh
    body_ptr = rffi.ptradd(buf, WAL_BLOCK_HEADER_SIZE)
    body_size = total_size - WAL_BLOCK_HEADER_SIZE
    expected_cs = _read_u64(buf, 24)
    
    if body_size > 0:
        actual_cs = xxh.compute_checksum(body_ptr, body_size)
        if actual_cs != expected_cs:
            from gnitz.core import errors
            raise errors.CorruptShardError("WAL block checksum mismatch")
            
    records =[]
    current_offset = WAL_BLOCK_HEADER_SIZE
    for i in range(entry_count):
        f_sz = _HDR_PAYLOAD_BASE + schema.memtable_stride
        heap_base = current_offset + f_sz
        
        pk, weight, row = decode_wal_record(schema, buf, current_offset, heap_base)
        rec = WALRecord(pk, weight, row)
        records.append(rec)
        
        _, h_sz = compute_record_size(schema, row)
        current_offset += f_sz + h_sz
        
    return WALBlock(lsn, tid, records)
