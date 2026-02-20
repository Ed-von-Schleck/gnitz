# gnitz/storage/wal_format.py
#
# WAL (Write-Ahead Log) serialisation and deserialisation.
#
# Both directions of the codec are updated to work with ``PayloadRow``
# directly, replacing the former ``List[TaggedValue]`` representation.
# ``TaggedValue`` and all ``TAG_*`` constants are removed from the codebase;
# column-type dispatch is now exclusively on
# ``schema.columns[i].field_type.code``.
#
# Wire format of a single WAL record
# ------------------------------------
# All multi-byte integers are little-endian.
#
#   Offset   Size   Field
#   ------   ----   -----
#        0     16   pk  (r_uint128, LE)
#       16      8   weight  (r_int64, LE)
#       24      8   null_word  (r_uint64, LE)
#                   Bit N of null_word is set when payload column N is null.
#                   (Payload column indices skip the PK column.)
#       32      *   Fixed-width payload section.
#                   Each column occupies exactly ``schema.column_sizes[i]``
#                   bytes at absolute position
#                   ``base + _HDR_PAYLOAD_BASE + schema.column_offsets[i]``.
#                   String columns store an 8-byte blob-reference
#                   (4-byte LE offset + 4-byte LE length) relative to
#                   heap_base.
#       ???      *  Heap section (string blob bytes, variable length).
#                   ``heap_base`` is provided by the caller as an absolute
#                   byte offset into ``buf``; relative blob offsets within
#                   blob-references are measured from this base.
#
# The PK column is not written in the payload section; it is the 16-byte
# header prefix.

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


# ---------------------------------------------------------------------------
# Buffer primitive readers
# ---------------------------------------------------------------------------
# All readers take an ``rffi.CCHARP`` buffer and an **absolute** byte offset.
# No bounds checking is performed; callers guarantee buffer sufficiency.

def _read_u8_raw(buf, offset):
    return rffi.cast(lltype.Unsigned, buf[offset])


def _read_i8(buf, offset):
    """Signed 8-bit integer."""
    raw = _read_u8_raw(buf, offset)
    if raw & 0x80:
        return r_int64(raw | (~r_uint64(0xFF)))  # sign-extend
    return r_int64(raw)


def _read_u8(buf, offset):
    return r_uint64(_read_u8_raw(buf, offset))


def _read_i16(buf, offset):
    """Signed little-endian 16-bit integer."""
    raw = (r_uint64(_read_u8_raw(buf, offset)) |
           (r_uint64(_read_u8_raw(buf, offset + 1)) << 8))
    if raw & r_uint64(0x8000):
        return r_int64(intmask(raw | (~r_uint64(0xFFFF))))
    return r_int64(intmask(raw))


def _read_u16(buf, offset):
    return (r_uint64(_read_u8_raw(buf, offset)) |
            (r_uint64(_read_u8_raw(buf, offset + 1)) << 8))


def _read_i32(buf, offset):
    """Signed little-endian 32-bit integer."""
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
    """Signed little-endian 64-bit integer."""
    acc = r_uint64(0)
    for k in range(8):
        acc |= r_uint64(_read_u8_raw(buf, offset + k)) << r_uint64(k * 8)
    return r_int64(intmask(acc))


def _read_u64(buf, offset):
    """Unsigned little-endian 64-bit integer."""
    acc = r_uint64(0)
    for k in range(8):
        acc |= r_uint64(_read_u8_raw(buf, offset + k)) << r_uint64(k * 8)
    return acc


def _read_u128(buf, offset):
    lo = r_uint128(_read_u64(buf, offset))
    hi = r_uint128(_read_u64(buf, offset + 8))
    return (hi << 64) | lo


def _read_f64(buf, offset):
    """
    Read a little-endian IEEE 754 double.

    Uses ``longlong2float`` (a bit-level reinterpret cast) to reconstruct the
    float without value conversion.  Do NOT use ``rffi.cast(rffi.DOUBLE, ...)``
    on an integer — that produces a C value cast that silently corrupts the
    result.
    """
    return longlong2float(_read_i64(buf, offset))


# ---------------------------------------------------------------------------
# Buffer primitive writers
# ---------------------------------------------------------------------------

def _write_u8(buf, offset, val_u64):
    buf[offset] = rffi.cast(rffi.UCHAR, val_u64 & r_uint64(0xFF))


def _write_i64(buf, offset, val_i64):
    v = r_uint64(intmask(val_i64))
    for k in range(8):
        buf[offset + k] = rffi.cast(rffi.UCHAR,
                                    (v >> r_uint64(k * 8)) & r_uint64(0xFF))


def _write_u64(buf, offset, val_u64):
    for k in range(8):
        buf[offset + k] = rffi.cast(rffi.UCHAR,
                                    (val_u64 >> r_uint64(k * 8)) & r_uint64(0xFF))


def _write_u128(buf, offset, val_u128):
    _write_u64(buf, offset,     r_uint64(val_u128))
    _write_u64(buf, offset + 8, r_uint64(val_u128 >> 64))


def _write_f64(buf, offset, val_f64):
    """
    Write a float using a bit-level reinterpret cast (``float2longlong``).

    This is the lossless inverse of ``_read_f64``.  Never use
    ``rffi.cast(rffi.LONGLONG, val_f64)`` — that is a C value cast that
    silently truncates the fractional part.
    """
    _write_i64(buf, offset, float2longlong(val_f64))


def _write_u32(buf, offset, val_u64):
    v = val_u64 & r_uint64(0xFFFFFFFF)
    for k in range(4):
        buf[offset + k] = rffi.cast(rffi.UCHAR,
                                    (v >> r_uint64(k * 8)) & r_uint64(0xFF))


def _write_u16(buf, offset, val_u64):
    buf[offset]     = rffi.cast(rffi.UCHAR, val_u64 & r_uint64(0xFF))
    buf[offset + 1] = rffi.cast(rffi.UCHAR, (val_u64 >> 8) & r_uint64(0xFF))


# ---------------------------------------------------------------------------
# String (blob) helpers
# ---------------------------------------------------------------------------
# String columns in the fixed payload store an 8-byte blob-reference:
#   bytes 0-3: 32-bit LE blob offset (relative to heap_base)
#   bytes 4-7: 32-bit LE blob length in bytes
# The actual UTF-8 blob bytes live in the heap section.

def _unpack_string(buf, col_offset, heap_base):
    """
    Read the string stored at the blob-reference at ``col_offset``.

    ``heap_base`` is the absolute byte offset of the heap section in ``buf``.
    """
    blob_off = intmask(_read_u32(buf, col_offset))
    blob_len = intmask(_read_u32(buf, col_offset + 4))
    abs_off  = heap_base + blob_off
    chars = []
    for k in range(blob_len):
        chars.append(chr(rffi.cast(lltype.Signed, buf[abs_off + k])))
    return "".join(chars)


def _pack_string_ref(buf, col_offset, blob_off_rel, blob_len):
    """Write the 8-byte blob-reference into the fixed payload area."""
    _write_u32(buf, col_offset,     r_uint64(blob_off_rel))
    _write_u32(buf, col_offset + 4, r_uint64(blob_len))


def _write_blob_bytes(buf, heap_base, blob_off_rel, val_str):
    """Write the raw UTF-8 bytes of ``val_str`` into the heap area."""
    abs_off = heap_base + blob_off_rel
    for k in range(len(val_str)):
        buf[abs_off + k] = rffi.cast(rffi.UCHAR, ord(val_str[k]))


# ---------------------------------------------------------------------------
# decode_wal_block
# ---------------------------------------------------------------------------

def decode_wal_block(schema, buf, base, heap_base):
    """
    Deserialise one WAL record into a ``(pk, weight, row)`` triple.

    Parameters
    ----------
    schema : TableSchema
        The table schema for this record.
    buf : rffi.CCHARP
        Raw WAL buffer.
    base : int
        Absolute byte offset of the record's first byte within ``buf``.
    heap_base : int
        Absolute byte offset of the record's heap section (string blobs)
        within ``buf``.

    Returns
    -------
    pk : r_uint128
    weight : r_int64
    row : PayloadRow
    """
    pk        = _read_u128(buf, base + _HDR_PK_OFFSET)
    weight    = _read_i64 (buf, base + _HDR_WEIGHT_OFFSET)
    null_word = _read_u64 (buf, base + _HDR_NULL_OFFSET)

    row = make_payload_row(schema)
    payload_col = 0

    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue

        col_type   = schema.columns[i].field_type
        col_offset = base + _HDR_PAYLOAD_BASE + schema.column_offsets[i]

        # If this payload column is null, push a null sentinel and skip
        # decoding the (always-zero) wire bytes for that slot.
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

        elif col_type.code == types.TYPE_STRING.code:
            s = _unpack_string(buf, col_offset, heap_base)
            row.append_string(s)

        elif col_type.code == types.TYPE_U128.code:
            lo = _read_u64(buf, col_offset)
            hi = _read_u64(buf, col_offset + 8)
            row.append_u128(lo, hi)

        else:
            # Unknown column type: fall back to raw i64 to avoid data loss.
            row.append_int(_read_i64(buf, col_offset))

        payload_col += 1

    return pk, weight, row


# ---------------------------------------------------------------------------
# write_wal_block
# ---------------------------------------------------------------------------

def write_wal_block(schema, pk, weight, row, buf, base, heap_base):
    """
    Serialise a ``(pk, weight, row)`` triple into a WAL record.

    Parameters
    ----------
    schema : TableSchema
        The table schema for this record.
    pk : r_uint128
        Primary key value.
    weight : r_int64
        Algebraic weight (positive for insertions, negative for deletions).
    row : PayloadRow
        Non-PK column values in schema column order (PK column excluded from
        ``row``'s arrays, consistent with ``PayloadRow``'s layout contract).
    buf : rffi.CCHARP
        Destination buffer, pre-allocated by the caller.
    base : int
        Absolute byte offset in ``buf`` at which to write this record.
    heap_base : int
        Absolute byte offset of the heap section for this record.  String
        blob bytes are written starting here; blob offsets stored in the
        fixed payload are relative to this base.
    """
    # Header
    _write_u128(buf, base + _HDR_PK_OFFSET,     pk)
    _write_i64 (buf, base + _HDR_WEIGHT_OFFSET, weight)
    # Null bitmap: read directly from the PayloadRow scalar field.
    # This field is always present in the object layout (8 bytes), regardless
    # of _has_nullable; for non-nullable schemas it is always r_uint64(0).
    _write_u64 (buf, base + _HDR_NULL_OFFSET,   row._null_word)

    # Payload
    # heap_cursor: relative byte offset into the heap section, advanced as
    # string blobs are appended.  Stored in a one-element list so that the
    # inner write helpers can mutate it in-place (RPython limitation: no
    # mutable integer closures, and multi-return from helpers called in loops
    # requires boxing).
    heap_cursor = [0]
    payload_col = 0

    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue

        col_type   = schema.columns[i].field_type
        col_offset = base + _HDR_PAYLOAD_BASE + schema.column_offsets[i]

        # Null columns: write zero bytes (null bitmap carries the semantic;
        # zero bytes keep the fixed-width layout intact).
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
            blob_off_rel = heap_cursor[0]
            blob_len     = len(s_val)
            _pack_string_ref(buf, col_offset, blob_off_rel, blob_len)
            _write_blob_bytes(buf, heap_base, blob_off_rel, s_val)
            heap_cursor[0] = blob_off_rel + blob_len

        elif col_type.code == types.TYPE_F64.code:
            _write_f64(buf, col_offset, row.get_float(payload_col))

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
            # Signed integers (i8/i16/i32/i64): all stored as r_int64 in _lo.
            # Write the full 8-byte slot; the schema's fixed column width
            # determines how many bytes the reader will consume.
            _write_i64(buf, col_offset, row.get_int_signed(payload_col))

        payload_col += 1
