"""ZSet batch encoding/decoding for IPC v2 wire format."""

import struct
from dataclasses import dataclass, field as datafield

from gnitz_client.protocol import (
    ALIGNMENT, IPC_STRING_STRIDE, IPC_NULL_STRING_OFFSET,
    META_FLAG_NULLABLE, META_FLAG_IS_PK, align_up,
)
from gnitz_client.types import (
    TypeCode, ColumnDef, Schema, META_SCHEMA, TYPE_STRIDES, TYPE_STRUCT_FMT,
)


@dataclass
class ZSetBatch:
    schema: Schema
    pk_lo: list[int] = datafield(default_factory=list)
    pk_hi: list[int] = datafield(default_factory=list)
    weights: list[int] = datafield(default_factory=list)
    nulls: list[int] = datafield(default_factory=list)
    columns: list[list] = datafield(default_factory=list)


def _walk_layout(schema: Schema, count: int):
    """Walk the ZSet wire layout, returning offset info for each section."""
    cur = 0
    struct_sz = count * 8

    pk_lo_off = cur
    cur = align_up(cur + struct_sz, ALIGNMENT)
    pk_hi_off = cur
    cur = align_up(cur + struct_sz, ALIGNMENT)
    weight_off = cur
    cur = align_up(cur + struct_sz, ALIGNMENT)
    null_off = cur
    cur = align_up(cur + struct_sz, ALIGNMENT)

    col_offsets = []
    col_strides = []

    for ci, col in enumerate(schema.columns):
        if ci == schema.pk_index:
            col_offsets.append(0)
            col_strides.append(0)
        elif col.type_code == TypeCode.STRING:
            col_offsets.append(cur)
            col_strides.append(IPC_STRING_STRIDE)
            cur = align_up(cur + count * IPC_STRING_STRIDE, ALIGNMENT)
        else:
            stride = TYPE_STRIDES[col.type_code]
            col_offsets.append(cur)
            col_strides.append(stride)
            cur = align_up(cur + stride * count, ALIGNMENT)

    blob_offset = cur
    return pk_lo_off, pk_hi_off, weight_off, null_off, col_offsets, col_strides, blob_offset


def encode_zset_section(schema: Schema, batch: ZSetBatch) -> tuple[bytes, int]:
    """Encode a ZSetBatch into wire bytes. Returns (data, total_blob_bytes)."""
    count = len(batch.pk_lo)
    if count == 0:
        return b"", 0

    (pk_lo_off, pk_hi_off, weight_off, null_off,
     col_offsets, col_strides, blob_offset) = _walk_layout(schema, count)

    # Collect blob bytes from string columns
    blob_parts: list[bytes] = []
    blob_total = 0
    # Pre-compute string column blob data
    string_blobs: dict[int, list[tuple[int, int]]] = {}  # ci -> [(blob_offset, length)]
    for ci, col in enumerate(schema.columns):
        if ci == schema.pk_index:
            continue
        if col.type_code == TypeCode.STRING:
            entries = []
            for row in range(count):
                val = batch.columns[ci][row]
                if val is None:
                    entries.append((IPC_NULL_STRING_OFFSET, 0))
                else:
                    s = val.encode("utf-8") if isinstance(val, str) else val
                    entries.append((blob_total, len(s)))
                    blob_parts.append(s)
                    blob_total += len(s)
            string_blobs[ci] = entries

    total_size = blob_offset + blob_total
    buf = bytearray(total_size)

    # Structural buffers
    for row in range(count):
        struct.pack_into("<Q", buf, pk_lo_off + row * 8, batch.pk_lo[row])
        struct.pack_into("<Q", buf, pk_hi_off + row * 8, batch.pk_hi[row])
        struct.pack_into("<q", buf, weight_off + row * 8, batch.weights[row])
        struct.pack_into("<Q", buf, null_off + row * 8, batch.nulls[row])

    # Payload columns
    for ci, col in enumerate(schema.columns):
        if ci == schema.pk_index:
            continue

        off = col_offsets[ci]
        if col.type_code == TypeCode.STRING:
            entries = string_blobs[ci]
            for row in range(count):
                o, l = entries[row]
                struct.pack_into("<II", buf, off + row * IPC_STRING_STRIDE, o, l)
        elif col.type_code == TypeCode.U128:
            for row in range(count):
                val = batch.columns[ci][row]
                struct.pack_into("<QQ", buf, off + row * 16, val & 0xFFFFFFFFFFFFFFFF, val >> 64)
        else:
            fmt = TYPE_STRUCT_FMT[col.type_code]
            stride = TYPE_STRIDES[col.type_code]
            for row in range(count):
                struct.pack_into("<" + fmt, buf, off + row * stride, batch.columns[ci][row])

    # Blob arena
    blob_pos = blob_offset
    for part in blob_parts:
        buf[blob_pos:blob_pos + len(part)] = part
        blob_pos += len(part)

    return bytes(buf), blob_total


def decode_zset_section(
    data: bytes, offset: int, schema: Schema, count: int, blob_sz: int,
) -> ZSetBatch:
    """Decode a ZSet section from wire bytes at the given offset."""
    if count == 0:
        cols = [[] for _ in schema.columns]
        return ZSetBatch(schema=schema, pk_lo=[], pk_hi=[], weights=[], nulls=[], columns=cols)

    (pk_lo_off, pk_hi_off, weight_off, null_off,
     col_offsets, col_strides, blob_offset) = _walk_layout(schema, count)

    base = offset

    pk_lo = [struct.unpack_from("<Q", data, base + pk_lo_off + r * 8)[0] for r in range(count)]
    pk_hi = [struct.unpack_from("<Q", data, base + pk_hi_off + r * 8)[0] for r in range(count)]
    weights = [struct.unpack_from("<q", data, base + weight_off + r * 8)[0] for r in range(count)]
    nulls = [struct.unpack_from("<Q", data, base + null_off + r * 8)[0] for r in range(count)]

    columns: list[list] = []
    for ci, col in enumerate(schema.columns):
        if ci == schema.pk_index:
            columns.append([])  # PK data is in pk_lo/pk_hi
            continue

        off = col_offsets[ci]
        col_data: list = []

        # Compute payload_idx for null bitmap check
        payload_idx = ci if ci < schema.pk_index else ci - 1

        if col.type_code == TypeCode.STRING:
            blob_base = base + blob_offset
            for row in range(count):
                # Check null bitmap
                is_null = bool(nulls[row] & (1 << payload_idx))
                str_off, str_len = struct.unpack_from(
                    "<II", data, base + off + row * IPC_STRING_STRIDE
                )
                if is_null or str_off == IPC_NULL_STRING_OFFSET:
                    col_data.append(None)
                else:
                    col_data.append(data[blob_base + str_off:blob_base + str_off + str_len].decode("utf-8"))
        elif col.type_code == TypeCode.U128:
            for row in range(count):
                lo, hi = struct.unpack_from("<QQ", data, base + off + row * 16)
                col_data.append(lo | (hi << 64))
        else:
            fmt = TYPE_STRUCT_FMT[col.type_code]
            stride = TYPE_STRIDES[col.type_code]
            for row in range(count):
                val = struct.unpack_from("<" + fmt, data, base + off + row * stride)[0]
                col_data.append(val)

        columns.append(col_data)

    return ZSetBatch(schema=schema, pk_lo=pk_lo, pk_hi=pk_hi, weights=weights, nulls=nulls, columns=columns)


def schema_to_batch(schema: Schema) -> ZSetBatch:
    """Convert a Schema into a META_SCHEMA batch (one row per column)."""
    pk_lo = []
    pk_hi = []
    weights = []
    nulls = []
    # META_SCHEMA columns: col_idx(pk), type_code, flags, name
    col_idx_col: list = []  # pk column, will be empty
    type_code_col: list = []
    flags_col: list = []
    name_col: list = []

    for ci, col in enumerate(schema.columns):
        pk_lo.append(ci)
        pk_hi.append(0)
        weights.append(1)
        nulls.append(0)

        col_idx_col.append(None)  # pk column placeholder
        type_code_col.append(col.type_code)
        flags = 0
        if col.is_nullable:
            flags |= META_FLAG_NULLABLE
        if ci == schema.pk_index:
            flags |= META_FLAG_IS_PK
        flags_col.append(flags)
        name_col.append(col.name)

    return ZSetBatch(
        schema=META_SCHEMA,
        pk_lo=pk_lo,
        pk_hi=pk_hi,
        weights=weights,
        nulls=nulls,
        columns=[col_idx_col, type_code_col, flags_col, name_col],
    )


def batch_to_schema(batch: ZSetBatch) -> Schema:
    """Reconstruct a Schema from a META_SCHEMA batch."""
    count = len(batch.pk_lo)
    columns = []
    pk_index = -1

    for i in range(count):
        col_idx = batch.pk_lo[i]  # PK is col_idx
        type_code = batch.columns[1][i]
        flags = batch.columns[2][i]
        name = batch.columns[3][i]

        if col_idx != i:
            raise ValueError(f"Schema batch col_idx out of order: expected {i}, got {col_idx}")

        is_nullable = bool(flags & META_FLAG_NULLABLE)
        is_pk = bool(flags & META_FLAG_IS_PK)

        columns.append(ColumnDef(name=name, type_code=type_code, is_nullable=is_nullable))
        if is_pk:
            pk_index = i

    if pk_index < 0:
        raise ValueError("No PK column found in schema batch")

    return Schema(columns=columns, pk_index=pk_index)
