"""Low-level IPC helpers for testing failure modes."""

import struct

from gnitz_client import transport
from gnitz_client.protocol import (
    MAGIC_V2, HEADER_SIZE, ALIGNMENT, STATUS_OK, STATUS_ERROR,
    IPC_STRING_STRIDE, IPC_NULL_STRING_OFFSET,
    META_FLAG_NULLABLE, META_FLAG_IS_PK,
    align_up, pack_header, unpack_header,
)
from gnitz_client.types import (
    TypeCode, ColumnDef, Schema, META_SCHEMA, TYPE_STRIDES,
    SCHEMA_TAB,
)
from gnitz_client.batch import (
    ZSetBatch, encode_zset_section, schema_to_batch,
)


class RawIpcClient:
    """Low-level IPC client for sending raw/malformed messages."""

    def __init__(self, socket_path):
        self.socket_path = socket_path
        self.sock = transport.connect(socket_path)

    def send_raw(self, data: bytes):
        transport.send_memfd(self.sock, data)

    def recv_raw(self) -> bytes:
        return transport.recv_memfd(self.sock)

    def recv_response(self) -> tuple[dict, bytes]:
        data = self.recv_raw()
        hdr = unpack_header(data)
        return hdr, data

    def send_and_recv(self, data: bytes) -> tuple[dict, bytes]:
        self.send_raw(data)
        return self.recv_response()

    def reconnect(self):
        try:
            self.sock.close()
        except OSError:
            pass
        self.sock = transport.connect(self.socket_path)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def get_error_message(data: bytes) -> str:
    """Extract error message string from a STATUS_ERROR response."""
    hdr = unpack_header(data)
    if hdr["status"] != STATUS_ERROR:
        return ""
    err_len = hdr["err_len"]
    if err_len == 0:
        return ""
    return data[HEADER_SIZE:HEADER_SIZE + err_len].decode("utf-8")


def assert_error_contains(hdr, data, *substrings):
    """Assert response is STATUS_ERROR and message contains all substrings."""
    assert hdr["status"] == STATUS_ERROR, (
        f"Expected STATUS_ERROR, got status={hdr['status']}"
    )
    msg = get_error_message(data)
    for s in substrings:
        assert s.lower() in msg.lower(), (
            f"Expected error containing {s!r}, got: {msg!r}"
        )


def assert_ok(hdr):
    """Assert response is STATUS_OK."""
    assert hdr["status"] == STATUS_OK, (
        f"Expected STATUS_OK, got status={hdr['status']}"
    )


def make_scan_message(target_id=SCHEMA_TAB, **header_overrides):
    """Build a minimal valid scan message (header only, 96 bytes)."""
    return pack_header(target_id=target_id, **header_overrides)


def build_schema_section(
    columns,
    pk_index=0,
    type_code_overrides=None,
    flag_overrides=None,
    col_idx_overrides=None,
):
    """Build a META_SCHEMA ZSet section.

    Returns (section_bytes, blob_sz, row_count).

    Override params let you inject defects:
      type_code_overrides: {col_index: bad_type_code}
      flag_overrides: {col_index: flags_value}
      col_idx_overrides: {col_index: wrong_col_idx}
    """
    schema = Schema(columns=columns, pk_index=pk_index)
    batch = schema_to_batch(schema)

    if type_code_overrides:
        for i, tc in type_code_overrides.items():
            batch.columns[1][i] = tc
    if flag_overrides:
        for i, f in flag_overrides.items():
            batch.columns[2][i] = f
    if col_idx_overrides:
        for i, idx in col_idx_overrides.items():
            batch.pk_lo[i] = idx

    section_bytes, blob_sz = encode_zset_section(META_SCHEMA, batch)
    return section_bytes, blob_sz, len(batch.pk_lo)


def build_data_section(schema, rows):
    """Build a data ZSet section from row data.

    rows: list of (pk_lo, weight, col_values)
      where col_values is a list matching schema.columns order
      (pk column entry is ignored).
    Returns (section_bytes, blob_sz).
    """
    batch = ZSetBatch(schema=schema)
    cols = [[] for _ in schema.columns]

    for pk, weight, values in rows:
        batch.pk_lo.append(pk)
        batch.pk_hi.append(0)
        batch.weights.append(weight)
        batch.nulls.append(0)
        for ci in range(len(schema.columns)):
            if ci == schema.pk_index:
                cols[ci].append(None)
            else:
                cols[ci].append(values[ci])

    batch.columns = cols
    return encode_zset_section(schema, batch)


def assemble_message(
    target_id,
    schema_section=None,
    schema_count=0,
    schema_blob_sz=0,
    data_section=None,
    data_count=0,
    data_blob_sz=0,
    data_pk_index=0,
    err_bytes=b"",
    **header_overrides,
):
    """Assemble a complete IPC message from pre-encoded components."""
    err_len = len(err_bytes)

    hdr = pack_header(
        target_id=target_id,
        err_len=err_len,
        schema_count=schema_count,
        schema_blob_sz=schema_blob_sz,
        data_count=data_count,
        data_blob_sz=data_blob_sz,
        data_pk_index=data_pk_index,
        **header_overrides,
    )

    body_start = align_up(HEADER_SIZE + err_len, ALIGNMENT)
    err_padding = body_start - HEADER_SIZE - err_len

    msg = hdr + err_bytes + b"\x00" * err_padding

    if schema_section:
        if data_section:
            data_start = align_up(body_start + len(schema_section), ALIGNMENT)
            schema_pad = data_start - body_start - len(schema_section)
            msg += schema_section + b"\x00" * schema_pad + data_section
        else:
            msg += schema_section
    elif data_section:
        msg += data_section

    return msg


# Commonly used test schemas

TWO_COL_SCHEMA = Schema(
    columns=[ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)],
    pk_index=0,
)

THREE_COL_SCHEMA = Schema(
    columns=[
        ColumnDef("pk", TypeCode.U64),
        ColumnDef("val", TypeCode.I64),
        ColumnDef("name", TypeCode.STRING),
    ],
    pk_index=0,
)
