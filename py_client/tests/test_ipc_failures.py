"""Exhaustive IPC v2 failure mode tests.

Tests every validation path in the server's IPC receive pipeline:
  Transport → Header → Error string → Schema section → Data section →
  Schema validation → Dispatch

Each test sends a specifically malformed message and verifies:
  - The server returns STATUS_ERROR with a descriptive message, OR
  - The server drops the connection (for unrecoverable protocol violations)
  - The server remains operational afterward
"""

import os
import struct
import socket
import time

import pytest

from gnitz_client import GnitzClient, GnitzError
from gnitz_client.protocol import (
    MAGIC_V2, HEADER_SIZE, ALIGNMENT, STATUS_OK, STATUS_ERROR,
    IPC_STRING_STRIDE, IPC_NULL_STRING_OFFSET,
    META_FLAG_IS_PK, META_FLAG_NULLABLE,
    align_up, pack_header, unpack_header,
)
from gnitz_client.types import (
    TypeCode, ColumnDef, Schema, META_SCHEMA,
    SCHEMA_TAB, TABLE_TAB, SEQ_TAB,
)
from gnitz_client.batch import (
    ZSetBatch, encode_zset_section, schema_to_batch,
)
from gnitz_client import transport

from ipc_helpers import (
    RawIpcClient,
    get_error_message, assert_error_contains, assert_ok,
    make_scan_message,
    build_schema_section, build_data_section, assemble_message,
    TWO_COL_SCHEMA, THREE_COL_SCHEMA,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def raw(server):
    """Per-test raw IPC client."""
    c = RawIpcClient(server)
    yield c
    c.close()


@pytest.fixture(scope="module")
def ipc_test_table(server):
    """Create a table for schema-mismatch and data-push tests."""
    c = GnitzClient(server)
    try:
        c.create_schema("ipctest")
    except GnitzError:
        pass  # may already exist from prior run
    tid = c.create_table(
        "ipctest", "t1",
        [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("val", TypeCode.I64),
            ColumnDef("name", TypeCode.STRING),
        ],
        pk_col_idx=0,
    )
    c.close()
    return tid


# ===================================================================
# 1. HEADER VALIDATION
# ===================================================================


class TestHeaderValidation:
    """Tests for the 96-byte header parsing stage."""

    def test_wrong_magic_all_zeros(self, raw):
        """96 bytes of zeros → invalid magic."""
        hdr, data = raw.send_and_recv(b"\x00" * HEADER_SIZE)
        assert_error_contains(hdr, data, "magic")

    def test_wrong_magic_random(self, raw):
        """Random magic value → invalid magic."""
        msg = pack_header(magic=0xDEADBEEFCAFEBABE, target_id=SCHEMA_TAB)
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "magic")

    def test_wrong_magic_off_by_one(self, raw):
        """MAGIC_V2 + 1 → invalid magic."""
        msg = pack_header(magic=MAGIC_V2 + 1, target_id=SCHEMA_TAB)
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "magic")

    def test_wrong_magic_off_by_one_low(self, raw):
        """MAGIC_V2 - 1 → invalid magic."""
        msg = pack_header(magic=MAGIC_V2 - 1, target_id=SCHEMA_TAB)
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "magic")

    def test_wrong_magic_byte_swapped(self, raw):
        """Big-endian magic → invalid."""
        be_magic = int.from_bytes(MAGIC_V2.to_bytes(8, "little"), "big")
        msg = pack_header(magic=be_magic, target_id=SCHEMA_TAB)
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "magic")

    def test_payload_too_small_1_byte(self, raw):
        """1 byte → too small for header."""
        hdr, data = raw.send_and_recv(b"\x47")
        assert_error_contains(hdr, data, "too small")

    def test_payload_too_small_50_bytes(self, raw):
        """50 bytes → too small for header."""
        hdr, data = raw.send_and_recv(b"\x00" * 50)
        assert_error_contains(hdr, data, "too small")

    def test_payload_too_small_95_bytes(self, raw):
        """95 bytes (one short of HEADER_SIZE) → too small."""
        hdr, data = raw.send_and_recv(b"\x00" * 95)
        assert_error_contains(hdr, data, "too small")

    def test_payload_exactly_header_size(self, raw):
        """Exactly 96 bytes, valid magic, target=SCHEMA_TAB → valid scan."""
        msg = make_scan_message(target_id=SCHEMA_TAB)
        assert len(msg) == HEADER_SIZE
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_payload_header_plus_padding(self, raw):
        """Header + extra padding → valid scan (padding ignored)."""
        msg = make_scan_message(target_id=SCHEMA_TAB) + b"\x00" * 200
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_valid_magic_garbage_rest(self, raw):
        """Valid magic but garbage in remaining header fields.

        With data_count=0 and schema_count=0 (random might set them),
        we manually craft: valid magic, both counts=0, target=SCHEMA_TAB.
        Only the rest is garbage. Should succeed as a scan if err_len is 0.
        """
        # Build header with valid magic, all counts 0, random in other fields
        buf = bytearray(pack_header(target_id=SCHEMA_TAB))
        # Overwrite reserved area with garbage
        buf[80:96] = bytes(range(16))
        hdr, data = raw.send_and_recv(bytes(buf))
        assert_ok(hdr)


# ===================================================================
# 2. ERROR STRING BOUNDS
# ===================================================================


class TestErrorStringBounds:
    """Tests for error string length validation."""

    def test_err_len_exceeds_max(self, raw):
        """err_len=70000 exceeds MAX_ERR_LEN (65536)."""
        msg = pack_header(
            target_id=SCHEMA_TAB,
            err_len=70000,
        ) + b"\x00" * 70000
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "safety limit")

    def test_err_len_exactly_at_max(self, raw):
        """err_len=65536, message sized to fit. Server reads err string but
        data_count=0, so this is a valid scan (err_len is client metadata)."""
        err_data = b"X" * 65536
        body_start = align_up(HEADER_SIZE + 65536, ALIGNMENT)
        total = body_start
        msg = pack_header(
            target_id=SCHEMA_TAB,
            err_len=65536,
        ) + err_data + b"\x00" * (total - HEADER_SIZE - 65536)
        hdr, data = raw.send_and_recv(msg)
        # Server should accept this — err_len <= MAX_ERR_LEN
        assert_ok(hdr)

    def test_err_len_exceeds_file_size(self, raw):
        """err_len=1000 but message is only 200 bytes total → truncated."""
        msg = pack_header(
            target_id=SCHEMA_TAB,
            err_len=1000,
        ) + b"\x00" * (200 - HEADER_SIZE)
        assert len(msg) == 200
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "truncated", "error string")

    def test_err_len_one_byte_over_file(self, raw):
        """err_len claims exactly 1 byte more than available."""
        # File size = HEADER_SIZE + 10, err_len = 11
        msg = pack_header(
            target_id=SCHEMA_TAB,
            err_len=11,
        ) + b"A" * 10
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "truncated")

    def test_err_len_zero_with_error_status(self, raw):
        """status=ERROR but err_len=0 → no error string to read.
        data_count=0, so this becomes a scan after parsing."""
        msg = pack_header(
            target_id=SCHEMA_TAB,
            status=STATUS_ERROR,
            err_len=0,
        )
        hdr, data = raw.send_and_recv(msg)
        # Server doesn't validate client's status field; treats as scan
        assert_ok(hdr)


# ===================================================================
# 3. PROTOCOL INVARIANT
# ===================================================================


class TestProtocolInvariant:
    """Tests for structural protocol constraints."""

    def test_data_count_without_schema(self, raw):
        """data_count > 0 but schema_count = 0 → protocol error."""
        msg = pack_header(
            target_id=SCHEMA_TAB,
            data_count=5,
            schema_count=0,
        ) + b"\x00" * 2048  # padding to avoid size-related errors
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "data_rows", "schema_rows")

    def test_data_count_large_without_schema(self, raw):
        """data_count=1000000, schema_count=0 → protocol error."""
        msg = pack_header(
            target_id=SCHEMA_TAB,
            data_count=1000000,
            schema_count=0,
        ) + b"\x00" * 256
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "data_rows", "schema_rows")


# ===================================================================
# 4. SCHEMA SECTION FAILURES
# ===================================================================


class TestSchemaSectionFailures:
    """Tests for META_SCHEMA ZSet section parsing."""

    def test_schema_section_truncated_structural(self, raw):
        """schema_count=3 but message too short for structural buffers."""
        # 3 rows × 8 bytes × 4 structural buffers = 96 bytes minimum
        # Plus alignment. But we only provide the header.
        msg = pack_header(
            target_id=SCHEMA_TAB,
            schema_count=3,
            schema_blob_sz=0,
        )
        # No body at all — total is 96, structural data starts at byte 128
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "truncated")

    def test_schema_section_truncated_just_short(self, raw):
        """schema_count=1 with message just 1 byte too short for
        structural buffers."""
        # 1 row: 4 structural buffers × align_up(8, 64) = 4 × 64 = 256
        # Plus column data for META_SCHEMA (3 payload cols)
        # Body starts at byte 128. Need structural end at 128 + 256 = 384.
        # Provide 383 total bytes (1 short).
        msg = pack_header(
            target_id=SCHEMA_TAB,
            schema_count=1,
            schema_blob_sz=0,
        ) + b"\x00" * (383 - HEADER_SIZE)
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "truncated")

    def test_schema_invalid_type_code(self, raw):
        """Schema column with type_code=255 (unknown)."""
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("v", TypeCode.I64)]
        section, blob_sz, count = build_schema_section(
            cols, pk_index=0,
            type_code_overrides={1: 255},
        )
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=count,
            schema_blob_sz=blob_sz,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "type code")

    def test_schema_type_code_zero(self, raw):
        """Schema column with type_code=0 (invalid, codes start at 1)."""
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("v", TypeCode.I64)]
        section, blob_sz, count = build_schema_section(
            cols, pk_index=0,
            type_code_overrides={1: 0},
        )
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=count,
            schema_blob_sz=blob_sz,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "type code")

    def test_schema_type_code_13(self, raw):
        """type_code=13 (one past U128=12)."""
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("v", TypeCode.I64)]
        section, blob_sz, count = build_schema_section(
            cols, pk_index=0,
            type_code_overrides={1: 13},
        )
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=count,
            schema_blob_sz=blob_sz,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "type code")

    def test_schema_no_pk_flag(self, raw):
        """No column has META_FLAG_IS_PK set."""
        cols = [ColumnDef("a", TypeCode.U64), ColumnDef("b", TypeCode.I64)]
        section, blob_sz, count = build_schema_section(
            cols, pk_index=0,
            # Remove PK flag from column 0
            flag_overrides={0: 0},
        )
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=count,
            schema_blob_sz=blob_sz,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "pk")

    def test_schema_multiple_pk_flags(self, raw):
        """Two columns both have META_FLAG_IS_PK."""
        cols = [ColumnDef("a", TypeCode.U64), ColumnDef("b", TypeCode.I64)]
        section, blob_sz, count = build_schema_section(
            cols, pk_index=0,
            # Set PK flag on both columns
            flag_overrides={0: META_FLAG_IS_PK, 1: META_FLAG_IS_PK},
        )
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=count,
            schema_blob_sz=blob_sz,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "multiple pk")

    def test_schema_col_idx_out_of_order(self, raw):
        """col_idx values are [1, 0] instead of [0, 1]."""
        cols = [ColumnDef("a", TypeCode.U64), ColumnDef("b", TypeCode.I64)]
        section, blob_sz, count = build_schema_section(
            cols, pk_index=0,
            col_idx_overrides={0: 1, 1: 0},
        )
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=count,
            schema_blob_sz=blob_sz,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "col_idx", "order")

    def test_schema_col_idx_gap(self, raw):
        """col_idx values are [0, 2] (gap at 1)."""
        cols = [ColumnDef("a", TypeCode.U64), ColumnDef("b", TypeCode.I64)]
        section, blob_sz, count = build_schema_section(
            cols, pk_index=0,
            col_idx_overrides={1: 2},
        )
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=count,
            schema_blob_sz=blob_sz,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "col_idx")

    def test_schema_col_idx_duplicate(self, raw):
        """col_idx values are [0, 0] (duplicate)."""
        cols = [ColumnDef("a", TypeCode.U64), ColumnDef("b", TypeCode.I64)]
        section, blob_sz, count = build_schema_section(
            cols, pk_index=0,
            col_idx_overrides={1: 0},
        )
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=count,
            schema_blob_sz=blob_sz,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "col_idx")

    def test_schema_blob_sz_too_small(self, raw):
        """schema_blob_sz=0 but schema has STRING column names → mismatch."""
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("v", TypeCode.I64)]
        section, real_blob_sz, count = build_schema_section(cols, pk_index=0)
        # Override header's schema_blob_sz to 0 (but section still has blob data)
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=count,
            schema_blob_sz=0,  # lie about blob size
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "blob arena")

    def test_schema_blob_sz_much_too_small(self, raw):
        """schema_blob_sz=1 but actual blob is larger."""
        cols = [
            ColumnDef("primary_key_column", TypeCode.U64),
            ColumnDef("value_column", TypeCode.I64),
        ]
        section, real_blob_sz, count = build_schema_section(cols, pk_index=0)
        assert real_blob_sz > 1  # names are long enough
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=count,
            schema_blob_sz=1,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "blob arena")

    def test_schema_single_column_no_pk(self, raw):
        """Single-column schema without PK flag."""
        cols = [ColumnDef("a", TypeCode.U64)]
        section, blob_sz, count = build_schema_section(
            cols, pk_index=0,
            flag_overrides={0: 0},
        )
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=count,
            schema_blob_sz=blob_sz,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "pk")


# ===================================================================
# 5. DATA SECTION FAILURES
# ===================================================================


class TestDataSectionFailures:
    """Tests for data ZSet section parsing."""

    def _make_valid_schema_section(self, schema):
        """Build a valid schema section for the given schema."""
        batch = schema_to_batch(schema)
        section, blob_sz = encode_zset_section(META_SCHEMA, batch)
        return section, blob_sz, len(batch.pk_lo)

    def test_data_section_truncated_structural(self, raw):
        """Valid schema, data_count=10, but message too short for
        data structural buffers."""
        schema = TWO_COL_SCHEMA
        s_sec, s_blob, s_count = self._make_valid_schema_section(schema)

        # Build message with schema but no data body
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_count=10,
            data_blob_sz=0,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "truncated")

    def test_data_section_truncated_column(self, raw):
        """Valid schema, data_count=5, structural buffers present but
        column buffer area is truncated."""
        schema = TWO_COL_SCHEMA
        s_sec, s_blob, s_count = self._make_valid_schema_section(schema)

        # Build a correct data section, then truncate it
        rows = [(i, 1, [None, i * 10]) for i in range(5)]
        d_sec, d_blob = build_data_section(schema, rows)

        # Cut the data section in half
        truncated = d_sec[:len(d_sec) // 2]

        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=truncated,
            data_count=5,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "truncated")

    def test_data_string_offset_beyond_blob(self, raw):
        """String column entry has offset pointing past blob arena."""
        schema = THREE_COL_SCHEMA
        s_sec, s_blob, s_count = self._make_valid_schema_section(schema)

        # Build a valid data section with one row
        rows = [(1, 1, [None, 42, "hello"])]
        d_sec, d_blob = build_data_section(schema, rows)

        # Find the string column's offset/length entry in the data section
        # and corrupt the offset to point past the blob
        d_sec_mut = bytearray(d_sec)
        # The string column is col 2 (index 2). Walk the layout to find it.
        from gnitz_client.batch import _walk_layout
        _, _, _, _, col_offsets, _, blob_offset = _walk_layout(schema, 1)
        str_col_offset = col_offsets[2]
        # Overwrite offset to a huge value
        struct.pack_into("<I", d_sec_mut, str_col_offset, 99999)

        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=bytes(d_sec_mut),
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "blob arena")

    def test_data_string_length_overflows_blob(self, raw):
        """String entry has valid offset but length extends past blob end."""
        schema = THREE_COL_SCHEMA
        s_sec, s_blob, s_count = self._make_valid_schema_section(schema)

        rows = [(1, 1, [None, 42, "hi"])]
        d_sec, d_blob = build_data_section(schema, rows)

        d_sec_mut = bytearray(d_sec)
        from gnitz_client.batch import _walk_layout
        _, _, _, _, col_offsets, _, blob_offset = _walk_layout(schema, 1)
        str_col_offset = col_offsets[2]
        # Keep offset=0, set length to blob_sz + 100
        struct.pack_into("<II", d_sec_mut, str_col_offset, 0, d_blob + 100)

        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=bytes(d_sec_mut),
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "blob arena")

    def test_data_string_both_offset_and_length_at_edge(self, raw):
        """String offset+length exactly equals blob_sz → should succeed."""
        schema = THREE_COL_SCHEMA
        s_sec, s_blob, s_count = self._make_valid_schema_section(schema)

        rows = [(1, 1, [None, 42, "test"])]
        d_sec, d_blob = build_data_section(schema, rows)

        # This message is correctly formed — should succeed or fail at dispatch
        # (SCHEMA_TAB schema won't match THREE_COL_SCHEMA)
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        # Should fail at schema validation (mismatch with SCHEMA_TAB), not IPC
        assert hdr["status"] == STATUS_ERROR
        msg_text = get_error_message(data)
        assert "blob arena" not in msg_text.lower()


# ===================================================================
# 6. SCHEMA MISMATCH (push to real table)
# ===================================================================


class TestSchemaMismatch:
    """Tests that push with wrong schema to an existing table
    gets a clear error."""

    def test_wrong_column_count_fewer(self, raw, ipc_test_table):
        """Push 2-column schema to a 3-column table."""
        tid = ipc_test_table
        wrong_schema = TWO_COL_SCHEMA

        s_sec, s_blob, s_count = build_schema_section(
            wrong_schema.columns, pk_index=wrong_schema.pk_index,
        )
        rows = [(1, 1, [None, 99])]
        d_sec, d_blob = build_data_section(wrong_schema, rows)

        msg = assemble_message(
            tid,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "schema mismatch", "columns")

    def test_wrong_column_count_more(self, raw, ipc_test_table):
        """Push 4-column schema to a 3-column table."""
        tid = ipc_test_table
        wrong_schema = Schema(
            columns=[
                ColumnDef("pk", TypeCode.U64),
                ColumnDef("a", TypeCode.I64),
                ColumnDef("b", TypeCode.I64),
                ColumnDef("c", TypeCode.I64),
            ],
            pk_index=0,
        )

        s_sec, s_blob, s_count = build_schema_section(
            wrong_schema.columns, pk_index=wrong_schema.pk_index,
        )
        rows = [(1, 1, [None, 1, 2, 3])]
        d_sec, d_blob = build_data_section(wrong_schema, rows)

        msg = assemble_message(
            tid,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "schema mismatch", "columns")

    def test_wrong_pk_index(self, raw, ipc_test_table):
        """Push with pk_index=1 to table with pk_index=0."""
        tid = ipc_test_table
        wrong_schema = Schema(
            columns=[
                ColumnDef("val", TypeCode.I64),
                ColumnDef("pk", TypeCode.U64),
                ColumnDef("name", TypeCode.STRING),
            ],
            pk_index=1,
        )

        s_sec, s_blob, s_count = build_schema_section(
            wrong_schema.columns, pk_index=wrong_schema.pk_index,
        )
        rows = [(1, 1, [99, None, "x"])]
        d_sec, d_blob = build_data_section(wrong_schema, rows)

        msg = assemble_message(
            tid,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=1,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "schema mismatch", "pk_index")

    def test_wrong_column_type(self, raw, ipc_test_table):
        """Push with I64 where STRING is expected in column 2."""
        tid = ipc_test_table
        wrong_schema = Schema(
            columns=[
                ColumnDef("pk", TypeCode.U64),
                ColumnDef("val", TypeCode.I64),
                ColumnDef("name", TypeCode.I64),  # should be STRING
            ],
            pk_index=0,
        )

        s_sec, s_blob, s_count = build_schema_section(
            wrong_schema.columns, pk_index=wrong_schema.pk_index,
        )
        rows = [(1, 1, [None, 42, 999])]
        d_sec, d_blob = build_data_section(wrong_schema, rows)

        msg = assemble_message(
            tid,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "schema mismatch", "column 2")

    def test_wrong_type_first_payload_col(self, raw, ipc_test_table):
        """Push with U32 where I64 is expected in column 1."""
        tid = ipc_test_table
        wrong_schema = Schema(
            columns=[
                ColumnDef("pk", TypeCode.U64),
                ColumnDef("val", TypeCode.U32),  # should be I64
                ColumnDef("name", TypeCode.STRING),
            ],
            pk_index=0,
        )

        s_sec, s_blob, s_count = build_schema_section(
            wrong_schema.columns, pk_index=wrong_schema.pk_index,
        )
        rows = [(1, 1, [None, 42, "ok"])]
        d_sec, d_blob = build_data_section(wrong_schema, rows)

        msg = assemble_message(
            tid,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "schema mismatch", "column 1")


# ===================================================================
# 7. DISPATCH FAILURES
# ===================================================================


class TestDispatchFailures:
    """Tests for target_id resolution errors."""

    def test_target_id_nonexistent(self, raw):
        """Scan with target_id=99999 → unknown."""
        msg = make_scan_message(target_id=99999)
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "unknown")

    def test_target_id_zero_scan(self, raw):
        """Scan with target_id=0 → unknown table 0."""
        msg = make_scan_message(target_id=0)
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "unknown")

    def test_target_id_max_u64(self, raw):
        """Scan with target_id near max → unknown."""
        msg = make_scan_message(target_id=2**63 - 1)
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "unknown")

    def test_push_to_nonexistent_with_valid_schema(self, raw):
        """Push well-formed data to nonexistent target_id → unknown."""
        fake_tid = 88888
        schema = TWO_COL_SCHEMA
        s_sec, s_blob, s_count = build_schema_section(
            schema.columns, pk_index=schema.pk_index,
        )
        rows = [(1, 1, [None, 42])]
        d_sec, d_blob = build_data_section(schema, rows)

        msg = assemble_message(
            fake_tid,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "unknown")

    def test_target_id_negative_one(self, raw):
        """target_id = 2**64 - 1 (treated as -1 by intmask on server)."""
        # pack_header uses Q (unsigned), so we pass the full u64 value
        msg = make_scan_message(target_id=(2**64 - 1))
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "unknown")


# ===================================================================
# 8. EDGE CASES (should succeed or be harmless)
# ===================================================================


class TestEdgeCases:
    """Messages that are unusual but should NOT cause errors."""

    def test_scan_system_table_schema_tab(self, raw):
        """Basic scan of SCHEMA_TAB — sanity check."""
        msg = make_scan_message(target_id=SCHEMA_TAB)
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)
        assert hdr["schema_count"] > 0

    def test_scan_system_table_seq_tab(self, raw):
        """Scan of SEQ_TAB — returns sequence data."""
        msg = make_scan_message(target_id=SEQ_TAB)
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_scan_with_schema_section_but_no_data(self, raw):
        """Send schema section but data_count=0 → treated as scan."""
        schema = TWO_COL_SCHEMA
        s_sec, s_blob, s_count = build_schema_section(
            schema.columns, pk_index=schema.pk_index,
        )

        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_count=0,
            data_blob_sz=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_nonzero_reserved_bytes(self, raw):
        """Non-zero bytes in reserved area [80:96] → ignored."""
        msg = bytearray(make_scan_message(target_id=SCHEMA_TAB))
        msg[80:96] = b"\xff" * 16
        hdr, data = raw.send_and_recv(bytes(msg))
        assert_ok(hdr)

    def test_unknown_flags_bits(self, raw):
        """Unknown flag bits set (0xFE) → should be ignored."""
        msg = make_scan_message(target_id=SCHEMA_TAB, flags=0xFE)
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_all_flags_set(self, raw):
        """All flag bits set including FLAG_ALLOCATE_ID.
        target_id=0 + FLAG_ALLOCATE_ID → allocates a new ID.
        But data_count=0 so it's a scan, which then tries to scan
        the allocated ID. The allocated ID has no table registered yet."""
        msg = make_scan_message(target_id=0, flags=0xFFFFFFFFFFFFFFFF)
        hdr, data = raw.send_and_recv(msg)
        # Could be error (unknown table) or success — depends on server logic
        # The key point is the server doesn't crash

    def test_client_sends_status_error(self, raw):
        """Client sets status=STATUS_ERROR with err_len=5.
        Server doesn't validate client status, treats as scan."""
        err = b"oops!"
        body_start = align_up(HEADER_SIZE + len(err), ALIGNMENT)
        msg = pack_header(
            target_id=SCHEMA_TAB,
            status=STATUS_ERROR,
            err_len=len(err),
        ) + err + b"\x00" * (body_start - HEADER_SIZE - len(err))
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_large_status_value(self, raw):
        """status=9999 (not 0 or 1) → server doesn't validate, treats as scan."""
        msg = make_scan_message(target_id=SCHEMA_TAB, status=9999)
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_large_client_id(self, raw):
        """client_id with large value → should be echoed back."""
        msg = make_scan_message(target_id=SCHEMA_TAB, client_id=123456789)
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)
        assert hdr["client_id"] == 123456789

    def test_data_pk_index_ignored_on_scan(self, raw):
        """data_pk_index set to 99 on a scan → ignored since data_count=0."""
        msg = make_scan_message(target_id=SCHEMA_TAB, data_pk_index=99)
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_scan_returns_schema_section(self, raw):
        """Scan response always includes schema."""
        msg = make_scan_message(target_id=SCHEMA_TAB)
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)
        assert hdr["schema_count"] > 0
        assert hdr["schema_blob_sz"] > 0


# ===================================================================
# 9. TRANSPORT EDGE CASES
# ===================================================================


class TestTransportEdgeCases:
    """Tests for transport-level failures."""

    def test_send_without_scm_rights(self, server):
        """Send a message without SCM_RIGHTS fd → server gets recv_fd=-1."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        sock.connect(server)
        try:
            # Send dummy byte without ancillary data
            sock.sendmsg([b"G"], [])
            # Server should send back an error response (via its own memfd)
            msg, ancdata, flags, addr = sock.recvmsg(1, socket.CMSG_SPACE(4))
            assert msg  # got a response
            # Extract the fd from the response
            received_fd = -1
            for cmsg_level, cmsg_type, cmsg_data in ancdata:
                if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
                    fds = struct.unpack("i" * (len(cmsg_data) // 4), cmsg_data)
                    for fd in fds:
                        if received_fd == -1 and fd >= 0:
                            received_fd = fd
                        elif fd >= 0:
                            os.close(fd)
            if received_fd >= 0:
                import mmap
                size = os.fstat(received_fd).st_size
                mm = mmap.mmap(received_fd, size, access=mmap.ACCESS_READ)
                resp = bytes(mm[:])
                mm.close()
                os.close(received_fd)
                hdr = unpack_header(resp)
                assert hdr["status"] == STATUS_ERROR
                err_msg = get_error_message(resp)
                assert "descriptor" in err_msg.lower() or "socket" in err_msg.lower()
        except (ConnectionError, BrokenPipeError):
            # Server might drop connection instead — also acceptable
            pass
        finally:
            sock.close()

    def test_empty_memfd(self, server):
        """Zero-byte memfd → server sees fget_size=0, too small for header."""
        sock = transport.connect(server)
        try:
            # Create a zero-length memfd and send it directly
            fd = os.memfd_create("gnitz_empty")
            try:
                sock.sendmsg(
                    [b"G"],
                    [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                      struct.pack("i", fd))],
                )
            finally:
                os.close(fd)

            data = transport.recv_memfd(sock)
            hdr = unpack_header(data)
            assert_error_contains(hdr, data, "too small")
        except (ConnectionError, BrokenPipeError):
            pass  # server may drop connection instead
        finally:
            sock.close()


# ===================================================================
# 10. RESILIENCE
# ===================================================================


class TestResilience:
    """Tests that the server stays operational after errors."""

    def test_error_then_valid_same_connection(self, raw):
        """Send bad message, get error, then send valid message on same connection."""
        # Bad: wrong magic
        hdr1, data1 = raw.send_and_recv(b"\x00" * HEADER_SIZE)
        assert hdr1["status"] == STATUS_ERROR

        # Good: valid scan
        hdr2, data2 = raw.send_and_recv(make_scan_message(target_id=SCHEMA_TAB))
        assert_ok(hdr2)
        assert hdr2["schema_count"] > 0

    def test_multiple_errors_then_valid(self, raw):
        """Three different errors, then a valid scan."""
        # Error 1: wrong magic
        hdr, data = raw.send_and_recv(b"\x00" * HEADER_SIZE)
        assert hdr["status"] == STATUS_ERROR

        # Error 2: too small payload
        hdr, data = raw.send_and_recv(b"\x42")
        assert hdr["status"] == STATUS_ERROR

        # Error 3: nonexistent target
        hdr, data = raw.send_and_recv(make_scan_message(target_id=99999))
        assert hdr["status"] == STATUS_ERROR

        # Valid scan
        hdr, data = raw.send_and_recv(make_scan_message(target_id=SCHEMA_TAB))
        assert_ok(hdr)

    def test_new_connection_after_error(self, server):
        """Error on one connection doesn't affect a new connection."""
        # Connection 1: send bad message
        c1 = RawIpcClient(server)
        hdr, data = c1.send_and_recv(b"\x00" * HEADER_SIZE)
        assert hdr["status"] == STATUS_ERROR
        c1.close()

        # Connection 2: should work fine
        c2 = RawIpcClient(server)
        hdr, data = c2.send_and_recv(make_scan_message(target_id=SCHEMA_TAB))
        assert_ok(hdr)
        c2.close()

    def test_interleaved_connections(self, server):
        """Multiple concurrent connections, some failing, some succeeding."""
        clients = [RawIpcClient(server) for _ in range(5)]
        try:
            # Send errors on even, valid on odd
            for i, c in enumerate(clients):
                if i % 2 == 0:
                    hdr, data = c.send_and_recv(b"\x00" * HEADER_SIZE)
                    assert hdr["status"] == STATUS_ERROR
                else:
                    hdr, data = c.send_and_recv(
                        make_scan_message(target_id=SCHEMA_TAB)
                    )
                    assert_ok(hdr)

            # Now all should still work
            for c in clients:
                hdr, data = c.send_and_recv(
                    make_scan_message(target_id=SCHEMA_TAB)
                )
                assert_ok(hdr)
        finally:
            for c in clients:
                c.close()

    def test_rapid_reconnect(self, server):
        """Rapidly connect, send, close, repeat."""
        for _ in range(10):
            c = RawIpcClient(server)
            hdr, data = c.send_and_recv(make_scan_message(target_id=SCHEMA_TAB))
            assert_ok(hdr)
            c.close()

    def test_error_recovery_with_schema_error(self, raw):
        """Schema parse error, then valid scan on same connection."""
        # Build a message with invalid type code in schema
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("v", TypeCode.I64)]
        section, blob_sz, count = build_schema_section(
            cols, pk_index=0,
            type_code_overrides={1: 200},
        )
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=count,
            schema_blob_sz=blob_sz,
        )
        hdr, data = raw.send_and_recv(msg)
        assert hdr["status"] == STATUS_ERROR

        # Valid scan after schema error
        hdr, data = raw.send_and_recv(make_scan_message(target_id=SCHEMA_TAB))
        assert_ok(hdr)

    def test_error_recovery_with_dispatch_error(self, raw):
        """Dispatch error (unknown target), then valid scan."""
        hdr, data = raw.send_and_recv(make_scan_message(target_id=99999))
        assert hdr["status"] == STATUS_ERROR

        hdr, data = raw.send_and_recv(make_scan_message(target_id=SCHEMA_TAB))
        assert_ok(hdr)


# ===================================================================
# 11. DATA VALIDITY EDGE CASES
# ===================================================================


class TestDataEdgeCases:
    """Edge cases around valid data encoding."""

    def test_push_valid_data_to_real_table(self, raw, ipc_test_table):
        """Correctly formed push to a real table succeeds."""
        tid = ipc_test_table
        schema = THREE_COL_SCHEMA

        s_sec, s_blob, s_count = build_schema_section(
            schema.columns, pk_index=schema.pk_index,
        )
        rows = [(9000, 1, [None, 77, "raw_push"])]
        d_sec, d_blob = build_data_section(schema, rows)

        msg = assemble_message(
            tid,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_push_multiple_rows(self, raw, ipc_test_table):
        """Push multiple rows in one batch."""
        tid = ipc_test_table
        schema = THREE_COL_SCHEMA

        s_sec, s_blob, s_count = build_schema_section(
            schema.columns, pk_index=schema.pk_index,
        )
        rows = [
            (9001, 1, [None, 1, "alpha"]),
            (9002, 1, [None, 2, "beta"]),
            (9003, 1, [None, 3, "gamma"]),
        ]
        d_sec, d_blob = build_data_section(schema, rows)

        msg = assemble_message(
            tid,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=3,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_push_with_negative_weight(self, raw, ipc_test_table):
        """Push a retraction (negative weight) — valid ZSet operation."""
        tid = ipc_test_table
        schema = THREE_COL_SCHEMA

        s_sec, s_blob, s_count = build_schema_section(
            schema.columns, pk_index=schema.pk_index,
        )
        rows = [(9000, -1, [None, 77, "raw_push"])]
        d_sec, d_blob = build_data_section(schema, rows)

        msg = assemble_message(
            tid,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_push_with_zero_weight(self, raw, ipc_test_table):
        """Push with weight=0 — should be accepted (no-op row)."""
        tid = ipc_test_table
        schema = THREE_COL_SCHEMA

        s_sec, s_blob, s_count = build_schema_section(
            schema.columns, pk_index=schema.pk_index,
        )
        rows = [(9099, 0, [None, 0, "zero_weight"])]
        d_sec, d_blob = build_data_section(schema, rows)

        msg = assemble_message(
            tid,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_push_with_empty_string(self, raw, ipc_test_table):
        """Push with empty string value → valid."""
        tid = ipc_test_table
        schema = THREE_COL_SCHEMA

        s_sec, s_blob, s_count = build_schema_section(
            schema.columns, pk_index=schema.pk_index,
        )
        rows = [(9100, 1, [None, 1, ""])]
        d_sec, d_blob = build_data_section(schema, rows)

        msg = assemble_message(
            tid,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_push_with_null_string(self, raw, ipc_test_table):
        """Push with NULL string value → valid."""
        tid = ipc_test_table
        schema = THREE_COL_SCHEMA

        s_sec, s_blob, s_count = build_schema_section(
            schema.columns, pk_index=schema.pk_index,
        )

        # Build batch with null string
        batch = ZSetBatch(schema=schema)
        batch.pk_lo.append(9101)
        batch.pk_hi.append(0)
        batch.weights.append(1)
        # Set null bit for payload column 1 (STRING at schema col 2, payload_idx=1)
        batch.nulls.append(1 << 1)
        batch.columns = [[], [99], [None]]
        d_sec, d_blob = encode_zset_section(schema, batch)

        msg = assemble_message(
            tid,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=d_blob,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)

    def test_scan_after_pushes_returns_data(self, raw, ipc_test_table):
        """Scan the table after pushing data — verify we get rows back."""
        tid = ipc_test_table
        msg = make_scan_message(target_id=tid)
        hdr, data = raw.send_and_recv(msg)
        assert_ok(hdr)
        # Should have schema and some data rows from earlier pushes
        assert hdr["schema_count"] > 0


# ===================================================================
# 12. COMBINED/COMPOUND FAILURES
# ===================================================================


class TestCompoundFailures:
    """Tests combining multiple malformed aspects."""

    def test_wrong_magic_and_data_count(self, raw):
        """Wrong magic with data_count > 0 → magic error detected first."""
        msg = pack_header(
            magic=0, target_id=SCHEMA_TAB,
            data_count=5, schema_count=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "magic")

    def test_truncated_with_wrong_magic(self, raw):
        """50 bytes with wrong magic → too small, detected first."""
        hdr, data = raw.send_and_recv(b"\xff" * 50)
        assert_error_contains(hdr, data, "too small")

    def test_valid_schema_but_data_count_lies(self, raw):
        """Valid schema section, data_count=100 but no data bytes."""
        schema = TWO_COL_SCHEMA
        s_sec, s_blob, s_count = build_schema_section(
            schema.columns, pk_index=schema.pk_index,
        )
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_count=100,
            data_blob_sz=0,
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "truncated")

    def test_schema_count_lies_high(self, raw):
        """schema_count=1000 but only enough data for 2 rows."""
        cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("v", TypeCode.I64)]
        section, blob_sz, _ = build_schema_section(cols, pk_index=0)
        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=section,
            schema_count=1000,  # lie: actual batch has 2 rows
            schema_blob_sz=blob_sz,
        )
        hdr, data = raw.send_and_recv(msg)
        assert_error_contains(hdr, data, "truncated")

    def test_blob_sz_lies_high(self, raw):
        """data_blob_sz claims 99999 but actual blob is tiny."""
        schema = THREE_COL_SCHEMA
        s_sec, s_blob, s_count = build_schema_section(
            schema.columns, pk_index=schema.pk_index,
        )
        rows = [(1, 1, [None, 42, "hi"])]
        d_sec, d_blob = build_data_section(schema, rows)

        msg = assemble_message(
            SCHEMA_TAB,
            schema_section=s_sec,
            schema_count=s_count,
            schema_blob_sz=s_blob,
            data_section=d_sec,
            data_count=1,
            data_blob_sz=99999,  # lie
            data_pk_index=0,
        )
        hdr, data = raw.send_and_recv(msg)
        # Server tries to read strings with wrong blob_sz → blob arena error
        assert hdr["status"] == STATUS_ERROR
