# IPC comprehensive test (WAL-block envelope)

import os
from rpython.rlib import rsocket
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)

from gnitz.core import types, batch, errors
from gnitz.core import strings as string_logic
from gnitz.core.batch import RowBuilder
from gnitz.storage import buffer, buffer as buffer_ops, mmap_posix
from gnitz.storage import wal_columnar
from gnitz.server import ipc, ipc_ffi
from rpython_tests.helpers.jit_stub import ensure_jit_reachable


# ------------------------------------------------------------------------------
# Assertions & Helpers
# ------------------------------------------------------------------------------


def assert_true(condition, msg):
    if not condition:
        os.write(2, "FAIL: " + msg + "\n")
        raise Exception("Assertion Failed")


def assert_equal_i(expected, actual, msg):
    if expected != actual:
        os.write(
            2,
            "FAIL: " + msg + " (Expected %d, got %d)\n" % (expected, actual),
        )
        raise Exception("Value Mismatch")


def make_test_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="id"),
        types.ColumnDefinition(types.TYPE_STRING, name="data"),
    ]
    return types.TableSchema(cols, 0)


def make_int_only_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
        types.ColumnDefinition(types.TYPE_U32, name="flag"),
    ]
    return types.TableSchema(cols, 0)


# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------


def test_unowned_buffer_lifecycle():
    """Verifies that unowned buffers inhibit growth and respect physical limits."""
    os.write(1, "[IPC] Testing Unowned Buffer Lifecycle...\n")

    size = 128
    raw_mem = lltype.malloc(rffi.CCHARP.TO, size, flavor="raw")

    try:
        buf = buffer.Buffer.from_external_ptr(raw_mem, size)
        assert_true(not buf.is_owned, "Buffer should be unowned")
        assert_true(not buf.growable, "Unowned buffer should not be growable")

        ptr = buf.alloc(64, alignment=1)
        assert_equal_i(
            rffi.cast(lltype.Signed, raw_mem),
            rffi.cast(lltype.Signed, ptr),
            "Pointer mismatch",
        )

        raised = False
        try:
            buf.alloc(128)
        except errors.MemTableFullError:
            raised = True
        assert_true(raised, "Unowned buffer failed to block overflow")

        buf.free()
        assert_true(
            buf.base_ptr == lltype.nullptr(rffi.CCHARP.TO),
            "Base ptr not neutralized",
        )

        raw_mem[0] = "A"
        assert_true(
            raw_mem[0] == "A", "Underlying memory corrupted after buffer free"
        )

    finally:
        lltype.free(raw_mem, flavor="raw")


def test_ipc_fd_hardening():
    """Verifies that the C-FFI layer handles FD passing and prevents leaks."""
    os.write(1, "[IPC] Testing Hardened FD Passing (SCM_RIGHTS)...\n")

    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)

    try:
        fd_to_send = mmap_posix.memfd_create_c("test_fd")
        mmap_posix.ftruncate_c(fd_to_send, 1024)

        res = ipc_ffi.send_fd(s1.fd, fd_to_send)
        assert_equal_i(0, res, "Failed to send FD")

        received_fd = ipc_ffi.recv_fd(s2.fd)
        assert_true(received_fd >= 0, "Failed to receive FD")

        assert_equal_i(
            1024,
            mmap_posix.fget_size(received_fd),
            "FD content mismatch",
        )

        os.close(fd_to_send)
        os.close(received_fd)
    finally:
        s1.close()
        s2.close()


def test_meta_schema_roundtrip():
    """Verifies schema -> batch -> schema roundtrip."""
    os.write(1, "[IPC] Testing Meta-Schema Roundtrip...\n")

    schema = make_test_schema()
    schema_batch = ipc.schema_to_batch(schema)

    assert_equal_i(2, schema_batch.length(), "Schema batch should have 2 rows")

    reconstructed = ipc.batch_to_schema(schema_batch)

    assert_equal_i(
        len(schema.columns),
        len(reconstructed.columns),
        "Column count mismatch",
    )
    assert_equal_i(schema.pk_index, reconstructed.pk_index, "PK index mismatch")

    for i in range(len(schema.columns)):
        assert_equal_i(
            schema.columns[i].field_type.code,
            reconstructed.columns[i].field_type.code,
            "Type code mismatch at col %d" % i,
        )

    schema_batch.free()


def test_ipc_roundtrip():
    """Verifies full serialize + receive with mixed columns (ints + strings)."""
    os.write(1, "[IPC] Testing IPC Roundtrip...\n")

    schema = make_test_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=10)
        rb = RowBuilder(schema, zbatch)
        rb.begin(r_uint64(42), r_uint64(0), r_int64(1))
        rb.put_string("Hello Zero-Copy World")
        rb.commit()
        rb.begin(r_uint64(99), r_uint64(0), r_int64(1))
        rb.put_string("Short")
        rb.commit()

        ipc.send_batch(s1.fd, 42, zbatch, status=0, error_msg="")

        payload = ipc.receive_payload(s2.fd)

        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(payload.schema is not None, "Schema should be present")
        assert_true(payload.batch is not None, "Batch should be present")
        assert_equal_i(2, payload.batch.length(), "Batch count mismatch")

        # Verify schema
        assert_equal_i(
            len(schema.columns),
            len(payload.schema.columns),
            "Schema column count mismatch",
        )
        assert_equal_i(
            schema.columns[0].field_type.code,
            payload.schema.columns[0].field_type.code,
            "Schema col 0 type mismatch",
        )
        assert_equal_i(
            schema.columns[1].field_type.code,
            payload.schema.columns[1].field_type.code,
            "Schema col 1 type mismatch",
        )

        # Verify data
        rec_batch = payload.batch
        acc = rec_batch.get_accessor(0)
        length, prefix, sptr, hptr, py_s = acc.get_str_struct(1)
        s = string_logic.resolve_string(sptr, hptr, py_s)
        assert_true(s == "Hello Zero-Copy World", "String corruption row 0")

        acc = rec_batch.get_accessor(1)
        length, prefix, sptr, hptr, py_s = acc.get_str_struct(1)
        s = string_logic.resolve_string(sptr, hptr, py_s)
        assert_true(s == "Short", "String corruption row 1")

        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()


def test_int_only_roundtrip():
    """Verifies roundtrip with integer-only schema (no strings)."""
    os.write(1, "[IPC] Testing Int-Only Roundtrip...\n")

    schema = make_int_only_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=10)
        rb = RowBuilder(schema, zbatch)
        rb.begin(r_uint64(1), r_uint64(0), r_int64(1))
        rb.put_int(r_int64(42))
        rb.put_int(r_int64(7))
        rb.commit()
        rb.begin(r_uint64(2), r_uint64(0), r_int64(1))
        rb.put_int(r_int64(-100))
        rb.put_int(r_int64(255))
        rb.commit()

        ipc.send_batch(s1.fd, 10, zbatch, status=0)

        payload = ipc.receive_payload(s2.fd)

        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(payload.batch is not None, "Batch should be present")
        assert_equal_i(2, payload.batch.length(), "Row count mismatch")

        acc = payload.batch.get_accessor(0)
        assert_equal_i(42, intmask(r_uint64(acc.get_int(1))), "Row 0 col 1 mismatch")

        acc = payload.batch.get_accessor(1)
        val = rffi.cast(rffi.LONGLONG, acc.get_int(1))
        assert_equal_i(-100, intmask(val), "Row 1 col 1 mismatch")

        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()


def test_error_path():
    """Verifies that error responses work."""
    os.write(1, "[IPC] Testing Error Path...\n")

    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)

    try:
        ipc.send_error(s1.fd, "Fatal VM Crash")

        payload = ipc.receive_payload(s2.fd)
        assert_equal_i(1, payload.status, "Error status mismatch")
        assert_true(
            payload.error_msg == "Fatal VM Crash", "Error string mismatch"
        )
        assert_true(
            payload.batch is None, "Error payload should have no batch"
        )
        assert_true(
            payload.schema is None, "Error payload should have no schema"
        )

        payload.close()
    finally:
        s1.close()
        s2.close()


def test_scan_request():
    """Verifies empty push (scan request) has no schema or data."""
    os.write(1, "[IPC] Testing Scan Request...\n")

    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)

    try:
        # Send empty push (scan request): no data, no schema
        ipc.send_batch(s1.fd, 42, None, status=0)

        payload = ipc.receive_payload(s2.fd)
        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(payload.batch is None, "Scan request should have no batch")
        assert_equal_i(42, payload.target_id, "Target ID mismatch")

        payload.close()
    finally:
        s1.close()
        s2.close()


def test_string_roundtrip():
    """Verifies IPC string roundtrip with various lengths."""
    os.write(1, "[IPC] Testing String Roundtrip...\n")

    schema = make_test_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=10)
        rb = RowBuilder(schema, zbatch)

        test_strings = [
            "",         # empty
            "Hi",       # 2 bytes (short, fits in prefix)
            "Test",     # 4 bytes (exact prefix length)
            "Hello World!",  # 12 bytes (SHORT_STRING_THRESHOLD)
            "13 bytes str!",  # 13 bytes (just over threshold)
            "A" * 100,  # 100 bytes (long string in blob)
        ]

        for i in range(len(test_strings)):
            rb.begin(r_uint64(i + 1), r_uint64(0), r_int64(1))
            rb.put_string(test_strings[i])
            rb.commit()

        ipc.send_batch(s1.fd, 42, zbatch, status=0)

        payload = ipc.receive_payload(s2.fd)
        assert_equal_i(len(test_strings), payload.batch.length(), "Row count mismatch")

        for i in range(len(test_strings)):
            acc = payload.batch.get_accessor(i)
            length, prefix, sptr, hptr, py_s = acc.get_str_struct(1)
            s = string_logic.resolve_string(sptr, hptr, py_s)
            assert_true(
                s == test_strings[i],
                "String mismatch at row %d: '%s' != '%s'" % (i, s, test_strings[i]),
            )

        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()


def test_null_strings():
    """Verifies NULL string encoding roundtrip."""
    os.write(1, "[IPC] Testing V2 NULL String Encoding...\n")

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_STRING, is_nullable=True, name="data"),
    ]
    schema = types.TableSchema(cols, 0)
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=10)
        rb = RowBuilder(schema, zbatch)

        # Row with non-null string
        rb.begin(r_uint64(1), r_uint64(0), r_int64(1))
        rb.put_string("not null")
        rb.commit()

        # Row with null string
        rb.begin(r_uint64(2), r_uint64(0), r_int64(1))
        rb.put_null()
        rb.commit()

        ipc.send_batch(s1.fd, 42, zbatch, status=0)

        payload = ipc.receive_payload(s2.fd)
        assert_equal_i(2, payload.batch.length(), "Row count mismatch")

        # Row 0: non-null
        acc = payload.batch.get_accessor(0)
        assert_true(not acc.is_null(1), "Row 0 should not be null")
        length, prefix, sptr, hptr, py_s = acc.get_str_struct(1)
        s = string_logic.resolve_string(sptr, hptr, py_s)
        assert_true(s == "not null", "Row 0 string mismatch")

        # Row 1: null
        acc = payload.batch.get_accessor(1)
        assert_true(acc.is_null(1), "Row 1 should be null")

        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()


def test_multi_string_column_roundtrip():
    """Exercises multi-string-column roundtrip."""
    os.write(1, "[IPC] Testing V2 Multi-String Column Roundtrip...\n")

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_STRING, name="name"),
        types.ColumnDefinition(types.TYPE_STRING, name="description"),
    ]
    schema = types.TableSchema(cols, 0)
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=4)
        rb = RowBuilder(schema, zbatch)

        # Four rows with varying string lengths in both columns.
        # Long strings (> 12 chars) exercise the blob allocator.
        names = ["Alice", "Bob", "Charlie Davidson", "D" * 60]
        descs = ["Short desc", "X" * 50, "Medium length description", "tiny"]

        for i in range(4):
            rb.begin(r_uint64(i + 1), r_uint64(0), r_int64(1))
            rb.put_string(names[i])
            rb.put_string(descs[i])
            rb.commit()

        ipc.send_batch(s1.fd, 77, zbatch, status=0)

        payload = ipc.receive_payload(s2.fd)
        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(payload.batch is not None, "Batch should be present")
        assert_equal_i(4, payload.batch.length(), "Row count mismatch")

        for i in range(4):
            acc = payload.batch.get_accessor(i)

            length, prefix, sptr, hptr, py_s = acc.get_str_struct(1)
            got_name = string_logic.resolve_string(sptr, hptr, py_s)
            assert_true(
                got_name == names[i],
                "name mismatch row %d: '%s' != '%s'" % (i, got_name, names[i]),
            )

            length, prefix, sptr, hptr, py_s = acc.get_str_struct(2)
            got_desc = string_logic.resolve_string(sptr, hptr, py_s)
            assert_true(
                got_desc == descs[i],
                "desc mismatch row %d: '%s' != '%s'" % (i, got_desc, descs[i]),
            )

        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()


def test_long_column_name_roundtrip():
    """Column names > 12 chars go to the blob arena (German String threshold).
    The schema block may then end at a non-16-aligned offset, exercising the
    schema->data block transition in encode_batch_append."""
    os.write(1, "[IPC] Testing long column name roundtrip...\n")

    schema = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U64,    name="primary_key_col"),   # 15 chars
            types.ColumnDefinition(types.TYPE_I64,    name="measurement_value"), # 17 chars
            types.ColumnDefinition(types.TYPE_STRING, name="description_text"),  # 16 chars
        ],
        pk_index=0,
    )
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
    try:
        b = batch.ArenaZSetBatch(schema, initial_capacity=2)
        rb = RowBuilder(schema, b)
        rb.begin(r_uint64(1), r_uint64(0), r_int64(1))
        rb.put_int(r_int64(42))
        rb.put_string("hello world!")
        rb.commit()
        rb.begin(r_uint64(2), r_uint64(0), r_int64(1))
        rb.put_int(r_int64(99))
        rb.put_string("this is a longer string value")
        rb.commit()

        ipc.send_batch(s1.fd, 7, b, status=0)

        payload = ipc.receive_payload(s2.fd)
        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(payload.batch is not None, "Batch should be present")
        assert_equal_i(2, payload.batch.length(), "Row count mismatch")

        acc = payload.batch.get_accessor(0)
        assert_equal_i(42, intmask(r_uint64(acc.get_int(1))), "Row 0 col 1 mismatch")

        acc = payload.batch.get_accessor(1)
        length, prefix, sptr, hptr, py_s = acc.get_str_struct(2)
        s = string_logic.resolve_string(sptr, hptr, py_s)
        assert_true(s == "this is a longer string value", "Row 1 string mismatch")

        payload.close()
        b.free()
    finally:
        s1.close()
        s2.close()


def test_schema_mismatch():
    """Verifies that schema validation rejects mismatched schemas."""
    os.write(1, "[IPC] Testing V2 Schema Mismatch Detection...\n")

    from gnitz.server.executor import _validate_schema_match

    schema_a = make_test_schema()
    schema_b = make_int_only_schema()

    raised = False
    try:
        _validate_schema_match(schema_a, schema_b)
    except errors.StorageError:
        raised = True
    assert_true(raised, "Schema validation should reject mismatched schemas")

    # Same schema should pass
    schema_c = make_test_schema()
    _validate_schema_match(schema_a, schema_c)


def test_control_schema_roundtrip():
    """Verifies CONTROL_SCHEMA encode/decode roundtrip preserves all fields."""
    os.write(1, "[IPC] Testing control schema roundtrip...\n")

    # --- Case 1: OK message, empty error string ---
    ctrl_batch = ipc._encode_control_batch(
        target_id=42, client_id=7,
        flags=r_uint64(ipc.FLAG_PUSH),
        seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0,
        status=ipc.STATUS_OK, error_msg="",
    )
    assert_equal_i(1, ctrl_batch.length(), "Control batch should have 1 row")

    buf = buffer_ops.Buffer(0)
    wal_columnar.encode_batch_to_buffer(
        buf, ipc.CONTROL_SCHEMA, r_uint64(0), ipc.IPC_CONTROL_TID, ctrl_batch
    )
    ctrl_batch.free()

    decoded = wal_columnar.decode_batch_from_ptr(
        buf.base_ptr, buf.offset, ipc.CONTROL_SCHEMA
    )
    p = ipc.IPCPayload()
    ipc._decode_control_batch(decoded, p)
    decoded.free()
    buf.free()

    assert_equal_i(42, p.target_id, "target_id mismatch")
    assert_equal_i(7, p.client_id, "client_id mismatch")
    assert_equal_i(ipc.STATUS_OK, p.status, "status mismatch")
    assert_true(p.error_msg == "", "error_msg should be empty")
    assert_true(
        p.flags == r_uint64(ipc.FLAG_PUSH),
        "flags mismatch",
    )

    # --- Case 2: ERROR message with non-null error string ---
    ctrl_batch2 = ipc._encode_control_batch(
        target_id=0, client_id=0,
        flags=r_uint64(0),
        seek_pk_lo=0, seek_pk_hi=0, seek_col_idx=0,
        status=ipc.STATUS_ERROR, error_msg="something went wrong",
    )
    buf2 = buffer_ops.Buffer(0)
    wal_columnar.encode_batch_to_buffer(
        buf2, ipc.CONTROL_SCHEMA, r_uint64(0), ipc.IPC_CONTROL_TID, ctrl_batch2
    )
    ctrl_batch2.free()

    decoded2 = wal_columnar.decode_batch_from_ptr(
        buf2.base_ptr, buf2.offset, ipc.CONTROL_SCHEMA
    )
    p2 = ipc.IPCPayload()
    ipc._decode_control_batch(decoded2, p2)
    decoded2.free()
    buf2.free()

    assert_equal_i(ipc.STATUS_ERROR, p2.status, "status mismatch")
    assert_true(
        p2.error_msg == "something went wrong",
        "error_msg mismatch: '%s'" % p2.error_msg,
    )


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------


def entry_point(argv):
    ensure_jit_reachable()
    os.write(1, "--- GnitzDB IPC Transport Test (WAL-block envelope) ---\n")
    try:
        test_unowned_buffer_lifecycle()
        test_ipc_fd_hardening()
        test_meta_schema_roundtrip()
        test_ipc_roundtrip()
        test_int_only_roundtrip()
        test_error_path()
        test_scan_request()
        test_string_roundtrip()
        test_null_strings()
        test_multi_string_column_roundtrip()
        test_long_column_name_roundtrip()
        test_schema_mismatch()
        test_control_schema_roundtrip()
        os.write(1, "\nALL IPC TRANSPORT TESTS PASSED\n")
    except Exception:
        os.write(2, "TESTING FAILED\n")
        return 1
    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    import sys

    entry_point(sys.argv)
