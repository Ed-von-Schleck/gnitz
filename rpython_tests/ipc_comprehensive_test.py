# ipc_comprehensive_test.py

import os
from rpython.rlib import rsocket
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
)

from gnitz.core import types, batch, errors
from gnitz.core import strings as string_logic
from gnitz.core.batch import RowBuilder
from gnitz.storage import buffer, mmap_posix
from gnitz.server import ipc, ipc_ffi


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


def test_v2_roundtrip():
    """Verifies full v2 serialize + receive with mixed columns (ints + strings)."""
    os.write(1, "[IPC] Testing V2 Full Roundtrip...\n")

    schema = make_test_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=10)
        rb = RowBuilder(schema, zbatch)
        rb.begin(r_uint128(42), r_int64(1))
        rb.put_string("Hello Zero-Copy World")
        rb.commit()
        rb.begin(r_uint128(99), r_int64(1))
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


def test_v2_int_only_roundtrip():
    """Verifies v2 roundtrip with integer-only schema (no strings)."""
    os.write(1, "[IPC] Testing V2 Int-Only Roundtrip...\n")

    schema = make_int_only_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=10)
        rb = RowBuilder(schema, zbatch)
        rb.begin(r_uint128(1), r_int64(1))
        rb.put_int(r_int64(42))
        rb.put_int(r_int64(7))
        rb.commit()
        rb.begin(r_uint128(2), r_int64(1))
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


def test_v2_error_path():
    """Verifies that error responses work via v2 format."""
    os.write(1, "[IPC] Testing V2 Error Path...\n")

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


def test_v2_scan_request():
    """Verifies empty push (scan request) has no schema or data."""
    os.write(1, "[IPC] Testing V2 Scan Request (empty push)...\n")

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


def test_v2_ipc_string_encoding():
    """Verifies IPC string encoding with various lengths."""
    os.write(1, "[IPC] Testing V2 IPC String Encoding...\n")

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
            rb.begin(r_uint128(r_uint64(i + 1)), r_int64(1))
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


def test_v2_null_strings():
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
        rb.begin(r_uint128(1), r_int64(1))
        rb.put_string("not null")
        rb.commit()

        # Row with null string
        rb.begin(r_uint128(2), r_int64(1))
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


def test_v2_multi_string_column_roundtrip():
    """Verifies IPC roundtrip with two string columns sharing a blob arena.

    Exercises the shared_blob_buf path: per-entry blob offsets must be global
    (relative to the single blob area start), not per-column-relative.
    """
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
            rb.begin(r_uint128(r_uint64(i + 1)), r_int64(1))
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


def test_v2_schema_mismatch():
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


# Bring in intmask for int comparisons
from rpython.rlib.rarithmetic import intmask


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------


def entry_point(argv):
    os.write(1, "--- GnitzDB IPC Transport Test (v2) ---\n")
    try:
        test_unowned_buffer_lifecycle()
        test_ipc_fd_hardening()
        test_meta_schema_roundtrip()
        test_v2_roundtrip()
        test_v2_int_only_roundtrip()
        test_v2_error_path()
        test_v2_scan_request()
        test_v2_ipc_string_encoding()
        test_v2_null_strings()
        test_v2_multi_string_column_roundtrip()
        test_v2_schema_mismatch()
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
