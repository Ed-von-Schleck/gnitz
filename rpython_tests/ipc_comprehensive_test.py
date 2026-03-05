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
# Mock Registry for IPC testing
# ------------------------------------------------------------------------------


class MockFamily(object):
    def __init__(self, schema):
        self._schema = schema

    def get_schema(self):
        return self._schema


class MockRegistry(object):
    def __init__(self, schema, tid):
        self.family = MockFamily(schema)
        self.tid = tid

    def has_id(self, tid):
        return tid == self.tid

    def get_by_id(self, tid):
        return self.family


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


def test_ipc_bounds_and_forgery():
    """Verifies that the receiver detects malicious/overflowing headers."""
    os.write(1, "[IPC] Testing Header Validation & Overflow Protection...\n")

    schema = make_test_schema()
    registry = MockRegistry(schema, 42)
    fd = mmap_posix.memfd_create_c("malicious_header")

    try:
        total_sz = 1024
        mmap_posix.ftruncate_c(fd, total_sz)
        ptr = mmap_posix.mmap_file(
            fd,
            total_sz,
            prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE,
        )

        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, ipc.OFF_MAGIC))[
            0
        ] = ipc.MAGIC_IPC
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, ipc.OFF_TARGET_ID))[
            0
        ] = r_uint64(42)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, ipc.OFF_COUNT))[
            0
        ] = r_uint64(99999)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, ipc.OFF_BLOB_SZ))[
            0
        ] = r_uint64(0)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, ipc.OFF_NUM_COLS))[
            0
        ] = r_uint64(2)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, ipc.OFF_PK_INDEX))[
            0
        ] = r_uint64(0)

        s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
        try:
            ipc_ffi.send_fd(s1.fd, fd)

            raised = False
            try:
                ipc.receive_payload(s2.fd, registry)
            except errors.StorageError:
                raised = True
            assert_true(
                raised, "IPC layer failed to detect column buffer overflow"
            )
        finally:
            s1.close()
            s2.close()
            mmap_posix.munmap_file(ptr, total_sz)
    finally:
        os.close(fd)


def test_zero_copy_roundtrip():
    """Verifies full serialization, transport, and reconstruction."""
    os.write(1, "[IPC] Testing Zero-Copy Roundtrip...\n")

    schema = make_test_schema()
    registry = MockRegistry(schema, 42)
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)

    try:
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=10)
        rb = RowBuilder(schema, zbatch)
        rb.begin(r_uint128(42), r_int64(1))
        rb.put_string("Hello Zero-Copy World")
        rb.commit()

        ipc.send_batch(s1.fd, 42, zbatch, status=0, error_msg="Success")

        payload = ipc.receive_payload(s2.fd, registry)

        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(
            payload.error_msg == "Success", "Error msg mismatch"
        )

        rec_batch = payload.batch
        assert_true(rec_batch is not None, "Batch should not be None")
        assert_equal_i(1, rec_batch.length(), "Batch count mismatch")
        assert_true(
            rec_batch.pk_lo_buf.is_owned == False,
            "Received batch should use unowned views",
        )

        acc = rec_batch.get_accessor(0)
        assert_true(
            acc.get_str_struct(1)[4] is None,
            "Accessor should point to raw memory, not Python str",
        )

        length, prefix, sptr, hptr, py_s = acc.get_str_struct(1)
        s = py_s if py_s is not None else string_logic.unpack_string(sptr, hptr)
        assert_true(
            s == "Hello Zero-Copy World",
            "String corruption in transport",
        )

        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()


def test_executor_error_path():
    """Verifies that VM errors are sent without dummy batch allocations."""
    os.write(1, "[IPC] Testing Executor Error Propagation...\n")

    schema = make_test_schema()
    registry = MockRegistry(schema, 42)
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)

    try:
        ipc.send_error(s1.fd, "Fatal VM Crash")

        payload = ipc.receive_payload(s2.fd, registry)
        assert_equal_i(1, payload.status, "Error status mismatch")
        assert_true(
            payload.error_msg == "Fatal VM Crash", "Error string mismatch"
        )
        assert_true(
            payload.batch is None, "Error payload should have no batch"
        )

        payload.close()
    finally:
        s1.close()
        s2.close()


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------


def entry_point(argv):
    os.write(1, "--- GnitzDB IPC Transport Test ---\n")
    try:
        test_unowned_buffer_lifecycle()
        test_ipc_fd_hardening()
        test_ipc_bounds_and_forgery()
        test_zero_copy_roundtrip()
        test_executor_error_path()
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
