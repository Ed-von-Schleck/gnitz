# rpython_tests/uring_test.py
#
# Tests for the raw io_uring FFI (gnitz/server/uring_ffi.py).
# Exercises NOP, file read/write, Unix socket accept+recv, and batched
# submissions.  Gracefully skips if io_uring is unavailable.

import sys
import os

from rpython.rlib import rposix
from rpython.rlib.rarithmetic import intmask, r_uint64
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.server import uring_ffi, ipc_ffi
from gnitz.core.errors import StorageError
from gnitz.catalog.metadata import ensure_dir
from rpython_tests.helpers.jit_stub import ensure_jit_reachable
from rpython_tests.helpers.assertions import (
    fail,
    assert_true,
    assert_equal_i,
    assert_equal_s,
    assert_equal_u64,
)
from rpython_tests.helpers.fs import cleanup


def log(msg):
    os.write(1, msg + "\n")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_uring_create_destroy():
    log("[uring] test_uring_create_destroy...")
    ring = uring_ffi.uring_create(32)
    uring_ffi.uring_destroy(ring)
    log("    [OK] create + destroy")


def test_uring_nop():
    log("[uring] test_uring_nop...")
    ring = uring_ffi.uring_create(32)
    try:
        uring_ffi.uring_prep_nop(ring, r_uint64(0x42))
        uring_ffi.uring_submit_and_wait(ring, 1)
        count, udata, res, flags = uring_ffi.uring_drain(ring, 16)
        assert_equal_i(1, count, "NOP drain count")
        assert_equal_u64(r_uint64(0x42), udata[0], "NOP user_data")
        assert_equal_i(0, res[0], "NOP res")
    finally:
        uring_ffi.uring_destroy(ring)
    log("    [OK] NOP submit + drain")


def test_uring_read_write(base_dir):
    log("[uring] test_uring_read_write...")
    ring = uring_ffi.uring_create(32)
    path = base_dir + "/uring_rw_test"
    fd = rposix.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    test_data = "hello_io_uring!"  # 15 bytes
    data_len = 15

    write_buf = lltype.malloc(rffi.CCHARP.TO, data_len, flavor='raw')
    read_buf = lltype.malloc(rffi.CCHARP.TO, data_len, flavor='raw')
    try:
        # Fill write buffer
        for i in range(data_len):
            write_buf[i] = test_data[i]

        # Write via io_uring
        uring_ffi.uring_prep_write(
            ring, fd, write_buf, data_len,
            r_uint64(0), r_uint64(1),
        )
        uring_ffi.uring_submit_and_wait(ring, 1)
        count, udata, res, flags = uring_ffi.uring_drain(ring, 16)
        assert_equal_i(1, count, "write drain count")
        assert_equal_u64(r_uint64(1), udata[0], "write user_data")
        assert_equal_i(data_len, res[0], "write res (bytes written)")

        # Zero read buffer
        for i in range(data_len):
            read_buf[i] = '\x00'

        # Read via io_uring
        uring_ffi.uring_prep_read(
            ring, fd, read_buf, data_len,
            r_uint64(0), r_uint64(2),
        )
        uring_ffi.uring_submit_and_wait(ring, 1)
        count, udata, res, flags = uring_ffi.uring_drain(ring, 16)
        assert_equal_i(1, count, "read drain count")
        assert_equal_u64(r_uint64(2), udata[0], "read user_data")
        assert_equal_i(data_len, res[0], "read res (bytes read)")

        got = rffi.charpsize2str(read_buf, data_len)
        assert_equal_s(test_data, got, "read data mismatch")
    finally:
        lltype.free(write_buf, flavor='raw')
        lltype.free(read_buf, flavor='raw')
        rposix.close(fd)
        os.unlink(path)
        uring_ffi.uring_destroy(ring)
    log("    [OK] file read + write")


def test_uring_accept_recv(base_dir):
    log("[uring] test_uring_accept_recv...")
    ring = uring_ffi.uring_create(32)
    sock_path = base_dir + "/uring_test.sock"

    server_fd = ipc_ffi.server_create(sock_path)
    assert_true(server_fd >= 0, "server_create failed")

    recv_buf = lltype.malloc(rffi.CCHARP.TO, 64, flavor='raw')
    client_fd = -1
    accepted_fd = -1

    try:
        # Submit multishot accept
        uring_ffi.uring_prep_accept(ring, server_fd, r_uint64(100))
        uring_ffi.uring_submit_and_wait(ring, 0)

        # Connect from client side
        client_fd = ipc_ffi.unix_connect(sock_path)
        assert_true(client_fd >= 0, "unix_connect failed")

        # Wait for accept completion
        uring_ffi.uring_submit_and_wait(ring, 1)
        count, udata, res, flags = uring_ffi.uring_drain(ring, 16)
        assert_true(count >= 1, "accept drain count >= 1")

        # Find the accept CQE
        found = -1
        for i in range(count):
            if udata[i] == r_uint64(100):
                found = i
        assert_true(found >= 0, "accept CQE not found")
        accepted_fd = res[found]
        assert_true(accepted_fd >= 0, "accepted fd >= 0")
        assert_true(
            (flags[found] & uring_ffi.CQE_F_MORE) != 0,
            "multishot accept should set CQE_F_MORE",
        )

        # Submit recv on the accepted fd
        uring_ffi.uring_prep_recv(
            ring, accepted_fd, recv_buf, 64, r_uint64(200),
        )

        # Send data from client
        os.write(client_fd, "test_uring")

        # Wait for recv completion
        uring_ffi.uring_submit_and_wait(ring, 1)
        count, udata, res, flags = uring_ffi.uring_drain(ring, 16)
        assert_true(count >= 1, "recv drain count >= 1")

        # Find the recv CQE
        found = -1
        for i in range(count):
            if udata[i] == r_uint64(200):
                found = i
        assert_true(found >= 0, "recv CQE not found")
        assert_equal_i(10, res[found], "recv bytes")

        got = rffi.charpsize2str(recv_buf, 10)
        assert_equal_s("test_uring", got, "recv data mismatch")

    finally:
        lltype.free(recv_buf, flavor='raw')
        if accepted_fd >= 0:
            rposix.close(accepted_fd)
        if client_fd >= 0:
            rposix.close(client_fd)
        rposix.close(server_fd)
        uring_ffi.uring_destroy(ring)
        try:
            os.unlink(sock_path)
        except OSError:
            pass
    log("    [OK] accept + recv")


def test_uring_batch_submit():
    log("[uring] test_uring_batch_submit...")
    ring = uring_ffi.uring_create(32)
    try:
        uring_ffi.uring_prep_nop(ring, r_uint64(0x10))
        uring_ffi.uring_prep_nop(ring, r_uint64(0x20))
        uring_ffi.uring_prep_nop(ring, r_uint64(0x30))
        uring_ffi.uring_submit_and_wait(ring, 3)
        count, udata, res, flags = uring_ffi.uring_drain(ring, 16)
        assert_equal_i(3, count, "batch drain count")

        # Collect user_data values (order not guaranteed)
        seen = [0, 0, 0]
        for i in range(3):
            v = udata[i]
            if v == r_uint64(0x10):
                seen[0] = 1
            elif v == r_uint64(0x20):
                seen[1] = 1
            elif v == r_uint64(0x30):
                seen[2] = 1
        assert_equal_i(1, seen[0], "batch: missing 0x10")
        assert_equal_i(1, seen[1], "batch: missing 0x20")
        assert_equal_i(1, seen[2], "batch: missing 0x30")
    finally:
        uring_ffi.uring_destroy(ring)
    log("    [OK] batch submit (3 NOPs)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def entry_point(argv):
    ensure_jit_reachable()
    os.write(1, "--- GnitzDB io_uring FFI Tests ---\n")

    base_dir = "uring_test_data"
    cleanup(base_dir)
    ensure_dir(base_dir)

    # Graceful skip if io_uring is unavailable
    try:
        probe = uring_ffi.uring_create(4)
        uring_ffi.uring_destroy(probe)
    except StorageError:
        os.write(1, "io_uring not available, skipping tests\n")
        cleanup(base_dir)
        return 0

    try:
        test_uring_create_destroy()
        test_uring_nop()
        test_uring_read_write(base_dir)
        test_uring_accept_recv(base_dir)
        test_uring_batch_submit()
        os.write(1, "\nALL IO_URING FFI TESTS PASSED\n")
    except Exception as e:
        os.write(2, "FAILURE\n")
        return 1
    finally:
        cleanup(base_dir)

    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
