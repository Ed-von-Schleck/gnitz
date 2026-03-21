# rpython_tests/eventfd_test.py
#
# Tests for eventfd FFI helpers, SAL file setup helpers (fallocate, NOCOW),
# and fdatasync_c. Validates the foundation modules for the SAL transport.

import sys
import os

from rpython.rlib import rposix
from rpython.rlib.rarithmetic import intmask

from gnitz.server import eventfd_ffi, sal_ffi
from gnitz.storage import mmap_posix
from gnitz.catalog.metadata import ensure_dir
from rpython_tests.helpers.jit_stub import ensure_jit_reachable
from rpython_tests.helpers.assertions import fail, assert_true, assert_equal_i
from rpython_tests.helpers.fs import cleanup


def log(msg):
    os.write(1, msg + "\n")


# ---------------------------------------------------------------------------
# eventfd tests
# ---------------------------------------------------------------------------

def test_eventfd_create_close():
    log("[eventfd] test_eventfd_create_close...")
    fd = eventfd_ffi.eventfd_create()
    assert_true(fd >= 0, "eventfd_create returned negative fd")
    rposix.close(fd)
    log("    [OK] create + close")


def test_eventfd_signal_wait():
    log("[eventfd] test_eventfd_signal_wait...")
    fd = eventfd_ffi.eventfd_create()
    eventfd_ffi.eventfd_signal(fd)
    r = eventfd_ffi.eventfd_wait(fd, 1000)
    assert_true(r > 0, "eventfd_wait should return >0 after signal")
    rposix.close(fd)
    log("    [OK] signal + wait round-trip")


def test_eventfd_wait_timeout():
    log("[eventfd] test_eventfd_wait_timeout...")
    fd = eventfd_ffi.eventfd_create()
    r = eventfd_ffi.eventfd_wait(fd, 10)
    assert_equal_i(0, r, "eventfd_wait should return 0 on timeout")
    rposix.close(fd)
    log("    [OK] wait timeout")


def test_eventfd_wait_any():
    log("[eventfd] test_eventfd_wait_any...")
    fd0 = eventfd_ffi.eventfd_create()
    fd1 = eventfd_ffi.eventfd_create()
    fd2 = eventfd_ffi.eventfd_create()
    efd_list = [fd0, fd1, fd2]

    # Signal only the middle fd
    eventfd_ffi.eventfd_signal(fd1)
    r = eventfd_ffi.eventfd_wait_any(efd_list, 1000)
    assert_true(r > 0, "eventfd_wait_any should return >0 when one fd ready")

    # After drain, none should be ready
    r2 = eventfd_ffi.eventfd_wait_any(efd_list, 10)
    assert_equal_i(0, r2, "eventfd_wait_any should return 0 after drain")

    rposix.close(fd0)
    rposix.close(fd1)
    rposix.close(fd2)
    log("    [OK] wait_any multi-fd")


# ---------------------------------------------------------------------------
# SAL file setup tests
# ---------------------------------------------------------------------------

def test_fallocate(base_dir):
    log("[sal_ffi] test_fallocate...")
    path = base_dir + "/fallocate_test"
    fd = rposix.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        sal_ffi.fallocate_c(fd, 1048576)  # 1 MB
        size = intmask(mmap_posix.fget_size(fd))
        assert_equal_i(1048576, size, "fallocate should set file size to 1 MB")
    finally:
        rposix.close(fd)
        os.unlink(path)
    log("    [OK] fallocate 1 MB")


def test_try_set_nocow(base_dir):
    log("[sal_ffi] test_try_set_nocow...")
    path = base_dir + "/nocow_test"
    fd = rposix.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        # Should not crash on any filesystem. On non-btrfs, the ioctl
        # returns ENOTTY and the wrapper silently ignores it.
        sal_ffi.try_set_nocow(fd)
    finally:
        rposix.close(fd)
        os.unlink(path)
    log("    [OK] try_set_nocow (silent on non-btrfs)")


# ---------------------------------------------------------------------------
# fdatasync test
# ---------------------------------------------------------------------------

def test_fdatasync(base_dir):
    log("[mmap_posix] test_fdatasync...")
    path = base_dir + "/fdatasync_test"
    fd = rposix.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, "hello fdatasync")
        mmap_posix.fdatasync_c(fd)
    finally:
        rposix.close(fd)
        os.unlink(path)
    log("    [OK] fdatasync_c")


# ---------------------------------------------------------------------------
# Cross-process mmap + volatile test
# ---------------------------------------------------------------------------

def test_w2m_cross_process():
    """Verify that volatile writes to a memfd mmap are visible across fork."""
    from gnitz.server.ipc import W2MRegion, W2M_HEADER_SIZE, atomic_load_u64, atomic_store_u64
    from rpython.rtyper.lltypesystem import rffi

    log("[w2m] test_w2m_cross_process...")
    fd = mmap_posix.memfd_create_c("test_w2m")
    mmap_posix.ftruncate_c(fd, 4096)
    ptr = mmap_posix.mmap_file(
        fd, 4096,
        prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE,
        flags=mmap_posix.MAP_SHARED,
    )
    # Write initial value
    atomic_store_u64(ptr, rffi.cast(rffi.ULONGLONG, 42))
    val = intmask(atomic_load_u64(ptr))
    assert_equal_i(42, val, "initial volatile read")

    efd = eventfd_ffi.eventfd_create()

    pid = os.fork()
    if pid == 0:
        # Child: write a new value and signal
        atomic_store_u64(ptr, rffi.cast(rffi.ULONGLONG, 9999))
        eventfd_ffi.eventfd_signal(efd)
        os._exit(0)
    else:
        # Parent: wait for signal then read
        eventfd_ffi.eventfd_wait(efd, 5000)
        val2 = intmask(atomic_load_u64(ptr))
        os.waitpid(pid, 0)
        mmap_posix.munmap_file(ptr, 4096)
        rposix.close(fd)
        rposix.close(efd)
        assert_equal_i(9999, val2, "cross-process volatile read after fork")
    log("    [OK] cross-process mmap volatile")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def entry_point(argv):
    ensure_jit_reachable()
    os.write(1, "--- GnitzDB eventfd + SAL FFI Tests ---\n")

    base_dir = "eventfd_test_data"
    cleanup(base_dir)
    ensure_dir(base_dir)

    try:
        test_eventfd_create_close()
        test_eventfd_signal_wait()
        test_eventfd_wait_timeout()
        test_eventfd_wait_any()
        test_fallocate(base_dir)
        test_try_set_nocow(base_dir)
        test_fdatasync(base_dir)
        test_w2m_cross_process()
        os.write(1, "\nALL EVENTFD/SAL FFI TESTS PASSED\n")
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
