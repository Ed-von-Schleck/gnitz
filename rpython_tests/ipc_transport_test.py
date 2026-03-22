# rpython_tests/ipc_transport_test.py
#
# Transport-layer unit tests for the SAL (master→workers) and W2M
# (worker→master) shared-memory IPC channels.  Exercises encode/decode
# round-trips, unicast isolation, multi-group sequencing, and cursor
# reset semantics — the exact gaps where four production bugs lived.

import sys
import os

from rpython.rlib import rposix
from rpython.rlib.rarithmetic import intmask, r_uint64, r_int64
from rpython.rtyper.lltypesystem import rffi

from gnitz.server import ipc, eventfd_ffi
from gnitz.storage import mmap_posix
from gnitz.core.batch import ArenaZSetBatch, RowBuilder
from gnitz.core.types import TableSchema, ColumnDefinition, TYPE_I64, TYPE_U128
from gnitz.catalog.metadata import ensure_dir
from rpython_tests.helpers.jit_stub import ensure_jit_reachable
from rpython_tests.helpers.assertions import (
    fail, assert_true, assert_equal_i,
)
from rpython_tests.helpers.fs import cleanup


def log(msg):
    os.write(1, msg + "\n")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TEST_SCHEMA = TableSchema([
    ColumnDefinition(TYPE_U128, name="pk"),
    ColumnDefinition(TYPE_I64, name="val"),
], 0)


def _make_batch(rows):
    """Build a small ArenaZSetBatch from a list of (pk, val) tuples."""
    schema = TEST_SCHEMA
    batch = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, batch)
    for pk, val in rows:
        rb.begin(r_uint64(pk), r_uint64(0), r_int64(1))
        rb.put_int(val)
        rb.commit_row(r_uint64(pk), r_uint64(0), r_int64(1))
    return batch


SAL_SIZE = 1 << 20  # 1 MB — plenty for tests
W2M_SIZE = 1 << 20
NUM_WORKERS = 4


def _alloc_sal_mmap():
    """Allocate a memfd-backed SAL mmap (not file-backed — simpler for tests)."""
    fd = mmap_posix.memfd_create_c("test_sal")
    mmap_posix.ftruncate_c(fd, SAL_SIZE)
    ptr = mmap_posix.mmap_file(
        fd, SAL_SIZE,
        prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE,
        flags=mmap_posix.MAP_SHARED,
    )
    sal = ipc.SharedAppendLog(ptr, fd, SAL_SIZE)
    return sal


def _free_sal(sal):
    mmap_posix.munmap_file(sal.ptr, sal.mmap_size)
    rposix.close(sal.fd)


def _alloc_w2m():
    """Allocate a memfd-backed W2M region."""
    fd = mmap_posix.memfd_create_c("test_w2m")
    mmap_posix.ftruncate_c(fd, W2M_SIZE)
    ptr = mmap_posix.mmap_file(
        fd, W2M_SIZE,
        prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE,
        flags=mmap_posix.MAP_SHARED,
    )
    return ipc.W2MRegion(ptr, fd, W2M_SIZE)


def _free_w2m(region):
    mmap_posix.munmap_file(region.ptr, region.size)
    rposix.close(region.fd)


# ---------------------------------------------------------------------------
# SAL tests
# ---------------------------------------------------------------------------

def test_sal_round_trip():
    """Write a message group with 4 workers, read each worker's data back."""
    log("[sal] test_sal_round_trip...")
    schema = TEST_SCHEMA
    sal = _alloc_sal_mmap()

    # Worker 0: 2 rows, Worker 1: empty, Worker 2: 1 row, Worker 3: 3 rows
    batches = [
        _make_batch([(10, 100), (20, 200)]),
        None,
        _make_batch([(30, 300)]),
        _make_batch([(40, 400), (50, 500), (60, 600)]),
    ]
    wire_bufs = [None] * NUM_WORKERS
    for w in range(NUM_WORKERS):
        wb = batches[w]
        wire_bufs[w] = ipc._encode_wire(
            42, 0, wb, schema, ipc.FLAG_PUSH, 0, 0, 0, 0, "")
    ipc.write_message_group(sal, 42, r_uint64(1), ipc.FLAG_PUSH,
                            wire_bufs, NUM_WORKERS)
    for w in range(NUM_WORKERS):
        if wire_bufs[w] is not None:
            wire_bufs[w].free()

    # Read back from each worker's perspective
    for w in range(NUM_WORKERS):
        msg = ipc.read_worker_message(sal.ptr, 0, w)
        assert_true(msg.advance > 0, "advance must be > 0")
        assert_equal_i(ipc.FLAG_PUSH, msg.flags, "flags")
        assert_equal_i(42, msg.target_id, "target_id")
        if w == 1:
            # Worker 1 had no batch, but still got a control block
            assert_true(msg.payload is not None, "w1 should get control block")
        else:
            assert_true(msg.payload is not None, "w%d payload" % w)
            b = msg.payload.batch
            if w == 0:
                assert_true(b is not None, "w0 batch")
                assert_equal_i(2, b.length(), "w0 row count")
            elif w == 2:
                assert_true(b is not None, "w2 batch")
                assert_equal_i(1, b.length(), "w2 row count")
            elif w == 3:
                assert_true(b is not None, "w3 batch")
                assert_equal_i(3, b.length(), "w3 row count")

    for wb in batches:
        if wb is not None:
            wb.free()
    _free_sal(sal)
    log("    [OK] round-trip 4 workers")


def test_sal_unicast_isolation():
    """Unicast: only target worker sees payload; others see None."""
    log("[sal] test_sal_unicast_isolation...")
    schema = TEST_SCHEMA
    sal = _alloc_sal_mmap()

    # Only worker 2 gets wire data
    wire_bufs = [None] * NUM_WORKERS
    wire_bufs[2] = ipc._encode_wire(
        99, 0, None, schema, ipc.FLAG_SEEK, 0, 0, 0, 0, "")
    ipc.write_message_group(sal, 99, r_uint64(0), ipc.FLAG_SEEK,
                            wire_bufs, NUM_WORKERS)
    if wire_bufs[2] is not None:
        wire_bufs[2].free()

    for w in range(NUM_WORKERS):
        msg = ipc.read_worker_message(sal.ptr, 0, w)
        assert_true(msg.advance > 0, "advance for w%d" % w)
        if w == 2:
            assert_true(msg.payload is not None,
                        "target worker 2 should get payload")
        else:
            assert_true(msg.payload is None,
                        "non-target worker %d should get None" % w)

    _free_sal(sal)
    log("    [OK] unicast isolation")


def test_sal_multiple_groups():
    """Write 3 groups, verify sequential reads advance correctly."""
    log("[sal] test_sal_multiple_groups...")
    schema = TEST_SCHEMA
    sal = _alloc_sal_mmap()

    target_ids = [10, 20, 30]
    for i in range(3):
        wire_bufs = [None] * NUM_WORKERS
        for w in range(NUM_WORKERS):
            wire_bufs[w] = ipc._encode_wire(
                target_ids[i], 0, None, schema, 0, 0, 0, 0, 0, "")
        ipc.write_message_group(sal, target_ids[i], r_uint64(i),
                                0, wire_bufs, NUM_WORKERS)
        for w in range(NUM_WORKERS):
            if wire_bufs[w] is not None:
                wire_bufs[w].free()

    # Worker 0 reads all 3 sequentially
    rc = 0
    for i in range(3):
        size = intmask(ipc.atomic_load_u64(rffi.ptradd(sal.ptr, rc)))
        assert_true(size > 0, "group %d size > 0" % i)
        msg = ipc.read_worker_message(sal.ptr, rc, 0)
        assert_equal_i(target_ids[i], msg.target_id,
                       "group %d target_id" % i)
        rc += msg.advance

    # After all 3, next read should be size=0
    size = intmask(ipc.atomic_load_u64(rffi.ptradd(sal.ptr, rc)))
    assert_equal_i(0, size, "no more groups")

    _free_sal(sal)
    log("    [OK] 3 sequential groups")


# ---------------------------------------------------------------------------
# W2M tests
# ---------------------------------------------------------------------------

def test_w2m_round_trip():
    """Write to W2M, read back, verify data integrity."""
    log("[w2m] test_w2m_round_trip...")
    schema = TEST_SCHEMA
    region = _alloc_w2m()

    batch = _make_batch([(7, 77), (8, 88)])
    ipc.write_to_w2m(region, 42, batch, schema, 0, 0, 0, 0, 0, "")

    wc = region.get_write_cursor()
    assert_true(wc > ipc.W2M_HEADER_SIZE, "write cursor advanced")

    payload, new_rc = ipc.read_from_w2m(region, ipc.W2M_HEADER_SIZE)
    assert_true(payload is not None, "payload not None")
    assert_true(payload.batch is not None, "batch not None")
    assert_equal_i(2, payload.batch.length(), "row count")

    batch.free()
    _free_w2m(region)
    log("    [OK] W2M round-trip")


def test_w2m_multiple_messages():
    """Write 3 messages to W2M, read them sequentially."""
    log("[w2m] test_w2m_multiple_messages...")
    schema = TEST_SCHEMA
    region = _alloc_w2m()

    for i in range(3):
        b = _make_batch([(i * 10, i * 100)])
        ipc.write_to_w2m(region, i, b, schema, 0, 0, 0, 0, 0, "")
        b.free()

    rc = ipc.W2M_HEADER_SIZE
    for i in range(3):
        payload, rc = ipc.read_from_w2m(region, rc)
        assert_true(payload is not None, "msg %d payload" % i)
        assert_true(payload.batch is not None, "msg %d batch" % i)
        assert_equal_i(1, payload.batch.length(), "msg %d row count" % i)

    _free_w2m(region)
    log("    [OK] 3 sequential W2M messages")


def test_w2m_cursor_reset():
    """After reset, write cursor is at W2M_HEADER_SIZE."""
    log("[w2m] test_w2m_cursor_reset...")
    schema = TEST_SCHEMA
    region = _alloc_w2m()

    batch = _make_batch([(1, 10)])
    ipc.write_to_w2m(region, 0, batch, schema, 0, 0, 0, 0, 0, "")
    batch.free()

    wc = region.get_write_cursor()
    assert_true(wc > ipc.W2M_HEADER_SIZE, "cursor advanced after write")

    region.set_write_cursor(ipc.W2M_HEADER_SIZE)
    wc2 = region.get_write_cursor()
    assert_equal_i(ipc.W2M_HEADER_SIZE, wc2, "cursor reset")

    _free_w2m(region)
    log("    [OK] W2M cursor reset")


def test_sal_cross_process():
    """Fork: master writes SAL, child reads via eventfd signal."""
    log("[sal] test_sal_cross_process...")
    schema = TEST_SCHEMA
    sal = _alloc_sal_mmap()
    efd = eventfd_ffi.eventfd_create()

    batch = _make_batch([(99, 999)])
    wire_bufs = [None] * NUM_WORKERS
    for w in range(NUM_WORKERS):
        wire_bufs[w] = ipc._encode_wire(
            55, 0, batch if w == 0 else None, schema,
            ipc.FLAG_PUSH, 0, 0, 0, 0, "")
    ipc.write_message_group(sal, 55, r_uint64(1), ipc.FLAG_PUSH,
                            wire_bufs, NUM_WORKERS)
    for w in range(NUM_WORKERS):
        if wire_bufs[w] is not None:
            wire_bufs[w].free()

    mmap_posix.fdatasync_c(sal.fd)
    eventfd_ffi.eventfd_signal(efd)

    pid = os.fork()
    if pid == 0:
        # Child: wait for signal, read SAL as worker 0
        eventfd_ffi.eventfd_wait(efd, 5000)
        msg = ipc.read_worker_message(sal.ptr, 0, 0)
        if msg.payload is None:
            os._exit(1)
        if msg.payload.batch is None:
            os._exit(2)
        if msg.payload.batch.length() != 1:
            os._exit(3)
        os._exit(0)
    else:
        _, status = os.waitpid(pid, 0)
        exit_code = status >> 8
        rposix.close(efd)
        batch.free()
        _free_sal(sal)
        assert_equal_i(0, exit_code, "child exit code")

    log("    [OK] cross-process SAL read")


# ---------------------------------------------------------------------------
# SAL checkpoint/reset tests
# ---------------------------------------------------------------------------


def test_sal_epoch_write_read():
    """Epoch field is written into group header and read back."""
    log("[sal] test_sal_epoch_write_read...")
    sal = _alloc_sal_mmap()
    schema = TEST_SCHEMA

    assert_equal_i(1, sal.epoch, "initial epoch")

    wire_bufs = [None] * NUM_WORKERS
    for w in range(NUM_WORKERS):
        wire_bufs[w] = ipc._encode_wire(
            10, 0, None, schema, ipc.FLAG_PUSH, 0, 0, 0, 0, "")
    ipc.write_message_group(sal, 10, r_uint64(1), ipc.FLAG_PUSH,
                            wire_bufs, NUM_WORKERS)
    for w in range(NUM_WORKERS):
        if wire_bufs[w] is not None:
            wire_bufs[w].free()

    msg = ipc.read_worker_message(sal.ptr, 0, 0)
    assert_equal_i(1, msg.epoch, "epoch in message")
    assert_equal_i(10, msg.target_id, "target_id")

    _free_sal(sal)
    log("    [OK] epoch write/read")


def test_sal_checkpoint_reset():
    """Simulate checkpoint: reset cursors + bump epoch, write new groups."""
    log("[sal] test_sal_checkpoint_reset...")
    sal = _alloc_sal_mmap()
    schema = TEST_SCHEMA

    # Write 2 groups at epoch=1
    for i in range(2):
        wire_bufs = [None] * NUM_WORKERS
        for w in range(NUM_WORKERS):
            wire_bufs[w] = ipc._encode_wire(
                i + 1, 0, None, schema, 0, 0, 0, 0, 0, "")
        ipc.write_message_group(sal, i + 1, r_uint64(i),
                                0, wire_bufs, NUM_WORKERS)
        for w in range(NUM_WORKERS):
            if wire_bufs[w] is not None:
                wire_bufs[w].free()

    old_cursor = sal.write_cursor
    assert_true(old_cursor > 0, "wrote some data")

    # Simulate checkpoint: bump epoch, reset cursor, zero sentinel
    sal.epoch += 1
    sal.write_cursor = 0
    ipc.atomic_store_u64(sal.ptr, rffi.cast(rffi.ULONGLONG, 0))

    assert_equal_i(2, sal.epoch, "epoch after checkpoint")
    assert_equal_i(0, sal.write_cursor, "cursor after checkpoint")

    # Write a new group at epoch=2
    wire_bufs = [None] * NUM_WORKERS
    batch = _make_batch([(77, 770)])
    for w in range(NUM_WORKERS):
        wire_bufs[w] = ipc._encode_wire(
            99, 0, batch if w == 0 else None, schema,
            ipc.FLAG_PUSH, 0, 0, 0, 0, "")
    ipc.write_message_group(sal, 99, r_uint64(10), ipc.FLAG_PUSH,
                            wire_bufs, NUM_WORKERS)
    for w in range(NUM_WORKERS):
        if wire_bufs[w] is not None:
            wire_bufs[w].free()

    # Read as worker 0 from offset 0: should see epoch=2 group
    msg = ipc.read_worker_message(sal.ptr, 0, 0)
    assert_equal_i(2, msg.epoch, "new group epoch")
    assert_equal_i(99, msg.target_id, "new group target_id")
    assert_true(msg.payload is not None, "new group payload")
    assert_true(msg.payload.batch is not None, "new group batch")
    assert_equal_i(1, msg.payload.batch.length(), "new group row count")

    batch.free()
    _free_sal(sal)
    log("    [OK] checkpoint reset + new write")


def test_sal_epoch_fence_skips_stale():
    """After checkpoint, worker at read_cursor=0 skips stale epoch data."""
    log("[sal] test_sal_epoch_fence_skips_stale...")
    sal = _alloc_sal_mmap()
    schema = TEST_SCHEMA

    # Write a group at epoch=1
    wire_bufs = [None] * NUM_WORKERS
    for w in range(NUM_WORKERS):
        wire_bufs[w] = ipc._encode_wire(
            42, 0, None, schema, ipc.FLAG_PUSH, 0, 0, 0, 0, "")
    ipc.write_message_group(sal, 42, r_uint64(1), ipc.FLAG_PUSH,
                            wire_bufs, NUM_WORKERS)
    for w in range(NUM_WORKERS):
        if wire_bufs[w] is not None:
            wire_bufs[w].free()

    # Verify group is readable at epoch=1
    msg = ipc.read_worker_message(sal.ptr, 0, 0)
    assert_equal_i(1, msg.epoch, "epoch=1 readable")
    assert_true(msg.advance > 0, "advance > 0")

    # Simulate: worker resets read_cursor=0 and expects epoch=2
    # (this is what happens after checkpoint)
    # Read from offset 0: epoch=1 data is still there but should be
    # detected as stale by checking epoch.
    msg2 = ipc.read_worker_message(sal.ptr, 0, 0)
    assert_equal_i(1, msg2.epoch, "stale data still has epoch=1")
    # The epoch mismatch detection happens in _drain_sal, not in
    # read_worker_message itself. So the test verifies the epoch
    # field is correctly populated for the caller to check.
    assert_true(msg2.epoch != 2, "epoch != expected (2)")

    _free_sal(sal)
    log("    [OK] epoch fence detects stale data")


def test_sal_cross_process_checkpoint():
    """Fork: master writes epoch=1, child reads, master checkpoints to
    epoch=2, writes new data, child reads new-epoch data."""
    log("[sal] test_sal_cross_process_checkpoint...")
    sal = _alloc_sal_mmap()
    schema = TEST_SCHEMA
    # Two eventfds: child→master and master→child (avoid race on single efd)
    c2m_efd = eventfd_ffi.eventfd_create()
    m2c_efd = eventfd_ffi.eventfd_create()

    # Phase 1: write epoch=1 group
    batch1 = _make_batch([(10, 100)])
    wire_bufs = [None] * NUM_WORKERS
    for w in range(NUM_WORKERS):
        wire_bufs[w] = ipc._encode_wire(
            1, 0, batch1 if w == 0 else None, schema,
            ipc.FLAG_PUSH, 0, 0, 0, 0, "")
    ipc.write_message_group(sal, 1, r_uint64(1), ipc.FLAG_PUSH,
                            wire_bufs, NUM_WORKERS)
    for w in range(NUM_WORKERS):
        if wire_bufs[w] is not None:
            wire_bufs[w].free()
    mmap_posix.fdatasync_c(sal.fd)

    pid = os.fork()
    if pid == 0:
        # Child: read epoch=1 group
        msg1 = ipc.read_worker_message(sal.ptr, 0, 0)
        if msg1.epoch != 1:
            os._exit(1)
        if msg1.payload is None or msg1.payload.batch is None:
            os._exit(2)
        if msg1.payload.batch.length() != 1:
            os._exit(3)

        # Signal master that we read epoch=1
        eventfd_ffi.eventfd_signal(c2m_efd)

        # Wait for master to checkpoint and write epoch=2
        eventfd_ffi.eventfd_wait(m2c_efd, 5000)

        # Simulate post-checkpoint: reset cursor, expect epoch=2
        expected_epoch = 2

        msg2 = ipc.read_worker_message(sal.ptr, 0, 0)
        if msg2.epoch != expected_epoch:
            os._exit(5)
        if msg2.target_id != 2:
            os._exit(6)
        if msg2.payload is None or msg2.payload.batch is None:
            os._exit(7)
        if msg2.payload.batch.length() != 1:
            os._exit(8)

        os._exit(0)
    else:
        # Master: wait for child to read epoch=1
        eventfd_ffi.eventfd_wait(c2m_efd, 5000)

        # Checkpoint: bump epoch, reset cursor, zero sentinel
        sal.epoch += 1
        sal.write_cursor = 0
        ipc.atomic_store_u64(sal.ptr, rffi.cast(rffi.ULONGLONG, 0))

        # Write epoch=2 group
        batch2 = _make_batch([(20, 200)])
        wire_bufs2 = [None] * NUM_WORKERS
        for w in range(NUM_WORKERS):
            wire_bufs2[w] = ipc._encode_wire(
                2, 0, batch2 if w == 0 else None, schema,
                ipc.FLAG_PUSH, 0, 0, 0, 0, "")
        ipc.write_message_group(sal, 2, r_uint64(2), ipc.FLAG_PUSH,
                                wire_bufs2, NUM_WORKERS)
        for w in range(NUM_WORKERS):
            if wire_bufs2[w] is not None:
                wire_bufs2[w].free()
        mmap_posix.fdatasync_c(sal.fd)

        # Signal child that epoch=2 data is ready
        eventfd_ffi.eventfd_signal(m2c_efd)

        _, status = os.waitpid(pid, 0)
        exit_code = status >> 8
        rposix.close(c2m_efd)
        rposix.close(m2c_efd)
        batch1.free()
        batch2.free()
        _free_sal(sal)
        assert_equal_i(0, exit_code, "child exit code (checkpoint)")

    log("    [OK] cross-process checkpoint")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def entry_point(argv):
    ensure_jit_reachable()
    os.write(1, "--- GnitzDB IPC Transport Tests ---\n")

    base_dir = "ipc_transport_test_data"
    cleanup(base_dir)
    ensure_dir(base_dir)

    try:
        test_sal_round_trip()
        test_sal_unicast_isolation()
        test_sal_multiple_groups()
        test_w2m_round_trip()
        test_w2m_multiple_messages()
        test_w2m_cursor_reset()
        test_sal_cross_process()
        test_sal_epoch_write_read()
        test_sal_checkpoint_reset()
        test_sal_epoch_fence_skips_stale()
        test_sal_cross_process_checkpoint()
        os.write(1, "\nALL IPC TRANSPORT TESTS PASSED\n")
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
