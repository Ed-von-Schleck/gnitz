# rpython_tests/master_worker_test.py
#
# Tests for Phase 1: PartitionAssignment, socketpair FFI, fork + IPC round-trip.

import sys
import os

from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import types
from gnitz.core.batch import RowBuilder, ArenaZSetBatch, ZSetBatch
from gnitz.server import ipc, ipc_ffi
from gnitz.server.master import PartitionAssignment, MasterDispatcher
from gnitz.storage.partitioned_table import (
    _partition_for_key,
    make_partitioned_persistent,
    NUM_PARTITIONS,
)
from gnitz.catalog.system_tables import FIRST_USER_TABLE_ID


# -- Helpers -------------------------------------------------------------------


def cleanup_dir(path):
    if not os.path.exists(path):
        return
    for item in os.listdir(path):
        p = os.path.join(path, item)
        if os.path.isdir(p):
            cleanup_dir(p)
        else:
            os.unlink(p)
    os.rmdir(path)


def make_u64_i64_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    return types.TableSchema(cols, 0)


def assert_true(condition, msg):
    if not condition:
        raise Exception("Assertion Failed: " + msg)


def assert_equal_i(expected, actual, msg):
    if expected != actual:
        raise Exception(
            msg + " -> Expected: %d, Actual: %d" % (expected, actual)
        )


def log(msg):
    os.write(1, "[TEST] " + msg + "\n")


# -- Tests ---------------------------------------------------------------------


def test_partition_assignment_coverage():
    log("test_partition_assignment_coverage")
    for num_workers in [1, 2, 4, 7, 16, 64, 256]:
        assignment = PartitionAssignment(num_workers)

        # Verify all 256 partitions are covered
        covered = [0] * 256
        for w in range(num_workers):
            start, end = assignment.range_for_worker(w)
            assert_true(start >= 0, "start must be >= 0")
            assert_true(end <= 256, "end must be <= 256")
            assert_true(start < end, "start must be < end")
            for p in range(start, end):
                covered[p] += 1

        for p in range(256):
            assert_equal_i(
                1, covered[p],
                "partition %d should be covered exactly once (workers=%d)"
                % (p, num_workers),
            )

        # Verify worker_for_partition round-trips
        for p in range(256):
            w = assignment.worker_for_partition(p)
            start, end = assignment.range_for_worker(w)
            assert_true(
                p >= start and p < end,
                "partition %d should be in worker %d's range [%d, %d)"
                % (p, w, start, end),
            )
    log("  PASSED")


def test_partition_assignment_no_overlap():
    log("test_partition_assignment_no_overlap")
    for num_workers in [3, 5, 13]:
        assignment = PartitionAssignment(num_workers)
        seen = {}
        for w in range(num_workers):
            start, end = assignment.range_for_worker(w)
            for p in range(start, end):
                assert_true(
                    p not in seen,
                    "partition %d assigned to both worker %d and %d"
                    % (p, seen.get(p, -1), w),
                )
                seen[p] = w
        assert_equal_i(256, len(seen), "all 256 partitions must be assigned")
    log("  PASSED")


def test_batch_splitting_by_worker():
    log("test_batch_splitting_by_worker")
    schema = make_u64_i64_schema()
    num_workers = 4
    assignment = PartitionAssignment(num_workers)

    # Create a batch with diverse PKs
    batch = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, batch)
    num_rows = 200
    for i in range(num_rows):
        pk = r_uint128(i + 1)
        rb.begin(pk, r_int64(1))
        rb.put_int(r_int64(i * 10))
        rb.commit()

    # Split by worker
    sub_batches = [None] * num_workers
    for i in range(batch.length()):
        pk_lo = r_uint64(batch._read_pk_lo(i))
        pk_hi = r_uint64(batch._read_pk_hi(i))
        p = _partition_for_key(pk_lo, pk_hi)
        w = assignment.worker_for_partition(p)
        if sub_batches[w] is None:
            sub_batches[w] = ArenaZSetBatch(schema)
        sub_batches[w]._direct_append_row(batch, i, batch.get_weight(i))

    # Verify total row count equals original
    total = 0
    for w in range(num_workers):
        if sub_batches[w] is not None:
            total += sub_batches[w].length()
            sub_batches[w].free()
    assert_equal_i(num_rows, total, "split batches should total original row count")

    batch.free()
    log("  PASSED")


def test_socketpair_ffi():
    log("test_socketpair_ffi")
    parent_fd, child_fd = ipc_ffi.create_socketpair()
    assert_true(parent_fd >= 0, "parent_fd must be >= 0")
    assert_true(child_fd >= 0, "child_fd must be >= 0")
    assert_true(parent_fd != child_fd, "fds must be different")

    # Write on one end, read on the other via IPC protocol
    schema = make_u64_i64_schema()
    batch = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, batch)
    rb.begin(r_uint128(42), r_int64(1))
    rb.put_int(r_int64(999))
    rb.commit()

    ipc.send_batch(parent_fd, 100, batch, schema=schema)
    batch.free()

    payload = ipc.receive_payload(child_fd)
    assert_equal_i(100, intmask(payload.target_id), "target_id round-trip")
    assert_true(payload.batch is not None, "batch must be received")
    assert_equal_i(1, payload.batch.length(), "batch must have 1 row")
    payload.close()

    os.close(parent_fd)
    os.close(child_fd)
    log("  PASSED")


def test_fork_ipc_push_round_trip(base_dir):
    log("test_fork_ipc_push_round_trip")
    schema = make_u64_i64_schema()
    d = base_dir + "/fork_push"
    os.mkdir(d)

    parent_fd, child_fd = ipc_ffi.create_socketpair()

    pid = os.fork()
    if pid == 0:
        # Child: receive batch, send ACK
        os.close(parent_fd)
        payload = ipc.receive_payload(child_fd)
        assert_true(payload.batch is not None, "child: batch must be received")
        assert_equal_i(5, payload.batch.length(), "child: batch must have 5 rows")
        payload.close()
        ipc.send_batch(child_fd, 0, None)
        os.close(child_fd)
        os._exit(0)

    # Parent: send batch, receive ACK
    os.close(child_fd)
    batch = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, batch)
    for i in range(5):
        rb.begin(r_uint128(i + 1), r_int64(1))
        rb.put_int(r_int64(i * 100))
        rb.commit()

    ipc.send_batch(parent_fd, 200, batch, schema=schema)
    batch.free()

    ack = ipc.receive_payload(parent_fd)
    ack.close()

    os.close(parent_fd)
    rpid, status = os.waitpid(pid, 0)
    assert_true(rpid == pid, "waitpid should return child pid")
    log("  PASSED")


def test_fork_ipc_scan_round_trip(base_dir):
    log("test_fork_ipc_scan_round_trip")
    schema = make_u64_i64_schema()
    d = base_dir + "/fork_scan"
    os.mkdir(d)

    parent_fd, child_fd = ipc_ffi.create_socketpair()

    pid = os.fork()
    if pid == 0:
        # Child: receive scan request, send back a batch with 3 rows
        os.close(parent_fd)
        payload = ipc.receive_payload(child_fd)
        assert_true(
            payload.batch is None or payload.batch.length() == 0,
            "child: scan request should have empty batch",
        )
        payload.close()

        result = ArenaZSetBatch(schema)
        rb = RowBuilder(schema, result)
        for i in range(3):
            rb.begin(r_uint128(i + 10), r_int64(1))
            rb.put_int(r_int64(i * 50))
            rb.commit()
        ipc.send_batch(child_fd, 0, result, schema=schema)
        result.free()
        os.close(child_fd)
        os._exit(0)

    # Parent: send scan, receive result
    os.close(child_fd)
    ipc.send_batch(parent_fd, 300, None, schema=schema)

    payload = ipc.receive_payload(parent_fd)
    assert_true(payload.batch is not None, "parent: scan result must have batch")
    assert_equal_i(3, payload.batch.length(), "parent: scan result must have 3 rows")
    payload.close()

    os.close(parent_fd)
    rpid, status = os.waitpid(pid, 0)
    assert_true(rpid == pid, "waitpid should return child pid")
    log("  PASSED")


def test_multi_worker_integration(base_dir):
    log("test_multi_worker_integration (4 workers, 200 rows)")
    schema = make_u64_i64_schema()
    num_workers = 4
    assignment = PartitionAssignment(num_workers)

    # Create socketpairs
    pairs = newlist_hint(num_workers)
    for w in range(num_workers):
        p_fd, c_fd = ipc_ffi.create_socketpair()
        pairs.append((p_fd, c_fd))

    pids = newlist_hint(num_workers)
    for w in range(num_workers):
        pid = os.fork()
        if pid == 0:
            # Child: close parent-side fds of all pairs, close other children fds
            for j in range(len(pairs)):
                os.close(pairs[j][0])
            for j in range(len(pairs)):
                if j != w:
                    os.close(pairs[j][1])

            my_fd = pairs[w][1]

            # Simple worker loop: handle one push, one scan, then exit
            # Push
            payload = ipc.receive_payload(my_fd)
            if payload.batch is not None and payload.batch.length() > 0:
                # Store rows in memory for scan response
                stored = payload.batch.clone()
                payload.close()
                ipc.send_batch(my_fd, 0, None)
            else:
                stored = ArenaZSetBatch(schema)
                payload.close()
                ipc.send_batch(my_fd, 0, None)

            # Scan
            payload2 = ipc.receive_payload(my_fd)
            payload2.close()
            ipc.send_batch(my_fd, 0, stored, schema=schema)
            stored.free()

            os.close(my_fd)
            os._exit(0)
        pids.append(pid)

    # Parent: close child-side fds
    for w in range(num_workers):
        os.close(pairs[w][1])

    worker_fds = newlist_hint(num_workers)
    for w in range(num_workers):
        worker_fds.append(pairs[w][0])

    # Create and push 200 rows, split by worker
    batch = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, batch)
    num_rows = 200
    for i in range(num_rows):
        rb.begin(r_uint128(i + 1), r_int64(1))
        rb.put_int(r_int64(i))
        rb.commit()

    # Split by worker
    sub_batches = [None] * num_workers
    for i in range(batch.length()):
        pk_lo = r_uint64(batch._read_pk_lo(i))
        pk_hi = r_uint64(batch._read_pk_hi(i))
        p = _partition_for_key(pk_lo, pk_hi)
        w = assignment.worker_for_partition(p)
        if sub_batches[w] is None:
            sub_batches[w] = ArenaZSetBatch(schema)
        sub_batches[w]._direct_append_row(batch, i, batch.get_weight(i))

    # Send sub-batches and collect ACKs
    sent_workers = newlist_hint(num_workers)
    for w in range(num_workers):
        sb = sub_batches[w]
        if sb is not None:
            ipc.send_batch(worker_fds[w], 0, sb, schema=schema)
            sb.free()
            sent_workers.append(w)
        else:
            # Send empty batch so worker can proceed
            ipc.send_batch(worker_fds[w], 0, None, schema=schema)
            sent_workers.append(w)

    for i in range(len(sent_workers)):
        w = sent_workers[i]
        ack = ipc.receive_payload(worker_fds[w])
        ack.close()

    # Send scan requests and collect responses
    for w in range(num_workers):
        ipc.send_batch(worker_fds[w], 0, None, schema=schema)

    total_scanned = 0
    for w in range(num_workers):
        resp = ipc.receive_payload(worker_fds[w])
        if resp.batch is not None:
            total_scanned += resp.batch.length()
        resp.close()

    assert_equal_i(num_rows, total_scanned, "total scanned rows across all workers")

    # Cleanup
    for w in range(num_workers):
        os.close(worker_fds[w])
    for w in range(len(pids)):
        os.waitpid(pids[w], 0)

    batch.free()
    log("  PASSED")


# -- Entry Point ---------------------------------------------------------------


def entry_point(argv):
    base_dir = "master_worker_test_data"
    if os.path.exists(base_dir):
        cleanup_dir(base_dir)
    os.mkdir(base_dir)

    try:
        test_partition_assignment_coverage()
        test_partition_assignment_no_overlap()
        test_batch_splitting_by_worker()
        test_socketpair_ffi()
        test_fork_ipc_push_round_trip(base_dir)
        test_fork_ipc_scan_round_trip(base_dir)
        test_multi_worker_integration(base_dir)
        os.write(1, "\nALL MASTER/WORKER TESTS PASSED\n")
    except Exception as e:
        os.write(2, "TEST FAILED: " + str(e) + "\n")
        return 1
    finally:
        cleanup_dir(base_dir)

    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
