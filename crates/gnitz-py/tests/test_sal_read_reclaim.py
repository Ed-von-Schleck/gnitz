"""A read-only workload must not exhaust the SAL.

Every read verb (SCAN, SCAN_SPEC, SEEK, SEEK_BY_INDEX, and the view-freshness
tick a view read drives) writes a SAL command group, and nothing on a read path
rewinds the write cursor: space is reclaimed only by a checkpoint, and the
checkpoint decision is made only when a `CommitRequest` reaches the committer —
i.e. only on a write. So a server serving reads alone used to march the cursor to
the end of the mapping and then refuse every read, and the first write that
followed would fatal-abort trying to fit its own checkpoint group.

The watchdog's 100 ms timer now fires a fire-and-forget reclaim barrier once less
than 1/8 of the mapping is free. These tests pin the SAL to its 16 MiB floor and
put the committer's own checkpoint threshold ABOVE that line, so the watchdog is
the only thing that can reclaim: with it removed, the read loops below wedge.
"""
import random
import threading
import time

import pytest
import gnitz
from _serverproc import HANG_TIMEOUT

# Enough to cross the watchdog's line at every worker count the fixture allows.
# The line sits at 14 MiB of the 16 MiB floor; a broadcast scan group is ~576 B
# at 2 workers and ~1120 B at 4, so the narrowest case needs ~25 500 scans.
SCANS = 40_000


def _setup(client, prefix):
    sn = prefix + str(random.randint(100_000, 999_999))
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "grp BIGINT NOT NULL, val BIGINT NOT NULL, tag BIGINT NOT NULL)",
        schema_name=sn,
    )
    client.execute_sql(
        "INSERT INTO t VALUES (1, 1, 10, 100), (2, 1, 20, 200), (3, 2, 30, 300)",
        schema_name=sn,
    )
    tid, _ = client.resolve_table(sn, "t")
    return sn, tid


def _live_rows(client, tid):
    return {row[0]: row[2] for row in client.scan(tid)}


def _read_loop(target, tid, expected, errors, keep_going):
    """Scan `tid` on its own connection while `keep_going()`, recording the first
    mismatch or connection failure in `errors`."""
    try:
        with gnitz.connect(target) as c:
            while keep_going():
                got = _live_rows(c, tid)
                if got != expected:
                    errors.append(f"wrong rows: {got}")
                    return
    except Exception as e:  # a dead master breaks the connection
        errors.append(repr(e))


def _counted(n):
    """A `keep_going` that allows `n` iterations."""
    remaining = iter(range(n))
    return lambda: next(remaining, None) is not None


def test_read_only_workload_does_not_exhaust_the_sal(tiny_sal_server):
    """Reads alone, with no intervening write, must stay healthy across a reclaim
    cycle — and a write afterwards must still commit.

    The loop must stay write-free: a mid-loop push would let the committer
    reclaim on its own and the test would pass with the watchdog trigger gone.
    """
    target, proc = tiny_sal_server
    with gnitz.connect(target) as client:
        sn, tid = _setup(client, "sro_")
        expected = {1: 10, 2: 20, 3: 30}

        errors = []
        _read_loop(target, tid, expected, errors, _counted(SCANS))
        assert not errors, f"read-only scans failed: {errors[:3]}"
        assert proc.poll() is None, "master died on a read-only workload"

        # The first write after a long read-only stretch used to hit a full SAL
        # inside `flush_round` and `_exit(134)`.
        client.execute_sql("INSERT INTO t VALUES (4, 2, 40, 400)", schema_name=sn)
        assert _live_rows(client, tid) == {1: 10, 2: 20, 3: 30, 4: 40}


def test_concurrent_read_only_clients_survive_reclaim(tiny_sal_server):
    """Four independent connections reading across a reclaim boundary.

    At the 16 MiB floor the window between the watchdog's 1/8 line and outright
    exhaustion is only ~900 groups per 100 ms tick, so under enough concurrency a
    clean per-request error is a legitimate outcome — the reserve keeps the
    checkpoint itself writable. What must hold is that the process survives and
    later reads succeed.
    """
    target, proc = tiny_sal_server
    with gnitz.connect(target) as client:
        sn, tid = _setup(client, "src_")
        expected = {1: 10, 2: 20, 3: 30}

        errors = []
        threads = [
            threading.Thread(
                target=_read_loop,
                args=(target, tid, expected, errors, _counted(SCANS // 4)),
            )
            for _ in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(HANG_TIMEOUT)
            assert not t.is_alive(), "reader thread hung — the SAL wedged"

        assert proc.poll() is None, "master died under concurrent read-only load"
        assert not errors, f"concurrent readers failed: {errors[:3]}"
        assert _live_rows(client, tid) == expected


def test_ddl_concurrent_with_reclaim_does_not_deadlock(tiny_sal_server):
    """A CREATE VIEW while the read loop holds the SAL past the watchdog's line.

    The watchdog's barrier must not start a checkpoint sequence inside the DDL's
    quiesce window: the sequence's drain would land in a tick loop the DDL has
    parked, and the DDL's own lock-held barrier resolves only at sequence end —
    a hard deadlock. Run with two concurrent CREATE VIEWs as well: a boolean
    latch instead of a depth passes the single-DDL shape and hangs here, because
    the first DDL to finish clears the window the second is still inside.
    """
    target, proc = tiny_sal_server
    with gnitz.connect(target) as client:
        sn, tid = _setup(client, "sdd_")
        expected = {1: 10, 2: 20, 3: 30}

        stop = threading.Event()
        errors = []
        readers = [
            threading.Thread(
                target=_read_loop,
                args=(target, tid, expected, errors, lambda: not stop.is_set()),
            )
            for _ in range(4)
        ]
        for t in readers:
            t.start()
        try:
            # Push the cursor past the watchdog's line so a reclaim barrier is in
            # flight for the whole DDL window. Four readers move roughly 4 MiB/s
            # of scan groups, against a 14 MiB line.
            time.sleep(6.0)

            ddl_errors = []

            def create_view(name, grp):
                try:
                    with gnitz.connect(target) as c:
                        c.execute_sql(
                            f"CREATE VIEW {name} AS SELECT pk, val FROM t "
                            f"WHERE grp = {grp}",
                            schema_name=sn,
                        )
                except Exception as e:
                    ddl_errors.append(repr(e))

            ddls = [
                threading.Thread(target=create_view, args=("v1", 1)),
                threading.Thread(target=create_view, args=("v2", 2)),
            ]
            for t in ddls:
                t.start()
            for t in ddls:
                t.join(HANG_TIMEOUT)
                assert not t.is_alive(), \
                    "CREATE VIEW hung — a checkpoint ran inside the DDL's quiesce window"
            assert not ddl_errors, f"CREATE VIEW failed: {ddl_errors}"
        finally:
            stop.set()
            for t in readers:
                t.join(HANG_TIMEOUT)

        assert proc.poll() is None, "master died on a DDL concurrent with reclaim"
        assert not errors, f"readers failed during the DDL: {errors[:3]}"

        v1, _ = client.resolve_table(sn, "v1")
        v2, _ = client.resolve_table(sn, "v2")
        assert {r[0]: r[1] for r in client.scan(v1)} == {1: 10, 2: 20}
        assert {r[0]: r[1] for r in client.scan(v2)} == {3: 30}


# `drain_tick_rows_into` empties `tick_tids` BEFORE the tick runs, so a tick that
# fails to emit used to strand its tids: no later Auto re-queued them, only a
# fresh push to that exact tid did, and the drain the reader was waiting on
# signalled success anyway — so the read returned STATUS_OK over a stale view,
# permanently. The emit failure needs a seam; a real one takes a full SAL.


def test_failed_tick_reports_and_requeues(tick_emit_fault_server):
    """The read that waited on the failed tick errors; the next one converges."""
    client = tick_emit_fault_server
    sn = "tef_" + str(random.randint(100_000, 999_999))
    client.create_schema(sn)
    # `tickfault` is the name the fixture armed the seam on.
    client.execute_sql(
        "CREATE TABLE tickfault (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW v AS SELECT pk, val FROM tickfault WHERE val > 5",
        schema_name=sn,
    )
    vid, _ = client.resolve_table(sn, "v")

    client.execute_sql(
        "INSERT INTO tickfault VALUES (1, 10), (2, 20), (3, 1)", schema_name=sn)

    # The tick driving this read's freshness fails, so the read must say so
    # rather than serve an empty view under STATUS_OK.
    with pytest.raises(Exception):
        list(client.scan(vid))

    # The seam is spent and the tid was re-queued, so the next read ticks it.
    expected = {1: 10, 2: 20}
    deadline = time.time() + 30
    got = None
    while time.time() < deadline:
        got = {row[0]: row[1] for row in client.scan(vid)}
        if got == expected:
            break
        time.sleep(0.1)
    assert got == expected, \
        f"view never converged after the failed tick: {got} != {expected}"
