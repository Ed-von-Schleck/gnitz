"""A read-only workload must not exhaust the SAL.

Every read verb (SCAN, SCAN_SPEC, SEEK, SEEK_BY_INDEX, and the view-freshness
tick a view read drives) writes a SAL command group, and nothing on a read path
rewinds the write cursor: space is reclaimed only by a checkpoint, and the
checkpoint decision is made only when a `CommitRequest` reaches the committer —
i.e. only on a write. So a server serving reads alone used to march the cursor to
the end of the mapping and then refuse every read, and the first write that
followed would fatal-abort trying to fit its own checkpoint group.

The watchdog's 100 ms timer now fires a fire-and-forget reclaim barrier once less
than 1/8 of the mapping is free. `tiny_sal_server` pins the SAL to its 16 MiB
floor and puts the committer's own checkpoint threshold ABOVE that line, so the
watchdog is the only thing that can reclaim: with it removed, the read loops
below wedge.
"""
import threading
import time

import pytest
import gnitz
from _read import bag
from _serverproc import HANG_TIMEOUT
from _uid import uid

# The watchdog's line sits at 14 MiB of the 16 MiB floor. Marching there on ~1 KB
# scan groups alone costs tens of thousands of round trips, so most of the
# distance is bought with one push of wide rows instead: ~8 KB/row puts ~10 MiB
# through the SAL in a fraction of a second. The push must stay clear of the
# committer's own 15 MiB threshold — a committer checkpoint would reclaim on its
# own and the tests would pass with the watchdog deleted.
_FILL_ROWS, _FILL_BATCH = 1_200, 600
_FILL_PAD = "x" * 8_000

# What is left of the distance, in scans. A broadcast scan group is ~576 B at 2
# workers and ~1120 B at 4, so this covers the remaining ~6 MiB at either count
# with room to spare, and keeps the pressure on across the reclaim.
SCANS = 12_000

# An out-of-space SAL refusal (STATUS_SAL_FULL). An ordinary group must leave the
# sentinel headroom and the checkpoint band alone, so once the readers below have
# driven the cursor past the watchdog's line a scan can be refused until the next
# reclaim — by design. A real client retries; so does this loop.
SAL_FULL = gnitz.GnitzSalFullError

# How long a reader tolerates an UNBROKEN run of those before calling it a wedge.
# The reclaim that clears it is at most one 100 ms watchdog tick away — two if a
# DDL window is open, since the watchdog and the committer both refuse to reclaim
# inside one. Anything past this is the failure these tests exist to catch: with
# the watchdog removed the read loops never recover, and this is what still makes
# them say so rather than spin silently until the test's own timeout.
SAL_FULL_GRACE_S = 5.0

_EXPECTED = {(1, 10): 1, (2, 20): 1, (3, 30): 1}


def _setup(client, prefix):
    """`t` with three rows to read back, and `filler` pre-loaded with the wide
    rows that put the SAL write cursor near the watchdog's line."""
    sn = prefix + uid()
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "grp BIGINT NOT NULL, val BIGINT NOT NULL, tag BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 1, 10, 100), (2, 1, 20, 200), (3, 2, 30, 300)",
        schema_name=sn)

    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("pad", gnitz.TypeCode.STRING)]
    filler = client.create_table(sn, "filler", cols)
    schema = gnitz.Schema(cols)
    for lo in range(0, _FILL_ROWS, _FILL_BATCH):
        batch = gnitz.ZSetBatch(schema)
        for i in range(lo, lo + _FILL_BATCH):
            batch.append(pk=i, pad=_FILL_PAD)
        client.push(filler, batch)

    tid, _ = client.resolve_table(sn, "t")
    return sn, tid


def _rows(client, tid):
    return bag(client.scan(tid), "pk", "val")


def _read_loop(target, tid, errors, keep_going):
    """Scan `tid` on its own connection while `keep_going()`, recording the first
    mismatch or connection failure in `errors`. A transient SAL-full refusal is
    retried rather than recorded; one that never clears is recorded. It is matched
    by its exception class, not by the text of a message."""
    try:
        with gnitz.connect(target) as c:
            while keep_going():
                # Retries do NOT consume `keep_going`: a counted loop's budget is
                # a number of scans, not of attempts. That also leaves the grace
                # below as the one thing that ends a wedged reader, instead of it
                # racing a budget that could expire first and exit looking clean.
                # Deliberately no backoff: a refused scan writes nothing, and the
                # pressure these readers keep on the cursor is what holds it on
                # the margin the reclaim path is being tested at. Sleeping here
                # would make the tests pass by not reaching that margin.
                refused_since = None
                while True:
                    try:
                        got = _rows(c, tid)
                        break
                    except Exception as e:
                        if not isinstance(e, SAL_FULL):
                            raise
                        now = time.monotonic()
                        if refused_since is None:
                            refused_since = now
                        elif now - refused_since > SAL_FULL_GRACE_S:
                            errors.append(f"SAL never reclaimed: {e!r}")
                            return
                if got != _EXPECTED:
                    errors.append(f"wrong rows: {got}")
                    return
    except Exception as e:  # a dead master breaks the connection
        errors.append(repr(e))


def _counted(n):
    """A `keep_going` that allows `n` iterations."""
    remaining = iter(range(n))
    return lambda: next(remaining, None) is not None


def test_a_read_only_workload_does_not_exhaust_the_sal(tiny_sal_server):
    """Reads alone, with no intervening write, must stay healthy across a reclaim
    cycle — and a write afterwards must still commit.

    The read loop must stay write-free: a mid-loop push would let the committer
    reclaim on its own and the test would pass with the watchdog trigger gone.
    The fill in `_setup` is before it and cannot.
    """
    target, proc = tiny_sal_server
    with gnitz.connect(target) as client:
        sn, tid = _setup(client, "sro_")

        errors = []
        _read_loop(target, tid, errors, _counted(SCANS))
        assert not errors, f"read-only scans failed: {errors[:3]}"
        assert proc.poll() is None, "master died on a read-only workload"

        # The first write after a long read-only stretch used to hit a full SAL
        # inside `flush_round` and `_exit(134)`.
        client.execute_sql("INSERT INTO t VALUES (4, 2, 40, 400)", schema_name=sn)
        assert _rows(client, tid) == _EXPECTED | {(4, 40): 1}


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
        _, tid = _setup(client, "src_")

        errors = []
        threads = [threading.Thread(target=_read_loop,
                                    args=(target, tid, errors, _counted(SCANS // 4)))
                   for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(HANG_TIMEOUT)
            assert not t.is_alive(), "reader thread hung — the SAL wedged"

        assert proc.poll() is None, "master died under concurrent read-only load"
        assert not errors, f"concurrent readers failed: {errors[:3]}"
        assert _rows(client, tid) == _EXPECTED


def test_a_ddl_concurrent_with_reclaim_does_not_deadlock(tiny_sal_server):
    """Two CREATE VIEWs while the read loop holds the SAL past the watchdog's
    line.

    The watchdog's barrier must not start a checkpoint sequence inside a DDL's
    quiesce window: the sequence's drain would land in a tick loop the DDL has
    parked, and the DDL's own lock-held barrier resolves only at sequence end — a
    hard deadlock. Two concurrent CREATE VIEWs, because a boolean latch instead
    of a depth passes the single-DDL shape and hangs here: the first DDL to
    finish clears the window the second is still inside.
    """
    target, proc = tiny_sal_server
    with gnitz.connect(target) as client:
        sn, tid = _setup(client, "sdd_")

        # Drive the cursor past the watchdog's line before the DDLs, so a reclaim
        # barrier is in flight for the whole window. A counted budget rather than
        # a sleep: the crossing is a number of scan groups, which no machine speed
        # can under-shoot.
        errors = []
        _read_loop(target, tid, errors, _counted(SCANS))
        assert not errors, f"readers failed before the DDL: {errors[:3]}"

        stop = threading.Event()
        readers = [threading.Thread(target=_read_loop,
                                    args=(target, tid, errors, lambda: not stop.is_set()))
                   for _ in range(4)]
        for t in readers:
            t.start()
        try:
            ddl_errors = []

            def create_view(name, grp):
                try:
                    with gnitz.connect(target) as c:
                        c.execute_sql(
                            f"CREATE VIEW {name} AS SELECT pk, val FROM t "
                            f"WHERE grp = {grp}", schema_name=sn)
                except Exception as e:
                    ddl_errors.append(repr(e))

            ddls = [threading.Thread(target=create_view, args=("v1", 1)),
                    threading.Thread(target=create_view, args=("v2", 2))]
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
        assert bag(client.scan(v1), "pk", "val") == {(1, 10): 1, (2, 20): 1}
        assert bag(client.scan(v2), "pk", "val") == {(3, 30): 1}


def test_a_failed_tick_reports_and_requeues(tick_emit_fault_server):
    """`drain_tick_rows_into` empties `tick_tids` BEFORE the tick runs, so a tick
    that fails to emit used to strand its tids: no later Auto re-queued them, only
    a fresh push to that exact tid did, and the drain the reader was waiting on
    signalled success anyway — so the read returned STATUS_OK over a stale view,
    permanently. The emit failure needs a seam; a real one takes a full SAL.

    The read that waited on the failed tick must error rather than serve the
    stale view, and the next read — which ticks the re-queued tid before it is
    served — must be correct."""
    client = tick_emit_fault_server
    sn = "tef_" + uid()
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE tickfault (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT pk, val FROM tickfault WHERE val > 5", schema_name=sn)
    vid, _ = client.resolve_table(sn, "v")

    client.execute_sql("INSERT INTO tickfault VALUES (1, 10), (2, 20), (3, 1)",
                       schema_name=sn)

    with pytest.raises(Exception):
        list(client.scan(vid))

    # The seam is spent and the tid was re-queued, so this read ticks it. A view
    # read is served only once its source closure is at the last completed tick's
    # watermark, so one read is the whole claim — polling for convergence would
    # also pass on a view that converges and then diverges again.
    assert bag(client.scan(vid), "pk", "val") == {(1, 10): 1, (2, 20): 1}
