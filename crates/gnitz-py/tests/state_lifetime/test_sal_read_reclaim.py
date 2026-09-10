"""A read-only workload must not exhaust the SAL.

Every read verb writes a SAL command group and nothing on a read path rewinds the
write cursor, so reads alone march it to the end of the mapping. Only the
watchdog's reclaim barrier — fired once less than 1/8 of the mapping is free —
can free it without a write.

`tiny_sal_server` puts the committer's own checkpoint threshold ABOVE the
watchdog's line, so the watchdog is the only thing that can reclaim: with it
removed, the read loops below wedge.
"""
import threading
import time

import gnitz
from _read import bag
from _serverproc import join_or_fail

# Most of the 14 MiB to the watchdog's line is bought with wide rows, which is
# far cheaper than ~1 KB scan groups. Sized to stay under the committer's own
# 15 MiB threshold: a committer checkpoint reclaims on its own, and these tests
# would then pass with the watchdog deleted.
_FILL_ROWS, _FILL_BATCH = 1_600, 800
_FILL_PAD = "x" * 8_000

# The rest of the distance, in scans. Measured: the cursor crosses at ~3 200 scans
# (W=2) and ~2 000 (W=4). The reclaim rewinds the cursor to 0, so there is no
# second crossing to buy — the surplus is pressure held on the margin, not more
# distance.
SCANS = 8_000

# A reader's ceiling on an UNBROKEN run of SAL-full refusals. The reclaim that
# clears one is a watchdog tick away, two inside a DDL window.
SAL_FULL_GRACE_S = 5.0

_EXPECTED = {(1, 10): 1, (2, 20): 1, (3, 30): 1}


def _setup(client):
    """`t` with three rows to read back, and `filler` pre-loaded with the wide
    rows that put the SAL write cursor near the watchdog's line."""
    sn = "sal"
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


def _served(client, tid):
    """One scan, retried past the SAL-full refusals a cursor past the watchdog's
    line legitimately produces — matched by exception class, not message text.
    Raises once the refusals outlast `SAL_FULL_GRACE_S`."""
    give_up_at = None
    while True:
        try:
            return _rows(client, tid)
        except gnitz.GnitzSalFullError as e:
            # No backoff: a refused scan writes nothing, and the pressure these
            # readers keep on the cursor is what holds it at the margin under test.
            now = time.monotonic()
            give_up_at = give_up_at or now + SAL_FULL_GRACE_S
            if now > give_up_at:
                raise AssertionError(f"SAL never reclaimed: {e!r}") from e


def _read_loop(target, tid, errors, keep_going):
    """Scan `tid` on its own connection while `keep_going()`, recording the first
    wrong answer, wedge or connection failure in `errors`. One `keep_going` tick
    is one scan, never one attempt."""
    try:
        with gnitz.connect(target) as c:
            while keep_going():
                got = _served(c, tid)
                if got != _EXPECTED:
                    errors.append(f"wrong rows: {got}")
                    return
    except Exception as e:  # a wedge, or a dead master breaking the connection
        errors.append(repr(e))


def _counted(n):
    """A `keep_going` that allows `n` iterations."""
    remaining = iter(range(n))
    return lambda: next(remaining, None) is not None


def test_concurrent_read_only_clients_survive_reclaim(tiny_sal_server):
    """Reads alone, with no intervening write, across a reclaim cycle — from four
    independent connections, then a write that must still commit.

    The loops stay write-free on purpose: a mid-loop push would let the committer
    reclaim on its own and the test would pass with the watchdog trigger gone.
    Per-request refusals under this much concurrency are legitimate and retried;
    the process surviving, no reader wedging, and the write landing are not.
    """
    target, proc = tiny_sal_server
    with gnitz.connect(target) as client:
        sn, tid = _setup(client)

        errors = []
        threads = [threading.Thread(daemon=True, target=_read_loop,
                                    args=(target, tid, errors, _counted(SCANS // 4)))
                   for _ in range(4)]
        for t in threads:
            t.start()
        join_or_fail("reader thread hung — the SAL wedged", *threads)

        assert proc.poll() is None, "master died under concurrent read-only load"
        assert not errors, f"concurrent readers failed: {errors[:3]}"
        assert _rows(client, tid) == _EXPECTED

        # The first write after a long read-only stretch used to hit a full SAL
        # inside `flush_round` and `_exit(134)`.
        client.execute_sql("INSERT INTO t VALUES (4, 2, 40, 400)", schema_name=sn)
        assert _rows(client, tid) == _EXPECTED | {(4, 40): 1}


def test_a_ddl_concurrent_with_reclaim_does_not_deadlock(tiny_sal_server):
    """Two CREATE VIEWs while the read loops hold the SAL past the watchdog's
    line. The watchdog's barrier must not start a checkpoint sequence inside a
    DDL's quiesce window — the drain would land in a tick loop the DDL parked,
    against a barrier that resolves only at sequence end.

    Two of them, not one: a boolean latch rather than a depth passes the
    single-DDL shape and hangs here, the first DDL clearing the window the second
    is still inside.
    """
    target, proc = tiny_sal_server
    with gnitz.connect(target) as client:
        sn, tid = _setup(client)

        # Cross the line before the DDLs, so a reclaim barrier is in flight for
        # the whole window. Counted, not slept: the crossing is a number of scan
        # groups, which no machine speed can under-shoot.
        errors = []
        _read_loop(target, tid, errors, _counted(SCANS))
        assert not errors, f"readers failed before the DDL: {errors[:3]}"

        stop = threading.Event()
        readers = [threading.Thread(daemon=True, target=_read_loop,
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

            ddls = [threading.Thread(daemon=True, target=create_view, args=("v1", 1)),
                    threading.Thread(daemon=True, target=create_view, args=("v2", 2))]
            for t in ddls:
                t.start()
            join_or_fail(
                "CREATE VIEW hung — a checkpoint ran inside the DDL's quiesce window",
                *ddls)
            assert not ddl_errors, f"CREATE VIEW failed: {ddl_errors}"
        finally:
            stop.set()
            join_or_fail("reader thread hung after the DDL", *readers)

        assert proc.poll() is None, "master died on a DDL concurrent with reclaim"
        assert not errors, f"readers failed during the DDL: {errors[:3]}"

        v1, _ = client.resolve_table(sn, "v1")
        v2, _ = client.resolve_table(sn, "v2")
        assert bag(client.scan(v1), "pk", "val") == {(1, 10): 1, (2, 20): 1}
        assert bag(client.scan(v2), "pk", "val") == {(3, 30): 1}
