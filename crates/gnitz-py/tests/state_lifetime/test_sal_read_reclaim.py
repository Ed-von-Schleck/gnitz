"""A read-only workload must not exhaust the SAL.

Every read verb writes a SAL command group and nothing on a read path rewinds the
write cursor, so reads alone march it to the end of the mapping. The committer
checks its checkpoint threshold only when something is sent to it, which a
read-only workload never does, so only the watchdog's reclaim wake — sent once
the cursor crosses that threshold, to make the committer re-test it — can free
it without a write.

The `TINY_SAL` threshold is high enough that the pushes below stop short of it
and only the reads cross it: with the watchdog removed, the read loops wedge.
"""
import threading
import time

import gnitz
import pytest
from _read import bag
from _serverproc import TINY_SAL, join_or_fail, spawn

# Most of the 15 MiB to the checkpoint threshold is bought with wide rows, which
# is far cheaper than ~1 KB scan groups. Sized to stop short of the threshold: a
# push past it would let the committer reclaim on its own, and these tests would
# then pass with the watchdog deleted.
_FILL_ROWS, _FILL_BATCH = 1_700, 850
_FILL_PAD = "x" * 8_000

# The rest of the distance, in scans, with a wide surplus. The reclaim rewinds
# the cursor to 0, so there is no second crossing to buy — the surplus is
# pressure held on the threshold, not more distance.
SCANS = 8_000

# A reader's ceiling on an UNBROKEN run of SAL-full refusals. The reclaim that
# clears one is a watchdog tick away, two inside a DDL window.
SAL_FULL_GRACE_S = 5.0

_EXPECTED = {(1, 10): 1, (2, 20): 1, (3, 30): 1}


@pytest.fixture
def target(own_server):
    return own_server.start(extra_env=TINY_SAL).target


def _setup(client):
    """`t` with three rows to read back, and `filler` pre-loaded with the wide
    rows that put the SAL write cursor just short of the checkpoint threshold.
    Returns `t`'s id and schema."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "grp BIGINT NOT NULL, val BIGINT NOT NULL, tag BIGINT NOT NULL)")
    client.execute_sql(
        "INSERT INTO t VALUES (1, 1, 10, 100), (2, 1, 20, 200), (3, 2, 30, 300)")

    schema = gnitz.Schema([gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
                           gnitz.ColumnDef("pad", gnitz.TypeCode.STRING)], [0])
    filler = client.create_table("filler", schema)
    for lo in range(0, _FILL_ROWS, _FILL_BATCH):
        client.push(filler, gnitz.ZSetBatch(schema).extend(
            {"pk": i, "pad": _FILL_PAD} for i in range(lo, lo + _FILL_BATCH)))

    return client.resolve_table("t")


def _rows(client, rel):
    return bag(client.scan(*rel), "pk", "val")


def _served(client, rel):
    """One scan, retried past the SAL-full refusals a cursor past the threshold
    legitimately produces — matched by exception class, not message text.
    Raises once the refusals outlast `SAL_FULL_GRACE_S`."""
    give_up_at = None
    while True:
        try:
            return _rows(client, rel)
        except gnitz.GnitzSalFullError as e:
            # No backoff: a refused scan writes nothing, and the pressure these
            # readers keep on the cursor is what holds it at the margin under test.
            now = time.monotonic()
            give_up_at = give_up_at or now + SAL_FULL_GRACE_S
            if now > give_up_at:
                raise AssertionError(f"SAL never reclaimed: {e!r}") from e


def _read_loop(target, rel, keep_going):
    """Scan `rel` on its own connection while `keep_going()`, raising on the
    first wrong answer, wedge or connection failure. One `keep_going` tick is one
    scan, never one attempt."""
    with gnitz.connect(target) as c:
        while keep_going():
            assert _served(c, rel) == _EXPECTED


def _counted(n):
    """A `keep_going` that allows `n` iterations."""
    remaining = iter(range(n))
    return lambda: next(remaining, None) is not None


def test_concurrent_read_only_clients_survive_reclaim(own_server, target):
    """Reads alone, with no intervening write, across a reclaim cycle — from four
    independent connections, then a write that must still commit.

    The loops stay write-free on purpose: a mid-loop push would let the committer
    reclaim on its own and the test would pass with the watchdog trigger gone.
    Per-request refusals under this much concurrency are legitimate and retried;
    the process surviving, no reader wedging, and the write landing are not.
    """
    client = gnitz.connect(target)
    rel = _setup(client)

    join_or_fail("reader thread hung — the SAL wedged",
                 *[spawn(_read_loop, target, rel, _counted(SCANS // 4)) for _ in range(4)])

    assert own_server.proc.poll() is None, "master died under concurrent read-only load"
    assert _rows(client, rel) == _EXPECTED

    # The first write after a long read-only stretch used to hit a full SAL
    # inside `flush_round` and `_exit(134)`.
    client.execute_sql("INSERT INTO t VALUES (4, 2, 40, 400)")
    assert _rows(client, rel) == _EXPECTED | {(4, 40): 1}


def test_a_ddl_concurrent_with_reclaim_does_not_deadlock(own_server, target):
    """Two CREATE VIEWs while the read loops hold the SAL past the checkpoint
    threshold. A checkpoint sequence the watchdog's reclaim wake starts must not
    run while a DDL holds the tick gate — its drain would wait on a tick the DDL
    keeps out.

    Two of them, not one: the second DDL queues on the gate the first holds.
    """
    client = gnitz.connect(target)
    rel = _setup(client)

    # Cross the threshold before the DDLs, so the watchdog sends reclaim wakes
    # for the whole window. Counted, not slept: the crossing is a number of scan
    # groups, which no machine speed can under-shoot.
    _read_loop(target, rel, _counted(SCANS))

    stop = threading.Event()
    readers = [spawn(_read_loop, target, rel, lambda: not stop.is_set()) for _ in range(4)]
    try:
        def create_view(name, grp):
            with gnitz.connect(target) as c:
                c.execute_sql(f"CREATE VIEW {name} AS SELECT pk, val FROM t WHERE grp = {grp}")

        join_or_fail(
            "CREATE VIEW hung — a checkpoint ran while a DDL held the tick gate",
            spawn(create_view, "v1", 1), spawn(create_view, "v2", 2))
    finally:
        stop.set()
        join_or_fail("reader thread hung after the DDL", *readers)

    assert own_server.proc.poll() is None, "master died on a DDL concurrent with reclaim"
    assert bag(client.scan(*client.resolve_table("v1")), "pk", "val") == {(1, 10): 1, (2, 20): 1}
    assert bag(client.scan(*client.resolve_table("v2")), "pk", "val") == {(3, 30): 1}
