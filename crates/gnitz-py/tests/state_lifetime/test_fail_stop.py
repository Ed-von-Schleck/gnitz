"""An error applying or emitting committed state must fail stop, never be
swallowed.

Committed state reaches a worker twice: once as the live PUSH apply, and once as
the SAL replay a boot runs before the master rewinds the log. Swallowing an error
on either path leaves the worker diverged from the durable SAL while the client
holds an ACK — and the next reset then destroys the only copy that could still
repair it. Aborting instead keeps the SAL intact for the following boot, which is
what the crash tests here read back.
"""

import pytest
import gnitz
from _read import bag, scanned
from _sql import values

_ROWS = "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)"
_WANT = {(1, 100): 1, (2, 200): 1, (3, 300): 1}

# Each seam fails a different apply of already-committed state during boot, and
# the worker logs the failure it aborts on. Both must abort before the SAL reset,
# so the pre-crash rows survive for the next boot: the worker base-table flush,
# and the worker's replay of a committed PUSH zone.
_BOOT_SEAMS = {
    "worker_boot_flush": ({"GNITZ_INJECT_BOOT_FLUSH_ERROR": "1"}, "injected boot flush fault"),
    "worker_replay_apply": ({"GNITZ_INJECT_INGEST_APPLY_ERROR": "store"}, "SAL replay apply failed"),
}


@pytest.mark.parametrize("seam", list(_BOOT_SEAMS))
def test_a_failed_boot_leaves_the_sal_intact(seam, own_server):
    """Create and insert, SIGKILL before any checkpoint so the rows live only in
    the SAL and the memtable, then boot into the fault. The boot must abort
    before it is ready, and the next clean boot must recover every row —
    which it can only do if the failed boot left the SAL alone."""
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)")
        conn.execute_sql(_ROWS)
    # Hard kill: the data directory takes one live process, so the failed boot
    # below must not race a still-running server.
    own_server.stop()

    env, marker = _BOOT_SEAMS[seam]
    own_server.start_expecting_exit(marker, extra_env=env)

    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        assert bag(scanned(conn, "t"), "pk", "val") == _WANT


@pytest.mark.parametrize("inject", ["store", "index"])
def test_a_failed_live_apply_aborts_the_cluster_and_replays(inject, own_server):
    """A storage error applying a committed PUSH aborts the owning worker inside
    the relation ingest, and the master's watchdog turns the dead worker
    into a cluster shutdown — exit 2, which neither a clean shutdown (0) nor a
    boot failure (1) can produce, so the status alone proves the abort fired. The
    load-bearing assertion is the next boot: the un-flushed PUSH zone stayed above
    the watermark and was replayed, so the rows the swallow would have orphaned
    are all there."""
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)")
        if inject == "index":
            conn.execute_sql("CREATE INDEX ON t(val)")

    rc = own_server.exit_code_on(_ROWS, {"GNITZ_INJECT_INGEST_APPLY_ERROR": inject})
    assert rc == 2, f"a worker crash must exit 2, got {rc}"

    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        assert bag(scanned(conn, "t"), "pk", "val") == _WANT


def test_a_restamp_owed_by_a_failed_drain_keeps_later_pushes_across_a_crash(own_server):
    """Pushes committed behind a checkpoint whose drain failed survive the
    restamp the next DDL owes, and a crash after it."""
    own_server.start(workers=4, extra_env={"GNITZ_CHECKPOINT_BYTES": "65536",
                                           "GNITZ_INJECT_TICK_EMIT_ERROR": "1"})
    pad = "x" * 64
    rows = [(pk, pad) for pk in range(2003)]
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        conn.execute_sql("CREATE VIEW f AS SELECT pk, v FROM t WHERE pk % 2 = 0")
        conn.execute_sql(f"INSERT INTO t VALUES {values(rows[:2000])}")
        conn.execute_sql(f"INSERT INTO t VALUES {values(rows[2000:])}")
        assert "checkpoint drain failed, skipping the ephemeral round" in own_server.log_text(), \
            "the checkpoint's drain must have failed, or no restamp is owed"
        conn.execute_sql("CREATE TABLE u (pk BIGINT NOT NULL PRIMARY KEY)")

    own_server.restart()
    with gnitz.connect(own_server.target) as conn:
        assert bag(scanned(conn, "t"), "pk", "v") == {r: 1 for r in rows}
        assert bag(scanned(conn, "f"), "pk", "v") == {r: 1 for r in rows if r[0] % 2 == 0}

