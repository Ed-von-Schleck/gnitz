"""An error applying or emitting committed state must fail stop, never be
swallowed.

Committed state reaches a worker twice: once as the live PUSH apply, and once as
the SAL replay a boot runs before the master rewinds the log. Swallowing an error
on either path leaves the worker diverged from the durable SAL while the client
holds an ACK — and the next reset then destroys the only copy that could still
repair it. Aborting instead keeps the SAL intact for the following boot, which is
what the crash tests here read back.

The last test is the same rule where there is no SAL to protect: a view tick that
fails to emit must surface as an error on the read that waited for it, rather
than as an OK reply over a view that silently stopped advancing.
"""

import os

import pytest
import gnitz
from _read import bag, scanned
from _sql import values

_ROWS = "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)"
_WANT = {(1, 100): 1, (2, 200): 1, (3, 300): 1}

# Each seam fails a different apply of already-committed state during boot. Both
# must abort before the SAL reset, so the pre-crash rows survive for the next
# boot: the worker base-table flush, and the worker's replay of a committed PUSH
# zone.
_BOOT_SEAMS = {
    "worker_boot_flush": {"GNITZ_INJECT_BOOT_FLUSH_ERROR": "1"},
    "worker_replay_apply": {"GNITZ_INJECT_INGEST_APPLY_ERROR": "store"},
}


@pytest.mark.parametrize("seam", list(_BOOT_SEAMS))
def test_a_failed_boot_leaves_the_sal_intact(seam, own_server):
    """Create and insert, SIGKILL before any checkpoint so the rows live only in
    the SAL and the memtable, then boot into the fault. The boot must abort
    without binding the socket, and the next clean boot must recover every row —
    which it can only do if the failed boot left the SAL alone."""
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)")
        conn.execute_sql(_ROWS)
    # Hard kill: the data directory takes one live process, so the failed boot
    # below must not race a still-running server.
    own_server.stop()

    rc = own_server.start_expecting_exit(extra_env=_BOOT_SEAMS[seam])
    assert rc != 0, f"boot should have aborted on the {seam} fault, got rc={rc}"
    assert not os.path.exists(own_server.sock_path), \
        "a failed boot must not accept requests"

    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        assert bag(scanned(conn, "t"), "pk", "val") == _WANT


@pytest.mark.parametrize("inject,with_index", [("store", False), ("index", True)])
def test_a_failed_live_apply_aborts_the_cluster_and_replays(inject, with_index,
                                                            own_server):
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
        if with_index:
            conn.execute_sql("CREATE INDEX ON t(val)")

    rc = own_server.exit_code_on(_ROWS, {"GNITZ_INJECT_INGEST_APPLY_ERROR": inject})
    assert rc == 2, f"a worker crash must exit 2, got {rc}"

    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        assert bag(scanned(conn, "t"), "pk", "val") == _WANT


def test_an_empty_index_projection_reports_no_write(own_server):
    """The `index` seam reports from an ingest that has already run. A push whose
    indexed column is NULL in every row projects to nothing and ingests nothing,
    so the seam has no write to report from and the push must survive it.

    Absorbing the empty-projection skip into the ingest would make this abort the
    cluster over a write that never happened."""
    own_server.start(extra_env={"GNITZ_INJECT_INGEST_APPLY_ERROR": "index"})
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)")
        conn.execute_sql("CREATE INDEX ON t(val)")
        conn.execute_sql("INSERT INTO t VALUES (1, NULL), (2, NULL)")
        assert bag(scanned(conn, "t"), "pk", "val") == {(1, None): 1, (2, None): 1}
    assert own_server.proc.poll() is None, \
        "no index ingest ran, so the seam must not have fired"


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


def test_a_failed_tick_reports_and_stays_pending(own_server):
    """A view tick that fails to emit fails the read waiting on it, and the next
    read ticks the same delta.

    The emit failure needs a seam; a real one takes a full SAL. The CREATE VIEW
    has nothing pending, emits no tick and so cannot spend it."""
    client = gnitz.connect(own_server.start(extra_env={"GNITZ_INJECT_TICK_EMIT_ERROR": "1"}).target)
    client.execute_sql(
        "CREATE TABLE tickfault (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)")
    client.execute_sql(
        "CREATE VIEW v AS SELECT pk, val FROM tickfault WHERE val > 5")
    vid, schema = client.resolve_table("v")

    client.execute_sql("INSERT INTO tickfault VALUES (1, 10), (2, 20), (3, 1)")

    with pytest.raises(gnitz.GnitzRefusedError):
        client.scan(vid, schema)

    # One read is the whole claim: polling for convergence would also pass on a
    # view that converges and then diverges again.
    assert bag(client.scan(vid, schema), "pk", "val") == {(1, 10): 1, (2, 20): 1}
