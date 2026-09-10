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
than as a STATUS_OK over a view that silently stopped advancing.
"""

import os

import pytest
import gnitz
from _read import bag, scanned
from _uid import uid

_ROWS = "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)"
_WANT = {(1, 100): 1, (2, 200): 1, (3, 300): 1}

# Each seam fails a different apply of already-committed state during boot. All
# three must abort before the SAL reset, so the pre-crash rows survive for the
# next boot: the worker base-table flush, the master's system-table flush, and
# the worker's replay of a committed PUSH zone.
_BOOT_SEAMS = {
    "worker_boot_flush": {"GNITZ_INJECT_BOOT_FLUSH_ERROR": "1"},
    "master_sys_flush": {"GNITZ_INJECT_SYS_FLUSH_ERROR": "1"},
    "worker_replay_apply": {"GNITZ_INJECT_INGEST_APPLY_ERROR": "store"},
}


@pytest.mark.parametrize("seam", list(_BOOT_SEAMS))
def test_a_failed_boot_leaves_the_sal_intact(seam, own_server):
    """Create and insert, SIGKILL before any checkpoint so the rows live only in
    the SAL and the memtable, then boot into the fault. The boot must abort
    without binding the socket, and the next clean boot must recover every row —
    which it can only do if the failed boot left the SAL alone."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("fs")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="fs")
        conn.execute_sql(_ROWS, schema_name="fs")
    # Hard kill: the data directory takes one live process, so the failed boot
    # below must not race a still-running server.
    own_server.stop()

    rc = own_server.start_expecting_exit(extra_env=_BOOT_SEAMS[seam])
    assert rc != 0, f"boot should have aborted on the {seam} fault, got rc={rc}"
    assert not os.path.exists(own_server.sock_path), \
        "a failed boot must not accept requests"

    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        assert bag(scanned(conn, "fs", "t"), "pk", "val") == _WANT


@pytest.mark.parametrize("inject,with_index", [("store", False), ("index", True)])
def test_a_failed_live_apply_aborts_the_cluster_and_replays(inject, with_index,
                                                            own_server):
    """A storage error applying a committed PUSH aborts the owning worker inside
    `ingest_store_and_indices`, and the master's watchdog turns the dead worker
    into a cluster shutdown — exit 2, which neither a clean shutdown (0) nor a
    boot failure (1) can produce, so the status alone proves the abort fired. The
    load-bearing assertion is the next boot: the un-flushed PUSH zone stayed above
    the watermark and was replayed, so the rows the swallow would have orphaned
    are all there."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("apply_err")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="apply_err")
        if with_index:
            conn.execute_sql("CREATE INDEX ON t(val)", schema_name="apply_err")

    own_server.restart(extra_env={"GNITZ_INJECT_INGEST_APPLY_ERROR": inject})
    try:
        # The ACK races the abort — the apply aborts before it can be sent, so
        # this raises on a dead socket (or, rarely, returns just before).
        with gnitz.connect(own_server.sock_path) as conn:
            conn.execute_sql(_ROWS, schema_name="apply_err")
    except Exception:
        pass
    rc = own_server.wait_for_exit()
    assert rc == 2, f"a worker crash must exit 2, got {rc}"

    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        assert bag(scanned(conn, "apply_err", "t"), "pk", "val") == _WANT


def test_a_failed_tick_reports_and_requeues(tick_emit_fault_server):
    """The same rule one rung up: a *view tick* that fails to emit must report,
    not serve.

    `drain_tick_rows_into` empties `tick_tids` BEFORE the tick runs, so a tick
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

    with pytest.raises(gnitz.GnitzError):
        list(client.scan(vid))

    # The seam is spent and the tid was re-queued, so this read ticks it. A view
    # read is served only once its source closure is at the last completed tick's
    # watermark, so one read is the whole claim — polling for convergence would
    # also pass on a view that converges and then diverges again.
    assert bag(client.scan(vid), "pk", "val") == {(1, 10): 1, (2, 20): 1}
