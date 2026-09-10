"""Resume or rebuild: which of the two a boot chooses, and that either answer
leaves the view correct.

A view resumes iff the recorded topology matches the launched `(worker_count,
STATE_FORMAT)`, every one of its output children is at the committed generation,
and every view it scans is itself valid; otherwise it is reset and rebuilt from
base. Correctness alone cannot tell the two apart — a rebuild re-derives the view
and silently repairs whatever the resume would have loaded — so every test here
reads the boot's own `recovery: rebuilding N invalid view(s)` marker alongside
the view's Z-set. That marker is the only observable the distinction has.
"""

import pytest
import gnitz
from _read import bag, scanned


def _checkpoint_cut(srv, schema, workers=None):
    """The 'cut' every test below starts from: `<schema>.t` with a view `v`
    (dbl = val * 2) and pks 1..5, made durable by a graceful stop, whose shutdown
    barrier runs a full checkpoint sequence."""
    srv.start(workers=workers)
    with gnitz.connect(srv.sock_path) as conn:
        conn.create_schema(schema)
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=schema)
        conn.execute_sql("CREATE VIEW v AS SELECT pk, val * 2 AS dbl FROM t",
                         schema_name=schema)
        conn.execute_sql(
            "INSERT INTO t VALUES " + ", ".join(f"({k}, {k * 10})" for k in range(1, 6)),
            schema_name=schema)
    srv.stop_graceful()


def _tail(srv, schema, keys):
    with gnitz.connect(srv.sock_path) as conn:
        conn.execute_sql(
            "INSERT INTO t VALUES " + ", ".join(f"({k}, {k * 10})" for k in keys),
            schema_name=schema)


def _assert_doubled(srv, schema, keys, ctx):
    """The view holds `dbl = val * 2` for exactly `keys`, each at weight 1."""
    with gnitz.connect(srv.sock_path) as conn:
        got = bag(scanned(conn, schema, "v"), "pk", "dbl")
    want = {(k, k * 20): 1 for k in keys}
    assert got == want, f"{ctx}: view is {got}, want {want}"


def test_a_checkpoint_cut_resumes_live_and_replays_only_the_tail(own_server):
    """SIGTERM drives a final checkpoint and exits 0; the restart must resume
    every view from it rather than re-derive it — asserted on the marker, since a
    rebuild produces the same rows. The resumed view then has to keep ticking
    live, and survive a SIGKILL that leaves the tail un-checkpointed: the second
    boot resumes the same cut and replays only that tail, so the view holds cut +
    tail with each row once rather than twice."""
    _checkpoint_cut(own_server, "ct")
    own_server.start()
    assert own_server.rebuilt_view_count() == 0, "a clean restart must not backfill"
    _tail(own_server, "ct", range(6, 11))
    _assert_doubled(own_server, "ct", range(1, 11), "the resumed view must keep ticking")

    own_server.restart()
    assert own_server.rebuilt_view_count() == 0, \
        "a valid checkpoint must be resumed and the tail replayed onto it"
    _assert_doubled(own_server, "ct", range(1, 11), "cut + tail, each row once")


@pytest.mark.parametrize("stage", ["genbump", "reset", "sweep", "backfill"])
def test_a_crash_in_the_recovery_window_forces_a_correct_rebuild(stage, own_server):
    """Recovery boot-flushes the base to cut + tail, then durably bumps the
    checkpoint generation before it resets the SAL. A crash anywhere after that
    bump leaves the tail durable only in the base while the views' checkpoints
    still name the stale cut — the trap the bump exists to close. The next boot's
    verdict must reject those views and rebuild them from the complete base, so
    the result is cut + tail and not the cut alone."""
    _checkpoint_cut(own_server, "ri")
    own_server.start()
    _tail(own_server, "ri", range(6, 11))
    own_server.stop()

    rc = own_server.start_expecting_exit(
        extra_env={"GNITZ_INJECT_RECOVERY_PANIC": stage})
    assert rc != 0, f"the injected panic at {stage} must crash boot"

    own_server.start()
    assert own_server.rebuilt_view_count() >= 1, \
        "the stale view must be rebuilt, not resumed"
    _assert_doubled(own_server, "ri", range(1, 11),
                    "a stale resume would show only the cut")


def test_a_changed_worker_count_rebuilds_the_cut_plus_tail_exactly(own_server):
    """Where re-homed shard state meets the tail re-slice: checkpoint at W=4, push
    a tail, SIGKILL, restart at W=2. The changed count fails the topology half of
    the verdict, so unlike its same-count sibling this restart rebuilds — and the
    base must still hold cut + tail exactly once, a checkpointed row re-applied
    from the tail showing as weight 2."""
    _checkpoint_cut(own_server, "cc", workers=4)
    own_server.start(workers=4)
    _tail(own_server, "cc", range(6, 11))
    own_server.restart(workers=2)

    assert own_server.resliced(), \
        "a restart at a changed worker count must replay every written slot"
    assert own_server.rebuilt_view_count() >= 1, \
        "a changed-count restart invalidates every view, so it must rebuild"

    with gnitz.connect(own_server.sock_path) as conn:
        assert bag(scanned(conn, "cc", "t"), "pk", "val") == {
            (k, k * 10): 1 for k in range(1, 11)}


# At the 16 MiB floor the backfill reclaims past 1/8 (2 MiB) and the committer
# checkpoints past 3/4 (12 MiB). The push below is sized into that gap, so the
# backfill's reclaim is the only one that fires. Byte volume is all that counts,
# hence wide rows. `GNITZ_LOG_LEVEL=normal` is what logs "SAL checkpoint epoch=";
# the server defaults to quiet.
_RECLAIM_ENV = {"GNITZ_SAL_BYTES": str(16 * 1024 * 1024), "GNITZ_LOG_LEVEL": "normal"}
_RECLAIM_ROWS = 400
_RECLAIM_PAD = "x" * 8000


def test_a_backfill_reclaim_restamps_the_state_it_invalidated(own_server):
    """A CREATE VIEW whose pre-backfill reclaim fires publishes every base table's
    shards and resets the SAL, so the rows it made durable exist in the base and
    nowhere else — and the generation bump inside `reclaim_base` invalidates every
    checkpointed view and index. The window must finish that checkpoint itself,
    re-stamping the derived state before it returns.

    Deferring the re-stamp to the committer needs something to wake it, and on a
    server that goes idle after the CREATE nothing does: the watchdog's reclaim
    barrier is gated on low SAL space, which the reclaim just freed. So nothing
    happens after the CREATE — a push or a scan would mask the bug — before the
    SIGKILL. Without the inline finish the restart rebuilds every view and index
    from base, and without the bump it resumes a view that is silently short with
    no SAL tail left to close the gap."""
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64),
            gnitz.ColumnDef("pad", gnitz.TypeCode.STRING)]
    schema = gnitz.Schema(cols)
    own_server.extra_env.update(_RECLAIM_ENV)

    # Phase 1: table, index and view, then a graceful stop so all three are
    # checkpointed at the generation the reclaim below will move past.
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("stale")
        tid = conn.create_table("stale", "t", cols)
        conn.execute_sql("CREATE INDEX ON t(val)", schema_name="stale")
        conn.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t", schema_name="stale")
    own_server.stop_graceful()

    # Phase 2: push into the (2, 12) MiB gap, then CREATE a second view whose
    # backfill reclaims. Nothing after it.
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        batch = gnitz.ZSetBatch(schema)
        for i in range(_RECLAIM_ROWS):
            batch.append(pk=i, val=i * 10, pad=_RECLAIM_PAD)
        conn.push(tid, batch)
        before = own_server.sal_checkpoints()
        conn.execute_sql("CREATE VIEW v2 AS SELECT pk FROM t", schema_name="stale")
    assert own_server.sal_checkpoints() > before, (
        "the CREATE VIEW backfill must have reclaimed the SAL — otherwise the "
        "un-checkpointed tail survives and this test proves nothing")

    own_server.stop()
    own_server.start()
    assert own_server.rebuilt_view_count() == 0, (
        "the CREATE VIEW window must re-stamp the derived state it invalidated, "
        "so every view resumes from its checkpoint")
    assert own_server.rebuilt_index_counts() == [0] * own_server.workers, (
        "the index the same reclaim invalidated must resume too")

    with gnitz.connect(own_server.sock_path) as conn:
        assert bag(scanned(conn, "stale", "v"), "pk", "val") == {
            (i, i * 10): 1 for i in range(_RECLAIM_ROWS)}, (
            "the resumed view must hold every base row exactly once")


def test_a_replaced_view_resumes_with_its_new_body(own_server):
    """A view retargeted under its own name is the one shape where a boot has two
    candidates for a name. The replacement takes a FRESH id and the retired
    definition's stores are torn down by the engine's cascade in the same DDL
    zone, so exactly one live view resumes, serving the NEW body, and the retired
    id is gone rather than merely shadowed.

    Both spellings are checked, since they build the same bundle."""
    sn = "replace_restart"
    own_server.start()
    conn = gnitz.connect(own_server.sock_path)
    conn.create_schema(sn)
    conn.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        "b BIGINT NOT NULL)", schema_name=sn)
    conn.execute_sql("INSERT INTO t VALUES (1, 10, 100)", schema_name=sn)

    conn.execute_sql("CREATE VIEW r AS SELECT pk, a FROM t", schema_name=sn)
    retired_r, _ = conn.resolve_table(sn, "r")
    conn.execute_sql("CREATE OR REPLACE VIEW r AS SELECT pk, b FROM t", schema_name=sn)
    live_r, _ = conn.resolve_table(sn, "r")

    conn.execute_sql("CREATE VIEW a2 AS SELECT pk, a FROM t", schema_name=sn)
    retired_a, _ = conn.resolve_table(sn, "a2")
    conn.execute_sql("ALTER VIEW a2 AS SELECT pk, b FROM t", schema_name=sn)
    live_a, _ = conn.resolve_table(sn, "a2")

    assert live_r != retired_r and live_a != retired_a, "a retarget takes a fresh id"
    conn.close()

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        for name, live, retired in (("r", live_r, retired_r), ("a2", live_a, retired_a)):
            again, _ = conn.resolve_table(sn, name)
            assert again == live, f"{name}: the name must resolve to the replacement"
            with pytest.raises(gnitz.GnitzError):
                conn.scan(retired)
            assert bag(scanned(conn, sn, name), "pk", "b") == {(1, 100): 1}, \
                f"{name}: must resume with the new body"

        conn.execute_sql("INSERT INTO t VALUES (2, 20, 200)", schema_name=sn)
        for name in ("r", "a2"):
            assert bag(scanned(conn, sn, name), "pk", "b") == {
                (1, 100): 1, (2, 200): 1}, f"{name}: not maintained after the restart"
