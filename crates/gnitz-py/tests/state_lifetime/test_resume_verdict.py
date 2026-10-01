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


def _checkpoint_cut(srv, workers=None):
    """The 'cut' every test below starts from: `t` with a view `v`
    (dbl = val * 2) and pks 1..5, made durable by a graceful stop, whose shutdown
    barrier runs a full checkpoint sequence."""
    srv.start(workers=workers)
    with gnitz.connect(srv.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)")
        conn.execute_sql("CREATE VIEW v AS SELECT pk, val * 2 AS dbl FROM t")
        conn.execute_sql(
            "INSERT INTO t VALUES " + ", ".join(f"({k}, {k * 10})" for k in range(1, 6)))
    srv.stop_graceful()


def _tail(srv, keys):
    with gnitz.connect(srv.target) as conn:
        conn.execute_sql(
            "INSERT INTO t VALUES " + ", ".join(f"({k}, {k * 10})" for k in keys))


def _assert_doubled(srv, keys, ctx):
    """The view holds `dbl = val * 2` for exactly `keys`, each at weight 1."""
    with gnitz.connect(srv.target) as conn:
        got = bag(scanned(conn, "v"), "pk", "dbl")
    want = {(k, k * 20): 1 for k in keys}
    assert got == want, f"{ctx}: view is {got}, want {want}"


def test_a_checkpoint_cut_resumes_live_and_replays_only_the_tail(own_server):
    """SIGTERM drives a final checkpoint and exits 0; the restart must resume
    every view from it rather than re-derive it — asserted on the marker, since a
    rebuild produces the same rows. The resumed view then has to keep ticking
    live, and survive a SIGKILL that leaves the tail un-checkpointed: the second
    boot resumes the same cut and replays only that tail, so the view holds cut +
    tail with each row once rather than twice."""
    _checkpoint_cut(own_server)
    own_server.start()
    assert own_server.rebuilt_view_count() == 0, "a clean restart must not backfill"
    _tail(own_server, range(6, 11))
    _assert_doubled(own_server, range(1, 11), "the resumed view must keep ticking")

    own_server.restart()
    assert own_server.rebuilt_view_count() == 0, \
        "a valid checkpoint must be resumed and the tail replayed onto it"
    _assert_doubled(own_server, range(1, 11), "cut + tail, each row once")


def test_a_stream_fed_view_rebuilds_alone_and_counts_the_tail_once(own_server):
    """A view over a stream is rejected on every boot, while its sibling over the
    table alone resumes. The rejected view's store opens empty and the recovery
    sweep must leave it alone: a tail ticked into it and then backfilled again
    would hold every tail row at weight 2."""
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)")
        conn.execute_sql(
            "CREATE TABLE s (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL) "
            "WITH (stream = true)")
        conn.execute_sql("CREATE VIEW ok AS SELECT id, val FROM t")
        conn.execute_sql(
            "CREATE VIEW mixed AS SELECT id FROM t UNION ALL SELECT id FROM s")
        conn.execute_sql(
            "INSERT INTO t VALUES " + ", ".join(f"({k}, {k * 10})" for k in range(1, 6)))
    own_server.restart(graceful=True)
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(
            "INSERT INTO t VALUES " + ", ".join(f"({k}, {k * 10})" for k in range(6, 11)))

    own_server.restart()
    assert own_server.rebuilt_view_count() == 1, "the stream-fed view alone rebuilds"
    with gnitz.connect(own_server.target) as conn:
        assert bag(scanned(conn, "mixed"), "id") == {(k,): 1 for k in range(1, 11)}, \
            "cut + tail, each row once"
        assert bag(scanned(conn, "ok"), "id", "val") == {
            (k, k * 10): 1 for k in range(1, 11)}


def test_a_stateful_view_resumes_from_its_operator_traces(own_server):
    """A GROUP BY view holds operator traces in `scratch_*` children of its own,
    where a projection holds none — so it is the only shape whose resume verdict
    reads trace manifests at all, and the only one that can catch an output store
    stamped at a generation its traces never reached.

    Two graceful stops, because the two boots compile the view by different
    routes: the first checkpoints a view the CREATE compiled, the second one the
    boot tick sweep compiled. A trace left behind by either shows up as a rebuild
    on the boot after it."""
    rows = {k: (k % 3, k * 10) for k in range(1, 10)}
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
            "val BIGINT NOT NULL)")
        conn.execute_sql(
            "CREATE VIEW g AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp")
        conn.execute_sql(
            "INSERT INTO t VALUES " + ", ".join(
                f"({k}, {grp}, {val})" for k, (grp, val) in rows.items()))
    own_server.stop_graceful()

    own_server.start()
    assert own_server.rebuilt_view_count() == 0, \
        "a stateful view must resume from its checkpointed traces"
    own_server.stop_graceful()

    own_server.start()
    assert own_server.rebuilt_view_count() == 0, \
        "the boot checkpoint must re-stamp the traces of a view the sweep compiled"

    totals = {}
    for grp, val in rows.values():
        totals[grp] = totals.get(grp, 0) + val
    with gnitz.connect(own_server.target) as conn:
        assert bag(scanned(conn, "g"), "grp", "total") == {
            (grp, total): 1 for grp, total in totals.items()}, \
            "a resumed aggregate must hold one row per group at its true sum"


@pytest.mark.parametrize("stage", ["genbump", "reset", "sweep", "backfill"])
def test_a_crash_in_the_recovery_window_forces_a_correct_rebuild(stage, own_server):
    """Recovery boot-flushes the base to cut + tail, then durably bumps the
    checkpoint generation before it resets the SAL. A crash anywhere after that
    bump leaves the tail durable only in the base while the views' checkpoints
    still name the stale cut — the trap the bump exists to close. The next boot's
    verdict must reject those views and rebuild them from the complete base, so
    the result is cut + tail and not the cut alone."""
    _checkpoint_cut(own_server)
    own_server.start()
    _tail(own_server, range(6, 11))
    own_server.stop()

    rc = own_server.start_expecting_exit(
        extra_env={"GNITZ_INJECT_RECOVERY_PANIC": stage})
    assert rc != 0, f"the injected panic at {stage} must crash boot"

    own_server.start()
    assert own_server.rebuilt_view_count() == 1, \
        "the stale view must be rebuilt, not resumed"
    _assert_doubled(own_server, range(1, 11),
                    "a stale resume would show only the cut")


def test_a_changed_worker_count_rebuilds_the_cut_plus_tail_exactly(own_server):
    """Where re-homed shard state meets the tail re-slice: checkpoint at W=4, push
    a tail, SIGKILL, restart at W=2. The changed count fails the topology half of
    the verdict, so unlike its same-count sibling this restart rebuilds — and the
    base must still hold cut + tail exactly once, a checkpointed row re-applied
    from the tail showing as weight 2."""
    _checkpoint_cut(own_server, workers=4)
    own_server.start(workers=4)
    _tail(own_server, range(6, 11))
    own_server.restart(workers=2)

    assert own_server.resliced(), \
        "a restart at a changed worker count must replay every written slot"
    assert own_server.rebuilt_view_count() == 1, \
        "a changed-count restart invalidates every view, so it must rebuild"

    with gnitz.connect(own_server.target) as conn:
        assert bag(scanned(conn, "t"), "pk", "val") == {
            (k, k * 10): 1 for k in range(1, 11)}
    _assert_doubled(own_server, range(1, 11), "the rebuild reads cut + tail, each row once")


def test_a_replaced_view_resumes_with_its_new_body(own_server):
    """A view retargeted under its own name is the one shape where a boot has two
    candidates for a name. The replacement takes a FRESH id and the retired
    definition's stores are torn down by the engine's cascade in the same DDL
    zone, so exactly one live view resumes, serving the NEW body, and the retired
    id is gone rather than merely shadowed.

    Both spellings are checked, since they build the same bundle."""
    own_server.start()
    conn = gnitz.connect(own_server.target)
    conn.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        "b BIGINT NOT NULL)")
    conn.execute_sql("INSERT INTO t VALUES (1, 10, 100)")

    conn.execute_sql("CREATE VIEW r AS SELECT pk, a FROM t")
    retired_r, _ = conn.resolve_table("r")
    conn.execute_sql("CREATE OR REPLACE VIEW r AS SELECT pk, b FROM t")
    live_r, _ = conn.resolve_table("r")

    conn.execute_sql("CREATE VIEW a2 AS SELECT pk, a FROM t")
    retired_a, _ = conn.resolve_table("a2")
    conn.execute_sql("ALTER VIEW a2 AS SELECT pk, b FROM t")
    live_a, _ = conn.resolve_table("a2")

    assert live_r != retired_r and live_a != retired_a, "a retarget takes a fresh id"
    conn.close()

    own_server.restart()
    with gnitz.connect(own_server.target) as conn:
        for name, live, retired in (("r", live_r, retired_r), ("a2", live_a, retired_a)):
            again, schema = conn.resolve_table(name)
            assert again == live, f"{name}: the name must resolve to the replacement"
            with pytest.raises(gnitz.GnitzNotFoundError):
                conn.scan(retired, schema)
            assert bag(scanned(conn, name), "pk", "b") == {(1, 100): 1}, \
                f"{name}: must resume with the new body"

        conn.execute_sql("INSERT INTO t VALUES (2, 20, 200)")
        for name in ("r", "a2"):
            assert bag(scanned(conn, name), "pk", "b") == {
                (1, 100): 1, (2, 200): 1}, f"{name}: not maintained after the restart"
