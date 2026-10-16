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

import glob
import os

import pytest
import gnitz
from _read import bag, scanned
from _serverproc import RECOVERY_STAGES
from _sql import insert


def _checkpoint_cut(srv):
    """The 'cut' every test below starts from: `t` with a view `v`
    (dbl = val * 2) and pks 1..5, made durable by a graceful stop, whose shutdown
    barrier runs a full checkpoint sequence."""
    srv.start()
    with gnitz.connect(srv.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)")
        conn.execute_sql("CREATE VIEW v AS SELECT pk, val * 2 AS dbl FROM t")
    _tail(srv, range(1, 6))
    srv.stop_graceful()


def _tail(srv, keys):
    with gnitz.connect(srv.target) as conn:
        insert(conn, "t", [(k, k * 10) for k in keys])


def _assert_doubled(srv, keys, ctx):
    """The view holds `dbl = val * 2` for exactly `keys`, each at weight 1."""
    with gnitz.connect(srv.target) as conn:
        got = bag(scanned(conn, "v"), "pk", "dbl")
    want = {(k, k * 20): 1 for k in keys}
    assert got == want, f"{ctx}: view is {got}, want {want}"


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
    _tail(own_server, range(1, 6))
    own_server.restart(graceful=True)
    _tail(own_server, range(6, 11))

    own_server.restart()
    assert own_server.rebuilt_view_count() == 1, "the stream-fed view alone rebuilds"
    with gnitz.connect(own_server.target) as conn:
        assert bag(scanned(conn, "mixed"), "id") == {(k,): 1 for k in range(1, 11)}, \
            "cut + tail, each row once"
        assert bag(scanned(conn, "ok"), "id", "val") == {
            (k, k * 10): 1 for k in range(1, 11)}


@pytest.mark.parametrize("stage", RECOVERY_STAGES)
@pytest.mark.parametrize("tail", [True, False], ids=["tail", "no-tail"])
def test_a_crash_in_the_recovery_window_leaves_the_view_exact(tail, stage, own_server):
    """Recovery boot-flushes the base to cut + tail, then durably bumps the
    checkpoint generation before it resets the SAL. A crash anywhere after that
    bump leaves the tail durable only in the base while the views' checkpoints
    still name the stale cut — the trap the bump exists to close. The next boot's
    verdict must reject those views and rebuild them from the complete base, so
    the result is cut + tail and not the cut alone.

    With no tail the boot advances no generation, so a crash anywhere in it leaves
    the checkpoint it started from as valid as it was, and the next boot resumes
    it. Either way, a tail pushed afterwards is replayed onto the resumed view
    exactly once."""
    _checkpoint_cut(own_server)
    keys = range(1, 6)
    if tail:
        own_server.start()
        keys = range(1, 11)
        _tail(own_server, range(6, 11))
        own_server.stop()

    own_server.start_expecting_exit(
        f"injected recovery panic at {stage}",
        extra_env={"GNITZ_INJECT_RECOVERY_PANIC": stage})

    own_server.start()
    assert own_server.rebuilt_view_count() == int(tail), \
        "a stale view must be rebuilt, and only a stale one"
    _assert_doubled(own_server, keys, "a stale resume would show only the cut")
    _tail(own_server, range(11, 16))
    own_server.restart()
    assert own_server.rebuilt_view_count() == 0
    _assert_doubled(own_server, [*keys, *range(11, 16)], "and the tail after it, each row once")


def _manifests(srv):
    """Every published manifest under the data dir, by path, as the inode it is:
    a publish renames a new file into place, so a manifest rewritten shows as a
    changed inode."""
    paths = glob.glob(os.path.join(srv.data_dir, "_relations", "**", "manifest.bin"), recursive=True)
    return {os.path.relpath(p, srv.data_dir): os.stat(p).st_ino for p in paths}


def test_a_checkpoint_cut_resumes_and_replays_only_the_tail(own_server):
    """SIGTERM drives a final checkpoint; the restart must resume every view from
    it rather than re-derive it. With no committed tail a boot moves no base
    store, so the checkpoint generation stands and every store's manifest is the
    one already in place: neither the boot's own checkpoint nor an idle shutdown's
    rewrites one. The resumed view then keeps ticking live, and a SIGKILL that
    leaves that tail un-checkpointed resumes the same cut and replays only the
    tail, each row once — advancing the generation and restamping the view, which
    is what shows the first half is not a boot that checkpoints nothing at all."""
    _checkpoint_cut(own_server)
    cut = _manifests(own_server)
    assert any("/w0of" in path for path in cut), f"no worker store among {sorted(cut)}"

    own_server.start()
    assert own_server.rebuilt_view_count() == 0, "a clean restart must not backfill"
    _assert_doubled(own_server, range(1, 6), "the resumed cut")
    with gnitz.connect(own_server.target) as conn:
        view_id = conn.resolve_table("v")[0]
    own_server.stop_graceful()
    assert _manifests(own_server) == cut, "a clean boot and an idle stop publish nothing"

    own_server.start()
    _tail(own_server, range(6, 11))
    _assert_doubled(own_server, range(1, 11), "the resumed view must keep ticking")
    own_server.restart()
    assert own_server.rebuilt_view_count() == 0, \
        "a valid checkpoint must be resumed and the tail replayed onto it"
    _assert_doubled(own_server, range(1, 11), "cut + tail, each row once")
    own_server.stop_graceful()
    restamped = _manifests(own_server)
    assert restamped.keys() == cut.keys()
    moved = {path for path in cut if restamped[path] != cut[path]}
    assert any(path.startswith(f"_relations/{view_id}/") for path in moved), \
        f"a replayed tail must restamp the view; rewritten: {sorted(moved)}"


def test_a_checkpoint_with_no_push_behind_it_publishes_new_derived_state_resumably(own_server):
    """A checkpoint that follows DDL but no push advances no generation, so the
    view and the index created since are published at the generation already in
    force, beside stores published there earlier. Both must resume from it, and a
    tail replayed onto them must count once: a view resumed onto the wrong cut
    shows the cut's rows missing or the tail's doubled.

    The GROUP BY holds operator traces where a projection holds none, so it is
    the shape whose verdict reads trace manifests. The two boots compile it by
    different routes — the CREATE, then the boot tick sweep — and each one's
    checkpoint must stamp them."""
    _checkpoint_cut(own_server)
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql("CREATE VIEW late AS SELECT val % 3 AS g, SUM(val) AS s FROM t GROUP BY val % 3")
        conn.execute_sql("CREATE INDEX late_ix ON t (val)")
    own_server.stop_graceful()

    own_server.start()
    assert own_server.rebuilt_view_count() == 0, "the late view resumes"
    assert own_server.rebuilt_index_counts() == [0] * own_server.workers
    _tail(own_server, range(6, 11))
    own_server.restart()
    assert own_server.rebuilt_view_count() == 0
    assert own_server.rebuilt_index_counts() == [0] * own_server.workers
    _assert_doubled(own_server, range(1, 11), "cut + tail, each row once")
    sums = {}
    for k in range(1, 11):
        sums[k * 10 % 3] = sums.get(k * 10 % 3, 0) + k * 10
    with gnitz.connect(own_server.target) as conn:
        assert bag(scanned(conn, "late"), "g", "s") == {kv: 1 for kv in sums.items()}
        tid, schema = conn.resolve_table("t")
        for k in (2, 7):
            got = bag(conn.seek_by_index(tid, schema, [1], [k * 10]), "pk", "val")
            assert got == {(k, k * 10): 1}, f"index seek {k}: {got}"


def test_a_replaced_view_resumes_with_its_new_body(own_server):
    """A view retargeted under its own name is the one shape where a boot has two
    candidates for a name. The retired definition's stores are torn down by the
    engine's cascade in the same DDL zone, so exactly one live view resumes,
    serving the NEW body, and the retired id is gone rather than merely shadowed."""
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
            "b BIGINT NOT NULL)")
        conn.execute_sql("INSERT INTO t VALUES (1, 10, 100)")
        conn.execute_sql("CREATE VIEW r AS SELECT pk, a FROM t")
        retired, _ = conn.resolve_table("r")
        conn.execute_sql("ALTER VIEW r AS SELECT pk, b FROM t")
        live, _ = conn.resolve_table("r")

    own_server.restart(graceful=True)
    assert own_server.rebuilt_view_count() == 0
    with gnitz.connect(own_server.target) as conn:
        again, schema = conn.resolve_table("r")
        assert again == live, "the name must resolve to the replacement"
        with pytest.raises(gnitz.GnitzNotFoundError):
            conn.scan(retired, schema)
        assert bag(scanned(conn, "r"), "pk", "b") == {(1, 100): 1}, "must resume with the new body"
        conn.execute_sql("INSERT INTO t VALUES (2, 20, 200)")
        assert bag(scanned(conn, "r"), "pk", "b") == {(1, 100): 1, (2, 200): 1}, \
            "not maintained after the restart"
