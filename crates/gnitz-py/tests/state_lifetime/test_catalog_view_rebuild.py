"""A view that reaches a system family is rebuilt at every boot, and is then
exact over the catalog the boot recovered."""

import pytest
import gnitz
from _read import bag, rows, scanned
from _serverproc import MULTI

# `cat` scans a family, `over` scans `cat`, `joined` mixes a family with a keyed
# table; `plain` reaches user tables alone and is the one that resumes.
CATALOG_VIEWS = 3


def _setup(srv, workers=None):
    srv.start(workers=workers)
    with gnitz.connect(srv.target) as conn:
        conn.execute_sql(
            "CREATE TABLE notes (table_id BIGINT UNSIGNED NOT NULL PRIMARY KEY, owner BIGINT NOT NULL)")
        conn.execute_sql(
            "CREATE VIEW cat AS SELECT table_id, name FROM _system.tables "
            f"WHERE table_id >= {gnitz.FIRST_USER_TABLE_ID}")
        conn.execute_sql("CREATE VIEW over AS SELECT COUNT(*) AS n FROM cat")
        conn.execute_sql(
            "CREATE VIEW joined AS SELECT t.name, n.owner FROM _system.tables t "
            "JOIN notes n ON t.table_id = n.table_id")
        conn.execute_sql("CREATE VIEW plain AS SELECT table_id, owner * 2 AS dbl FROM notes")
        _make(conn, ["a", "b"])


def _make(conn, names):
    for i, name in enumerate(names):
        conn.execute_sql(f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY)")
        tid = conn.resolve_table(name)[0]
        conn.execute_sql(f"INSERT INTO notes VALUES ({tid}, {len(name) * 10 + i})")


def _assert_exact(srv, ctx):
    """Every view equals what its body reads now, each row at weight 1."""
    with gnitz.connect(srv.target) as conn:
        tables = bag(rows(
            conn,
            f"SELECT table_id, name FROM _system.tables WHERE table_id >= {gnitz.FIRST_USER_TABLE_ID}"))
        notes = bag(rows(conn, "SELECT table_id, owner FROM notes"))
        assert set(tables.values()) == {1} and set(notes.values()) == {1}, ctx
        assert bag(scanned(conn, "cat"), "table_id", "name") == tables, ctx
        assert bag(scanned(conn, "over"), "n") == {(len(tables),): 1}, ctx
        names = {tid: name for (tid, name) in tables}
        # A note outlives a dropped table, and then joins nothing.
        assert bag(scanned(conn, "joined"), "name", "owner") == {
            (names[tid], owner): 1 for (tid, owner) in notes if tid in names}, ctx
        assert bag(scanned(conn, "plain"), "table_id", "dbl") == {
            (tid, owner * 2): 1 for (tid, owner) in notes}, ctx
        return sorted(names.values())


def test_a_graceful_restart_rebuilds_the_catalog_views_alone(own_server):
    _setup(own_server)
    own_server.restart(graceful=True)
    assert own_server.rebuilt_view_count() == CATALOG_VIEWS, \
        "every view reaching a family rebuilds; the one over user tables resumes"
    assert _assert_exact(own_server, "after a graceful restart") == ["a", "b", "notes"]

    # A rebuilt view keeps ticking.
    with gnitz.connect(own_server.target) as conn:
        _make(conn, ["c"])
        conn.execute_sql("DROP TABLE a")
    assert _assert_exact(own_server, "live after the rebuild") == ["b", "c", "notes"]


@pytest.mark.parametrize("ddl_in_tail", [False, True], ids=["push-tail", "ddl-tail"])
def test_a_crash_rebuilds_the_catalog_views_over_the_whole_catalog(own_server, ddl_in_tail):
    """Checkpointed, then killed with a tail: the tail's catalog rows are in the
    families before the views exist, so the rebuild reads them once."""
    _setup(own_server)
    own_server.restart(graceful=True)
    want = ["a", "b", "notes"]
    with gnitz.connect(own_server.target) as conn:
        if ddl_in_tail:
            _make(conn, ["c"])
            conn.execute_sql("ALTER TABLE a RENAME TO z")
            conn.execute_sql("DROP TABLE b")
            want = ["c", "notes", "z"]
        else:
            conn.execute_sql("UPDATE notes SET owner = owner + 1")
    own_server.restart()
    assert own_server.rebuilt_view_count() == CATALOG_VIEWS
    assert _assert_exact(own_server, "after the crash") == want
    with gnitz.connect(own_server.target) as conn:
        _make(conn, ["d"])
    assert _assert_exact(own_server, "live after the crash") == sorted(want + ["d"])


@pytest.mark.parametrize("stage", ["genbump", "reset", "sweep", "backfill"])
def test_a_crash_in_the_recovery_window_leaves_the_catalog_views_exact(stage, own_server):
    _setup(own_server)
    own_server.restart(graceful=True)
    with gnitz.connect(own_server.target) as conn:
        _make(conn, ["c"])
        conn.execute_sql("DROP TABLE a")
    own_server.stop()

    rc = own_server.start_expecting_exit(extra_env={"GNITZ_INJECT_RECOVERY_PANIC": stage})
    assert rc != 0, f"the injected panic at {stage} must crash boot"

    own_server.start()
    assert _assert_exact(own_server, f"after a crash at {stage}") == ["b", "c", "notes"]


@pytest.mark.parametrize("before, after", [(MULTI, 1), (1, MULTI), (2, 4)])
def test_a_restart_at_another_worker_count_rebuilds_them_exact(own_server, before, after):
    _setup(own_server, workers=before)
    own_server.restart(graceful=True)
    with gnitz.connect(own_server.target) as conn:
        _make(conn, ["c"])
    own_server.restart(workers=after)
    assert _assert_exact(own_server, f"{before} -> {after} workers") == ["a", "b", "c", "notes"]
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql("DROP TABLE b")
    assert _assert_exact(own_server, "live at the new count") == ["a", "c", "notes"]
