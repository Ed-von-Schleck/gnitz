"""A view over a system family is maintained from catalog deltas: every DDL's
rows have reached it by the time the DDL is acknowledged."""

import pytest
import gnitz
from _catalog import family, schema_id
from _read import bag, scanned
from _uid import uid


def _tables_view(client, name="tv"):
    sid = schema_id(client)
    client.execute_sql(
        f"CREATE VIEW {name} AS SELECT table_id, name FROM _system.tables WHERE schema_id = {sid}")
    return sid


def _same(client, view, fam, sid, *cols):
    got = bag(scanned(client, view), *cols)
    want = family(client, fam, *cols, where=f"schema_id = {sid}")
    assert got == want, f"{view} is {got}, the family reads {want}"
    return got


def test_a_view_over_tables_tracks_create_drop_and_rename(client):
    sid = _tables_view(client)
    assert bag(scanned(client, "tv")) == {}

    client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
    client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY)")
    got = _same(client, "tv", "tables", sid, "table_id", "name")
    assert sorted(name for (_, name) in got) == ["a", "b"]
    assert set(got.values()) == {1}

    client.execute_sql("ALTER TABLE a RENAME TO c")
    got = _same(client, "tv", "tables", sid, "table_id", "name")
    assert sorted(name for (_, name) in got) == ["b", "c"]

    client.execute_sql("DROP TABLE b")
    got = _same(client, "tv", "tables", sid, "table_id", "name")
    assert [name for (_, name) in got] == ["c"]


def test_a_second_view_created_while_the_first_scans_equals_it(client):
    """The second view's backfill reads the family's sealed prefix and takes the
    rows of its own CREATE through the tick the first view takes them through:
    counted by both, they would stand at weight 2."""
    sid = _tables_view(client, "first")
    client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY)")
    _tables_view(client, "second")
    client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY)")
    first = _same(client, "first", "tables", sid, "table_id", "name")
    assert bag(scanned(client, "second"), "table_id", "name") == first
    assert sorted(name for (_, name) in first) == ["a", "b"]


def test_a_view_over_views_holds_its_own_row(client):
    sid = schema_id(client)
    client.execute_sql(
        f"CREATE VIEW vv AS SELECT view_id, name FROM _system.views WHERE schema_id = {sid}")
    assert [name for (_, name) in _same(client, "vv", "views", sid, "view_id", "name")] == ["vv"]

    client.execute_sql(
        f"CREATE VIEW ww AS SELECT view_id, name FROM _system.views WHERE schema_id = {sid}")
    got = _same(client, "vv", "views", sid, "view_id", "name")
    assert sorted(name for (_, name) in got) == ["vv", "ww"]
    assert bag(scanned(client, "ww"), "view_id", "name") == got

    client.execute_sql("DROP VIEW ww")
    assert [name for (_, name) in _same(client, "vv", "views", sid, "view_id", "name")] == ["vv"]


def test_a_view_over_columns_tracks_add_and_drop_column(client):
    """ADD COLUMN is one new row; DROP COLUMN is logical, so it moves the
    column's row to its hidden form — a retraction and an insert under one key."""
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)")
    tid = client.resolve_table("t")[0]
    client.execute_sql(
        "CREATE VIEW cv AS SELECT owner_id, col_idx, name, is_hidden FROM _system.columns "
        f"WHERE owner_id = {tid}")

    def check(want):
        got = bag(scanned(client, "cv"), "col_idx", "name", "is_hidden")
        assert got == family(client, "columns", "col_idx", "name", "is_hidden", where=f"owner_id = {tid}")
        assert got == {row: 1 for row in want}

    check([(0, "id", 0), (1, "a", 0)])
    client.execute_sql("ALTER TABLE t ADD COLUMN b BIGINT")
    check([(0, "id", 0), (1, "a", 0), (2, "b", 0)])
    client.execute_sql("ALTER TABLE t DROP COLUMN a")
    got = bag(scanned(client, "cv"), "col_idx", "is_hidden")
    assert got == family(client, "columns", "col_idx", "is_hidden", where=f"owner_id = {tid}")
    assert got == {(0, 0): 1, (1, 1): 1, (2, 0): 1}


def test_a_view_over_a_catalog_view_tracks_it(client):
    sid = _tables_view(client)
    client.execute_sql("CREATE VIEW names AS SELECT table_id, name FROM tv")
    client.execute_sql("CREATE VIEW n AS SELECT COUNT(*) AS n FROM tv")
    client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY)")
    client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY)")
    client.execute_sql("DROP TABLE a")
    got = _same(client, "tv", "tables", sid, "table_id", "name")
    assert bag(scanned(client, "names"), "table_id", "name") == got
    assert bag(scanned(client, "n"), "n") == {(1,): 1}


def test_a_family_joins_a_keyed_table_changed_from_both_sides(client):
    """A replicated source beside a keyed one: the catalog side changes by DDL,
    the table side by a push, and each reaches the join once."""
    sid = schema_id(client)
    client.execute_sql(
        "CREATE TABLE notes (table_id BIGINT UNSIGNED NOT NULL PRIMARY KEY, owner BIGINT NOT NULL)")
    client.execute_sql(
        "CREATE VIEW owned AS SELECT t.name, n.owner FROM _system.tables t "
        f"JOIN notes n ON t.table_id = n.table_id WHERE t.schema_id = {sid}")
    client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY)")
    client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY)")
    a, b = client.resolve_table("a")[0], client.resolve_table("b")[0]
    assert bag(scanned(client, "owned"), "name", "owner") == {}

    client.execute_sql(f"INSERT INTO notes VALUES ({a}, 7), ({b}, 8)")
    assert bag(scanned(client, "owned"), "name", "owner") == {("a", 7): 1, ("b", 8): 1}

    client.execute_sql("ALTER TABLE a RENAME TO c")
    client.execute_sql("DROP TABLE b")
    client.execute_sql(f"UPDATE notes SET owner = 9 WHERE table_id = {a}")
    assert bag(scanned(client, "owned"), "name", "owner") == {("c", 9): 1}

    client.execute_sql("CREATE TABLE d (id BIGINT NOT NULL PRIMARY KEY)")
    d = client.resolve_table("d")[0]
    client.execute_sql(f"INSERT INTO notes VALUES ({d}, 1)")
    assert bag(scanned(client, "owned"), "name", "owner") == {("c", 9): 1, ("d", 1): 1}


def test_a_cascade_that_takes_a_catalog_view_leaves_the_others_exact(client, server):
    """The dropped schema's view scans `tables` until its own row is retracted,
    so the cascade's deltas land above the cut on its account and must still be
    ticked into the view that stays."""
    sid = _tables_view(client)
    other = "o" + uid()
    with gnitz.connect(server, schema=other) as conn:
        conn.create_schema(other)
        _tables_view(conn, "theirs")
        conn.execute_sql("CREATE TABLE x (id BIGINT NOT NULL PRIMARY KEY)")
        client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY)")
        conn.drop_schema(other)
    client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY)")
    got = _same(client, "tv", "tables", sid, "table_id", "name")
    assert sorted(name for (_, name) in got) == ["a", "b"]
    assert set(got.values()) == {1}


def test_a_catalog_view_created_after_the_last_one_was_dropped_is_exact(client, server):
    """No view scans a family once the cascade is through, and the next one to
    is exact from its backfill on."""
    other = "o" + uid()
    with gnitz.connect(server, schema=other) as conn:
        conn.create_schema(other)
        conn.execute_sql("CREATE TABLE x (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
        conn.execute_sql("CREATE INDEX ON x (v)")
        conn.execute_sql("CREATE VIEW theirs AS SELECT index_id, name FROM _system.indices")
        conn.drop_schema(other)
    client.execute_sql("CREATE VIEW mine AS SELECT index_id, name FROM _system.indices")
    assert bag(scanned(client, "mine"), "index_id", "name") == family(client, "indices", "index_id", "name")


def test_a_failed_bundle_leaves_every_catalog_view_unchanged(client):
    """Both bundles are refused by the engine, the first after its column and
    circuit rows were applied."""
    sid = _tables_view(client)
    client.execute_sql(
        f"CREATE VIEW vv AS SELECT view_id, name FROM _system.views WHERE schema_id = {sid}")
    client.execute_sql("CREATE VIEW cols AS SELECT COUNT(*) AS n FROM _system.columns")
    client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY)")

    def snapshot():
        n = family(client, "columns", "owner_id", "col_idx")
        assert bag(scanned(client, "cols"), "n") == {(len(n),): 1}
        return (
            _same(client, "tv", "tables", sid, "table_id", "name"),
            _same(client, "vv", "views", sid, "view_id", "name"),
            len(n),
        )

    before = snapshot()
    for stmt in [
        "CREATE VIEW sv AS SELECT * FROM _system.sequences",
        "CREATE TABLE _system.mine (id BIGINT NOT NULL PRIMARY KEY)",
    ]:
        with pytest.raises(gnitz.GnitzRefusedError):
            client.execute_sql(stmt)
        assert snapshot() == before, stmt

    client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY)")
    tables, views, ncols = snapshot()
    assert sorted(name for (_, name) in tables) == ["a", "c"]
    assert (views, ncols) == (before[1], before[2] + 1)
