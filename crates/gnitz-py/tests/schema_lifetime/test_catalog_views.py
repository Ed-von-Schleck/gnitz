"""A view over a system family is maintained from catalog deltas: every DDL's
rows have reached it by the time the DDL is acknowledged.

The per-DDL deltas of such a view are `storage_policy/test_catalog_feed.py`'s.
"""

import pytest
import gnitz
from _catalog import family, schema_id
from _read import bag, scanned
from _uid import uid

_ID = {"tables": "table_id", "views": "view_id"}


def _over(client, fam, name):
    """`name`: a view over `fam`'s rows of the client's schema."""
    client.execute_sql(
        f"CREATE VIEW {name} AS SELECT {_ID[fam]}, name FROM _system.{fam} WHERE schema_id = {schema_id(client)}")


def _names(client, view, fam):
    """`view`'s names, once it equals `fam` read ad hoc, every row at weight 1."""
    got = bag(scanned(client, view))
    assert got == family(client, fam, _ID[fam], "name", where=f"schema_id = {schema_id(client)}"), view
    assert set(got.values()) <= {1}, got
    return sorted(name for _, name in got)


def test_a_second_view_created_while_the_first_scans_equals_it(client):
    """The second view's backfill reads the family's sealed prefix and takes the
    rows of its own CREATE through the tick the first view takes them through:
    counted by both, they would stand at weight 2."""
    _over(client, "tables", "first")
    client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY)")
    _over(client, "tables", "second")
    client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY)")
    assert _names(client, "first", "tables") == _names(client, "second", "tables") == ["a", "b"]


def test_a_view_over_views_holds_its_own_row(client):
    _over(client, "views", "vv")
    assert _names(client, "vv", "views") == ["vv"]
    _over(client, "views", "ww")
    assert _names(client, "vv", "views") == _names(client, "ww", "views") == ["vv", "ww"]
    client.execute_sql("DROP VIEW ww")
    assert _names(client, "vv", "views") == ["vv"]


def test_a_view_over_columns_tracks_add_and_drop_column(client):
    """ADD COLUMN is one new row; DROP COLUMN is logical, so it moves the
    column's row to its hidden form — a retraction and an insert under one key."""
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)")
    tid = client.resolve_table("t")[0]
    client.execute_sql(
        f"CREATE VIEW cv AS SELECT col_idx, name, is_hidden FROM _system.columns WHERE owner_id = {tid}")

    def check(*want):
        got = bag(scanned(client, "cv"))
        assert got == family(client, "columns", "col_idx", "name", "is_hidden", where=f"owner_id = {tid}")
        assert got == {row: 1 for row in want}

    check((0, "id", 0), (1, "a", 0))
    client.execute_sql("ALTER TABLE t ADD COLUMN b BIGINT")
    check((0, "id", 0), (1, "a", 0), (2, "b", 0))
    client.execute_sql("ALTER TABLE t DROP COLUMN a")
    check((0, "id", 0), (1, "a", 1), (2, "b", 0))


def test_a_family_joins_a_keyed_table_changed_from_both_sides(client):
    """A replicated source beside a keyed one: the catalog side changes by DDL,
    the table side by a push, and each reaches the join once."""
    client.execute_sql(
        "CREATE TABLE notes (table_id BIGINT UNSIGNED NOT NULL PRIMARY KEY, owner BIGINT NOT NULL); "
        "CREATE VIEW owned AS SELECT t.name, n.owner FROM _system.tables t "
        f"JOIN notes n ON t.table_id = n.table_id WHERE t.schema_id = {schema_id(client)}; "
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY); CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY)")
    a, b = client.resolve_table("a")[0], client.resolve_table("b")[0]
    client.execute_sql(f"INSERT INTO notes VALUES ({a}, 7), ({b}, 8)")
    assert bag(scanned(client, "owned")) == {("a", 7): 1, ("b", 8): 1}

    client.execute_sql(
        f"ALTER TABLE a RENAME TO c; DROP TABLE b; UPDATE notes SET owner = 9 WHERE table_id = {a}")
    assert bag(scanned(client, "owned")) == {("c", 9): 1}

    client.execute_sql("CREATE TABLE d (id BIGINT NOT NULL PRIMARY KEY)")
    client.execute_sql(f"INSERT INTO notes VALUES ({client.resolve_table('d')[0]}, 1)")
    assert bag(scanned(client, "owned")) == {("c", 9): 1, ("d", 1): 1}


def test_a_cascade_that_takes_a_catalog_view_leaves_the_others_exact(client, server):
    """The dropped schema's view scans `tables` until its own row is retracted,
    so the cascade's deltas land above the cut on its account and must still be
    ticked into the view that stays — and into the views over that one."""
    _over(client, "tables", "tv")
    client.execute_sql(
        "CREATE VIEW names AS SELECT table_id, name FROM tv; CREATE VIEW n AS SELECT COUNT(*) AS n FROM tv")
    other = "o" + uid()
    with gnitz.connect(server, schema=other) as conn:
        conn.create_schema(other)
        _over(conn, "tables", "theirs")
        conn.execute_sql("CREATE TABLE x (id BIGINT NOT NULL PRIMARY KEY)")
        client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY)")
        conn.drop_schema(other)
    client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY)")
    assert _names(client, "tv", "tables") == ["a", "b"]

    client.execute_sql("DROP TABLE a")
    assert _names(client, "tv", "tables") == ["b"]
    assert bag(scanned(client, "names")) == bag(scanned(client, "tv"))
    assert bag(scanned(client, "n")) == {(1,): 1}


def test_a_catalog_view_created_after_the_last_one_was_dropped_is_exact(client, server):
    """No view scans a family once the cascade is through, and the next one to
    is exact from its backfill on, through the ticks after it."""
    client.execute_sql(
        "CREATE TABLE y (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL); CREATE INDEX iy ON y (v)")
    other = "o" + uid()
    with gnitz.connect(server, schema=other) as conn:
        conn.create_schema(other)
        conn.execute_sql("CREATE TABLE x (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
        conn.execute_sql("CREATE INDEX ON x (v)")
        conn.execute_sql("CREATE VIEW theirs AS SELECT index_id, name FROM _system.indices")
        conn.drop_schema(other)
    client.execute_sql("CREATE VIEW mine AS SELECT index_id, name FROM _system.indices")

    def mine():
        got = bag(scanned(client, "mine"))
        assert got == family(client, "indices", "index_id", "name")
        return got

    before = mine()
    client.execute_sql("DROP INDEX iy")
    assert len(before) - len(mine()) == 1


def test_a_failed_bundle_leaves_every_catalog_view_unchanged(client):
    """Each bundle is refused by the engine, the views' after their column and
    circuit rows were applied."""
    _over(client, "tables", "tv")
    _over(client, "views", "vv")
    client.execute_sql(
        "CREATE VIEW cols AS SELECT COUNT(*) AS n FROM _system.columns; "
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY)")

    def snapshot():
        n = len(family(client, "columns", "owner_id", "col_idx"))
        assert bag(scanned(client, "cols")) == {(n,): 1}
        return _names(client, "tv", "tables"), _names(client, "vv", "views"), n

    before = snapshot()
    for stmt, refusal in [
        ("CREATE VIEW sv AS SELECT * FROM _system.sequences", "holds no set a view can scan"),
        ("CREATE TABLE _system.mine (id BIGINT NOT NULL PRIMARY KEY)", "system schema takes no relation"),
        ("CREATE VIEW _system.mine AS SELECT table_id, name FROM _system.tables",
         "system schema takes no relation"),
    ]:
        with pytest.raises(gnitz.GnitzRefusedError, match=refusal):
            client.execute_sql(stmt)
        assert snapshot() == before, stmt

    client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY)")
    assert snapshot() == (["a", "c"], before[1], before[2] + 1)
