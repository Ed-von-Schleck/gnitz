"""E2E tests: SELECT * EXCEPT/EXCLUDE/RENAME honored, REPLACE/ILIKE rejected.

Covers CREATE VIEW (incremental), a RENAME round-trip scanned by the new name,
INSERT ... RETURNING (the path that bypasses the SELECT clause chokepoint, so
the helper-based REPLACE/ILIKE rejection is the only guard), and direct-SELECT /
view rejection of REPLACE / ILIKE.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_wildcard_modifiers.py -v --tb=short
"""
import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, *names):
    for name in names:
        for stmt in (f"DROP VIEW {name}", f"DROP TABLE {name}"):
            try:
                client.execute_sql(stmt, schema_name=sn)
                break
            except Exception:
                pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _returned_rows(result):
    """Row list from an execute_sql Rows result (RETURNING / SELECT)."""
    r = next(x for x in result if x["type"] == "Rows")
    return list(r["rows"])


def _view_rows(client, vid, *cols):
    """{(col values…): net_weight} over positive-weight rows of a view."""
    out = {}
    for row in client.scan(vid):
        if row.weight == 0:
            continue
        key = tuple(getattr(row, c) for c in cols)
        out[key] = out.get(key, 0) + row.weight
    return {k: w for k, w in out.items() if w != 0}


def test_view_except_incremental(client):
    """A `SELECT * EXCEPT (b)` view drops b and tracks inserts/deletes."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT * EXCEPT (b) FROM t", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]

        # The view presents only id + a (b dropped).
        client.execute_sql("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
        assert _view_rows(client, vid, "id", "a") == {(1, 10): 1, (2, 20): 1}
        # `b` is not a column of the view.
        first = next(iter(client.scan(vid)))
        assert not hasattr(first, "b"), "b must not appear in the view"

        # Incremental insert + delete.
        client.execute_sql("INSERT INTO t VALUES (3, 30, 300)", schema_name=sn)
        assert _view_rows(client, vid, "id", "a") == {(1, 10): 1, (2, 20): 1, (3, 30): 1}
        client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
        assert _view_rows(client, vid, "id", "a") == {(2, 20): 1, (3, 30): 1}
    finally:
        _cleanup(client, sn, "v", "t")


def test_view_rename_roundtrip(client):
    """A `RENAME (a AS years)` view is scannable by the new column name."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT * RENAME (a AS years) FROM t", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        client.execute_sql("INSERT INTO t VALUES (1, 42), (2, 43)", schema_name=sn)
        # Scan by the renamed column.
        assert _view_rows(client, vid, "id", "years") == {(1, 42): 1, (2, 43): 1}
    finally:
        _cleanup(client, sn, "v", "t")


def test_returning_star_except(client):
    """INSERT ... RETURNING * EXCEPT (a) returns the reduced column set."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
            schema_name=sn,
        )
        res = client.execute_sql(
            "INSERT INTO t VALUES (1, 10, 100) RETURNING * EXCEPT (a)", schema_name=sn
        )
        rows = _returned_rows(res)
        assert len(rows) == 1
        assert rows[0].id == 1
        assert rows[0].b == 100
        assert not hasattr(rows[0], "a"), "a must be excluded from RETURNING"
    finally:
        _cleanup(client, sn, "t")


def test_returning_star_replace_rejected(client):
    """RETURNING bypasses the SELECT clause chokepoint, so the helper-based
    rejection in WildcardRewrite::for_item is the only guard for REPLACE here."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        with pytest.raises(gnitz.GnitzError, match="REPLACE is not supported"):
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10) RETURNING * REPLACE (a + 1 AS a)", schema_name=sn
            )
    finally:
        _cleanup(client, sn, "t")


def test_replace_ilike_rejected(client):
    """REPLACE / ILIKE are rejected uniformly in CREATE VIEW and direct SELECT."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        with pytest.raises(gnitz.GnitzError, match="REPLACE is not supported"):
            client.execute_sql("CREATE VIEW v AS SELECT * REPLACE (a + 1 AS a) FROM t", schema_name=sn)
        with pytest.raises(gnitz.GnitzError, match="ILIKE is not supported"):
            client.execute_sql("CREATE VIEW v AS SELECT * ILIKE 'a%' FROM t", schema_name=sn)
        with pytest.raises(gnitz.GnitzError, match="REPLACE is not supported"):
            client.execute_sql("SELECT * REPLACE (a + 1 AS a) FROM t", schema_name=sn)
    finally:
        _cleanup(client, sn, "t")
