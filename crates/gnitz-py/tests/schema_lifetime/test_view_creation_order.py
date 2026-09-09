"""Creating views around data that is already there, across every worker.

A view created after the rows must backfill to the same value as one created
before them, a view over a view must backfill in dependency order, and two
views over one table must each get the whole source.
"""

import os
import pytest
from _uid import uid as _uid

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)


def _drop_all(client, sn, tables=(), views=(), indices=()):
    for idx in indices:
        try:
            client.execute_sql(f"DROP INDEX {idx}", schema_name=sn)
        except Exception:
            pass
    for v in views:
        try:
            client.execute_sql(f"DROP VIEW {v}", schema_name=sn)
        except Exception:
            pass
    for t in tables:
        try:
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    client.drop_schema(sn)



















@_NEEDS_MULTI
def test_workers_ddl_create_table(client):
    """Create table while workers running; push rows; scan → all rows present."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        n = 50
        vals = ",".join(f"({i}, {i * 10})" for i in range(1, n + 1))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        pks = sorted(r.pk for r in client.scan(tid))
        assert pks == list(range(1, n + 1))
    finally:
        _drop_all(client, sn, tables=["t"])

@_NEEDS_MULTI
def test_workers_view_cascade(client):
    """V1 = filter view, V2 = passthrough of V1; push rows; verify V2 has matching rows."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10",
            schema_name=sn,
        )
        v1_id, v1_schema = client.resolve_table(sn, "v1")
        v2_id = client.create_view(sn, "v2", v1_id, v1_schema)

        client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 50), (3, 100)", schema_name=sn)

        v1_pks = sorted(r.pk for r in client.scan(v1_id))
        v2_pks = sorted(r.pk for r in client.scan(v2_id))
        assert v1_pks == [2, 3]
        assert v2_pks == [2, 3]
    finally:
        try:
            client.drop_view(sn, "v2")
        except Exception:
            pass
        _drop_all(client, sn, views=["v1"], tables=["t"])

@_NEEDS_MULTI
def test_workers_view_ddl_then_push(client):
    """Push rows THEN create view; push more rows; scan view → all rows via backfill."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        # Push rows BEFORE view creation — should appear via backfill
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)

        client.execute_sql("CREATE VIEW v AS SELECT * FROM t", schema_name=sn)

        # Push rows AFTER view creation — should also appear in view
        client.execute_sql("INSERT INTO t VALUES (2, 20), (3, 30)", schema_name=sn)

        vid, _ = client.resolve_table(sn, "v")
        pks = sorted(r.pk for r in client.scan(vid))
        # Pre-creation row now visible via backfill
        assert pks == [1, 2, 3]
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])

@_NEEDS_MULTI
def test_workers_view_on_view_backfill(client):
    """Insert rows, then create v1 then v2 (view on view); both get backfilled."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 30), (3, 100)", schema_name=sn)

        client.execute_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10", schema_name=sn)
        client.execute_sql("CREATE VIEW v2 AS SELECT * FROM v1 WHERE val > 50", schema_name=sn)

        v1_id, _ = client.resolve_table(sn, "v1")
        v2_id, _ = client.resolve_table(sn, "v2")

        v1_pks = sorted(r.pk for r in client.scan(v1_id))
        v2_pks = sorted(r.pk for r in client.scan(v2_id))
        assert v1_pks == [2, 3], "v1 backfill: expected [2,3] got %s" % v1_pks
        assert v2_pks == [3], "v2 backfill: expected [3] got %s" % v2_pks
    finally:
        _drop_all(client, sn, views=["v2", "v1"], tables=["t"])

@_NEEDS_MULTI
def test_workers_multiple_views_same_table(client):
    """Two views on same table; both get pushed data."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10", schema_name=sn)
        client.execute_sql("CREATE VIEW v2 AS SELECT * FROM t WHERE val > 50", schema_name=sn)

        client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 30), (3, 100)", schema_name=sn)

        v1_id, _ = client.resolve_table(sn, "v1")
        v2_id, _ = client.resolve_table(sn, "v2")

        v1_pks = sorted(r.pk for r in client.scan(v1_id))
        v2_pks = sorted(r.pk for r in client.scan(v2_id))
        assert v1_pks == [2, 3]
        assert v2_pks == [3]
    finally:
        _drop_all(client, sn, views=["v1", "v2"], tables=["t"])
