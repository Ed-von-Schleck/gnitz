"""Upsert, delete-by-PK and retraction across a partitioned table.

Which worker owns a PK is the engine's business, so a write addressed by PK
has to reach that worker and only that one — and a passthrough view over the
table has to show the retraction, not just the insert.
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
def test_unique_pk_across_workers(client):
    """
    SQL-standard ON CONFLICT DO UPDATE works correctly across workers:
    re-inserting the same PK via explicit UPSERT replaces the row.
    Scan must return exactly 1 row with the updated value.
    """
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 200) "
            "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
            schema_name=sn,
        )

        tid, _ = client.resolve_table(sn, "t")
        result = client.scan(tid)
        rows = list(result)
        assert len(rows) == 1, f"expected 1 row after upsert, got {len(rows)}"
        assert rows[0].pk == 1
        assert rows[0].val == 200
    finally:
        _drop_all(client, sn, tables=["t"])

@_NEEDS_MULTI
def test_workers_view_passthrough(client):
    """SQL passthrough view; push rows; scan view → all rows."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT * FROM t", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn)

        vid, _ = client.resolve_table(sn, "v")
        pks = sorted(r.pk for r in client.scan(vid))
        assert pks == [1, 2, 3]
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])

@_NEEDS_MULTI
def test_workers_view_deletes(client):
    """Push inserts then retractions; passthrough view shows only non-retracted rows."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT * FROM t", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn)
        client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)

        vid, _ = client.resolve_table(sn, "v")
        pks = sorted(r.pk for r in client.scan(vid))
        assert pks == [1, 3]
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])

@_NEEDS_MULTI
def test_workers_upsert(client):
    """Insert PK=1 val=10, then UPSERT PK=1 val=99; scan → one row with val=99."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 99) "
            "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
            schema_name=sn,
        )

        tid, _ = client.resolve_table(sn, "t")
        rows = list(client.scan(tid))
        assert len(rows) == 1
        assert rows[0].pk == 1
        assert rows[0].val == 99
    finally:
        _drop_all(client, sn, tables=["t"])

@_NEEDS_MULTI
def test_workers_delete_by_pk(client):
    """Push 3 rows; delete PK=2; scan → 2 rows (PK=1,3)."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn)
        client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        pks = sorted(r.pk for r in client.scan(tid))
        assert pks == [1, 3]
    finally:
        _drop_all(client, sn, tables=["t"])
