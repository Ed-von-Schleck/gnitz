"""SUM over GROUP BY, SELECT DISTINCT and EXCEPT, maintained across partitions.

The reduce case runs seven ticks of interleaved inserts and deletes, which is
what drives the view store's L0 compaction rather than only its first batch.
"""

import os
import pytest
from _uid import uid as _uid

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)

def _reduce_totals(client, vid):
    """Scan a reduce view -> {group_val: agg_val} over the visible [grp, agg] layout."""
    return {r[0]: r[1] for r in client.scan(vid)}

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
def test_workers_reduce_sum(client):
    """SQL SUM GROUP BY across partitions produces correct per-group sums."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp",
            schema_name=sn,
        )
        # Push 100 rows across two groups
        vals = ",".join(
            f"({i}, {1 if i % 2 == 0 else 2}, {i})" for i in range(1, 101)
        )
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        vid, _ = client.resolve_table(sn, "v")
        # Visible layout: row[0]=grp, row[1]=total (the synthetic group PK is hidden).
        rows = list(client.scan(vid))
        totals = {r[0]: r[1] for r in rows}

        # group 1: even PKs (2,4,...,100) — sum = 2+4+...+100 = 2550
        # group 2: odd PKs (1,3,...,99) — sum = 1+3+...+99 = 2500
        assert totals[1] == sum(i for i in range(1, 101) if i % 2 == 0)
        assert totals[2] == sum(i for i in range(1, 101) if i % 2 != 0)
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])

@_NEEDS_MULTI
def test_workers_reduce_incremental(client):
    """7 ticks of inserts/deletes through reduce SUM view: exercises view store L0 compaction."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        # Tick 1: sum=300
        client.execute_sql(
            "INSERT INTO t VALUES (1, 1, 100), (2, 1, 200)", schema_name=sn
        )
        assert _reduce_totals(client, vid)[1] == 300

        # Tick 2: delete pk=2 → sum=100
        client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
        assert _reduce_totals(client, vid)[1] == 100

        # Tick 3: insert (3,1,50) → sum=150
        client.execute_sql("INSERT INTO t VALUES (3, 1, 50)", schema_name=sn)
        assert _reduce_totals(client, vid)[1] == 150

        # Tick 4: insert (4,1,75) → sum=225
        client.execute_sql("INSERT INTO t VALUES (4, 1, 75)", schema_name=sn)
        assert _reduce_totals(client, vid)[1] == 225

        # Tick 5: delete pk=1 → sum=125  [compaction fires here]
        client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
        assert _reduce_totals(client, vid)[1] == 125

        # Tick 6: insert (5,1,30) → sum=155
        client.execute_sql("INSERT INTO t VALUES (5, 1, 30)", schema_name=sn)
        assert _reduce_totals(client, vid)[1] == 155

        # Tick 7: delete pk=3 → sum=105
        client.execute_sql("DELETE FROM t WHERE pk = 3", schema_name=sn)
        assert _reduce_totals(client, vid)[1] == 105
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])

@_NEEDS_MULTI
def test_workers_distinct_view(client):
    """SELECT DISTINCT view with GNITZ_WORKERS=4."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT DISTINCT * FROM t",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        # Insert enough rows to spread across workers
        vals = ", ".join(f"({i}, {i * 10})" for i in range(1, 51))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        rows = list(client.scan(vid))
        assert len(rows) == 50, f"expected 50, got {len(rows)}"

        # Delete a subset and verify retraction
        client.execute_sql("DELETE FROM t WHERE pk = 10", schema_name=sn)
        client.execute_sql("DELETE FROM t WHERE pk = 25", schema_name=sn)
        rows = list(client.scan(vid))
        assert len(rows) == 48, f"expected 48 after deletes, got {len(rows)}"
    finally:
        _drop_all(client, sn, tables=["t"], views=["v"])

@_NEEDS_MULTI
def test_workers_except_multiworker(client):
    """EXCEPT (anti-join) with multi-worker distribution."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM a EXCEPT SELECT * FROM b",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]

        # Insert b first (exclusion set)
        b_vals = ", ".join(f"({i}, {i * 10})" for i in [5, 10, 15, 20])
        client.execute_sql(f"INSERT INTO b VALUES {b_vals}", schema_name=sn)

        # Insert a (full set) — 20 rows, 4 overlap with b
        a_vals = ", ".join(f"({i}, {i * 10})" for i in range(1, 21))
        client.execute_sql(f"INSERT INTO a VALUES {a_vals}", schema_name=sn)

        rows = client.scan(vid)
        pks = sorted(r["pk"] for r in rows)
        expected = sorted(set(range(1, 21)) - {5, 10, 15, 20})
        assert pks == expected, f"expected {expected}, got {pks}"
    finally:
        _drop_all(client, sn, tables=["a", "b"], views=["v"])
