"""E2E: CREATE VIEW over already-populated base tables.

An exchange-requiring view (JOIN / GROUP BY) created AFTER its base tables hold
data must reflect that pre-existing data, exactly as if the data had been
inserted after the view existed. These tests load data first, then create the
view, and assert the view sees the data. The view-first control proves the
harness.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_view_backfill_existing_data.py -v --tb=short
"""
import os
import random
import pytest
import gnitz

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, tables=None, views=None):
    for name in (views or []):
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
    for name in (tables or []):
        try:
            client.execute_sql(f"DROP TABLE {name}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _scan_dicts(client, tid):
    return [r._asdict() for r in client.scan(tid) if r.weight > 0]


def test_vbf_control_view_first_join_sees_data(client):
    """Control: view created BEFORE data — steady-state ticks populate it."""
    sn = "vbfctl" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn)
        client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
        client.execute_sql("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.id", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (10, 100), (20, 200)", schema_name=sn)
        client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20)", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        rows = _scan_dicts(client, vid)
        assert len(rows) == 2, f"control (view-first) expected 2 rows, got {len(rows)}: {rows}"
    finally:
        _cleanup(client, sn, tables=["a", "b"], views=["v"])


@pytest.mark.xfail(
    reason="live CREATE of an exchange view (JOIN) does not backfill already-committed "
    "base data; it only fills from post-create deltas. Fix pending.",
    strict=True,
)
def test_vbf_join_over_populated_tables_sees_existing_data(client):
    """A JOIN view created AFTER data is loaded must still see that data.

    The base-table inserts are forced to tick/commit (scan drives a tick that
    drains pending deltas) BEFORE the view exists — otherwise the view would
    pick the rows up as still-pending deltas on the next tick and mask the gap.
    """
    sn = "vbfjoin" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn)
        client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
        # Data BEFORE the view.
        client.execute_sql("INSERT INTO b VALUES (10, 100), (20, 200)", schema_name=sn)
        client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20)", schema_name=sn)
        # Force the inserts to tick/commit before the view exists.
        aid = client.resolve_table(sn, "a")[0]
        bid = client.resolve_table(sn, "b")[0]
        assert len(_scan_dicts(client, aid)) == 2, "base table a should hold 2 rows pre-view"
        assert len(_scan_dicts(client, bid)) == 2, "base table b should hold 2 rows pre-view"
        client.execute_sql("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.id", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        rows = _scan_dicts(client, vid)
        assert len(rows) == 2, f"view over populated tables expected 2 rows, got {len(rows)}: {rows}"
    finally:
        _cleanup(client, sn, tables=["a", "b"], views=["v"])


@pytest.mark.xfail(
    reason="live CREATE of an exchange view (GROUP BY) does not backfill already-committed "
    "base data; it only fills from post-create deltas. Fix pending.",
    strict=True,
)
def test_vbf_group_by_over_populated_table_sees_existing_data(client):
    """A GROUP BY view created AFTER committed data must still see that data."""
    sn = "vbfgb" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, n BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 7, 100), (2, 7, 200), (3, 9, 300)", schema_name=sn)
        # Force the inserts to tick/commit before the view exists.
        tid = client.resolve_table(sn, "t")[0]
        assert len(_scan_dicts(client, tid)) == 3, "base table t should hold 3 rows pre-view"
        client.execute_sql("CREATE VIEW v AS SELECT g, SUM(n) AS s FROM t GROUP BY g", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        rows = _scan_dicts(client, vid)
        assert len(rows) == 2, f"group-by view over populated table expected 2 groups, got {len(rows)}: {rows}"
    finally:
        _cleanup(client, sn, tables=["t"], views=["v"])
