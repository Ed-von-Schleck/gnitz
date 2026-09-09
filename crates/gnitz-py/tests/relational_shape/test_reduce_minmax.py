"""MIN / MAX reduce, grouped and global.

MIN and MAX are the non-linear aggregates: retracting the current extremum
needs the next value out of history, which is what the secondary value index
carries. These drive that promotion — retract the row currently holding the
extremum and require the next one to take its place — for both the grouped
and the global (no GROUP BY) funnel.
"""

import os

from _serverproc import NEEDS_MULTI
from _uid import uid as _uid

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
def _drop_all(client, sn, tables=(), views=()):
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


def _scan_reduce_map(client, vid):
    """Scan a reduce view → {group_val: agg_val} for positive-weight rows.

    The visible layout of a GROUP BY view is [group_col, agg] whether the view
    is keyed naturally or by a (hidden) synthetic group PK.
    """
    return {row[0]: row[1] for row in client.scan(vid)}


@NEEDS_MULTI
def test_min_multiworker_incremental(client):
    """Multi-worker MIN with incremental updates.  The gather-reduce must
    correctly fold the old global MIN when a new partial arrives."""
    sn = "mmi_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, MIN(val) AS m FROM t GROUP BY grp",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        # Tick 1: 20 rows, MIN = 11
        vals1 = ", ".join(f"({i}, 1, {10 + i})" for i in range(1, 21))
        client.execute_sql(f"INSERT INTO t VALUES {vals1}", schema_name=sn)
        m1 = _scan_reduce_map(client, vid)
        assert m1[1] == 11, f"tick 1 MIN: {m1[1]} != 11"

        # Tick 2: new smaller value → MIN updates
        client.execute_sql("INSERT INTO t VALUES (100, 1, 5)", schema_name=sn)
        m2 = _scan_reduce_map(client, vid)
        assert m2[1] == 5, f"tick 2 MIN: {m2[1]} != 5"

        # Tick 3: even smaller
        client.execute_sql("INSERT INTO t VALUES (101, 1, 2)", schema_name=sn)
        m3 = _scan_reduce_map(client, vid)
        assert m3[1] == 2, f"tick 3 MIN: {m3[1]} != 2"
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


@NEEDS_MULTI
def test_max_multiworker_incremental(client):
    """Multi-worker MAX with incremental updates."""
    sn = "xmi_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, MAX(val) AS m FROM t GROUP BY grp",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        vals1 = ", ".join(f"({i}, 1, {i})" for i in range(1, 21))
        client.execute_sql(f"INSERT INTO t VALUES {vals1}", schema_name=sn)
        m1 = _scan_reduce_map(client, vid)
        assert m1[1] == 20, f"tick 1 MAX: {m1[1]} != 20"

        client.execute_sql("INSERT INTO t VALUES (100, 1, 999)", schema_name=sn)
        m2 = _scan_reduce_map(client, vid)
        assert m2[1] == 999, f"tick 2 MAX: {m2[1]} != 999"

        # Value below current max — max stays
        client.execute_sql("INSERT INTO t VALUES (101, 1, 500)", schema_name=sn)
        m3 = _scan_reduce_map(client, vid)
        assert m3[1] == 999, f"tick 3 MAX: {m3[1]} != 999"
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


# -----------------------------------------------------------------------
# Grouped MIN/MAX retraction (correct path): each group lives on a single
# worker (rows partition by group key), so retracting a group's current
# extremum is handled by the local reduce's trace_in / AggValueIndex and
# recomputes the next-best. These pass; they guard that the common grouped
# case is unaffected by the global-aggregate gap documented below.
# -----------------------------------------------------------------------


@NEEDS_MULTI
def test_grouped_min_multiworker_retract_current_min(client):
    sn = "gmin_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, MIN(val) AS m FROM t GROUP BY grp",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        # Three groups, each with a clear MIN.
        rows = []
        pk = 0
        for grp in (1, 2, 3):
            for k in range(5):
                rows.append(f"({pk}, {grp}, {grp * 100 + k})")
                pk += 1
        client.execute_sql(f"INSERT INTO t VALUES {', '.join(rows)}", schema_name=sn)
        m = _scan_reduce_map(client, vid)
        assert m == {1: 100, 2: 200, 3: 300}

        # Retract group 2's current MIN (pk=5, val=200) → next-best is 201.
        client.execute_sql("DELETE FROM t WHERE pk = 5", schema_name=sn)
        m = _scan_reduce_map(client, vid)
        assert m == {1: 100, 2: 201, 3: 300}, f"grouped MIN retraction: {m}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


@NEEDS_MULTI
def test_grouped_max_multiworker_retract_current_max(client):
    sn = "gmax_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, MAX(val) AS m FROM t GROUP BY grp",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        rows = []
        pk = 0
        for grp in (1, 2, 3):
            for k in range(5):
                rows.append(f"({pk}, {grp}, {grp * 100 + k})")
                pk += 1
        client.execute_sql(f"INSERT INTO t VALUES {', '.join(rows)}", schema_name=sn)
        m = _scan_reduce_map(client, vid)
        assert m == {1: 104, 2: 204, 3: 304}

        # Retract group 2's current MAX (pk=9, val=204) → next-best is 203.
        client.execute_sql("DELETE FROM t WHERE pk = 9", schema_name=sn)
        m = _scan_reduce_map(client, vid)
        assert m == {1: 104, 2: 203, 3: 304}, f"grouped MAX retraction: {m}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


# -----------------------------------------------------------------------
# Global (no-GROUP BY) MIN/MAX retraction must recompute the next-best extremum.
#
# A global MIN/MAX stays on the single-worker FUNNEL: its ExchangeShard(∅) routes
# every row to V₀'s owner, where one reduce holds the full input history
# (trace_in / AggValueIndex) and recomputes the next-best on retraction. The
# two-phase distributable path is restricted to all-linear aggregates precisely
# because a per-worker MIN/MAX partial cannot be retraction-combined without that
# per-extremum history — so MIN/MAX keeps the funnel and these stay correct.
# -----------------------------------------------------------------------

def _scan_global_agg(client, vid):
    """Single positive-weight row of a global (no GROUP BY) aggregate → m."""
    rows = list(client.scan(vid))
    assert len(rows) == 1, f"expected one global row, got {len(rows)}"
    return rows[0]["m"]


@NEEDS_MULTI
def test_global_min_multiworker_retract_current_min(client):
    sn = "gmr_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT MIN(val) AS m FROM t",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        # Global MIN = 5 (pk=0). 30 rows spread across workers.
        vals = ", ".join(f"({i}, {5 + i})" for i in range(30))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        assert _scan_global_agg(client, vid) == 5

        # Delete the global minimum; next-best is 6 (pk=1).
        client.execute_sql("DELETE FROM t WHERE pk = 0", schema_name=sn)
        assert _scan_global_agg(client, vid) == 6, "global MIN must recompute to next-best after deletion"
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


@NEEDS_MULTI
def test_global_max_multiworker_retract_current_max(client):
    sn = "gxr_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT MAX(val) AS m FROM t",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        # Global MAX = 34 (pk=29). Next-best after deletion is 33.
        vals = ", ".join(f"({i}, {5 + i})" for i in range(30))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        assert _scan_global_agg(client, vid) == 34

        client.execute_sql("DELETE FROM t WHERE pk = 29", schema_name=sn)
        assert _scan_global_agg(client, vid) == 33, "global MAX must recompute to next-best after deletion"
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


# -----------------------------------------------------------------------
# EXCEPT incremental correctness
# -----------------------------------------------------------------------
