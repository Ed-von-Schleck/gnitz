"""Multi-worker operator tests: sub-8-byte aggregates, STRING exchange routing,
set operations (EXCEPT/INTERSECT) with updates, and gather-reduce MIN/MAX."""

import os
import random

import pytest
import gnitz

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)


def _uid():
    return str(random.randint(100_000, 999_999))


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


def _scan_positive(client, vid):
    return list(client.scan(vid))


def _scan_reduce_map(client, vid):
    """Scan a reduce view → {group_val: agg_val} for positive-weight rows.

    The visible layout of a GROUP BY view is [group_col, agg] whether the view
    is keyed naturally or by a (hidden) synthetic group PK.
    """
    return {row[0]: row[1] for row in client.scan(vid)}


# -----------------------------------------------------------------------
# Sub-8-byte column types (I32) in SUM / MIN / MAX
# -----------------------------------------------------------------------


# -----------------------------------------------------------------------
# STRING column in exchange routing
# -----------------------------------------------------------------------


@_NEEDS_MULTI
def test_string_group_by_multiworker(client):
    """STRING column as GROUP BY key routed across workers.  STRING is 16 bytes;
    exchange routing must hash the content, not copy 16 bytes into an 8-byte buffer."""
    sn = "sgb_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "name VARCHAR(100) NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT name, SUM(val) AS total FROM t GROUP BY name",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        vals = ", ".join([
            "(1, 'alpha', 10)", "(2, 'alpha', 20)", "(3, 'alpha', 30)",
            "(4, 'beta', 100)", "(5, 'beta', 200)",
            "(6, 'a_very_long_group_name', 5)", "(7, 'a_very_long_group_name', 15)",
        ])
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        totals = _scan_reduce_map(client, vid)

        assert totals["alpha"] == 60, f"alpha: {totals.get('alpha')}"
        assert totals["beta"] == 300, f"beta: {totals.get('beta')}"
        assert totals["a_very_long_group_name"] == 20, (
            f"long name: {totals.get('a_very_long_group_name')}"
        )
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


# -----------------------------------------------------------------------
# EXCEPT cursor re-seek on UPDATE (same-PK retraction + insertion)
# -----------------------------------------------------------------------


@_NEEDS_MULTI
def test_except_stable_after_update(client):
    """UPDATE on a UNIQUE PK table generates a retraction + insertion with the
    same PK in one delta.  The anti-join DT cursor must re-seek for the second
    row, not advance past the PK after the first."""
    sn = "exu_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY UNIQUE, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY UNIQUE, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM a EXCEPT SELECT * FROM b",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        # Full-row identity: b's (1,999) does NOT exclude a's (1,100) — they
        # differ in payload — so both a rows survive.
        client.execute_sql("INSERT INTO b VALUES (1, 999)", schema_name=sn)
        client.execute_sql("INSERT INTO a VALUES (1, 100), (2, 200)", schema_name=sn)

        rows1 = _scan_positive(client, vid)
        pks1 = sorted(r["pk"] for r in rows1)
        assert pks1 == [1, 2], f"before update: {pks1}"

        # UPDATE pk=2 → delta has (pk=2,val=200,w=-1) and (pk=2,val=300,w=+1).
        # pk=2 is NOT in b, so both delta rows pass anti-join.
        # Without cursor re-seek: second row misses → pk=2 disappears.
        client.execute_sql("UPDATE a SET val = 300 WHERE pk = 2", schema_name=sn)

        rows2 = _scan_positive(client, vid)
        pks2 = sorted(r["pk"] for r in rows2)
        assert pks2 == [1, 2], f"after update: expected [1, 2], got {pks2}"
        val2 = next(r["val"] for r in rows2 if r["pk"] == 2)
        assert val2 == 300, f"expected pk=2 val=300, got {val2}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


@_NEEDS_MULTI
def test_intersect_stable_after_update(client):
    """UPDATE through INTERSECT: the join-free min(da,db) arithmetic must track
    membership per full-row identity so the new row (same PK, different payload)
    is treated as a distinct element."""
    sn = "inu_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY UNIQUE, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY UNIQUE, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM a INTERSECT SELECT * FROM b",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        client.execute_sql("INSERT INTO b VALUES (1, 100), (2, 200)", schema_name=sn)
        client.execute_sql("INSERT INTO a VALUES (1, 100), (3, 300)", schema_name=sn)

        rows1 = _scan_positive(client, vid)
        pks1 = sorted(r["pk"] for r in rows1)
        assert pks1 == [1], f"before update: {pks1}"

        # UPDATE pk=1 val=100→500. Delta: (pk=1,val=100,w=-1), (pk=1,val=500,w=+1).
        # The retraction of (1,100) must drop the intersect row. Under full-row
        # identity the new (1,500) does NOT match b's (1,100), so pk=1 leaves the
        # INTERSECT entirely.
        client.execute_sql("UPDATE a SET val = 500 WHERE pk = 1", schema_name=sn)

        rows2 = _scan_positive(client, vid)
        pks2 = sorted(r["pk"] for r in rows2)
        assert pks2 == [], f"after update: expected [], got {pks2}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


# -----------------------------------------------------------------------
# INTERSECT no weight inflation
# -----------------------------------------------------------------------


@_NEEDS_MULTI
def test_intersect_no_weight_inflation(client):
    """INTERSECT must not duplicate rows when one side has multiple entries for
    the same projected value. The leaf distinct caps each side's multiplicity at
    1, so min(da, db) stays in {0,1}."""
    sn = "iwi_" + _uid()
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
            "CREATE VIEW v AS SELECT * FROM a INTERSECT SELECT * FROM b",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        b_vals = ", ".join(f"({i}, {i * 10})" for i in [1, 2, 3])
        client.execute_sql(f"INSERT INTO b VALUES {b_vals}", schema_name=sn)

        # 30 rows >> 3 in b → triggers swap path
        a_vals = ", ".join(f"({i}, {i * 10})" for i in range(1, 31))
        client.execute_sql(f"INSERT INTO a VALUES {a_vals}", schema_name=sn)

        rows = _scan_positive(client, vid)
        pks = sorted(r["pk"] for r in rows)
        assert pks == [1, 2, 3], f"expected [1,2,3], got {pks}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


# -----------------------------------------------------------------------
# Exchange merge consolidated flag
# -----------------------------------------------------------------------


@_NEEDS_MULTI
def test_exchange_merge_sum_correct(client):
    """SUM across workers after exchange merge must not double-count.
    Verifies the exchange output is not falsely marked as consolidated."""
    sn = "emc_" + _uid()
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

        vals = ", ".join(f"({i}, 1, {i})" for i in range(1, 101))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        totals = _scan_reduce_map(client, vid)
        expected = sum(range(1, 101))  # 5050
        assert totals[1] == expected, f"SUM: {totals[1]} != {expected}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


# -----------------------------------------------------------------------
# Multi-worker MIN/MAX incremental gather-reduce
# -----------------------------------------------------------------------


@_NEEDS_MULTI
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


@_NEEDS_MULTI
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


@_NEEDS_MULTI
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


@_NEEDS_MULTI
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


@_NEEDS_MULTI
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


@_NEEDS_MULTI
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


@_NEEDS_MULTI
def test_except_update_non_excluded_row(client):
    """UPDATE a row that is NOT excluded by b.  The delta has same-PK
    retraction + insertion; the anti-join DT must process both correctly."""
    sn = "eur_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY UNIQUE, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY UNIQUE, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM a EXCEPT SELECT * FROM b",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        # b excludes pk=1; pk=2 is free
        client.execute_sql("INSERT INTO b VALUES (1, 10)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn,
        )
        rows1 = _scan_positive(client, vid)
        assert sorted(r["pk"] for r in rows1) == [2, 3]

        # Update pk=3 (not excluded) → should stay in output with new value
        client.execute_sql("UPDATE a SET val = 999 WHERE pk = 3", schema_name=sn)
        rows2 = _scan_positive(client, vid)
        pks2 = sorted(r["pk"] for r in rows2)
        assert pks2 == [2, 3], f"after update: expected [2, 3], got {pks2}"
        val3 = next(r["val"] for r in rows2 if r["pk"] == 3)
        assert val3 == 999, f"expected val=999, got {val3}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


@_NEEDS_MULTI
def test_except_multi_tick_exclusion(client):
    """EXCEPT across multiple ticks: insert b, then a in separate epochs.
    Verifies correct PK-based exclusion with 20 rows across workers."""
    sn = "emt_" + _uid()
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
        vid, _ = client.resolve_table(sn, "v")

        # b excludes even PKs
        b_vals = ", ".join(f"({i}, {i * 10})" for i in range(2, 21, 2))
        client.execute_sql(f"INSERT INTO b VALUES {b_vals}", schema_name=sn)

        # a has all 20 PKs
        a_vals = ", ".join(f"({i}, {i * 10})" for i in range(1, 21))
        client.execute_sql(f"INSERT INTO a VALUES {a_vals}", schema_name=sn)

        rows = _scan_positive(client, vid)
        pks = sorted(r["pk"] for r in rows)
        expected = sorted(range(1, 21, 2))  # odd PKs only
        assert pks == expected, f"expected {expected}, got {pks}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


# ---------------------------------------------------------------------------
# Regression: multi-schema exchange interleaving
# ---------------------------------------------------------------------------

@_NEEDS_MULTI
class TestMultiSchemaExchange:
    """Regression tests for multi-worker exchange correctness when multiple
    schemas contain exchange-requiring views (join + agg).

    The bugs these cover:
    - Exchange loop consumed DDL_SYNC messages from the SAL instead of
      waiting for the exchange relay, producing wrong column counts.
    - poll_tick_progress (async tick) did not propagate the exchange schema
      to relay_exchange, causing the relay to use the view output schema.
    - Join-shard exchange path did not set batch.schema, causing encode_wire
      to panic on None schema.
    """

    def test_join_agg_view_across_schemas(self, client):
        """Two schemas each with a join+agg view chain. The first schema's
        views must survive tick processing while the second schema's DDL
        creates interleaved SAL messages."""
        rng = random.Random(42)

        # --- Schema A: filter → join → agg (4-column fact table) ---
        sa = "mw_xchg_a_" + _uid()
        client.create_schema(sa)
        try:
            client.execute_sql(
                "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
                "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL, "
                "category BIGINT NOT NULL)", schema_name=sa)
            client.execute_sql(
                "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
                "region BIGINT NOT NULL)", schema_name=sa)
            client.execute_sql(
                "CREATE VIEW v_filter AS SELECT * FROM fact WHERE amount > 0",
                schema_name=sa)
            client.execute_sql(
                "CREATE VIEW v_joined AS SELECT v_filter.pk, v_filter.amount, "
                "v_filter.category, dim.region "
                "FROM v_filter INNER JOIN dim ON v_filter.dim_pk = dim.pk",
                schema_name=sa)
            client.execute_sql(
                "CREATE VIEW v_agg AS SELECT region, SUM(amount) AS total "
                "FROM v_joined GROUP BY region", schema_name=sa)

            # Load dim via push
            dt, ds = client.resolve_table(sa, "dim")
            batch = gnitz.ZSetBatch(ds)
            for i in range(1, 11):
                batch.append(pk=i, region=i % 5)
            client.push(dt, batch)

            # INSERT rows (triggers tick → exchange)
            for bi in range(5):
                bp = bi * 20
                vals = ", ".join(
                    f"({bp+j+1}, {(bp+j) % 10 + 1}, {rng.randint(1, 1000)}, {rng.randint(0, 5)})"
                    for j in range(20))
                client.execute_sql(
                    f"INSERT INTO fact (pk, dim_pk, amount, category) VALUES {vals}",
                    schema_name=sa)

            # --- Schema B: join → agg (3-column fact, different column count) ---
            sb = "mw_xchg_b_" + _uid()
            client.create_schema(sb)
            try:
                client.execute_sql(
                    "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
                    "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL)",
                    schema_name=sb)
                client.execute_sql(
                    "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
                    "region BIGINT NOT NULL)", schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v_joined AS SELECT fact.pk, fact.amount, dim.region "
                    "FROM fact INNER JOIN dim ON fact.dim_pk = dim.pk",
                    schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v_agg AS SELECT region, SUM(amount) AS total "
                    "FROM v_joined GROUP BY region", schema_name=sb)

                dt2, ds2 = client.resolve_table(sb, "dim")
                batch2 = gnitz.ZSetBatch(ds2)
                for i in range(1, 11):
                    batch2.append(pk=i, region=i % 5)
                client.push(dt2, batch2)

                for bi in range(5):
                    bp = bi * 20
                    vals = ", ".join(
                        f"({bp+j+1}, {(bp+j) % 10 + 1}, {rng.randint(1, 1000)})"
                        for j in range(20))
                    client.execute_sql(
                        f"INSERT INTO fact (pk, dim_pk, amount) VALUES {vals}",
                        schema_name=sb)

                # Verify both agg views produce correct results
                _, sa_agg_schema = client.resolve_table(sa, "v_agg")
                _, sb_agg_schema = client.resolve_table(sb, "v_agg")
                sa_vid, _ = client.resolve_table(sa, "v_agg")
                sb_vid, _ = client.resolve_table(sb, "v_agg")

                sa_rows = list(client.scan(sa_vid))
                sb_rows = list(client.scan(sb_vid))

                assert len(sa_rows) > 0, "schema A agg view should have results"
                assert len(sb_rows) > 0, "schema B agg view should have results"

            finally:
                _drop_all(client, sb, views=["v_agg", "v_joined"],
                          tables=["fact", "dim"])
        finally:
            _drop_all(client, sa, views=["v_agg", "v_joined", "v_filter"],
                      tables=["fact", "dim"])

    def test_deep_pipeline_after_prior_schemas(self, client):
        """A deep view pipeline (5 views) created after other schemas with
        exchange views are already active."""
        rng = random.Random(42)

        # --- Schema A: join + agg ---
        sa = "mw_deep_a_" + _uid()
        client.create_schema(sa)
        try:
            client.execute_sql(
                "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
                "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL, "
                "category BIGINT NOT NULL)", schema_name=sa)
            client.execute_sql(
                "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
                "label BIGINT NOT NULL)", schema_name=sa)
            client.execute_sql(
                "CREATE VIEW v_joined AS SELECT fact.pk, fact.amount, "
                "fact.category, dim.label "
                "FROM fact INNER JOIN dim ON fact.dim_pk = dim.pk",
                schema_name=sa)
            client.execute_sql(
                "CREATE VIEW v_agg AS SELECT label, SUM(amount) AS total "
                "FROM v_joined GROUP BY label", schema_name=sa)

            dt, ds = client.resolve_table(sa, "dim")
            batch = gnitz.ZSetBatch(ds)
            for i in range(1, 21):
                batch.append(pk=i, label=i % 10)
            client.push(dt, batch)

            for bi in range(3):
                bp = bi * 20
                vals = ", ".join(
                    f"({bp+j+1}, {(bp+j) % 20 + 1}, {rng.randint(1, 1000)}, {rng.randint(0, 5)})"
                    for j in range(20))
                client.execute_sql(
                    f"INSERT INTO fact (pk, dim_pk, amount, category) VALUES {vals}",
                    schema_name=sa)

            # --- Schema B: 5-view chain ---
            sb = "mw_deep_b_" + _uid()
            client.create_schema(sb)
            try:
                client.execute_sql(
                    "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
                    "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL, "
                    "category BIGINT NOT NULL)", schema_name=sb)
                client.execute_sql(
                    "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
                    "label BIGINT NOT NULL)", schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v1 AS SELECT * FROM fact WHERE amount > 50",
                    schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v2 AS SELECT v1.pk, v1.amount, v1.category, "
                    "dim.label FROM v1 INNER JOIN dim ON v1.dim_pk = dim.pk",
                    schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v3 AS SELECT label, SUM(amount) AS total, "
                    "COUNT(*) AS cnt FROM v2 GROUP BY label",
                    schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v4 AS SELECT * FROM v3 WHERE total > 10",
                    schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v5 AS SELECT DISTINCT label FROM v4",
                    schema_name=sb)

                dt2, ds2 = client.resolve_table(sb, "dim")
                batch2 = gnitz.ZSetBatch(ds2)
                for i in range(1, 11):
                    batch2.append(pk=i, label=i % 5)
                client.push(dt2, batch2)

                for bi in range(3):
                    bp = bi * 20
                    vals = ", ".join(
                        f"({bp+j+1}, {(bp+j) % 10 + 1}, {rng.randint(1, 1000)}, {rng.randint(0, 5)})"
                        for j in range(20))
                    client.execute_sql(
                        f"INSERT INTO fact (pk, dim_pk, amount, category) VALUES {vals}",
                        schema_name=sb)

                # Verify the deep pipeline produces results
                vid5, _ = client.resolve_table(sb, "v5")
                rows = list(client.scan(vid5))
                assert len(rows) > 0, "v5 (distinct labels) should have results"

            finally:
                _drop_all(client, sb, views=["v5", "v4", "v3", "v2", "v1"],
                          tables=["fact", "dim"])
        finally:
            _drop_all(client, sa, views=["v_agg", "v_joined"],
                      tables=["fact", "dim"])
