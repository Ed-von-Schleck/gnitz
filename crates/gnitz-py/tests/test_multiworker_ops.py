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
    return [r for r in client.scan(vid) if r.weight > 0]


def _scan_reduce_map(client, vid):
    """Scan a reduce view → {group_val: agg_val} for positive-weight rows."""
    return {row[1]: row[2] for row in client.scan(vid) if row.weight > 0}


# -----------------------------------------------------------------------
# Sub-8-byte column types (I32) in SUM / MIN / MAX
# -----------------------------------------------------------------------


def _make_i32_table(client, sn):
    """Create (pk U64 PK, grp I64, val I32) table via client API."""
    cols = [
        gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("grp", gnitz.TypeCode.I64),
        gnitz.ColumnDef("val", gnitz.TypeCode.I32),
    ]
    schema = gnitz.Schema(cols)
    tname = "t_" + _uid()
    tid = client.create_table(sn, tname, cols)
    return tid, schema, tname


def _make_i32_reduce_view(client, sn, tid, agg_func_id, vname=None):
    if vname is None:
        vname = "v_" + _uid()
    cb = client.circuit_builder(source_table_id=tid)
    inp = cb.input_delta()
    red = cb.reduce(inp, group_by_cols=[1], agg_func_id=agg_func_id, agg_col_idx=2)
    cb.sink(red)
    circuit = cb.build()
    out_cols = [
        gnitz.ColumnDef("pk", gnitz.TypeCode.U128, primary_key=True),
        gnitz.ColumnDef("grp", gnitz.TypeCode.I64),
        gnitz.ColumnDef("agg", gnitz.TypeCode.I64),
    ]
    vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)
    return vid, vname


def _push_i32(client, tid, schema, rows, weight=1):
    batch = gnitz.ZSetBatch(schema)
    for pk, grp, val in rows:
        batch.append(pk=pk, grp=grp, val=val, weight=weight)
    client.push(tid, batch)


@_NEEDS_MULTI
def test_sum_i32_multiworker(client):
    """SUM on I32 column across multiple workers must read 4-byte values
    correctly, not 8-byte garbage."""
    sn = "si32_" + _uid()
    client.create_schema(sn)
    try:
        tid, schema, tname = _make_i32_table(client, sn)
        vid, vname = _make_i32_reduce_view(client, sn, tid, agg_func_id=2)  # SUM

        rows = [(i, 1 if i % 2 == 0 else 2, i) for i in range(1, 101)]
        _push_i32(client, tid, schema, rows)

        totals = _scan_reduce_map(client, vid)
        expected_g1 = sum(i for i in range(1, 101) if i % 2 == 0)  # 2550
        expected_g2 = sum(i for i in range(1, 101) if i % 2 != 0)  # 2500
        assert totals[1] == expected_g1, f"SUM(I32) group 1: {totals[1]} != {expected_g1}"
        assert totals[2] == expected_g2, f"SUM(I32) group 2: {totals[2]} != {expected_g2}"
    finally:
        _drop_all(client, sn, views=[vname], tables=[tname])


@_NEEDS_MULTI
def test_min_i32_negative_values(client):
    """MIN on I32 column including negative values.  Verifies sign-extension
    works correctly for sub-8-byte signed types."""
    sn = "mi32_" + _uid()
    client.create_schema(sn)
    try:
        tid, schema, tname = _make_i32_table(client, sn)
        vid, vname = _make_i32_reduce_view(client, sn, tid, agg_func_id=3)  # MIN

        rows = [
            (1, 1, 100), (2, 1, 50), (3, 1, 200),
            (4, 2, 10), (5, 2, -30), (6, 2, 5),
        ]
        _push_i32(client, tid, schema, rows)

        totals = _scan_reduce_map(client, vid)
        assert totals[1] == 50, f"MIN(I32) group 1: {totals[1]} != 50"
        assert totals[2] == -30, f"MIN(I32) group 2: {totals[2]} != -30"
    finally:
        _drop_all(client, sn, views=[vname], tables=[tname])


@_NEEDS_MULTI
def test_max_i32_negative_values(client):
    """MAX on I32 column including negative values."""
    sn = "xi32_" + _uid()
    client.create_schema(sn)
    try:
        tid, schema, tname = _make_i32_table(client, sn)
        vid, vname = _make_i32_reduce_view(client, sn, tid, agg_func_id=4)  # MAX

        rows = [
            (1, 1, -5), (2, 1, -10), (3, 1, -1),
            (4, 2, 100), (5, 2, 200), (6, 2, 150),
        ]
        _push_i32(client, tid, schema, rows)

        totals = _scan_reduce_map(client, vid)
        assert totals[1] == -1, f"MAX(I32) group 1: {totals[1]} != -1"
        assert totals[2] == 200, f"MAX(I32) group 2: {totals[2]} != 200"
    finally:
        _drop_all(client, sn, views=[vname], tables=[tname])


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

        rows = _scan_positive(client, vid)
        totals = {r[1]: r[2] for r in rows}

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

        # b excludes pk=1; pk=2 is free
        client.execute_sql("INSERT INTO b VALUES (1, 999)", schema_name=sn)
        client.execute_sql("INSERT INTO a VALUES (1, 100), (2, 200)", schema_name=sn)

        rows1 = _scan_positive(client, vid)
        pks1 = sorted(r["pk"] for r in rows1)
        assert pks1 == [2], f"before update: {pks1}"

        # UPDATE pk=2 → delta has (pk=2,val=200,w=-1) and (pk=2,val=300,w=+1)
        # pk=2 is NOT in b, so both delta rows pass anti-join.
        # Without cursor re-seek: second row misses → pk=2 disappears.
        client.execute_sql("UPDATE a SET val = 300 WHERE pk = 2", schema_name=sn)

        rows2 = _scan_positive(client, vid)
        pks2 = sorted(r["pk"] for r in rows2)
        assert pks2 == [2], f"after update: expected [2], got {pks2}"
        assert rows2[0]["val"] == 300, f"expected val=300, got {rows2[0]['val']}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


@_NEEDS_MULTI
def test_intersect_stable_after_update(client):
    """UPDATE through INTERSECT: the semi-join DT cursor must re-seek so the
    new row (same PK, different payload) still matches the trace."""
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

        # UPDATE pk=1 val=100→500.  Delta: (pk=1,val=100,w=-1), (pk=1,val=500,w=+1).
        # pk=1 is in b → both delta rows should be semi-join matched.
        # Without re-seek: second row misses cursor → pk=1 disappears from INTERSECT.
        client.execute_sql("UPDATE a SET val = 500 WHERE pk = 1", schema_name=sn)

        rows2 = _scan_positive(client, vid)
        pks2 = sorted(r["pk"] for r in rows2)
        assert pks2 == [1], f"after update: expected [1], got {pks2}"
        assert rows2[0]["val"] == 500, f"expected val=500, got {rows2[0]['val']}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


# -----------------------------------------------------------------------
# INTERSECT no weight inflation (swapped semi-join path)
# -----------------------------------------------------------------------


@_NEEDS_MULTI
def test_intersect_no_weight_inflation(client):
    """INTERSECT must not duplicate rows when the trace side has multiple
    entries for the same PK.  A large delta triggers the swapped semi-join
    path (iterate trace, binary-search delta)."""
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
