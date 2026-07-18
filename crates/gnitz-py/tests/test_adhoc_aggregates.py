"""E2E parity tests for ad-hoc single-relation aggregation (GROUP BY / global
aggregate / HAVING / DISTINCT) served by the ReadSpec aggregate hash-fold sink +
client finishing, versus a `CREATE VIEW` of the same statement.

The load-bearing assertion is **parity**: for a query with no ORDER BY / LIMIT,
the ad-hoc result equals a scan of `CREATE VIEW v AS <same query>` — exact for
integer data, approximate for float SUM/AVG (cross-worker addition order).

Run with GNITZ_WORKERS=4 (the fold is per-worker; the client merges partials):
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_adhoc_aggregates.py -v --tb=short
"""
import math
import random

import pytest


def _uid():
    return str(random.randint(100000, 999999))


def _rows(client, sn, q):
    res = client.execute_sql(q, schema_name=sn)[0]
    assert res["type"] == "Rows", f"expected Rows, got {res['type']}: {res}"
    return list(res["rows"])


def _norm(rows, approx=False):
    """Canonicalize rows to a sorted list of sorted (name, value) tuples, so two
    result sets compare order-independently. Floats are rounded under `approx`."""
    out = []
    for r in rows:
        items = []
        for k, v in sorted(r._asdict().items()):
            if approx and isinstance(v, float):
                v = None if math.isnan(v) else round(v, 6)
            items.append((k, v))
        out.append(tuple(items))
    return sorted(out, key=repr)


def _cleanup(client, sn, *names):
    for name in names:
        for kind in ("VIEW", "TABLE"):
            try:
                client.execute_sql(f"DROP {kind} {name}", schema_name=sn)
            except Exception:
                pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _parity(client, sn, query, approx=False):
    """Assert the ad-hoc result of `query` (no ORDER BY / LIMIT) equals a scan of
    a view built from the identical statement."""
    adhoc = _rows(client, sn, query)
    vn = "pv_" + _uid()
    client.execute_sql(f"CREATE VIEW {vn} AS {query}", schema_name=sn)
    vid = client.resolve_table(sn, vn)[0]
    view = list(client.scan(vid))
    client.execute_sql(f"DROP VIEW {vn}", schema_name=sn)

    a, v = _norm(adhoc, approx), _norm(view, approx)
    assert a == v, f"ad-hoc != view for `{query}`\n  ad-hoc: {a}\n  view:   {v}"
    # Column-name parity (visible output columns).
    if adhoc and view:
        assert set(adhoc[0]._asdict().keys()) == set(view[0]._asdict().keys()), (
            f"column names differ for `{query}`"
        )
    return adhoc


# ---------------------------------------------------------------------------
# Grouped + global aggregate parity, WHERE, HAVING
# ---------------------------------------------------------------------------


def _setup_orders(client, sn):
    client.execute_sql(
        "CREATE TABLE orders ("
        "  pk BIGINT NOT NULL PRIMARY KEY,"
        "  category BIGINT NOT NULL,"
        "  amount BIGINT NOT NULL,"
        "  note BIGINT"  # nullable
        ")",
        schema_name=sn,
    )
    rows = []
    for i in range(1, 41):
        cat = i % 5
        amount = (i * 7) % 100
        note = "NULL" if i % 3 == 0 else str((i * 3) % 50)
        rows.append(f"({i}, {cat}, {amount}, {note})")
    client.execute_sql("INSERT INTO orders VALUES " + ",".join(rows), schema_name=sn)


@pytest.mark.parametrize(
    "agg",
    [
        "COUNT(*) AS m",
        "COUNT(note) AS m",
        "SUM(amount) AS m",
        "MIN(amount) AS m",
        "MAX(amount) AS m",
        "AVG(amount) AS m",
    ],
)
def test_grouped_parity(client, agg):
    sn = "aa" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        approx = "AVG" in agg
        _parity(client, sn, f"SELECT category, {agg} FROM orders GROUP BY category", approx=approx)
        # With a WHERE (bounded PK range + residual).
        _parity(
            client,
            sn,
            f"SELECT category, {agg} FROM orders WHERE pk > 5 AND amount < 80 GROUP BY category",
            approx=approx,
        )
    finally:
        _cleanup(client, sn, "orders")


@pytest.mark.parametrize(
    "agg",
    [
        "COUNT(*) AS m",
        "COUNT(note) AS m",
        "SUM(amount) AS m",
        "MIN(amount) AS m",
        "MAX(amount) AS m",
        "AVG(amount) AS m",
    ],
)
def test_global_parity(client, agg):
    sn = "ag" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        approx = "AVG" in agg
        _parity(client, sn, f"SELECT {agg} FROM orders", approx=approx)
        _parity(client, sn, f"SELECT {agg} FROM orders WHERE category = 2", approx=approx)
    finally:
        _cleanup(client, sn, "orders")


def test_multi_aggregate_and_group_col_parity(client):
    sn = "ma" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        _parity(
            client,
            sn,
            "SELECT category, COUNT(*) AS c, SUM(amount) AS s, MIN(amount) AS mn, MAX(amount) AS mx "
            "FROM orders GROUP BY category",
        )
    finally:
        _cleanup(client, sn, "orders")


def test_having_parity(client):
    sn = "hv" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        # HAVING over a projected aggregate, and over an aggregate only in HAVING.
        _parity(client, sn, "SELECT category, COUNT(*) AS c FROM orders GROUP BY category HAVING COUNT(*) > 7")
        _parity(client, sn, "SELECT category FROM orders GROUP BY category HAVING SUM(amount) > 300")
        _parity(client, sn, "SELECT category, AVG(amount) AS a FROM orders GROUP BY category HAVING AVG(amount) > 40", approx=True)
        # An aggregate that is NULL for some groups (COUNT(note)=0 → HAVING drops it, 3VL).
        _parity(client, sn, "SELECT category, MIN(note) AS mn FROM orders GROUP BY category HAVING MIN(note) > 10")
        # Global HAVING (grounds then filters).
        _parity(client, sn, "SELECT COUNT(*) AS c FROM orders HAVING COUNT(*) > 0")
    finally:
        _cleanup(client, sn, "orders")


def test_null_group_and_all_null_agg(client):
    sn = "ng" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT PRIMARY KEY, g BIGINT, v BIGINT)", schema_name=sn
        )
        # g is nullable → a NULL group; some groups have all-NULL v → COUNT(v)=0.
        client.execute_sql(
            "INSERT INTO t VALUES (1, 10, NULL), (2, 10, NULL), (3, NULL, 5), (4, NULL, 7), (5, 20, 3)",
            schema_name=sn,
        )
        # A NULL group forms its own distinct group.
        _parity(client, sn, "SELECT g, COUNT(*) AS c FROM t GROUP BY g")
        # A group whose aggregated column is entirely NULL still emits (COUNT(v)=0).
        _parity(client, sn, "SELECT g, COUNT(v) AS cv, SUM(v) AS sv, MIN(v) AS mv FROM t GROUP BY g")
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# DISTINCT
# ---------------------------------------------------------------------------


def test_distinct_parity(client):
    sn = "di" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE u (pk BIGINT PRIMARY KEY, city TEXT, region BIGINT)", schema_name=sn
        )
        client.execute_sql(
            "INSERT INTO u VALUES (1,'nyc',1),(2,'sf',2),(3,'nyc',1),(4,'sf',2),(5,'la',3),(6,'nyc',1)",
            schema_name=sn,
        )
        _parity(client, sn, "SELECT DISTINCT city FROM u")
        _parity(client, sn, "SELECT DISTINCT city, region FROM u")
        _parity(client, sn, "SELECT DISTINCT region FROM u WHERE region > 1")
    finally:
        _cleanup(client, sn, "u")


def test_distinct_star_wide_table(client):
    sn = "dw" + _uid()
    client.create_schema(sn)
    try:
        # A >8-column (wide group) table with a non-PK duplicate: DISTINCT over the
        # non-PK columns collapses the duplicate, and DISTINCT * keeps all (unique PK).
        cols = ", ".join(f"c{i} BIGINT" for i in range(10))
        client.execute_sql(f"CREATE TABLE w (pk BIGINT PRIMARY KEY, {cols})", schema_name=sn)
        vals = ", ".join(str(j) for j in range(10))
        vals2 = ", ".join(str(j + 1) for j in range(10))
        client.execute_sql(
            f"INSERT INTO w VALUES (1, {vals}), (2, {vals}), (3, {vals2})", schema_name=sn
        )
        proj = ", ".join(f"c{i}" for i in range(10))
        # DISTINCT over the 10 non-PK columns: rows 1 and 2 collapse.
        rows = _parity(client, sn, f"SELECT DISTINCT {proj} FROM w")
        assert len(rows) == 2
        assert len([k for k in rows[0]._asdict() if k != "weight"]) == 10
        # DISTINCT * keeps the PK, so all three rows survive.
        _parity(client, sn, "SELECT DISTINCT * FROM w")
    finally:
        _cleanup(client, sn, "w")


# ---------------------------------------------------------------------------
# Empty input, ground row, bag-valued (UNION ALL weight-2) source
# ---------------------------------------------------------------------------


def test_empty_table(client):
    sn = "et" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE e (pk BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        # Global aggregate over an empty table → single ground row.
        rows = _rows(client, sn, "SELECT COUNT(*) AS c, SUM(v) AS s, MAX(v) AS m FROM e")
        assert len(rows) == 1
        assert rows[0]["c"] == 0
        assert rows[0]["s"] is None
        assert rows[0]["m"] is None
        # Grouped aggregate over an empty table → zero rows.
        assert _rows(client, sn, "SELECT v, COUNT(*) AS c FROM e GROUP BY v") == []
        # WHERE filtering out every row behaves identically.
        client.execute_sql("INSERT INTO e VALUES (1, 5)", schema_name=sn)
        rows = _rows(client, sn, "SELECT COUNT(*) AS c, SUM(v) AS s FROM e WHERE v > 100")
        assert rows[0]["c"] == 0 and rows[0]["s"] is None
        assert _rows(client, sn, "SELECT v, COUNT(*) AS c FROM e WHERE v > 100 GROUP BY v") == []
    finally:
        _cleanup(client, sn, "e")


def test_union_all_weight_two_count(client):
    sn = "ua" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE base (pk BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        client.execute_sql("INSERT INTO base VALUES (1, 10), (2, 20), (3, 10)", schema_name=sn)
        # A UNION ALL view whose rows carry weight 2 (base UNION ALL base).
        client.execute_sql(
            "CREATE VIEW dbl AS SELECT pk, v FROM base UNION ALL SELECT pk, v FROM base",
            schema_name=sn,
        )
        rows = _rows(client, sn, "SELECT COUNT(*) AS c FROM dbl")
        assert rows[0]["c"] == 6, rows  # 3 base rows × weight 2
        # Grouped counts logical rows too.
        _parity(client, sn, "SELECT v, COUNT(*) AS c FROM dbl GROUP BY v")
    finally:
        _cleanup(client, sn, "dbl", "base")


# ---------------------------------------------------------------------------
# ORDER BY / LIMIT (client-side sink), rejections
# ---------------------------------------------------------------------------


def test_order_by_alias_and_position(client):
    sn = "ob" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        rows = _rows(
            client, sn, "SELECT category, COUNT(*) AS c FROM orders GROUP BY category ORDER BY c DESC LIMIT 3"
        )
        counts = [r["c"] for r in rows]
        assert len(rows) == 3
        assert counts == sorted(counts, reverse=True)
        # Positional ORDER BY over the visible output columns.
        rows2 = _rows(client, sn, "SELECT category, COUNT(*) AS c FROM orders GROUP BY category ORDER BY 1")
        assert [r["category"] for r in rows2] == sorted(r["category"] for r in rows2)
    finally:
        _cleanup(client, sn, "orders")


def test_rejections(client):
    sn = "rj" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        # ORDER BY <aggregate function> stays rejected (alias/position only).
        with pytest.raises(Exception):
            _rows(client, sn, "SELECT category, COUNT(*) FROM orders GROUP BY category ORDER BY COUNT(*)")
        # DISTINCT over a float key is rejected (parity with the view path).
        client.execute_sql("CREATE TABLE fp (pk BIGINT PRIMARY KEY, price DOUBLE)", schema_name=sn)
        client.execute_sql("INSERT INTO fp VALUES (1, 1.5), (2, 2.5)", schema_name=sn)
        with pytest.raises(Exception):
            _rows(client, sn, "SELECT DISTINCT price FROM fp")
    finally:
        _cleanup(client, sn, "orders", "fp")


# ---------------------------------------------------------------------------
# Many aggregate columns (well under the fold's physical-spec cap)
# ---------------------------------------------------------------------------


def test_many_aggregates(client):
    """A wide multi-aggregate query still folds. The fold's one width gate is
    the partial reply schema (1 + group cols + agg specs <= MAX_COLUMNS, checked
    client-side; wider plans route to the executor) — this exercises the
    many-accumulator fold path itself, well under that bound."""
    sn = "fb" + _uid()
    client.create_schema(sn)
    try:
        n = 8
        cols = ", ".join(f"c{i} BIGINT" for i in range(n))
        client.execute_sql(f"CREATE TABLE t (pk BIGINT PRIMARY KEY, g BIGINT NOT NULL, {cols})", schema_name=sn)
        rows = []
        for i in range(1, 31):
            vals = ", ".join(str((i * (j + 1)) % 50) for j in range(n))
            rows.append(f"({i}, {i % 4}, {vals})")
        client.execute_sql("INSERT INTO t VALUES " + ",".join(rows), schema_name=sn)
        aggs = ", ".join(f"SUM(c{i}) AS s{i}, MAX(c{i}) AS m{i}, COUNT(c{i}) AS n{i}" for i in range(n))
        _parity(client, sn, f"SELECT g, {aggs} FROM t GROUP BY g")
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Runtime per-worker group cap (dedicated low-cap server)
# ---------------------------------------------------------------------------


def test_group_cap_aborts(adhoc_group_cap_server):
    client = adhoc_group_cap_server
    sn = "gc" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (pk BIGINT PRIMARY KEY, g BIGINT NOT NULL)", schema_name=sn)
        # 100 distinct groups; with the cap at 4 per worker, some worker exceeds it.
        vals = ", ".join(f"({i}, {i})" for i in range(1, 101))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        with pytest.raises(Exception) as ei:
            _rows(client, sn, "SELECT g, COUNT(*) AS c FROM t GROUP BY g")
        assert "CREATE VIEW" in str(ei.value)
        # The worker keeps serving afterwards.
        rows = _rows(client, sn, "SELECT COUNT(*) AS c FROM t")
        assert rows[0]["c"] == 100
    finally:
        _cleanup(client, sn, "t")
