"""E2E tests for the parameterized bounded read (`ReadSpec`) — the server-side
ad-hoc SELECT path (bound pushdown, server-side predicate + projection, ORDER BY
/ LIMIT top-k). These exercise capabilities the old client-side thin path lacked:
string / computed predicates and projections server-side, PK / index bound
pushdown, and PK-order-preserving projection with a hidden source PK.

Run with GNITZ_WORKERS=4 (the union-gather + top-k merge across partitions):
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_read_spec.py -v --tb=short
"""
import random


def _uid():
    return str(random.randint(100000, 999999))


def _rows(client, sn, q):
    res = client.execute_sql(q, schema_name=sn)[0]
    assert res["type"] == "Rows", f"expected Rows, got {res['type']}: {res}"
    return list(res["rows"])


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


def _insert_range(client, sn, table, pairs):
    vals = ",".join(f"({a}, {b})" for a, b in pairs)
    client.execute_sql(f"INSERT INTO {table} VALUES {vals}", schema_name=sn)


# ---------------------------------------------------------------------------
# Server-side predicate + projection (new capabilities)
# ---------------------------------------------------------------------------


def test_string_predicate_server_side(client):
    """`WHERE name = 'x'` — a string comparison the old client interpreter could
    not run — is now evaluated server-side."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'alice')", schema_name=sn)
        rows = _rows(client, sn, "SELECT id FROM t WHERE name = 'alice'")
        assert sorted(r.id for r in rows) == [1, 3]
    finally:
        _cleanup(client, sn, "t")


def test_computed_projection_with_pk_bound(client):
    """`SELECT a + 1 FROM t WHERE pk = 5` — a computed projection over a PK
    point bound."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (5, 100), (6, 200)", schema_name=sn)
        rows = _rows(client, sn, "SELECT a + 1 AS ap1 FROM t WHERE id = 5")
        assert [r.ap1 for r in rows] == [101]
    finally:
        _cleanup(client, sn, "t")


def test_projection_order_pk_not_first(client):
    """`SELECT v, id` returns both columns even though `id` is the PK (the read
    path hidden-prepends the physical PK and keeps SELECT-list columns)."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        rows = _rows(client, sn, "SELECT v, id FROM t")
        assert rows[0].v == 10 and rows[0].id == 1
    finally:
        _cleanup(client, sn, "t")


def test_duplicate_projection_items(client):
    """`SELECT id AS a, id AS b` yields two identical columns."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (7, 70)", schema_name=sn)
        rows = _rows(client, sn, "SELECT id AS x, id AS y FROM t")
        assert rows[0].x == 7 and rows[0].y == 7
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Bound pushdown
# ---------------------------------------------------------------------------


def test_pk_range(client):
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        _insert_range(client, sn, "t", [(i, i * 10) for i in range(20)])
        rows = _rows(client, sn, "SELECT id FROM t WHERE id >= 15")
        assert sorted(r.id for r in rows) == [15, 16, 17, 18, 19]
        rows = _rows(client, sn, "SELECT id FROM t WHERE id > 5 AND id < 9")
        assert sorted(r.id for r in rows) == [6, 7, 8]
    finally:
        _cleanup(client, sn, "t")


def test_pk_range_signed(client):
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (-5, 1), (-1, 2), (0, 3), (3, 4)", schema_name=sn)
        rows = _rows(client, sn, "SELECT id FROM t WHERE id > -2")
        assert sorted(r.id for r in rows) == [-1, 0, 3]
    finally:
        _cleanup(client, sn, "t")


def test_pk_in_large(client):
    """A large `pk IN (…)` list is one bounded gather; absent keys miss silently."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        _insert_range(client, sn, "t", [(i, i) for i in range(1000)])
        in_list = ",".join(str(v) for v in [3, 17, 500, 999, 12345])
        rows = _rows(client, sn, f"SELECT id FROM t WHERE id IN ({in_list})")
        assert sorted(r.id for r in rows) == [3, 17, 500, 999]
    finally:
        _cleanup(client, sn, "t")


def test_non_selective_indexed_predicate(client):
    """A ≤8-byte-int indexed `WHERE flag = 1` whose bound the selectivity gate may
    degrade to a full cursor still returns only matching rows (the conjunct stays
    in the server predicate)."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, flag BIGINT)", schema_name=sn)
        client.execute_sql("CREATE INDEX ix ON t (flag)", schema_name=sn)
        _insert_range(client, sn, "t", [(i, i % 2) for i in range(40)])
        rows = _rows(client, sn, "SELECT id FROM t WHERE flag = 1")
        assert sorted(r.id for r in rows) == [i for i in range(40) if i % 2 == 1]
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# ORDER BY / LIMIT / OFFSET over the read path
# ---------------------------------------------------------------------------


def test_order_by_limit(client):
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        _insert_range(client, sn, "t", [(i, i) for i in range(50)])
        rows = _rows(client, sn, "SELECT v FROM t ORDER BY v DESC LIMIT 3")
        assert [r.v for r in rows] == [49, 48, 47]
        rows = _rows(client, sn, "SELECT v FROM t ORDER BY v ASC LIMIT 3 OFFSET 2")
        assert [r.v for r in rows] == [2, 3, 4]
    finally:
        _cleanup(client, sn, "t")


def test_limit_zero_empty(client):
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 1), (2, 2)", schema_name=sn)
        rows = _rows(client, sn, "SELECT v FROM t LIMIT 0")
        assert rows == []
    finally:
        _cleanup(client, sn, "t")


def test_offset_no_limit(client):
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        _insert_range(client, sn, "t", [(i, i) for i in range(10)])
        rows = _rows(client, sn, "SELECT v FROM t ORDER BY v OFFSET 7")
        assert [r.v for r in rows] == [7, 8, 9]
    finally:
        _cleanup(client, sn, "t")


def test_order_by_string_nulls_limit(client):
    """ORDER BY over a TEXT key with NULLs + LIMIT across workers: the worker
    top-k discards rows under its comparator, so it must agree with the client
    window's (German-string order, absolute NULL placement) — a divergence
    would drop rows before the client sort, not merely reorder them."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 'pear'), (2, 'apple'), (3, NULL), (4, 'banana'), "
            "(5, 'apricot'), (6, NULL), (7, 'cherry')",
            schema_name=sn,
        )
        rows = _rows(client, sn, "SELECT name FROM t ORDER BY name LIMIT 3")
        assert [r.name for r in rows] == ["apple", "apricot", "banana"]
        # DESC defaults to NULLS FIRST (absolute placement, not value-flipped).
        rows = _rows(client, sn, "SELECT name FROM t ORDER BY name DESC LIMIT 3")
        assert [r.name for r in rows] == [None, None, "pear"]
        # Explicit NULLS LAST on ASC keeps NULLs out of the window entirely.
        rows = _rows(client, sn, "SELECT name FROM t ORDER BY name ASC NULLS LAST LIMIT 5")
        assert [r.name for r in rows] == ["apple", "apricot", "banana", "cherry", "pear"]
    finally:
        _cleanup(client, sn, "t")


def test_order_by_float_limit(client):
    """ORDER BY over a DOUBLE key + LIMIT: worker top-k and client window must
    agree on the float order (negative, fractional, zero)."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, x DOUBLE)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 2.5), (2, -1.75), (3, 0.0), (4, -3.25), (5, 1.25)",
            schema_name=sn,
        )
        rows = _rows(client, sn, "SELECT id FROM t ORDER BY x LIMIT 3")
        assert [r.id for r in rows] == [4, 2, 3]
        rows = _rows(client, sn, "SELECT id FROM t ORDER BY x DESC LIMIT 2")
        assert [r.id for r in rows] == [1, 5]
    finally:
        _cleanup(client, sn, "t")


def test_empty_relation(client):
    """A predicate matching nothing still returns a well-formed empty result."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        rows = _rows(client, sn, "SELECT v, id FROM t WHERE id = 999")
        assert rows == []
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# View target (freshness) and weighted / distributed sources
# ---------------------------------------------------------------------------


def test_view_reflects_preceding_push(client):
    """A read of a view reflects an immediately-preceding insert (pending ticks
    are drained before the scan)."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        client.execute_sql("CREATE VIEW pos AS SELECT id, v FROM t WHERE v > 0", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, -5), (3, 20)", schema_name=sn)
        rows = _rows(client, sn, "SELECT id FROM pos ORDER BY id")
        assert [r.id for r in rows] == [1, 3]
    finally:
        _cleanup(client, sn, "pos", "t")


def test_union_all_view_weighted_top_k(client):
    """ORDER BY + LIMIT over a UNION ALL view (weighted rows across partitions):
    the per-worker top-k merges to the correct global window (multiplicity-aware)."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW dup AS SELECT id, v FROM t UNION ALL SELECT id, v FROM t",
            schema_name=sn,
        )
        _insert_range(client, sn, "t", [(i, i) for i in range(30)])
        # Each (id, v) has weight 2 in the UNION ALL view; top-3 by v DESC over
        # the bag = 29, 29, 28.
        rows = _rows(client, sn, "SELECT v FROM dup ORDER BY v DESC LIMIT 3")
        assert [r.v for r in rows] == [29, 29, 28]
    finally:
        _cleanup(client, sn, "dup", "t")
