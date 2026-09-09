"""E2E tests for the parameterized bounded read (`ReadSpec`) — the server-side
ad-hoc SELECT path (bound pushdown, server-side predicate + projection, ORDER BY
/ LIMIT top-k). These exercise capabilities the old client-side thin path lacked:
string / computed predicates and projections server-side, PK / index bound
pushdown, and PK-order-preserving projection with a hidden source PK.

Run with GNITZ_WORKERS=4 (the union-gather + top-k merge across partitions):
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/read_verb/test_read_spec.py -v --tb=short
"""

import gnitz
from _uid import uid as _uid




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


def _insert_range(client, sn, table, rows):
    vals = ",".join("(" + ",".join(str(v) for v in row) + ")" for row in rows)
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


# ---------------------------------------------------------------------------
# Single-partition confinement: a PK range that provably lands on one worker is
# unicast rather than broadcast. Every test here fails as a SHORT result (rows
# answered by the wrong worker are simply absent), never as a wrong value.
# ---------------------------------------------------------------------------


def test_point_select_on_compound_pk(client):
    """A full point on a compound PK: the range is one key wide, so it unicasts
    to the worker owning that key's partition. `a IN (x)` is `a = x`, so both
    spellings of the leading conjunct must confine to the same worker."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT, PRIMARY KEY (a, b))",
            schema_name=sn,
        )
        vals = ",".join(f"({a}, {b}, {a * 100 + b})" for a in range(6) for b in range(6))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        for a, b in [(0, 0), (3, 4), (5, 5)]:
            for lead in (f"a = {a}", f"a IN ({a})"):
                rows = _rows(client, sn, f"SELECT v FROM t WHERE {lead} AND b = {b}")
                assert [r.v for r in rows] == [a * 100 + b], f"({a}, {b}) via {lead}"
    finally:
        _cleanup(client, sn, "t")


def test_cluster_by_prefix_range_returns_every_row(client):
    """With `CLUSTER BY a` every row sharing `a` lands in one partition, so a
    bound pinning `a` and ranging `b` is confined — and must still return the
    whole group, not the one key the range starts at."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT, "
            "PRIMARY KEY (a, b)) CLUSTER BY a",
            schema_name=sn,
        )
        vals = ",".join(f"({a}, {b}, {a * 100 + b})" for a in range(4) for b in range(10))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        rows = _rows(client, sn, "SELECT b FROM t WHERE a = 2 AND b BETWEEN 3 AND 7")
        assert sorted(r.b for r in rows) == [3, 4, 5, 6, 7]
        # The whole `a` group, unbounded above.
        rows = _rows(client, sn, "SELECT b FROM t WHERE a = 2 AND b >= 0")
        assert sorted(r.b for r in rows) == list(range(10))
    finally:
        _cleanup(client, sn, "t")


def test_point_select_on_partitioned_view(client):
    """A view over a non-replicated source has a hash-partitioned output store,
    so a point on its key is confinable — and must find its row."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT, v BIGINT)", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW agg AS SELECT g, SUM(v) AS total FROM t GROUP BY g",
            schema_name=sn,
        )
        _insert_range(client, sn, "t", [(i, i % 5, i) for i in range(40)])
        for g in range(5):
            rows = _rows(client, sn, f"SELECT total FROM agg WHERE g = {g}")
            assert [r.total for r in rows] == [sum(i for i in range(40) if i % 5 == g)], f"g={g}"
    finally:
        _cleanup(client, sn, "agg", "t")


def test_empty_pk_range_count_returns_zero(client):
    """A provably-empty PK range skips the fan-out for a rows sink — but a fold
    still owes its ground row, so COUNT(*) must answer 0, not an empty result."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        _insert_range(client, sn, "t", [(i, i) for i in range(20)])
        rows = _rows(client, sn, "SELECT COUNT(*) AS c FROM t WHERE id > 10 AND id < 5")
        assert [r.c for r in rows] == [0]
        # The rows sink over the same empty range is genuinely empty.
        assert _rows(client, sn, "SELECT id FROM t WHERE id > 10 AND id < 5") == []
    finally:
        _cleanup(client, sn, "t")


def test_pk_in_spans_every_worker(client):
    """`pk IN (…)` broadcasts and each worker keeps only the keys it can own —
    a filter that must mirror the cursor exactly, or the result comes up short."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        _insert_range(client, sn, "t", [(i, i * 3) for i in range(400)])
        wanted = list(range(0, 400, 3))
        in_list = ",".join(str(v) for v in wanted)
        rows = _rows(client, sn, f"SELECT id, v FROM t WHERE id IN ({in_list})")
        assert sorted(r.id for r in rows) == wanted
        assert all(r.v == r.id * 3 for r in rows)
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


def test_all_empty_broadcast_keeps_the_column_metadata(client):
    """An indexed read matching on no worker: every worker's reply frame carries
    neither rows nor a schema block, so the master forwards none of them and the
    client sees only the terminal frame. The result must still present the column
    metadata — which the client authored and shipped with the request, since a
    read-spec reply never carries a schema block back.

    The unprojected source PK rides along as a hidden leading column and is not
    part of the presented shape."""
    sn = "rs" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, flag BIGINT)", schema_name=sn)
        client.execute_sql("CREATE INDEX ix ON t (flag)", schema_name=sn)
        _insert_range(client, sn, "t", [(i, i % 4) for i in range(40)])

        hit = _rows(client, sn, "SELECT id, flag FROM t WHERE flag = 2")
        assert sorted(r.id for r in hit) == [i for i in range(40) if i % 4 == 2]

        res = client.execute_sql("SELECT id, flag FROM t WHERE flag = 999", schema_name=sn)[0]
        assert res["type"] == "Rows", f"expected Rows, got {res}"
        miss = res["rows"]
        assert len(miss) == 0
        visible = [(c.name, c.type_code) for c in miss.schema.columns if not c.is_hidden]
        assert visible == [("id", gnitz.TypeCode.I64), ("flag", gnitz.TypeCode.I64)]
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
