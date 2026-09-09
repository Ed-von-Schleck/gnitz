"""HIR CTE/derived commit — compositions the CTE/derived scoping + column-pruning
flip enables: a CTE or derived-table body that is any relational body (a set
operation, a grouped query, a join) rather than a plain SELECT; GROUP BY /
DISTINCT / a set operation *over* a derived table; a linear body over a derived
source (the non-`Get` linear-source lowering arm); a subquery over a compiled
CTE; and the pre-existing zero-payload COUNT(*) path (no floor added by the
`as_body` pruning). Maintained under inserts *and* deletes and the
data-before-view (backfill) order — asserted on **weights**, not row presence."""

from _uid import uid as _uid


def _weights(client, sn, view, keys):
    """Net weight per row — the Z-set observable (a wrong multiplicity or a
    weight-0 ghost is invisible to a presence check but caught here)."""
    vid = client.resolve_table(sn, view)[0]
    acc = {}
    for r in client.scan(vid):
        k = tuple(r._asdict()[c] for c in keys)
        acc[k] = acc.get(k, 0) + r.weight
    return {k: w for k, w in sorted(acc.items()) if w != 0}


def _tu(client, sn):
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)", schema_name=sn)


# ── CTE bodies that are no longer restricted to a plain SELECT ────────────────


def test_setop_cte_body(client):
    """A CTE whose body is a set operation (item 2/3): compiled to a segment,
    the final body reads it by name."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS WITH c AS (SELECT a FROM t UNION SELECT a FROM u) SELECT a FROM c", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 0), (2, 20, 0)", schema_name=sn)
        client.execute_sql("INSERT INTO u VALUES (1, 20, 0), (2, 30, 0)", schema_name=sn)
        assert _weights(client, sn, "v", ["a"]) == {(10,): 1, (20,): 1, (30,): 1}
        client.execute_sql("DELETE FROM u WHERE id = 2", schema_name=sn)  # drops a=30
        assert _weights(client, sn, "v", ["a"]) == {(10,): 1, (20,): 1}
    finally:
        client.drop_schema(sn)


def test_grouped_cte_body(client):
    """A CTE whose body is a GROUP BY (item 3), read by the final body."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS WITH c AS (SELECT a, COUNT(*) AS n FROM t GROUP BY a) SELECT a, n FROM c",
            schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 0), (2, 10, 0), (3, 20, 0)", schema_name=sn)
        assert _weights(client, sn, "v", ["a", "n"]) == {(10, 2): 1, (20, 1): 1}
        client.execute_sql("INSERT INTO t VALUES (4, 20, 0)", schema_name=sn)
        assert _weights(client, sn, "v", ["a", "n"]) == {(10, 2): 1, (20, 2): 1}
        client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
        assert _weights(client, sn, "v", ["a", "n"]) == {(10, 1): 1, (20, 2): 1}
    finally:
        client.drop_schema(sn)


def test_cte_in_subquery(client):
    """An IN subquery whose inner FROM is a compiled CTE (item 4): the CTE
    resolves through the binder cache to its segment, no `resolve_inner` change."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS WITH c AS (SELECT a FROM u WHERE b > 0) "
            "SELECT id FROM t WHERE t.a IN (SELECT a FROM c)", schema_name=sn)
        client.execute_sql("INSERT INTO u VALUES (1, 10, 1), (2, 20, 1), (3, 30, 0)", schema_name=sn)  # c = {10,20}
        client.execute_sql("INSERT INTO t VALUES (1, 10, 0), (2, 30, 0), (3, 20, 0)", schema_name=sn)
        # t.a in {10,20}: ids 1 and 3.
        assert _weights(client, sn, "v", ["id"]) == {(1,): 1, (3,): 1}
        client.execute_sql("UPDATE u SET b = 1 WHERE id = 3", schema_name=sn)  # c gains 30
        assert _weights(client, sn, "v", ["id"]) == {(1,): 1, (2,): 1, (3,): 1}
    finally:
        client.drop_schema(sn)


# ── derived-table bodies and operators over them ─────────────────────────────


def test_setop_derived_body(client):
    """A derived table whose body is a set operation (item 2 derived half): the
    non-`Get` linear-source arm cuts it to a segment."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT a FROM (SELECT a FROM t UNION SELECT a FROM u) d", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 0)", schema_name=sn)
        client.execute_sql("INSERT INTO u VALUES (1, 10, 0), (2, 20, 0)", schema_name=sn)
        assert _weights(client, sn, "v", ["a"]) == {(10,): 1, (20,): 1}
        client.execute_sql("DELETE FROM u WHERE id = 2", schema_name=sn)
        assert _weights(client, sn, "v", ["a"]) == {(10,): 1}
    finally:
        client.drop_schema(sn)


def test_grouped_over_derived(client):
    """GROUP BY over a derived table (item 3): reduce cuts the derived source."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT a, COUNT(*) AS n FROM (SELECT a FROM t WHERE b > 0) d GROUP BY a",
            schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 1), (2, 10, 1), (3, 10, 0), (4, 20, 5)", schema_name=sn)
        # b>0 keeps ids 1,2,4 → a=10 twice, a=20 once.
        assert _weights(client, sn, "v", ["a", "n"]) == {(10, 2): 1, (20, 1): 1}
        client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
        assert _weights(client, sn, "v", ["a", "n"]) == {(10, 1): 1, (20, 1): 1}
    finally:
        client.drop_schema(sn)


def test_distinct_over_derived(client):
    """DISTINCT over a derived UNION ALL body (item 3)."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT DISTINCT a FROM (SELECT a FROM t UNION ALL SELECT a FROM u) d",
            schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 0), (2, 20, 0)", schema_name=sn)
        client.execute_sql("INSERT INTO u VALUES (1, 20, 0), (2, 30, 0)", schema_name=sn)
        assert _weights(client, sn, "v", ["a"]) == {(10,): 1, (20,): 1, (30,): 1}
        client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)  # a=20 still in u
        assert _weights(client, sn, "v", ["a"]) == {(10,): 1, (20,): 1, (30,): 1}
    finally:
        client.drop_schema(sn)


def test_nested_derived(client):
    """A derived table nested inside a derived table (item 7)."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT a FROM "
            "(SELECT a FROM (SELECT a, b FROM t WHERE a > 5) inner_d WHERE b < 100) d", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 50), (2, 3, 50), (3, 20, 200)", schema_name=sn)
        # a>5 keeps 1,3; b<100 keeps 1 → a=10.
        assert _weights(client, sn, "v", ["a"]) == {(10,): 1}
        client.execute_sql("INSERT INTO t VALUES (4, 30, 10)", schema_name=sn)
        assert _weights(client, sn, "v", ["a"]) == {(10,): 1, (30,): 1}
    finally:
        client.drop_schema(sn)


def test_linear_over_derived(client):
    """A linear body (WHERE + projection) over a derived source — the non-`Get`
    linear-source arm: the WHERE runs over the materialized derived segment."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT a FROM (SELECT a, b FROM t) d WHERE b > 10", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 5), (2, 20, 50), (3, 30, 20)", schema_name=sn)
        assert _weights(client, sn, "v", ["a"]) == {(20,): 1, (30,): 1}
        client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
        assert _weights(client, sn, "v", ["a"]) == {(30,): 1}
    finally:
        client.drop_schema(sn)


def test_setop_side_is_derived(client):
    """A set operation one of whose sides reads FROM a derived table."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT a FROM (SELECT a, b FROM t WHERE b > 0) d "
            "UNION SELECT a FROM u", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 1), (2, 20, 0)", schema_name=sn)  # b>0 keeps a=10
        client.execute_sql("INSERT INTO u VALUES (1, 30, 0)", schema_name=sn)
        assert _weights(client, sn, "v", ["a"]) == {(10,): 1, (30,): 1}
        client.execute_sql("UPDATE t SET b = 1 WHERE id = 2", schema_name=sn)  # a=20 enters
        assert _weights(client, sn, "v", ["a"]) == {(10,): 1, (20,): 1, (30,): 1}
    finally:
        client.drop_schema(sn)


# ── backfill (data-before-view) order ────────────────────────────────────────


def test_derived_backfill(client):
    """The data-before-view order: a grouped-over-derived view backfills from the
    already-populated base through the cut derived segment."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 1), (2, 10, 1), (3, 20, 1)", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT a, COUNT(*) AS n FROM (SELECT a FROM t WHERE b > 0) d GROUP BY a",
            schema_name=sn)
        assert _weights(client, sn, "v", ["a", "n"]) == {(10, 2): 1, (20, 1): 1}
        client.execute_sql("INSERT INTO t VALUES (4, 20, 1)", schema_name=sn)
        assert _weights(client, sn, "v", ["a", "n"]) == {(10, 2): 1, (20, 2): 1}
    finally:
        client.drop_schema(sn)


# ── the pre-existing zero-payload path (no floor added by as_body pruning) ────


def test_count_star_over_join(client):
    """SELECT COUNT(*) over a join — the reduce cuts the join with an empty live
    set (zero-payload). The pre-existing keep-set floor holds; no floor is added."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT COUNT(*) AS n FROM t JOIN u ON t.a = u.a", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 0), (2, 20, 0)", schema_name=sn)
        client.execute_sql("INSERT INTO u VALUES (1, 10, 0), (2, 10, 0)", schema_name=sn)
        # t.a=10 (id1) joins u rows id1,id2 → 2 matches; t.a=20 joins none.
        assert _weights(client, sn, "v", ["n"]) == {(2,): 1}
        client.execute_sql("DELETE FROM u WHERE id = 2", schema_name=sn)
        assert _weights(client, sn, "v", ["n"]) == {(1,): 1}
    finally:
        client.drop_schema(sn)


def test_count_star_over_derived(client):
    """SELECT COUNT(*) over a derived table — the derived `Project` is cut and the
    `as_body` narrowing reduces it to zero payload (live set empty). Confirms the
    zero-item narrowed segment still counts rows correctly."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT COUNT(*) AS n FROM (SELECT a FROM t WHERE b > 0) d", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 1), (2, 20, 1), (3, 30, 0)", schema_name=sn)
        assert _weights(client, sn, "v", ["n"]) == {(2,): 1}
        client.execute_sql("INSERT INTO t VALUES (4, 40, 1)", schema_name=sn)
        assert _weights(client, sn, "v", ["n"]) == {(3,): 1}
        client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
        assert _weights(client, sn, "v", ["n"]) == {(2,): 1}
    finally:
        client.drop_schema(sn)
