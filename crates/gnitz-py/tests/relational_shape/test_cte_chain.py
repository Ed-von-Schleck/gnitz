"""The lowering's cut rule: a nested sub-body becomes a hidden segment that the
final view reads by name.

A CTE, a derived table and an inline set-op or join side are three spellings of
one bound tree, and the lowering — not the spelling — decides what is cut. So
the facts here are: a cut body answers exactly as the uncut one, any body may be
cut, any operator may read a cut segment, a chain of cuts backfills in
dependency order, and a name crossing a cut resolves against what the segment
exposes.

Weights throughout: a segment feeding two consumers, or a chain seeding its
backfill twice, shows up as a doubled weight and not as an extra row.
"""

import pytest
from _read import bag, scanned


def _tu(client, sn):
    """The two sources every body below is cut out of."""
    for name in ("t", "u"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
            schema_name=sn)


def test_a_cut_body_answers_the_same_as_the_uncut_one(client, schema_name):
    """The filter feeding a join, spelled as a CTE, a derived table, a
    pass-through CTE with the predicate in the final, and one inline body. The
    first two cut a segment and the last two do not; all four carry the same
    weights through every write."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql(
        "CREATE VIEW v_cte AS WITH d AS (SELECT id, a FROM t WHERE a > 100) "
        "SELECT d.id AS did, u.b AS ub FROM d JOIN u ON d.id = u.id", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_derived AS SELECT d.id AS did, u.b AS ub "
        "FROM (SELECT id, a FROM t WHERE a > 100) d JOIN u ON d.id = u.id", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_passthrough AS WITH d AS (SELECT * FROM t) "
        "SELECT d.id AS did, u.b AS ub FROM d JOIN u ON d.id = u.id WHERE d.a > 100", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_inline AS SELECT t.id AS did, u.b AS ub "
        "FROM t JOIN u ON t.id = u.id WHERE t.a > 100", schema_name=sn)
    names = ("v_cte", "v_derived", "v_passthrough", "v_inline")

    def agree(want):
        for v in names:
            assert bag(scanned(client, sn, v), "did", "ub") == want, v

    client.execute_sql("INSERT INTO u VALUES (1, 0, 10), (2, 0, 20), (3, 0, 30)", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 200, 0), (2, 50, 0)", schema_name=sn)
    agree({(1, 10): 1})
    client.execute_sql("UPDATE t SET a = 300 WHERE id = 2", schema_name=sn)
    agree({(1, 10): 1, (2, 20): 1})
    client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
    agree({(2, 20): 1})
    client.execute_sql("INSERT INTO t VALUES (3, 150, 0)", schema_name=sn)
    agree({(2, 20): 1, (3, 30): 1})


def test_any_body_may_be_cut_to_a_segment(client, schema_name):
    """A set operation, a GROUP BY, a join, a DISTINCT and a derived table nested
    in a derived table each ride behind the cut, read by a trivial final."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql(
        "CREATE VIEW v_setop AS WITH c AS (SELECT a FROM t UNION SELECT a FROM u) "
        "SELECT a FROM c", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_grouped AS WITH c AS (SELECT a, COUNT(*) AS n FROM t GROUP BY a) "
        "SELECT a, n FROM c", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_join AS WITH c AS (SELECT t.a AS ta, u.b AS ub FROM t JOIN u ON t.b = u.id) "
        "SELECT ta, ub FROM c", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_distinct AS SELECT a FROM (SELECT DISTINCT a FROM t) d", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_nested AS SELECT a FROM "
        "(SELECT a FROM (SELECT a, b FROM t WHERE a > 5) inner_d WHERE b < 100) d", schema_name=sn)

    client.execute_sql("INSERT INTO t VALUES (1, 10, 7), (2, 10, 7), (3, 20, 200)", schema_name=sn)
    client.execute_sql("INSERT INTO u VALUES (7, 30, 0), (8, 40, 0)", schema_name=sn)
    assert bag(scanned(client, sn, "v_setop"), "a") == {(10,): 1, (20,): 1, (30,): 1, (40,): 1}
    assert bag(scanned(client, sn, "v_grouped"), "a", "n") == {(10, 2): 1, (20, 1): 1}
    # Both t rows with b=7 match u.id=7, so the joined pair carries weight 2.
    assert bag(scanned(client, sn, "v_join"), "ta", "ub") == {(10, 0): 2}
    assert bag(scanned(client, sn, "v_distinct"), "a") == {(10,): 1, (20,): 1}
    assert bag(scanned(client, sn, "v_nested"), "a") == {(10,): 2}

    client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
    assert bag(scanned(client, sn, "v_setop"), "a") == {(10,): 1, (20,): 1, (30,): 1, (40,): 1}
    assert bag(scanned(client, sn, "v_grouped"), "a", "n") == {(10, 1): 1, (20, 1): 1}
    assert bag(scanned(client, sn, "v_join"), "ta", "ub") == {(10, 0): 1}
    assert bag(scanned(client, sn, "v_distinct"), "a") == {(10,): 1, (20,): 1}
    assert bag(scanned(client, sn, "v_nested"), "a") == {(10,): 1}


def test_any_operator_may_read_a_cut_segment(client, schema_name):
    """A cut segment stands wherever a base relation does: under a WHERE, a
    GROUP BY, a DISTINCT, one side of a set operation, an IN subquery, both
    sides of a join, and a COUNT(*) whose keep set narrows the segment to zero
    payload."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql(
        "CREATE VIEW c_where AS SELECT a FROM (SELECT a, b FROM t) d WHERE b > 10", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW c_grouped AS SELECT a, COUNT(*) AS n "
        "FROM (SELECT a FROM t WHERE b > 0) d GROUP BY a", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW c_distinct AS SELECT DISTINCT a "
        "FROM (SELECT a FROM t UNION ALL SELECT a FROM u) d", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW c_setop AS SELECT a FROM (SELECT a, b FROM t WHERE b > 0) d "
        "UNION SELECT a FROM u", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW c_subquery AS WITH c AS (SELECT a FROM u WHERE b > 0) "
        "SELECT id FROM t WHERE t.a IN (SELECT a FROM c)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW c_twojoin AS SELECT x.a AS xa, y.b AS yb "
        "FROM (SELECT id, a FROM t WHERE a > 10) x JOIN (SELECT id, b FROM u WHERE b < 100) y "
        "ON x.id = y.id", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW c_count AS SELECT COUNT(*) AS n FROM (SELECT a FROM t WHERE b > 0) d",
        schema_name=sn)

    client.execute_sql("INSERT INTO t VALUES (1, 20, 50), (2, 20, 5), (3, 30, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO u VALUES (1, 20, 60), (2, 90, 0)", schema_name=sn)
    assert bag(scanned(client, sn, "c_where"), "a") == {(20,): 1}
    assert bag(scanned(client, sn, "c_grouped"), "a", "n") == {(20, 2): 1}
    assert bag(scanned(client, sn, "c_distinct"), "a") == {(20,): 1, (30,): 1, (90,): 1}
    assert bag(scanned(client, sn, "c_setop"), "a") == {(20,): 1, (90,): 1}
    assert bag(scanned(client, sn, "c_subquery"), "id") == {(1,): 1, (2,): 1}
    assert bag(scanned(client, sn, "c_twojoin"), "xa", "yb") == {(20, 60): 1, (20, 0): 1}
    assert bag(scanned(client, sn, "c_count"), "n") == {(2,): 1}

    client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "c_where"), "a") == {}
    assert bag(scanned(client, sn, "c_grouped"), "a", "n") == {(20, 1): 1}
    assert bag(scanned(client, sn, "c_distinct"), "a") == {(20,): 1, (30,): 1, (90,): 1}
    assert bag(scanned(client, sn, "c_setop"), "a") == {(20,): 1, (90,): 1}
    assert bag(scanned(client, sn, "c_subquery"), "id") == {(2,): 1}
    assert bag(scanned(client, sn, "c_twojoin"), "xa", "yb") == {(20, 0): 1}
    assert bag(scanned(client, sn, "c_count"), "n") == {(1,): 1}


def test_a_nested_set_op_is_cut_and_rehashed_by_the_outer(client, schema_name):
    """An inner set operation is a segment whose synthetic key the outer must
    re-hash on its own identity columns, in a chain and across quantifiers."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql(
        "CREATE VIEW v_chain AS SELECT a FROM t UNION SELECT a FROM u UNION SELECT b FROM t",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_mixed AS (SELECT a FROM t UNION ALL SELECT a FROM u) "
        "INTERSECT SELECT b FROM t", schema_name=sn)

    client.execute_sql("INSERT INTO t VALUES (1, 7, 7), (2, 3, 9)", schema_name=sn)
    client.execute_sql("INSERT INTO u VALUES (1, 9, 0)", schema_name=sn)
    assert bag(scanned(client, sn, "v_chain"), "a") == {(3,): 1, (7,): 1, (9,): 1}
    assert bag(scanned(client, sn, "v_mixed"), "a") == {(7,): 1, (9,): 1}

    client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
    assert bag(scanned(client, sn, "v_chain"), "a") == {(7,): 1, (9,): 1}
    assert bag(scanned(client, sn, "v_mixed"), "a") == {(7,): 1}


def test_a_chain_of_cuts_backfills_in_dependency_order(client, schema_name):
    """Views created over data that already exists: each segment must seed before
    its consumer, or the consumer reads a still-empty sibling and silently loses
    every pre-existing row. Still incremental afterwards."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql("INSERT INTO t VALUES (1, 10, 2), (2, 10, 3), (3, 20, 5)", schema_name=sn)
    client.execute_sql("INSERT INTO u VALUES (7, 99, 0)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_grouped AS SELECT a, COUNT(*) AS n "
        "FROM (SELECT a FROM t WHERE b > 0) d GROUP BY a", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_chain AS SELECT a FROM t UNION SELECT a FROM u UNION SELECT b FROM t",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_grouped_side AS SELECT a AS k, SUM(b) AS s FROM t GROUP BY a "
        "UNION ALL SELECT id, a FROM u", schema_name=sn)

    assert bag(scanned(client, sn, "v_grouped"), "a", "n") == {(10, 2): 1, (20, 1): 1}
    assert bag(scanned(client, sn, "v_chain"), "a") == {
        (2,): 1, (3,): 1, (5,): 1, (10,): 1, (20,): 1, (99,): 1}
    assert bag(scanned(client, sn, "v_grouped_side"), "k", "s") == {
        (7, 99): 1, (10, 5): 1, (20, 5): 1}

    client.execute_sql("INSERT INTO t VALUES (4, 20, 1)", schema_name=sn)
    assert bag(scanned(client, sn, "v_grouped"), "a", "n") == {(10, 2): 1, (20, 2): 1}
    assert bag(scanned(client, sn, "v_chain"), "a") == {
        (1,): 1, (2,): 1, (3,): 1, (5,): 1, (10,): 1, (20,): 1, (99,): 1}
    assert bag(scanned(client, sn, "v_grouped_side"), "k", "s") == {
        (7, 99): 1, (10, 5): 1, (20, 6): 1}


def test_a_combine_leafs_identity_may_be_a_computed_column(client, schema_name):
    """DISTINCT and a set-op branch hash the identity they are given, so it may
    be an expression the leaf materializes rather than a source column."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql("CREATE VIEW v_dist AS SELECT DISTINCT a + 1 AS a1, b FROM t", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_exc AS SELECT a * 2 AS x FROM t EXCEPT SELECT b FROM u", schema_name=sn)

    client.execute_sql("INSERT INTO t VALUES (1, 1, 2), (2, 1, 2), (3, 3, 2)", schema_name=sn)
    client.execute_sql("INSERT INTO u VALUES (1, 0, 6)", schema_name=sn)
    assert bag(scanned(client, sn, "v_dist"), "a1", "b") == {(2, 2): 1, (4, 2): 1}
    assert bag(scanned(client, sn, "v_exc"), "x") == {(2,): 1}

    # One of the two rows behind (2, 2) goes; the computed identity survives.
    client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v_dist"), "a1", "b") == {(2, 2): 1, (4, 2): 1}
    client.execute_sql("DELETE FROM u WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v_exc"), "x") == {(2,): 1, (6,): 1}


def test_a_segments_column_aliases_survive_the_steps_above_it(client, schema_name):
    """`WITH d(k, amt)` and `(SELECT ...) AS x(k, val)` rename what the segment
    exposes, and the aliases live only on the columns the FROM binder hands the
    scope — the bound subtree keeps its inner names. A join step that rebuilt its
    scope from the tree would resolve `x.id` and reject `x.k`, as an ON key, as a
    projection, and through an outer join's null-widening."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql(
        "CREATE VIEW v_cte AS WITH d(k, amt) AS (SELECT id, a FROM t WHERE a > 100) "
        "SELECT d.k AS kk, d.amt AS aa FROM d JOIN u ON d.k = u.id", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_join AS SELECT x.k AS xk, x.val AS xv, u.b AS ub "
        "FROM (SELECT id, a FROM t) AS x(k, val) JOIN u ON x.k = u.id", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_left AS SELECT x.k AS xk, u.b AS ub "
        "FROM (SELECT id, a FROM t) AS x(k, val) LEFT JOIN u ON x.k = u.id", schema_name=sn)

    client.execute_sql("INSERT INTO t VALUES (1, 200, 0), (2, 50, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO u VALUES (1, 0, 7)", schema_name=sn)
    assert bag(scanned(client, sn, "v_cte"), "kk", "aa") == {(1, 200): 1}
    assert bag(scanned(client, sn, "v_join"), "xk", "xv", "ub") == {(1, 200, 7): 1}
    assert bag(scanned(client, sn, "v_left"), "xk", "ub") == {(1, 7): 1, (2, None): 1}


def test_a_sibling_derived_table_is_not_in_scope(client, schema_name):
    """A non-LATERAL derived table is not correlated: a same-named reference in a
    later sibling binds the catalog relation, not the sibling. If it bound the
    sibling, `b.v` would not resolve at all."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, dummy BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT b.v AS bv "
        "FROM (SELECT id FROM t) a JOIN (SELECT id, v FROM a) b ON a.id = b.id", schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 100), (2, 200)", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 0), (2, 0)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "bv") == {(100,): 1, (200,): 1}


def test_an_outer_join_inside_a_cut_segment_null_fills(client, schema_name):
    """A join in a CTE body is cut, so its null-fill is emitted by the segment
    rather than by the final view. A band join and a pure-range one, the latter
    with several right rows per left row so the matched multiplicity is carried
    rather than collapsed to a witness."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql(
        "CREATE VIEW v_band AS WITH d AS ("
        "SELECT t.id AS did, u.b AS ub FROM t LEFT JOIN u ON t.id = u.id AND t.a < u.b"
        ") SELECT did, ub FROM d", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_range AS WITH d AS ("
        "SELECT t.id AS did, u.b AS ub FROM t LEFT JOIN u ON t.a < u.b"
        ") SELECT did, ub FROM d", schema_name=sn)

    client.execute_sql("INSERT INTO u VALUES (1, 0, 100), (2, 0, 50)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 30, 0), (2, 80, 0), (3, 40, 0), (4, 500, 0)", schema_name=sn)
    assert bag(scanned(client, sn, "v_band"), "did", "ub") == {
        (1, 100): 1, (2, None): 1, (3, None): 1, (4, None): 1}
    assert bag(scanned(client, sn, "v_range"), "did", "ub") == {
        (1, 100): 1, (1, 50): 1, (2, 100): 1, (3, 100): 1, (3, 50): 1, (4, None): 1}

    # Retracting a matched row removes it and emits no stray null-fill.
    client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v_band"), "did", "ub") == {
        (2, None): 1, (3, None): 1, (4, None): 1}
    assert bag(scanned(client, sn, "v_range"), "did", "ub") == {
        (2, 100): 1, (3, 100): 1, (3, 50): 1, (4, None): 1}


@pytest.mark.parametrize("body,base", [
    ("SELECT x.a AS xa, y.b AS yb FROM t x JOIN t y ON x.b = y.id", "t"),
    ("SELECT t.a AS ta, w.b AS wb FROM t JOIN u ON t.b = u.id JOIN w ON u.b = w.id", "u"),
    ("WITH agg AS (SELECT a, SUM(b) AS s FROM t GROUP BY a) "
     "SELECT u.b AS ub, agg.s AS s FROM agg JOIN u ON agg.a = u.id", "t"),
], ids=["self-join-passthrough", "join-segment", "reduce-segment"])
def test_a_generated_relation_is_a_real_node_in_the_dependency_graph(
        client, schema_name, body, base):
    """Whatever the lowering generates — a collision pass-through, a join segment,
    a reduce segment — is a relation in the dependency graph like any other: it
    holds a reference on its source, which is why the base refuses to drop under
    a live view; and `DROP VIEW` retires the whole bundle, so the base is free
    straight after. An orphaned generated relation would keep RESTRICTing it.

    `base` is reached only through the generated relation in each case, so the
    refusal is the segment's and not the final view's.
    """
    sn = schema_name
    _tu(client, sn)
    client.execute_sql(
        "CREATE TABLE w (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(f"CREATE VIEW v AS {body}", schema_name=sn)

    with pytest.raises(Exception, match="dependency"):
        client.execute_sql(f"DROP TABLE {base}", schema_name=sn)

    client.execute_sql("DROP VIEW v", schema_name=sn)
    client.execute_sql(f"DROP TABLE {base}", schema_name=sn)


def test_a_derived_table_without_an_alias_is_refused(client, schema_name):
    """Nothing can name a segment that has no alias, so the cut is refused rather
    than compiled to an unreachable relation."""
    sn = schema_name
    _tu(client, sn)
    with pytest.raises(Exception) as ei:
        client.execute_sql(
            "CREATE VIEW v AS SELECT id FROM (SELECT id, a FROM t WHERE a > 10)", schema_name=sn)
    assert "alias" in str(ei.value).lower(), str(ei.value)
