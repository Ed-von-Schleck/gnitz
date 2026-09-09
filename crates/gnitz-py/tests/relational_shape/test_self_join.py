"""The same relation on both sides of a join.

`FROM emp e JOIN emp m ON e.mgr = m.id` would feed one source into both inputs,
and single-source-per-epoch would then drop the bilinear cross-term: a delta
would join only against the trace, never against the other input's own delta.
The planner wraps the *repeated* occurrence in an auto-generated pass-through
hidden view under a fresh id, so the join sees two distinct sources, the shared
base reaches them in two separate epochs, and the cross-term is emitted exactly
once. Three occurrences means two pass-throughs.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/relational_shape/test_self_join.py
"""
import pytest
from _read import bag, scanned


def _emp(client, sn, extra="nm BIGINT NOT NULL"):
    client.execute_sql(
        f"CREATE TABLE emp (id BIGINT PRIMARY KEY, mgr BIGINT NOT NULL, {extra})",
        schema_name=sn)


def test_one_push_reaches_both_occurrences_exactly_once(client, schema_name):
    """The classic employee-to-manager join. One INSERT is one push into `emp`,
    which both occurrences read; each new row must appear on the employee side
    and on the manager side without being counted twice, and retracting a manager
    must retract every report's row. Weights, not row presence: a cross-term
    emitted in both epochs would show up as weight 2."""
    sn = schema_name
    _emp(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT e.nm AS emp, m.nm AS boss FROM emp e JOIN emp m ON e.mgr = m.id",
        schema_name=sn)
    client.execute_sql("INSERT INTO emp VALUES (1, 0, 100), (2, 1, 200), (3, 1, 300)",
                       schema_name=sn)
    # 2 and 3 report to 1; 1's own mgr 0 has no row.
    assert bag(scanned(client, sn, "v"), "emp", "boss") == {(200, 100): 1, (300, 100): 1}

    client.execute_sql("INSERT INTO emp VALUES (4, 2, 400)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "emp", "boss") == \
        {(200, 100): 1, (300, 100): 1, (400, 200): 1}

    client.execute_sql("DELETE FROM emp WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "emp", "boss") == {(400, 200): 1}


def test_the_pass_through_backfills_with_the_join(client, schema_name):
    """Data already in the base when the view is created: the pass-through has to
    be seeded before the join reads it, and the backfilled value must equal what
    incremental maintenance then continues from."""
    sn = schema_name
    _emp(client, sn)
    client.execute_sql("INSERT INTO emp VALUES (1, 0, 100), (2, 1, 200), (3, 1, 300)",
                       schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT e.nm AS emp, m.nm AS boss FROM emp e JOIN emp m ON e.mgr = m.id",
        schema_name=sn)
    assert bag(scanned(client, sn, "v"), "emp", "boss") == {(200, 100): 1, (300, 100): 1}

    client.execute_sql("INSERT INTO emp VALUES (4, 2, 400)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "emp", "boss") == \
        {(200, 100): 1, (300, 100): 1, (400, 200): 1}


def test_a_residual_reads_both_occurrences(client, schema_name):
    """Employees earning more than their manager: the residual compares a column
    of the base against the same column of its pass-through copy, so both have to
    survive into the join output under their own provenance."""
    sn = schema_name
    _emp(client, sn, "sal BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW v AS SELECT e.id AS eid FROM emp e JOIN emp m ON e.mgr = m.id "
        "WHERE e.sal > m.sal", schema_name=sn)
    client.execute_sql("INSERT INTO emp VALUES (1, 0, 500), (2, 1, 600), (3, 1, 400)",
                       schema_name=sn)
    assert bag(scanned(client, sn, "v"), "eid") == {(2,): 1}

    client.execute_sql("UPDATE emp SET sal = 700 WHERE id = 3", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "eid") == {(2,): 1, (3,): 1}


def test_three_occurrences_chain_through_two_pass_throughs(client, schema_name):
    """Employee, manager and grand-manager: the same base three times, so the
    left-deep chain stacks two pass-throughs, and the narrow projection prunes
    each occurrence to its join keys and the one name it contributes."""
    sn = schema_name
    _emp(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT e.nm AS emp, g.nm AS grand "
        "FROM emp e JOIN emp m ON e.mgr = m.id JOIN emp g ON m.mgr = g.id", schema_name=sn)
    # 1 is top (mgr 0); 2 reports to 1; 3 to 2. Only e=3 has a full chain.
    client.execute_sql("INSERT INTO emp VALUES (1, 0, 100), (2, 1, 200), (3, 2, 300)",
                       schema_name=sn)
    assert bag(scanned(client, sn, "v"), "emp", "grand") == {(300, 100): 1}

    client.execute_sql("INSERT INTO emp VALUES (4, 3, 400)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "emp", "grand") == {(300, 100): 1, (400, 200): 1}

    client.execute_sql("DELETE FROM emp WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "emp", "grand") == {(400, 200): 1}


def test_a_self_join_composes_with_a_distinct_third_relation(client, schema_name):
    """A repeated occurrence and an ordinary one in the same chain: only the
    repeat is wrapped, and the unwrapped relation joins as it always would."""
    sn = schema_name
    _emp(client, sn, "dept BIGINT NOT NULL")
    client.execute_sql("CREATE TABLE dept (id BIGINT PRIMARY KEY, budget BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT e.id AS eid, m.id AS mid, d.budget AS bud "
        "FROM emp e JOIN emp m ON e.mgr = m.id JOIN dept d ON e.dept = d.id", schema_name=sn)
    client.execute_sql("INSERT INTO dept VALUES (10, 999)", schema_name=sn)
    client.execute_sql("INSERT INTO emp VALUES (1, 0, 10), (2, 1, 10)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "eid", "mid", "bud") == {(2, 1, 999): 1}


def test_a_self_join_spelled_with_using(client, schema_name):
    """`USING (k)` resolves its names before the right alias enters the scope,
    and the wrapper still has to see two distinct sources afterwards — the one
    order in which the merge and the self-collision rewrite could disagree."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT x.id AS xid, y.id AS yid FROM t x JOIN t y USING (k)",
        schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 5), (3, 6)", schema_name=sn)

    rows = [(1, 5), (2, 5), (3, 6)]
    assert bag(scanned(client, sn, "v"), "xid", "yid") == \
        {(xi, yi): 1 for (xi, xk) in rows for (yi, yk) in rows if xk == yk}


def test_the_pass_through_is_a_real_relation_in_the_dependency_graph(client, schema_name):
    """Both occurrences depend on the base, so it cannot be dropped under a live
    self-join; DROP VIEW retires the generated pass-through, and an orphaned one
    would keep RESTRICTing the base afterwards."""
    sn = schema_name
    _emp(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT e.nm AS emp, m.nm AS boss FROM emp e JOIN emp m ON e.mgr = m.id",
        schema_name=sn)
    with pytest.raises(Exception):
        client.execute_sql("DROP TABLE emp", schema_name=sn)

    client.execute_sql("DROP VIEW v", schema_name=sn)
    client.execute_sql("DROP TABLE emp", schema_name=sn)
