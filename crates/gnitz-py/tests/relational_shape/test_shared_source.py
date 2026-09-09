"""Two operands of one combine node that resolve to the same source.

One rule, in three shapes. The planner wraps the *repeated* occurrence in an
auto-generated pass-through hidden view under a fresh id, so the combine sees two
distinct sources and the shared base reaches them in two separate epochs. The
discriminator is source-id equality, not base-table overlap — two *distinct*
views over one base must not be wrapped, and must not be refused either.

A join whose inputs both trace to one relation (`t ⋈ view-over-t`, or the same
table twice in one FROM) is reached by one push in two separate epochs, so the
bilinear cross-term `dA ⋈ dB` is emitted exactly once. That is a claim about
weights: the failure is a doubled weight or a missing product row, and a row-set
comparison sees neither. Three occurrences means two pass-throughs.

A set operation whose branches resolve to the same tid cannot be driven by the
clamp algebra in one epoch, so the repeat is wrapped and one push becomes two
cascade epochs.

Every case compares against `_oracle`'s from-scratch recompute over base state
the test maintains itself, after each epoch (one `execute_sql` is one epoch), or
through `bag`, which sums the weights the same way.
"""

import _oracle as oracle
from _read import bag, scanned


def _t(client, sn, cols="k BIGINT NOT NULL, v BIGINT NOT NULL"):
    client.execute_sql(
        f"CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, {cols})", schema_name=sn)


def _vid(client, sn, name):
    return client.resolve_table(sn, name)[0]


def _c(state, pred):
    """The weight-multiset of `c` over the rows of `state` satisfying `pred`."""
    return oracle.oracle_filter_project(state, pred, ["c"])


# The four operators whose two branches may resolve to one source, as
# `(view-name prefix, SQL)`. INTERSECT ALL / EXCEPT ALL add no shape: their
# branches route through the same wrapper as the deduplicating pair.
_ONE_SOURCE_OPS = [("ex", "EXCEPT"), ("inr", "INTERSECT"),
                   ("ua", "UNION ALL"), ("ud", "UNION")]

_JOIN_COLS = ["lid", "lk", "lv", "rid", "rk", "rv"]


def _join_of(left, right):
    """The `t ⋈ vt` product over two pk -> row maps, keyed on `k`."""
    return oracle.oracle_equijoin(
        left=left, lwhere=None, lkey="k", lproj=["id", "k", "v"],
        right=right, rwhere=None, rkey="k", rproj=["id", "k", "v"],
        out_cols=_JOIN_COLS)


def test_a_join_against_a_view_of_its_own_source_emits_the_cross_term_once(client, schema_name):
    """`t JOIN vt` where `vt` filters `t`: the join reads two dependency edges
    onto one relation, so the scheduler evaluates it once per edge. One INSERT
    that creates matches on *both* sides at once is the decisive case — three
    rows sharing a key must yield the full 3x3 product, which a two-term drop of
    the cross-term would undercount."""
    sn = schema_name
    _t(client, sn)
    client.execute_sql("CREATE VIEW vt AS SELECT id, k, v FROM t WHERE v > 0", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW j AS "
        "SELECT t.id AS lid, t.k AS lk, t.v AS lv, vt.id AS rid, vt.k AS rk, vt.v AS rv "
        "FROM t JOIN vt ON t.k = vt.k", schema_name=sn)
    vid = _vid(client, sn, "j")
    state = {}

    def check(ctx):
        expected = _join_of(state, {pk: r for pk, r in state.items() if r["v"] > 0})
        oracle.assert_view_matches(client, vid, _JOIN_COLS, expected, ctx=ctx)
        return expected

    client.execute_sql("INSERT INTO t VALUES (1, 5, 10), (2, 5, 20), (3, 5, 30)", schema_name=sn)
    oracle.apply_insert(state, "id", [
        {"id": 1, "k": 5, "v": 10}, {"id": 2, "k": 5, "v": 20}, {"id": 3, "k": 5, "v": 30}])
    expected = check("one-epoch-matches-on-both-sides")
    assert sum(expected.values()) == 9, "the oracle itself must be the full 3x3 product"

    # id 2 stays in t (left) but leaves vt (right).
    client.execute_sql("UPDATE t SET v = -1 WHERE id = 2", schema_name=sn)
    oracle.apply_update(state, "id", 2, {"v": -1})
    check("after-update-out-of-the-filter")

    client.execute_sql("INSERT INTO t VALUES (4, 5, 11), (5, 6, 7)", schema_name=sn)
    oracle.apply_insert(state, "id", [{"id": 4, "k": 5, "v": 11}, {"id": 5, "k": 6, "v": 7}])
    check("after-second-insert")

    client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
    oracle.apply_delete(state, "id", [1])
    check("after-delete-of-a-product-participant")


def test_two_views_over_one_base_join_at_full_multiplicity(client, schema_name):
    """`vt1 JOIN vt2`, both views over `t`: distinct source ids, so two edges
    again, and the product must carry every pair including the rows both
    predicates admit."""
    sn = schema_name
    _t(client, sn)
    client.execute_sql("CREATE VIEW vt1 AS SELECT id, k, v FROM t WHERE v > 0", schema_name=sn)
    client.execute_sql("CREATE VIEW vt2 AS SELECT id, k, v FROM t WHERE v > 50", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW j AS "
        "SELECT vt1.id AS lid, vt1.k AS lk, vt1.v AS lv, vt2.id AS rid, vt2.k AS rk, vt2.v AS rv "
        "FROM vt1 JOIN vt2 ON vt1.k = vt2.k", schema_name=sn)
    vid = _vid(client, sn, "j")
    state = {}

    def check(ctx):
        oracle.assert_view_matches(
            client, vid, _JOIN_COLS,
            _join_of({pk: r for pk, r in state.items() if r["v"] > 0},
                     {pk: r for pk, r in state.items() if r["v"] > 50}), ctx=ctx)

    client.execute_sql("INSERT INTO t VALUES (1, 3, 10), (2, 3, 60), (3, 3, 99)", schema_name=sn)
    oracle.apply_insert(state, "id", [
        {"id": 1, "k": 3, "v": 10}, {"id": 2, "k": 3, "v": 60}, {"id": 3, "k": 3, "v": 99}])
    check("after-insert")

    client.execute_sql("UPDATE t SET v = 70 WHERE id = 1", schema_name=sn)
    oracle.apply_update(state, "id", 1, {"v": 70})
    check("after-update-into-the-right-branch")

    client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
    oracle.apply_delete(state, "id", [2])
    check("after-delete")


def test_a_set_op_whose_branches_resolve_to_one_source_converges(client, schema_name):
    """Identical branches, differing branches, and branches over one view: every
    shape whose two operands resolve to one tid compiles and converges, for every
    operator. The clamping pair needs the wrapper — `A EXCEPT A` is empty and
    `A INTERSECT A` is `distinct(A)` at every stage, one push driving two cascade
    epochs rather than one epoch the clamp algebra cannot settle. The linear pair
    needs no correction term at all: a value both branches admit reaches UNION ALL
    at weight 2 and UNION at weight 1."""
    sn = schema_name
    _t(client, sn, cols="c BIGINT NOT NULL")
    client.execute_sql("CREATE VIEW vt AS SELECT id, c FROM t WHERE c > 0", schema_name=sn)
    for name, op in _ONE_SOURCE_OPS:
        client.execute_sql(
            f"CREATE VIEW {name}_split AS SELECT c FROM t WHERE c > 0 {op} SELECT c FROM t WHERE c < 10",
            schema_name=sn)
        client.execute_sql(
            f"CREATE VIEW {name}_same AS SELECT c FROM t {op} SELECT c FROM t", schema_name=sn)
        client.execute_sql(
            f"CREATE VIEW {name}_view AS SELECT c FROM vt {op} SELECT c FROM vt", schema_name=sn)
    state = {}

    def check(ctx):
        whole = _c(state, None)
        positive = _c(state, lambda r: r["c"] > 0)
        branches = {"split": (positive, _c(state, lambda r: r["c"] < 10)),
                    "same": (whole, whole),
                    "view": (positive, positive)}
        for prefix, op in _ONE_SOURCE_OPS:
            for suffix, (left, right) in branches.items():
                oracle.assert_view_matches(
                    client, _vid(client, sn, f"{prefix}_{suffix}"), ["c"],
                    oracle.oracle_setop(op, left, right), ctx=f"{prefix}_{suffix} {ctx}")

    client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 15), (3, 8)", schema_name=sn)
    oracle.apply_insert(state, "id", [{"id": 1, "c": 5}, {"id": 2, "c": 15}, {"id": 3, "c": 8}])
    check("after-insert")

    client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
    oracle.apply_delete(state, "id", [2])
    check("after-delete")


def test_a_wrapped_set_op_seeds_its_backfill(client, schema_name):
    """The same shapes over data that already exists: the wrapper segment has to
    seed the distributed backfill, else the other branch reads a still-empty
    sibling and the view silently loses every pre-existing row."""
    sn = schema_name
    _t(client, sn, cols="c BIGINT NOT NULL")
    client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 6)", schema_name=sn)
    client.execute_sql("CREATE VIEW ex AS SELECT c FROM t EXCEPT SELECT c FROM t", schema_name=sn)
    client.execute_sql("CREATE VIEW inr AS SELECT c FROM t INTERSECT SELECT c FROM t", schema_name=sn)

    assert bag(scanned(client, sn, "ex"), "c") == {}
    assert bag(scanned(client, sn, "inr"), "c") == {(5,): 1, (6,): 1}


def test_two_views_over_one_base_are_distinct_sources(client, schema_name):
    """The non-over-rejection case: `vt1` and `vt2` read the same base table but
    are distinct sources, so neither the wrapper nor a refusal applies and the
    ordinary clamp algebra must settle in one epoch."""
    sn = schema_name
    _t(client, sn, cols="c BIGINT NOT NULL")
    client.execute_sql("CREATE VIEW vt1 AS SELECT id, c FROM t WHERE c > 0", schema_name=sn)
    client.execute_sql("CREATE VIEW vt2 AS SELECT id, c FROM t WHERE c > 50", schema_name=sn)
    client.execute_sql("CREATE VIEW ex AS SELECT c FROM vt1 EXCEPT SELECT c FROM vt2", schema_name=sn)
    client.execute_sql("CREATE VIEW inr AS SELECT c FROM vt1 INTERSECT SELECT c FROM vt2", schema_name=sn)
    state = {}

    def check(ctx):
        left, right = _c(state, lambda r: r["c"] > 0), _c(state, lambda r: r["c"] > 50)
        for name, op in (("ex", "EXCEPT"), ("inr", "INTERSECT")):
            oracle.assert_view_matches(client, _vid(client, sn, name), ["c"],
                                       oracle.oracle_setop(op, left, right), ctx=f"{name} {ctx}")

    client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 60), (3, 99)", schema_name=sn)
    oracle.apply_insert(state, "id", [{"id": 1, "c": 10}, {"id": 2, "c": 60}, {"id": 3, "c": 99}])
    check("after-insert")

    client.execute_sql("UPDATE t SET c = 70 WHERE id = 1", schema_name=sn)
    oracle.apply_update(state, "id", 1, {"c": 70})
    check("after-update-across-the-right-predicate")

    client.execute_sql("DELETE FROM t WHERE id = 3", schema_name=sn)
    oracle.apply_delete(state, "id", [3])
    check("after-delete")


# ── the same relation twice in one FROM ───────────────────────────────────────


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
