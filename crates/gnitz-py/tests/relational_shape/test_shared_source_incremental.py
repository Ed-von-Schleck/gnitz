"""Two operands of one combine node that resolve to the same source.

Two lowering rules meet here.

A join whose inputs both trace to one relation (`t ⋈ view-over-t`) is reached by
one push in two separate epochs, so the bilinear cross-term `dA ⋈ dB` is emitted
exactly once. That is a claim about weights: the failure is a doubled weight or a
missing product row, and a row-set comparison sees neither.

A set operation whose branches resolve to the same tid cannot be driven by the
clamp algebra in one epoch, so the repeat is wrapped in a pass-through segment
and one push becomes two cascade epochs. The discriminator is source-id
equality, not base-table overlap — two *distinct* views over one base must not be
wrapped, and must not be refused either.

Every case compares against `_oracle`'s from-scratch recompute over base state
the test maintains itself, after each epoch (one `execute_sql` is one epoch).
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


def test_a_set_op_whose_branches_resolve_to_one_source_is_wrapped(client, schema_name):
    """Identical branches, differing branches, and branches over one view: every
    shape whose two operands resolve to one tid compiles and converges. `A EXCEPT
    A` is empty and `A INTERSECT A` is `distinct(A)` at every stage — one push
    driving two cascade epochs, not one epoch the clamp algebra cannot settle."""
    sn = schema_name
    _t(client, sn, cols="c BIGINT NOT NULL")
    client.execute_sql("CREATE VIEW vt AS SELECT id, c FROM t WHERE c > 0", schema_name=sn)
    for name, op in (("ex", "EXCEPT"), ("inr", "INTERSECT")):
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
        for prefix, op in (("ex", "EXCEPT"), ("inr", "INTERSECT")):
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


def test_a_union_over_one_source_needs_no_wrapper(client, schema_name):
    """UNION and UNION ALL are linear, so overlapping branches over one table
    carry no correction term: a value both predicates admit reaches UNION ALL at
    weight 2 and UNION at weight 1."""
    sn = schema_name
    _t(client, sn, cols="c BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW ua AS SELECT c FROM t WHERE c > 0 UNION ALL SELECT c FROM t WHERE c < 100",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW ud AS SELECT c FROM t WHERE c > 0 UNION SELECT c FROM t WHERE c < 100",
        schema_name=sn)
    state = {}

    def check(ctx):
        left, right = _c(state, lambda r: r["c"] > 0), _c(state, lambda r: r["c"] < 100)
        for name, op in (("ua", "UNION ALL"), ("ud", "UNION")):
            oracle.assert_view_matches(client, _vid(client, sn, name), ["c"],
                                       oracle.oracle_setop(op, left, right), ctx=f"{name} {ctx}")

    # c=50 satisfies both predicates; c=150 and c=-5 only one each.
    client.execute_sql("INSERT INTO t VALUES (1, 50), (2, 150), (3, -5)", schema_name=sn)
    oracle.apply_insert(state, "id", [{"id": 1, "c": 50}, {"id": 2, "c": 150}, {"id": 3, "c": -5}])
    check("after-insert")

    client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
    oracle.apply_delete(state, "id", [1])
    check("after-delete-of-the-doubled-value")


def test_a_branch_with_duplicate_projected_values_contributes_weight_one(client, schema_name):
    """Two source rows projecting to one value consolidate to a single tuple at
    weight 2, so a deduplicating set operation must lift each branch through
    distinct before subtracting. Without it a right-side cover retracts weight 1
    and leaves the value surviving at weight 1 instead of nothing."""
    sn = schema_name
    for name in ("a", "b"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)",
            schema_name=sn)
    client.execute_sql("CREATE VIEW ex AS SELECT c FROM a EXCEPT SELECT c FROM b", schema_name=sn)
    client.execute_sql("CREATE VIEW inr AS SELECT c FROM a INTERSECT SELECT c FROM b", schema_name=sn)
    a_state, b_state = {}, {}

    def check(ctx):
        left, right = _c(a_state, None), _c(b_state, None)
        for name, op in (("ex", "EXCEPT"), ("inr", "INTERSECT")):
            oracle.assert_view_matches(client, _vid(client, sn, name), ["c"],
                                       oracle.oracle_setop(op, left, right), ctx=f"{name} {ctx}")

    client.execute_sql("INSERT INTO a VALUES (1, 5), (2, 5)", schema_name=sn)
    oracle.apply_insert(a_state, "id", [{"id": 1, "c": 5}, {"id": 2, "c": 5}])
    check("left-duplicate-right-empty")

    client.execute_sql("INSERT INTO b VALUES (10, 5), (11, 5)", schema_name=sn)
    oracle.apply_insert(b_state, "id", [{"id": 10, "c": 5}, {"id": 11, "c": 5}])
    check("both-sides-duplicate")

    client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=sn)
    oracle.apply_delete(a_state, "id", [1])
    check("one-of-the-two-left-rows-removed")

    client.execute_sql("DELETE FROM b WHERE id = 10", schema_name=sn)
    oracle.apply_delete(b_state, "id", [10])
    check("one-of-the-two-right-rows-removed")

    client.execute_sql("DELETE FROM b WHERE id = 11", schema_name=sn)
    oracle.apply_delete(b_state, "id", [11])
    check("right-empty")
