"""The lowering's cut rule: a nested sub-body becomes a hidden segment that the
final view reads by name.

A CTE, a derived table and an inline set-op or join side are three spellings of
one bound tree, and the lowering — not the spelling — decides what is cut. So
the facts here are: a cut body answers exactly as the uncut one, any body may be
cut, any operator may read a cut segment, and a name crossing a cut resolves
against what the segment exposes. A chain of cuts over existing rows backfilling
in dependency order is schema_lifetime's; an outer join inside a cut is
test_outer_join.py's.

Weights throughout: a segment feeding two consumers shows up as a doubled weight
and not as an extra row.
"""
from collections import Counter

from _read import bag, scanned


def _set(vals):
    return dict.fromkeys(((v,) for v in vals), 1)


def _t_a(t):
    return [a for a, _ in t.values()]


def _filter_into_join(t, u):
    return {(i, u[i][1]): 1 for i, (a, _) in t.items() if a > 100 and i in u}


# view -> (body, the columns it is read by, its bag over t and u as {id: (a, b)}).
_VIEWS = {
    # The filter feeding a join as a CTE and as a derived table (both cut), and as
    # a pass-through CTE with the predicate in the final and one inline body
    # (neither cut).
    "cut_cte": ("WITH d AS (SELECT id, a FROM t WHERE a > 100) "
                "SELECT d.id AS did, u.b AS ub FROM d JOIN u ON d.id = u.id",
                ("did", "ub"), _filter_into_join),
    "cut_derived": ("SELECT d.id AS did, u.b AS ub FROM (SELECT id, a FROM t WHERE a > 100) d "
                    "JOIN u ON d.id = u.id", ("did", "ub"), _filter_into_join),
    "uncut_pass_through": ("WITH d AS (SELECT * FROM t) SELECT d.id AS did, u.b AS ub "
                           "FROM d JOIN u ON d.id = u.id WHERE d.a > 100", ("did", "ub"), _filter_into_join),
    "uncut_inline": ("SELECT t.id AS did, u.b AS ub FROM t JOIN u ON t.id = u.id WHERE t.a > 100",
                     ("did", "ub"), _filter_into_join),
    # Any body behind the cut.
    "body_set_op": ("WITH c AS (SELECT a FROM t UNION SELECT a FROM u) SELECT a FROM c",
                    ("a",), lambda t, u: _set(_t_a(t) + _t_a(u))),
    "body_grouped": ("WITH c AS (SELECT a, COUNT(*) AS n FROM t GROUP BY a) SELECT a, n FROM c",
                     ("a", "n"), lambda t, u: dict.fromkeys(Counter(_t_a(t)).items(), 1)),
    "body_join": ("WITH c AS (SELECT t.a AS ta, u.b AS ub FROM t JOIN u ON t.b = u.id) SELECT ta, ub FROM c",
                  ("ta", "ub"), lambda t, u: Counter((a, u[b][1]) for a, b in t.values() if b in u)),
    "body_distinct": ("SELECT a FROM (SELECT DISTINCT a FROM t) d", ("a",), lambda t, u: _set(_t_a(t))),
    "body_nested": ("SELECT a FROM (SELECT a FROM (SELECT a, b FROM t WHERE a > 5) inner_d WHERE b < 100) d",
                    ("a",), lambda t, u: Counter((a,) for a, b in t.values() if a > 5 and b < 100)),
    # Any operator over the cut; the COUNT(*) narrows the segment to no payload.
    "read_where": ("SELECT a FROM (SELECT a, b FROM t) d WHERE b > 10",
                   ("a",), lambda t, u: Counter((a,) for a, b in t.values() if b > 10)),
    "read_grouped": ("SELECT a, COUNT(*) AS n FROM (SELECT a FROM t WHERE b > 0) d GROUP BY a",
                     ("a", "n"), lambda t, u: dict.fromkeys(Counter(a for a, b in t.values() if b > 0).items(), 1)),
    "read_distinct": ("SELECT DISTINCT a FROM (SELECT a FROM t UNION ALL SELECT a FROM u) d",
                      ("a",), lambda t, u: _set(_t_a(t) + _t_a(u))),
    "read_set_op": ("SELECT a FROM (SELECT a, b FROM t WHERE b > 0) d UNION SELECT a FROM u",
                    ("a",), lambda t, u: _set([a for a, b in t.values() if b > 0] + _t_a(u))),
    "read_in": ("WITH c AS (SELECT a FROM u WHERE b > 0) SELECT id FROM t WHERE t.a IN (SELECT a FROM c)",
                ("id",), lambda t, u: {(i,): 1 for i, (a, _) in t.items()
                                       if a in {ua for ua, ub in u.values() if ub > 0}}),
    "read_both_sides": ("SELECT x.a AS xa, y.b AS yb FROM (SELECT id, a FROM t WHERE a > 10) x "
                        "JOIN (SELECT id, b FROM u WHERE b < 100) y ON x.id = y.id",
                        ("xa", "yb"), lambda t, u: Counter((a, u[i][1]) for i, (a, _) in t.items()
                                                           if a > 10 and i in u and u[i][1] < 100)),
    "read_count": ("SELECT COUNT(*) AS n FROM (SELECT a FROM t WHERE b > 0) d",
                   ("n",), lambda t, u: {(sum(b > 0 for _, b in t.values()),): 1}),
    # An inner set op is a segment the outer re-hashes on its own identity, in a
    # chain and across quantifiers; a leaf's identity may be computed.
    "set_op_chain": ("SELECT a FROM t UNION SELECT a FROM u UNION SELECT b FROM t",
                     ("a",), lambda t, u: _set(_t_a(t) + _t_a(u) + [b for _, b in t.values()])),
    "set_op_mixed": ("(SELECT a FROM t UNION ALL SELECT a FROM u) INTERSECT SELECT b FROM t",
                     ("a",), lambda t, u: _set(set(_t_a(t) + _t_a(u)) & {b for _, b in t.values()})),
    "computed_distinct": ("SELECT DISTINCT a + 1 AS a1, b FROM t",
                          ("a1", "b"), lambda t, u: dict.fromkeys(((a + 1, b) for a, b in t.values()), 1)),
    "computed_except": ("SELECT a * 2 AS x FROM t EXCEPT SELECT b FROM u",
                        ("x",), lambda t, u: _set({a * 2 for a in _t_a(t)} - {b for _, b in u.values()})),
    # Column aliases live on what the FROM binder hands the scope, so a join step
    # above them resolves `x.k` — as an ON key, a projection, and through an outer
    # join's null-widening.
    "alias_cte": ("WITH d(k, amt) AS (SELECT id, a FROM t WHERE a > 100) "
                  "SELECT d.k AS kk, d.amt AS aa FROM d JOIN u ON d.k = u.id",
                  ("kk", "aa"), lambda t, u: {(i, a): 1 for i, (a, _) in t.items() if a > 100 and i in u}),
    "alias_derived": ("SELECT x.k AS xk, x.val AS xv, u.b AS ub FROM (SELECT id, a FROM t) AS x(k, val) "
                      "JOIN u ON x.k = u.id",
                      ("xk", "xv", "ub"), lambda t, u: {(i, a, u[i][1]): 1 for i, (a, _) in t.items() if i in u}),
    "alias_left": ("SELECT x.k AS xk, u.b AS ub FROM (SELECT id, a FROM t) AS x(k, val) LEFT JOIN u ON x.k = u.id",
                   ("xk", "ub"), lambda t, u: {(i, u[i][1] if i in u else None): 1 for i in t}),
    # A non-LATERAL sibling is not in scope: `u` inside the second derived table
    # binds the catalog table, since the first sibling has no `b` to resolve.
    "sibling": ("SELECT x.b AS xb FROM (SELECT id FROM t) u JOIN (SELECT id, b FROM u) x ON u.id = x.id",
                ("xb",), lambda t, u: Counter((u[i][1],) for i in t if i in u)),
}

# (statement, table, {id: (a, b)}); a `None` row deletes the id.
_CHURN = [
    ("INSERT INTO u VALUES (1, 30, 60), (2, 90, 0), (4, 7, 20), (7, 30, 0)",
     "u", {1: (30, 60), 2: (90, 0), 4: (7, 20), 7: (30, 0)}),
    # t(1) and t(2) share every column but the key, so a join through them carries weight 2.
    ("INSERT INTO t VALUES (1, 10, 7), (2, 10, 7), (3, 20, 200), (4, 200, 50), (5, 150, 0)",
     "t", {1: (10, 7), 2: (10, 7), 3: (20, 200), 4: (200, 50), 5: (150, 0)}),
    ("UPDATE t SET a = 300 WHERE id = 2", "t", {2: (300, 7)}),
    ("DELETE FROM t WHERE id = 1", "t", {1: None}),
    ("DELETE FROM u WHERE id = 2", "u", {2: None}),
    ("INSERT INTO t VALUES (6, 3, 7)", "t", {6: (3, 7)}),
]


def test_a_cut_segment_answers_as_the_body_it_was_cut_from(client, schema_name):
    """Every cut, every consumer and every name crossing a cut, checked after
    each epoch of a churn on both sources."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        + "; ".join(f"CREATE VIEW {name} AS {body}" for name, (body, _, _) in _VIEWS.items()),
        schema_name=sn)

    state = {"t": {}, "u": {}}
    for sql, table, changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        for i, row in changes.items():
            if row is None:
                del state[table][i]
            else:
                state[table][i] = row
        for name, (_, cols, want) in _VIEWS.items():
            assert bag(scanned(client, sn, name), *cols) == want(state["t"], state["u"]), (sql, name)
