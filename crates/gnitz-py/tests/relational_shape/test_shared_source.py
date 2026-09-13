"""A join whose two inputs trace to one relation.

`t ⋈ view-over-t`, two views over one base, and the same table twice in one
FROM are each reached by one push in two separate epochs, so the bilinear
cross-term `dA ⋈ dB` is emitted exactly once. For the same table named twice the
planner wraps the *repeated* occurrence in a pass-through under a fresh id, so
the join sees two distinct sources; three occurrences stack two pass-throughs.
That is a claim about weights: the failure is a doubled weight or a missing
product row, and a row-set comparison sees neither. The set-operation form of
the same rule is test_set_ops.py's.
"""
from collections import Counter

from _read import bag, scanned

_COLS = ("id", "mgr", "k", "v", "sal", "dept")

# (statement, {id: row}); a `None` row deletes the id.
_CHURN = [
    # One epoch creates matches on both sides of every join at once: three rows
    # sharing k=5 must make the full 3x3 product, which dropping the cross-term
    # would undercount.
    ("INSERT INTO emp VALUES (1, 0, 5, 10, 500, 10), (2, 1, 5, 20, 600, 10), (3, 1, 5, 30, 400, 10)",
     {1: (0, 5, 10, 500, 10), 2: (1, 5, 20, 600, 10), 3: (1, 5, 30, 400, 10)}),
    # emp(2) stays in `emp` but leaves `positive`.
    ("UPDATE emp SET v = -1 WHERE id = 2", {2: (1, 5, -1, 600, 10)}),
    ("INSERT INTO emp VALUES (4, 3, 5, 11, 700, 10), (5, 4, 6, 70, 100, 20)",
     {4: (3, 5, 11, 700, 10), 5: (4, 6, 70, 100, 20)}),
    ("UPDATE emp SET sal = 900 WHERE id = 3", {3: (1, 5, 30, 900, 10)}),
    ("UPDATE emp SET v = 60 WHERE id = 1", {1: (0, 5, 60, 500, 10)}),
    # A manager leaves: every report's row and every product row it was in.
    ("DELETE FROM emp WHERE id = 1", {1: None}),
]


def test_a_join_over_one_source_emits_every_product_row_once(client, schema_name):
    """`emp ⋈ positive` reads two dependency edges onto one relation; `positive
    ⋈ over50` two views over one base; employee-to-manager is `emp` twice, with a
    residual comparing the base against its pass-through copy, a third occurrence
    for the grand-manager, an ordinary relation beside the repeat, and a USING
    spelling, whose names resolve before the right alias enters scope."""
    sn = schema_name
    em = "FROM emp e JOIN emp m ON e.mgr = m.id"
    client.execute_sql(
        "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, k BIGINT NOT NULL, "
        "v BIGINT NOT NULL, sal BIGINT NOT NULL, dept BIGINT NOT NULL); "
        "CREATE TABLE dept (id BIGINT NOT NULL PRIMARY KEY, budget BIGINT NOT NULL); "
        "INSERT INTO dept VALUES (10, 999); "
        "CREATE VIEW positive AS SELECT id, k, v FROM emp WHERE v > 0; "
        "CREATE VIEW over50 AS SELECT id, k, v FROM emp WHERE v > 50; "
        "CREATE VIEW base_view AS SELECT emp.id AS lid, positive.id AS rid "
        "FROM emp JOIN positive ON emp.k = positive.k; "
        "CREATE VIEW two_views AS SELECT positive.id AS lid, over50.id AS rid "
        "FROM positive JOIN over50 ON positive.k = over50.k; "
        f"CREATE VIEW boss AS SELECT e.id AS eid, m.id AS mid {em}; "
        f"CREATE VIEW richer AS SELECT e.id AS eid {em} WHERE e.sal > m.sal; "
        f"CREATE VIEW grand AS SELECT e.id AS eid, g.id AS gid {em} JOIN emp g ON m.mgr = g.id; "
        f"CREATE VIEW with_dept AS SELECT e.id AS eid, m.id AS mid, d.budget AS bud {em} "
        "JOIN dept d ON e.dept = d.id; "
        "CREATE VIEW using_self AS SELECT x.id AS xid, y.id AS yid FROM emp x JOIN emp y USING (k)",
        schema_name=sn)

    emp = {}
    for sql, changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        for i, row in changes.items():
            if row is None:
                del emp[i]
            else:
                emp[i] = dict(zip(_COLS, (i, *row)))
        rows = list(emp.values())

        def pairs(left, right, on):
            return Counter((l["id"], r["id"]) for l in left for r in right if on(l, r))

        positive = [r for r in rows if r["v"] > 0]
        same_k = lambda l, r: l["k"] == r["k"]  # noqa: E731
        reports = pairs(rows, rows, lambda e, m: e["mgr"] == m["id"])
        assert bag(scanned(client, sn, "base_view"), "lid", "rid") == pairs(rows, positive, same_k), sql
        assert bag(scanned(client, sn, "two_views"), "lid", "rid") == \
            pairs(positive, [r for r in rows if r["v"] > 50], same_k), sql
        assert bag(scanned(client, sn, "boss"), "eid", "mid") == reports, sql
        assert bag(scanned(client, sn, "richer"), "eid") == Counter(
            (e,) for e, m in reports.elements() if emp[e]["sal"] > emp[m]["sal"]), sql
        assert bag(scanned(client, sn, "grand"), "eid", "gid") == Counter(
            (e, emp[m]["mgr"]) for e, m in reports.elements() if emp[m]["mgr"] in emp), sql
        assert bag(scanned(client, sn, "with_dept"), "eid", "mid", "bud") == Counter(
            (e, m, 999) for e, m in reports.elements() if emp[e]["dept"] == 10), sql
        assert bag(scanned(client, sn, "using_self"), "xid", "yid") == pairs(rows, rows, same_k), sql
