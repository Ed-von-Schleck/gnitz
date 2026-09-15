"""N-way joins: the left-deep chain of hidden two-way segments.

`a JOIN b JOIN c` is decomposed into `(a ⋈ b) ⋈ c`, where the inner step becomes
a hidden join view — a first-class relation whose registered schema already
carries its `_join_pk`, so the outer step resolves its columns like any other
relation. A provenance map carries the original aliases forward, so a later ON,
WHERE or projection naming `a.x` resolves to the accumulated segment's physical
column. Writing the segments by hand as CTEs produces the same chain.

Each segment projects only the columns something above it still reads. That is
invisible in the result by construction, so the tests drive it through the cases
where it could stop being invisible: a column only a later step reads, a column
nothing reads being updated, an outer step whose null-fill sits inside a hidden
segment, and a chain wide enough that pruning is what keeps it under the column
cap.
"""
from collections import Counter

from _read import bag, scanned

# (statement, table, {id: row}); a `None` row deletes the id. Rows are
# a: (k, v, p, dead), b: (k, v, q), c: (k, v).
_CHURN = [
    ("INSERT INTO b VALUES (10, 500, 11, 4), (20, 600, 22, 4), (30, 999, 33, 1)",
     "b", {10: (500, 11, 4), 20: (600, 22, 4), 30: (999, 33, 1)}),
    ("INSERT INTO c VALUES (500, 0, 99), (600, 5, 88), (700, 50, 88), (800, 0, 10), (5, 3, 77)",
     "c", {500: (0, 99), 600: (5, 88), 700: (50, 88), 800: (0, 10), 5: (3, 77)}),
    # a(3)'s key names no b; a(4)'s b(30) names no c.
    ("INSERT INTO a VALUES (1, 10, 700, 5, 999), (2, 10, 800, 3, 0), (3, 99, 700, 9, 1), "
     "(4, 30, 5, 9, 2)",
     "a", {1: (10, 700, 5, 999), 2: (10, 800, 3, 0), 3: (99, 700, 9, 1), 4: (30, 5, 9, 2)}),
    # Re-keying the head moves its row onto another path in one epoch.
    ("UPDATE a SET k = 20 WHERE id = 1", "a", {1: (20, 700, 5, 999)}),
    ("UPDATE a SET dead = 12345 WHERE id = 1", "a", {1: (20, 700, 5, 12345)}),
    ("DELETE FROM c WHERE id = 700", "c", {700: None}),
    # The bridge row in the middle relation retracts everything derived from it.
    ("DELETE FROM b WHERE id = 10", "b", {10: None}),
    ("INSERT INTO c VALUES (999, 0, 0)", "c", {999: (0, 0)}),
]


def test_a_chain_resolves_every_relation_below_each_step(client, schema_name):
    """A hand-written segment and the direct form compile to one chain. A later
    ON may key on a column deep in the accumulator, and a WHERE over the whole
    chain lands at the step whose inputs it names. A CTE alias list lines up with
    a join body's visible columns and skips its hidden key. An outer step
    null-fills wherever it sits: a preserved row with no match still reaches the
    next step through its own columns, a right-only fill carries NULL keys into
    the next inner step and matches nothing, and a later delta that satisfies a
    preserved row retracts its fill. A range step composes with the equi segment
    below it, and a residual reading columns nothing projects keeps them alive
    while an unread column's update leaves the result alone."""
    sn = schema_name
    ab = "FROM a JOIN b ON a.k = b.id"
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL, "
        "p BIGINT NOT NULL, dead BIGINT NOT NULL); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL, "
        "q BIGINT NOT NULL); "
        "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL); "
        f"CREATE VIEW direct AS SELECT a.id AS aid, c.v AS cv {ab} JOIN c ON b.k = c.id; "
        f"CREATE VIEW nested_cte AS WITH h0 AS (SELECT a.id AS aid, b.k AS bk {ab}) "
        "SELECT h0.aid AS aid, c.v AS cv FROM h0 JOIN c ON h0.bk = c.id; "
        f"CREATE VIEW deep AS SELECT a.v AS av, b.v AS bv, c.v AS cv {ab} JOIN c ON a.v = c.id "
        "WHERE c.v > 50; "
        f"CREATE VIEW aliased AS WITH h0(x, y) AS (SELECT a.id AS aid, b.k AS bk {ab}) "
        "SELECT h0.x AS x FROM h0 JOIN c ON h0.y = c.id; "
        "CREATE VIEW first_left AS SELECT a.id AS aid, b.id AS bid, c.v AS cv "
        "FROM a LEFT JOIN b ON a.k = b.id JOIN c ON a.v = c.id; "
        f"CREATE VIEW last_left AS SELECT a.id AS aid, c.id AS cid {ab} LEFT JOIN c ON b.k = c.id; "
        "CREATE VIEW full_mid AS SELECT a.k AS ak, b.v AS bv, c.v AS cv "
        "FROM a FULL JOIN b ON a.k = b.id JOIN c ON a.k = c.id; "
        f"CREATE VIEW range_step AS SELECT a.id AS aid, c.id AS cid {ab} JOIN c ON a.v < c.k; "
        "CREATE VIEW residual AS SELECT a.id AS aid, c.v AS cv "
        "FROM a JOIN b ON a.k = b.id AND a.p > b.q JOIN c ON a.v = c.id",
        schema_name=sn)

    state = {"a": {}, "b": {}, "c": {}}
    for sql, table, changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        for i, row in changes.items():
            if row is None:
                del state[table][i]
            else:
                state[table][i] = row
        a, b, c = state["a"], state["b"], state["c"]
        via_b = {i: b[k] for i, (k, *_) in a.items() if k in b}

        direct = Counter((i, c[bk][1]) for i, (bk, *_) in via_b.items() if bk in c)
        for name in ("direct", "nested_cte"):
            assert bag(scanned(client, sn, name), "aid", "cv") == direct, (sql, name)
        assert bag(scanned(client, sn, "aliased"), "x") == Counter((i,) for i, _ in direct), sql
        assert bag(scanned(client, sn, "deep"), "av", "bv", "cv") == Counter(
            (a[i][1], bv, c[a[i][1]][1]) for i, (_, bv, _) in via_b.items()
            if a[i][1] in c and c[a[i][1]][1] > 50), sql
        assert bag(scanned(client, sn, "first_left"), "aid", "bid", "cv") == Counter(
            (i, k if k in b else None, c[v][1]) for i, (k, v, *_) in a.items() if v in c), sql
        assert bag(scanned(client, sn, "last_left"), "aid", "cid") == Counter(
            (i, bk if bk in c else None) for i, (bk, *_) in via_b.items()), sql
        assert bag(scanned(client, sn, "full_mid"), "ak", "bv", "cv") == Counter(
            (k, b[k][1] if k in b else None, c[k][1]) for k, *_ in a.values() if k in c), sql
        assert bag(scanned(client, sn, "range_step"), "aid", "cid") == Counter(
            (i, ci) for i in via_b for ci, (ck, _) in c.items() if a[i][1] < ck), sql
        assert bag(scanned(client, sn, "residual"), "aid", "cv") == Counter(
            (i, c[a[i][1]][1]) for i, (_, _, q) in via_b.items() if a[i][2] > q and a[i][1] in c), sql


def test_a_deep_or_wide_chain_compiles_and_maintains(client, schema_name):
    """Seven hidden segments stacked: the decomposition is not depth-limited.
    And four 18-column tables under a two-column SELECT: carrying every source
    column forward would put the last segment at 1 + 55 + 18 = 74 columns, over
    the 65-column cap, so pruning to live columns is what lets it compile."""
    sn = schema_name
    n = 8
    pads = ", ".join(f"p{i} BIGINT NOT NULL" for i in range(16))
    zeros = ", ".join("0" for _ in range(16))
    joins = " ".join(f"JOIN t{i} ON t{i - 1}.k = t{i}.id" for i in range(1, n))
    client.execute_sql(
        "; ".join(f"CREATE TABLE t{i} (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                  "v BIGINT NOT NULL)" for i in range(n)) + "; "
        f"CREATE VIEW deep AS SELECT t0.id AS x, t{n - 1}.v AS y FROM t0 {joins}; "
        + "; ".join(f"CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, {key} BIGINT NOT NULL, {pads})"
                    for t, key in (("wa", "k1"), ("wb", "k2"), ("wc", "k3"), ("wd", "val"))) + "; "
        "CREATE VIEW wide AS SELECT wa.id AS aid, wd.val AS dval FROM wa JOIN wb ON wa.k1 = wb.id "
        "JOIN wc ON wb.k2 = wc.id JOIN wd ON wc.k3 = wd.id; "
        + "; ".join(f"INSERT INTO t{i} VALUES (1, {1 if i < n - 1 else 0}, {100 + i})" for i in range(n))
        + f"; INSERT INTO wd VALUES (30, 42, {zeros}); INSERT INTO wc VALUES (20, 30, {zeros}); "
        f"INSERT INTO wb VALUES (10, 20, {zeros}); INSERT INTO wa VALUES (1, 10, {zeros})",
        schema_name=sn)
    assert bag(scanned(client, sn, "deep"), "x", "y") == {(1, 100 + n - 1): 1}
    assert bag(scanned(client, sn, "wide"), "aid", "dval") == {(1, 42): 1}

    client.execute_sql("DELETE FROM wc WHERE id = 20; DELETE FROM t3 WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "wide"), "aid", "dval") == {}
    assert bag(scanned(client, sn, "deep"), "x", "y") == {}
    client.execute_sql(f"INSERT INTO wc VALUES (20, 30, {zeros}); UPDATE wd SET val = 99 WHERE id = 30",
                       schema_name=sn)
    assert bag(scanned(client, sn, "wide"), "aid", "dval") == {(1, 99): 1}
