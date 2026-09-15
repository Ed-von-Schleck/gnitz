"""Scalar aggregate subquery view bodies — correlated and uncorrelated — and the
`ANY` / `ALL` quantified comparisons that lower onto the same machinery.

A scalar subquery becomes a reduce whose one row per correlation group (or one
row for the whole relation) is joined back onto the outer row, which reads the
subquery as that joined column wherever it appeared. The two coordinates that decide the
compiled shape are therefore:

  * **correlated** — a grouped reduce keyed on the correlation columns, joined
    back as a LEFT join so an outer row with no inner group survives;
  * **uncorrelated** — a global reduce whose single ground row is broadcast and
    joined as an INNER join, so an empty source's ground *value* is what decides
    whether the outer row survives at all.

The ground row is where the two aggregate families part: COUNT's is a real 0,
every other aggregate's is NULL, and a comparison against NULL is UNKNOWN. A
lowering that keyed the join on the raw reduce column instead of the finalized
one reads that NULL as 0 and admits exactly the rows the ground row must
exclude, so the outer values include the ones a raw 0 would wrongly admit.

Every assertion is a weighted bag: a flip that emits the new value without
retracting the old leaves both present, which a value lookup would not see.

Run with GNITZ_WORKERS=4 — a global reduce funnels, a grouped one exchanges.
"""
from _read import bag, scanned


def _gt(l, r):
    return l is not None and r is not None and l > r


def _sum(vals):
    live = [v for v in vals if v is not None]
    return sum(live) if live else None


_COUNT = "(SELECT COUNT(*) FROM b WHERE b.k = a.k)"
_SUM = "(SELECT SUM(w) FROM b WHERE b.k = a.k)"

_VIEWS = [
    "proj AS SELECT a.id, " + _COUNT + " AS c, " + _SUM + " AS s, "
    "(SELECT MIN(w) FROM b WHERE b.k = a.k) AS mn, (SELECT MAX(w) FROM b WHERE b.k = a.k) AS mx, "
    "(SELECT AVG(w) FROM b WHERE b.k = a.k) AS av FROM a",
    # An inner-local conjunct filters the reduce's input, not the group key.
    "filtered AS SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k AND b.w > 5) AS c FROM a",
    f"at_least_two AS SELECT a.id FROM a WHERE {_COUNT} >= 2",
    f"mixed AS SELECT a.id, CASE WHEN {_COUNT} > 0 THEN 1 ELSE 0 END AS hit FROM a "
    f"WHERE a.v > 0 OR {_SUM} > 100",
    # The inner relation is the outer one under another alias.
    "own_group AS SELECT a1.id FROM a a1 WHERE a1.v >= (SELECT SUM(a2.v) FROM a a2 WHERE a2.k = a1.k)",
    f"stacked AS SELECT a.id, {_SUM} AS sb, (SELECT SUM(x) FROM c WHERE c.k = a.k) AS sc FROM a",
    # COUNT is never NULL, so these fold: never to the default, even for no group.
    f"coalesced AS SELECT a.id, COALESCE({_COUNT}, 5) AS c FROM a",
    f"not_null AS SELECT a.id FROM a WHERE {_COUNT} IS NOT NULL",
    "global_not_null AS SELECT a.id FROM a WHERE (SELECT COUNT(*) FROM b) IS NOT NULL",
    "eq_any AS SELECT a.id FROM a WHERE a.k = ANY (SELECT k FROM b)",
    "ne_all AS SELECT a.id FROM a WHERE a.k <> ALL (SELECT k FROM b)",
    "lt_any AS SELECT a.id FROM a WHERE a.v < ANY (SELECT w FROM b WHERE b.k = a.k)",
    "ge_all AS SELECT a.id FROM a WHERE a.v >= ALL (SELECT w FROM b WHERE b.k = a.k)",
    "not_lt_any AS SELECT a.id FROM a WHERE NOT (a.v < ANY (SELECT w FROM b WHERE b.k = a.k))",
    "global_lt_any AS SELECT a.id FROM a WHERE a.v < ANY (SELECT w FROM b)",
    f"composed AS SELECT a.id, {_COUNT} AS c FROM a WHERE a.v < (SELECT MAX(w) FROM b)",
    "lt_max AS SELECT a.id FROM a WHERE a.v < (SELECT MAX(w) FROM b)",
    "eq_min AS SELECT a.id FROM a WHERE a.v = (SELECT MIN(w) FROM b)",
    "eq_count AS SELECT a.id FROM a WHERE a.v = (SELECT COUNT(*) FROM b)",
    "eq_sum AS SELECT a.id FROM a WHERE a.v = (SELECT SUM(y) FROM d)",
    "lt_sum AS SELECT a.id FROM a WHERE a.v < (SELECT SUM(y) FROM d)",
    "global_proj AS SELECT a.id, (SELECT COUNT(*) FROM b) AS n, (SELECT MAX(w) FROM b) AS mx FROM a",
    "ne_count AS SELECT a.id FROM a WHERE a.v <> (SELECT COUNT(*) FROM b)",
    "global_ge_all AS SELECT a.id FROM a WHERE a.v >= ALL (SELECT w FROM b)",
    "not_global_lt_any AS SELECT a.id FROM a WHERE NOT (a.v < ANY (SELECT w FROM b))",
    "avg_cmp AS SELECT a.id FROM a WHERE a.v < (SELECT AVG(w) FROM b)",
    "or_global AS SELECT a.id FROM a WHERE a.v = 0 OR a.v > (SELECT MIN(w) FROM b)",
    "shifted AS SELECT a.id FROM a WHERE a.v + 1 < (SELECT MAX(w) FROM b)",
]

_CHURN = [
    ("SELECT 1", "b", {}),
    ("INSERT INTO c VALUES (1, 10, 9)", "c", {1: (10, 9)}),
    # Every b group, and b itself, empty: COUNT reads 0, everything else NULL.
    ("INSERT INTO b VALUES (1, 10, 4), (2, 10, 10)", "b", {1: (10, 4), 2: (10, 10)}),
    # A genuine 0 minimum over the whole of b.
    ("INSERT INTO b VALUES (3, 20, 0)", "b", {3: (20, 0)}),
    ("INSERT INTO d VALUES (1, 5)", "d", {1: (5,)}),
    ("INSERT INTO d VALUES (2, NULL)", "d", {2: (None,)}),
    # The MIN of group 10 is retracted and re-derives from history.
    ("DELETE FROM b WHERE id = 1", "b", {1: None}),
    # Only a NULL left in d: SUM is NULL again.
    ("DELETE FROM d WHERE id = 1", "d", {1: None}),
    ("DELETE FROM b", "b", {2: None, 3: None}),
]


def test_a_scalar_subquery_reads_its_groups_ground_value_through_churn(client, schema_name):
    """Every placement of a correlated aggregate — projected, compared in a
    WHERE, inside a CASE and under OR, over the outer relation itself, stacked
    with a second one — reads its group's value, and an outer row with no group
    reads the ground value. The ordering quantifiers become an extremum plus an
    existence conjunct, since ANY(∅) is a definite FALSE and ALL(∅) TRUE, which is
    what makes a negated ANY over an empty group TRUE. The uncorrelated forms join
    on the finalized ground value: NULL for MIN/MAX/SUM, so `v < MAX` and
    `v = MIN` admit nothing where a raw 0 would admit `v = 0` and `v < 0`, and 0
    for COUNT."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL); "
        "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT NOT NULL); "
        "CREATE TABLE d (id BIGINT NOT NULL PRIMARY KEY, y BIGINT); "
        + "; ".join(f"CREATE VIEW {v}" for v in _VIEWS) + "; "
        # k=10 sums to 105, so only a row that is its whole group reaches `own_group`;
        # v = 0 and v = -7 are the values a raw 0 ground would admit.
        "INSERT INTO a VALUES (1, 10, 100), (2, 10, 5), (3, 20, 7), (4, 30, 0), (5, 40, -7)",
        schema_name=sn)
    a = {1: (10, 100), 2: (10, 5), 3: (20, 7), 4: (30, 0), 5: (40, -7)}

    state = {"b": {}, "c": {}, "d": {}}
    for sql, table, changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        for pk, row in changes.items():
            if row is None:
                del state[table][pk]
            else:
                state[table][pk] = row
        b, c, d = state["b"], state["c"], state["d"]
        ws = {i: [w for bk, w in b.values() if bk == k] for i, (k, _) in a.items()}
        every_w = [w for _, w in b.values()]
        top, low = max(every_w, default=None), min(every_w, default=None)
        dsum = _sum(y for (y,) in d.values())

        def ids(keep):
            return {(i,): 1 for i, (k, v) in a.items() if keep(i, k, v)}

        assert bag(scanned(client, sn, "proj"), "id", "c", "s", "mn", "mx", "av") == {
            (i, len(g), _sum(g), min(g, default=None), max(g, default=None),
             sum(g) / len(g) if g else None): 1 for i, g in ws.items()}, sql
        assert bag(scanned(client, sn, "filtered"), "id", "c") == {
            (i, sum(w > 5 for w in g)): 1 for i, g in ws.items()}, sql
        assert bag(scanned(client, sn, "at_least_two"), "id") == ids(lambda i, k, v: len(ws[i]) >= 2), sql
        assert bag(scanned(client, sn, "mixed"), "id", "hit") == {
            (i, int(bool(ws[i]))): 1 for i, (_, v) in a.items() if v > 0 or _gt(_sum(ws[i]), 100)}, sql
        assert bag(scanned(client, sn, "own_group"), "id") == ids(
            lambda i, k, v: v >= sum(ov for ok, ov in a.values() if ok == k)), sql
        assert bag(scanned(client, sn, "stacked"), "id", "sb", "sc") == {
            (i, _sum(ws[i]), _sum(x for ck, x in c.values() if ck == k)): 1 for i, (k, _) in a.items()}, sql
        assert bag(scanned(client, sn, "coalesced"), "id", "c") == {(i, len(g)): 1 for i, g in ws.items()}, sql
        for name in ("not_null", "global_not_null"):
            assert bag(scanned(client, sn, name), "id") == ids(lambda i, k, v: True), (sql, name)
        b_keys = {bk for bk, _ in b.values()}
        assert bag(scanned(client, sn, "eq_any"), "id") == ids(lambda i, k, v: k in b_keys), sql
        assert bag(scanned(client, sn, "ne_all"), "id") == ids(lambda i, k, v: k not in b_keys), sql
        lt_any = lambda i, k, v: bool(ws[i]) and v < max(ws[i])  # noqa: E731
        assert bag(scanned(client, sn, "lt_any"), "id") == ids(lt_any), sql
        assert bag(scanned(client, sn, "ge_all"), "id") == ids(
            lambda i, k, v: not ws[i] or v >= max(ws[i])), sql
        assert bag(scanned(client, sn, "not_lt_any"), "id") == ids(lambda i, k, v: not lt_any(i, k, v)), sql
        assert bag(scanned(client, sn, "global_lt_any"), "id") == ids(lambda i, k, v: _gt(top, v)), sql
        assert bag(scanned(client, sn, "composed"), "id", "c") == {
            (i, len(ws[i])): 1 for i, (_, v) in a.items() if _gt(top, v)}, sql
        assert bag(scanned(client, sn, "lt_max"), "id") == ids(lambda i, k, v: _gt(top, v)), sql
        assert bag(scanned(client, sn, "eq_min"), "id") == ids(lambda i, k, v: low is not None and v == low), sql
        assert bag(scanned(client, sn, "eq_count"), "id") == ids(lambda i, k, v: v == len(b)), sql
        assert bag(scanned(client, sn, "eq_sum"), "id") == ids(lambda i, k, v: dsum is not None and v == dsum), sql
        assert bag(scanned(client, sn, "lt_sum"), "id") == ids(lambda i, k, v: _gt(dsum, v)), sql
        assert bag(scanned(client, sn, "global_proj"), "id", "n", "mx") == {(i, len(b), top): 1 for i in a}, sql
        assert bag(scanned(client, sn, "ne_count"), "id") == ids(lambda i, k, v: v != len(b)), sql
        assert bag(scanned(client, sn, "global_ge_all"), "id") == ids(lambda i, k, v: top is None or v >= top), sql
        assert bag(scanned(client, sn, "not_global_lt_any"), "id") == ids(lambda i, k, v: not _gt(top, v)), sql
        assert bag(scanned(client, sn, "avg_cmp"), "id") == ids(
            lambda i, k, v: bool(every_w) and v < sum(every_w) / len(every_w)), sql
        assert bag(scanned(client, sn, "or_global"), "id") == ids(lambda i, k, v: v == 0 or _gt(v, low)), sql
        assert bag(scanned(client, sn, "shifted"), "id") == ids(lambda i, k, v: _gt(top, v + 1)), sql


def test_an_uncorrelated_scalar_over_a_replicated_relation_counts_once(client, schema_name):
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL); "
        "CREATE TABLE rb (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL) WITH (replicated = true); "
        "CREATE VIEW lt_max_r AS SELECT a.id FROM a WHERE a.v < (SELECT MAX(w) FROM rb); "
        "CREATE VIEW proj_r AS SELECT a.id, (SELECT COUNT(*) FROM rb) AS n FROM a; "
        "INSERT INTO a VALUES " + ", ".join(f"({i}, {i})" for i in range(1, 21)),
        schema_name=sn)
    rb = {}
    for sql, changes in [
        ("SELECT 1", {}),
        ("INSERT INTO rb VALUES (1, 5), (2, 12)", {1: 5, 2: 12}),
        ("INSERT INTO rb VALUES (3, 30)", {3: 30}),
        ("DELETE FROM rb WHERE id = 3", {3: None}),
        ("DELETE FROM rb", {1: None, 2: None}),
    ]:
        client.execute_sql(sql, schema_name=sn)
        for pk, w in changes.items():
            if w is None:
                del rb[pk]
            else:
                rb[pk] = w
        top = max(rb.values(), default=None)
        assert bag(scanned(client, sn, "lt_max_r"), "id") == {(i,): 1 for i in range(1, 21) if _gt(top, i)}, sql
        assert bag(scanned(client, sn, "proj_r"), "id", "n") == {(i, len(rb)): 1 for i in range(1, 21)}, sql
