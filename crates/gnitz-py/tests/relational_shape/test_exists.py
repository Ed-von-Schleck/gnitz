"""`[NOT] EXISTS` and `x [NOT] IN (SELECT ...)` view bodies.

There is no anti-join operator: a semi-join emits the preserved row at
`w_A · [S > 0]` and an anti-join at `w_A · [S = 0]`, where `S` is the summed
weight of the inner rows it matches. Output weight therefore depends on match
*existence*, not on weight arithmetic, and the failure a row-set check cannot
see is a second inner match doubling an outer row. Every assertion here is a
weighted bag, against existence recomputed from the rows the test wrote.

Two lowerings share the file. A subquery that is a top-level `AND` conjunct of
the WHERE becomes the semi/anti shape directly; one in any other boolean
position — under OR, under NOT, inside CASE, or projected as a column — is
rewritten to a 0/1 **mark** that ordinary expression evaluation consumes.

Run with GNITZ_WORKERS=4: the correlation key is an exchange key.
"""
from _read import bag, scanned


def _lt(l, r):
    return l is not None and r is not None and l < r


def _eq(l, r):
    return l is not None and r is not None and l == r


# name -> (correlation SQL, the same predicate over (a row, b row)). A NULL key
# matches nothing; `compound` pairs an INT with a BIGINT column; `band` is an
# equality prefix plus a range; `range` has no equality at all, so the inner side
# collapses to one MAX/MIN threshold row.
_CORR = {
    "eq": ("b.k = a.k", lambda a, b: _eq(b["k"], a["k"])),
    "compound": ("b.k1 = a.k1 AND b.k2 = a.k2", lambda a, b: (b["k1"], b["k2"]) == (a["k1"], a["k2"])),
    "band": ("b.k = a.k AND b.t < a.t", lambda a, b: _eq(b["k"], a["k"]) and b["t"] < a["t"]),
    "range": ("b.y < a.x", lambda a, b: _lt(b["y"], a["x"])),
    # Local conjuncts: the inner one pre-filters b, so a failing row is no match.
    "local": ("b.k = a.k AND b.w > 5", lambda a, b: _eq(b["k"], a["k"]) and b["w"] > 5),
}

# The mark lowering, over NOT NULL operands.
_MARK = {
    "eq": ("b.k2 = a.k2", lambda a, b: b["k2"] == a["k2"]),
    "band": ("b.k2 = a.k2 AND b.w < a.v", lambda a, b: b["k2"] == a["k2"] and b["w"] < a["v"]),
    "range": ("b.w < a.v", lambda a, b: b["w"] < a["v"]),
}

_A_COLS = ("id", "k", "k1", "k2", "t", "x", "v")
_B_COLS = ("id", "k", "k1", "k2", "t", "w", "y")

_CHURN = [
    ("INSERT INTO a VALUES (1, 10, 7, 70, 50, 10, 100), (2, 20, 7, 71, 5, 20, 200), "
     "(3, NULL, 8, 70, 50, NULL, 300), (4, 30, 8, 80, 60, 30, 999)",
     "a", [(1, 10, 7, 70, 50, 10, 100), (2, 20, 7, 71, 5, 20, 200),
           (3, None, 8, 70, 50, None, 300), (4, 30, 8, 80, 60, 30, 999)]),
    # A NULL-keyed inner row, and an all-NULL range column: an empty threshold.
    ("INSERT INTO b VALUES (1, NULL, 7, 70, 40, 5, NULL)", "b", [(1, None, 7, 70, 40, 5, None)]),
    ("INSERT INTO b VALUES (2, 10, 7, 70, 45, 8, 15)", "b", [(2, 10, 7, 70, 45, 8, 15)]),
    # A second match for a(1) under every correlation: its weight stays 1.
    ("INSERT INTO b VALUES (3, 10, 7, 71, 5, 9, 5)", "b", [(3, 10, 7, 71, 5, 9, 5)]),
    ("INSERT INTO b VALUES (4, 20, 8, 80, 1, 3, 100)", "b", [(4, 20, 8, 80, 1, 3, 100)]),
    ("INSERT INTO b VALUES (5, 20, 8, 80, 2, 6, 1)", "b", [(5, 20, 8, 80, 2, 6, 1)]),
    # One of two matches goes; then the range threshold's holder.
    ("DELETE FROM b WHERE id = 2", "b", [2]),
    ("DELETE FROM b WHERE id = 5", "b", [5]),
    # A NULL outer key becomes a real one.
    ("UPDATE a SET k = 20 WHERE id = 3", "a", [(3, 20, 8, 70, 50, None, 300)]),
    ("DELETE FROM b", "b", [1, 3, 4]),
]


def test_a_subquery_emits_each_outer_row_once_while_it_matches(client, schema_name):
    """Every correlation shape as a semi/anti pair, plus `IN`/`NOT IN` over the
    same shape, an EXISTS over views feeding a further view, and `SELECT *`,
    which takes the identity projection for the equi shape, the band shape and
    the mark path, whose mark the WHERE consumes without materializing. The mark
    is an ordinary boolean under OR, under NOT and as a `NOT IN`; projected bare
    or through a searched or a simple CASE, a flip must retract the old row."""
    sn = schema_name
    ex = "SELECT 1 FROM b WHERE"
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, k1 INT NOT NULL, "
        "k2 BIGINT NOT NULL, t BIGINT NOT NULL, x BIGINT, v BIGINT NOT NULL); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, k1 BIGINT NOT NULL, "
        "k2 BIGINT NOT NULL, t BIGINT NOT NULL, w BIGINT NOT NULL, y BIGINT); "
        + "; ".join(f"CREATE VIEW semi_{n} AS SELECT id FROM a WHERE EXISTS ({ex} {corr}); "
                    f"CREATE VIEW anti_{n} AS SELECT id FROM a WHERE NOT EXISTS ({ex} {corr})"
                    for n, (corr, _) in _CORR.items() if n != "local") + "; "
        f"CREATE VIEW semi_local AS SELECT id FROM a WHERE v > 100 AND EXISTS ({ex} {_CORR['local'][0]}); "
        "CREATE VIEW in_k1 AS SELECT id FROM a WHERE k1 IN (SELECT k1 FROM b); "
        "CREATE VIEW not_in_k1 AS SELECT id FROM a WHERE k1 NOT IN (SELECT k1 FROM b); "
        "CREATE VIEW in_nullable AS SELECT id FROM a WHERE k IN (SELECT k FROM b); "
        "CREATE VIEW av AS SELECT * FROM a WHERE v > 0; "
        "CREATE VIEW bv AS SELECT * FROM b WHERE w > 0; "
        "CREATE VIEW over_views AS SELECT id FROM av WHERE EXISTS (SELECT 1 FROM bv WHERE bv.k = av.k); "
        "CREATE VIEW above AS SELECT id FROM over_views WHERE id >= 2; "
        f"CREATE VIEW star_eq AS SELECT * FROM a WHERE EXISTS ({ex} {_CORR['eq'][0]}); "
        f"CREATE VIEW star_band AS SELECT * FROM a WHERE EXISTS ({ex} {_CORR['band'][0]}); "
        f"CREATE VIEW star_mark AS SELECT * FROM a WHERE v = 999 OR EXISTS ({ex} {_MARK['eq'][0]}); "
        f"CREATE VIEW mark_or AS SELECT id FROM a WHERE EXISTS ({ex} {_MARK['eq'][0]}) OR v = 100; "
        f"CREATE VIEW mark_not AS SELECT id FROM a WHERE NOT (EXISTS ({ex} {_MARK['eq'][0]}) AND v > 150); "
        "CREATE VIEW mark_not_in AS SELECT id FROM a WHERE v = 100 OR k1 NOT IN (SELECT k1 FROM b); "
        f"CREATE VIEW mark_band AS SELECT id FROM a WHERE v = 100 OR EXISTS ({ex} {_MARK['band'][0]}); "
        f"CREATE VIEW mark_range AS SELECT id FROM a WHERE v = 100 OR EXISTS ({ex} {_MARK['range'][0]}); "
        f"CREATE VIEW flag AS SELECT id, EXISTS ({ex} {_MARK['eq'][0]}) AS f FROM a; "
        f"CREATE VIEW searched AS SELECT id, CASE WHEN EXISTS ({ex} {_MARK['eq'][0]}) THEN v ELSE 0 END AS f FROM a; "
        f"CREATE VIEW simple AS SELECT id, CASE EXISTS ({ex} {_MARK['eq'][0]}) WHEN 1 THEN 10 WHEN 0 THEN 20 END AS f FROM a",
        schema_name=sn)

    a, b = {}, {}
    for sql, table, changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        state, cols = (a, _A_COLS) if table == "a" else (b, _B_COLS)
        for change in changes:
            if isinstance(change, int):
                del state[change]
            else:
                state[change[0]] = dict(zip(cols, change))

        def ids(keep):
            return {(r["id"],): 1 for r in a.values() if keep(r)}

        def exists(pred):
            return lambda r: any(pred(r, br) for br in b.values())

        for n, (_, pred) in _CORR.items():
            assert bag(scanned(client, sn, f"semi_{n}"), "id") == \
                ids(lambda r: (n != "local" or r["v"] > 100) and exists(pred)(r)), (sql, n)
            if n != "local":
                assert bag(scanned(client, sn, f"anti_{n}"), "id") == ids(lambda r: not exists(pred)(r)), (sql, n)
        b_k1 = {br["k1"] for br in b.values()}
        assert bag(scanned(client, sn, "in_k1"), "id") == ids(lambda r: r["k1"] in b_k1), sql
        assert bag(scanned(client, sn, "not_in_k1"), "id") == ids(lambda r: r["k1"] not in b_k1), sql
        in_eq = ids(exists(_CORR["eq"][1]))
        assert bag(scanned(client, sn, "in_nullable"), "id") == in_eq, sql
        assert bag(scanned(client, sn, "over_views"), "id") == in_eq, sql
        assert bag(scanned(client, sn, "above"), "id") == {i: 1 for i in in_eq if i[0] >= 2}, sql
        for name, keep in (("star_eq", exists(_CORR["eq"][1])), ("star_band", exists(_CORR["band"][1])),
                           ("star_mark", lambda r: r["v"] == 999 or exists(_MARK["eq"][1])(r))):
            star = scanned(client, sn, name)
            assert bag(star, *_A_COLS) == {tuple(r.values()): 1 for r in a.values() if keep(r)}, (sql, name)
            assert all(set(row._fields) == set(_A_COLS) for row in star), (sql, name)

        mark = exists(_MARK["eq"][1])
        for name, keep in (
            ("mark_or", lambda r: mark(r) or r["v"] == 100),
            ("mark_not", lambda r: not (mark(r) and r["v"] > 150)),
            ("mark_not_in", lambda r: r["v"] == 100 or r["k1"] not in b_k1),
            ("mark_band", lambda r: r["v"] == 100 or exists(_MARK["band"][1])(r)),
            ("mark_range", lambda r: r["v"] == 100 or exists(_MARK["range"][1])(r)),
        ):
            assert bag(scanned(client, sn, name), "id") == ids(keep), (sql, name)
        for name, value in (("flag", lambda r: int(mark(r))),
                            ("searched", lambda r: r["v"] if mark(r) else 0),
                            ("simple", lambda r: 10 if mark(r) else 20)):
            assert bag(scanned(client, sn, name), "id", "f") == {
                (r["id"], value(r)): 1 for r in a.values()}, (sql, name)
