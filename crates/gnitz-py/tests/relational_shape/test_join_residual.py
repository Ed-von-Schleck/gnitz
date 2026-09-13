"""The non-key predicate over an inner join, written into the ON as a residual
or into the WHERE above it.

A conjunct the key classifier cannot use — an inequality, a comparison against a
literal, a second range — becomes a linear Filter over the join's output. For an
INNER join that is also what a top-level WHERE becomes, so `ON (k AND p)` and
`ON k WHERE p` are one plan. Over an OUTER join the two part company; that is
test_outer_join.py's.

Ground truth is recomputed from the rows the test wrote, never scanned back
from the engine, and compared as a weighted bag.
"""
from _read import bag, scanned


def _known(*vals):
    return all(v is not None for v in vals)


# name -> (the ON beyond `a.k = b.k`, or a whole ON where it starts with "ON:",
# the same predicate over (a row, b row)).
_PREDICATES = {
    "literal": ("a.x > 150", lambda a, b: a["x"] > 150),
    "both_sides": ("a.x > b.y", lambda a, b: a["x"] > b["y"]),
    "two_conjuncts": ("a.x > 100 AND b.y = 5", lambda a, b: a["x"] > 100 and b["y"] == 5),
}

# view -> (ON clause, the same predicate). A residual comparison against NULL is
# UNKNOWN and drops the pair; a join carries at most one range in its physical
# shape, so a second inequality rides above a band or a pure range; a string
# residual compares content — every value shares a 13-byte prefix and so lives
# out of line, differing only in the heap tail — and a literal operand takes the
# same opcodes; `IS NULL` over a NOT NULL column folds to an always-empty view.
_RESIDUALS = {
    "not_equal": ("a.k = b.k AND a.v <> b.v",
                  lambda a, b: a["k"] == b["k"] and _known(a["v"]) and a["v"] != b["v"]),
    "band_second_range": ("a.k = b.k AND a.lo < b.hi AND a.x > b.y",
                          lambda a, b: a["k"] == b["k"] and a["lo"] < b["hi"] and a["x"] > b["y"]),
    "pure_second_range": ("a.lo < b.hi AND a.x > b.y", lambda a, b: a["lo"] < b["hi"] and a["x"] > b["y"]),
    "string_pair": ("a.k = b.k AND a.s <> b.s", lambda a, b: a["k"] == b["k"] and a["s"] != b["s"]),
    "string_literal": ("a.k = b.k AND a.s < 'commonprefix_M'",
                       lambda a, b: a["k"] == b["k"] and a["s"] < "commonprefix_M"),
    "folded": ("a.k = b.k AND a.x IS NULL", lambda a, b: False),
}

_A = ("id", "k", "v", "lo", "x", "s")
_B = ("id", "k", "v", "hi", "y", "s")

_CHURN = [
    ("INSERT INTO b VALUES (10, 1, 5, 50, 5, 'commonprefix_BBBB'), (11, 1, 8, 10, 5, 'commonprefix_SAME'), "
     "(12, 2, 9, 99, 0, 'commonprefix_ZZZZ')",
     "b", [(10, 1, 5, 50, 5, "commonprefix_BBBB"), (11, 1, 8, 10, 5, "commonprefix_SAME"),
           (12, 2, 9, 99, 0, "commonprefix_ZZZZ")]),
    # a(5)'s key has no b at all, so no predicate can readmit it.
    ("INSERT INTO a VALUES (1, 1, 5, 20, 9, 'commonprefix_AAAA'), (2, 1, 7, 5, 1, 'commonprefix_SAME'), "
     "(3, 2, NULL, 1, 200, 'commonprefix_SAME'), (4, 1, NULL, 1, 160, 'commonprefix_AAAB'), "
     "(5, 99, 1, 1, 400, 'commonprefix_SAME')",
     "a", [(1, 1, 5, 20, 9, "commonprefix_AAAA"), (2, 1, 7, 5, 1, "commonprefix_SAME"),
           (3, 2, None, 1, 200, "commonprefix_SAME"), (4, 1, None, 1, 160, "commonprefix_AAAB"),
           (5, 99, 1, 1, 400, "commonprefix_SAME")]),
    # Deltas that change only a residual's outcome must still enter or leave.
    ("UPDATE b SET v = 7 WHERE id = 11", "b", [(11, 1, 7, 10, 5, "commonprefix_SAME")]),
    ("UPDATE a SET v = 6 WHERE id = 1", "a", [(1, 1, 6, 20, 9, "commonprefix_AAAA")]),
    ("UPDATE a SET x = 0 WHERE id = 1", "a", [(1, 1, 6, 20, 0, "commonprefix_AAAA")]),
    ("UPDATE a SET v = 9 WHERE id = 3", "a", [(3, 2, 9, 1, 200, "commonprefix_SAME")]),
    ("DELETE FROM b WHERE id = 12", "b", [12]),
    ("DELETE FROM a WHERE id = 2", "a", [2]),
]


def test_a_residual_filters_the_join_output_through_churn(client, schema_name):
    """`ON k AND p` and `ON k WHERE p` select the same pairs after every epoch,
    and each residual shape is maintained through deltas that flip only its own
    outcome — an UPDATE on either side to a column that is not the key."""
    sn = schema_name
    pair = "SELECT a.id AS aid, b.id AS bid FROM a JOIN b"
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT, "
        "lo BIGINT NOT NULL, x BIGINT NOT NULL, s TEXT NOT NULL); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL, "
        "hi BIGINT NOT NULL, y BIGINT NOT NULL, s TEXT NOT NULL); "
        + "; ".join(f"CREATE VIEW on_{n} AS {pair} ON a.k = b.k AND ({p}); "
                    f"CREATE VIEW where_{n} AS {pair} ON a.k = b.k WHERE {p}"
                    for n, (p, _) in _PREDICATES.items()) + "; "
        + "; ".join(f"CREATE VIEW {n} AS {pair} ON {on}" for n, (on, _) in _RESIDUALS.items()),
        schema_name=sn)

    a, b = {}, {}
    for sql, table, changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        state, cols = (a, _A) if table == "a" else (b, _B)
        for change in changes:
            if isinstance(change, int):
                del state[change]
            else:
                state[change[0]] = dict(zip(cols, change))

        def pairs(on):
            return {(ar["id"], br["id"]): 1 for ar in a.values() for br in b.values() if on(ar, br)}

        for n, (_, p) in _PREDICATES.items():
            want = pairs(lambda ar, br, p=p: ar["k"] == br["k"] and p(ar, br))
            for form in ("on", "where"):
                assert bag(scanned(client, sn, f"{form}_{n}"), "aid", "bid") == want, (sql, form, n)
        for n, (_, on) in _RESIDUALS.items():
            assert bag(scanned(client, sn, n), "aid", "bid") == pairs(on), (sql, n)
