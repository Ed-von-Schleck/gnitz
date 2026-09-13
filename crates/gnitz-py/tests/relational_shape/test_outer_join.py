"""Outer joins — LEFT, RIGHT and FULL — over an equality key, a band and a pure
range, and what a WHERE, a reduce or a cut does above one.

There is no fused outer opcode. `LEFT JOIN = inner ∪ null_extend(ν)`, where `ν`
is the unmatched preserved rows at their true multiplicity: per preserved
identity `x` at weight `w_A ≥ 0`, and `S ≥ 0` the summed other-side weight it
matches, `ν(x) = w_A · [S = 0]`. RIGHT swaps the sides and FULL runs both. An
equi or band ν subtracts the matched preserved rows from the unfiltered
preserved input; a pure range decides existence from one threshold,
`∃b. a.x < b.y ⟺ a.x < MAX(b.y)`, and supports LEFT only.

The expectation is the outer join's own definition over the rows the test
wrote. A wrong ν is a spurious weight-`w−1` null-fill, a broadcast that fills
per worker is weight W, and a matched row's leftover `(x, NULL)` tombstone is
an extra row — each read as correct by a row count. The bag-valued preserved
side is test_bag_valued_input.py's.

Run with GNITZ_WORKERS=4: ν must cancel per worker, before the output exchange.
"""
import operator
from collections import Counter

from _read import bag, scanned


def _cmp(op):
    """`op` under SQL 3VL: a comparison against NULL admits nothing."""
    return lambda l, r: l is not None and r is not None and op(l, r)


_EQ, _LE, _LT, _GT = (_cmp(o) for o in (operator.eq, operator.le, operator.lt, operator.gt))


def _join(a, b, on, kind):
    """`a <kind> JOIN b ON on` as (a row, b row) pairs, the absent side None."""
    out = [(ar, br) for ar in a for br in b if on(ar, br)]
    if kind in ("LEFT", "FULL"):
        out += [(ar, None) for ar in a if not any(on(ar, br) for br in b)]
    if kind in ("RIGHT", "FULL"):
        out += [(None, br) for br in b if not any(on(ar, br) for ar in a)]
    return out


def _col(row, name):
    return None if row is None else row[name]


# name -> (SQL ON, the same predicate over a and b rows).
_ON = {
    "eq": ("a.k = b.k", lambda ar, br: _EQ(ar["k"], br["k"])),
    "band": ("a.k = b.k AND a.x <= b.y", lambda ar, br: _EQ(ar["k"], br["k"]) and _LE(ar["x"], br["y"])),
    "pair": ("a.k = b.k AND a.x = b.y", lambda ar, br: _EQ(ar["k"], br["k"]) and _EQ(ar["x"], br["y"])),
    **{f"range_{n}": (f"a.x {op} b.y", lambda ar, br, f=_cmp(f): f(ar["x"], br["y"]))
       for n, op, f in (("lt", "<", operator.lt), ("le", "<=", operator.le),
                        ("gt", ">", operator.gt), ("ge", ">=", operator.ge))},
}

# view -> (ON, kind, SQL WHERE or None, the same filter over the output pair).
_VIEWS = {
    **{f"eq_{k.lower()}": ("eq", k, None, None) for k in ("INNER", "LEFT", "RIGHT", "FULL")},
    **{f"band_{k.lower()}": ("band", k, None, None) for k in ("LEFT", "RIGHT", "FULL")},
    "pair_left": ("pair", "LEFT", None, None),
    **{f"{on}_left": (on, "LEFT", None, None) for on in _ON if on.startswith("range_")},
    # The WHERE is one 3VL filter over the post-null-fill output: a preserved-side
    # predicate keeps the fills that pass, an other-side one drops every fill, and
    # `IS NULL` over an other-side key selects exactly the unmatched rows.
    "eq_left_where_a": ("eq", "LEFT", "a.x > 15", lambda ar, br: _GT(ar["x"], 15)),
    "eq_left_where_b": ("eq", "LEFT", "b.y > 0", lambda ar, br: _GT(_col(br, "y"), 0)),
    "eq_left_unmatched": ("eq", "LEFT", "b.id IS NULL", lambda ar, br: br is None),
    "eq_full_where_a": ("eq", "FULL", "a.id2 IS NOT NULL", lambda ar, br: ar is not None),
    "band_left_where_a": ("band", "LEFT", "a.x > 15", lambda ar, br: _GT(ar["x"], 15)),
    "range_lt_left_where_a": ("range_lt", "LEFT", "a.x > 15", lambda ar, br: _GT(ar["x"], 15)),
}

_OUT = "a.id2 AS aid, a.s AS s, a.note AS note, b.id AS bid"

_DEL = object()

# (statement, changes to a by id2, changes to b by id) — rows as column dicts,
# `_DEL` deleting the key. Every `a` row has id1 = 0.
_CHURN = [
    # No b yet: every preserved row fills, a NULL key and a NULL range column alike.
    ("INSERT INTO a VALUES (0, 1, 1, 10, 'alpha', 7), (0, 2, 2, 20, 'beta', NULL), "
     "(0, 3, NULL, 30, 'gamma', 42), (0, 4, 3, NULL, 'delta', NULL)",
     {1: (1, 10, "alpha", 7), 2: (2, 20, "beta", None), 3: (None, 30, "gamma", 42),
      4: (3, None, "delta", None)}, {}),
    # b rows no key matches; b(2) is still a range match for `>`.
    ("INSERT INTO b VALUES (1, NULL, NULL), (2, NULL, 5)", {}, {1: (None, None), 2: (None, 5)}),
    # a(1)'s first match retracts its fill in the epoch the pair appears.
    ("INSERT INTO b VALUES (3, 1, 50)", {}, {3: (1, 50)}),
    # A second match leaves a(1) matched at weight 1; b(6) completes a(2)'s k=2 key.
    ("INSERT INTO b VALUES (4, 1, 15), (6, 2, 20)", {}, {4: (1, 15), 6: (2, 20)}),
    # The extreme `y`, and a b row no a row matches.
    ("INSERT INTO b VALUES (5, 9, 100)", {}, {5: (9, 100)}),
    # Deleting the extreme moves the pure-range threshold; the next b does not.
    ("DELETE FROM b WHERE id = 5", {}, {5: _DEL}),
    ("DELETE FROM b WHERE id = 4", {}, {4: _DEL}),
    # A matched preserved row leaves no `(x, NULL)` tombstone.
    ("DELETE FROM a WHERE id1 = 0 AND id2 = 1", {1: _DEL}, {}),
    # b is seeded, so a new a's match is decided in the epoch it arrives.
    ("INSERT INTO a VALUES (0, 5, 1, 12, 'epsilon', 3)", {5: (1, 12, "epsilon", 3)}, {}),
    # A left-only and a right-only fill that both pack their pair key to zeros,
    # told apart only by their null bitmaps.
    ("INSERT INTO a VALUES (0, 0, 77, 1000, 'zero', NULL)", {0: (77, 1000, "zero", None)}, {}),
    ("INSERT INTO b VALUES (0, 78, 1)", {}, {0: (78, 1)}),
]


def test_an_outer_join_null_fills_exactly_its_unmatched_rows_through_churn(client, schema_name):
    """Every orientation and shape over one churn: a preserved row null-fills
    once while nothing matches it — however many rows match its partner, and
    whether its key is absent or NULL — retracts the fill in the epoch a match
    appears, and restores it when the last match leaves. The pure-range operators
    each pick their own threshold (`< <=` against MAX, `> >=` against MIN), with
    the exact-boundary rows in the data. Every view carries a string and a
    nullable preserved payload, so a fill must carry the preserved row byte for
    byte. Above the join, a reduce groups the fills as a NULL group, a DISTINCT
    holds one NULL that leaves with its last carrier, a cut segment emits the
    fills itself, and an INNER step keyed through a null-filled column matches
    nothing — a NULL key and a residual `NULL = x` both drop the row."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id1 BIGINT NOT NULL, id2 BIGINT NOT NULL, k BIGINT, x BIGINT, "
        "s TEXT NOT NULL, note BIGINT, PRIMARY KEY (id1, id2)); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, y BIGINT); "
        "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL); "
        "INSERT INTO c VALUES (1, 50), (2, 5), (3, 0); "
        + "; ".join(f"CREATE VIEW {name} AS SELECT {_OUT} FROM a {kind} JOIN b ON {_ON[on][0]}"
                    + (f" WHERE {where}" if where else "")
                    for name, (on, kind, where, _) in _VIEWS.items()) + "; "
        "CREATE VIEW grouped AS SELECT b.y AS yy, COUNT(*) AS n FROM a LEFT JOIN b ON a.k = b.k "
        "WHERE a.x > 15 GROUP BY b.y; "
        "CREATE VIEW band_grouped AS SELECT b.y AS yy, COUNT(*) AS n "
        "FROM a LEFT JOIN b ON a.k = b.k AND a.x <= b.y WHERE a.x > 15 GROUP BY b.y; "
        "CREATE VIEW range_distinct AS SELECT DISTINCT b.id AS bid FROM a LEFT JOIN b ON a.x < b.y "
        "WHERE a.x > 15; "
        + "; ".join(f"CREATE VIEW cut_{on} AS WITH d AS (SELECT a.id2 AS aid, b.id AS bid "
                    f"FROM a LEFT JOIN b ON {_ON[on][0]}) SELECT aid, bid FROM d"
                    for on in ("band", "range_lt")) + "; "
        "CREATE VIEW promoted AS SELECT a.id2 AS aid, c.id AS cid "
        "FROM a LEFT JOIN b ON a.k = b.k JOIN c ON a.id2 = c.id WHERE b.y = c.w",
        schema_name=sn)
    c = {1: 50, 2: 5, 3: 0}

    a, b = {}, {}
    for sql, a_changes, b_changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        for state, changes, cols in ((a, a_changes, ("k", "x", "s", "note")), (b, b_changes, ("k", "y"))):
            for key, row in changes.items():
                if row is _DEL:
                    del state[key]
                else:
                    state[key] = {"id": key, **dict(zip(cols, row))}
        A, B = list(a.values()), list(b.values())

        for name, (on, kind, _, where) in _VIEWS.items():
            assert bag(scanned(client, sn, name), "aid", "s", "note", "bid") == Counter(
                (_col(ar, "id"), _col(ar, "s"), _col(ar, "note"), _col(br, "id"))
                for ar, br in _join(A, B, _ON[on][1], kind) if where is None or where(ar, br)), (sql, name)
        for name, on in (("grouped", "eq"), ("band_grouped", "band")):
            assert bag(scanned(client, sn, name), "yy", "n") == dict.fromkeys(Counter(
                _col(br, "y") for ar, br in _join(A, B, _ON[on][1], "LEFT") if _GT(ar["x"], 15)).items(), 1), \
                (sql, name)
        assert bag(scanned(client, sn, "range_distinct"), "bid") == {
            (_col(br, "id"),): 1 for ar, br in _join(A, B, _ON["range_lt"][1], "LEFT") if _GT(ar["x"], 15)}, sql
        for on in ("band", "range_lt"):
            assert bag(scanned(client, sn, f"cut_{on}"), "aid", "bid") == Counter(
                (ar["id"], _col(br, "id")) for ar, br in _join(A, B, _ON[on][1], "LEFT")), (sql, on)
        assert bag(scanned(client, sn, "promoted"), "aid", "cid") == Counter(
            (ar["id"], ar["id"]) for ar, br in _join(A, B, _ON["eq"][1], "LEFT")
            if ar["id"] in c and _EQ(_col(br, "y"), c[ar["id"]])), sql
