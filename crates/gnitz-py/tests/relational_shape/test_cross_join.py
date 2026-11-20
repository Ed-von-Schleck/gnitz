"""The keyless (cross) join in CREATE VIEW.

A keyless step is INNER only: its output PK is the pair `[a.pk…, b.pk…]`, a
residual filters the product, and the product is computed partition-locally under
a broadcast of the delta, with no second exchange. Every epoch emits
`|Δ| × |other side|` rows.

Because each delta is broadcast and paired, on every worker, with that worker's
slice of the other side, every check here is a weighted bag: a row set reads a
W× duplication as correct.

Run with GNITZ_WORKERS=4 — the broadcast and the trace's own-PK exchange are what
keep the product from multiplying.
"""
from _read import bag, scanned
from _sql import churn, inserting

# view -> (FROM clause, what it admits of (t.v, u.w)). `CROSS JOIN`, the comma
# and an ON comparing no column across the sides are one keyless step; a residual
# in the ON or the WHERE filters the product rather than keying it.
_SPELLINGS = {
    "cx": ("FROM t CROSS JOIN u", lambda v, w: True),
    "cm": ("FROM t, u", lambda v, w: True),
    "c1": ("FROM t INNER JOIN u ON 1 = 1", lambda v, w: True),
    "cr": ("FROM t JOIN u ON t.v <> u.w", lambda v, w: v != w),
    "cw": ("FROM t, u WHERE t.v <> u.w", lambda v, w: v != w),
    "cf": ("FROM t CROSS JOIN u WHERE t.v = 0", lambda v, w: v == 0),
    # A one-sided WHERE filters its side's input before the product.
    "cu": ("FROM t, u WHERE u.w > 0", lambda v, w: w > 0),
    "cb": ("FROM t, u WHERE t.v > 0 AND u.w < 5 AND t.v <> u.w", lambda v, w: v > 0 and w < 5 and v != w),
}

_LONG = "two-one-" + "x" * 40

# (statement, table, {pk: row}); a `None` row deletes the pk.
_CHURN = [
    (f"INSERT INTO c VALUES (1, 1, 'one-one'), (1, 2, 'one-two'), (2, 1, '{_LONG}')",
     "c", {(1, 1): "one-one", (1, 2): "one-two", (2, 1): _LONG}),
    inserting("t", {1: 1, 2: 2, 3: 0}),
    inserting("u", {10: 0, 20: 1}),
    inserting("t", {4: 4}),
    ("DELETE FROM t WHERE id = 2", "t", {2: None}),
    inserting("u", {30: 1}),
    ("UPDATE u SET w = 9 WHERE id = 10", "u", {10: 9}),
    ("UPDATE t SET v = 7 WHERE id = 3", "t", {3: 7}),
    ("DELETE FROM c WHERE a = 1 AND b = 2", "c", {(1, 2): None}),
    ("DELETE FROM u", "u", {10: None, 20: None, 30: None}),
    inserting("u", {40: 4}),
]


def test_each_delta_adds_or_retracts_exactly_its_pairs(client):
    """Every keyless shape over one churn: the spellings of one step; a table
    paired with itself, whose second copy is wrapped in a segment so the two
    inputs are distinct sources; a three-way product, whose inner product is cut
    into a segment keying the outer step; a compound-PK side, whose three pair-PK
    slots stay hidden under `SELECT *` while strings past the inline length ride
    through; and a GROUP BY over the product. Each delta, on any side, adds or
    retracts exactly the pairs it forms with the other sides' current rows."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL); "
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL); "
        "CREATE TABLE c (a BIGINT NOT NULL, b BIGINT NOT NULL, s TEXT NOT NULL, PRIMARY KEY (a, b)); "
        + "; ".join(f"CREATE VIEW {n} AS SELECT t.id AS tid, t.v AS tv, u.id AS uid, u.w AS uw {body}"
                    for n, (body, _) in _SPELLINGS.items()) + "; "
        "CREATE VIEW sq AS SELECT x.id AS xid, y.id AS yid FROM t x, t y; "
        "CREATE VIEW offdiag AS SELECT x.id AS xid, y.id AS yid FROM t x CROSS JOIN t y "
        "WHERE x.id <> y.id; "
        "CREATE VIEW tri AS SELECT t.id AS tid, u.id AS uid, c.a AS ca, c.b AS cb FROM t, u, c; "
        "CREATE VIEW star AS SELECT * FROM c CROSS JOIN u; "
        "CREATE VIEW x AS SELECT t.id AS tid, u.w AS uw FROM t CROSS JOIN u; "
        "CREATE VIEW g AS SELECT tid, COUNT(*) AS n, SUM(uw) AS s FROM x GROUP BY tid")

    state = {"t": {}, "u": {}, "c": {}}
    for sql in churn(client, state, _CHURN):
        t, u, c = state["t"], state["u"], state["c"]

        for name, (_, admits) in _SPELLINGS.items():
            assert bag(scanned(client, name), "tid", "tv", "uid", "uw") == {
                (ti, tv, ui, uw): 1 for ti, tv in t.items() for ui, uw in u.items()
                if admits(tv, uw)}, (sql, name)
        assert bag(scanned(client, "sq"), "xid", "yid") == \
            {(x, y): 1 for x in t for y in t}, sql
        assert bag(scanned(client, "offdiag"), "xid", "yid") == \
            {(x, y): 1 for x in t for y in t if x != y}, sql
        assert bag(scanned(client, "tri"), "tid", "uid", "ca", "cb") == \
            {(ti, ui, a, b): 1 for ti in t for ui in u for a, b in c}, sql
        assert bag(scanned(client, "star")) == {
            (a, b, s, ui, uw): 1 for (a, b), s in c.items() for ui, uw in u.items()}, sql
        assert bag(scanned(client, "g"), "tid", "n", "s") == (
            {(ti, len(u), sum(u.values())): 1 for ti in t} if u else {}), sql


# view -> (body, its columns, its rows from (m, a, c)): `m` is b's MAX, None once
# b is empty, `a` `{id: v}`, `c` a set of ids. The one-row side is the step's left
# operand.
_ONE_ROW_LEFT = {
    "lx": ("SELECT s.m, a.id AS aid FROM (SELECT MAX(w) AS m FROM b) s CROSS JOIN a", ("m", "aid"),
           lambda m, a, c: [(m, i) for i in a]),
    "lr": ("SELECT s.m, a.id AS aid FROM (SELECT MAX(w) AS m FROM b) s JOIN a ON s.m > a.v", ("m", "aid"),
           lambda m, a, c: [(m, i) for i, v in a.items() if m is not None and m > v]),
    "l3": ("SELECT s.m, a.id AS aid, c.id AS cid FROM (SELECT MAX(w) AS m FROM b) s, a, c", ("m", "aid", "cid"),
           lambda m, a, c: [(m, i, k) for i in a for k in c]),
}


def test_a_one_row_left_side_pairs_each_row_once(client):
    """A global aggregate on the left of a keyless or pure-range step is the side
    of at most one row: it is broadcast whole while the other side's rows stay
    on their PK's owner. Each view holds every pair exactly once, weight 1, as
    the other side churns, the aggregate's value moves, and its source empties
    — its ground row then pairs NULL — and fills again. A copy created after
    the load, a pure backfill, holds the same rows at the same weights."""
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL); "
        "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL); "
        + "; ".join(f"CREATE VIEW {n} AS {body}" for n, (body, _, _) in _ONE_ROW_LEFT.items()))

    def make_twins(client):
        client.execute_sql("; ".join(f"CREATE VIEW {n}_twin AS {body}" for n, (body, _, _) in _ONE_ROW_LEFT.items()))

    twins = False
    state = {"a": {}, "b": {}, "c": {}}
    for sql in churn(client, state, [
        (None, {}),
        inserting("a", {1: 3, 2: 8, 3: 5, 4: 12, 5: 0}),
        inserting("c", {10: 0, 20: 0}),
        inserting("b", {1: 6, 2: 4}),
        (make_twins, {}),
        # The other side churns under a fixed aggregate.
        inserting("a", {6: 1, 7: 20}),
        ("DELETE FROM a WHERE id = 2", "a", {2: None}),
        ("UPDATE a SET v = 2 WHERE id = 4", "a", {4: 2}),
        # The aggregate's value moves up, then back down.
        inserting("b", {3: 9}),
        ("UPDATE b SET w = 1 WHERE id = 3", "b", {3: 1}),
        ("DELETE FROM b WHERE id = 1", "b", {1: None}),
        inserting("c", {30: 0}),
        # Its source empties, then fills again.
        ("DELETE FROM b", "b", {2: None, 3: None}),
        inserting("a", {8: 4}),
        inserting("b", {4: 7}),
        ("DELETE FROM a WHERE id = 1 OR id = 7", "a", {1: None, 7: None}),
    ]):
        twins = twins or sql is make_twins
        b = state["b"]
        m = max(b.values()) if b else None
        for name, (_, cols, rows) in _ONE_ROW_LEFT.items():
            want = {r: 1 for r in rows(m, state["a"], state["c"])}
            assert bag(scanned(client, name), *cols) == want, (sql, name)
            if twins:
                assert bag(scanned(client, name + "_twin"), *cols) == want, (sql, name + "_twin")
