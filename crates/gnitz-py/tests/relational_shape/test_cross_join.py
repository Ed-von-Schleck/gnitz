"""The keyless (cross) join in CREATE VIEW.

A keyless step is INNER only: its output PK is the pair `[a.pk…, b.pk…]`, a
residual filters the product, and the product is computed partition-locally under
a broadcast of the delta, with no second exchange. Every epoch emits
`|Δ| × |other side|` rows.

Because each delta is broadcast and paired, on every worker, with that worker's
slice of the other side, every check here is a weighted bag: a row set reads a
W× duplication as correct.

Run with GNITZ_WORKERS=4 — the broadcast and the trace's worker filter are what
keep the product from multiplying.
"""
from _read import bag, scanned

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
}

_LONG = "two-one-" + "x" * 40

# (statement, table, {pk: row}); a `None` row deletes the pk.
_CHURN = [
    (f"INSERT INTO c VALUES (1, 1, 'one-one'), (1, 2, 'one-two'), (2, 1, '{_LONG}')",
     "c", {(1, 1): "one-one", (1, 2): "one-two", (2, 1): _LONG}),
    ("INSERT INTO t VALUES (1, 1), (2, 2), (3, 0)", "t", {1: 1, 2: 2, 3: 0}),
    ("INSERT INTO u VALUES (10, 0), (20, 1)", "u", {10: 0, 20: 1}),
    ("INSERT INTO t VALUES (4, 4)", "t", {4: 4}),
    ("DELETE FROM t WHERE id = 2", "t", {2: None}),
    ("INSERT INTO u VALUES (30, 1)", "u", {30: 1}),
    ("UPDATE u SET w = 9 WHERE id = 10", "u", {10: 9}),
    ("UPDATE t SET v = 7 WHERE id = 3", "t", {3: 7}),
    ("DELETE FROM c WHERE a = 1 AND b = 2", "c", {(1, 2): None}),
    ("DELETE FROM u", "u", {10: None, 20: None, 30: None}),
    ("INSERT INTO u VALUES (40, 4)", "u", {40: 4}),
]


def test_each_delta_adds_or_retracts_exactly_its_pairs(client, schema_name):
    """Every keyless shape over one churn: the spellings of one step; a table
    paired with itself, whose second copy is wrapped in a segment so the two
    inputs are distinct sources; a three-way product, whose inner product is cut
    into a segment keying the outer step; a compound-PK side, whose three pair-PK
    slots stay hidden under `SELECT *` while strings past the inline length ride
    through; and a GROUP BY over the product. Each delta, on any side, adds or
    retracts exactly the pairs it forms with the other sides' current rows."""
    sn = schema_name
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
        "CREATE VIEW g AS SELECT tid, COUNT(*) AS n, SUM(uw) AS s FROM x GROUP BY tid",
        schema_name=sn)

    state = {"t": {}, "u": {}, "c": {}}
    for sql, table, changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        for pk, row in changes.items():
            if row is None:
                del state[table][pk]
            else:
                state[table][pk] = row
        t, u, c = state["t"], state["u"], state["c"]

        for name, (_, admits) in _SPELLINGS.items():
            assert bag(scanned(client, sn, name), "tid", "tv", "uid", "uw") == {
                (ti, tv, ui, uw): 1 for ti, tv in t.items() for ui, uw in u.items()
                if admits(tv, uw)}, (sql, name)
        assert bag(scanned(client, sn, "sq"), "xid", "yid") == \
            {(x, y): 1 for x in t for y in t}, sql
        assert bag(scanned(client, sn, "offdiag"), "xid", "yid") == \
            {(x, y): 1 for x in t for y in t if x != y}, sql
        assert bag(scanned(client, sn, "tri"), "tid", "uid", "ca", "cb") == \
            {(ti, ui, a, b): 1 for ti in t for ui in u for a, b in c}, sql
        star = scanned(client, sn, "star")
        assert bag(star, "a", "b", "s", "id", "w") == {
            (a, b, s, ui, uw): 1 for (a, b), s in c.items() for ui, uw in u.items()}, sql
        assert all(set(r._fields) == {"a", "b", "s", "id", "w"} for r in star), sql
        assert bag(scanned(client, sn, "g"), "tid", "n", "s") == (
            {(ti, len(u), sum(u.values())): 1 for ti in t} if u else {}), sql
