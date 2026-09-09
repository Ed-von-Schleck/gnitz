"""The keyless (cross) join in CREATE VIEW.

A keyless step is INNER only: its output PK is the pair `[a.pk…, b.pk…]`, a
residual filters the product, and the product is computed partition-locally under
a broadcast of the delta, with no second exchange. Every epoch emits
`|Δ| × |other side|` rows.

Because each delta is broadcast and paired, on every worker, with that worker's
slice of the other side, every check here is a weight-multiset comparison: a row
set reads a W× duplication as correct.

Run with GNITZ_WORKERS=4 — the broadcast and the trace's worker filter are what
keep the product from multiplying.
"""

from collections import Counter

import pytest
import _oracle as oracle

ROWS_T = [(i, i % 3) for i in range(1, 8)]        # (id, v)
ROWS_U = [(10 * i, i % 2) for i in range(1, 6)]   # (id, w)


@pytest.fixture
def tu(client, schema_name):
    """Empty `t(id, v)` / `u(id, w)` in a fresh schema."""
    for name, col in (("t", "v"), ("u", "w")):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, {col} BIGINT NOT NULL)",
            schema_name=schema_name)
    return schema_name


def _fill(client, sn, table, rows):
    client.execute_sql(
        f"INSERT INTO {table} VALUES " + ",".join(f"({a},{b})" for a, b in rows),
        schema_name=sn)


def _product(rows_t, rows_u, pred=None):
    """`{(t.id, u.id): 1}` over the pairs `pred` admits — the reference product."""
    return Counter({(t[0], u[0]): 1 for t in rows_t for u in rows_u
                    if pred is None or pred(t, u)})


def _vid(client, sn, name):
    return client.resolve_table(sn, name)[0]


def test_every_keyless_spelling_is_the_product(client, tu):
    """`CROSS JOIN`, the comma, and a `JOIN … ON` whose predicate compares no
    column across the two sides are one keyless INNER step; a residual on it — in
    the ON or in the WHERE — filters the product rather than keying it."""
    cases = {
        "cx": ("FROM t CROSS JOIN u", None),
        "cm": ("FROM t, u", None),
        "c1": ("FROM t INNER JOIN u ON 1 = 1", None),
        "cr": ("FROM t JOIN u ON t.v <> u.w", lambda t, u: t[1] != u[1]),
        "cw": ("FROM t, u WHERE t.v <> u.w", lambda t, u: t[1] != u[1]),
        "cf": ("FROM t CROSS JOIN u WHERE t.v = 0", lambda t, u: t[1] == 0),
    }
    for name, (body, _) in cases.items():
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT t.id AS tid, u.id AS uid {body}",
            schema_name=tu)
    _fill(client, tu, "t", ROWS_T)
    _fill(client, tu, "u", ROWS_U)

    for name, (_, pred) in cases.items():
        oracle.assert_view_matches(client, _vid(client, tu, name), ["tid", "uid"],
                                   _product(ROWS_T, ROWS_U, pred), name)


@pytest.mark.parametrize("kind", ["LEFT", "RIGHT", "FULL"])
def test_a_keyless_step_is_inner_only(client, tu, kind):
    """Only an INNER step may be keyless. An outer step decides its null-fill from
    a key, and a keyless one would need a global "is the other side empty" witness
    no emitter builds — so this is refused at plan time rather than compiled into a
    view that quietly differs from the product."""
    with pytest.raises(Exception):
        client.execute_sql(
            f"CREATE VIEW v AS SELECT t.id AS tid, u.id AS uid "
            f"FROM t {kind} JOIN u ON 1 = 1", schema_name=tu)
    with pytest.raises(Exception):
        client.resolve_table(tu, "v")


def test_the_output_pk_is_the_pair_of_source_pks_at_their_own_arity(client, schema_name):
    """The output PK is `[a.pk…, b.pk…]`, so a two-column PK on one side gives a
    three-slot pair. Those slots are hidden, so `SELECT *` presents every user
    column of both sides and none of them, and the payload rides through untouched
    — strings past the German-string inline length included."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v TEXT NOT NULL, "
        "PRIMARY KEY (a, b))", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE u (uid BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT * FROM t CROSS JOIN u", schema_name=sn)
    rows_t = [(1, 1, "one-one"), (1, 2, "one-two"), (2, 1, "two-one-" + "x" * 40)]
    rows_u = [(7, "seven"), (8, "eight-" + "y" * 40)]
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({a},{b},'{v}')" for a, b, v in rows_t),
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO u VALUES " + ",".join(f"({i},'{n}')" for i, n in rows_u),
        schema_name=sn)

    vid = _vid(client, sn, "v")
    presented = client.scan(vid).mappings()
    assert presented and all(set(r) == {"a", "b", "v", "uid", "name"} for r in presented)
    oracle.assert_view_matches(
        client, vid, ["a", "b", "v", "uid", "name"],
        Counter({(a, b, v, i, n): 1 for a, b, v in rows_t for i, n in rows_u}))


def test_incremental_changes_on_both_sides(client, tu):
    """Each delta, on either side, adds or retracts exactly the pairs it forms with
    the other side's current rows: insert, delete and update (a retraction plus an
    insertion under one PK) on each side in turn."""
    client.execute_sql(
        "CREATE VIEW v AS SELECT t.id AS tid, t.v AS tv, u.id AS uid, u.w AS uw "
        "FROM t CROSS JOIN u", schema_name=tu)
    vid = _vid(client, tu, "v")
    t, u = {}, {}

    def check(ctx):
        exp = Counter({(ti, tv, ui, uw): 1
                       for ti, tv in t.items() for ui, uw in u.items()})
        oracle.assert_view_matches(client, vid, ["tid", "tv", "uid", "uw"], exp, ctx)

    check("both empty")
    client.execute_sql("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)", schema_name=tu)
    t.update({1: 1, 2: 2, 3: 3})
    check("t alone pairs with nothing")
    client.execute_sql("INSERT INTO u VALUES (10, 0), (20, 0)", schema_name=tu)
    u.update({10: 0, 20: 0})
    check("first product")
    client.execute_sql("INSERT INTO t VALUES (4, 4)", schema_name=tu)
    t[4] = 4
    check("a t row pairs with every u row")
    client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=tu)
    del t[2]
    check("a t deletion retracts its pairs")
    client.execute_sql("INSERT INTO u VALUES (30, 1)", schema_name=tu)
    u[30] = 1
    check("a u row pairs with every t row")
    client.execute_sql("UPDATE u SET w = 9 WHERE id = 10", schema_name=tu)
    u[10] = 9
    check("a u update swaps its pairs")
    client.execute_sql("UPDATE t SET v = 7 WHERE id = 3", schema_name=tu)
    t[3] = 7
    check("a t update swaps its pairs")
    client.execute_sql("DELETE FROM u", schema_name=tu)
    u.clear()
    check("an emptied side empties the product")
    client.execute_sql("INSERT INTO u VALUES (40, 4)", schema_name=tu)
    u[40] = 4
    check("and refilling it rebuilds the product")


def test_backfill_over_existing_rows(client, tu):
    """A view created over populated tables backfills the whole product, and keeps
    maintaining it afterwards."""
    _fill(client, tu, "t", ROWS_T)
    _fill(client, tu, "u", ROWS_U)
    client.execute_sql(
        "CREATE VIEW v AS SELECT t.id AS tid, u.id AS uid FROM t, u", schema_name=tu)
    vid = _vid(client, tu, "v")
    oracle.assert_view_matches(client, vid, ["tid", "uid"],
                               _product(ROWS_T, ROWS_U), "backfill")

    client.execute_sql("INSERT INTO u VALUES (99, 1)", schema_name=tu)
    oracle.assert_view_matches(client, vid, ["tid", "uid"],
                               _product(ROWS_T, ROWS_U + [(99, 1)]), "after")


def test_self_product(client, tu):
    """`FROM t a, t b` pairs a table with itself: the second copy is wrapped in a
    segment so the two inputs are distinct sources, and the product holds every
    ordered pair including each row with itself."""
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id AS x, b.id AS y FROM t a, t b", schema_name=tu)
    client.execute_sql(
        "CREATE VIEW w AS SELECT a.id AS x, b.id AS y FROM t a CROSS JOIN t b "
        "WHERE a.id <> b.id", schema_name=tu)
    _fill(client, tu, "t", ROWS_T)
    vid, wid = _vid(client, tu, "v"), _vid(client, tu, "w")

    def off_diagonal(a, b):
        return a[0] != b[0]

    oracle.assert_view_matches(client, vid, ["x", "y"], _product(ROWS_T, ROWS_T), "v")
    oracle.assert_view_matches(client, wid, ["x", "y"],
                               _product(ROWS_T, ROWS_T, off_diagonal), "w")

    client.execute_sql("DELETE FROM t WHERE id = 4", schema_name=tu)
    rows = [r for r in ROWS_T if r[0] != 4]
    oracle.assert_view_matches(client, vid, ["x", "y"],
                               _product(rows, rows), "v after delete")
    oracle.assert_view_matches(client, wid, ["x", "y"],
                               _product(rows, rows, off_diagonal), "w after delete")


def test_three_way_product(client, schema_name):
    """`FROM a, b, c` is two keyless steps: the inner product is cut into a segment
    whose pair-PK becomes the left key of the outer one."""
    sn = schema_name
    for name in ("a", "b", "c"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid, c.id AS cid FROM a, b, c",
        schema_name=sn)
    for name, ids in (("a", (1, 2, 3)), ("b", (10, 20)), ("c", (100, 200, 300, 400))):
        _fill(client, sn, name, [(i, 0) for i in ids])
    vid = _vid(client, sn, "v")

    oracle.assert_view_matches(client, vid, ["aid", "bid", "cid"], Counter({
        (x, y, z): 1 for x in (1, 2, 3) for y in (10, 20)
        for z in (100, 200, 300, 400)}))
    client.execute_sql("DELETE FROM b WHERE id = 10", schema_name=sn)
    oracle.assert_view_matches(client, vid, ["aid", "bid", "cid"], Counter({
        (x, 20, z): 1 for x in (1, 2, 3) for z in (100, 200, 300, 400)}), "after delete")


def test_weights_multiply_over_a_bag_valued_side(client, schema_name):
    """A side whose rows carry weight 2 (a `UNION ALL` of two copies) pairs at
    weight 2: the join multiplies weights, it does not count matches."""
    sn = schema_name
    for name, col in (("a1", "k"), ("a2", "k"), ("u", "w")):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, {col} BIGINT NOT NULL)",
            schema_name=sn)
    client.execute_sql(
        "CREATE VIEW bag AS SELECT k FROM a1 UNION ALL SELECT k FROM a2", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT bag.k AS k, u.id AS uid FROM bag CROSS JOIN u",
        schema_name=sn)
    _fill(client, sn, "a1", [(1, 5), (2, 6)])
    _fill(client, sn, "a2", [(1, 5)])
    _fill(client, sn, "u", [(10, 0), (20, 0)])

    oracle.assert_view_matches(client, _vid(client, sn, "v"), ["k", "uid"], Counter(
        {(5, 10): 2, (5, 20): 2, (6, 10): 1, (6, 20): 1}))


def test_a_grouped_view_over_the_product(client, tu):
    """A view over a cross view reads its pair-PK-keyed rows like any other
    relation: the per-t counts are the other side's size."""
    client.execute_sql(
        "CREATE VIEW x AS SELECT t.id AS tid, u.w AS uw FROM t CROSS JOIN u",
        schema_name=tu)
    client.execute_sql(
        "CREATE VIEW g AS SELECT tid, COUNT(*) AS n, SUM(uw) AS s FROM x GROUP BY tid",
        schema_name=tu)
    _fill(client, tu, "t", ROWS_T)
    _fill(client, tu, "u", ROWS_U)
    gid = _vid(client, tu, "g")

    def want(rows_u):
        total = sum(w for _, w in rows_u)
        return Counter({(ti, len(rows_u), total): 1 for ti, _ in ROWS_T})

    oracle.assert_view_matches(client, gid, ["tid", "n", "s"], want(ROWS_U))
    client.execute_sql("DELETE FROM u WHERE id = 10", schema_name=tu)
    oracle.assert_view_matches(client, gid, ["tid", "n", "s"],
                               want([r for r in ROWS_U if r[0] != 10]), "after delete")
