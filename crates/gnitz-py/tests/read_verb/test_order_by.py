"""The ORDER BY / OFFSET / LIMIT read bound, over a base table and over a view.

Two properties carry the file. An ORDER BY key absent from the SELECT list rides
the reply as a hidden column; and the sink counts LIMIT/OFFSET by
logical **multiplicity** — summed weight — not by Z-set entries, so a cut lands
in the same place however a bag is split across workers and entries.

Under a LIMIT the worker top-k discards rows before the client ever sees them,
so the two comparators must agree exactly: a divergence drops rows rather than
merely reordering them. That is why the TEXT-with-NULLs and float cases pin
values and not just an order.

What the bound refuses is pinned in the planner's `gnitz-sql/tests/plan_read.rs`.
"""

import pytest
from _read import bag, rows
from _sql import insert


@pytest.fixture(scope="module")
def src(module_client):
    """A connection holding the read-only sources for every case: `t (id, v, w, a)`, a TEXT column with
    NULLs, a DOUBLE column, and `dup` — a `UNION ALL` of two identical tables,
    so every row carries logical multiplicity 2."""
    conn = module_client
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT, w BIGINT, a BIGINT NOT NULL); "
        "CREATE TABLE names (id BIGINT PRIMARY KEY, name TEXT); "
        "CREATE TABLE floats (id BIGINT PRIMARY KEY, x DOUBLE); "
        "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "CREATE VIEW dup AS SELECT * FROM a UNION ALL SELECT * FROM b")
    insert(conn, "t", [(1, 30, 10, 1), (2, 10, None, 1), (3, 20, 20, 2), (4, 40, 5, 2),
                           (5, 50, 15, 3)])
    insert(conn, "names", [(1, "pear"), (2, "apple"), (3, None), (4, "banana"),
                               (5, "apricot"), (6, None), (7, "cherry")])
    insert(conn, "floats", [(1, 2.5), (2, -1.75), (3, 0.0), (4, -3.25), (5, 1.25)])
    for name in ("a", "b"):
        insert(conn, name, [(1, 10), (2, 20)])
    return conn


# ---------------------------------------------------------------------------
# Base-table ordering, and sort before projection
# ---------------------------------------------------------------------------


_ALL = ["id", "v", "w", "a"]


@pytest.mark.parametrize("q,want,fields", [
    ("SELECT * FROM t ORDER BY v", [2, 3, 1, 4, 5], _ALL),
    ("SELECT * FROM t ORDER BY v DESC", [5, 4, 1, 3, 2], _ALL),
    # NULLS placement is absolute, not value-flipped: ASC defaults to LAST and
    # DESC to FIRST, and an explicit clause overrides either.
    ("SELECT * FROM t ORDER BY w", [4, 1, 5, 3, 2], _ALL),
    ("SELECT * FROM t ORDER BY w DESC", [2, 3, 5, 1, 4], _ALL),
    ("SELECT * FROM t ORDER BY w ASC NULLS FIRST", [2, 4, 1, 5, 3], _ALL),
    ("SELECT * FROM t ORDER BY w DESC NULLS LAST", [3, 5, 1, 4, 2], _ALL),
    ("SELECT * FROM t ORDER BY a, v", [2, 1, 3, 4, 5], _ALL),
    ("SELECT * FROM t ORDER BY a ASC, v DESC", [1, 2, 4, 3, 5], _ALL),
    ("SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 1", [2, 3], _ALL),
    ("SELECT * FROM t ORDER BY id LIMIT 1, 2", [2, 3], _ALL),     # MySQL `LIMIT off, lim`
    ("SELECT * FROM t ORDER BY id OFFSET 3", [4, 5], _ALL),
    ("SELECT * FROM t ORDER BY id OFFSET 99", [], _ALL),          # past the end
    ("SELECT * FROM t ORDER BY id LIMIT 0", [], _ALL),
    ("SELECT * FROM t ORDER BY id DESC LIMIT 2", [5, 4], _ALL),
    # A PK-set gather cut by the ORDER BY comparator, not by which keys the
    # gather answered first.
    ("SELECT * FROM t WHERE id IN (1, 2, 3, 4, 5) ORDER BY v LIMIT 2", [2, 3], _ALL),
    ("SELECT * FROM t WHERE id IN (1, 2, 3, 4, 5) ORDER BY v LIMIT 2 OFFSET 1", [3, 1], _ALL),
    # A key outside the projection still orders the result, by the output alias
    # or by the source name even when it was aliased away.
    ("SELECT id FROM t ORDER BY v", [2, 3, 1, 4, 5], ["id"]),
    ("SELECT id, v AS foo FROM t ORDER BY foo", [2, 3, 1, 4, 5], ["id", "foo"]),
    ("SELECT id, v AS foo FROM t ORDER BY v DESC", [5, 4, 1, 3, 2], ["id", "foo"]),
    # An ORDER BY expression binds over the source like a SELECT item and rides
    # as a hidden column, so it never widens the presented shape.
    ("SELECT id FROM t ORDER BY 0 - v", [5, 4, 1, 3, 2], ["id"]),
    ("SELECT id FROM t ORDER BY v % 20, id DESC LIMIT 2", [4, 3], ["id"]),
])
def test_a_base_table_sort_and_window(src, q, want, fields):
    got = rows(src, q)
    assert [r.id for r in got] == want
    if got:
        assert list(got[0]._fields) == fields


# ---------------------------------------------------------------------------
# The worker top-k's comparator must be the client window's
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("q,want", [
    # German-string order and absolute NULL placement: DESC defaults to NULLS
    # FIRST, so the NULLs occupy the window; explicit NULLS LAST keeps them out.
    ("SELECT name FROM names ORDER BY name LIMIT 3", ["apple", "apricot", "banana"]),
    ("SELECT name FROM names ORDER BY name DESC LIMIT 3", [None, None, "pear"]),
    ("SELECT name FROM names ORDER BY name ASC NULLS LAST LIMIT 5",
     ["apple", "apricot", "banana", "cherry", "pear"]),
    # Negative, fractional and zero under the same total order on both sides.
    ("SELECT id FROM floats ORDER BY x LIMIT 3", [4, 2, 3]),
    ("SELECT id FROM floats ORDER BY x DESC LIMIT 2", [1, 5]),
])
def test_a_top_k_cuts_where_the_client_would(src, q, want):
    assert [r[0] for r in rows(src, q)] == want


# ---------------------------------------------------------------------------
# Views: multiplicity-correct cuts
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tail,want", [
    # LIMIT 2 = 2 logical rows, so only the smallest value survives; an
    # entry-counting LIMIT would spill into val=20.
    ("ORDER BY val LIMIT 2", {(10,): 2}),
    # LIMIT 3 straddles the boundary, clipping the boundary entry to weight 1.
    ("ORDER BY val LIMIT 3", {(10,): 2, (20,): 1}),
    # A window landing inside the weight-2 group.
    ("ORDER BY val LIMIT 1 OFFSET 1", {(10,): 1}),
    ("ORDER BY val DESC LIMIT 3", {(20,): 2, (10,): 1}),
    ("ORDER BY val", {(10,): 2, (20,): 2}),
])
def test_a_cut_over_a_bag_counts_logical_rows(src, tail, want):
    assert bag(rows(src, f"SELECT val FROM dup {tail}")) == want


def test_a_grouped_views_hidden_key_is_not_a_positional_column(client):
    """A GROUP BY view over a nullable column carries a hidden `_group_pk`, so
    positional ORDER BY must target the first VISIBLE column — and ORDER BY over
    the aggregate works."""
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cat BIGINT); "
        "CREATE VIEW v AS SELECT cat, COUNT(*) AS cnt FROM orders GROUP BY cat; "
        "INSERT INTO orders VALUES (1,10),(2,10),(3,10),(4,20),(5,30),(6,30)")
    assert [(r.cat, r.cnt) for r in rows(client, "SELECT * FROM v ORDER BY cnt DESC LIMIT 2")] \
        == [(10, 3), (30, 2)]
    assert [r.cat for r in rows(client, "SELECT * FROM v ORDER BY 1")] == [10, 20, 30]


# ---------------------------------------------------------------------------
# A cut in key order: each worker stops at its first rows
# ---------------------------------------------------------------------------


def test_a_cut_in_key_order_matches_a_full_sort(client):
    """Ordered ascending by a leading run of the key, each worker ships only the
    first rows of its own slice; their union must still hold the global window.
    Negative keys cross the sign-flipped key order, and `g` is a view."""
    client.execute_sql(
        "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY); "
        "CREATE TABLE c (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b)); "
        "CREATE VIEW g AS SELECT a, COUNT(*) AS n FROM c GROUP BY a")
    p_keys = [(i,) for i in range(-200, 201)]
    c_keys = [(a, b) for a in range(-10, 10) for b in range(-10, 10)]
    g_rows = [(a, 20) for a in range(-10, 10)]
    insert(client, "p", p_keys)
    insert(client, "c", c_keys)
    for q, want in [
        ("SELECT id FROM p ORDER BY id LIMIT 7", p_keys[:7]),
        ("SELECT id FROM p ORDER BY id LIMIT 5 OFFSET 11", p_keys[11:16]),
        ("SELECT id FROM p WHERE id > 50 ORDER BY id LIMIT 4", [(i,) for i in range(51, 55)]),
        ("SELECT id FROM p WHERE id % 7 = 0 ORDER BY id LIMIT 3", [(-196,), (-189,), (-182,)]),
        ("SELECT id FROM p WHERE id IN (-150, 3, 99, 7) ORDER BY id LIMIT 2", [(-150,), (3,)]),
        ("SELECT * FROM c ORDER BY a, b LIMIT 9 OFFSET 30", c_keys[30:39]),
        ("SELECT * FROM c WHERE a = 3 ORDER BY a, b LIMIT 4", [(3, b) for b in range(-10, -6)]),
        ("SELECT * FROM g ORDER BY a LIMIT 3 OFFSET 2", g_rows[2:5]),
    ]:
        assert "early-stop" in rows(client, "EXPLAIN " + q)[-1][0], q
        assert [tuple(r) for r in rows(client, q)] == want, q
    # Only the leading key column orders the cut, so its ties land anywhere.
    got = rows(client, "SELECT * FROM c ORDER BY a LIMIT 25")
    assert [r.a for r in got] == [-10] * 20 + [-9] * 5
    assert len({tuple(r) for r in got}) == 25


# ---------------------------------------------------------------------------
# A cut over many tied rows on every worker
# ---------------------------------------------------------------------------


def _sorted_by(data, keys):
    """`data` (dict rows) ordered by `keys` — `(column, desc, nulls_first)` each — ties
    in ascending `id`, the identity a cut breaks them by."""
    out = sorted(data, key=lambda r: r["id"])
    for col, desc, nulls_first in reversed(keys):
        nulls = [r for r in out if r[col] is None]
        # A reversed sort still keeps equal keys in their order.
        vals = sorted((r for r in out if r[col] is not None), key=lambda r: r[col], reverse=desc)
        out = nulls + vals if nulls_first else vals + nulls
    return out


def _window(entries, offset, limit):
    """`entries` — `(row, weight)` in order — cut to logical rows `[offset, offset + limit)`."""
    out, at = [], 0
    for row, w in entries:
        kept = min(at + w, offset + limit) - max(at, offset)
        if kept > 0:
            out.append((row, kept))
        at += w
    return out


def _entries(got, cols):
    """The result as `(values of cols, weight)` in order, one entry per run of equal rows."""
    out = []
    for r in got:
        key = tuple(getattr(r, c) for c in cols)
        if out and out[-1][0] == key:
            out[-1] = (key, out[-1][1] + r._weight)
        else:
            out.append((key, r._weight))
    return out


def test_a_cut_over_tied_keys_on_every_worker_keeps_the_sorted_prefix(client):
    """Each worker trims thousands of rows whose leading key ties heavily — 16 values and
    NULLs, strings sharing their first 8 bytes — to its own window, and the client cuts
    their union. The result is the sorted prefix weight for weight, over a table and over
    a `UNION ALL` view whose rows weigh 1 or 2, with a window that starts inside a row."""
    client.execute_sql(
        "CREATE TABLE big (id BIGINT NOT NULL PRIMARY KEY, v BIGINT, s TEXT); "
        "CREATE TABLE twice (id BIGINT NOT NULL PRIMARY KEY, v BIGINT, s TEXT); "
        "CREATE VIEW both AS SELECT * FROM big UNION ALL SELECT * FROM twice")
    data = [{"id": i,
             "v": None if i % 11 == 0 else (i * 7919) % 16,
             "s": None if i % 13 == 0 else f"user_000{(i * 104729) % 977:04d}"}
            for i in range(1, 6001)]
    as_rows = [(r["id"], r["v"], r["s"]) for r in data]
    for at in range(0, len(as_rows), 1000):
        insert(client, "big", as_rows[at:at + 1000])
        insert(client, "twice", [r for r in as_rows[at:at + 1000] if r[0] % 3 == 0])
    cols = ("id", "v", "s")
    values = lambda r: tuple(r[c] for c in cols)
    weight = lambda r: 2 if r["id"] % 3 == 0 else 1

    # `(ORDER BY, the keys it names, OFFSET, LIMIT)`; a cut breaks ties by ascending id.
    for order, keys, offset, limit in [
        ("v", [("v", False, False)], 0, 37),
        ("v DESC", [("v", True, True)], 500, 37),
        ("v NULLS FIRST", [("v", False, True)], 0, 700),
        ("v DESC NULLS LAST", [("v", True, False)], 5400, 300),
        ("s", [("s", False, False)], 0, 50),
        ("s DESC NULLS LAST", [("s", True, False)], 20, 50),
        ("s NULLS FIRST, v DESC", [("s", False, True), ("v", True, True)], 450, 40),
        ("v, id DESC", [("v", False, False), ("id", True, True)], 300, 100),
    ]:
        q = f"SELECT * FROM big ORDER BY {order} LIMIT {limit} OFFSET {offset}"
        want = [(values(r), 1) for r in _sorted_by(data, keys)[offset:offset + limit]]
        assert _entries(rows(client, q), cols) == want, q

    # Uncut, ties are open: the written keys must be total for the order to be pinned.
    for order, keys in [
        ("v, id", [("v", False, False)]),
        ("s DESC, id", [("s", True, True)]),
    ]:
        q = f"SELECT * FROM big ORDER BY {order}"
        assert _entries(rows(client, q), cols) == [(values(r), 1) for r in _sorted_by(data, keys)], q

    for order, keys in [
        ("v, id", [("v", False, False)]),
        ("s DESC, id", [("s", True, True)]),
    ]:
        full = [(r, weight(r)) for r in _sorted_by(data, keys)]
        # The window opens on the second logical row of the first weight-2 entry past 40.
        first = next(i for i, (_, w) in enumerate(full) if i >= 40 and w == 2)
        offset = sum(w for _, w in full[:first]) + 1
        for limit in (1, 2, 60):
            q = f"SELECT * FROM both ORDER BY {order} LIMIT {limit} OFFSET {offset}"
            want = [(values(r), w) for r, w in _window(full, offset, limit)]
            assert want[0][1] == 1, q
            assert _entries(rows(client, q), cols) == want, q
    # Ordered by the leading key alone, the view's ties break by its own identity; the
    # values the window holds are fixed all the same.
    full = [(r, weight(r)) for r in _sorted_by(data, [("v", False, False)])]
    got = rows(client, "SELECT v FROM both ORDER BY v LIMIT 900 OFFSET 333")
    want = {}
    for r, w in _window(full, 333, 900):
        want[(r["v"],)] = want.get((r["v"],), 0) + w
    assert bag(got) == dict(sorted(want.items(), key=repr))
