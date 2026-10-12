"""Window functions: `f(...) OVER (PARTITION BY ... ORDER BY ...)`, QUALIFY and
a named WINDOW clause.

An ordered window is an operator walking each partition in order; one over its
whole partition is a join of the body to a reduce over it. What each view states
is which frame it reads: every view is checked against its frame computed from
the rows the test wrote, on weights — a row whose value moves is retracted and
re-emitted, which is exactly where a weight bug would hide.

Run with GNITZ_WORKERS=4 — a window's partitions and its joins exchange.
"""
import functools
from collections import Counter

import gnitz
import pytest
from _read import bag, scanned


def _agg(kind, vals):
    """One aggregate over a frame: COUNT* counts every value, the rest skip NULLs."""
    if kind == "COUNT*":
        return len(vals)
    live = [v for v in vals if v is not None]
    if kind == "COUNT":
        return len(live)
    if not live:
        return None
    return {"SUM": sum, "MIN": min, "MAX": max, "AVG": lambda v: sum(v) / len(v)}[kind](live)


def _partitions(rows, key):
    parts = {}
    for r in rows:
        parts.setdefault(tuple(r[c] for c in key), []).append(r)
    return parts.values()


def _order(order):
    """A key realizing `order`, a list of (column, ascending) over NOT NULL integers."""
    return lambda r: tuple(r[c] if asc else -r[c] for c, asc in order)


def _whole(rows, partition, kind, arg=None):
    """`{id: kind(arg) OVER (PARTITION BY partition)}`."""
    return {r["id"]: _agg(kind, [x[arg] if arg else 1 for x in part])
            for part in _partitions(rows, partition) for r in part}


def _running(rows, partition, order, kind, arg=None):
    """The same with an ORDER BY: the default frame holds every row at or before
    the current row's order value, peers included."""
    key = _order(order)
    return {r["id"]: _agg(kind, [x[arg] if arg else 1 for x in part if key(x) <= key(r)])
            for part in _partitions(rows, partition) for r in part}


def _rank(rows, partition, order, dense=False):
    key = _order(order)
    return {r["id"]: (len({key(x) for x in part if key(x) < key(r)}) if dense
                      else sum(key(x) < key(r) for x in part)) + 1
            for part in _partitions(rows, partition) for r in part}


_VIEWS = [
    # No ORDER BY frames the whole partition: one row entering, leaving or moving
    # partition moves every row of it.
    "whole AS SELECT id, k, a, SUM(a) OVER (PARTITION BY k) AS s, COUNT(*) OVER (PARTITION BY k) AS c, "
    "COUNT(b) OVER (PARTITION BY k) AS cb, SUM(b) OVER (PARTITION BY k) AS sb, "
    "MIN(a) OVER (PARTITION BY k) AS mn, MAX(b) OVER (PARTITION BY k) AS mx, "
    "AVG(a) OVER (PARTITION BY k) AS av FROM ev",
    # `OVER ()` is a keyless join against one aggregate row.
    "global AS SELECT id, a * 100 / SUM(a) OVER () AS pct, COUNT(*) OVER () AS n FROM ev",
    # RANK skips tied positions, DENSE_RANK does not, ROW_NUMBER breaks ties on the row key.
    "ranks AS SELECT id, k, a, RANK() OVER (PARTITION BY k ORDER BY a) AS r, "
    "DENSE_RANK() OVER (PARTITION BY k ORDER BY a DESC) AS d, "
    "ROW_NUMBER() OVER (PARTITION BY k ORDER BY a DESC) AS rn, RANK() OVER (ORDER BY a DESC, ts) AS g FROM ev",
    # A projected RANK in QUALIFY filters the window's value; the top-N recognizer
    # reads only an unprojected ROW_NUMBER.
    "top_rank AS SELECT id, k, RANK() OVER (PARTITION BY k ORDER BY a DESC) AS r FROM ev QUALIFY r <= 2",
    "running AS SELECT id, k, ts, SUM(a) OVER (PARTITION BY k ORDER BY ts) AS running, "
    "COUNT(*) OVER (PARTITION BY k ORDER BY ts) AS n, MAX(a) OVER (PARTITION BY k ORDER BY ts) AS peak, "
    "MIN(b) OVER (PARTITION BY k ORDER BY ts) AS low, AVG(b) OVER (PARTITION BY k ORDER BY ts) AS avb, "
    "SUM(a) OVER (ORDER BY ts DESC, k) AS lex FROM ev",
    # Over a GROUP BY the window reads groups: its order key and argument are aggregates.
    "grouped AS SELECT k, SUM(a) AS total, RANK() OVER (ORDER BY SUM(a) DESC) AS r, "
    "SUM(a) * 100 / SUM(SUM(a)) OVER () AS pct FROM ev GROUP BY k",
    # Partitioned by the other side of a join, so a dimension row moves partitions.
    "joined AS SELECT ev.id, grp.region, ev.a, SUM(ev.a) OVER (PARTITION BY grp.region) AS rtotal, "
    "RANK() OVER (PARTITION BY grp.region ORDER BY ev.a DESC) AS r FROM ev JOIN grp ON ev.k = grp.id",
    # The WHERE runs first; one named window serves both calls; both keys are computed.
    "named AS SELECT id, k % 2 AS parity, a, SUM(a) OVER w AS s, RANK() OVER w AS r FROM ev WHERE a > 1 "
    "WINDOW w AS (PARTITION BY k % 2 ORDER BY a * 2 DESC)",
    # COUNT(*) and COUNT(a) over a NOT NULL `a` are one column of the window, which
    # AVG reads too; ROW_NUMBER ranks under its own specification's order, the row
    # key included, beside an aggregate over it.
    "shared AS SELECT id, COUNT(*) OVER w AS c, COUNT(a) OVER w AS ca, AVG(a) OVER w AS av, SUM(b) OVER w AS sb, "
    "RANK() OVER w AS r, ROW_NUMBER() OVER w AS rn, COUNT(*) OVER (PARTITION BY k ORDER BY a, id) AS cid "
    "FROM ev WINDOW w AS (PARTITION BY k ORDER BY a)",
    # A ranking function ignores its frame.
    "framed AS SELECT id, ROW_NUMBER() OVER w AS rn, RANK() OVER w AS r FROM ev "
    "WINDOW w AS (PARTITION BY k ORDER BY a DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
    # An order key the partition fixes orders nothing: every row is its partition's peer.
    "fixed AS SELECT id, RANK() OVER (PARTITION BY k ORDER BY k) AS r, SUM(a) OVER (PARTITION BY k ORDER BY k) AS s "
    "FROM ev",
    # A windowed view is a source like any other.
    "ranked AS SELECT id, k, a, RANK() OVER (PARTITION BY k ORDER BY a DESC) AS r FROM ev",
    "best AS SELECT k, SUM(a) AS s FROM ranked WHERE r <= 2 GROUP BY k",
]

_EV = ("id", "k", "ts", "a", "b")

# (statement, table, {id: row or changed columns}); `None` deletes the id.
_CHURN = [
    ("INSERT INTO grp VALUES (1, 10), (2, 10), (3, 20)", "grp", {1: {"region": 10}, 2: {"region": 10}, 3: {"region": 20}}),
    ("INSERT INTO ev VALUES (1, 1, 10, 5, 1), (2, 1, 20, 3, NULL), (3, 1, 20, 8, 4), (4, 2, 5, 3, NULL), "
     "(5, 2, 15, 3, NULL), (6, 3, 1, 9, 2)",
     "ev", {i: dict(zip(_EV, r)) for i, r in ((1, (1, 1, 10, 5, 1)), (2, (2, 1, 20, 3, None)),
                                              (3, (3, 1, 20, 8, 4)), (4, (4, 2, 5, 3, None)),
                                              (5, (5, 2, 15, 3, None)), (6, (6, 3, 1, 9, 2)))}),
    # A new maximum shifts every rank in its partition.
    ("INSERT INTO ev VALUES (7, 1, 30, 100, NULL)", "ev", {7: dict(zip(_EV, (7, 1, 30, 100, None)))}),
    ("UPDATE ev SET a = 2, b = NULL WHERE id = 3", "ev", {3: {"a": 2, "b": None}}),
    # Partition 1's last non-NULL b goes: SUM(b)/MAX(b) become NULL.
    ("UPDATE ev SET b = NULL WHERE id = 1", "ev", {1: {"b": None}}),
    ("DELETE FROM ev WHERE id = 4", "ev", {4: None}),
    ("DELETE FROM ev WHERE id = 5", "ev", {5: None}),
    ("UPDATE ev SET k = 1 WHERE id = 6", "ev", {6: {"k": 1}}),
    # A three-way tie, broken on the row key where a function breaks ties.
    ("INSERT INTO ev VALUES (8, 1, 99, 3, NULL)", "ev", {8: dict(zip(_EV, (8, 1, 99, 3, None)))}),
    ("UPDATE ev SET ts = 21 WHERE id = 2", "ev", {2: {"ts": 21}}),
    ("UPDATE ev SET a = 50 WHERE id = 2", "ev", {2: {"a": 50}}),
    # An earlier event shifts every later running value; a peer shares the frame.
    ("INSERT INTO ev VALUES (9, 1, 0, 7, 10)", "ev", {9: dict(zip(_EV, (9, 1, 0, 7, 10)))}),
    ("INSERT INTO ev VALUES (10, 1, 20, 1, NULL)", "ev", {10: dict(zip(_EV, (10, 1, 20, 1, None)))}),
    ("UPDATE ev SET ts = 5 WHERE id = 10", "ev", {10: {"ts": 5}}),
    ("UPDATE grp SET region = 20 WHERE id = 2", "grp", {2: {"region": 20}}),
    ("DELETE FROM grp WHERE id = 3", "grp", {3: None}),
    # A row leaving the WHERE leaves every window it was in.
    ("UPDATE ev SET a = 1 WHERE id = 6", "ev", {6: {"a": 1}}),
    ("UPDATE ev SET a = 9, k = 4 WHERE id = 6", "ev", {6: {"a": 9, "k": 4}}),
    ("DELETE FROM ev WHERE id = 1", "ev", {1: None}),
    ("INSERT INTO ev VALUES (11, 3, 2, 20, NULL), (12, 3, 3, 1, NULL)",
     "ev", {11: dict(zip(_EV, (11, 3, 2, 20, None))), 12: dict(zip(_EV, (12, 3, 3, 1, None)))}),
]


def test_every_window_moves_with_the_data(client):
    """Every frame shape over one churn, each view compared whole after every
    epoch."""
    client.execute_sql(
        "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, ts BIGINT NOT NULL, "
        "a BIGINT NOT NULL, b BIGINT); "
        "CREATE TABLE grp (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL); "
        + "; ".join(f"CREATE VIEW {v}" for v in _VIEWS))

    state = {"ev": {}, "grp": {}}
    for sql, table, changes in _CHURN:
        client.execute_sql(sql)
        for i, change in changes.items():
            if change is None:
                del state[table][i]
            else:
                state[table][i] = {**state[table].get(i, {}), **change}
        ev, grp = state["ev"], state["grp"]
        rows = list(ev.values())

        def check(view, cols, want):
            assert bag(scanned(client, view), *cols) == Counter(want), (sql, view)

        s, c, cb, sb = (_whole(rows, ["k"], *f) for f in (("SUM", "a"), ("COUNT*",), ("COUNT", "b"), ("SUM", "b")))
        mn, mx, av = (_whole(rows, ["k"], *f) for f in (("MIN", "a"), ("MAX", "b"), ("AVG", "a")))
        check("whole", ("id", "k", "a", "s", "c", "cb", "sb", "mn", "mx", "av"),
              [(i, r["k"], r["a"], s[i], c[i], cb[i], sb[i], mn[i], mx[i], av[i]) for i, r in ev.items()])

        total = sum(r["a"] for r in rows)
        check("global", ("id", "pct", "n"), [(i, r["a"] * 100 // total, len(rows)) for i, r in ev.items()])

        r_asc = _rank(rows, ["k"], [("a", True)])
        d_desc = _rank(rows, ["k"], [("a", False)], dense=True)
        rn_desc = _rank(rows, ["k"], [("a", False), ("id", True)])
        g = _rank(rows, [], [("a", False), ("ts", True)])
        check("ranks", ("id", "k", "a", "r", "d", "rn", "g"),
              [(i, x["k"], x["a"], r_asc[i], d_desc[i], rn_desc[i], g[i]) for i, x in ev.items()])

        by_a = [("a", True)]
        sc, sav, ssb = (_running(rows, ["k"], by_a, *f) for f in (("COUNT*",), ("AVG", "a"), ("SUM", "b")))
        srn = _rank(rows, ["k"], [("a", True), ("id", True)])
        check("shared", ("id", "c", "ca", "av", "sb", "r", "rn", "cid"),
              [(i, sc[i], sc[i], sav[i], ssb[i], r_asc[i], srn[i], srn[i]) for i in ev])

        r_desc = _rank(rows, ["k"], [("a", False)])
        check("framed", ("id", "rn", "r"), [(i, rn_desc[i], r_desc[i]) for i in ev])
        check("fixed", ("id", "r", "s"), [(i, 1, s[i]) for i in ev])
        check("top_rank", ("id", "k", "r"), [(i, x["k"], r_desc[i]) for i, x in ev.items() if r_desc[i] <= 2])
        check("ranked", ("id", "k", "a", "r"), [(i, x["k"], x["a"], r_desc[i]) for i, x in ev.items()])
        best = Counter()
        for i, x in ev.items():
            if r_desc[i] <= 2:
                best[x["k"]] += x["a"]
        check("best", ("k", "s"), best.items())

        by_ts = [("ts", True)]
        running = {name: _running(rows, ["k"], by_ts, *f) for name, f in (
            ("running", ("SUM", "a")), ("n", ("COUNT*",)), ("peak", ("MAX", "a")),
            ("low", ("MIN", "b")), ("avb", ("AVG", "b")))}
        lex = _running(rows, [], [("ts", False), ("k", True)], "SUM", "a")
        check("running", ("id", "k", "ts", "running", "n", "peak", "low", "avb", "lex"),
              [(i, x["k"], x["ts"], *(running[n][i] for n in ("running", "n", "peak", "low", "avb")), lex[i])
               for i, x in ev.items()])

        groups = [dict(id=part[0]["k"], total=sum(r["a"] for r in part)) for part in _partitions(rows, ["k"])]
        grank = _rank(groups, [], [("total", False)])
        grand = sum(x["total"] for x in groups)
        check("grouped", ("k", "total", "r", "pct"),
              [(x["id"], x["total"], grank[x["id"]], x["total"] * 100 // grand) for x in groups])

        joined = [dict(id=e["id"], region=grp[e["k"]]["region"], a=e["a"]) for e in rows if e["k"] in grp]
        rtotal = _whole(joined, ["region"], "SUM", "a")
        jrank = _rank(joined, ["region"], [("a", False)])
        check("joined", ("id", "region", "a", "rtotal", "r"),
              [(j["id"], j["region"], j["a"], rtotal[j["id"]], jrank[j["id"]]) for j in joined])

        kept = [dict(x, parity=x["k"] % 2, a2=x["a"] * 2) for x in rows if x["a"] > 1]
        ns = _running(kept, ["parity"], [("a2", False)], "SUM", "a")
        nr = _rank(kept, ["parity"], [("a2", False)])
        check("named", ("id", "parity", "a", "s", "r"),
              [(x["id"], x["parity"], x["a"], ns[x["id"]], nr[x["id"]]) for x in kept])


def test_a_computed_window_argument_or_key_is_declared_as_the_value_it_computes(client):
    """A window hoists each computed argument and key into a column of its own,
    declared as the value it computes: `-q` over an unsigned column is a negative
    64-bit value, a cast to REAL a double. A date difference over a NOT NULL date
    is a provably NOT NULL key — a date literal is never NULL — and partitions
    exactly as the date does."""
    client.execute_sql(
        "CREATE TABLE w (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, q INT UNSIGNED NOT NULL, "
        "x BIGINT NOT NULL, d DATE NOT NULL); "
        "CREATE VIEW neg AS SELECT id, g, SUM(-q) OVER (PARTITION BY g) AS s FROM w; "
        "CREATE VIEW reals AS SELECT id, g, SUM(CAST(x AS REAL)) OVER (PARTITION BY g) AS s FROM w; "
        "CREATE VIEW by_day AS SELECT id, COUNT(*) OVER (PARTITION BY d - DATE '2020-01-01') AS c FROM w; "
        "CREATE VIEW by_d AS SELECT id, COUNT(*) OVER (PARTITION BY d) AS c FROM w")

    def check(neg, reals, per_day):
        assert bag(scanned(client, "neg"), "id", "g", "s") == dict.fromkeys(neg, 1)
        assert bag(scanned(client, "reals"), "id", "g", "s") == dict.fromkeys(reals, 1)
        assert bag(scanned(client, "by_day"), "id", "c") == bag(scanned(client, "by_d"), "id", "c") \
            == dict.fromkeys(per_day, 1)

    client.execute_sql(
        "INSERT INTO w VALUES (1, 1, 4000000000, 5, DATE '2020-01-03'), (2, 1, 3, -7, DATE '2020-01-03'), "
        "(3, 2, 10, 70000, DATE '2019-12-31')")
    check([(1, 1, -4000000003), (2, 1, -4000000003), (3, 2, -10)],
          [(1, 1, -2.0), (2, 1, -2.0), (3, 2, 70000.0)],
          [(1, 2), (2, 2), (3, 1)])
    client.execute_sql("UPDATE w SET d = DATE '2019-12-31', g = 2 WHERE id = 2")
    check([(1, 1, -4000000000), (2, 2, -13), (3, 2, -13)],
          [(1, 1, 5.0), (2, 2, 69993.0), (3, 2, 69993.0)],
          [(1, 1), (2, 2), (3, 2)])


def test_row_number_reads_the_row_key_off_the_registered_view(client):
    """`ROW_NUMBER` numbers the rows of its input, so it needs one whose PK
    identifies a row. Which relations have one is carried on the relation itself
    and travels back to a later statement's planner through RESOLVE: a view over
    a stream, a join view keyed on the join key and a `LIMIT n > 1` top-N view all
    lose it, while a plain projection over a table keeps it."""
    client.execute_sql(
        "CREATE TABLE s (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL) "
        "WITH (stream = true); "
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL); "
        "CREATE TABLE u (uid BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL); "
        "CREATE VIEW sv AS SELECT id, a FROM s; "
        "CREATE VIEW jv AS SELECT t.id AS id, t.a AS a FROM t JOIN u ON t.k = u.k; "
        "CREATE VIEW tv AS SELECT id, a FROM t ORDER BY a LIMIT 2; "
        "CREATE VIEW pv AS SELECT id, a FROM t")

    for name in ("sv", "jv", "tv"):
        with pytest.raises(gnitz.GnitzRefusedError, match="unique row key"):
            client.execute_sql(
                f"CREATE VIEW rn_{name} AS SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS rn FROM {name}")
    client.execute_sql(
        "CREATE VIEW rn_pv AS SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS rn FROM pv")
    client.execute_sql("INSERT INTO t VALUES (1, 1, 30), (2, 1, 10), (3, 2, 20)")
    assert bag(scanned(client, "rn_pv"), "a", "rn") == {(10, 1): 1, (20, 2): 1, (30, 3): 1}


def _keyed(rows, partition, order):
    """Each partition of `rows` as its peer groups in `order`, a list of
    `(column, descending, nulls_first)`: rows equal on every key are peers."""
    def cmp(x, y):
        for col, desc, nulls_first in order:
            a, b = x[col], y[col]
            if (a is None) != (b is None):
                return -1 if (a is None) == nulls_first else 1
            if a is not None and a != b:
                return (-1 if a < b else 1) * (-1 if desc else 1)
        return 0
    for part in _partitions(rows, partition):
        groups = []
        for r in sorted(part, key=functools.cmp_to_key(cmp)):
            if groups and cmp(groups[-1][0], r) == 0:
                groups[-1].append(r)
            else:
                groups.append([r])
        yield groups


def _values(rows, partition, order, arg=None):
    """`{id: (rank, dense rank, running sum of arg, running count)}`, each row at
    its own multiplicity `w`."""
    out = {}
    for groups in _keyed(rows, partition, order):
        before = total = count = 0
        for dense, peers in enumerate(groups, 1):
            for r in peers:
                count += r.get("w", 1)
                if arg and r[arg] is not None:
                    total += r[arg] * r.get("w", 1)
            for r in peers:
                out[r["id"]] = (before + 1, dense, total, count)
            before = count
    return out


def test_a_window_takes_an_ordered_indexs_keys(client):
    """An ordered window keys an index, not a join: its order may be NULL, a
    string or a float, placed where the ORDER BY says, and its partition may be
    NULL — one partition. An aggregate may read its own partition column."""
    client.execute_sql(
        "CREATE TABLE n (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, p BIGINT, o BIGINT, "
        "s TEXT, f DOUBLE NOT NULL, a BIGINT NOT NULL); "
        "CREATE VIEW nf AS SELECT id, RANK() OVER w AS r, DENSE_RANK() OVER w AS d, SUM(a) OVER w AS t "
        "FROM n WINDOW w AS (PARTITION BY k ORDER BY o NULLS FIRST); "
        "CREATE VIEW nl AS SELECT id, RANK() OVER w AS r, COUNT(*) OVER w AS c "
        "FROM n WINDOW w AS (PARTITION BY k ORDER BY o DESC NULLS LAST); "
        "CREATE VIEW st AS SELECT id, RANK() OVER (ORDER BY s DESC NULLS LAST, a) AS r, "
        "SUM(a) OVER (PARTITION BY k ORDER BY s NULLS FIRST) AS t FROM n; "
        "CREATE VIEW fl AS SELECT id, DENSE_RANK() OVER (PARTITION BY k ORDER BY f DESC) AS d FROM n; "
        "CREATE VIEW np AS SELECT id, SUM(a) OVER (PARTITION BY p ORDER BY a DESC) AS t, "
        "RANK() OVER (PARTITION BY p ORDER BY a DESC) AS r FROM n; "
        "CREATE VIEW own AS SELECT id, SUM(k) OVER w AS t, MAX(k) OVER w AS m, COUNT(*) OVER w AS c "
        "FROM n WINDOW w AS (PARTITION BY k ORDER BY a)")

    cols = ("k", "p", "o", "s", "f", "a")
    rows = {}

    def write(sql, changes):
        client.execute_sql(sql)
        for i, r in changes.items():
            if r is None:
                del rows[i]
            else:
                rows[i] = dict(zip(("id", *cols), (i, *r)))
        state = list(rows.values())

        def check(view, names, want):
            assert bag(scanned(client, view), "id", *names) == dict(sorted(Counter(want).items(), key=repr)), (sql, view)

        v = _values(state, ["k"], [("o", False, True)], "a")
        check("nf", ("r", "d", "t"), [(i, v[i][0], v[i][1], v[i][2]) for i in rows])
        v = _values(state, ["k"], [("o", True, False)])
        check("nl", ("r", "c"), [(i, v[i][0], v[i][3]) for i in rows])
        r = _values(state, [], [("s", True, False), ("a", False, False)])
        t = _values(state, ["k"], [("s", False, True)], "a")
        check("st", ("r", "t"), [(i, r[i][0], t[i][2]) for i in rows])
        v = _values(state, ["k"], [("f", True, False)])
        check("fl", ("d",), [(i, v[i][1]) for i in rows])
        v = _values(state, ["p"], [("a", True, False)], "a")
        check("np", ("t", "r"), [(i, v[i][2], v[i][0]) for i in rows])
        v = _values(state, ["k"], [("a", False, False)], "k")
        check("own", ("t", "m", "c"), [(i, v[i][2], x["k"], v[i][3]) for i, x in rows.items()])

    write("INSERT INTO n VALUES (1, 1, 1, 5, 'b', 1.5, 10), (2, 1, NULL, NULL, NULL, 2.5, 20), "
          "(3, 1, 1, 7, 'a', 1.5, 30), (4, 2, NULL, 3, 'b', 0.5, 40), (5, 2, NULL, NULL, 'c', 0.25, 50), "
          "(6, 2, 2, 3, NULL, 9.0, 60), (7, 3, 2, NULL, '', -1.0, 70)",
          {1: (1, 1, 5, "b", 1.5, 10), 2: (1, None, None, None, 2.5, 20), 3: (1, 1, 7, "a", 1.5, 30),
           4: (2, None, 3, "b", 0.5, 40), 5: (2, None, None, "c", 0.25, 50), 6: (2, 2, 3, None, 9.0, 60),
           7: (3, 2, None, "", -1.0, 70)})
    # A NULL key gains a value, and a value becomes NULL.
    write("UPDATE n SET o = 6, s = 'bb' WHERE id = 2", {2: (1, None, 6, "bb", 2.5, 20)})
    write("UPDATE n SET o = NULL, p = NULL WHERE id = 3", {3: (1, None, None, "a", 1.5, 30)})
    # A second NULL peer, then the first one goes.
    write("INSERT INTO n VALUES (8, 1, 1, NULL, NULL, 2.5, 5)", {8: (1, 1, None, None, 2.5, 5)})
    write("DELETE FROM n WHERE id = 3", {3: None})
    write("UPDATE n SET k = 2, f = 0.5 WHERE id = 1", {1: (2, 1, 5, "b", 0.5, 10)})
    write("DELETE FROM n WHERE id = 4", {4: None})
    write("DELETE FROM n WHERE id = 5", {5: None})


def test_a_window_over_a_bag_weighs_every_copy(client):
    """Over a body whose rows repeat, a row's copies are peers: each counts
    toward the ranks behind it and folds into every aggregate at its
    multiplicity, and the row is emitted at that multiplicity."""
    client.execute_sql(
        "CREATE TABLE x (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL); "
        "CREATE TABLE y (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL); "
        "CREATE VIEW both AS SELECT k, a FROM x UNION ALL SELECT k, a FROM y; "
        "CREATE VIEW w AS SELECT k, a, RANK() OVER o AS r, DENSE_RANK() OVER o AS d, SUM(a) OVER o AS t, "
        "COUNT(*) OVER o AS c FROM both WINDOW o AS (PARTITION BY k ORDER BY a)")
    held = {"x": {}, "y": {}}

    def write(table, sql, changes):
        client.execute_sql(sql)
        for i, r in changes.items():
            if r is None:
                del held[table][i]
            else:
                held[table][i] = r
        copies = Counter(r for t in held.values() for r in t.values())
        state = [dict(id=(k, a), k=k, a=a, w=w) for (k, a), w in copies.items()]
        v = _values(state, ["k"], [("a", False, False)], "a")
        want = {(k, a, *v[k, a]): w for (k, a), w in copies.items()}
        assert bag(scanned(client, "w"), "k", "a", "r", "d", "t", "c") == dict(sorted(want.items(), key=repr)), sql

    write("x", "INSERT INTO x VALUES (1, 1, 10), (2, 1, 10), (3, 1, 20), (4, 2, 5)",
          {1: (1, 10), 2: (1, 10), 3: (1, 20), 4: (2, 5)})
    write("y", "INSERT INTO y VALUES (1, 1, 10), (2, 1, 30), (3, 2, 5)", {1: (1, 10), 2: (1, 30), 3: (2, 5)})
    write("x", "DELETE FROM x WHERE id = 2", {2: None})
    write("y", "UPDATE y SET a = 20 WHERE id = 1", {1: (1, 20)})
    write("x", "DELETE FROM x WHERE id = 1", {1: None})
    write("y", "DELETE FROM y WHERE id = 3", {3: None})
