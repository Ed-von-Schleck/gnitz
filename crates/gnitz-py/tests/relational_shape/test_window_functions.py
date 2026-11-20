"""Window functions: `f(...) OVER (PARTITION BY ... ORDER BY ...)`, QUALIFY and
a named WINDOW clause.

An ordered window is an operator walking each partition in order; one over its
whole partition is a join of the body to a reduce over it. What each view states
is which frame it reads: every view is checked against its frame computed from
the rows the test wrote, on weights — a row whose value moves is retracted and
re-emitted, which is exactly where a weight bug would hide.

Run with GNITZ_WORKERS=4 — a window's partitions and its joins exchange.
"""
from collections import Counter

import gnitz
import pytest
from _exprs import declared
from _read import bag, scanned
from _shapes import agg, groups, sorted_by
from _sql import churn, inserting


def _frames(rows, partition, order):
    """`{id: (rank, dense rank, frame)}` of `rows` (dicts) windowed by
    `partition`, a list of columns, and `order`, a list of `(column, desc,
    nulls_first)`. The frame is the default one: every row up to the current
    row's last peer, each repeated by its weight `w` — with no order, the whole
    partition."""
    out = {}
    for part in groups(rows, lambda r: tuple(r[c] for c in partition)).values():
        frame = []
        peers = groups(sorted_by(part, order), lambda r: tuple(r[c] for c, *_ in order))
        for dense, ps in enumerate(peers.values(), 1):
            rank = len(frame) + 1
            frame = frame + [p for p in ps for _ in range(p.get("w", 1))]
            for r in ps:
                out[r["id"]] = (rank, dense, frame)
    return out


def _over(frames, kind, arg=None):
    """`{id: kind(arg)}` over each row's frame."""
    return {i: agg(kind, [x[arg] for x in frame] if arg else frame) for i, (_, _, frame) in frames.items()}


_ASC, _DESC, _ID = ("a", False, False), ("a", True, True), ("id", False, False)

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

# (statement, table, {id: row}); `None` deletes the id. `ev` rows are (k, ts, a, b),
# `grp` rows a region.
_CHURN = [
    inserting("grp", {1: 10, 2: 10, 3: 20}),
    inserting("ev", {1: (1, 10, 5, 1), 2: (1, 20, 3, None), 3: (1, 20, 8, 4), 4: (2, 5, 3, None),
                     5: (2, 15, 3, None), 6: (3, 1, 9, 2)}),
    # A new maximum shifts every rank in its partition.
    inserting("ev", {7: (1, 30, 100, None)}),
    ("UPDATE ev SET a = 2, b = NULL WHERE id = 3", "ev", {3: (1, 20, 2, None)}),
    # Partition 1's last non-NULL b goes: SUM(b)/MAX(b) become NULL.
    ("UPDATE ev SET b = NULL WHERE id = 1", "ev", {1: (1, 10, 5, None)}),
    ("DELETE FROM ev WHERE id = 4", "ev", {4: None}),
    ("DELETE FROM ev WHERE id = 5", "ev", {5: None}),
    ("UPDATE ev SET k = 1 WHERE id = 6", "ev", {6: (1, 1, 9, 2)}),
    # A tie, broken on the row key where a function breaks ties.
    inserting("ev", {8: (1, 99, 3, None)}),
    ("UPDATE ev SET ts = 21 WHERE id = 2", "ev", {2: (1, 21, 3, None)}),
    ("UPDATE ev SET a = 50 WHERE id = 2", "ev", {2: (1, 21, 50, None)}),
    # An earlier event shifts every later running value; a peer shares the frame.
    inserting("ev", {9: (1, 0, 7, 10)}),
    inserting("ev", {10: (1, 20, 1, None)}),
    ("UPDATE ev SET ts = 5 WHERE id = 10", "ev", {10: (1, 5, 1, None)}),
    ("UPDATE grp SET region = 20 WHERE id = 2", "grp", {2: 20}),
    ("DELETE FROM grp WHERE id = 3", "grp", {3: None}),
    # A row leaving the WHERE leaves every window it was in.
    ("UPDATE ev SET a = 1 WHERE id = 6", "ev", {6: (1, 1, 1, 2)}),
    ("UPDATE ev SET a = 9, k = 4 WHERE id = 6", "ev", {6: (4, 1, 9, 2)}),
    ("DELETE FROM ev WHERE id = 1", "ev", {1: None}),
    inserting("ev", {11: (3, 2, 20, None), 12: (3, 3, 1, None)}),
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
    for sql in churn(client, state, _CHURN):
        ev = {i: dict(zip(_EV, (i, *r))) for i, r in state["ev"].items()}
        grp = state["grp"]
        rows = list(ev.values())

        def check(view, cols, want):
            assert bag(scanned(client, view), *cols) == Counter(want), (sql, view)

        whole = _frames(rows, ["k"], [])
        s, c, cb, sb, mn, mx, av = (_over(whole, *f) for f in (
            ("SUM", "a"), ("COUNT*",), ("COUNT", "b"), ("SUM", "b"), ("MIN", "a"), ("MAX", "b"), ("AVG", "a")))
        check("whole", ("id", "k", "a", "s", "c", "cb", "sb", "mn", "mx", "av"),
              [(i, r["k"], r["a"], s[i], c[i], cb[i], sb[i], mn[i], mx[i], av[i]) for i, r in ev.items()])

        total = sum(r["a"] for r in rows)
        check("global", ("id", "pct", "n"), [(i, r["a"] * 100 // total, len(rows)) for i, r in ev.items()])

        asc, desc = _frames(rows, ["k"], [_ASC]), _frames(rows, ["k"], [_DESC])
        rn_desc = _frames(rows, ["k"], [_DESC, _ID])
        g = _frames(rows, [], [_DESC, ("ts", False, False)])
        check("ranks", ("id", "k", "a", "r", "d", "rn", "g"),
              [(i, x["k"], x["a"], asc[i][0], desc[i][1], rn_desc[i][0], g[i][0]) for i, x in ev.items()])

        sc, sav, ssb = (_over(asc, *f) for f in (("COUNT*",), ("AVG", "a"), ("SUM", "b")))
        srn = _frames(rows, ["k"], [_ASC, _ID])
        check("shared", ("id", "c", "ca", "av", "sb", "r", "rn", "cid"),
              [(i, sc[i], sc[i], sav[i], ssb[i], asc[i][0], srn[i][0], srn[i][0]) for i in ev])

        check("framed", ("id", "rn", "r"), [(i, rn_desc[i][0], desc[i][0]) for i in ev])
        check("fixed", ("id", "r", "s"), [(i, 1, s[i]) for i in ev])
        check("top_rank", ("id", "k", "r"), [(i, x["k"], desc[i][0]) for i, x in ev.items() if desc[i][0] <= 2])
        check("ranked", ("id", "k", "a", "r"), [(i, x["k"], x["a"], desc[i][0]) for i, x in ev.items()])
        best = Counter()
        for i, x in ev.items():
            if desc[i][0] <= 2:
                best[x["k"]] += x["a"]
        check("best", ("k", "s"), best.items())

        by_ts = _frames(rows, ["k"], [("ts", False, False)])
        running = [_over(by_ts, *f) for f in (("SUM", "a"), ("COUNT*",), ("MAX", "a"), ("MIN", "b"), ("AVG", "b"))]
        lex = _over(_frames(rows, [], [("ts", True, True), ("k", False, False)]), "SUM", "a")
        check("running", ("id", "k", "ts", "running", "n", "peak", "low", "avb", "lex"),
              [(i, x["k"], x["ts"], *(f[i] for f in running), lex[i]) for i, x in ev.items()])

        totals = [dict(id=k, total=sum(r["a"] for r in part)) for k, part in groups(rows, lambda r: r["k"]).items()]
        grank = _frames(totals, [], [("total", True, True)])
        grand = sum(x["total"] for x in totals)
        check("grouped", ("k", "total", "r", "pct"),
              [(x["id"], x["total"], grank[x["id"]][0], x["total"] * 100 // grand) for x in totals])

        joined = [dict(id=e["id"], region=grp[e["k"]], a=e["a"]) for e in rows if e["k"] in grp]
        rtotal = _over(_frames(joined, ["region"], []), "SUM", "a")
        jrank = _frames(joined, ["region"], [_DESC])
        check("joined", ("id", "region", "a", "rtotal", "r"),
              [(j["id"], j["region"], j["a"], rtotal[j["id"]], jrank[j["id"]][0]) for j in joined])

        kept = [dict(x, parity=x["k"] % 2, a2=x["a"] * 2) for x in rows if x["a"] > 1]
        named = _frames(kept, ["parity"], [("a2", True, True)])
        ns = _over(named, "SUM", "a")
        check("named", ("id", "parity", "a", "s", "r"),
              [(x["id"], x["parity"], x["a"], ns[x["id"]], named[x["id"]][0]) for x in kept])


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
    assert declared(client, "neg")["s"] == (gnitz.TypeCode.I64, 0)
    assert declared(client, "reals")["s"] == (gnitz.TypeCode.F64, 0)

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

    state = {"n": {}}
    for sql in churn(client, state, [
        inserting("n", {1: (1, 1, 5, "b", 1.5, 10), 2: (1, None, None, None, 2.5, 20),
                        3: (1, 1, 7, "a", 1.5, 30), 4: (2, None, 3, "b", 0.5, 40),
                        5: (2, None, None, "c", 0.25, 50), 6: (2, 2, 3, None, 9.0, 60),
                        7: (3, 2, None, "", -1.0, 70)}),
        # A NULL key gains a value, and a value becomes NULL.
        ("UPDATE n SET o = 6, s = 'bb' WHERE id = 2", "n", {2: (1, None, 6, "bb", 2.5, 20)}),
        ("UPDATE n SET o = NULL, p = NULL WHERE id = 3", "n", {3: (1, None, None, "a", 1.5, 30)}),
        # A second NULL peer, then the first one goes.
        inserting("n", {8: (1, 1, None, None, 2.5, 5)}),
        ("DELETE FROM n WHERE id = 3", "n", {3: None}),
        ("UPDATE n SET k = 2, f = 0.5 WHERE id = 1", "n", {1: (2, 1, 5, "b", 0.5, 10)}),
        ("DELETE FROM n WHERE id = 4", "n", {4: None}),
        ("DELETE FROM n WHERE id = 5", "n", {5: None}),
    ]):
        rows = [dict(zip(("id", "k", "p", "o", "s", "f", "a"), (i, *r))) for i, r in state["n"].items()]

        def check(view, names, want):
            assert bag(scanned(client, view), "id", *names) == Counter(want), (sql, view)

        f = _frames(rows, ["k"], [("o", False, True)])
        t = _over(f, "SUM", "a")
        check("nf", ("r", "d", "t"), [(i, f[i][0], f[i][1], t[i]) for i in f])
        f = _frames(rows, ["k"], [("o", True, False)])
        check("nl", ("r", "c"), [(i, f[i][0], len(f[i][2])) for i in f])
        f = _frames(rows, [], [("s", True, False), _ASC])
        t = _over(_frames(rows, ["k"], [("s", False, True)]), "SUM", "a")
        check("st", ("r", "t"), [(i, f[i][0], t[i]) for i in f])
        f = _frames(rows, ["k"], [("f", True, True)])
        check("fl", ("d",), [(i, f[i][1]) for i in f])
        f = _frames(rows, ["p"], [_DESC])
        t = _over(f, "SUM", "a")
        check("np", ("t", "r"), [(i, t[i], f[i][0]) for i in f])
        f = _frames(rows, ["k"], [_ASC])
        t, m = _over(f, "SUM", "k"), _over(f, "MAX", "k")
        check("own", ("t", "m", "c"), [(i, t[i], m[i], len(f[i][2])) for i in f])


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

    state = {"x": {}, "y": {}}
    for sql in churn(client, state, [
        inserting("x", {1: (1, 10), 2: (1, 10), 3: (1, 20), 4: (2, 5)}),
        inserting("y", {1: (1, 10), 2: (1, 30), 3: (2, 5)}),
        ("DELETE FROM x WHERE id = 2", "x", {2: None}),
        ("UPDATE y SET a = 20 WHERE id = 1", "y", {1: (1, 20)}),
        ("DELETE FROM x WHERE id = 1", "x", {1: None}),
        ("DELETE FROM y WHERE id = 3", "y", {3: None}),
    ]):
        copies = Counter(r for t in state.values() for r in t.values())
        f = _frames([dict(id=(k, a), k=k, a=a, w=w) for (k, a), w in copies.items()], ["k"], [_ASC])
        t = _over(f, "SUM", "a")
        assert bag(scanned(client, "w"), "k", "a", "r", "d", "t", "c") == {
            (*ka, f[ka][0], f[ka][1], t[ka], len(f[ka][2])): w for ka, w in copies.items()}, sql


def test_an_unpartitioned_aggregate_over_a_not_null_argument_is_never_null(client):
    """`SUM`, `MIN`, `MAX` and `AVG` over the whole of a NOT NULL column are
    declared NOT NULL: every row's frame holds the row itself. The views exist
    before the table holds a row, so each value is computed incrementally as the
    table fills, empties and fills again — never read off an empty input."""
    funcs = {"s": "SUM", "mn": "MIN", "mx": "MAX", "av": "AVG"}
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL); "
        + "; ".join(f"CREATE VIEW w_{c} AS SELECT a.id AS aid, a.v, {f}(a.v) OVER () AS {c} FROM a"
                    for c, f in funcs.items()))
    for c in funcs:
        assert not next(d for d in client.resolve_table(f"w_{c}")[1].columns if d.name == c).is_nullable, c

    state = {"a": {}}
    for sql in churn(client, state, [
        inserting("a", {1: 5, 2: 7}),
        inserting("a", {3: -2}),
        ("UPDATE a SET v = 40 WHERE id = 1", "a", {1: 40}),
        ("DELETE FROM a WHERE id = 2", "a", {2: None}),
        ("DELETE FROM a", "a", {1: None, 3: None}),
        inserting("a", {4: 9}),
    ]):
        vals = list(state["a"].values())
        for c, f in funcs.items():
            assert bag(scanned(client, f"w_{c}"), "aid", "v", c) == {
                (i, v, agg(f, vals)): 1 for i, v in state["a"].items()}, (sql, c)
