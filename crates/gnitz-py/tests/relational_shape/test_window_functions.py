"""Window functions: `f(...) OVER (PARTITION BY ... ORDER BY ...)`, QUALIFY and
a named WINDOW clause.

A window call is not an operator — the planner desugars it into ordinary joins
and reduces over the body's own relation, so what each view states is which
frame that desugar produces. Every view is checked against its frame computed
from the rows the test wrote, on weights: the band self-join plus GROUP BY
behind an ordered window is exactly where a weight bug would hide.

Run with GNITZ_WORKERS=4 — the desugared joins exchange.
"""
from collections import Counter

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
    # A projected RANK in QUALIFY filters the desugar's value; the top-N recognizer
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


def test_every_window_moves_with_the_data(client, schema_name):
    """Every frame shape over one churn, each view compared whole after every
    epoch."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, ts BIGINT NOT NULL, "
        "a BIGINT NOT NULL, b BIGINT); "
        "CREATE TABLE grp (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL); "
        + "; ".join(f"CREATE VIEW {v}" for v in _VIEWS), schema_name=sn)

    state = {"ev": {}, "grp": {}}
    for sql, table, changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        for i, change in changes.items():
            if change is None:
                del state[table][i]
            else:
                state[table][i] = {**state[table].get(i, {}), **change}
        ev, grp = state["ev"], state["grp"]
        rows = list(ev.values())

        def check(view, cols, want):
            assert bag(scanned(client, sn, view), *cols) == Counter(want), (sql, view)

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

        r_desc = _rank(rows, ["k"], [("a", False)])
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
