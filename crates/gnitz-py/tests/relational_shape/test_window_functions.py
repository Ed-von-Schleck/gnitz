"""Window functions: `f(...) OVER (PARTITION BY ... ORDER BY ...)`, QUALIFY and
a named WINDOW clause.

A window call is not an operator — the planner desugars it into ordinary joins
and reduces over the body's own relation, so what each test states is which
frame that desugar produces. Every case is checked against a pure-Python oracle
recomputed from base state the test maintains itself, on weights: the band
self-join plus GROUP BY behind an ordered window is exactly where a weight bug
would hide, and a row-set comparison cannot see one.

Run with GNITZ_WORKERS=4 — the desugared joins exchange.
"""
from collections import Counter

import pytest
import _oracle


class _Table:
    """The test's own copy of a base table: pk -> row dict, mutated in lockstep
    with the SQL sent to the engine."""

    def __init__(self, client, sn, name, ddl):
        self.client, self.sn, self.name = client, sn, name
        self.rows = {}
        client.execute_sql(ddl, schema_name=sn)

    def insert(self, *rows):
        cols = list(rows[0].keys())
        vals = ", ".join("(" + ", ".join(_lit(r[c]) for c in cols) + ")" for r in rows)
        self.client.execute_sql(f"INSERT INTO {self.name} ({', '.join(cols)}) VALUES {vals}", schema_name=self.sn)
        _oracle.apply_insert(self.rows, "id", rows)

    def delete(self, pk):
        self.client.execute_sql(f"DELETE FROM {self.name} WHERE id = {pk}", schema_name=self.sn)
        _oracle.apply_delete(self.rows, "id", [pk])

    def update(self, pk, **changes):
        sets = ", ".join(f"{c} = {_lit(v)}" for c, v in changes.items())
        self.client.execute_sql(f"UPDATE {self.name} SET {sets} WHERE id = {pk}", schema_name=self.sn)
        _oracle.apply_update(self.rows, "id", pk, changes)


def _lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


# ── Oracle ─────────────────────────────────────────────────────────────────────


def _partition(rows, key):
    parts = {}
    for r in rows:
        parts.setdefault(tuple(r[c] for c in key), []).append(r)
    return parts


def _sort_key(order):
    """A key function realizing `order`, a list of (col, asc) over NOT NULL
    integer columns, so DESC negates."""
    return lambda r: tuple(r[c] if asc else -r[c] for c, asc in order)


def oracle_whole(rows, partition, agg, arg):
    """{id: value} of `agg(arg) OVER (PARTITION BY partition)`, `agg` an
    `_oracle.agg_over` kind."""
    out = {}
    for part in _partition(rows, partition).values():
        vals = [r[arg] for r in part] if arg else [1] * len(part)
        out.update({r["id"]: _oracle.agg_over(agg, vals) for r in part})
    return out


def oracle_cumulative(rows, partition, order, agg, arg):
    """{id: value} of `agg(arg) OVER (PARTITION BY partition ORDER BY order)`
    with the default frame — every row of the partition at or before the
    current row's order value, peers included."""
    out = {}
    key = _sort_key(order)
    for part in _partition(rows, partition).values():
        for r in part:
            frame = [x for x in part if key(x) <= key(r)]
            vals = [x[arg] for x in frame] if arg else [1] * len(frame)
            out[r["id"]] = _oracle.agg_over(agg, vals)
    return out


def oracle_rank(rows, partition, order, dense=False):
    out = {}
    key = _sort_key(order)
    for part in _partition(rows, partition).values():
        for r in part:
            before = [x for x in part if key(x) < key(r)]
            if dense:
                out[r["id"]] = len({key(x) for x in before}) + 1
            else:
                out[r["id"]] = len(before) + 1
    return out


def oracle_row_number(rows, partition, order):
    """ROW_NUMBER with the engine's tiebreak: the primary key, ascending."""
    return oracle_rank(rows, partition, order + [("id", True)])


_EV = "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, ts BIGINT NOT NULL, a BIGINT NOT NULL, b BIGINT)"

_ROWS = [
    dict(id=1, k=1, ts=10, a=5, b=1),
    dict(id=2, k=1, ts=20, a=3, b=None),
    dict(id=3, k=1, ts=20, a=8, b=4),
    dict(id=4, k=2, ts=5, a=3, b=None),
    dict(id=5, k=2, ts=15, a=3, b=None),
    dict(id=6, k=3, ts=1, a=9, b=2),
]


def test_every_aggregate_over_a_partition_moves_with_the_data(client, schema_name):
    """A PARTITION BY with no ORDER BY frames the whole partition, so every
    aggregate is a partition-level value: one row entering, leaving, or changing
    partition moves every row of that partition. COUNT(*) counts row weight,
    every other aggregate skips NULLs, and a partition with no non-NULL input
    yields NULL."""
    sn = schema_name
    ev = _Table(client, sn, "ev", _EV)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, k, a, "
        "SUM(a) OVER (PARTITION BY k) AS s, COUNT(*) OVER (PARTITION BY k) AS c, "
        "COUNT(b) OVER (PARTITION BY k) AS cb, SUM(b) OVER (PARTITION BY k) AS sb, "
        "MIN(a) OVER (PARTITION BY k) AS mn, MAX(b) OVER (PARTITION BY k) AS mx, "
        "AVG(a) OVER (PARTITION BY k) AS av FROM ev",
        schema_name=sn,
    )
    vid = client.resolve_table(sn, "v")[0]

    def check():
        rows = list(ev.rows.values())
        s = oracle_whole(rows, ["k"], "SUM", "a")
        c = oracle_whole(rows, ["k"], "COUNT*", None)
        cb = oracle_whole(rows, ["k"], "COUNT", "b")
        sb = oracle_whole(rows, ["k"], "SUM", "b")
        mn = oracle_whole(rows, ["k"], "MIN", "a")
        mx = oracle_whole(rows, ["k"], "MAX", "b")
        av = oracle_whole(rows, ["k"], "AVG", "a")
        exp = Counter(
            (r["id"], r["k"], r["a"], s[i], c[i], cb[i], sb[i], mn[i], mx[i], av[i])
            for i, r in ev.rows.items()
        )
        _oracle.assert_view_matches(client, vid, ["id", "k", "a", "s", "c", "cb", "sb", "mn", "mx", "av"], exp)

    check()
    ev.insert(*_ROWS)
    check()
    # A partition-level value changes: every row of the partition moves.
    ev.insert(dict(id=7, k=1, ts=30, a=1, b=None))
    check()
    ev.update(3, a=2, b=None)
    check()
    # The last non-null b of partition 1 goes: SUM(b)/MAX(b) become NULL.
    ev.update(1, b=None)
    check()
    ev.delete(4)
    ev.delete(5)
    check()
    ev.update(6, k=1)
    check()


def test_a_global_window_is_a_keyless_join_against_one_row(client, schema_name):
    """`OVER ()` is one partition for the whole relation — a keyless join against
    the single-row aggregate. Over the empty relation it emits nothing, and after
    that every row's value moves with any write anywhere."""
    sn = schema_name
    ev = _Table(client, sn, "ev", _EV)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, a * 100 / SUM(a) OVER () AS pct, COUNT(*) OVER () AS n FROM ev",
        schema_name=sn,
    )
    vid = client.resolve_table(sn, "v")[0]

    def check():
        rows = list(ev.rows.values())
        total = sum(r["a"] for r in rows)
        exp = Counter((i, r["a"] * 100 // total, len(rows)) for i, r in ev.rows.items())
        _oracle.assert_view_matches(client, vid, ["id", "pct", "n"], exp)

    _oracle.assert_view_matches(client, vid, ["id", "pct", "n"], Counter())
    ev.insert(*_ROWS)
    check()
    ev.delete(6)
    check()
    ev.update(1, a=50)
    check()


def test_rank_dense_rank_row_number_with_ties_in_both_directions(client, schema_name):
    """The three ranking functions differ only in how they treat a tie: RANK
    skips the tied positions, DENSE_RANK does not, and ROW_NUMBER breaks the tie
    on the input's row key. Ties made, broken and re-made, in both sort
    directions, partitioned and global."""
    sn = schema_name
    ev = _Table(client, sn, "ev", _EV)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, k, a, "
        "RANK() OVER (PARTITION BY k ORDER BY a) AS r, "
        "DENSE_RANK() OVER (PARTITION BY k ORDER BY a DESC) AS d, "
        "ROW_NUMBER() OVER (PARTITION BY k ORDER BY a DESC) AS rn, "
        "RANK() OVER (ORDER BY a DESC, ts) AS g FROM ev",
        schema_name=sn,
    )
    vid = client.resolve_table(sn, "v")[0]

    def check():
        rows = list(ev.rows.values())
        r = oracle_rank(rows, ["k"], [("a", True)])
        d = oracle_rank(rows, ["k"], [("a", False)], dense=True)
        rn = oracle_row_number(rows, ["k"], [("a", False)])
        g = oracle_rank(rows, [], [("a", False), ("ts", True)])
        exp = Counter((i, x["k"], x["a"], r[i], d[i], rn[i], g[i]) for i, x in ev.rows.items())
        _oracle.assert_view_matches(client, vid, ["id", "k", "a", "r", "d", "rn", "g"], exp)

    ev.insert(*_ROWS)
    check()
    # A new maximum shifts every rank in its partition.
    ev.insert(dict(id=7, k=1, ts=25, a=100, b=None))
    check()
    # Breaking a tie.
    ev.update(4, a=4)
    check()
    # Making a three-way tie.
    ev.update(4, a=3)
    ev.insert(dict(id=8, k=2, ts=99, a=3, b=None))
    check()
    ev.delete(7)
    check()
    # Moving a row between partitions.
    ev.update(8, k=1)
    check()


def test_top_n_per_group_through_qualify(client, schema_name):
    """QUALIFY filters on the window value, so the surviving rows follow the
    data: a newer event displaces the leader of its partition and deleting it
    restores the previous one. A tie on the ORDER BY key is broken by the row
    key, so which row wins is a function of the data alone."""
    sn = schema_name
    ev = _Table(client, sn, "ev", _EV)
    client.execute_sql(
        "CREATE VIEW latest AS SELECT id, k, ts, a FROM ev "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC) = 1",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW top2 AS SELECT id, k, RANK() OVER (PARTITION BY k ORDER BY a DESC) AS r FROM ev "
        "QUALIFY r <= 2",
        schema_name=sn,
    )
    latest = client.resolve_table(sn, "latest")[0]
    top2 = client.resolve_table(sn, "top2")[0]

    def check():
        rows = list(ev.rows.values())
        rn = oracle_row_number(rows, ["k"], [("ts", False)])
        exp_latest = Counter((i, x["k"], x["ts"], x["a"]) for i, x in ev.rows.items() if rn[i] == 1)
        _oracle.assert_view_matches(client, latest, ["id", "k", "ts", "a"], exp_latest)
        r = oracle_rank(rows, ["k"], [("a", False)])
        exp_top = Counter((i, x["k"], r[i]) for i, x in ev.rows.items() if r[i] <= 2)
        _oracle.assert_view_matches(client, top2, ["id", "k", "r"], exp_top)

    ev.insert(*_ROWS)
    check()
    # A newer event replaces the latest of its partition …
    ev.insert(dict(id=7, k=2, ts=100, a=1, b=None))
    check()
    # … and deleting it restores the previous one.
    ev.delete(7)
    check()
    # A tie on ts is broken by the primary key: id 3 wins over id 2 in
    # partition 1 (both ts=20); updating 2 to a later ts makes it win.
    ev.update(2, ts=21)
    check()
    ev.update(2, a=50)
    check()


def test_running_aggregates_include_peers(client, schema_name):
    """An ORDER BY in the window frames every row of the partition at or before
    the current row's order value, peers included — so a row joining at an
    existing order value enters its peers' frames as well as its own, and an
    earlier row shifts every later value."""
    sn = schema_name
    ev = _Table(client, sn, "ev", _EV)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, k, ts, "
        "SUM(a) OVER (PARTITION BY k ORDER BY ts) AS running, "
        "COUNT(*) OVER (PARTITION BY k ORDER BY ts) AS n, "
        "MAX(a) OVER (PARTITION BY k ORDER BY ts) AS peak, "
        "MIN(b) OVER (PARTITION BY k ORDER BY ts) AS low, "
        "AVG(b) OVER (PARTITION BY k ORDER BY ts) AS avb, "
        "SUM(a) OVER (ORDER BY ts DESC, k) AS lex FROM ev",
        schema_name=sn,
    )
    vid = client.resolve_table(sn, "v")[0]

    def check():
        rows = list(ev.rows.values())
        o = [("ts", True)]
        running = oracle_cumulative(rows, ["k"], o, "SUM", "a")
        n = oracle_cumulative(rows, ["k"], o, "COUNT*", None)
        peak = oracle_cumulative(rows, ["k"], o, "MAX", "a")
        low = oracle_cumulative(rows, ["k"], o, "MIN", "b")
        avb = oracle_cumulative(rows, ["k"], o, "AVG", "b")
        lex = oracle_cumulative(rows, [], [("ts", False), ("k", True)], "SUM", "a")
        exp = Counter(
            (i, x["k"], x["ts"], running[i], n[i], peak[i], low[i], avb[i], lex[i])
            for i, x in ev.rows.items()
        )
        cols = ["id", "k", "ts", "running", "n", "peak", "low", "avb", "lex"]
        _oracle.assert_view_matches(client, vid, cols, exp)

    ev.insert(*_ROWS)
    check()
    # An earlier event shifts every later running value in its partition.
    ev.insert(dict(id=7, k=1, ts=0, a=7, b=10))
    check()
    # A peer joins at an existing ts: peers share the frame.
    ev.insert(dict(id=8, k=1, ts=20, a=1, b=None))
    check()
    ev.update(7, a=0, b=None)
    check()
    ev.delete(3)
    check()
    ev.update(8, ts=5)
    check()


def test_a_window_over_a_grouped_body_ranks_its_groups(client, schema_name):
    """A window over a GROUP BY body reads groups, not rows: the ORDER BY and the
    aggregate argument are themselves aggregates, so `RANK() OVER (ORDER BY
    SUM(a) DESC)` ranks the groups and `SUM(SUM(a)) OVER ()` totals them. QUALIFY
    over the same body cuts groups rather than rows."""
    sn = schema_name
    ev = _Table(client, sn, "ev", _EV)
    client.execute_sql(
        "CREATE VIEW v AS SELECT k, SUM(a) AS total, RANK() OVER (ORDER BY SUM(a) DESC) AS r, "
        "SUM(a) * 100 / SUM(SUM(a)) OVER () AS pct FROM ev GROUP BY k",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW top AS SELECT k, COUNT(*) AS n FROM ev GROUP BY k "
        "QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, k) <= 2",
        schema_name=sn,
    )
    vid = client.resolve_table(sn, "v")[0]
    top = client.resolve_table(sn, "top")[0]

    def check():
        groups = [
            dict(id=k, k=k, total=sum(r["a"] for r in part), n=len(part))
            for k, part in sorted(_partition(ev.rows.values(), ["k"]).items(), key=lambda kv: kv[0][0])
            for k in [k[0]]
        ]
        grand = sum(g["total"] for g in groups)
        r = oracle_rank(groups, [], [("total", False)])
        exp = Counter((g["k"], g["total"], r[g["k"]], g["total"] * 100 // grand) for g in groups)
        _oracle.assert_view_matches(client, vid, ["k", "total", "r", "pct"], exp)
        rn = oracle_rank(groups, [], [("n", False), ("k", True)])
        exp_top = Counter((g["k"], g["n"]) for g in groups if rn[g["k"]] <= 2)
        _oracle.assert_view_matches(client, top, ["k", "n"], exp_top)

    ev.insert(*_ROWS)
    check()
    ev.insert(dict(id=7, k=3, ts=2, a=20, b=None), dict(id=8, k=3, ts=3, a=1, b=None))
    check()
    ev.delete(1)
    ev.delete(2)
    check()
    ev.update(4, k=9)
    check()


def test_a_window_over_a_join_body_partitions_by_the_other_side(client, schema_name):
    """A window over a join body may partition by a column of the side the row
    did not come from, so moving one row of that side moves the partition — and
    therefore every window value — of every row joined to it."""
    sn = schema_name
    ev = _Table(client, sn, "ev", _EV)
    grp = _Table(
        client, sn, "grp", "CREATE TABLE grp (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)"
    )
    client.execute_sql(
        "CREATE VIEW v AS SELECT ev.id, grp.region, ev.a, "
        "SUM(ev.a) OVER (PARTITION BY grp.region) AS rtotal, "
        "RANK() OVER (PARTITION BY grp.region ORDER BY ev.a DESC) AS r "
        "FROM ev JOIN grp ON ev.k = grp.id",
        schema_name=sn,
    )
    vid = client.resolve_table(sn, "v")[0]

    def check():
        joined = [
            dict(id=e["id"], region=grp.rows[e["k"]]["region"], a=e["a"])
            for e in ev.rows.values()
            if e["k"] in grp.rows
        ]
        rtotal = oracle_whole(joined, ["region"], "SUM", "a")
        r = oracle_rank(joined, ["region"], [("a", False)])
        exp = Counter((j["id"], j["region"], j["a"], rtotal[j["id"]], r[j["id"]]) for j in joined)
        _oracle.assert_view_matches(client, vid, ["id", "region", "a", "rtotal", "r"], exp)

    grp.insert(dict(id=1, region=10), dict(id=2, region=10), dict(id=3, region=20))
    ev.insert(*_ROWS)
    check()
    # Moving a group to another region moves its events' partition.
    grp.update(2, region=20)
    check()
    grp.delete(3)
    check()
    ev.insert(dict(id=7, k=3, ts=1, a=1, b=None), dict(id=8, k=1, ts=1, a=100, b=None))
    check()


def test_where_named_window_and_a_computed_key(client, schema_name):
    """The WHERE runs before the window, so a row leaving it leaves every window
    it was in; a `WINDOW w AS (...)` clause is one window however many calls name
    it; and both the partition and the order key may be computed expressions."""
    sn = schema_name
    ev = _Table(client, sn, "ev", _EV)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, k % 2 AS parity, a, "
        "SUM(a) OVER w AS s, RANK() OVER w AS r FROM ev WHERE a > 1 "
        "WINDOW w AS (PARTITION BY k % 2 ORDER BY a * 2 DESC)",
        schema_name=sn,
    )
    vid = client.resolve_table(sn, "v")[0]

    def check():
        rows = [dict(r, parity=r["k"] % 2, a2=r["a"] * 2) for r in ev.rows.values() if r["a"] > 1]
        s = oracle_cumulative(rows, ["parity"], [("a2", False)], "SUM", "a")
        r = oracle_rank(rows, ["parity"], [("a2", False)])
        exp = Counter((x["id"], x["parity"], x["a"], s[x["id"]], r[x["id"]]) for x in rows)
        _oracle.assert_view_matches(client, vid, ["id", "parity", "a", "s", "r"], exp)

    ev.insert(*_ROWS)
    check()
    # A row leaving the WHERE leaves every window it was in.
    ev.update(6, a=1)
    check()
    ev.update(6, a=9, k=4)
    check()
    ev.delete(1)
    check()


def test_a_windowed_view_is_a_source_like_any_other(client, schema_name):
    """A windowed view is an ordinary maintained relation: a downstream view
    filters on its window column and aggregates what survives, and every base
    write flows through both."""
    sn = schema_name
    ev = _Table(client, sn, "ev", _EV)
    client.execute_sql(
        "CREATE VIEW ranked AS SELECT id, k, a, RANK() OVER (PARTITION BY k ORDER BY a DESC) AS r FROM ev",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW best AS SELECT k, SUM(a) AS s FROM ranked WHERE r <= 2 GROUP BY k",
        schema_name=sn,
    )
    best = client.resolve_table(sn, "best")[0]

    def check():
        rows = list(ev.rows.values())
        r = oracle_rank(rows, ["k"], [("a", False)])
        sums = {}
        for x in rows:
            if r[x["id"]] <= 2:
                sums[x["k"]] = sums.get(x["k"], 0) + x["a"]
        _oracle.assert_view_matches(client, best, ["k", "s"], Counter(sums.items()))

    ev.insert(*_ROWS)
    check()
    ev.insert(dict(id=7, k=1, ts=1, a=50, b=None))
    check()
    ev.delete(7)
    check()


@pytest.mark.parametrize(
    "body, needle",
    [
        ("SELECT id, SUM(a) OVER (PARTITION BY b) FROM ev", "must be provably NOT NULL"),
        ("SELECT id, RANK() OVER (ORDER BY b) FROM ev", "must be provably NOT NULL"),
        ("SELECT id, RANK() OVER (PARTITION BY k) FROM ev", "need an ORDER BY"),
        (
            "SELECT id, SUM(a) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM ev",
            "ROWS / GROUPS",
        ),
        ("SELECT id FROM ev WHERE SUM(a) OVER () > 1", "only supported in the SELECT list and QUALIFY"),
        ("SELECT id, k FROM ev QUALIFY a > 1", "QUALIFY needs a window function"),
        ("SELECT id, LAG(a) OVER (ORDER BY ts) FROM ev", "not supported"),
    ],
)
def test_the_planner_names_what_it_refuses(client, schema_name, body, needle):
    """Each refusal names the rule it enforces, so an unsupported window shape
    cannot compile to a differently-shaped graph."""
    sn = schema_name
    client.execute_sql(_EV, schema_name=sn)
    with pytest.raises(Exception) as exc:
        client.execute_sql(f"CREATE VIEW v AS {body}", schema_name=sn)
    assert needle in str(exc.value), str(exc.value)


def test_an_ad_hoc_read_has_no_windows(client, schema_name):
    """A window call desugars only in a view body; the ad-hoc read path has no
    such desugar and refuses it rather than answering something else."""
    sn = schema_name
    client.execute_sql(_EV, schema_name=sn)
    with pytest.raises(Exception) as exc:
        client.execute_sql("SELECT id, RANK() OVER (ORDER BY a) FROM ev", schema_name=sn)
    assert "OVER" in str(exc.value) or "window" in str(exc.value).lower(), str(exc.value)
