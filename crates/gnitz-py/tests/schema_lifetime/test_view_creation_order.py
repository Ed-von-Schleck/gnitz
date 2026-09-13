"""`CREATE VIEW` over base tables whose rows have not ticked yet.

A push applies to the store at once, and the DDL drains every reachable base
before `VIEW_TAB` registers the view, so the backfill sees a full store. What
this pins is *where that drain sits*: moved after registration, the pending rows
would reach the view twice — through the backfill and through the deferred tick.
So every assertion is a weighted bag, where that failure is doubled weights over
the right row set.

Run at GNITZ_WORKERS=4 — the exchange/fanout paths only engage at W>1.
"""

import pytest
from _read import bag, scanned
from _serverproc import NEEDS_MULTI
from _sql import values
from _uid import uid


_A = ("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL); "
      f"INSERT INTO a VALUES {values((i, i % 5, i * 10) for i in range(30))}")
# `a` plus `b(id, bv)`, one row per `a.k`, with `bv = 100 · id`.
_AB = (_A + "; CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL); "
       f"INSERT INTO b VALUES {values((j, j * 100) for j in range(5))}")
_T = "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"
_AI, _BJ = range(30), range(5)


def _band(i):
    return i * 10 < (i % 5) * 100

# name -> (tables and their rows, the view DDL — several statements for a chain —,
#          the columns the last view `v` is read by, its expected bag)
_SHAPES = {
    "proj": (_A, "CREATE VIEW v AS SELECT id, av + 1 AS p FROM a",
             ("id", "p"), {(i, i * 10 + 1): 1 for i in range(30)}),
    "grp": (_A, "CREATE VIEW v AS SELECT k, COUNT(*) AS c FROM a GROUP BY k",
            ("k", "c"), {(k, 6): 1 for k in range(5)}),
    "distinct": (_A, "CREATE VIEW v AS SELECT DISTINCT k FROM a",
                 ("k",), {(k,): 1 for k in range(5)}),
    "join": (_AB, "CREATE VIEW v AS SELECT a.id AS aid, b.bv AS bv FROM a JOIN b ON a.k = b.id",
             ("aid", "bv"), {(i, (i % 5) * 100): 1 for i in _AI}),
    "band": (_AB, "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
             "FROM a JOIN b ON a.k = b.id AND a.av < b.bv",
             ("aid", "bid"), {(i, i % 5): 1 for i in _AI if _band(i)}),
    "band_left": (_AB, "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                  "FROM a LEFT JOIN b ON a.k = b.id AND a.av < b.bv",
                  ("aid", "bid"), {(i, i % 5 if _band(i) else None): 1 for i in _AI}),
    "range_left": (_AB, "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                   "FROM a LEFT JOIN b ON a.av < b.bv",
                   ("aid", "bid"), {(i, j): 1 for i in _AI for j in _BJ if i * 10 < j * 100}
                   | {(i, None): 1 for i in _AI if i * 10 >= 400}),
    "cross": (_AB, "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid FROM a CROSS JOIN b",
              ("aid", "bid"), {(i, j): 1 for i in _AI for j in _BJ}),
    # The repeated occurrence is wrapped in a pass-through that must seed first.
    "self_join": (_A, "CREATE VIEW v AS SELECT x.id AS xid, y.id AS yid FROM a x JOIN a y ON x.k = y.id",
                  ("xid", "yid"), {(i, i % 5): 1 for i in _AI}),
    "self_intersect": (_A, "CREATE VIEW v AS SELECT k FROM a INTERSECT SELECT k FROM a",
                       ("k",), {(k,): 1 for k in _BJ}),
    "exists": (_AB, "CREATE VIEW v AS SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.bv = a.av)",
               ("id",), {(i,): 1 for i in (0, 10, 20)}),
    "not_exists": (_AB, "CREATE VIEW v AS SELECT id FROM a "
                   "WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.bv = a.av)",
                   ("id",), {(i,): 1 for i in _AI if i not in (0, 10, 20)}),
    "correlated_scalar": (_AB, "CREATE VIEW v AS SELECT b.id AS id, "
                          "(SELECT COUNT(*) FROM a WHERE a.k = b.id) AS c FROM b",
                          ("id", "c"), {(j, 6): 1 for j in _BJ}),
    "uncorrelated_scalar": (_AB, "CREATE VIEW v AS SELECT id FROM b WHERE b.bv < (SELECT MAX(av) FROM a)",
                            ("id",), {(0,): 1, (1,): 1, (2,): 1}),
    # One join-output key spans six groups, so each group's MIN must isolate.
    "min_over_fanout_join": (_AB, "CREATE VIEW v AS SELECT a.av AS g, MIN(a.id) AS lo, MAX(b.bv) AS hi "
                             "FROM b JOIN a ON b.id = a.k GROUP BY a.av",
                             ("g", "lo", "hi"), {(i * 10, i, (i % 5) * 100): 1 for i in _AI}),
    "distinct_over_join": (_AB, "CREATE VIEW v AS SELECT DISTINCT b.bv AS bv FROM a JOIN b ON a.k = b.id",
                           ("bv",), {(j * 100,): 1 for j in _BJ}),
    "reduce_into_join": (_AB, "CREATE VIEW v AS WITH s AS (SELECT k, SUM(av) AS t FROM a GROUP BY k) "
                         "SELECT b.bv AS bv, s.t AS t FROM s JOIN b ON s.k = b.id",
                         ("bv", "t"), {(k * 100, sum(i * 10 for i in _AI if i % 5 == k)): 1 for k in _BJ}),
    "grouped_set_op_side": (_AB, "CREATE VIEW v AS SELECT k AS x, COUNT(*) AS n FROM a GROUP BY k "
                            "UNION ALL SELECT id, bv FROM b",
                            ("x", "n"), {(k, 6): 1 for k in _BJ} | {(j, j * 100): 1 for j in _BJ}),
    "set_op_chain": (_AB, "CREATE VIEW v AS SELECT k FROM a UNION SELECT id FROM b UNION SELECT bv FROM b",
                     ("k",), {(x,): 1 for x in (0, 1, 2, 3, 4, 100, 200, 300, 400)}),
    "setop": ("CREATE TABLE s1 (id BIGINT NOT NULL PRIMARY KEY); "
              "CREATE TABLE s2 (id BIGINT NOT NULL PRIMARY KEY); "
              f"INSERT INTO s1 VALUES {values((i,) for i in range(30))}; "
              f"INSERT INTO s2 VALUES {values((i,) for i in range(20, 40))}",
              "CREATE VIEW v AS SELECT id FROM s1 UNION SELECT id FROM s2",
              ("id",), {(i,): 1 for i in range(40)}),
    "rangejoin": ("CREATE TABLE ra (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL); "
                  "CREATE TABLE rb (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL); "
                  f"INSERT INTO ra VALUES {values((i, i) for i in range(10))}; "
                  f"INSERT INTO rb VALUES {values((j, j) for j in range(10))}",
                  "CREATE VIEW v AS SELECT ra.x AS x, rb.y AS y FROM ra JOIN rb ON ra.x < rb.y",
                  ("x", "y"), {(i, j): 1 for i in range(10) for j in range(10) if i < j}),
    # A chain of separate DDLs: `v` backfills from a view the previous DDL filled.
    "viewonview": (_A, "CREATE VIEW mid AS SELECT id, k, av FROM a WHERE av > 100; "
                   "CREATE VIEW v AS SELECT id, av FROM mid WHERE k = 3",
                   ("id", "av"),
                   {(i, i * 10): 1 for i in range(30) if i * 10 > 100 and i % 5 == 3}),
    "view_on_grouped_view": (
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, v BIGINT NOT NULL); "
        "INSERT INTO t VALUES (1, 7, 20), (2, 7, 5), (3, 9, 4)",
        "CREATE VIEW g AS SELECT g, SUM(v) AS sv FROM t GROUP BY g; "
        "CREATE VIEW v AS SELECT g FROM g WHERE sv > 10",
        ("g",), {(7,): 1}),
    # A linear final whose delta source is an in-bundle hidden segment that
    # exchanges — grouped, joined, or a linear segment between two grouped ones —
    # so the segment must be filled before the final reads it.
    "grouped_cte": (_T + "; INSERT INTO t VALUES (1, 20), (2, 5), (3, 30)",
                    "CREATE VIEW v AS WITH c AS (SELECT id, SUM(v) AS sv FROM t GROUP BY id) "
                    "SELECT id FROM c WHERE sv > 10",
                    ("id",), {(1,): 1, (3,): 1}),
    "join_cte": ("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL); "
                 "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL); "
                 "INSERT INTO a VALUES (1, 10), (2, 20), (3, 10); "
                 "INSERT INTO b VALUES (10, 5), (20, 0)",
                 "CREATE VIEW v AS WITH c AS "
                 "(SELECT a.id AS id, b.x AS x FROM a JOIN b ON a.k = b.id) "
                 "SELECT id FROM c WHERE x > 0",
                 ("id",), {(1,): 1, (3,): 1}),
    "linear_between": (_T + "; INSERT INTO t VALUES (1, 20), (2, 5), (3, 20), (4, 30)",
                       "CREATE VIEW v AS "
                       "WITH g AS (SELECT id, SUM(v) AS sv FROM t GROUP BY id), "
                       "     l AS (SELECT id, sv FROM g WHERE sv > 10) "
                       "SELECT sv, COUNT(*) AS c FROM l GROUP BY sv",
                       ("sv", "c"), {(20, 2): 1, (30, 1): 1}),
}


@pytest.mark.parametrize("shape", list(_SHAPES))
def test_a_view_created_over_pending_rows_holds_them_once(client, schema_name, shape):
    setup, views, cols, want = _SHAPES[shape]
    client.execute_sql(f"{setup}; {views}", schema_name=schema_name)
    assert bag(scanned(client, schema_name, "v"), *cols) == want


# ── another schema's DDL landing while this one's views tick ────────────────

_NDIM = 10
_NFACT = 40
_DIM_ROWS = "INSERT INTO dim VALUES " + ", ".join(f"({i}, {i % 5})" for i in range(1, _NDIM + 1))


def _dim_of(pk):
    """The dim row fact `pk` references — every fact matches one."""
    return ((pk - 1) % _NDIM) + 1


@pytest.fixture
def second_schema(client):
    """A second schema beside the `schema_name` one, dropped whole at teardown."""
    sn = "s" + uid()
    client.create_schema(sn)
    yield sn
    client.drop_schema(sn)


@NEEDS_MULTI
def test_a_second_schemas_ddl_does_not_disturb_a_ticking_schema(
        client, schema_name, second_schema):
    """Each schema's exchange relay stays bound to its own operand schema, and
    one table name in two schemas addresses two relations.

    The two facts differ in width (4 columns against 3) and the two view chains
    in depth, so a relay that labelled its batches with the wrong side's schema
    would cross the two and produce wrong aggregates rather than none. Schema A
    is asserted once before B exists and again after B is fully live, with a
    further insert in between — that last insert is what makes A tick while B's
    views are already running.
    """
    a, b = schema_name, second_schema

    def fact_rows(lo, hi):
        # amount = pk, so every fact passes A's filter and the sums are exact.
        return "INSERT INTO fact VALUES " + ", ".join(
            f"({pk}, {_dim_of(pk)}, {pk}, {pk % 6})" for pk in range(lo, hi))

    def want_a(hi):
        totals = {}
        for pk in range(1, hi):
            region = _dim_of(pk) % 5
            totals[region] = totals.get(region, 0) + pk
        return {(region, total): 1 for region, total in totals.items()}

    # A: filter -> join -> SUM by region, over a 4-column fact.
    client.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, dim_pk BIGINT NOT NULL, "
        "amount BIGINT NOT NULL, category BIGINT NOT NULL); "
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL); "
        "CREATE VIEW v_filter AS SELECT * FROM fact WHERE amount > 0; "
        "CREATE VIEW v_joined AS SELECT v_filter.pk, v_filter.amount, dim.region "
        "FROM v_filter INNER JOIN dim ON v_filter.dim_pk = dim.pk; "
        "CREATE VIEW v_agg AS SELECT region, SUM(amount) AS total FROM v_joined GROUP BY region; "
        f"{_DIM_ROWS}; {fact_rows(1, _NFACT + 1)}", schema_name=a)
    assert bag(scanned(client, a, "v_agg"), "region", "total") == want_a(_NFACT + 1), "A before B"

    # B: a five-view chain over a 3-column fact, created while A is live. `v4`'s
    # threshold excludes the smallest label group, so the DISTINCT below it sees
    # fewer labels than `v3` produced.
    client.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, dim_pk BIGINT NOT NULL, "
        "amount BIGINT NOT NULL); "
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, label BIGINT NOT NULL); "
        "CREATE VIEW v1 AS SELECT * FROM fact WHERE amount > 50; "
        "CREATE VIEW v2 AS SELECT v1.pk, v1.amount, dim.label "
        "FROM v1 INNER JOIN dim ON v1.dim_pk = dim.pk; "
        "CREATE VIEW v3 AS SELECT label, SUM(amount) AS total, COUNT(*) AS cnt "
        "FROM v2 GROUP BY label; "
        "CREATE VIEW v4 AS SELECT * FROM v3 WHERE total > 1000; "
        "CREATE VIEW v5 AS SELECT DISTINCT label FROM v4; "
        f"{_DIM_ROWS}; "
        "INSERT INTO fact VALUES " + ", ".join(
            f"({pk}, {_dim_of(pk)}, {pk * 7})" for pk in range(1, _NFACT + 1)),
        schema_name=b)

    by_label = {}
    for pk in range(1, _NFACT + 1):
        if pk * 7 <= 50:
            continue
        label = _dim_of(pk) % 5
        total, cnt = by_label.get(label, (0, 0))
        by_label[label] = (total + pk * 7, cnt + 1)
    b3 = {(label, total, cnt): 1 for label, (total, cnt) in by_label.items()}
    b5 = {(label,): 1 for label, (total, _) in by_label.items() if total > 1000}
    assert 0 < len(b5) < len(b3), "v4's threshold must exclude some but not all labels"

    assert bag(scanned(client, b, "v3"), "label", "total", "cnt") == b3
    assert bag(scanned(client, b, "v5"), "label") == b5

    # A ticks while B's chain is live: the relay must still be A's own.
    client.execute_sql(fact_rows(_NFACT + 1, _NFACT + 21), schema_name=a)
    assert bag(scanned(client, a, "v_agg"), "region", "total") == want_a(_NFACT + 21), "A after B"


def test_the_backfill_streams_long_strings_across_chunk_boundaries(tiny_ddl_chunk_server):
    """The backfill streams the source in `scan_chunk_rows`-sized chunks, so a
    view over a populated table must equal it row for row whatever the chunk
    size. Strings above the inline threshold are what make the chunking visible:
    each chunk relocates its payload into a fresh blob arena, so a value that
    survives within a chunk can still be lost or aliased across one.

    At the 65 536-row default a test table is a single chunk and pins nothing;
    the 3-row fixture makes 10 rows span four.
    """
    c, sn = tiny_ddl_chunk_server, "public"
    want = {(i, f"row_{i:02d}_" + "x" * 40): 1 for i in range(10)}
    c.execute_sql(
        "CREATE TABLE base (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL); "
        "INSERT INTO base VALUES " + ", ".join(f"({i}, '{s}')" for i, s in sorted(want)) + "; "
        "CREATE VIEW v AS SELECT id, name FROM base", schema_name=sn)
    assert bag(scanned(c, sn, "v"), "id", "name") == want

    # Still incremental over the backfilled rows.
    added = "row_10_" + "y" * 40
    c.execute_sql(f"INSERT INTO base VALUES (10, '{added}'); DELETE FROM base WHERE id = 3",
                  schema_name=sn)
    want[(10, added)] = 1
    del want[(3, "row_03_" + "x" * 40)]
    assert bag(scanned(c, sn, "v"), "id", "name") == want
