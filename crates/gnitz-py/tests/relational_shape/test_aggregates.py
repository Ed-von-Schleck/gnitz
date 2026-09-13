"""GROUP BY and the ungrouped (global) aggregate as dataflow shapes: the reduce
node, the value index its non-linear aggregates read, the finalize map above it,
and the HAVING filter above that.

Every assertion is a weighted bag, so a ghost or a doubled delta is a mismatch
rather than a plausible row set. Where an expectation would be a long literal it
is recomputed from the rows the test wrote, by each aggregate's SQL definition —
never read back from the engine. Value families (widths, strings, 16-byte types,
NULL against zero) are value_domain's; this file holds the value fixed.

Run with GNITZ_WORKERS=4: a GROUP BY shards each group whole onto one worker and
a global aggregate funnels or combines per-worker partials, so both routing
decisions are only real at W > 1.
"""
from collections import Counter

from _read import bag, scanned


def _fold(vals):
    """`(COUNT, SUM, AVG, MIN, MAX)` over the non-NULL `vals`: NULL for all but
    COUNT once none is left."""
    live = [v for v in vals if v is not None]
    if not live:
        return 0, None, None, None, None
    return len(live), sum(live), sum(live) / len(live), min(live), max(live)


def _groups(rows, key):
    out = {}
    for r in rows:
        out.setdefault(key(r), []).append(r)
    return out


def _apply(rows, changes):
    """Upsert `{pk: row}` into `rows`; a `None` row deletes the pk."""
    for pk, row in changes.items():
        if row is None:
            del rows[pk]
        else:
            rows[pk] = row


_LONG1, _LONG2 = "long-heap-backed-key-1", "long-heap-backed-key-2"

# (statement, {pk: (g, s, a, n)}).
_GROUPED_CHURN = [
    # g=10's `a` averages to 7/3, g=20's `n` is all NULL, and only g=30 passes
    # `a > 100`; `s` is inline for "ab" and heap-backed for the long keys.
    ("INSERT INTO t VALUES (1, 10, 'ab', 1, 5), (2, 10, 'ab', 2, NULL), "
     f"(3, 10, '{_LONG1}', 4, 15), (4, 20, '{_LONG2}', 5, NULL), (5, 30, 'ab', 200, NULL), "
     f"(6, 30, '{_LONG1}', 300, NULL)",
     {1: (10, "ab", 1, 5), 2: (10, "ab", 2, None), 3: (10, _LONG1, 4, 15),
      4: (20, _LONG2, 5, None), 5: (30, "ab", 200, None), 6: (30, _LONG1, 300, None)}),
    ("UPDATE t SET a = 9 WHERE pk = 3", {3: (10, _LONG1, 9, 15)}),
    # The extremum's holder leaves: MIN/MAX recompute the next-best from history.
    ("DELETE FROM t WHERE pk = 3", {3: None}),
    ("UPDATE t SET n = 0 WHERE pk = 2", {2: (10, "ab", 2, 0)}),
    ("INSERT INTO t VALUES (7, 20, 'ab', 6, 3)", {7: (20, "ab", 6, 3)}),
    # g=20's last non-NULL `n` leaves; the group lives on at COUNT(*) 1.
    ("DELETE FROM t WHERE pk = 7", {7: None}),
    ("DELETE FROM t WHERE g = 10", {1: None, 2: None}),
    # A group born with every `n` NULL.
    ("INSERT INTO t VALUES (8, 10, 'ab', 8, NULL)", {8: (10, "ab", 8, None)}),
]


def test_a_grouped_reduce_tracks_every_aggregate_through_churn(client, schema_name):
    """COUNT/SUM/AVG/MIN/MAX over a NOT NULL and a nullable column after every
    epoch. The `lone_*` views carry no COUNT(*), so group existence rides on the
    hidden cardinality: an emptied group vanishes rather than surviving as a
    zero, and an all-NULL group is present with its aggregate NULL (0 for COUNT)
    — a COUNT(*) beside them would mask that gate. AVG and a nullable SUM put a
    finalize map above the reduce, which must carry a TEXT key (inline and
    heap-backed) and a compound key through. A WHERE runs below the fold, so a
    group whose every row fails it never appears. `COUNT(ALL n)` is `COUNT(n)`."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, s TEXT NOT NULL, "
        "a BIGINT NOT NULL, n BIGINT); "
        "CREATE VIEW every AS SELECT g, COUNT(*) AS c, SUM(a) AS sa, AVG(a) AS aa, "
        "MIN(a) AS mina, MAX(a) AS maxa, COUNT(n) AS cn, COUNT(ALL n) AS can, SUM(n) AS sn, "
        "AVG(n) AS an, MIN(n) AS minn, MAX(n) AS maxn FROM t GROUP BY g; "
        + "; ".join(f"CREATE VIEW lone_{f} AS SELECT g, {f}(n) AS m FROM t GROUP BY g"
                    for f in ("count", "sum", "avg", "min")) + "; "
        "CREATE VIEW by_s AS SELECT s, AVG(a) AS aa, SUM(n) AS sn FROM t GROUP BY s; "
        "CREATE VIEW by_gs AS SELECT g, s, COUNT(*) AS c, AVG(a) AS aa FROM t GROUP BY g, s; "
        "CREATE VIEW filtered AS SELECT g, COUNT(*) AS c, SUM(a) AS sa FROM t "
        "WHERE a > 100 GROUP BY g", schema_name=sn)

    rows = {}
    for sql, changes in _GROUPED_CHURN:
        client.execute_sql(sql, schema_name=sn)
        _apply(rows, changes)
        by_g = {g: (len(rs), _fold(r[2] for r in rs), _fold(r[3] for r in rs))
                for g, rs in _groups(rows.values(), lambda r: r[0]).items()}

        assert bag(scanned(client, sn, "every"), "g", "c", "sa", "aa", "mina", "maxa", "cn",
                   "can", "sn", "an", "minn", "maxn") == {
            (g, c, *fa[1:], fn[0], *fn): 1 for g, (c, fa, fn) in by_g.items()}, sql
        for f, i in (("count", 0), ("sum", 1), ("avg", 2), ("min", 3)):
            assert bag(scanned(client, sn, f"lone_{f}"), "g", "m") == {
                (g, fn[i]): 1 for g, (_, _, fn) in by_g.items()}, (sql, f)
        assert bag(scanned(client, sn, "by_s"), "s", "aa", "sn") == {
            (s, _fold(r[2] for r in rs)[2], _fold(r[3] for r in rs)[1]): 1
            for s, rs in _groups(rows.values(), lambda r: r[1]).items()}, sql
        assert bag(scanned(client, sn, "by_gs"), "g", "s", "c", "aa") == {
            (g, s, len(rs), _fold(r[2] for r in rs)[2]): 1
            for (g, s), rs in _groups(rows.values(), lambda r: r[:2]).items()}, sql
        assert bag(scanned(client, sn, "filtered"), "g", "c", "sa") == {
            (g, len(rs), sum(r[2] for r in rs)): 1
            for g, rs in _groups([r for r in rows.values() if r[2] > 100], lambda r: r[0]).items()}, sql


def test_a_reduce_over_a_derived_delta_sees_only_net_changes(client, schema_name):
    """Three reduce inputs that are another operator's output deltas: a DISTINCT,
    whose boundary crossings reach COUNT(*) once per pair, through duplicate
    carriers and a cross-group move; another reduce, whose emptied group must
    vanish rather than feed a phantom `(s=0, c=1)` downstream; and a fan-out join,
    where one join-output key spans several groups, so a group's extremes must
    never pull a neighbour's rows."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE e (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, k BIGINT NOT NULL); "
        "CREATE VIEW d AS SELECT DISTINCT g, k FROM e; "
        "CREATE VIEW dc AS SELECT g, COUNT(*) AS c FROM d GROUP BY g; "
        "CREATE VIEW iv AS SELECT g, SUM(k) AS s FROM e GROUP BY g; "
        "CREATE VIEW ov AS SELECT s, COUNT(*) AS c FROM iv GROUP BY s", schema_name=sn)

    rows = {}
    for sql, changes in [
        ("INSERT INTO e VALUES (1, 1, 7), (2, 1, 7), (3, 1, 8), (4, 2, 9), (5, 2, 10)",
         {1: (1, 7), 2: (1, 7), 3: (1, 8), 4: (2, 9), 5: (2, 10)}),
        ("DELETE FROM e WHERE pk = 1", {1: None}),
        ("DELETE FROM e WHERE pk = 2", {2: None}),
        ("INSERT INTO e VALUES (6, 2, 9)", {6: (2, 9)}),
        ("UPDATE e SET k = 9 WHERE pk = 5", {5: (2, 9)}),
        # One epoch retracting a pair from one group and inserting it into another.
        ("UPDATE e SET g = 1 WHERE pk = 4", {4: (1, 9)}),
        ("DELETE FROM e WHERE g = 2", {5: None, 6: None}),
    ]:
        client.execute_sql(sql, schema_name=sn)
        _apply(rows, changes)
        by_g = _groups(rows.values(), lambda r: r[0])
        assert bag(scanned(client, sn, "dc"), "g", "c") == {
            (g, len({k for _, k in rs})): 1 for g, rs in by_g.items()}, sql
        assert bag(scanned(client, sn, "ov"), "s", "c") == {
            sc: 1 for sc in Counter(sum(k for _, k in rs) for rs in by_g.values()).items()}, sql

    client.execute_sql(
        "CREATE TABLE fact (fid BIGINT NOT NULL PRIMARY KEY); "
        "CREATE TABLE dim (did BIGINT NOT NULL PRIMARY KEY, fkey BIGINT NOT NULL, g INT NOT NULL, "
        "x BIGINT NOT NULL, y BIGINT NOT NULL); "
        "CREATE VIEW j AS SELECT fact.fid AS fid, dim.g AS g, dim.x AS x, dim.y AS y "
        "FROM fact JOIN dim ON fact.fid = dim.fkey; "
        "CREATE VIEW agg AS SELECT g, MIN(x) AS lo, MAX(y) AS hi, COUNT(*) AS c FROM j GROUP BY g; "
        "INSERT INTO fact VALUES (1), (2); "
        "INSERT INTO dim VALUES (1, 1, 10, 5, 100), (2, 1, 20, 7, 200), (3, 2, 10, 3, 50), "
        "(4, 2, 20, 9, 300)", schema_name=sn)
    assert bag(scanned(client, sn, "agg"), "g", "lo", "hi", "c") == {
        (10, 3, 100, 2): 1, (20, 7, 300, 2): 1}
    # g=10's MIN holder goes: it recomputes to 5, never to g=20's smaller entry.
    client.execute_sql("DELETE FROM dim WHERE did = 3", schema_name=sn)
    assert bag(scanned(client, sn, "agg"), "g", "lo", "hi", "c") == {
        (10, 5, 100, 1): 1, (20, 7, 300, 2): 1}


# HAVING predicate -> the groups it admits before and after pk 1 leaves. Over
# k=10 {5, NULL}, k=20 {NULL}, k=30 {0}, k=40 {5, -5}, a genuine zero sum, a
# genuine zero extremum and an all-NULL group are present at once; the delete
# leaves k=10 all-NULL while its raw SUM column nets back to zero bytes.
_HAVING = {
    "COUNT(*) > 1": ({10, 40}, {40}),
    "COUNT(*) = 1": ({20, 30}, {10, 20, 30}),
    "NOT (COUNT(*) = 1)": ({10, 40}, {40}),
    "COUNT(*) IN (1, 3)": ({20, 30}, {10, 20, 30}),
    "CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1": ({10, 40}, {40}),
    "COUNT(*) IS NOT NULL": ({10, 20, 30, 40}, {10, 20, 30, 40}),
    "COUNT(*) IS NULL": (set(), set()),
    "SUM(v) * 2 > 5": ({10}, set()),
    "SUM(v) BETWEEN 0 AND 4": ({30, 40}, {30, 40}),
    "SUM(v) IS NULL": ({20}, {10, 20}),
    "SUM(v) = 0": ({30, 40}, {30, 40}),
    "SUM(v) IS DISTINCT FROM 5": ({20, 30, 40}, {10, 20, 30, 40}),
    "SUM(v) IN (0, 5)": ({10, 30, 40}, {30, 40}),
    "COALESCE(SUM(v), -1) = -1": ({20}, {10, 20}),
    "ABS(SUM(v)) > 3": ({10}, set()),
    "(k + SUM(v)) IS NULL": ({20}, {10, 20}),
    "MIN(v) IS NULL": ({20}, {10, 20}),
    "MIN(v) IS NOT NULL": ({10, 30, 40}, {30, 40}),
    "MIN(v) = 0": ({30}, {30}),
    "MIN(h.v) > 3": ({10}, set()),
    "MAX(v) <= 10": ({10, 30, 40}, {30, 40}),
}


def test_having_filters_groups_on_the_grouped_relation(client, schema_name):
    """HAVING is a full expression over the grouped relation: `Mul`, `NOT`, CASE
    and the `BETWEEN` desugar bind there, and an aggregate buried in one is
    materialised in the reduce though the SELECT list never names it. A NULL
    aggregate is UNKNOWN — reading the raw column's zero bytes would admit it as a
    genuine 0 — while COUNT(*) is never NULL and folds. HAVING binds before the
    projection, so it may name an unprojected group column and must use a group
    column's source name where SELECT aliases it; a null test on a PK group column
    folds at plan time, for a single-column and a compound key alike; and a
    SMALLINT MAX is read at its own width by both HAVING and the projection."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE h (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT, "
        "w SMALLINT NOT NULL); "
        "CREATE TABLE h2 (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b)); "
        + "; ".join(f"CREATE VIEW p{i} AS SELECT k, COUNT(*) AS c FROM h GROUP BY k HAVING {p}"
                    for i, p in enumerate(_HAVING)) + "; "
        "CREATE VIEW unprojected_key AS SELECT k, COUNT(*) AS c FROM h GROUP BY k, v HAVING v > 0; "
        "CREATE VIEW aliased AS SELECT k AS x, COUNT(*) AS c FROM h GROUP BY k HAVING k > 25; "
        "CREATE VIEW unprojected_agg AS SELECT k FROM h GROUP BY k HAVING COUNT(*) > 1; "
        "CREATE VIEW pk_null AS SELECT pk, COUNT(*) AS c FROM h GROUP BY pk HAVING pk IS NULL; "
        "CREATE VIEW pk_not_null AS SELECT pk, COUNT(*) AS c FROM h GROUP BY pk HAVING pk IS NOT NULL; "
        "CREATE VIEW compound_pk AS SELECT a, b, COUNT(*) AS c FROM h2 GROUP BY a, b "
        "HAVING a IS NOT NULL; "
        "CREATE VIEW narrow AS SELECT k, MAX(w) AS m FROM h GROUP BY k HAVING MAX(w) > 1000; "
        "INSERT INTO h VALUES (1, 10, 5, 900), (2, 10, NULL, 1500), (3, 20, NULL, 30000), "
        "(4, 30, 0, 1000), (5, 40, 5, -32768), (6, 40, -5, 7); "
        "INSERT INTO h2 VALUES (5, 6), (5, 7)", schema_name=sn)

    counts = {10: 2, 20: 1, 30: 1, 40: 2}
    for step in (0, 1):
        if step:
            client.execute_sql("DELETE FROM h WHERE pk = 1", schema_name=sn)
            counts[10] = 1
        for i, (pred, admitted) in enumerate(_HAVING.items()):
            assert bag(scanned(client, sn, f"p{i}"), "k", "c") == {
                (k, counts[k]): 1 for k in admitted[step]}, (step, pred)
        assert bag(scanned(client, sn, "unprojected_key"), "k", "c") == \
            ({(10, 1): 1, (40, 1): 1} if step == 0 else {(40, 1): 1})
        assert bag(scanned(client, sn, "aliased"), "x", "c") == {(30, 1): 1, (40, 2): 1}
        assert bag(scanned(client, sn, "unprojected_agg"), "k") == \
            ({(10,): 1, (40,): 1} if step == 0 else {(40,): 1})
        assert bag(scanned(client, sn, "pk_null"), "pk", "c") == {}
        assert bag(scanned(client, sn, "pk_not_null"), "pk", "c") == {
            (pk, 1): 1 for pk in range(1 + step, 7)}
        assert bag(scanned(client, sn, "compound_pk"), "a", "b", "c") == {(5, 6, 1): 1, (5, 7, 1): 1}
        assert bag(scanned(client, sn, "narrow"), "k", "m") == {(10, 1500): 1, (20, 30000): 1}


def test_a_global_aggregate_is_one_row_over_any_source(client, schema_name):
    """No GROUP BY is one logical group, and SQL requires exactly one output row
    even over an empty or fully retracted source: the ground row supplies COUNT 0
    and every other aggregate NULL. MIN/MAX funnel every row onto one worker;
    `two_phase` is all-linear, so each worker folds a partial and the partials
    combine — weight-exact at any worker count, and a COUNT over a *fresh*
    all-NULL column must ground to 0 there too. A lone aggregate grounds when the
    cardinality gate sheds its computed row; a projection computed on the way in
    or out keeps the one row; and HAVING filters the ground row like any other,
    so `SUM(a) = 0` admits a genuine zero sum and never the ground's NULL."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, n BIGINT); "
        "CREATE VIEW funnel AS SELECT COUNT(*) AS c, SUM(a) AS sa, AVG(a) AS aa, MIN(a) AS lo, "
        "MAX(a) AS hi FROM t; "
        "CREATE VIEW two_phase AS SELECT COUNT(*) AS c, SUM(a) AS sa, AVG(a) AS aa, COUNT(n) AS cn "
        "FROM t; "
        "CREATE VIEW nullable AS SELECT COUNT(n) AS cn, SUM(n) AS sn, AVG(n) AS an, MIN(n) AS lo, "
        "MAX(n) AS hi FROM t; "
        "CREATE VIEW lone_count AS SELECT COUNT(n) AS cn FROM t; "
        "CREATE VIEW lone_sum AS SELECT SUM(a) AS sa FROM t; "
        "CREATE VIEW computed AS SELECT COUNT(*) + 1 AS c, SUM(a * 2) AS s, 'x' AS lit FROM t; "
        "CREATE VIEW having_count AS SELECT COUNT(*) AS c FROM t HAVING COUNT(*) > 2; "
        "CREATE VIEW having_zero AS SELECT SUM(a) AS s FROM t HAVING SUM(a) = 0",
        schema_name=sn)

    mixed = {pk: ((pk * 7) % 50, None if pk % 3 == 0 else pk) for pk in range(10, 50)}
    rows = {}
    for sql, changes in [
        ("SELECT 1", {}),
        # A genuine zero sum, with `n` fresh and all NULL.
        ("INSERT INTO t VALUES (1, 5, NULL), (2, -5, NULL)", {1: (5, None), 2: (-5, None)}),
        ("INSERT INTO t VALUES " + ", ".join(
            f"({pk}, {a}, {'NULL' if n is None else n})" for pk, (a, n) in mixed.items()), mixed),
        ("UPDATE t SET a = 100 WHERE pk = 15", {15: (100, mixed[15][1])}),
        ("UPDATE t SET n = 9 WHERE pk = 12", {12: (mixed[12][0], 9)}),
        ("DELETE FROM t WHERE pk IN (1, " + ", ".join(map(str, range(11, 50, 2))) + ")",
         {pk: None for pk in [1, *range(11, 50, 2)]}),
        ("DELETE FROM t", {pk: None for pk in [2, *range(10, 50, 2)]}),
        ("INSERT INTO t VALUES (99, 3, 4)", {99: (3, 4)}),
    ]:
        client.execute_sql(sql, schema_name=sn)
        _apply(rows, changes)
        c = len(rows)
        _, sa, aa, lo, hi = _fold(a for a, _ in rows.values())
        fn = _fold(n for _, n in rows.values())

        assert bag(scanned(client, sn, "funnel"), "c", "sa", "aa", "lo", "hi") == \
            {(c, sa, aa, lo, hi): 1}, sql
        assert bag(scanned(client, sn, "two_phase"), "c", "sa", "aa", "cn") == \
            {(c, sa, aa, fn[0]): 1}, sql
        assert bag(scanned(client, sn, "nullable"), "cn", "sn", "an", "lo", "hi") == {fn: 1}, sql
        assert bag(scanned(client, sn, "lone_count"), "cn") == {(fn[0],): 1}, sql
        assert bag(scanned(client, sn, "lone_sum"), "sa") == {(sa,): 1}, sql
        assert bag(scanned(client, sn, "computed"), "c", "s", "lit") == \
            {(c + 1, None if sa is None else 2 * sa, "x"): 1}, sql
        assert bag(scanned(client, sn, "having_count"), "c") == ({(c,): 1} if c > 2 else {}), sql
        assert bag(scanned(client, sn, "having_zero"), "s") == ({(0,): 1} if sa == 0 else {}), sql


def test_a_distinct_aggregate_is_distinct_composed_into_the_aggregate(client, schema_name):
    """`agg(DISTINCT x)` lowers to a plain aggregate over a hidden DISTINCT
    (group cols, x) segment, so it tracks the churn like its definition: NULL is
    not a distinct value, a group whose only value is NULL still exists, and a
    cross-group move crosses both groups' boundaries. Every DISTINCT aggregate of
    one argument shares one segment, including in HAVING and over a computed
    argument or group key; `MIN`/`MAX(DISTINCT x)` is the plain aggregate, so it
    may keep company with a COUNT(*) and a second DISTINCT argument."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE ev (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, u BIGINT, "
        "s TEXT NOT NULL); "
        "CREATE VIEW per_k AS SELECT k, COUNT(DISTINCT u) AS n FROM ev GROUP BY k; "
        "CREATE VIEW overall AS SELECT COUNT(DISTINCT s) AS n FROM ev; "
        "CREATE VIEW in_having AS SELECT k, COUNT(DISTINCT s) AS n FROM ev GROUP BY k "
        "HAVING COUNT(DISTINCT s) > 1; "
        "CREATE VIEW shared AS SELECT k, SUM(DISTINCT u) AS su, MAX(DISTINCT u) AS mu, "
        "COUNT(DISTINCT u) AS n FROM ev WHERE k < 30 GROUP BY k; "
        "CREATE VIEW computed AS SELECT COUNT(DISTINCT u % 2) AS n FROM ev GROUP BY k * 2; "
        "CREATE VIEW plain AS SELECT k, MAX(DISTINCT u) AS mu, MIN(DISTINCT pk) AS lo, "
        "COUNT(*) AS c FROM ev GROUP BY k", schema_name=sn)

    rows = {}
    for sql, changes in [
        ("INSERT INTO ev VALUES (1, 10, 7, 'a'), (2, 10, 7, 'b'), (3, 10, 8, 'a'), "
         "(4, 20, 7, 'a'), (5, 20, NULL, 'a'), (6, 30, NULL, 'c')",
         {1: (10, 7, "a"), 2: (10, 7, "b"), 3: (10, 8, "a"), 4: (20, 7, "a"),
          5: (20, None, "a"), 6: (30, None, "c")}),
        ("DELETE FROM ev WHERE pk = 1", {1: None}),
        ("DELETE FROM ev WHERE pk = 2", {2: None}),
        ("UPDATE ev SET k = 10 WHERE pk = 4", {4: (10, 7, "a")}),
        ("INSERT INTO ev VALUES (7, 20, 1, 'z')", {7: (20, 1, "z")}),
        ("DELETE FROM ev WHERE pk = 6", {6: None}),
    ]:
        client.execute_sql(sql, schema_name=sn)
        _apply(rows, changes)
        by_k = {k: (rs, {r[1] for r in rs} - {None})
                for k, rs in _groups(rows.values(), lambda r: r[0]).items()}

        assert bag(scanned(client, sn, "per_k"), "k", "n") == {
            (k, len(us)): 1 for k, (_, us) in by_k.items()}, sql
        assert bag(scanned(client, sn, "overall"), "n") == {
            (len({r[2] for r in rows.values()}),): 1}, sql
        assert bag(scanned(client, sn, "in_having"), "k", "n") == {
            (k, len({r[2] for r in rs})): 1 for k, (rs, _) in by_k.items()
            if len({r[2] for r in rs}) > 1}, sql
        assert bag(scanned(client, sn, "shared"), "k", "su", "mu", "n") == {
            (k, sum(us) if us else None, max(us, default=None), len(us)): 1
            for k, (_, us) in by_k.items() if k < 30}, sql
        assert bag(scanned(client, sn, "computed"), "n") == Counter(
            (len({u % 2 for u in us}),) for _, us in by_k.values()), sql
        assert bag(scanned(client, sn, "plain"), "k", "mu", "lo", "c") == {
            (k, max(us, default=None), min(pk for pk, r in rows.items() if r[0] == k), len(rs)): 1
            for k, (rs, us) in by_k.items()}, sql


def test_a_compound_group_key_is_emitted_in_source_key_order(client, schema_name):
    """The grouping *list* is not the output key order: a reduce over a whole
    compound source key emits it in the key's declared order, whatever order the
    GROUP BY named its columns — `ka` must carry `a`'s values, not `b`'s. HAVING
    binds by source name against that output, so a filter on the leading column
    selects a different pair than one on the trailing column would. The
    four-column key is the widest a reduce carries, and a singleton group's
    retraction takes its extremes with it."""
    sn = schema_name
    quads = [(1, 1, 1, 1, 10), (1, 1, 1, 2, 20), (1, 2, 3, 4, 30), (2, 1, 1, 1, 40)]
    client.execute_sql(
        "CREATE TABLE t2 (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
        "v BIGINT NOT NULL, PRIMARY KEY (a, b)); "
        "CREATE VIEW perm AS SELECT a AS ka, b AS kb, SUM(v) AS s FROM t2 GROUP BY b, a; "
        "CREATE VIEW hav AS SELECT a AS ka, b AS kb, SUM(v) AS s FROM t2 GROUP BY a, b HAVING a > 1; "
        "CREATE TABLE t4 (a INT UNSIGNED NOT NULL, b INT UNSIGNED NOT NULL, "
        "c INT UNSIGNED NOT NULL, d INT UNSIGNED NOT NULL, v BIGINT NOT NULL, "
        "PRIMARY KEY (a, b, c, d)); "
        "CREATE VIEW g4 AS SELECT a, b, c, d, COUNT(*) AS n, MIN(v) AS lo, MAX(v) AS hi "
        "FROM t4 GROUP BY a, b, c, d; "
        "INSERT INTO t2 VALUES (1, 7, 100), (2, 8, 200), (3, 1, 300); "
        f"INSERT INTO t4 VALUES {', '.join(map(str, quads))}", schema_name=sn)

    assert bag(scanned(client, sn, "perm"), "ka", "kb", "s") == {
        (1, 7, 100): 1, (2, 8, 200): 1, (3, 1, 300): 1}
    # A `b > 1` mis-binding would keep (1,7) and (2,8) instead.
    assert bag(scanned(client, sn, "hav"), "ka", "kb", "s") == {(2, 8, 200): 1, (3, 1, 300): 1}

    cols = ("a", "b", "c", "d", "n", "lo", "hi")
    assert bag(scanned(client, sn, "g4"), *cols) == {(*q[:4], 1, q[4], q[4]): 1 for q in quads}
    client.execute_sql("DELETE FROM t4 WHERE a = 1 AND b = 1 AND c = 1 AND d = 2", schema_name=sn)
    assert bag(scanned(client, sn, "g4"), *cols) == {
        (*q[:4], 1, q[4], q[4]): 1 for q in quads if q[3] != 2}
