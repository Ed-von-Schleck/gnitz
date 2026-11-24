"""Ad-hoc single-relation aggregation (GROUP BY / global aggregate / HAVING /
DISTINCT) served by the ReadSpec aggregate hash-fold sink + client finishing,
versus a `CREATE VIEW` of the same statement.

The central assertion is **parity**: for a query with no ORDER BY / LIMIT, the
ad-hoc result equals a scan of `CREATE VIEW v AS <same query>`, exactly — every
parity source here is integer, string or UUID valued, so both paths agree bit
for bit. Parity is compared as a weighted bag, so a group emitted twice by one
path is a divergence rather than a matching row set; and since parity would
also hold were both paths wrong, every case is pinned against an in-Python
oracle too. A float SUM/AVG instead follows the summation order; the tests at
the bottom pin what that does and does not promise.
"""
import random

import gnitz
import pytest
from _caps import conjunct_ladder, first_rejected
from _read import access, bag, ordered, reply, rows
from _serverproc import TINY_SCAN_CHUNKS
from _shapes import agg, groups
from _sql import insert
from _uid import uid


def _parity(client, query):
    """The ad-hoc bag of `query` (no ORDER BY / LIMIT), asserted equal — names
    and weighted bag — to a scan of a view built from the identical statement.
    The view is left for the schema's teardown."""
    adhoc = reply(client.execute_sql(query))
    vn = "pv_" + uid()
    client.execute_sql(f"CREATE VIEW {vn} AS {query}")
    view = client.scan(*client.resolve_table(vn))
    assert adhoc._fields == view._fields, f"column names differ for `{query}`"
    assert bag(adhoc) == bag(view), f"ad-hoc != view for `{query}`"
    return bag(adhoc)


def _by(src, key, value):
    """`{(key(group), value(rows of that group)): 1}` over `src` — the weighted
    bag a `SELECT key, value … GROUP BY key` owes."""
    return {(k, value(g)): 1 for k, g in groups(src, key).items()}


# ---------------------------------------------------------------------------
# The shared read-only sources
#
# Module-scoped: every case only reads, and a `_parity` view is created under a
# unique name. Each oracle computes over the same rows the fixture inserts, so
# changing a fixture cannot leave an expectation quietly describing the old data.
# ---------------------------------------------------------------------------


# `(pk, category, amount, note)`.
_ORDERS = [(i, i % 5, (i * 7) % 100, None if i % 3 == 0 else (i * 3) % 50) for i in range(1, 41)]
# `(x, s, id, v)`, PK `id` declared third.
_NL = [(i * 1.5, "a" if i % 2 else "b", i, i % 4) for i in range(1, 13)]
# `(pk, g, label, u)`.
_WX_LABELS = ["shared/prefix/aa", "shared/prefix/a", "shared/prefix/ab", "b", ""]
_WX = [(i, i % 3, _WX_LABELS[i % 5], f"550e8400-e29b-41d4-a716-4466554400{i:02d}")
       for i in range(20)]
# `(pk, c, a)`, replicated.
_RT = [(i, i % 3, i) for i in range(1, 13)]
# `(pk, g, c0..c7)`.
_M8 = [(i, i % 4, *((i * (j + 1)) % 50 for j in range(8))) for i in range(1, 31)]
_WMAX = f"w{gnitz.MAX_COLUMNS}"


@pytest.fixture(scope="module")
def src(module_client):
    conn = module_client
    conn.execute_sql(
        "CREATE TABLE orders (pk BIGINT NOT NULL PRIMARY KEY, category BIGINT NOT NULL,"
        " amount BIGINT NOT NULL, note BIGINT)")
    insert(conn, "orders", _ORDERS)
    # The HAVING source: one column of each shape the compiled predicate has a
    # separate arm for — a nullable TEXT, a U64 crossing i64::MAX, a narrow
    # SMALLINT, a NOT NULL BIGINT and a nullable one.
    conn.execute_sql(
        "CREATE TABLE hg (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL, s TEXT,"
        "                 u BIGINT UNSIGNED NOT NULL, sm SMALLINT, nn BIGINT NOT NULL,"
        "                 v BIGINT); "
        "INSERT INTO hg VALUES "
        "(1, 1, 'a',  9223372036854775808, -5,   10, 1),"
        "(2, 1, 'a',  9223372036854775808, -1,   20, NULL),"
        "(3, 2, 'b',  5,                    3,   30, 7),"
        "(4, 2, NULL, 5,                    NULL, 40, NULL),"
        "(5, 3, 'c',  18446744073709551615, 9,   50, NULL),"
        "(6, 3, 'c',  18446744073709551615, 2,   60, NULL)")
    # A DOUBLE and a TEXT column ahead of the PK.
    conn.execute_sql(
        "CREATE TABLE nl (x DOUBLE NOT NULL, s TEXT NOT NULL, id BIGINT NOT NULL,"
        " v BIGINT NOT NULL, PRIMARY KEY (id))")
    insert(conn, "nl", _NL)
    conn.execute_sql(
        "CREATE TABLE wx (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL,"
        " label TEXT NOT NULL, u UUID NOT NULL)")
    insert(conn, "wx", _WX)
    conn.execute_sql(
        "CREATE TABLE rt (pk BIGINT PRIMARY KEY, c BIGINT NOT NULL, a BIGINT NOT NULL) "
        "WITH (replicated = true)")
    insert(conn, "rt", _RT)
    # The DISTINCT sources: `u` repeating its cities, `c` whose computed sums
    # collide, `dup` a join view naming `pk` and `a` twice, and `w` a group wider
    # than 8 columns.
    conn.execute_sql(
        "CREATE TABLE u (pk BIGINT PRIMARY KEY, city TEXT, region BIGINT); "
        "INSERT INTO u VALUES (1,'nyc',1),(2,'sf',2),(3,'nyc',1),(4,'sf',2),(5,'la',3),"
        "(6,'nyc',1); "
        "CREATE TABLE c (pk BIGINT PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "INSERT INTO c VALUES (1,1,4),(2,2,3),(3,5,0),(4,1,4); "
        "CREATE TABLE d (pk BIGINT PRIMARY KEY, a BIGINT NOT NULL); "
        "INSERT INTO d VALUES (1, 1), (2, 2); "
        "CREATE VIEW dup AS SELECT * FROM c JOIN d ON c.a = d.pk; "
        "CREATE TABLE w (pk BIGINT PRIMARY KEY, " + ", ".join(f"c{i} BIGINT" for i in range(10)) + ")")
    insert(conn, "w", [(1, *range(10)), (2, *range(10)), (3, *range(1, 11))])
    # An empty source; a UNION ALL view whose rows weigh 2; eight aggregated
    # columns; a source at the column limit.
    conn.execute_sql(
        "CREATE TABLE e (pk BIGINT PRIMARY KEY, v BIGINT); "
        "CREATE TABLE base (pk BIGINT PRIMARY KEY, v BIGINT); "
        "INSERT INTO base VALUES (1, 10), (2, 20), (3, 10); "
        "CREATE VIEW dbl AS SELECT pk, v FROM base UNION ALL SELECT pk, v FROM base; "
        "CREATE TABLE m8 (pk BIGINT PRIMARY KEY, g BIGINT NOT NULL, "
        + ", ".join(f"c{i} BIGINT" for i in range(8)) + "); "
        f"CREATE TABLE {_WMAX} (pk BIGINT PRIMARY KEY, "
        + ", ".join(f"c{i} BIGINT NOT NULL" for i in range(gnitz.MAX_COLUMNS - 1)) + ")")
    insert(conn, "m8", _M8)
    insert(conn, _WMAX, [range(gnitz.MAX_COLUMNS)])
    return conn


# ---------------------------------------------------------------------------
# Every parity case: `(query, the oracle's bag)`
# ---------------------------------------------------------------------------


# Each aggregate over `_ORDERS`: its kind and the column it reads.
_AGGS = {"COUNT(*)": ("COUNT*", 0), "COUNT(note)": ("COUNT", 3), "SUM(amount)": ("SUM", 2),
         "MIN(amount)": ("MIN", 2), "MAX(amount)": ("MAX", 2), "AVG(amount)": ("AVG", 2)}


def _fold(name, g):
    kind, col = _AGGS[name]
    return agg(kind, [r[col] for r in g])


_I64_MAX, _U64_MAX, _I64_MIN_U = 2**63 - 1, 2**64 - 1, 2**63

_CASES = [
    # Every aggregate, grouped and global, with and without a WHERE that pushes a
    # bounded PK range plus a residual, and one that keeps no row.
    *((f"SELECT category, {a} AS m FROM orders{where} GROUP BY category",
       _by([r for r in _ORDERS if keep(r)], lambda r: r[1], lambda g, a=a: _fold(a, g)))
      for a in _AGGS
      for where, keep in [("", lambda r: True),
                          (" WHERE pk > 5 AND amount < 80", lambda r: r[0] > 5 and r[2] < 80),
                          (" WHERE pk > 1000", lambda r: False)]),
    *((f"SELECT {a} AS m FROM orders{where}", {(_fold(a, [r for r in _ORDERS if keep(r)]),): 1})
      for a in _AGGS
      for where, keep in [("", lambda r: True),
                          (" WHERE category = 2", lambda r: r[1] == 2),
                          (" WHERE pk > 1000", lambda r: False)]),

    # Computed group keys and computed finalize items. A computed GROUP BY key or
    # aggregate argument is the **pre-map**, which runs on the worker between the
    # predicate and the fold; an expression over the aggregates is the **finalize
    # map**, which the client runs over its combined reduce output.
    *((f"SELECT {select} FROM orders GROUP BY {group}", _by(_ORDERS, key, value))
      for select, group, key, value in [
          # An expression whose operand is an aggregate — the finalize map.
          ("category, SUM(amount) + 1 AS m", "category",
           lambda r: r[1], lambda g: sum(r[2] for r in g) + 1),
          # Two aggregates in one expression, and an aggregate under a second operator.
          ("category, SUM(amount) - COUNT(*) * 2 AS m", "category",
           lambda r: r[1], lambda g: sum(r[2] for r in g) - len(g) * 2),
          # AVG's own divide feeding another operator.
          ("category, AVG(amount) * 2 AS m", "category",
           lambda r: r[1], lambda g: sum(r[2] for r in g) / len(g) * 2),
          # A group column inside an expression, mixed with an aggregate.
          ("category, category * 100 + COUNT(*) AS m", "category",
           lambda r: r[1], lambda g: g[0][1] * 100 + len(g)),
          # An aggregate whose argument is computed — the pre-map.
          ("category, SUM(amount * category) AS m", "category",
           lambda r: r[1], lambda g: sum(r[2] * r[1] for r in g)),
          # A pre-map column feeding a non-linear aggregate.
          ("category, MAX(amount + pk) AS m", "category",
           lambda r: r[1], lambda g: max(r[2] + r[0] for r in g)),
          # A GROUP BY over an expression — the pre-map materializing the key.
          ("category + amount AS k, COUNT(*) AS m", "category + amount",
           lambda r: r[1] + r[2], len),
          # A computed key and a computed aggregate argument in one reduce.
          ("category + 1 AS k, SUM(amount * 3) AS m", "category + 1",
           lambda r: r[1] + 1, lambda g: sum(r[2] * 3 for r in g)),
      ]),
    # The written key reaches the SELECT list and HAVING by its expression, not
    # by a name.
    ("SELECT amount * 2 AS k, SUM(pk) AS s FROM orders GROUP BY amount * 2 HAVING SUM(pk) > 20",
     {(k, s): 1 for (k, s) in _by(_ORDERS, lambda r: r[2] * 2, lambda g: sum(r[0] for r in g))
      if s > 20}),
    ("SELECT category + amount AS k, category + amount AS again, COUNT(*) AS c "
     "FROM orders GROUP BY category + amount",
     {(k, k, c): 1 for k, c in _by(_ORDERS, lambda r: r[1] + r[2], len)}),
    # The pre-map carries only the PK and what the fold reads, so a computed key
    # over a maximally wide source still fits.
    (f"SELECT c0 + 1 AS k, COUNT(*) AS c FROM {_WMAX} GROUP BY c0 + 1", {(2, 1): 1}),
    # The pre-map's output schema is the source's columns with the PK moved to
    # the front, so over `nl` it is a *permutation* of the source — and a WHERE
    # is resolved to positions against the source, not that permutation. The
    # columns ahead of the PK are a TEXT and a DOUBLE because the symptom of
    # confusing the two orders is a *type* mismatch at the permuted slot: the
    # TEXT is type-rejected, the DOUBLE fails in the engine.
    *((f"SELECT {select} FROM nl WHERE {where} GROUP BY {group}",
       _by([r for r in _NL if keep(r)], key, value))
      for where, select, group, keep, key, value in [
          ("s = 'a'", "v + 1 AS k, COUNT(*) AS m", "v + 1",
           lambda r: r[1] == "a", lambda r: r[3] + 1, len),
          ("x > 6.0", "v AS k, SUM(id * 2) AS m", "v",
           lambda r: r[0] > 6.0, lambda r: r[3], lambda g: sum(r[2] * 2 for r in g)),
          # The pre-map reached by a computed argument rather than by the key.
          ("s = 'b'", "v AS k, MAX(id * 3) AS m", "v",
           lambda r: r[1] == "b", lambda r: r[3], lambda g: max(r[2] * 3 for r in g)),
      ]),
    # Over a replicated source every worker holds every row, so a fold that
    # double-counted would still return plausible groups: once with no pre-map,
    # once through the pre-map's derived schema.
    ("SELECT c, COUNT(*) AS n FROM rt GROUP BY c", {(0, 4): 1, (1, 4): 1, (2, 4): 1}),
    ("SELECT c + 1 AS k, SUM(a * 2) AS s FROM rt GROUP BY c + 1",
     _by(_RT, lambda r: r[1] + 1, lambda g: sum(r[2] * 2 for r in g))),

    # HAVING through the shared expression evaluator. The ad-hoc HAVING and a
    # grouped view's post-reduce filter run one compiled program, so parity
    # cannot catch a wrong *expression* result; what it still discriminates is
    # the surrounding shape: where the group key lives (a view's PK region
    # against the fold's payload slot), the nullability each side declares, and
    # the client's reduce-output batch against the engine's.
    #
    # A single NOT NULL U64 group column: the view keeps it in its PK region,
    # ad-hoc in a payload slot. Also pins the unsigned comparison.
    (f"SELECT u, COUNT(*) AS c FROM hg GROUP BY u HAVING u > {_I64_MAX}",
     {(_I64_MIN_U, 2): 1, (_U64_MAX, 2): 1}),
    # Every U64 value is above a negative literal, on both paths.
    ("SELECT u, COUNT(*) AS c FROM hg GROUP BY u HAVING u > -1",
     {(5, 2): 1, (_I64_MIN_U, 2): 1, (_U64_MAX, 2): 1}),
    # A NOT NULL aggregate source, grouped: the two sides declare its
    # nullability differently, so they take different evaluation arms.
    ("SELECT cat, SUM(nn) AS t FROM hg GROUP BY cat HAVING SUM(nn) > 0",
     {(1, 30): 1, (2, 70): 1, (3, 110): 1}),
    # The only narrow (2-byte) aggregate partial: MIN over SMALLINT keeps the
    # source width, so the value round-trips through sign extension.
    ("SELECT cat, MIN(sm) AS m FROM hg GROUP BY cat HAVING MIN(sm) < 0", {(1, -5): 1}),
    # Strings, IS NULL, a CASE, and IN over both a string and an aggregate.
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s = 'a'", {("a", 2): 1}),
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s > 'a'", {("b", 1): 1, ("c", 2): 1}),
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s IN ('a', 'c')", {("a", 2): 1, ("c", 2): 1}),
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s IS NULL", {(None, 1): 1}),
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s IS NOT NULL",
     {("a", 2): 1, ("b", 1): 1, ("c", 2): 1}),
    ("SELECT cat, MIN(v) AS m FROM hg GROUP BY cat HAVING MIN(v) IS NULL", {(3, None): 1}),
    ("SELECT cat, MIN(v) AS m FROM hg GROUP BY cat HAVING MIN(v) IN (1, 7)", {(1, 1): 1, (2, 7): 1}),
    ("SELECT cat, COUNT(*) AS c FROM hg GROUP BY cat "
     "HAVING CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1", {(1, 2): 1, (2, 2): 1, (3, 2): 1}),
    # A scalar function wrapping the aggregate.
    ("SELECT cat, SUM(nn) AS t FROM hg GROUP BY cat HAVING ABS(SUM(nn)) > 0",
     {(1, 30): 1, (2, 70): 1, (3, 110): 1}),
    # Constant predicates: TRUE folds away at plan time, FALSE compiles to a
    # constant that drops every group, NULL is UNKNOWN and never true, and a
    # statically-false one drops everything.
    ("SELECT cat FROM hg GROUP BY cat HAVING TRUE", {(1,): 1, (2,): 1, (3,): 1}),
    ("SELECT cat FROM hg GROUP BY cat HAVING FALSE", {}),
    ("SELECT cat FROM hg GROUP BY cat HAVING NULL", {}),
    ("SELECT cat FROM hg GROUP BY cat HAVING 'a' = 'b'", {}),
    # An aggregate appearing only in HAVING: all groups dropped, all kept, and
    # several filter ranges rather than one.
    ("SELECT cat FROM hg GROUP BY cat HAVING COUNT(*) > 99", {}),
    ("SELECT cat FROM hg GROUP BY cat HAVING COUNT(*) > 0", {(1,): 1, (2,): 1, (3,): 1}),
    ("SELECT cat FROM hg GROUP BY cat HAVING cat <> 2", {(1,): 1, (3,): 1}),
    # A HAVING with no GROUP BY groups the whole relation, even when the
    # projection carries no aggregate or none is written at all.
    ("SELECT COUNT(*) AS c FROM hg HAVING COUNT(*) > 0", {(6,): 1}),
    ("SELECT 1 AS one FROM hg HAVING SUM(cat) > 1", {(1,): 1}),
    ("SELECT 1 AS one FROM hg HAVING 1 = 1", {(1,): 1}),
    # A NULL group forms its own group, and a group whose aggregated column is
    # entirely NULL still emits.
    ("SELECT s, COUNT(v) AS cv, SUM(v) AS sv, MIN(v) AS mv FROM hg GROUP BY s",
     {("a", 1, 1, 1): 1, ("b", 1, 7, 7): 1, ("c", 0, None, None): 1, (None, 0, None, None): 1}),

    # AVG reads its SUM accumulator at the declared type. The client combines
    # integer SUM partials into an i64 whose bit pattern is the sum mod 2^64. A
    # SUM over a `BIGINT UNSIGNED` source is declared U64, so past 2^63 that bit
    # pattern reads correctly only as unsigned — and AVG divides the value,
    # where SUM's render only reinterprets its bits. cat = 1's two 2^63 cells
    # wrap to 0; cat = 3 sums two 2^64-1 cells to 2^64-2, which has no exact f64
    # image and rounds to 2^64, so its average is 2^63 — and a signed read of
    # the accumulator would render it negative.
    ("SELECT cat, AVG(u) AS a FROM hg GROUP BY cat",
     {(1, 0.0): 1, (2, 5.0): 1, (3, 9.223372036854776e18): 1}),
    # The same accumulator with no group columns at all.
    ("SELECT AVG(u) AS a FROM hg WHERE cat = 3", {(9.223372036854776e18,): 1}),
    # One 2^64-1 cell does not wrap, so its true average is representable.
    ("SELECT AVG(u) AS a FROM hg WHERE pk = 5", {(1.8446744073709552e19,): 1}),
    # HAVING and the render read one accumulator, so a mis-signed render
    # disagrees with the predicate that kept it.
    ("SELECT cat, AVG(u) AS a FROM hg GROUP BY cat HAVING AVG(u) > 1.0",
     {(2, 5.0): 1, (3, 9.223372036854776e18): 1}),
    # SUM's raw-bits render wraps to the u64.
    ("SELECT cat, SUM(u) AS t FROM hg WHERE cat = 3 GROUP BY cat", {(3, _U64_MAX - 1): 1}),
    # What the unsigned read must not disturb: a signed source (every non-U64
    # integer widens to the same signed accumulator), an all-NULL group, and the
    # global ground row.
    ("SELECT cat, AVG(sm) AS a FROM hg GROUP BY cat", {(1, -3.0): 1, (2, 3.0): 1, (3, 5.5): 1}),
    ("SELECT cat, AVG(v) AS a FROM hg GROUP BY cat", {(1, 1.0): 1, (2, 7.0): 1, (3, None): 1}),
    ("SELECT AVG(v) AS a FROM hg WHERE cat = 99", {(None,): 1}),

    # DISTINCT is the weight-clamp primitive, so what it owes is not a row set but
    # a weight: every survivor at exactly 1, however many source rows folded into
    # it. `SELECT ALL` spells the default quantifier: the bag.
    ("SELECT DISTINCT city FROM u", {("nyc",): 1, ("sf",): 1, ("la",): 1}),
    ("SELECT ALL city FROM u", {("nyc",): 3, ("sf",): 2, ("la",): 1}),
    ("SELECT ALL city, COUNT(*) AS n FROM u GROUP BY city", {("nyc", 3): 1, ("sf", 2): 1, ("la", 1): 1}),
    ("SELECT DISTINCT city, region FROM u", {("nyc", 1): 1, ("sf", 2): 1, ("la", 3): 1}),
    ("SELECT DISTINCT region FROM u WHERE region > 1", {(2,): 1, (3,): 1}),
    # A computed set-identity key: rows 1, 2 and 4 all sum to 5, so the three
    # collapse to one survivor.
    ("SELECT DISTINCT a + b AS s FROM c", {(5,): 1}),
    ("SELECT DISTINCT a, a + b AS s FROM c", {(1, 5): 1, (2, 5): 1, (5, 5): 1}),
    # A name-preserving wildcard names nothing of its own, so a duplicate among
    # the source's names rides through positionally.
    ("SELECT DISTINCT * FROM dup", {(1, 1, 4, 1, 1): 1, (2, 2, 3, 2, 2): 1, (4, 1, 4, 1, 1): 1}),
    # A >8-column group collapses the duplicate pair.
    ("SELECT DISTINCT " + ", ".join(f"c{i}" for i in range(10)) + " FROM w",
     {tuple(range(10)): 1, tuple(range(1, 11)): 1}),

    # An empty source grounds a global fold but not a grouped one, and HAVING
    # still filters the ground row: a NULL aggregate drops it, COUNT keeps it.
    ("SELECT COUNT(*) AS c, SUM(v) AS s, MAX(v) AS m FROM e", {(0, None, None): 1}),
    ("SELECT v, COUNT(*) AS c FROM e GROUP BY v", {}),
    ("SELECT COUNT(*) AS c FROM e HAVING SUM(v) = 0", {}),
    ("SELECT COUNT(*) AS c FROM e HAVING MAX(v) >= 0", {}),
    ("SELECT COUNT(*) AS c FROM e HAVING COUNT(*) = 0", {(0,): 1}),
    # Over a bag, COUNT(*) counts the multiplicity, not the entries.
    ("SELECT COUNT(*) AS c FROM dbl", {(6,): 1}),
    ("SELECT v, COUNT(*) AS c FROM dbl GROUP BY v", {(10, 4): 1, (20, 2): 1}),
    # Every accumulator kind at once over eight columns, with a group column.
    ("SELECT g, " + ", ".join(f"SUM(c{i}) AS s{i}, MIN(c{i}) AS l{i}, MAX(c{i}) AS m{i}, "
                              f"COUNT(c{i}) AS n{i}" for i in range(8)) + " FROM m8 GROUP BY g",
     {(k, *(agg(f, [r[2 + j] for r in g]) for j in range(8) for f in ("SUM", "MIN", "MAX", "COUNT"))): 1
      for k, g in groups(_M8, lambda r: r[1]).items()}),

    # MIN/MAX over a TEXT or UUID column: every worker's partial carries the
    # value itself, and the client's combine orders them as the engine does —
    # strings by content, past the inline bound and across a shared prefix. An
    # empty source still grounds the global extreme to NULL.
    *(case for a, col in [("MIN(label)", 2), ("MAX(label)", 2), ("MIN(u)", 3), ("MAX(u)", 3)]
      for pick in [min if a.startswith("MIN") else max]
      for case in [
          (f"SELECT g, {a} AS m FROM wx GROUP BY g",
           _by(_WX, lambda r: r[1], lambda g, col=col, pick=pick: pick(r[col] for r in g))),
          (f"SELECT {a} AS m FROM wx", {(pick(r[col] for r in _WX),): 1}),
          (f"SELECT {a} AS m FROM wx WHERE pk > 100", {(None,): 1}),
      ]),
    # A TEXT group column some of whose values spill past the German-string
    # inline bound: it is copied out of the fold's group batch into the result,
    # where a spilled cell's heap offset means nothing — a verbatim copy of the
    # 16-byte struct hands back an empty string for exactly those values.
    ("SELECT label, SUM(pk) AS s FROM wx GROUP BY label",
     _by(_WX, lambda r: r[2], lambda g: sum(r[0] for r in g))),
]


@pytest.mark.parametrize("query,want", _CASES, ids=[q for q, _ in _CASES])
def test_an_adhoc_fold_is_its_views_fold(src, query, want):
    assert _parity(src, query) == want


def test_the_register_cap_is_located_not_pinned(src):
    """The register cap on the direct path, found rather than hardcoded."""
    def having(k):
        return f"SELECT cat FROM hg GROUP BY cat HAVING {conjunct_ladder('cat', k)}"

    k = first_rejected(lambda k: rows(src, having(k)), range(1, 64))
    assert _parity(src, having(k - 1)) == {(1,): 1}

    def having_in(n):
        items = ", ".join(repr(v) for v in ["a", *(f"v{i}" for i in range(1, n))])
        return f"SELECT s FROM hg GROUP BY s HAVING s IN ({items})"

    n = first_rejected(lambda n: rows(src, having_in(n)), range(1, 64))
    assert _parity(src, having_in(n - 1)) == {("a",): 1}


def test_the_per_worker_group_cap_aborts_and_the_worker_keeps_serving(own_server):
    # Not a debug-only seam: the cap is read at bootstrap on every build.
    with gnitz.connect(own_server.start(extra_env={"GNITZ_ADHOC_GROUP_CAP": "4"}).target) as client:
        client.execute_sql("CREATE TABLE t (pk BIGINT PRIMARY KEY, g BIGINT NOT NULL)")
        # 100 distinct groups; with the cap at 4 per worker, some worker exceeds it.
        insert(client, "t", [(i, i) for i in range(1, 101)])
        with pytest.raises(gnitz.GnitzRefusedError, match="CREATE VIEW"):
            rows(client, "SELECT g, COUNT(*) AS c FROM t GROUP BY g")
        assert bag(rows(client, "SELECT COUNT(*) AS c FROM t")) == {(100,): 1}
        # Rows already a set read as rows: no fold, so no cap.
        every_row = {(i, i): 1 for i in range(1, 101)}
        assert bag(rows(client, "SELECT DISTINCT * FROM t")) == every_row
        assert bag(rows(client, "SELECT DISTINCT pk, g FROM t")) == every_row


# ---------------------------------------------------------------------------
# Which rows a tied cut returns is a function of (data, query) alone
#
# The ad-hoc output's hidden output key leads the ordering sink's identity
# tiebreak, so it fully decides every tie the ORDER BY keys leave open. It is the
# engine's key for the group — a pure function of the group's column values —
# carried out of the partial reply.
# ---------------------------------------------------------------------------


def _tied_groups(client):
    """16 groups of 3 rows each: every COUNT is tied, so an ORDER BY over the
    count leaves the whole result set for the tiebreak to order. `h` gives the
    DISTINCT case a tied non-key column to order by."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, h BIGINT NOT NULL)")
    insert(client, "t", [(i, i % 16, i % 2) for i in range(1, 49)])


def test_a_tied_cut_is_worker_count_independent(own_server):
    """16 fully-tied groups and a `LIMIT 4`: the cut is entirely the tiebreak's
    to decide, so the same four groups must come back when the same data is
    served by a different worker count."""
    queries = {
        "asc": "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY c LIMIT 4",
        "desc": "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY c DESC LIMIT 4",
        "distinct": "SELECT DISTINCT g, h FROM t ORDER BY h LIMIT 4",
    }
    with gnitz.connect(own_server.start(workers=2).target) as client:
        _tied_groups(client)
        two = {k: ordered(rows(client, q)) for k, q in queries.items()}
    with gnitz.connect(own_server.restart(graceful=True, workers=4).target) as client:
        four = {k: ordered(rows(client, q)) for k, q in queries.items()}
    assert two == four
    # A single non-nullable integer group column keys on the order-preserving
    # route key, so the GROUP BY tie breaks ascending by `g` — in both
    # directions, because tiebreak keys are always ASC. DISTINCT over two
    # columns keys on a digest: deterministic, but arbitrary with respect to
    # the data.
    for k in ("asc", "desc"):
        assert two[k] == [((("g", g), ("c", 3)), 1) for g in range(4)], k


def test_a_tied_cut_agrees_with_a_views_cut(client):
    """The ad-hoc cut and a cut read off a view of the same body return the
    *same* tied rows, in order. Over a single non-nullable integer group column
    a view keys on that column's OPK and the fold on a wider group key — not the
    same bytes, but the route key is order-preserving, so both induce the
    identical order; over two group columns both key on the digest. (An ad-hoc
    `SELECT DISTINCT` keys its ties off the reduce group key and a view's off a
    content digest, so those two do not agree.)"""
    _tied_groups(client)
    for body, tails in [
        ("SELECT g, COUNT(*) AS c FROM t GROUP BY g",
         ["ORDER BY c LIMIT 4", "ORDER BY c DESC LIMIT 4", "ORDER BY c OFFSET 5 LIMIT 6"]),
        ("SELECT g, h, COUNT(*) AS c FROM t GROUP BY g, h", ["ORDER BY c LIMIT 5"]),
    ]:
        vn = "po_" + uid()
        client.execute_sql(f"CREATE VIEW {vn} AS {body}")
        for tail in tails:
            assert ordered(rows(client, f"{body} {tail}")) == \
                ordered(rows(client, f"SELECT * FROM {vn} {tail}")), f"{body} {tail}"


def test_order_by_an_aggregate_by_alias_call_and_position(src):
    """ORDER BY an aggregate or an expression over the grouped relation binds
    like a SELECT item, by call or by alias alike."""
    # Every category's sum is distinct, so the aggregate key alone decides the
    # order and the category tiebreak never fires.
    sums = _by(_ORDERS, lambda r: r[1], lambda g: sum(r[2] for r in g))
    want = [(cs, 1) for cs in sorted(sums, key=lambda cs: -cs[1])]
    by_call = rows(src,
                   "SELECT category, SUM(amount) FROM orders GROUP BY category "
                   "ORDER BY SUM(amount) DESC, category")
    by_alias = rows(src,
                    "SELECT category, SUM(amount) AS s FROM orders GROUP BY category "
                    "ORDER BY s DESC, category")
    assert ordered(by_call, *by_call[0]._fields) == ordered(by_alias, "category", "s") == want
    positional = rows(src,
                      "SELECT category, COUNT(*) AS c FROM orders GROUP BY category ORDER BY 1")
    assert ordered(positional, "category", "c") == [((c, 8), 1) for c in range(5)]


# ---------------------------------------------------------------------------
# The float aggregate value contract
#
# A float SUM / AVG is a function of the summation order, so of (query, data,
# worker count, access path, scan chunk size). It is stable for a fixed
# deployment and a fixed plan — which is the guarantee, and is what this pins.
# ---------------------------------------------------------------------------


def _bits(rows):
    """Each `s` as its exact bit pattern — `float.hex()` round-trips losslessly,
    so two sums compare equal only if they are the same double."""
    return [r.s.hex() for r in rows]


def test_a_float_sum_repeats_and_an_index_keeps_the_exact_answer(own_server):
    """Under a full scan and then under an index-bounded walk, which sorts and
    dedups PKs one 3-row chunk at a time: the rows and the aggregates exact over
    them are the oracle's, and a float SUM repeats bit for bit under each plan.
    The two plans' float sums are never compared: the access path is part of the
    summation order."""
    rnd = random.Random(11)
    # 2000 rows over 500 categories, so `cat >= 5 AND cat < 13` is 1.6% of the
    # table — selective enough to clear the index cost gate, and wide enough
    # that each worker's share spans several chunks. Alternating ±1e15 and 1e-8
    # magnitudes cancel catastrophically, so any reassociation moves the low bits.
    data = [(i, (i * 37) % 500, (1e15 if i % 4 == 0 else -1e15 if i % 4 == 1 else 1e-8)
             * (1 + rnd.random())) for i in range(1, 2001)]
    sel = [(pk, x) for pk, cat, x in data if 5 <= cat < 13]
    src = "FROM f WHERE cat >= 5 AND cat < 13"
    rows_q = f"SELECT pk, x {src}"
    exact_q = f"SELECT COUNT(*) AS c, SUM(pk) AS sp, MIN(x) AS mn, MAX(x) AS mx {src}"
    float_q = f"SELECT SUM(x) AS s {src}"
    repeat_qs = (float_q, "SELECT SUM(x) AS s FROM f", "SELECT cat, SUM(x) AS s FROM f GROUP BY cat",
                 "SELECT cat, AVG(x) AS s FROM f GROUP BY cat")
    with gnitz.connect(own_server.start(extra_env=TINY_SCAN_CHUNKS).target) as client:
        client.execute_sql(
            "CREATE TABLE f (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL, x DOUBLE NOT NULL)")
        insert(client, "f", data)
        for walk in ("full scan", "index range on (cat)"):
            if walk != "full scan":
                client.execute_sql("CREATE INDEX fi ON f (cat)")
            # One access planner serves the rows sink and the fold sink alike.
            for q in (rows_q, exact_q, float_q):
                assert access(client, q) == walk, q
            assert bag(rows(client, rows_q)) == dict.fromkeys(sel, 1)
            assert bag(rows(client, exact_q)) == \
                {(len(sel), sum(pk for pk, _ in sel), min(x for _, x in sel), max(x for _, x in sel)): 1}
            for q in repeat_qs:
                assert _bits(rows(client, q)) == _bits(rows(client, q)), f"`{q}` is not repeatable"
