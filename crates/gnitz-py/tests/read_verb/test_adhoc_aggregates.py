"""Ad-hoc single-relation aggregation (GROUP BY / global aggregate / HAVING /
DISTINCT) served by the ReadSpec aggregate hash-fold sink + client finishing,
versus a `CREATE VIEW` of the same statement.

The central assertion is **parity**: for a query with no ORDER BY / LIMIT, the
ad-hoc result equals a scan of `CREATE VIEW v AS <same query>`, exactly — every
fixture here aggregates an integer source, so both paths agree bit for bit.
Parity is compared as a weighted bag, so a group emitted twice by one path is a
divergence rather than a matching row set. A float SUM/AVG instead follows the
summation order; the tests at the bottom pin what that does and does not
promise.

Run with GNITZ_WORKERS=4 (the fold is per-worker; the client merges partials).
"""
import random

import gnitz
import pytest
from _caps import conjunct_ladder, first_rejected
from _read import access, bag, ordered, rows, scanned
from _sql import insert
from _uid import uid as _uid


def _parity(client, sn, query):
    """Assert the ad-hoc result of `query` (no ORDER BY / LIMIT) equals a scan of
    a view built from the identical statement, as weighted bags, and return the
    ad-hoc rows. The view is left for the schema's teardown."""
    adhoc = rows(client, sn, query)
    vn = "pv_" + _uid()
    client.execute_sql(f"CREATE VIEW {vn} AS {query}", schema_name=sn)
    view = scanned(client, sn, vn)

    assert bag(adhoc) == bag(view), f"ad-hoc != view for `{query}`"
    if adhoc and view:
        assert set(adhoc[0]._asdict()) == set(view[0]._asdict()), \
            f"column names differ for `{query}`"
    return adhoc


def _parity_ordered(client, sn, body, *tails):
    """Assert the ad-hoc result of `body tail` equals reading a view of `body`
    with the same `tail` applied — row for row, in order, for each of `tails`
    against one shared view — and return the first ad-hoc result.

    `CREATE VIEW` rejects ORDER BY, which is what makes `_parity` blind to tie
    order; here the cut runs as a second read over a view of the body, so the
    client's tiebreak is compared against the worker's.

    GROUP BY only. An ad-hoc `SELECT DISTINCT` keys its ties off the reduce group
    key and a view's DISTINCT off a content digest, so each is deterministic but
    they do not agree; for DISTINCT the guarantee is cross-worker-count
    determinism alone.
    """
    vn = "po_" + _uid()
    client.execute_sql(f"CREATE VIEW {vn} AS {body}", schema_name=sn)
    out = []
    for tail in tails:
        adhoc = rows(client, sn, f"{body} {tail}")
        view = rows(client, sn, f"SELECT * FROM {vn} {tail}")
        assert ordered(adhoc) == ordered(view), f"ordered ad-hoc != view for `{body} {tail}`"
        out.append(adhoc)
    return out[0]


def _reject_both(client, sn, query):
    """Assert `query` is rejected on the direct path AND as a CREATE VIEW."""
    with pytest.raises(gnitz.GnitzError):
        rows(client, sn, query)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(f"CREATE VIEW rv_{_uid()} AS {query}", schema_name=sn)


def _grouped_oracle(src, key, value):
    """`{(key(group), value(rows of that group)): 1}` over `src` — the weighted
    bag a `SELECT key, value … GROUP BY key` owes."""
    groups = {}
    for row in src:
        groups.setdefault(key(row), []).append(row)
    return {(k, value(g)): 1 for k, g in groups.items()}


# ---------------------------------------------------------------------------
# The shared read-only sources
#
# Module-scoped: every test sharing one only reads from it, and a `_parity` view
# is created under a unique name. Each oracle computes over the same rows the
# fixture inserts, so changing a fixture cannot leave an expectation quietly
# describing the old data.
# ---------------------------------------------------------------------------


# `(pk, category, amount, note)`.
_ORDERS = [(i, i % 5, (i * 7) % 100, None if i % 3 == 0 else (i * 3) % 50) for i in range(1, 41)]
# `(x, s, id, v)`, PK `id` declared third.
_NL = [(i * 1.5, "a" if i % 2 else "b", i, i % 4) for i in range(1, 13)]
# `(pk, g, label, u)`.
_WX_LABELS = ["shared/prefix/aa", "shared/prefix/a", "shared/prefix/ab", "b", ""]
_WX = [(i, i % 3, _WX_LABELS[i % 5], f"550e8400-e29b-41d4-a716-4466554400{i:02d}")
       for i in range(20)]


@pytest.fixture(scope="module")
def orders(module_schema):
    conn, sn = module_schema
    conn.execute_sql(
        "CREATE TABLE orders (pk BIGINT NOT NULL PRIMARY KEY, category BIGINT NOT NULL,"
        " amount BIGINT NOT NULL, note BIGINT)", schema_name=sn)
    insert(conn, sn, "orders", _ORDERS)
    return sn


@pytest.fixture(scope="module")
def hg(module_schema):
    """The HAVING source: one column of each shape the compiled predicate has a
    separate arm for — a nullable TEXT, a U64 crossing i64::MAX, a narrow
    SMALLINT, a NOT NULL BIGINT and a nullable one."""
    conn, sn = module_schema
    conn.execute_sql(
        "CREATE TABLE hg (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL, s TEXT,"
        "                 u BIGINT UNSIGNED NOT NULL, sm SMALLINT, nn BIGINT NOT NULL,"
        "                 v BIGINT)", schema_name=sn)
    conn.execute_sql(
        "INSERT INTO hg VALUES "
        "(1, 1, 'a',  9223372036854775808, -5,   10, 1),"
        "(2, 1, 'a',  9223372036854775808, -1,   20, NULL),"
        "(3, 2, 'b',  5,                    3,   30, 7),"
        "(4, 2, NULL, 5,                    NULL, 40, NULL),"
        "(5, 3, 'c',  18446744073709551615, 9,   50, NULL),"
        "(6, 3, 'c',  18446744073709551615, 2,   60, NULL)", schema_name=sn)
    return sn


@pytest.fixture(scope="module")
def nl(module_schema):
    """`nl (x DOUBLE, s TEXT, id, v)` — a DOUBLE and a TEXT column ahead of the
    PK, holding `_NL`."""
    conn, sn = module_schema
    conn.execute_sql(
        "CREATE TABLE nl (x DOUBLE NOT NULL, s TEXT NOT NULL, id BIGINT NOT NULL,"
        " v BIGINT NOT NULL, PRIMARY KEY (id))", schema_name=sn)
    insert(conn, sn, "nl", _NL)
    return sn


@pytest.fixture(scope="module")
def wx(module_schema):
    """`wx (pk, g, label TEXT, u UUID)` holding `_WX`."""
    conn, sn = module_schema
    conn.execute_sql(
        "CREATE TABLE wx (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL,"
        " label TEXT NOT NULL, u UUID NOT NULL)", schema_name=sn)
    insert(conn, sn, "wx", _WX)
    return sn


@pytest.fixture(scope="module")
def gg(module_schema):
    """An empty `gg (pk, price DOUBLE, n)`: every grouped guard is a planning
    refusal, so no row is needed to reach one."""
    conn, sn = module_schema
    conn.execute_sql(
        "CREATE TABLE gg (pk BIGINT PRIMARY KEY, price DOUBLE NOT NULL, n BIGINT NOT NULL)",
        schema_name=sn)
    return sn


# ---------------------------------------------------------------------------
# Grouped + global aggregate parity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("agg", ["COUNT(*) AS m", "COUNT(note) AS m", "SUM(amount) AS m",
                                 "MIN(amount) AS m", "MAX(amount) AS m", "AVG(amount) AS m"])
@pytest.mark.parametrize("select,tail", [
    ("category, {agg}", "GROUP BY category"),
    ("category, {agg}", "WHERE pk > 5 AND amount < 80 GROUP BY category"),
    ("{agg}", ""),
    ("{agg}", "WHERE category = 2"),
], ids=["grouped", "grouped-where", "global", "global-where"])
def test_aggregate_parity(client, orders, agg, select, tail):
    """Every aggregate, grouped and global, with and without a WHERE that pushes
    a bounded PK range plus a residual."""
    _parity(client, orders, f"SELECT {select.format(agg=agg)} FROM orders {tail}")


# ---------------------------------------------------------------------------
# Computed group keys and computed finalize items
#
# A computed GROUP BY key or aggregate argument is the **pre-map**, which runs on
# the worker between the predicate and the fold; an expression over the
# aggregates is the **finalize map**, which the client runs over its combined
# reduce output. Parity alone would also hold if both paths were wrong, so each
# case is pinned against an in-Python oracle too.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("select,group,key,value", [
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
])
def test_computed_key_and_finalize_parity(client, orders, select, group, key, value):
    kname = "k" if " AS k" in select else "category"
    got = _parity(client, orders, f"SELECT {select} FROM orders GROUP BY {group}")
    assert bag(got, kname, "m") == _grouped_oracle(_ORDERS, key, value)


def test_a_computed_key_reaches_every_position_as_one_column(client, orders):
    """The written key reaches the SELECT list and HAVING by its expression, not
    by a name, and the same expression written twice is ONE pre-map column
    rather than two."""
    got = _parity(client, orders,
                  "SELECT amount * 2 AS k, SUM(pk) AS s FROM orders "
                  "GROUP BY amount * 2 HAVING SUM(pk) > 20")
    assert bag(got, "k", "s") == {
        (k, s): 1 for (k, s) in _grouped_oracle(
            _ORDERS, lambda r: r[2] * 2, lambda g: sum(r[0] for r in g)) if s > 20}

    got = _parity(client, orders,
                  "SELECT category + amount AS k, category + amount AS again, "
                  "COUNT(*) AS c FROM orders GROUP BY category + amount")
    assert all(r.k == r.again for r in got)


def test_a_premap_over_a_source_at_the_column_limit(client, schema_name):
    """The pre-map carries only the PK and what the fold reads, so a computed key
    over a maximally wide source still fits."""
    sn = schema_name
    name = f"w{gnitz.MAX_COLUMNS}"
    payload = ", ".join(f"c{i} BIGINT NOT NULL" for i in range(gnitz.MAX_COLUMNS - 1))
    client.execute_sql(f"CREATE TABLE {name} (pk BIGINT PRIMARY KEY, {payload})", schema_name=sn)
    insert(client, sn, name, [range(gnitz.MAX_COLUMNS)])
    q = f"SELECT c0 + 1 AS k, COUNT(*) AS c FROM {name} GROUP BY c0 + 1"
    assert bag(rows(client, sn, q)) == {(2, 1): 1}


@pytest.mark.parametrize("where,select,group,keep,key,value", [
    # A TEXT column sits where the permutation puts an integer, so a predicate
    # resolved against the permuted order would be type-rejected.
    ("s = 'a'", "v + 1 AS k, COUNT(*) AS m", "v + 1",
     lambda r: r[1] == "a", lambda r: r[3] + 1, len),
    # A DOUBLE column likewise, which would fail in the engine instead.
    ("x > 6.0", "v AS k, SUM(id * 2) AS m", "v",
     lambda r: r[0] > 6.0, lambda r: r[3], lambda g: sum(r[2] * 2 for r in g)),
    # The pre-map reached by a computed argument rather than by the key.
    ("s = 'b'", "v AS k, MAX(id * 3) AS m", "v",
     lambda r: r[1] == "b", lambda r: r[3], lambda g: max(r[2] * 3 for r in g)),
])
def test_a_premap_over_a_source_whose_pk_is_not_first(
        client, nl, where, select, group, keep, key, value):
    """The pre-map's output schema is the source's columns with the PK columns
    moved to the front, so it is a *permutation* of the source whenever the PK
    is not already leading — and a WHERE is resolved to positions against the
    source, not against that permutation. Every other fixture here declares `pk`
    first, which makes the permutation the identity and hides any confusion
    between the two orders.

    The columns ahead of the PK are a DOUBLE and a TEXT because the symptom of
    that confusion is a *type* mismatch at the permuted slot, where a same-width
    integer would just compare the wrong column.
    """
    got = _parity(client, nl, f"SELECT {select} FROM nl WHERE {where} GROUP BY {group}")
    assert bag(got, "k", "m") == _grouped_oracle([r for r in _NL if keep(r)], key, value)


def test_a_computed_key_over_a_replicated_source(client, schema_name):
    """Replication is the trap for anything that touches reduce routing: every
    worker holds every row, so a fold that double-counted would still return
    plausible groups. The pre-map hands the fold a derived schema (source PK
    columns plus the computed ones), which must not change which rows a worker
    folds."""
    sn = schema_name
    src = [(i, i % 3, i) for i in range(1, 13)]
    client.execute_sql(
        "CREATE TABLE rt (pk BIGINT PRIMARY KEY, c BIGINT NOT NULL, a BIGINT NOT NULL) "
        "WITH (replicated = true)", schema_name=sn)
    insert(client, sn, "rt", src)
    # The control: no pre-map, so a double-count would show here too.
    assert bag(_parity(client, sn, "SELECT c, COUNT(*) AS n FROM rt GROUP BY c")) == \
        {(0, 4): 1, (1, 4): 1, (2, 4): 1}
    got = _parity(client, sn, "SELECT c + 1 AS k, SUM(a * 2) AS s FROM rt GROUP BY c + 1")
    assert bag(got) == _grouped_oracle(src, lambda r: r[1] + 1, lambda g: sum(r[2] * 2 for r in g))


@pytest.mark.parametrize("query", [
    # A float GROUP BY key, written bare and as an expression — the expression
    # form has no column name to blame, so it is a separate rejection site. A
    # float key breaks the byte-equal key contract (±0.0 differ byte-wise but
    # compare equal), so losing this guard would silently split or merge groups
    # rather than fail.
    "SELECT price, COUNT(*) AS c FROM gg GROUP BY price",
    "SELECT price + 1 AS k, COUNT(*) AS c FROM gg GROUP BY price + 1",
    "SELECT DISTINCT price FROM gg",
    # A column that is neither grouped nor aggregated, bare and inside an
    # expression.
    "SELECT pk, COUNT(*) AS c FROM gg GROUP BY n",
    "SELECT pk + 1 AS k, COUNT(*) AS c FROM gg GROUP BY n",
    # An expression that is not the grouped one: `n * 2` is not `n + 1`, so it
    # reads an ungrouped column however it is spelled.
    "SELECT n * 2 AS k FROM gg GROUP BY n + 1",
    # An aggregate over an aggregate.
    "SELECT SUM(COUNT(pk)) AS s FROM gg GROUP BY n",
    # HAVING over an ungrouped column.
    "SELECT n, COUNT(*) AS c FROM gg GROUP BY n HAVING pk > 1",
])
def test_a_grouped_guard_fires_on_both_paths(client, gg, query):
    """The grouped rejections a direct SELECT and a view body share. A guard that
    stops firing on one path turns that path's refusal into a wrong answer."""
    _reject_both(client, gg, query)


# ---------------------------------------------------------------------------
# HAVING through the shared expression evaluator
#
# The ad-hoc HAVING and a grouped view's post-reduce filter run one compiled
# program, so `_parity` cannot catch a wrong *expression* result — each case
# pins the surviving groups as well. What parity still discriminates is the
# surrounding shape: where the group key lives (a view's PK region against the
# fold's payload slot), the nullability each side declares, and the client's
# reduce-output batch against the engine's.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("query,col,want", [
    # A single NOT NULL U64 group column: the view keeps it in its PK region,
    # ad-hoc in a payload slot. Also pins the unsigned comparison.
    ("SELECT u, COUNT(*) AS c FROM hg GROUP BY u HAVING u > 9223372036854775807",
     "u", [9223372036854775808, 18446744073709551615]),
    # A negative literal against a U64 column reads as u64::MAX on both paths, so
    # the predicate is always false.
    ("SELECT u, COUNT(*) AS c FROM hg GROUP BY u HAVING u > -1", "u", []),
    # A NOT NULL aggregate source, grouped: the two sides declare its
    # nullability differently, so they take different evaluation arms.
    ("SELECT cat, SUM(nn) AS t FROM hg GROUP BY cat HAVING SUM(nn) > 0", "cat", [1, 2, 3]),
    # The only narrow (2-byte) aggregate partial: MIN over SMALLINT keeps the
    # source width, so the value round-trips through sign extension.
    ("SELECT cat, MIN(sm) AS m FROM hg GROUP BY cat HAVING MIN(sm) < 0", "cat", [1]),
    # Strings, IS NULL, a CASE, and IN over both a string and an aggregate.
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s = 'a'", "s", ["a"]),
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s > 'a'", "s", ["b", "c"]),
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s IN ('a', 'c')", "s", ["a", "c"]),
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s IS NULL", "s", [None]),
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s IS NOT NULL", "s", ["a", "b", "c"]),
    ("SELECT cat, MIN(v) AS m FROM hg GROUP BY cat HAVING MIN(v) IS NULL", "cat", [3]),
    ("SELECT cat, MIN(v) AS m FROM hg GROUP BY cat HAVING MIN(v) IN (1, 7)", "cat", [1, 2]),
    ("SELECT cat, COUNT(*) AS c FROM hg GROUP BY cat "
     "HAVING CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1", "cat", [1, 2, 3]),
    # A scalar function wrapping the aggregate.
    ("SELECT cat, SUM(nn) AS t FROM hg GROUP BY cat HAVING ABS(SUM(nn)) > 0", "cat", [1, 2, 3]),
    # Constant predicates: NULL is UNKNOWN and never truthy, a statically-false
    # one drops everything, and a bare float evaluating to -0.0 is KEPT because
    # the engine filter bit-tests rather than comparing to 0.0.
    ("SELECT cat FROM hg GROUP BY cat HAVING NULL", "cat", []),
    ("SELECT cat FROM hg GROUP BY cat HAVING 'a' = 'b'", "cat", []),
    ("SELECT cat FROM hg GROUP BY cat HAVING -(SUM(cat) * 0.0)", "cat", [1, 2, 3]),
    # An aggregate appearing only in HAVING: all groups dropped, all kept, and
    # several filter ranges rather than one.
    ("SELECT cat FROM hg GROUP BY cat HAVING COUNT(*) > 99", "cat", []),
    ("SELECT cat FROM hg GROUP BY cat HAVING COUNT(*) > 0", "cat", [1, 2, 3]),
    ("SELECT cat FROM hg GROUP BY cat HAVING cat <> 2", "cat", [1, 3]),
    # A HAVING with no GROUP BY groups the whole relation, even when the
    # projection carries no aggregate or none is written at all.
    ("SELECT COUNT(*) AS c FROM hg HAVING COUNT(*) > 0", "c", [6]),
    ("SELECT 1 AS one FROM hg HAVING SUM(cat) > 1", "one", [1]),
    ("SELECT 1 AS one FROM hg HAVING 1 = 1", "one", [1]),
])
def test_having_parity_and_value(client, hg, query, col, want):
    assert bag(_parity(client, hg, query), col) == {(w,): 1 for w in want}


def test_a_constant_having_that_never_reaches_the_engine(client, hg):
    """`HAVING 1` folds away at plan time and `HAVING 0` compiles to a constant
    that drops every group."""
    assert bag(rows(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING 1")) == \
        {(1,): 1, (2,): 1, (3,): 1}
    assert rows(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING 0") == []


def test_the_register_cap_is_located_not_pinned(client, hg):
    """The register cap on the direct path, found rather than hardcoded. The
    builder folds identical instructions, so the conjuncts must share nothing:
    `cat * k < k + 1` over distinct odd `k` is true exactly for `cat = 1`."""
    def having(k):
        return f"SELECT cat FROM hg GROUP BY cat HAVING {conjunct_ladder('cat', k)}"

    k = first_rejected(lambda k: rows(client, hg, having(k)), range(1, 64))
    assert bag(_parity(client, hg, having(k - 1)), "cat") == {(1,): 1}

    def having_in(n):
        items = ", ".join(repr(f"v{i}") for i in range(n))
        return f"SELECT s FROM hg GROUP BY s HAVING s IN ({items})"

    n = first_rejected(lambda n: rows(client, hg, having_in(n)), range(1, 64))
    assert _parity(client, hg, having_in(n - 1)) == []


# ---------------------------------------------------------------------------
# AVG reads its SUM accumulator at the declared type
#
# The client combines integer SUM partials into an i64 whose bit pattern is the
# sum mod 2^64. A SUM over a `BIGINT UNSIGNED` source is declared U64, so past
# 2^63 that bit pattern reads correctly only as unsigned — and AVG divides the
# value, where SUM's render only reinterprets its bits.
# ---------------------------------------------------------------------------


def test_avg_reads_its_accumulator_at_the_declared_type(client, hg):
    """cat = 3 sums two 2^64-1 cells: the high bit is set, so a signed read of
    the accumulator renders the average negative."""
    # 2^64-2 has no exact f64 image; it rounds to 2^64, so the average is 2^63.
    got = _parity(client, hg, "SELECT cat, AVG(u) AS a FROM hg GROUP BY cat")
    assert {r.cat: r.a for r in got}[3] == 9.223372036854776e18, got
    # The same accumulator with no group columns at all.
    assert [r.a for r in _parity(client, hg, "SELECT AVG(u) AS a FROM hg WHERE cat = 3")] \
        == [9.223372036854776e18]
    # One 2^64-1 cell does not wrap, so its true average is representable.
    assert [r.a for r in _parity(client, hg, "SELECT AVG(u) AS a FROM hg WHERE pk = 5")] \
        == [1.8446744073709552e19]
    # HAVING and the render read one accumulator: the predicate keeps cat = 3 on
    # a correctly-computed average, so a mis-signed render disagrees with itself.
    got = _parity(client, hg, "SELECT cat, AVG(u) AS a FROM hg GROUP BY cat HAVING AVG(u) > 1.0")
    assert bag(got, "cat") == {(2,): 1, (3,): 1}, got
    # SUM's raw-bits render wraps to the u64.
    got = _parity(client, hg, "SELECT cat, SUM(u) AS t FROM hg WHERE cat = 3 GROUP BY cat")
    assert [r.t for r in got] == [18446744073709551614], got

    # What the unsigned read must not disturb: a signed source (every non-U64
    # integer widens to the same signed accumulator), an all-NULL group, and the
    # global ground row.
    _parity(client, hg, "SELECT cat, AVG(sm) AS a FROM hg GROUP BY cat")
    got = _parity(client, hg, "SELECT cat, AVG(v) AS a FROM hg GROUP BY cat")
    assert {r.cat: r.a for r in got}[3] is None, got
    assert [r.a for r in _parity(client, hg, "SELECT AVG(v) AS a FROM hg WHERE cat = 99")] \
        == [None]


def test_a_null_group_and_an_all_null_aggregate(client, hg):
    """A NULL group forms its own group, and a group whose aggregated column is
    entirely NULL still emits."""
    got = _parity(client, hg,
                  "SELECT s, COUNT(v) AS cv, SUM(v) AS sv, MIN(v) AS mv FROM hg GROUP BY s")
    assert bag(got) == {("a", 1, 1, 1): 1, ("b", 1, 7, 7): 1,
                        ("c", 0, None, None): 1, (None, 0, None, None): 1}


# ---------------------------------------------------------------------------
# DISTINCT
# ---------------------------------------------------------------------------


def test_distinct_clamps_every_survivor_to_weight_one(client, schema_name):
    """DISTINCT is the weight-clamp primitive, so what it owes is not a row set
    but a weight: every survivor at exactly 1, however many source rows folded
    into it. A row count cannot see a survivor that kept weight 3."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE u (pk BIGINT PRIMARY KEY, city TEXT, region BIGINT); "
        "INSERT INTO u VALUES (1,'nyc',1),(2,'sf',2),(3,'nyc',1),(4,'sf',2),(5,'la',3),"
        "(6,'nyc',1)", schema_name=sn)
    assert bag(_parity(client, sn, "SELECT DISTINCT city FROM u")) == \
        {("nyc",): 1, ("sf",): 1, ("la",): 1}
    assert bag(_parity(client, sn, "SELECT DISTINCT city, region FROM u")) == \
        {("nyc", 1): 1, ("sf", 2): 1, ("la", 3): 1}
    assert bag(_parity(client, sn, "SELECT DISTINCT region FROM u WHERE region > 1")) == \
        {(2,): 1, (3,): 1}


def test_distinct_over_a_computed_key_and_a_duplicate_name(client, schema_name):
    """`SELECT DISTINCT <expr>` and `SELECT DISTINCT *` over a duplicate-name view
    both bind through the one front end, so each accepts exactly what the
    identical `CREATE VIEW` body accepts."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE c (pk BIGINT PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "INSERT INTO c VALUES (1,1,4),(2,2,3),(3,5,0),(4,1,4)", schema_name=sn)
    # A computed set-identity key: rows 1, 2 and 4 all sum to 5, so the three
    # collapse to one survivor at weight 1.
    assert bag(_parity(client, sn, "SELECT DISTINCT a + b AS s FROM c")) == {(5,): 1}
    _parity(client, sn, "SELECT DISTINCT a, a + b AS s FROM c")

    # A name-preserving wildcard names nothing of its own, so a duplicate among
    # the source's names rides through positionally — on both paths.
    client.execute_sql(
        "CREATE TABLE d (pk BIGINT PRIMARY KEY, a BIGINT NOT NULL); "
        "INSERT INTO d VALUES (1, 1), (2, 2); "
        "CREATE VIEW dup AS SELECT * FROM c JOIN d ON c.a = d.pk", schema_name=sn)
    assert sorted(bag(rows(client, sn, "SELECT DISTINCT * FROM dup")).values()) == [1, 1, 1]


def test_distinct_over_a_wide_group(client, schema_name):
    """A >8-column group: DISTINCT over the 10 non-PK columns collapses the
    duplicate pair to one survivor at weight 1, and DISTINCT * keeps all three
    because the PK makes each row unique."""
    sn = schema_name
    cols = ", ".join(f"c{i} BIGINT" for i in range(10))
    client.execute_sql(f"CREATE TABLE w (pk BIGINT PRIMARY KEY, {cols})", schema_name=sn)
    insert(client, sn, "w", [(1, *range(10)), (2, *range(10)), (3, *range(1, 11))])
    proj = ", ".join(f"c{i}" for i in range(10))
    got = bag(_parity(client, sn, f"SELECT DISTINCT {proj} FROM w"))
    assert got == {tuple(range(10)): 1, tuple(range(1, 11)): 1}
    assert sorted(bag(_parity(client, sn, "SELECT DISTINCT * FROM w")).values()) == [1, 1, 1]


# ---------------------------------------------------------------------------
# Empty input, ground row, bag-valued source, fold width, group cap
# ---------------------------------------------------------------------------


def test_an_empty_source_grounds_a_global_fold_but_not_a_grouped_one(client, schema_name):
    sn = schema_name
    client.execute_sql("CREATE TABLE e (pk BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
    assert bag(rows(client, sn, "SELECT COUNT(*) AS c, SUM(v) AS s, MAX(v) AS m FROM e")) \
        == {(0, None, None): 1}
    assert rows(client, sn, "SELECT v, COUNT(*) AS c FROM e GROUP BY v") == []
    # HAVING still filters the ground row: a NULL aggregate drops it, COUNT
    # keeps it.
    assert _parity(client, sn, "SELECT COUNT(*) AS c FROM e HAVING SUM(v) = 0") == []
    assert _parity(client, sn, "SELECT COUNT(*) AS c FROM e HAVING MAX(v) >= 0") == []
    assert bag(_parity(client, sn, "SELECT COUNT(*) AS c FROM e HAVING COUNT(*) = 0")) == \
        {(0,): 1}
    # A WHERE filtering out every row behaves identically to an empty table.
    client.execute_sql("INSERT INTO e VALUES (1, 5)", schema_name=sn)
    assert bag(rows(client, sn, "SELECT COUNT(*) AS c, SUM(v) AS s FROM e WHERE v > 100")) \
        == {(0, None): 1}
    assert rows(client, sn, "SELECT v, COUNT(*) AS c FROM e WHERE v > 100 GROUP BY v") == []


def test_a_fold_over_a_bag_counts_logical_rows(client, schema_name):
    """A UNION ALL view whose rows carry weight 2: COUNT(*) counts the
    multiplicity, not the entries."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE base (pk BIGINT PRIMARY KEY, v BIGINT); "
        "INSERT INTO base VALUES (1, 10), (2, 20), (3, 10); "
        "CREATE VIEW dbl AS SELECT pk, v FROM base UNION ALL SELECT pk, v FROM base",
        schema_name=sn)
    assert bag(rows(client, sn, "SELECT COUNT(*) AS c FROM dbl")) == {(6,): 1}
    _parity(client, sn, "SELECT v, COUNT(*) AS c FROM dbl GROUP BY v")


def test_many_aggregates(client, schema_name):
    """A wide multi-aggregate query with a group column still folds, every
    accumulator kind at once."""
    sn = schema_name
    n = 8
    cols = ", ".join(f"c{i} BIGINT" for i in range(n))
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT PRIMARY KEY, g BIGINT NOT NULL, {cols})", schema_name=sn)
    insert(client, sn, "t", [(i, i % 4, *((i * (j + 1)) % 50 for j in range(n)))
                             for i in range(1, 31)])
    aggs = ", ".join(f"SUM(c{i}) AS s{i}, MIN(c{i}) AS l{i}, MAX(c{i}) AS m{i}, COUNT(c{i}) AS n{i}"
                     for i in range(n))
    _parity(client, sn, f"SELECT g, {aggs} FROM t GROUP BY g")


def test_the_per_worker_group_cap_aborts_and_the_worker_keeps_serving(adhoc_group_cap_server):
    client, sn = adhoc_group_cap_server, "public"
    client.execute_sql("CREATE TABLE t (pk BIGINT PRIMARY KEY, g BIGINT NOT NULL)", schema_name=sn)
    # 100 distinct groups; with the cap at 4 per worker, some worker exceeds it.
    insert(client, sn, "t", [(i, i) for i in range(1, 101)])
    with pytest.raises(gnitz.GnitzError, match="CREATE VIEW"):
        rows(client, sn, "SELECT g, COUNT(*) AS c FROM t GROUP BY g")
    assert bag(rows(client, sn, "SELECT COUNT(*) AS c FROM t")) == {(100,): 1}


# ---------------------------------------------------------------------------
# Which rows a tied cut returns is a function of (data, query) alone
#
# The ad-hoc output's hidden `_group_pk` leads the ordering sink's identity
# tiebreak, so it fully decides every tie the ORDER BY keys leave open. It is the
# engine's key for the group — a pure function of the group's column values —
# carried out of the partial reply.
# ---------------------------------------------------------------------------


def _tied_groups(client, sn):
    """16 groups of 3 rows each: every COUNT is tied, so an ORDER BY over the
    count leaves the whole result set for the tiebreak to order. `h` gives the
    DISTINCT case a tied non-key column to order by."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, h BIGINT NOT NULL)",
        schema_name=sn)
    insert(client, sn, "t", [(i, i % 16, i % 2) for i in range(1, 49)])


def test_a_tied_cut_is_worker_count_independent(seamed_server):
    """16 fully-tied groups and a `LIMIT 4`: the cut is entirely the tiebreak's
    to decide, so the same four groups must come back at every worker count."""
    queries = {
        "asc": "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY c LIMIT 4",
        "desc": "SELECT g, COUNT(*) AS c FROM t GROUP BY g ORDER BY c DESC LIMIT 4",
        "distinct": "SELECT DISTINCT g, h FROM t ORDER BY h LIMIT 4",
    }
    per_count = {}
    for workers in ("2", "4"):
        client = seamed_server({"GNITZ_WORKERS": workers})
        _tied_groups(client, "public")
        per_count[workers] = got = {k: rows(client, "public", q) for k, q in queries.items()}
        # A single non-nullable integer group column keys on the
        # order-preserving route key, so the GROUP BY tie breaks ascending by `g`
        # — ASC in both directions, because tiebreak keys are always ASC. (DISTINCT
        # over two columns keys on a digest: deterministic, but arbitrary with
        # respect to the data, so only cross-worker-count equality is asserted.)
        for k in ("asc", "desc"):
            assert [r.g for r in got[k]] == [0, 1, 2, 3], (workers, got[k])

    for k, q in queries.items():
        assert ordered(per_count["2"][k]) == ordered(per_count["4"][k]), \
            f"`{q}` differs across worker counts"


def test_a_tied_cut_agrees_with_a_views_cut(client, schema_name):
    """The ad-hoc cut and a view's cut return the *same* tied rows. The two keys
    are not the same bytes — a view over a single non-nullable integer group
    column keys on that column's OPK, the fold on a wider group key — but the
    route key is order-preserving, so both induce the identical order."""
    sn = schema_name
    _tied_groups(client, sn)
    # A single natural group column: the key is the plain route key.
    first = _parity_ordered(
        client, sn, "SELECT g, COUNT(*) AS c FROM t GROUP BY g",
        "ORDER BY c LIMIT 4", "ORDER BY c DESC LIMIT 4", "ORDER BY c OFFSET 5 LIMIT 6")
    assert [r.g for r in first] == [0, 1, 2, 3]
    # A two-column group key: both sides key on the digest, so this is where a
    # divergence between the fold's stamp and the view's would show.
    _parity_ordered(client, sn, "SELECT g, h, COUNT(*) AS c FROM t GROUP BY g, h",
                    "ORDER BY c LIMIT 5")


def test_order_by_an_aggregate_by_alias_call_and_position(client, orders):
    """ORDER BY an aggregate or an expression over the grouped relation binds
    like a SELECT item, by call or by alias alike; a column the grouping does not
    cover is rejected."""
    by_call = rows(client, orders,
                   "SELECT category, COUNT(*) FROM orders GROUP BY category "
                   "ORDER BY COUNT(*) DESC, category")
    by_alias = rows(client, orders,
                    "SELECT category, COUNT(*) AS c FROM orders GROUP BY category "
                    "ORDER BY c DESC, category")
    assert [r.category for r in by_call] == [r.category for r in by_alias]
    positional = rows(client, orders,
                      "SELECT category, COUNT(*) AS c FROM orders GROUP BY category ORDER BY 1")
    assert [r.category for r in positional] == sorted(r.category for r in positional)

    with pytest.raises(gnitz.GnitzError, match="must appear in GROUP BY"):
        rows(client, orders,
             "SELECT category, COUNT(*) FROM orders GROUP BY category ORDER BY amount")


# ---------------------------------------------------------------------------
# The float aggregate value contract
#
# A float SUM / AVG is a function of the summation order, so of (query, data,
# worker count, access path, scan chunk size). It is stable for a fixed
# deployment and a fixed plan — which is the guarantee, and is what these pin.
# ---------------------------------------------------------------------------


# 2000 rows over 500 categories. The category count is what makes the range
# `cat >= 5 AND cat < 13` about 1.6% of the table — selective enough to clear the
# index cost gate, and wide enough that the range spans several scan chunks.
_FLOAT_ROWS = 2000
_FLOAT_CATS = 500


def _float_table(client, sn):
    rnd = random.Random(11)
    out = []
    for i in range(1, _FLOAT_ROWS + 1):
        # Alternating +/-1e15 and 1e-8 magnitudes: the sums cancel
        # catastrophically, so any reassociation moves the low bits.
        mag = 1e15 if i % 4 == 0 else (-1e15 if i % 4 == 1 else 1e-8)
        out.append((i, (i * 37) % _FLOAT_CATS, mag * (1 + rnd.random())))
    client.execute_sql(
        "CREATE TABLE f (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL, x DOUBLE NOT NULL)",
        schema_name=sn)
    insert(client, sn, "f", out)


def _bits(rows):
    """Each `s` as its exact bit pattern — `float.hex()` round-trips losslessly,
    so two sums compare equal only if they are the same double."""
    return [r.s.hex() for r in rows]


def test_a_float_sum_is_stable_for_a_fixed_plan(client, schema_name):
    """Same query, same data, same worker count, same access path → bit-identical.
    The one reproducibility guarantee the float contract does make."""
    sn = schema_name
    _float_table(client, sn)
    for q in ("SELECT SUM(x) AS s FROM f",
              "SELECT cat, SUM(x) AS s FROM f GROUP BY cat",
              "SELECT cat, AVG(x) AS s FROM f GROUP BY cat"):
        assert _bits(rows(client, sn, q)) == _bits(rows(client, sn, q)), f"`{q}` is not repeatable"


def test_an_index_plan_keeps_the_exact_answer_and_a_repeatable_float_sum(tiny_ddl_chunk_server):
    """`CREATE INDEX` re-plans these reads onto an index-bounded walk, which sorts
    and dedups PKs one chunk at a time. What that owes the caller is pinned here:
    the same rows, and the same aggregates that are exact over them, chunk
    boundaries and all — plus a float SUM that still repeats under the new plan.

    What it does not owe is the full scan's float bits. A float SUM follows the
    summation order and the access path is part of that order, so the indexed and
    unindexed float sums are deliberately never compared: they may legitimately
    agree, and requiring either answer would pin an access-path detail as a promise.

    The 3-row chunk is the point: each worker's share spans several chunks, the
    only regime in which the walk visits rows in an order a PK scan never would."""
    client, sn = tiny_ddl_chunk_server, "public"
    _float_table(client, sn)
    src = "FROM f WHERE cat >= 5 AND cat < 13"
    rows_q = f"SELECT pk, x {src}"
    exact_q = f"SELECT COUNT(*) AS c, SUM(pk) AS sp, MIN(x) AS mn, MAX(x) AS mx {src}"
    float_q = f"SELECT SUM(x) AS s {src}"

    # The control: with no index to bound on these are full scans, so the
    # assertion after CREATE INDEX reads the plan and not a constant.
    for q in (rows_q, exact_q, float_q):
        assert access(client, sn, q).startswith("access: full scan"), q
    before_rows = bag(rows(client, sn, rows_q))
    before_exact = bag(rows(client, sn, exact_q))
    # 8 of `_FLOAT_CATS` categories at 4 rows each — coupled to that fixture.
    # 1.6% of the table, well inside the worker's cost gate, and 8 rows per
    # worker at 4 workers: three 3-row chunks each.
    assert len(before_rows) == 32, before_rows

    client.execute_sql("CREATE INDEX fi ON f (cat)", schema_name=sn)
    # One access planner serves the rows sink and the fold sink alike, so all
    # three re-plan.
    for q in (rows_q, exact_q, float_q):
        assert access(client, sn, q).startswith("access: index range on (cat)"), q

    # A chunked drain and an unchunked one yield identical multisets: the rows,
    # then the aggregates exact over them — COUNT and an integer SUM fold
    # associatively, and MIN/MAX copy a cell's bits rather than combining them.
    assert bag(rows(client, sn, rows_q)) == before_rows
    assert bag(rows(client, sn, exact_q)) == before_exact
    # The float SUM under the index plan: repeatable, and that alone.
    assert _bits(rows(client, sn, float_q)) == _bits(rows(client, sn, float_q))


# ---------------------------------------------------------------------------
# Wide (string / UUID) extremes and a spilling group column
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("agg", ["MIN(label)", "MAX(label)", "MIN(u)", "MAX(u)"])
def test_a_wide_extreme_combines_by_content(client, wx, agg):
    """MIN/MAX over a TEXT or UUID column: every worker's partial carries the
    value itself, and the client's combine orders them as the engine does —
    strings by content, past the inline bound and across a shared prefix."""
    col = 2 if "label" in agg else 3
    pick = min if agg.startswith("MIN") else max
    got = _parity(client, wx, f"SELECT g, {agg} AS m FROM wx GROUP BY g")
    assert bag(got, "g", "m") == _grouped_oracle(_WX, lambda r: r[1],
                                                 lambda g: pick(r[col] for r in g))
    _parity(client, wx, f"SELECT {agg} AS m FROM wx")
    # An empty source still grounds the global extreme to NULL.
    assert [r.m for r in _parity(client, wx, f"SELECT {agg} AS m FROM wx WHERE pk > 100")] \
        == [None]


def test_a_spilling_string_group_column_survives_the_finalize(client, schema_name):
    """A GROUP BY over a TEXT column longer than the German-string inline bound.

    The group column is copied out of the fold's own group batch into the result,
    and a spilled cell's heap offset means nothing in the destination's arena —
    so a copy that moved the 16-byte struct verbatim would hand back an empty
    string for exactly the values that do not fit inline."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE labelled (pk BIGINT NOT NULL PRIMARY KEY, label TEXT NOT NULL,"
        " amount BIGINT NOT NULL)", schema_name=sn)
    # Two labels well past the 12-byte inline bound, one below it.
    labels = ["a string far past the inline bound", "another long spilling label", "short"]
    src = [(i, labels[i % 3], i) for i in range(12)]
    insert(client, sn, "labelled", src)

    got = _parity(client, sn, "SELECT label, SUM(amount) AS s FROM labelled GROUP BY label")
    assert bag(got) == _grouped_oracle(src, lambda r: r[1], lambda g: sum(r[2] for r in g))
