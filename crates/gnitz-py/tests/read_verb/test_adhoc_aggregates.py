"""Ad-hoc single-relation aggregation (GROUP BY / global aggregate / HAVING /
DISTINCT) served by the ReadSpec aggregate hash-fold sink + client finishing,
versus a `CREATE VIEW` of the same statement.

The load-bearing assertion is **parity**: for a query with no ORDER BY / LIMIT,
the ad-hoc result equals a scan of `CREATE VIEW v AS <same query>`, exactly —
every fixture here aggregates an integer source, so both paths agree bit for
bit. Parity is compared as a weighted bag, so a group emitted twice by one path
is a divergence rather than a matching row set. A float SUM/AVG instead follows
the summation order (the contract in `agg_finish.rs`); the tests at the bottom
pin what that does and does not promise.

`_parity_ordered` is the ordered sibling: it also pins which of a set of *tied*
rows a cut returns, which `_parity` cannot see.

Run with GNITZ_WORKERS=4 (the fold is per-worker; the client merges partials).
"""
import random

import gnitz
import pytest
from _caps import conjunct_ladder, first_rejected
from _read import access, bag, rows
from _uid import uid as _uid


def _items(rows):
    """Each row as a sorted tuple of (name, value) pairs, weight included."""
    return [(tuple(sorted(r._asdict().items())), r.weight) for r in rows]


def _parity(client, sn, query):
    """Assert the ad-hoc result of `query` (no ORDER BY / LIMIT) equals a scan of
    a view built from the identical statement.

    Order-*insensitive* by construction — `CREATE VIEW` rejects ORDER BY, so the
    view is built from the whole statement and both sides fold to a weighted
    bag. Use `_parity_ordered` to compare which rows a cut returns.
    """
    adhoc = rows(client, sn, query)
    vn = "pv_" + _uid()
    client.execute_sql(f"CREATE VIEW {vn} AS {query}", schema_name=sn)
    vid = client.resolve_table(sn, vn)[0]
    view = list(client.scan(vid))
    client.execute_sql(f"DROP VIEW {vn}", schema_name=sn)

    assert bag(adhoc) == bag(view), f"ad-hoc != view for `{query}`"
    if adhoc and view:
        assert set(adhoc[0]._asdict()) == set(view[0]._asdict()), \
            f"column names differ for `{query}`"
    return adhoc


def _parity_ordered(client, sn, body, *tails):
    """Assert the ad-hoc result of `body tail` equals reading a view of `body`
    with the same `tail` applied — row for row, in order, for each of `tails`
    against one shared view.

    A separate helper rather than an option on `_parity`: `CREATE VIEW` rejects
    ORDER BY, so `_parity` has to build the view from the whole statement and
    compare bags, which is precisely what makes it blind to tie order. Here the
    view carries only the body and the cut runs as a second read over it, so the
    two sinks' tiebreaks are what is being compared — the client's identity
    tiebreak over `_agg_pk` against the worker's over whichever key the view's
    reduce chose.

    GROUP BY only. An ad-hoc `SELECT DISTINCT` keys its ties off the reduce group
    key and a view's DISTINCT off `reindex_hash_row`, so each is deterministic but
    they do not agree; for DISTINCT the guarantee is cross-worker-count
    determinism alone.
    """
    vn = "po_" + _uid()
    client.execute_sql(f"CREATE VIEW {vn} AS {body}", schema_name=sn)
    try:
        out = []
        for tail in tails:
            adhoc = rows(client, sn, f"{body} {tail}")
            view = rows(client, sn, f"SELECT * FROM {vn} {tail}")
            assert _items(adhoc) == _items(view), f"ordered ad-hoc != view for `{body} {tail}`"
            out.append(adhoc)
    finally:
        client.execute_sql(f"DROP VIEW {vn}", schema_name=sn)
    return out[0]


def _reject_both(client, sn, query):
    """Assert `query` is rejected on the direct path AND as a CREATE VIEW, and
    return the direct path's message."""
    with pytest.raises(Exception) as ei:
        rows(client, sn, query)
    with pytest.raises(Exception):
        client.execute_sql(f"CREATE VIEW rv_{_uid()} AS {query}", schema_name=sn)
    return str(ei.value)


def _as_dict(rows, kname, vname):
    return {r._asdict()[kname]: r._asdict()[vname] for r in rows}


# ---------------------------------------------------------------------------
# The shared read-only sources
#
# Module-scoped, on their own connection off the session server rather than the
# function-scoped `client`: every test sharing one only reads from it, and a
# `_parity` view is created and dropped under a unique name. The alternative is
# rebuilding the same table for each of ~30 statements that change nothing.
# ---------------------------------------------------------------------------


def _orders_oracle():
    """The `orders` fixture's rows as `(pk, category, amount, note)`.

    The one definition: the fixture builds its INSERT from this, and the oracles
    below compute over it, so changing the fixture cannot leave a hand-written
    expectation quietly describing the old data.
    """
    return [(i, i % 5, (i * 7) % 100, None if i % 3 == 0 else (i * 3) % 50)
            for i in range(1, 41)]


def _grouped_oracle(key, value):
    """`{key(row): value(rows of that group)}` over the orders fixture."""
    groups = {}
    for row in _orders_oracle():
        groups.setdefault(key(row), []).append(row)
    return {k: value(v) for k, v in groups.items()}


@pytest.fixture(scope="module")
def orders(server):
    with gnitz.connect(server) as conn:
        sn = "aggorders"
        conn.create_schema(sn)
        conn.execute_sql(
            "CREATE TABLE orders ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  category BIGINT NOT NULL,"
            "  amount BIGINT NOT NULL,"
            "  note BIGINT"  # nullable
            ")", schema_name=sn)
        conn.execute_sql(
            "INSERT INTO orders VALUES " + ",".join(
                f"({pk}, {cat}, {amt}, {'NULL' if note is None else note})"
                for pk, cat, amt, note in _orders_oracle()), schema_name=sn)
        yield sn
        conn.drop_schema(sn)


@pytest.fixture(scope="module")
def hg(server):
    """The HAVING source: one column of each shape the compiled predicate has a
    separate arm for — a nullable TEXT, a U64 crossing i64::MAX, a narrow
    SMALLINT, a NOT NULL BIGINT and a nullable one."""
    with gnitz.connect(server) as conn:
        sn = "agghaving"
        conn.create_schema(sn)
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
        yield sn
        conn.drop_schema(sn)


def _keys(rows, col):
    return sorted(getattr(r, col) for r in rows)


# ---------------------------------------------------------------------------
# Grouped + global aggregate parity, WHERE, HAVING
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


def test_multi_aggregate_and_group_col_parity(client, orders):
    _parity(client, orders,
            "SELECT category, COUNT(*) AS c, SUM(amount) AS s, MIN(amount) AS mn, "
            "MAX(amount) AS mx FROM orders GROUP BY category")


@pytest.mark.parametrize("query", [
    # Over a projected aggregate, and over one appearing only in HAVING.
    "SELECT category, COUNT(*) AS c FROM orders GROUP BY category HAVING COUNT(*) > 7",
    "SELECT category FROM orders GROUP BY category HAVING SUM(amount) > 300",
    "SELECT category, AVG(amount) AS a FROM orders GROUP BY category HAVING AVG(amount) > 40",
    # An aggregate that is NULL for some groups (COUNT(note)=0 → 3VL drops it).
    "SELECT category, MIN(note) AS mn FROM orders GROUP BY category HAVING MIN(note) > 10",
    # Global HAVING (grounds then filters).
    "SELECT COUNT(*) AS c FROM orders HAVING COUNT(*) > 0",
    # A HAVING with no GROUP BY groups the whole relation even when the
    # projection carries no aggregate — and with no aggregate written at all the
    # reduce is `Reduce([], [])`, which the two surfaces reach by different
    # mechanisms (the ad-hoc fold emits no cardinality COUNT and grounds
    # client-side; the view path mints one).
    "SELECT 1 AS one FROM orders HAVING SUM(amount) > 1",
    "SELECT 1 AS one FROM orders HAVING 1 = 1",
])
def test_having_parity(client, orders, query):
    _parity(client, orders, query)


# ---------------------------------------------------------------------------
# Computed group keys and computed finalize items
#
# The shapes below used to be the whole visible difference between the two
# grouped front ends: a view accepted each of them and a direct SELECT rejected
# it, because the ad-hoc path ran a second, narrower binder. There is one binder
# now, so `_parity` is the regression test — but parity alone would also hold if
# BOTH paths were wrong, so each case is pinned against an in-Python oracle too.
#
# Physically these exercise the two maps a reduce can carry. A computed GROUP BY
# key or aggregate argument is the **pre-map**, which runs on the worker between
# the predicate and the fold (`AggReadSpec.pre`); an expression over the
# aggregates is the **finalize map**, which the client runs over its combined
# reduce output. A run at W=4 is what makes the pre-map's fused filter->map path
# and the cross-worker combine both real.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("select,group,key,value", [
    # An expression whose operand is an aggregate — the post-reduce finalize map.
    # `SUM(amount) + 1` is the case a direct SELECT used to refuse with "GROUP BY
    # SELECT: only column refs and aggregates supported".
    ("category, SUM(amount) + 1 AS m", "category",
     lambda r: r[1], lambda g: sum(r[2] for r in g) + 1),
    # Two aggregates in one expression, and an aggregate under a second operator
    # — the composite is deeper than one binary node.
    ("category, SUM(amount) - COUNT(*) * 2 AS m", "category",
     lambda r: r[1], lambda g: sum(r[2] for r in g) - len(g) * 2),
    # An expression over AVG: the finalize composite (a divide) feeding another
    # operator, so the divide is no longer the whole item.
    ("category, AVG(amount) * 2 AS m", "category",
     lambda r: r[1], lambda g: sum(r[2] for r in g) / len(g) * 2),
    # A group column inside an expression, mixed with an aggregate.
    ("category, category * 100 + COUNT(*) AS m", "category",
     lambda r: r[1], lambda g: g[0][1] * 100 + len(g)),
    # An aggregate whose argument is computed — the pre-map, on the worker.
    # `SUM(amount * category)` used to refuse with "aggregate on computed
    # expression not supported".
    ("category, SUM(amount * category) AS m", "category",
     lambda r: r[1], lambda g: sum(r[2] * r[1] for r in g)),
    # MIN/MAX over a computed argument: the pre-map column feeds a non-linear
    # aggregate, not just a sum.
    ("category, MAX(amount + pk) AS m", "category",
     lambda r: r[1], lambda g: max(r[2] + r[0] for r in g)),
    # A GROUP BY over an expression — the pre-map materializing the key.
    # `GROUP BY category + amount` used to refuse with "GROUP BY: only simple
    # column references supported".
    ("category + amount AS k, COUNT(*) AS m", "category + amount",
     lambda r: r[1] + r[2], len),
    # A computed key and a computed aggregate argument in one reduce: both
    # pre-map columns, deduped against the same memo.
    ("category + 1 AS k, SUM(amount * 3) AS m", "category + 1",
     lambda r: r[1] + 1, lambda g: sum(r[2] * 3 for r in g)),
])
def test_computed_key_and_finalize_parity(client, orders, select, group, key, value):
    kname = "k" if " AS k" in select else "category"
    got = _parity(client, orders, f"SELECT {select} FROM orders GROUP BY {group}")
    assert _as_dict(got, kname, "m") == _grouped_oracle(key, value)


def test_a_computed_key_reaches_every_position_as_one_column(client, orders):
    """The written key reaches the SELECT list and HAVING by its expression, not
    by a name, and the same expression written twice is ONE pre-map column
    rather than two."""
    got = _parity(client, orders,
                  "SELECT amount * 2 AS k, SUM(pk) AS s FROM orders "
                  "GROUP BY amount * 2 HAVING SUM(pk) > 20")
    assert _as_dict(got, "k", "s") == {
        k: v for k, v in _grouped_oracle(
            lambda r: r[2] * 2, lambda g: sum(r[0] for r in g)).items() if v > 20}

    got = _parity(client, orders,
                  "SELECT category + amount AS k, category + amount AS again, "
                  "COUNT(*) AS c FROM orders GROUP BY category + amount")
    assert all(r._asdict()["k"] == r._asdict()["again"] for r in got)


def test_a_premap_needs_a_free_column_slot(client, schema_name):
    """A pre-map on a source already at the schema's column limit.

    The pre-map's output is the source's columns *plus* the computed ones, so a
    maximally wide source has no room for one. That has to be a plan-time feature
    message: the worker refuses the same width, but only as a trust boundary, and
    reaching it would surface a server error for a legal statement.

    The control matters as much as the rejection — one column narrower, the same
    query must compile — or a gate that simply rejected every wide table would
    pass the first half.
    """
    sn = schema_name
    # MAX_COLUMNS is 65 and counts the PK, so 65 total leaves no room for a
    # computed key; 64 leaves exactly one.
    for n_cols, should_compile in ((64, True), (65, False)):
        name = f"w{n_cols}"
        payload = ", ".join(f"c{i} BIGINT NOT NULL" for i in range(n_cols - 1))
        client.execute_sql(
            f"CREATE TABLE {name} (pk BIGINT PRIMARY KEY, {payload})", schema_name=sn)
        client.execute_sql(
            f"INSERT INTO {name} VALUES (1, {', '.join(str(i) for i in range(n_cols - 1))})",
            schema_name=sn)
        q = f"SELECT c0 + 1 AS k, COUNT(*) AS c FROM {name} GROUP BY c0 + 1"
        if should_compile:
            assert _as_dict(rows(client, sn, q), "k", "c") == {1: 1}
        else:
            with pytest.raises(Exception) as ei:
                rows(client, sn, q)
            assert "65" in str(ei.value), f"the limit must name itself: {ei.value}"


@pytest.mark.parametrize("where,select,group,keep,key,value", [
    # A string WHERE: the source's TEXT column sits where the permutation puts an
    # integer, so a predicate typed against the pre-map output is rejected
    # outright rather than answering wrongly.
    ("s = 'a'", "v + 1 AS k, COUNT(*) AS m", "v + 1",
     lambda r: r[1] == "a", lambda r: r[3] + 1, lambda g: len(g)),
    # A float WHERE, which fails in the engine rather than the planner.
    ("x > 6.0", "v AS k, SUM(id * 2) AS m", "v",
     lambda r: r[0] > 6.0, lambda r: r[3], lambda g: sum(r[2] * 2 for r in g)),
    # A computed aggregate argument over the same shape, so the pre-map is
    # reached by the argument rather than by the key.
    ("s = 'b'", "v AS k, MAX(id * 3) AS m", "v",
     lambda r: r[1] == "b", lambda r: r[3], lambda g: max(r[2] * 3 for r in g)),
])
def test_a_premap_over_a_source_whose_pk_is_not_first(
        client, schema_name, where, select, group, keep, key, value):
    """The pre-map's output schema is the source's columns with the PK columns
    moved to the front (`place_pk_front`), so it is a *permutation* of the source
    whenever the PK is not already leading — and a WHERE is resolved to positions
    against the source, not against that permutation. Every other fixture here
    declares `pk` first, which makes the permutation the identity and hides any
    confusion between the two orders; this one does not.

    A DOUBLE and a TEXT column ahead of the PK, because the symptom is a *type*
    mismatch at the permuted slot: the predicate reads whichever column the
    permutation moved into its position, so the wrong-typed cases fail loudly
    while a same-width integer one would just compare the wrong column.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE nl (x DOUBLE NOT NULL, s TEXT NOT NULL, id BIGINT NOT NULL,"
        " v BIGINT NOT NULL, PRIMARY KEY (id))", schema_name=sn)
    src = [(i * 1.5, "a" if i % 2 else "b", i, i % 4) for i in range(1, 13)]
    client.execute_sql(
        "INSERT INTO nl VALUES " + ",".join(f"({x}, '{s}', {i}, {v})" for x, s, i, v in src),
        schema_name=sn)

    got = _parity(client, sn, f"SELECT {select} FROM nl WHERE {where} GROUP BY {group}")
    groups = {}
    for r in (r for r in src if keep(r)):
        groups.setdefault(key(r), []).append(r)
    assert _as_dict(got, "k", "m") == {k: value(g) for k, g in groups.items()}


def test_a_computed_key_over_a_replicated_source(client, schema_name):
    """Replication is the trap for anything that touches reduce routing: every
    worker holds every row, so a fold that double-counted would still return
    plausible groups. The pre-map hands the fold a *derived* schema (source PK
    columns plus the computed ones) rather than the source's, so this pins that
    swapping the descriptor did not change which rows a worker folds."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE rt (pk BIGINT PRIMARY KEY, c BIGINT NOT NULL, a BIGINT NOT NULL) "
        "WITH (replicated = true)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO rt VALUES " + ",".join(f"({i}, {i % 3}, {i})" for i in range(1, 13)),
        schema_name=sn)
    # The control: no pre-map, so a double-count would show here too.
    assert _as_dict(_parity(client, sn, "SELECT c, COUNT(*) AS n FROM rt GROUP BY c"),
                    "c", "n") == {0: 4, 1: 4, 2: 4}
    got = _parity(client, sn, "SELECT c + 1 AS k, SUM(a * 2) AS s FROM rt GROUP BY c + 1")
    want = {}
    for i in range(1, 13):
        want[i % 3 + 1] = want.get(i % 3 + 1, 0) + i * 2
    assert _as_dict(got, "k", "s") == want


@pytest.mark.parametrize("query", [
    # A float GROUP BY key, written bare and as an expression — the expression
    # form has no column name to blame, so it is a separate rejection site. This
    # is the load-bearing guard: a float key breaks the byte-equal key contract
    # (±0.0 differ byte-wise but compare equal), so losing it would silently
    # split or merge groups rather than fail.
    "SELECT price, COUNT(*) AS c FROM gg GROUP BY price",
    "SELECT price + 1 AS k, COUNT(*) AS c FROM gg GROUP BY price + 1",
    "SELECT DISTINCT price FROM gg",
    # A column that is neither grouped nor aggregated, bare and inside an
    # expression — the second only became expressible with the finalize map.
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
def test_a_grouped_guard_fires_on_both_paths(client, schema_name, query):
    """The grouped rejections a direct SELECT must keep, now that it shares the
    view's binder rather than running one of its own.

    Widening a front end is where guards get dropped silently — a rejection that
    was a literal in the narrower binder and has no counterpart in the wider one
    simply stops firing, and the query returns a wrong answer instead of an
    error.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE gg (pk BIGINT PRIMARY KEY, price DOUBLE NOT NULL, n BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("INSERT INTO gg VALUES (1, 1.5, 7), (2, 2.5, 8)", schema_name=sn)
    _reject_both(client, sn, query)


# ---------------------------------------------------------------------------
# HAVING through the shared expression evaluator
#
# The ad-hoc HAVING is compiled by the same `BoundExpr -> ExprProgram` compiler a
# grouped view's post-reduce FILTER uses, and run by the same evaluator. So
# `_parity` cannot catch a wrong *expression* result — both sides run the same
# program. What it still discriminates is the surrounding physical shape: the
# out-key layout (a single non-nullable natural key lives in the view's PK region
# and is read with LoadPk, while ad-hoc is always the SyntheticFold payload
# layout), the nullability verdict (ad-hoc declares every aggregate column
# nullable, the view uses the per-spec rule), and the client's own materialized
# reduce-output batch against the engine's.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("query,col,want", [
    # A single NOT NULL U64 group column: the view routes it through the OPK PK
    # region (SingleNaturalCol + LoadPk), ad-hoc through a payload slot. Also
    # pins the unsigned comparison.
    ("SELECT u, COUNT(*) AS c FROM hg GROUP BY u HAVING u > 9223372036854775807",
     "u", [9223372036854775808, 18446744073709551615]),
    # A negative literal against a U64 column reads as u64::MAX on both paths, so
    # the predicate is always false.
    ("SELECT u, COUNT(*) AS c FROM hg GROUP BY u HAVING u > -1", "u", []),
    # A NOT NULL aggregate source, grouped: the view resolves `no_nulls = true`
    # and reads the verdict out of the register file, ad-hoc resolves it false
    # and reads `bool_bits & !null_bits`. Both evaluation arms of `filter`.
    ("SELECT cat, SUM(nn) AS t FROM hg GROUP BY cat HAVING SUM(nn) > 0", "cat", [1, 2, 3]),
    # The only narrow (2-byte) aggregate partial: MIN over SMALLINT keeps the
    # source width, so the value round-trips i64 -> 2 bytes -> sign-extended i64.
    ("SELECT cat, MIN(sm) AS m FROM hg GROUP BY cat HAVING MIN(sm) < 0", "cat", [1]),
    # Capabilities the direct path lacked before the swap: strings, IS NULL, a
    # CASE, and IN over both a string and an aggregate.
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s = 'a'", "s", ["a"]),
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s > 'a'", "s", ["b", "c"]),
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s IN ('a', 'c')", "s", ["a", "c"]),
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s IS NULL", "s", [None]),
    ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s IS NOT NULL", "s", ["a", "b", "c"]),
    ("SELECT cat, MIN(v) AS m FROM hg GROUP BY cat HAVING MIN(v) IS NULL", "cat", [3]),
    ("SELECT cat, MIN(v) AS m FROM hg GROUP BY cat HAVING MIN(v) IN (1, 7)", "cat", [1, 2]),
    ("SELECT cat, COUNT(*) AS c FROM hg GROUP BY cat "
     "HAVING CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1", "cat", [1, 2, 3]),
    # A scalar function wrapping the aggregate: consumed above the leaf on both
    # paths, and its aggregate resolves through the leaf on the recursion.
    ("SELECT cat, SUM(nn) AS t FROM hg GROUP BY cat HAVING ABS(SUM(nn)) > 0", "cat", [1, 2, 3]),
    # Constant predicates: NULL is UNKNOWN and never truthy, a statically-true
    # one folds away, and a bare float evaluating to -0.0 is KEPT because the
    # engine filter bit-tests rather than comparing to 0.0.
    ("SELECT cat FROM hg GROUP BY cat HAVING NULL", "cat", []),
    ("SELECT cat FROM hg GROUP BY cat HAVING 'a' = 'b'", "cat", []),
    ("SELECT cat FROM hg GROUP BY cat HAVING -(SUM(cat) * 0.0)", "cat", [1, 2, 3]),
    # All groups dropped, all kept, and several filter ranges rather than one.
    ("SELECT cat FROM hg GROUP BY cat HAVING COUNT(*) > 99", "cat", []),
    ("SELECT cat FROM hg GROUP BY cat HAVING COUNT(*) > 0", "cat", [1, 2, 3]),
    ("SELECT cat FROM hg GROUP BY cat HAVING cat <> 2", "cat", [1, 3]),
])
def test_having_parity_and_value(client, hg, query, col, want):
    assert _keys(_parity(client, hg, query), col) == sorted(want, key=lambda x: (x is None, x))


def test_a_constant_having_that_never_reaches_the_engine(client, hg):
    """`HAVING 1` folds away at plan time and `HAVING 0` compiles to a real
    LoadConst that drops every group — neither goes through `_parity`, since the
    folded form has no view counterpart to compare against."""
    assert _keys(rows(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING 1"), "cat") == [1, 2, 3]
    assert rows(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING 0") == []


def test_a_global_having_over_an_empty_source_still_grounds(client, hg):
    """A global aggregate over an empty source grounds to one synthetic row,
    which HAVING still filters: NULL aggregates drop it, COUNT keeps it."""
    assert _parity(client, hg, "SELECT COUNT(*) AS c FROM hg WHERE pk < 0 HAVING SUM(cat) = 0") == []
    assert _parity(client, hg, "SELECT COUNT(*) AS c FROM hg WHERE pk < 0 HAVING MAX(cat) >= 0") == []
    got = _parity(client, hg, "SELECT COUNT(*) AS c FROM hg WHERE pk < 0 HAVING COUNT(*) = 0")
    assert bag(got) == {(0,): 1}, got


def test_the_register_cap_is_located_not_pinned(client, hg):
    """The 64-register cap, found rather than hardcoded. The builder folds
    identical instructions, so the conjuncts must share nothing: `cat * k < k + 1`
    over distinct odd `k` is true exactly for `cat = 1`. Only the direct path is
    asserted: CREATE VIEW ships the same over-cap program to the engine, which
    rejects it, but the DDL still succeeds and the view is silently empty."""
    def having(k):
        return f"SELECT cat FROM hg GROUP BY cat HAVING {conjunct_ladder('cat', k)}"

    k = first_rejected(lambda k: rows(client, hg, having(k)), range(1, 64))
    assert _keys(_parity(client, hg, having(k - 1)), "cat") == [1]

    def having_in(n):
        items = ", ".join(repr(f"v{i}") for i in range(n))
        return f"SELECT s FROM hg GROUP BY s HAVING s IN ({items})"

    n = first_rejected(lambda n: rows(client, hg, having_in(n)), range(1, 64))
    assert _parity(client, hg, having_in(n - 1)) == []


def test_no_float_modulo_instruction_exists(client, hg):
    _reject_both(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING SUM(v) % 2.0 > 0.5")


# ---------------------------------------------------------------------------
# AVG reads its SUM accumulator at the declared type
#
# The client combines integer SUM partials into an i64 whose bit pattern is the
# sum mod 2^64. A SUM over a `BIGINT UNSIGNED` source is declared U64, so past
# 2^63 that bit pattern only reads correctly as unsigned — SUM's own render
# reinterprets the raw bits and was always right, but AVG's divisor is the first
# consumer to do arithmetic on the value.
# ---------------------------------------------------------------------------


def test_avg_over_an_unsigned_accumulator_past_two_to_the_63(client, hg):
    """cat = 3 sums two 2^64-1 cells: the high bit is set, so a signed read of
    the accumulator renders the average negative."""
    # 2^64-2 has no exact f64 image; it rounds to 2^64, so the average is 2^63.
    got = _parity(client, hg, "SELECT cat, AVG(u) AS a FROM hg GROUP BY cat")
    assert {r.cat: r.a for r in got}[3] == 9.223372036854776e18, got
    # The same accumulator with no group columns at all.
    assert [r.a for r in _parity(client, hg, "SELECT AVG(u) AS a FROM hg WHERE cat = 3")] \
        == [9.223372036854776e18]
    # HAVING and the render read one accumulator: the predicate keeps cat = 3 on
    # a correctly-computed average, so a mis-signed render disagrees with itself.
    got = _parity(client, hg, "SELECT cat, AVG(u) AS a FROM hg GROUP BY cat HAVING AVG(u) > 1.0")
    assert _keys(got, "cat") == [2, 3], got
    # The raw-bits render is unchanged: SUM(u) still wraps to the same u64.
    got = _parity(client, hg, "SELECT cat, SUM(u) AS t FROM hg WHERE cat = 3 GROUP BY cat")
    assert [r.t for r in got] == [18446744073709551614], got


def test_avg_over_an_unsigned_cell_that_does_not_wrap(client, schema_name):
    """A group of one 2^64-1 cell: the sum does not wrap, so the true average is
    representable and can be asserted as a value rather than as view-parity."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE ua (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL,"
        " u BIGINT UNSIGNED NOT NULL)", schema_name=sn)
    client.execute_sql("INSERT INTO ua VALUES (1, 1, 18446744073709551615)", schema_name=sn)
    got = _parity(client, sn, "SELECT cat, AVG(u) AS a FROM ua GROUP BY cat")
    assert [r.a for r in got] == [1.8446744073709552e19], got


def test_the_avg_paths_the_unsigned_read_must_not_disturb(client, hg):
    """A signed source, an all-NULL group, and the global ground row. Every
    non-U64 integer source widens to the same signed accumulator, so one signed
    case covers them."""
    _parity(client, hg, "SELECT cat, AVG(sm) AS a FROM hg GROUP BY cat")
    # cat = 3 has v NULL throughout, so its count is 0 and the average is NULL.
    got = _parity(client, hg, "SELECT cat, AVG(v) AS a FROM hg GROUP BY cat")
    assert {r.cat: r.a for r in got}[3] is None, got
    # No surviving partial: the synthesized global ground row averages to NULL.
    assert [r.a for r in _parity(client, hg, "SELECT AVG(v) AS a FROM hg WHERE cat = 99")] \
        == [None]


def test_a_null_group_and_an_all_null_aggregate(client, schema_name):
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT PRIMARY KEY, g BIGINT, v BIGINT)", schema_name=sn)
    # g is nullable → a NULL group; some groups have all-NULL v → COUNT(v)=0.
    client.execute_sql(
        "INSERT INTO t VALUES (1, 10, NULL), (2, 10, NULL), (3, NULL, 5), (4, NULL, 7),"
        " (5, 20, 3)", schema_name=sn)
    # A NULL group forms its own distinct group, and a group whose aggregated
    # column is entirely NULL still emits.
    _parity(client, sn, "SELECT g, COUNT(*) AS c FROM t GROUP BY g")
    _parity(client, sn,
            "SELECT g, COUNT(v) AS cv, SUM(v) AS sv, MIN(v) AS mv FROM t GROUP BY g")


# ---------------------------------------------------------------------------
# DISTINCT
# ---------------------------------------------------------------------------


def test_distinct_clamps_every_survivor_to_weight_one(client, schema_name):
    """DISTINCT is the weight-clamp primitive, so what it owes is not a row set
    but a weight: every survivor at exactly 1, however many source rows folded
    into it. A row count cannot see a survivor that kept weight 3."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE u (pk BIGINT PRIMARY KEY, city TEXT, region BIGINT)", schema_name=sn)
    client.execute_sql(
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
        "CREATE TABLE c (pk BIGINT PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (1,1,4),(2,2,3),(3,5,0),(4,1,4)", schema_name=sn)
    # A computed set-identity key: rows 1, 2 and 4 all sum to 5, so the three
    # collapse to one survivor at weight 1.
    assert bag(_parity(client, sn, "SELECT DISTINCT a + b AS s FROM c")) == {(5,): 1}
    _parity(client, sn, "SELECT DISTINCT a, a + b AS s FROM c")

    # A name-preserving wildcard names nothing of its own, so a duplicate among
    # the source's names rides through positionally — on both paths.
    client.execute_sql(
        "CREATE TABLE d (pk BIGINT PRIMARY KEY, a BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("INSERT INTO d VALUES (1, 1), (2, 2)", schema_name=sn)
    client.execute_sql("CREATE VIEW dup AS SELECT * FROM c JOIN d ON c.a = d.pk",
                       schema_name=sn)
    assert sorted(bag(rows(client, sn, "SELECT DISTINCT * FROM dup")).values()) == [1, 1, 1]


def test_distinct_over_a_wide_group(client, schema_name):
    """A >8-column group: DISTINCT over the 10 non-PK columns collapses the
    duplicate pair to one survivor at weight 1, and DISTINCT * keeps all three
    because the PK makes each row unique."""
    sn = schema_name
    cols = ", ".join(f"c{i} BIGINT" for i in range(10))
    client.execute_sql(f"CREATE TABLE w (pk BIGINT PRIMARY KEY, {cols})", schema_name=sn)
    a = ", ".join(str(j) for j in range(10))
    b = ", ".join(str(j + 1) for j in range(10))
    client.execute_sql(f"INSERT INTO w VALUES (1, {a}), (2, {a}), (3, {b})", schema_name=sn)
    proj = ", ".join(f"c{i}" for i in range(10))
    got = bag(_parity(client, sn, f"SELECT DISTINCT {proj} FROM w"))
    assert got == {tuple(range(10)): 1, tuple(range(1, 11)): 1}
    assert sorted(bag(_parity(client, sn, "SELECT DISTINCT * FROM w")).values()) == [1, 1, 1]


# ---------------------------------------------------------------------------
# Empty input, ground row, bag-valued source
# ---------------------------------------------------------------------------


def test_an_empty_source_grounds_a_global_fold_but_not_a_grouped_one(client, schema_name):
    sn = schema_name
    client.execute_sql("CREATE TABLE e (pk BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
    assert bag(rows(client, sn, "SELECT COUNT(*) AS c, SUM(v) AS s, MAX(v) AS m FROM e")) \
        == {(0, None, None): 1}
    assert rows(client, sn, "SELECT v, COUNT(*) AS c FROM e GROUP BY v") == []
    # A WHERE filtering out every row behaves identically to an empty table.
    client.execute_sql("INSERT INTO e VALUES (1, 5)", schema_name=sn)
    assert bag(rows(client, sn, "SELECT COUNT(*) AS c, SUM(v) AS s FROM e WHERE v > 100")) \
        == {(0, None): 1}
    assert rows(client, sn, "SELECT v, COUNT(*) AS c FROM e WHERE v > 100 GROUP BY v") == []


def test_a_fold_over_a_bag_counts_logical_rows(client, schema_name):
    """A UNION ALL view whose rows carry weight 2: COUNT(*) counts the
    multiplicity, not the entries."""
    sn = schema_name
    client.execute_sql("CREATE TABLE base (pk BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
    client.execute_sql("INSERT INTO base VALUES (1, 10), (2, 20), (3, 10)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW dbl AS SELECT pk, v FROM base UNION ALL SELECT pk, v FROM base",
        schema_name=sn)
    assert bag(rows(client, sn, "SELECT COUNT(*) AS c FROM dbl")) == {(6,): 1}
    _parity(client, sn, "SELECT v, COUNT(*) AS c FROM dbl GROUP BY v")


def test_many_aggregates(client, schema_name):
    """A wide multi-aggregate query still folds. The fold's one width gate is the
    partial reply schema (1 + group cols + agg specs <= MAX_COLUMNS, checked
    client-side; wider plans route to the executor) — this exercises the
    many-accumulator fold path itself, well under that bound."""
    sn = schema_name
    n = 8
    cols = ", ".join(f"c{i} BIGINT" for i in range(n))
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT PRIMARY KEY, g BIGINT NOT NULL, {cols})", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES " + ",".join(
        f"({i}, {i % 4}, " + ", ".join(str((i * (j + 1)) % 50) for j in range(n)) + ")"
        for i in range(1, 31)), schema_name=sn)
    aggs = ", ".join(f"SUM(c{i}) AS s{i}, MAX(c{i}) AS m{i}, COUNT(c{i}) AS n{i}"
                     for i in range(n))
    _parity(client, sn, f"SELECT g, {aggs} FROM t GROUP BY g")


def test_the_per_worker_group_cap_aborts_and_the_worker_keeps_serving(adhoc_group_cap_server):
    client = adhoc_group_cap_server
    sn = "gc" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT PRIMARY KEY, g BIGINT NOT NULL)", schema_name=sn)
        # 100 distinct groups; with the cap at 4 per worker, some worker exceeds it.
        client.execute_sql(
            "INSERT INTO t VALUES " + ", ".join(f"({i}, {i})" for i in range(1, 101)),
            schema_name=sn)
        with pytest.raises(Exception) as ei:
            rows(client, sn, "SELECT g, COUNT(*) AS c FROM t GROUP BY g")
        assert "CREATE VIEW" in str(ei.value)
        assert bag(rows(client, sn, "SELECT COUNT(*) AS c FROM t")) == {(100,): 1}
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Which rows a tied cut returns is a function of (data, query) alone
#
# The ad-hoc output's hidden `_agg_pk` leads the ordering sink's identity
# tiebreak, so it fully decides every tie the ORDER BY keys leave open. It is the
# engine's `_group_pk` for the group — a pure function of the group's column
# values — carried out of the representative partial reply.
# ---------------------------------------------------------------------------


def _tied_groups(client, sn):
    """16 groups of 3 rows each: every COUNT is tied, so an ORDER BY over the
    count leaves the whole result set for the tiebreak to order. `h` gives the
    DISTINCT case a tied non-key column to order by."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, h BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES " + ", ".join(
        f"({i}, {i % 16}, {i % 2})" for i in range(1, 49)), schema_name=sn)


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
        sn = "wc" + _uid()
        client.create_schema(sn)
        _tied_groups(client, sn)
        per_count[workers] = {k: _items(rows(client, sn, q)) for k, q in queries.items()}

    for k in queries:
        assert per_count["2"][k] == per_count["4"][k], (
            f"`{queries[k]}` differs across worker counts:\n"
            f"  W=2: {per_count['2'][k]}\n  W=4: {per_count['4'][k]}")
    # Pin the GROUP BY answer outright, so the test still catches a regression if
    # two worker counts ever happen to agree on a wrong one. A single
    # non-nullable integer group column keys on the order-preserving route key,
    # so the tie breaks ascending by `g` — ASC in both directions, because
    # tiebreak keys are always ASC. (DISTINCT over two columns keys on a digest:
    # deterministic, but arbitrary with respect to the data, so only the
    # cross-worker-count equality above is asserted for it.)
    for k in ("asc", "desc"):
        assert [dict(r).get("g") for r, _w in per_count["4"][k]] == [0, 1, 2, 3], per_count["4"][k]


def test_a_tied_cut_agrees_with_a_views_cut(client, schema_name):
    """The ad-hoc cut and a view's cut return the *same* tied rows. The two keys
    are not the same bytes — a view over a single non-nullable integer group
    column keys on that column's 8-byte OPK, not on a U128 `_group_pk` — but
    `route_key` is order-preserving, so the fold's U128 key induces the identical
    order, and the client's identity tiebreak reproduces the worker's."""
    sn = schema_name
    _tied_groups(client, sn)
    # A single natural group column: the key is the plain route key.
    first = _parity_ordered(
        client, sn, "SELECT g, COUNT(*) AS c FROM t GROUP BY g",
        "ORDER BY c LIMIT 4", "ORDER BY c DESC LIMIT 4", "ORDER BY c OFFSET 5 LIMIT 6")
    assert [r._asdict()["g"] for r in first] == [0, 1, 2, 3]
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
    assert [r["category"] for r in by_call] == [r["category"] for r in by_alias]
    assert tuple(by_call[0]._fields) == ("category", "_count1"), by_call[0]

    # Every category holds 8 rows, so a LIMIT cuts a fully tied set and the
    # identity tiebreak alone decides it — ascending by category, ASC even under
    # `ORDER BY c DESC`. See the tied-cut tests above.
    cut = rows(client, orders,
               "SELECT category, COUNT(*) AS c FROM orders GROUP BY category "
               "ORDER BY c DESC LIMIT 3")
    assert [(r["category"], r["c"]) for r in cut] == [(0, 8), (1, 8), (2, 8)]
    positional = rows(client, orders,
                      "SELECT category, COUNT(*) AS c FROM orders GROUP BY category ORDER BY 1")
    assert [r["category"] for r in positional] == sorted(r["category"] for r in positional)

    with pytest.raises(Exception, match="must appear in GROUP BY"):
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
        out.append(f"({i}, {(i * 37) % _FLOAT_CATS}, {mag * (1 + rnd.random())!r})")
    client.execute_sql(
        "CREATE TABLE f (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL, x DOUBLE NOT NULL)",
        schema_name=sn)
    client.execute_sql("INSERT INTO f VALUES " + ",".join(out), schema_name=sn)


def _bits(rows):
    """Each `s` as its exact bit pattern — `float.hex()` round-trips losslessly,
    so two sums compare equal only if they are the same double."""
    return [r.s.hex() for r in rows]


@pytest.mark.parametrize("q", [
    "SELECT SUM(x) AS s FROM f",
    "SELECT cat, SUM(x) AS s FROM f GROUP BY cat",
    "SELECT cat, AVG(x) AS s FROM f GROUP BY cat",
])
def test_a_float_sum_is_stable_for_a_fixed_plan(client, schema_name, q):
    """Same query, same data, same worker count, same access path → bit-identical.
    The one reproducibility guarantee the float contract does make."""
    sn = schema_name
    _float_table(client, sn)
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

    The 3-row chunk is the point. `TestIndexBoundPushdown`'s integer-aggregate
    cases in this directory cover the same invariant at the 65 536-row default,
    where any test range is one chunk and the per-chunk sort is the global one;
    here each worker's share spans several chunks, the only regime in which the
    walk visits rows in an order a PK scan never would."""
    client = tiny_ddl_chunk_server
    sn = "ix" + _uid()
    client.create_schema(sn)
    try:
        _float_table(client, sn)
        src = "FROM f WHERE cat >= 5 AND cat < 13"
        # A strict subset of the columns, so this cannot route as the
        # unprojected plain scan and its access line is unambiguous.
        rows_q = f"SELECT pk, x {src}"
        exact_q = f"SELECT COUNT(*) AS c, SUM(pk) AS sp, MIN(x) AS mn, MAX(x) AS mx {src}"
        float_q = f"SELECT SUM(x) AS s {src}"

        # The control: with no index to bound on these are full scans, so the
        # assertion after CREATE INDEX reads the plan and not a constant.
        for q in (rows_q, exact_q, float_q):
            assert access(client, sn, q).startswith("access: full scan"), q
        before_rows = bag(rows(client, sn, rows_q))
        before_exact = _items(rows(client, sn, exact_q))
        # 8 of `_FLOAT_CATS` categories at 4 rows each — coupled to that fixture.
        # 1.6% of the table, a 4x margin inside the worker's 1/16-of-the-slice
        # cost gate, and 8 rows per worker at 4 workers: three 3-row chunks each.
        assert len(before_rows) == 32, before_rows

        client.execute_sql("CREATE INDEX fi ON f (cat)", schema_name=sn)
        # The plan-time choice, which is sink-independent: one access planner
        # serves the rows sink and the fold sink alike, so all three re-plan.
        for q in (rows_q, exact_q, float_q):
            assert access(client, sn, q).startswith("access: index range on (cat)"), q

        # The walk's own claim: a chunked drain and an unchunked one yield
        # identical multisets. The rows, then the aggregates exact over them —
        # COUNT and an integer SUM fold associatively, and MIN/MAX copy a cell's
        # bits rather than combining them.
        assert bag(rows(client, sn, rows_q)) == before_rows
        assert _items(rows(client, sn, exact_q)) == before_exact
        # The float SUM under the index plan: repeatable, and that alone.
        assert _bits(rows(client, sn, float_q)) == _bits(rows(client, sn, float_q))
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Wide (string / UUID) extremes and a spilling group column
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("agg", ["MIN(label)", "MAX(label)", "MIN(u)", "MAX(u)"])
def test_a_wide_extreme_combines_by_content(client, schema_name, agg):
    """MIN/MAX over a TEXT or UUID column: every worker's partial carries the
    value itself, and the client's combine orders them as the engine does —
    strings by content, past the inline bound and across a shared prefix."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE labelled (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL,"
        " label TEXT NOT NULL, u UUID NOT NULL)", schema_name=sn)
    labels = ["shared/prefix/aa", "shared/prefix/a", "shared/prefix/ab", "b", ""]
    src = [(i, i % 3, labels[i % 5], f"550e8400-e29b-41d4-a716-4466554400{i:02d}")
           for i in range(20)]
    client.execute_sql("INSERT INTO labelled VALUES " + ",".join(
        f"({pk}, {g}, '{label}', '{u}')" for pk, g, label, u in src), schema_name=sn)

    got = _as_dict(_parity(client, sn, f"SELECT g, {agg} AS m FROM labelled GROUP BY g"), "g", "m")
    col = 2 if "label" in agg else 3
    pick = min if agg.startswith("MIN") else max
    assert got == {g: pick(r[col] for r in src if r[1] == g) for g in range(3)}
    _parity(client, sn, f"SELECT {agg} AS m FROM labelled")
    # An empty source still grounds the global extreme to NULL.
    assert [r.m for r in _parity(client, sn, f"SELECT {agg} AS m FROM labelled WHERE pk > 100")] \
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
    # Two labels well past SHORT_STRING_THRESHOLD (12), one below it.
    labels = ["a string far past the inline bound", "another long spilling label", "short"]
    src = [(i, labels[i % 3], i) for i in range(12)]
    client.execute_sql("INSERT INTO labelled VALUES " + ",".join(
        f"({pk}, '{label}', {amount})" for pk, label, amount in src), schema_name=sn)

    got = _as_dict(_parity(client, sn,
                           "SELECT label, SUM(amount) AS s FROM labelled GROUP BY label"),
                   "label", "s")
    want = {}
    for _, label, amount in src:
        want[label] = want.get(label, 0) + amount
    assert got == want
