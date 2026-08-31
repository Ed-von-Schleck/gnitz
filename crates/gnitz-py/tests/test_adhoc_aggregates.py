"""E2E parity tests for ad-hoc single-relation aggregation (GROUP BY / global
aggregate / HAVING / DISTINCT) served by the ReadSpec aggregate hash-fold sink +
client finishing, versus a `CREATE VIEW` of the same statement.

The load-bearing assertion is **parity**: for a query with no ORDER BY / LIMIT,
the ad-hoc result equals a scan of `CREATE VIEW v AS <same query>`, exactly —
every fixture here aggregates an integer source, so both paths agree bit for bit.
A float SUM/AVG instead follows the summation order (the contract in
`agg_finish.rs`); the tests at the bottom pin what that does and does not promise.

`_parity_ordered` is the ordered sibling: it also pins which of a set of *tied*
rows a cut returns, which `_parity` cannot see.

Run with GNITZ_WORKERS=4 (the fold is per-worker; the client merges partials):
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_adhoc_aggregates.py -v --tb=short
"""
import random

import pytest
from _uid import uid as _uid




def _rows(client, sn, q):
    res = client.execute_sql(q, schema_name=sn)[0]
    assert res["type"] == "Rows", f"expected Rows, got {res['type']}: {res}"
    return list(res["rows"])


def _items(rows):
    """Each row as a sorted tuple of (name, value) pairs."""
    return [tuple(sorted(r._asdict().items())) for r in rows]


def _norm(rows):
    """`_items`, ordered, so two result sets compare order-independently.

    Exact, floats included: every `_parity` fixture aggregates an integer source,
    so both paths divide an exact sum by an exact count and agree bit for bit. A
    tolerance would only hide a real arithmetic bug. The float sources live in the
    tests below, which compare bit patterns directly."""
    return sorted(_items(rows), key=repr)


def _cleanup(client, sn, *names):
    for name in names:
        for kind in ("VIEW", "TABLE"):
            try:
                client.execute_sql(f"DROP {kind} {name}", schema_name=sn)
            except Exception:
                pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _parity(client, sn, query):
    """Assert the ad-hoc result of `query` (no ORDER BY / LIMIT) equals a scan of
    a view built from the identical statement.

    Order-*insensitive* by construction — `CREATE VIEW` rejects ORDER BY, so the
    view is built from the whole statement and `_norm` sorts both sides. Use
    `_parity_ordered` to compare which rows a cut returns."""
    adhoc = _rows(client, sn, query)
    vn = "pv_" + _uid()
    client.execute_sql(f"CREATE VIEW {vn} AS {query}", schema_name=sn)
    vid = client.resolve_table(sn, vn)[0]
    view = list(client.scan(vid))
    client.execute_sql(f"DROP VIEW {vn}", schema_name=sn)

    a, v = _norm(adhoc), _norm(view)
    assert a == v, f"ad-hoc != view for `{query}`"
    # Column-name parity (visible output columns).
    if adhoc and view:
        assert set(adhoc[0]._asdict().keys()) == set(view[0]._asdict().keys()), (
            f"column names differ for `{query}`"
        )
    return adhoc


def _parity_ordered(client, sn, body, *tails):
    """Assert the ad-hoc result of `body tail` equals reading a view of `body`
    with the same `tail` applied — row for row, in order, for each of `tails`
    against one shared view.

    A separate helper rather than an option on `_parity`: `CREATE VIEW` rejects
    ORDER BY, so `_parity` has to build the view from the whole statement and
    sort both sides, which is precisely what makes it blind to tie order. Here
    the view carries only the body and the cut runs as a second read over it, so
    the two sinks' tiebreaks are what is being compared — the client's identity
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
            adhoc = _rows(client, sn, f"{body} {tail}")
            view = _rows(client, sn, f"SELECT * FROM {vn} {tail}")
            assert _items(adhoc) == _items(view), f"ordered ad-hoc != view for `{body} {tail}`"
            out.append(adhoc)
    finally:
        client.execute_sql(f"DROP VIEW {vn}", schema_name=sn)
    return out[0]


# ---------------------------------------------------------------------------
# Grouped + global aggregate parity, WHERE, HAVING
# ---------------------------------------------------------------------------


def _setup_orders(client, sn):
    client.execute_sql(
        "CREATE TABLE orders ("
        "  pk BIGINT NOT NULL PRIMARY KEY,"
        "  category BIGINT NOT NULL,"
        "  amount BIGINT NOT NULL,"
        "  note BIGINT"  # nullable
        ")",
        schema_name=sn,
    )
    rows = [
        f"({pk}, {cat}, {amount}, {'NULL' if note is None else note})"
        for pk, cat, amount, note in _orders_oracle()
    ]
    client.execute_sql("INSERT INTO orders VALUES " + ",".join(rows), schema_name=sn)


@pytest.mark.parametrize(
    "agg",
    [
        "COUNT(*) AS m",
        "COUNT(note) AS m",
        "SUM(amount) AS m",
        "MIN(amount) AS m",
        "MAX(amount) AS m",
        "AVG(amount) AS m",
    ],
)
def test_grouped_parity(client, agg):
    sn = "aa" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        _parity(client, sn, f"SELECT category, {agg} FROM orders GROUP BY category")
        # With a WHERE (bounded PK range + residual).
        _parity(
            client,
            sn,
            f"SELECT category, {agg} FROM orders WHERE pk > 5 AND amount < 80 GROUP BY category",
        )
    finally:
        _cleanup(client, sn, "orders")


@pytest.mark.parametrize(
    "agg",
    [
        "COUNT(*) AS m",
        "COUNT(note) AS m",
        "SUM(amount) AS m",
        "MIN(amount) AS m",
        "MAX(amount) AS m",
        "AVG(amount) AS m",
    ],
)
def test_global_parity(client, agg):
    sn = "ag" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        _parity(client, sn, f"SELECT {agg} FROM orders")
        _parity(client, sn, f"SELECT {agg} FROM orders WHERE category = 2")
    finally:
        _cleanup(client, sn, "orders")


def test_multi_aggregate_and_group_col_parity(client):
    sn = "ma" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        _parity(
            client,
            sn,
            "SELECT category, COUNT(*) AS c, SUM(amount) AS s, MIN(amount) AS mn, MAX(amount) AS mx "
            "FROM orders GROUP BY category",
        )
    finally:
        _cleanup(client, sn, "orders")


def test_having_parity(client):
    sn = "hv" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        # HAVING over a projected aggregate, and over an aggregate only in HAVING.
        _parity(client, sn, "SELECT category, COUNT(*) AS c FROM orders GROUP BY category HAVING COUNT(*) > 7")
        _parity(client, sn, "SELECT category FROM orders GROUP BY category HAVING SUM(amount) > 300")
        _parity(client, sn, "SELECT category, AVG(amount) AS a FROM orders GROUP BY category HAVING AVG(amount) > 40")
        # An aggregate that is NULL for some groups (COUNT(note)=0 → HAVING drops it, 3VL).
        _parity(client, sn, "SELECT category, MIN(note) AS mn FROM orders GROUP BY category HAVING MIN(note) > 10")
        # Global HAVING (grounds then filters).
        _parity(client, sn, "SELECT COUNT(*) AS c FROM orders HAVING COUNT(*) > 0")
    finally:
        _cleanup(client, sn, "orders")


# ---------------------------------------------------------------------------
# Computed group keys and computed finalize items
#
# The three shapes below used to be the whole visible difference between the two
# grouped front ends: a view accepted each of them and a direct SELECT rejected
# it, because the ad-hoc path ran a second, narrower binder. There is one binder
# now, so `_parity` is the regression test — but parity alone would also hold if
# BOTH paths were wrong, so each case is pinned against an in-Python oracle too.
#
# Physically these exercise the two maps a reduce can carry. A computed GROUP BY
# key or aggregate argument is the **pre-map**, which runs on the worker between
# the predicate and the fold (`AggReadSpec.pre_map`); an expression over the
# aggregates is the **finalize map**, which the client runs over its combined
# reduce output. A run at W=4 is what makes the pre-map's fused filter->map path
# and the cross-worker combine both real.
# ---------------------------------------------------------------------------


def _orders_oracle():
    """The `orders` fixture's rows as `(pk, category, amount, note)`.

    The one definition: `_setup_orders` builds its INSERT from this, and the
    oracles below compute over it, so changing the fixture cannot leave a
    hand-written expectation quietly describing the old data.
    """
    return [(i, i % 5, (i * 7) % 100, None if i % 3 == 0 else (i * 3) % 50) for i in range(1, 41)]


def _grouped_oracle(key, value):
    """`{key(row): value(rows of that group)}` over the orders fixture."""
    groups = {}
    for row in _orders_oracle():
        groups.setdefault(key(row), []).append(row)
    return {k: value(v) for k, v in groups.items()}


def _as_dict(rows, kname, vname):
    return {r._asdict()[kname]: r._asdict()[vname] for r in rows}


def test_expression_over_aggregates_parity(client):
    """An expression whose operand is an aggregate — the post-reduce finalize map.

    `SUM(amount) + 1` is the case a direct SELECT used to refuse with "GROUP BY
    SELECT: only column refs and aggregates supported"."""
    sn = "cf" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        rows = _parity(client, sn, "SELECT category, SUM(amount) + 1 AS s FROM orders GROUP BY category")
        assert _as_dict(rows, "category", "s") == _grouped_oracle(
            lambda r: r[1], lambda g: sum(r[2] for r in g) + 1
        )
        # Two aggregates in one expression, and an aggregate under a second
        # operator — the composite is deeper than one binary node.
        rows = _parity(
            client,
            sn,
            "SELECT category, SUM(amount) - COUNT(*) * 2 AS d FROM orders GROUP BY category",
        )
        assert _as_dict(rows, "category", "d") == _grouped_oracle(
            lambda r: r[1], lambda g: sum(r[2] for r in g) - len(g) * 2
        )
        # An expression over AVG: the finalize composite (a divide) feeding
        # another operator, so the divide is no longer the whole item.
        rows = _parity(client, sn, "SELECT category, AVG(amount) * 2 AS a FROM orders GROUP BY category")
        assert _as_dict(rows, "category", "a") == _grouped_oracle(
            lambda r: r[1], lambda g: sum(r[2] for r in g) / len(g) * 2
        )
        # A group column inside an expression, mixed with an aggregate.
        rows = _parity(
            client,
            sn,
            "SELECT category, category * 100 + COUNT(*) AS mixed FROM orders GROUP BY category",
        )
        assert _as_dict(rows, "category", "mixed") == _grouped_oracle(
            lambda r: r[1], lambda g: g[0][1] * 100 + len(g)
        )
    finally:
        _cleanup(client, sn, "orders")


def test_aggregate_over_expression_parity(client):
    """An aggregate whose argument is computed — the pre-map, on the worker.

    `SUM(amount * category)` is the case a direct SELECT used to refuse with
    "aggregate on computed expression not supported"."""
    sn = "ca" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        rows = _parity(client, sn, "SELECT category, SUM(amount * category) AS s FROM orders GROUP BY category")
        assert _as_dict(rows, "category", "s") == _grouped_oracle(
            lambda r: r[1], lambda g: sum(r[2] * r[1] for r in g)
        )
        # MIN/MAX over a computed argument: the pre-map column feeds a
        # non-linear aggregate, not just a sum.
        rows = _parity(client, sn, "SELECT category, MAX(amount + pk) AS mx FROM orders GROUP BY category")
        assert _as_dict(rows, "category", "mx") == _grouped_oracle(
            lambda r: r[1], lambda g: max(r[2] + r[0] for r in g)
        )
        # With a WHERE, so the worker runs predicate -> pre-map -> fold and the
        # map sees survivor ranges rather than whole chunks.
        rows = _parity(
            client,
            sn,
            "SELECT category, SUM(amount * 2) AS s FROM orders WHERE pk > 5 AND amount < 80 GROUP BY category",
        )
        # Filtered before grouping, so a category with no survivor is absent
        # rather than present at 0 — which is what the fold emits.
        kept = [r for r in _orders_oracle() if r[0] > 5 and r[2] < 80]
        expected = {}
        for r in kept:
            expected[r[1]] = expected.get(r[1], 0) + r[2] * 2
        assert _as_dict(rows, "category", "s") == expected
    finally:
        _cleanup(client, sn, "orders")


def test_premap_at_the_column_limit(client):
    """A pre-map on a source already at the schema's column limit.

    The pre-map's output is the source's columns *plus* the computed ones, so a
    maximally wide source has no room for one. That has to be a plan-time feature
    message: the worker refuses the same width, but only as a trust boundary, and
    reaching it would surface a server error for a legal statement.

    The control matters as much as the rejection — one column narrower, the same
    query must compile — or a gate that simply rejected every wide table would
    pass the first half.
    """
    sn = "cl" + _uid()
    client.create_schema(sn)
    # MAX_COLUMNS is 65 and counts the PK, so 65 total leaves no room for a
    # computed key; 64 leaves exactly one.
    try:
        for n_cols, should_compile in ((64, True), (65, False)):
            name = f"w{n_cols}"
            payload = ", ".join(f"c{i} BIGINT NOT NULL" for i in range(n_cols - 1))
            client.execute_sql(
                f"CREATE TABLE {name} (pk BIGINT PRIMARY KEY, {payload})", schema_name=sn
            )
            vals = ", ".join(str(i) for i in range(n_cols - 1))
            client.execute_sql(f"INSERT INTO {name} VALUES (1, {vals})", schema_name=sn)
            q = f"SELECT c0 + 1 AS k, COUNT(*) AS c FROM {name} GROUP BY c0 + 1"
            if should_compile:
                assert _as_dict(_rows(client, sn, q), "k", "c") == {1: 1}
            else:
                with pytest.raises(Exception) as ei:
                    _rows(client, sn, q)
                msg = str(ei.value)
                assert "65" in msg, f"the limit must name itself, got: {msg}"
    finally:
        _cleanup(client, sn, "w64", "w65")


def test_premap_over_a_source_whose_pk_is_not_first(client):
    """A pre-map reduce over a table whose PRIMARY KEY is not column 0.

    The pre-map's output schema is the source's columns with the PK columns moved
    to the front (`place_pk_front`), so it is a *permutation* of the source
    whenever the PK is not already leading — and a WHERE is resolved to positions
    against the source, not against that permutation. Every other fixture here
    declares `pk` first, which makes the permutation the identity and hides any
    confusion between the two orders; this one does not.

    A DOUBLE and a TEXT column ahead of the PK, because the symptom is a *type*
    mismatch at the permuted slot: the predicate reads whichever column the
    permutation moved into its position, so the wrong-typed cases fail loudly
    while a same-width integer one would just compare the wrong column.
    """
    sn = "nl" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE nl (x DOUBLE NOT NULL, s TEXT NOT NULL, id BIGINT NOT NULL, v BIGINT NOT NULL,"
            " PRIMARY KEY (id))",
            schema_name=sn,
        )
        rows = [(i * 1.5, "a" if i % 2 else "b", i, i % 4) for i in range(1, 13)]
        client.execute_sql(
            "INSERT INTO nl VALUES " + ",".join(f"({x}, '{s}', {i}, {v})" for x, s, i, v in rows),
            schema_name=sn,
        )
        # A string WHERE: the source's TEXT column sits where the permutation puts
        # an integer, so a predicate typed against the pre-map output is rejected
        # outright ("comparison Eq against a string needs both operands to be
        # strings") rather than answering wrongly.
        got = _parity(
            client, sn, "SELECT v + 1 AS k, COUNT(*) AS c FROM nl WHERE s = 'a' GROUP BY v + 1"
        )
        expected = {}
        for x, s, i, v in rows:
            if s == "a":
                expected[v + 1] = expected.get(v + 1, 0) + 1
        assert _as_dict(got, "k", "c") == expected
        # A float WHERE, which fails in the engine rather than the planner.
        got = _parity(
            client, sn, "SELECT v AS k, SUM(id * 2) AS s FROM nl WHERE x > 6.0 GROUP BY v"
        )
        expected = {}
        for x, s, i, v in rows:
            if x > 6.0:
                expected[v] = expected.get(v, 0) + i * 2
        assert _as_dict(got, "k", "s") == expected
        # A computed aggregate argument over the same shape, so the pre-map is
        # reached by the argument rather than by the key.
        got = _parity(
            client, sn, "SELECT v AS k, MAX(id * 3) AS m FROM nl WHERE s = 'b' GROUP BY v"
        )
        expected = {}
        for x, s, i, v in rows:
            if s == "b":
                expected[v] = max(expected.get(v, 0), i * 3)
        assert _as_dict(got, "k", "m") == expected
    finally:
        _cleanup(client, sn, "nl")


def test_computed_key_over_a_replicated_source(client):
    """A pre-map fold over a replicated table.

    Replication is the trap for anything that touches reduce routing: every
    worker holds every row, so a fold that double-counted would still return
    plausible groups. The pre-map hands the fold a *derived* schema (source PK
    columns plus the computed ones) rather than the source's, so this pins that
    swapping the descriptor did not change which rows a worker folds.
    """
    sn = "rp" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE rt (pk BIGINT PRIMARY KEY, c BIGINT NOT NULL, a BIGINT NOT NULL) "
            "WITH (replicated = true)",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO rt VALUES " + ",".join(f"({i}, {i % 3}, {i})" for i in range(1, 13)),
            schema_name=sn,
        )
        # The control: no pre-map, so a double-count would show here too.
        rows = _parity(client, sn, "SELECT c, COUNT(*) AS n FROM rt GROUP BY c")
        assert _as_dict(rows, "c", "n") == {0: 4, 1: 4, 2: 4}
        rows = _parity(client, sn, "SELECT c + 1 AS k, SUM(a * 2) AS s FROM rt GROUP BY c + 1")
        expected = {}
        for i in range(1, 13):
            expected[i % 3 + 1] = expected.get(i % 3 + 1, 0) + i * 2
        assert _as_dict(rows, "k", "s") == expected
    finally:
        _cleanup(client, sn, "rt")


def test_grouped_guards_survive_on_both_paths(client):
    """The grouped rejections a direct SELECT must keep, now that it shares the
    view's binder rather than running one of its own.

    Widening a front end is where guards get dropped silently — a rejection that
    was a literal in the narrower binder and has no counterpart in the wider one
    simply stops firing, and the query returns a wrong answer instead of an
    error. The float-key one is the load-bearing case: a float group key breaks
    the byte-equal key contract (±0.0 differ byte-wise but compare equal), so
    losing it would silently split or merge groups rather than fail.
    """
    sn = "gg" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE gg (pk BIGINT PRIMARY KEY, price DOUBLE NOT NULL, n BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO gg VALUES (1, 1.5, 7), (2, 2.5, 8)", schema_name=sn)
        for q in (
            # A float GROUP BY key, written bare and as an expression — the
            # expression form has no column name to blame, so it is a separate
            # rejection site.
            "SELECT price, COUNT(*) AS c FROM gg GROUP BY price",
            "SELECT price + 1 AS k, COUNT(*) AS c FROM gg GROUP BY price + 1",
            # A column that is neither grouped nor aggregated, bare and inside an
            # expression — the second only became expressible with the finalize
            # map, so it is new surface for this guard.
            "SELECT pk, COUNT(*) AS c FROM gg GROUP BY n",
            "SELECT pk + 1 AS k, COUNT(*) AS c FROM gg GROUP BY n",
            # An expression that is not the grouped one: `n * 2` is not `n + 1`,
            # so it reads an ungrouped column however it is spelled.
            "SELECT n * 2 AS k FROM gg GROUP BY n + 1",
            # An aggregate over an aggregate.
            "SELECT SUM(COUNT(pk)) AS s FROM gg GROUP BY n",
            # HAVING over an ungrouped column.
            "SELECT n, COUNT(*) AS c FROM gg GROUP BY n HAVING pk > 1",
        ):
            _reject_both(client, sn, q)
    finally:
        _cleanup(client, sn, "gg")


def test_computed_group_key_parity(client):
    """A GROUP BY over an expression — the pre-map materializing the key.

    `GROUP BY category + amount` is the case a direct SELECT used to refuse with
    "GROUP BY: only simple column references supported"."""
    sn = "ck" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        rows = _parity(
            client,
            sn,
            "SELECT category + amount AS k, COUNT(*) AS c FROM orders GROUP BY category + amount",
        )
        assert _as_dict(rows, "k", "c") == _grouped_oracle(lambda r: r[1] + r[2], len)
        # The written key reaches the SELECT list and HAVING by its expression,
        # not by a name — one pre-map column serves all three positions.
        rows = _parity(
            client,
            sn,
            "SELECT amount * 2 AS k2, SUM(pk) AS s FROM orders GROUP BY amount * 2 HAVING SUM(pk) > 20",
        )
        assert _as_dict(rows, "k2", "s") == {
            k: v
            for k, v in _grouped_oracle(lambda r: r[2] * 2, lambda g: sum(r[0] for r in g)).items()
            if v > 20
        }
        # A computed key and a computed aggregate argument in one reduce: both
        # pre-map columns, deduped against the same memo.
        rows = _parity(
            client,
            sn,
            "SELECT category + 1 AS k, SUM(amount * 3) AS s FROM orders GROUP BY category + 1",
        )
        assert _as_dict(rows, "k", "s") == _grouped_oracle(
            lambda r: r[1] + 1, lambda g: sum(r[2] * 3 for r in g)
        )
        # The same expression written as the key and as the projection is ONE
        # column: `SELECT category + amount` binds to the pre-map column the
        # reduce already groups by, rather than minting a second one.
        rows = _parity(
            client,
            sn,
            "SELECT category + amount AS k, category + amount AS again, COUNT(*) AS c "
            "FROM orders GROUP BY category + amount",
        )
        assert all(r._asdict()["k"] == r._asdict()["again"] for r in rows)
    finally:
        _cleanup(client, sn, "orders")


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


@pytest.fixture
def hg(client):
    """A schema holding the `hg` table every HAVING test below reads, yielded as
    the schema name."""
    sn = "hg" + _uid()
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE hg (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL, s TEXT,"
        "                 u BIGINT UNSIGNED NOT NULL, sm SMALLINT, nn BIGINT NOT NULL, v BIGINT)",
        schema_name=sn,
    )
    client.execute_sql(
        "INSERT INTO hg VALUES "
        "(1, 1, 'a',  9223372036854775808, -5,   10, 1),"
        "(2, 1, 'a',  9223372036854775808, -1,   20, NULL),"
        "(3, 2, 'b',  5,                    3,   30, 7),"
        "(4, 2, NULL, 5,                    NULL, 40, NULL),"
        "(5, 3, 'c',  18446744073709551615, 9,   50, NULL),"
        "(6, 3, 'c',  18446744073709551615, 2,   60, NULL)",
        schema_name=sn,
    )
    yield sn
    _cleanup(client, sn, "hg")


def _keys(rows, col):
    return sorted(getattr(r, col) for r in rows)


def _reject_both(client, sn, query):
    """Assert `query` is rejected on the direct path AND as a CREATE VIEW, and
    return the direct path's message."""
    with pytest.raises(Exception) as ei:
        _rows(client, sn, query)
    msg = str(ei.value)
    vn = "rv_" + _uid()
    with pytest.raises(Exception):
        client.execute_sql(f"CREATE VIEW {vn} AS {query}", schema_name=sn)
    return msg


def test_having_discriminating_parity(client, hg):
    """The cases where the ad-hoc and view physical shapes genuinely differ."""
    # A single NOT NULL U64 group column: the view routes it through the OPK PK
    # region (SingleNaturalCol + LoadPk), ad-hoc through a payload slot. Also
    # pins the unsigned comparison.
    got = _parity(client, hg, "SELECT u, COUNT(*) AS c FROM hg GROUP BY u HAVING u > 9223372036854775807")
    assert _keys(got, "u") == [9223372036854775808, 18446744073709551615], got
    # A negative literal against a U64 column reads as u64::MAX on both paths,
    # so the predicate is always false.
    got = _parity(client, hg, "SELECT u, COUNT(*) AS c FROM hg GROUP BY u HAVING u > -1")
    assert got == [], got
    # A NOT NULL aggregate source, grouped: the view resolves `no_nulls = true`
    # and reads the verdict out of the register file, ad-hoc resolves it false
    # and reads `bool_bits & !null_bits`. Both evaluation arms of `filter`.
    got = _parity(client, hg, "SELECT cat, SUM(nn) AS t FROM hg GROUP BY cat HAVING SUM(nn) > 0")
    assert _keys(got, "cat") == [1, 2, 3], got
    # The only narrow (2-byte) aggregate partial: MIN over SMALLINT keeps the
    # source width, so the value round-trips i64 -> 2 bytes -> sign-extended i64.
    got = _parity(client, hg, "SELECT cat, MIN(sm) AS m FROM hg GROUP BY cat HAVING MIN(sm) < 0")
    assert _keys(got, "cat") == [1], got


def test_having_new_capabilities(client, hg):
    """Every one of these was rejected on the direct path before the swap."""
    cases = [
        ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s = 'a'", "s", ["a"]),
        ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s > 'a'", "s", ["b", "c"]),
        ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s IN ('a', 'c')", "s", ["a", "c"]),
        ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s IS NULL", "s", [None]),
        ("SELECT s, COUNT(*) AS c FROM hg GROUP BY s HAVING s IS NOT NULL", "s", ["a", "b", "c"]),
        ("SELECT cat, MIN(v) AS m FROM hg GROUP BY cat HAVING MIN(v) IS NULL", "cat", [3]),
        (
            "SELECT cat, COUNT(*) AS c FROM hg GROUP BY cat "
            "HAVING CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1",
            "cat",
            [1, 2, 3],
        ),
        ("SELECT cat, MIN(v) AS m FROM hg GROUP BY cat HAVING MIN(v) IN (1, 7)", "cat", [1, 2]),
    ]
    for q, col, want in cases:
        got = _parity(client, hg, q)
        assert _keys(got, col) == sorted(want), f"{q} -> {got}"


def test_having_constants_and_edges(client, hg):
    """Constant predicates, all-dropped / all-kept, non-contiguous survivors, an
    empty source, and -0.0 truthiness — each with a positive control, so a
    "drops everything" implementation cannot pass."""
    # A statically-true HAVING folds away; 0 compiles to a real LoadConst that
    # drops every group, and NULL is UNKNOWN — never truthy — so it drops every
    # group too.
    assert _keys(_rows(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING 1"), "cat") == [1, 2, 3]
    assert _rows(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING 0") == []
    assert _parity(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING NULL") == []
    # Every group dropped, and its control.
    assert _parity(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING COUNT(*) > 99") == []
    assert _keys(_parity(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING COUNT(*) > 0"), "cat") == [1, 2, 3]
    # Several filter ranges rather than one (0, n).
    assert _keys(_parity(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING cat <> 2"), "cat") == [1, 3]
    # A global aggregate over an empty source grounds to one synthetic row,
    # which HAVING still filters: NULL aggregates drop it, COUNT keeps it.
    assert _parity(client, hg, "SELECT COUNT(*) AS c FROM hg WHERE pk < 0 HAVING SUM(cat) = 0") == []
    got = _parity(client, hg, "SELECT COUNT(*) AS c FROM hg WHERE pk < 0 HAVING COUNT(*) = 0")
    assert [r.c for r in got] == [0], got
    assert _parity(client, hg, "SELECT COUNT(*) AS c FROM hg WHERE pk < 0 HAVING MAX(cat) >= 0") == []
    # A bare float HAVING evaluating to -0.0: its bit pattern is nonzero, so
    # the group is KEPT (the engine filter bit-tests, it does not compare 0.0).
    got = _parity(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING -(SUM(cat) * 0.0)")
    assert _keys(got, "cat") == [1, 2, 3], got


def test_having_rejections(client, hg):
    """What the shared compiler still refuses — the same refusal on both paths —
    plus the 64-register cap, with a control one conjunct under it."""
    # No float-modulo instruction exists.
    _reject_both(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING SUM(v) % 2.0 > 0.5")
    # Two string literals now compare through the string register channel rather
    # than needing a column operand to dispatch on — so this is a constant-false
    # HAVING, on both paths.
    assert _parity(client, hg, "SELECT cat FROM hg GROUP BY cat HAVING 'a' = 'b'") == []

    # The 64-register cap. The planner's expression builder never reuses a register, so an equality
    # conjunct over a plain group column costs 3 (load_col + load_const + cmp)
    # and each AND one more: k conjuncts need 4k - 1, so 16 fit in 64 and 17 do
    # not. A string IN costs 2N - 1 (one str_col_eq_const per item, whose column
    # and const-pool indices are immediates, plus N-1 BoolOr): 32 fit, 33 do not.
    #
    # Only the direct path is asserted. CREATE VIEW ships the same over-cap
    # program to the engine, which rejects it — but the DDL still succeeds and
    # the view is silently empty, a separate defect not fixed here.
    def _cap_rejection(query):
        with pytest.raises(Exception) as ei:
            _rows(client, hg, query)
        msg = str(ei.value)
        assert "64" in msg, f"the register-cap message must name the limit, got: {msg}"

    conj = lambda k: " AND ".join(["cat = 1"] * k)
    _cap_rejection(f"SELECT cat FROM hg GROUP BY cat HAVING {conj(17)}")
    got = _parity(client, hg, f"SELECT cat FROM hg GROUP BY cat HAVING {conj(16)}")
    assert _keys(got, "cat") == [1], got

    in_list = lambda n: ", ".join(f"'v{i}'" for i in range(n))
    _cap_rejection(f"SELECT s FROM hg GROUP BY s HAVING s IN ({in_list(33)})")
    assert _parity(client, hg, f"SELECT s FROM hg GROUP BY s HAVING s IN ({in_list(32)})") == []


# ---------------------------------------------------------------------------
# AVG reads its SUM accumulator at the declared type
#
# The client combines integer SUM partials into an i64 whose bit pattern is the
# sum mod 2^64. A SUM over a `BIGINT UNSIGNED` source is declared U64, so past
# 2^63 that bit pattern only reads correctly as unsigned — SUM's own render
# reinterprets the raw bits and was always right, but AVG's divisor is the first
# consumer to do arithmetic on the value.
# ---------------------------------------------------------------------------


def test_unsigned_avg_parity(client, hg):
    """AVG over a U64 source whose group sum passes 2^63, against the view."""
    # cat = 3 sums two 2^64-1 cells: the high bit is set, so a signed read of the
    # accumulator renders the average negative.
    got = _parity(client, hg, "SELECT cat, AVG(u) AS a FROM hg GROUP BY cat")
    # 2^64-2 has no exact f64 image; it rounds to 2^64, so the average is 2^63.
    assert {r.cat: r.a for r in got}[3] == 9.223372036854776e18, got
    # The same accumulator with no group columns at all.
    got = _parity(client, hg, "SELECT AVG(u) AS a FROM hg WHERE cat = 3")
    assert [r.a for r in got] == [9.223372036854776e18], got
    # HAVING and the render read one accumulator: the predicate keeps cat = 3 on
    # a correctly-computed average, so a mis-signed render disagrees with itself.
    got = _parity(client, hg, "SELECT cat, AVG(u) AS a FROM hg GROUP BY cat HAVING AVG(u) > 1.0")
    assert _keys(got, "cat") == [2, 3], got
    # The raw-bits render is unchanged: SUM(u) still wraps to the same u64.
    got = _parity(client, hg, "SELECT cat, SUM(u) AS t FROM hg WHERE cat = 3 GROUP BY cat")
    assert [r.t for r in got] == [18446744073709551614], got


def test_unsigned_avg_arithmetic_truth(client):
    """A group of one 2^64-1 cell: the sum does not wrap, so the true average is
    representable and can be asserted as a value rather than as view-parity."""
    sn = "ua" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE ua (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL, u BIGINT UNSIGNED NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO ua VALUES (1, 1, 18446744073709551615)", schema_name=sn)
        got = _parity(client, sn, "SELECT cat, AVG(u) AS a FROM ua GROUP BY cat")
        assert [r.a for r in got] == [1.8446744073709552e19], got
    finally:
        _cleanup(client, sn, "ua")


def test_signed_and_null_avg_parity(client, hg):
    """The AVG paths the unsigned read must not disturb: a signed source, an
    all-NULL group, and the global ground row. Every non-U64 integer source
    widens to the same signed accumulator, so one signed case covers them."""
    _parity(client, hg, "SELECT cat, AVG(sm) AS a FROM hg GROUP BY cat")
    # cat = 3 has v NULL throughout, so its count is 0 and the average is NULL.
    got = _parity(client, hg, "SELECT cat, AVG(v) AS a FROM hg GROUP BY cat")
    assert {r.cat: r.a for r in got}[3] is None, got
    # No surviving partial: the synthesized global ground row averages to NULL.
    got = _parity(client, hg, "SELECT AVG(v) AS a FROM hg WHERE cat = 99")
    assert [r.a for r in got] == [None], got


def test_null_group_and_all_null_agg(client):
    sn = "ng" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT PRIMARY KEY, g BIGINT, v BIGINT)", schema_name=sn
        )
        # g is nullable → a NULL group; some groups have all-NULL v → COUNT(v)=0.
        client.execute_sql(
            "INSERT INTO t VALUES (1, 10, NULL), (2, 10, NULL), (3, NULL, 5), (4, NULL, 7), (5, 20, 3)",
            schema_name=sn,
        )
        # A NULL group forms its own distinct group.
        _parity(client, sn, "SELECT g, COUNT(*) AS c FROM t GROUP BY g")
        # A group whose aggregated column is entirely NULL still emits (COUNT(v)=0).
        _parity(client, sn, "SELECT g, COUNT(v) AS cv, SUM(v) AS sv, MIN(v) AS mv FROM t GROUP BY g")
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# DISTINCT
# ---------------------------------------------------------------------------


def test_distinct_parity(client):
    sn = "di" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE u (pk BIGINT PRIMARY KEY, city TEXT, region BIGINT)", schema_name=sn
        )
        client.execute_sql(
            "INSERT INTO u VALUES (1,'nyc',1),(2,'sf',2),(3,'nyc',1),(4,'sf',2),(5,'la',3),(6,'nyc',1)",
            schema_name=sn,
        )
        _parity(client, sn, "SELECT DISTINCT city FROM u")
        _parity(client, sn, "SELECT DISTINCT city, region FROM u")
        _parity(client, sn, "SELECT DISTINCT region FROM u WHERE region > 1")
    finally:
        _cleanup(client, sn, "u")


def test_distinct_star_wide_table(client):
    sn = "dw" + _uid()
    client.create_schema(sn)
    try:
        # A >8-column (wide group) table with a non-PK duplicate: DISTINCT over the
        # non-PK columns collapses the duplicate, and DISTINCT * keeps all (unique PK).
        cols = ", ".join(f"c{i} BIGINT" for i in range(10))
        client.execute_sql(f"CREATE TABLE w (pk BIGINT PRIMARY KEY, {cols})", schema_name=sn)
        vals = ", ".join(str(j) for j in range(10))
        vals2 = ", ".join(str(j + 1) for j in range(10))
        client.execute_sql(
            f"INSERT INTO w VALUES (1, {vals}), (2, {vals}), (3, {vals2})", schema_name=sn
        )
        proj = ", ".join(f"c{i}" for i in range(10))
        # DISTINCT over the 10 non-PK columns: rows 1 and 2 collapse.
        rows = _parity(client, sn, f"SELECT DISTINCT {proj} FROM w")
        assert len(rows) == 2
        assert len([k for k in rows[0]._asdict() if k != "weight"]) == 10
        # DISTINCT * keeps the PK, so all three rows survive.
        _parity(client, sn, "SELECT DISTINCT * FROM w")
    finally:
        _cleanup(client, sn, "w")


# ---------------------------------------------------------------------------
# Empty input, ground row, bag-valued (UNION ALL weight-2) source
# ---------------------------------------------------------------------------


def test_empty_table(client):
    sn = "et" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE e (pk BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        # Global aggregate over an empty table → single ground row.
        rows = _rows(client, sn, "SELECT COUNT(*) AS c, SUM(v) AS s, MAX(v) AS m FROM e")
        assert len(rows) == 1
        assert rows[0]["c"] == 0
        assert rows[0]["s"] is None
        assert rows[0]["m"] is None
        # Grouped aggregate over an empty table → zero rows.
        assert _rows(client, sn, "SELECT v, COUNT(*) AS c FROM e GROUP BY v") == []
        # WHERE filtering out every row behaves identically.
        client.execute_sql("INSERT INTO e VALUES (1, 5)", schema_name=sn)
        rows = _rows(client, sn, "SELECT COUNT(*) AS c, SUM(v) AS s FROM e WHERE v > 100")
        assert rows[0]["c"] == 0 and rows[0]["s"] is None
        assert _rows(client, sn, "SELECT v, COUNT(*) AS c FROM e WHERE v > 100 GROUP BY v") == []
    finally:
        _cleanup(client, sn, "e")


def test_union_all_weight_two_count(client):
    sn = "ua" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE base (pk BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
        client.execute_sql("INSERT INTO base VALUES (1, 10), (2, 20), (3, 10)", schema_name=sn)
        # A UNION ALL view whose rows carry weight 2 (base UNION ALL base).
        client.execute_sql(
            "CREATE VIEW dbl AS SELECT pk, v FROM base UNION ALL SELECT pk, v FROM base",
            schema_name=sn,
        )
        rows = _rows(client, sn, "SELECT COUNT(*) AS c FROM dbl")
        assert rows[0]["c"] == 6, rows  # 3 base rows × weight 2
        # Grouped counts logical rows too.
        _parity(client, sn, "SELECT v, COUNT(*) AS c FROM dbl GROUP BY v")
    finally:
        _cleanup(client, sn, "dbl", "base")


# ---------------------------------------------------------------------------
# ORDER BY / LIMIT (client-side sink), rejections
# ---------------------------------------------------------------------------


def test_order_by_alias_and_position(client):
    sn = "ob" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        rows = _rows(
            client, sn, "SELECT category, COUNT(*) AS c FROM orders GROUP BY category ORDER BY c DESC LIMIT 3"
        )
        counts = [r["c"] for r in rows]
        assert len(rows) == 3
        assert counts == sorted(counts, reverse=True)
        # Every category holds 8 rows, so the LIMIT cuts a fully tied set and the
        # identity tiebreak alone decides it — ascending by category, ASC even
        # under `ORDER BY c DESC`. See the tied-cut tests at the bottom.
        assert [r["category"] for r in rows] == [0, 1, 2]
        # Positional ORDER BY over the visible output columns.
        rows2 = _rows(client, sn, "SELECT category, COUNT(*) AS c FROM orders GROUP BY category ORDER BY 1")
        assert [r["category"] for r in rows2] == sorted(r["category"] for r in rows2)
    finally:
        _cleanup(client, sn, "orders")


def test_rejections(client):
    sn = "rj" + _uid()
    client.create_schema(sn)
    try:
        _setup_orders(client, sn)
        # ORDER BY <aggregate function> stays rejected (alias/position only).
        with pytest.raises(Exception):
            _rows(client, sn, "SELECT category, COUNT(*) FROM orders GROUP BY category ORDER BY COUNT(*)")
        # DISTINCT over a float key is rejected (parity with the view path).
        client.execute_sql("CREATE TABLE fp (pk BIGINT PRIMARY KEY, price DOUBLE)", schema_name=sn)
        client.execute_sql("INSERT INTO fp VALUES (1, 1.5), (2, 2.5)", schema_name=sn)
        with pytest.raises(Exception):
            _rows(client, sn, "SELECT DISTINCT price FROM fp")
    finally:
        _cleanup(client, sn, "orders", "fp")


# ---------------------------------------------------------------------------
# Many aggregate columns (well under the fold's physical-spec cap)
# ---------------------------------------------------------------------------


def test_many_aggregates(client):
    """A wide multi-aggregate query still folds. The fold's one width gate is
    the partial reply schema (1 + group cols + agg specs <= MAX_COLUMNS, checked
    client-side; wider plans route to the executor) — this exercises the
    many-accumulator fold path itself, well under that bound."""
    sn = "fb" + _uid()
    client.create_schema(sn)
    try:
        n = 8
        cols = ", ".join(f"c{i} BIGINT" for i in range(n))
        client.execute_sql(f"CREATE TABLE t (pk BIGINT PRIMARY KEY, g BIGINT NOT NULL, {cols})", schema_name=sn)
        rows = []
        for i in range(1, 31):
            vals = ", ".join(str((i * (j + 1)) % 50) for j in range(n))
            rows.append(f"({i}, {i % 4}, {vals})")
        client.execute_sql("INSERT INTO t VALUES " + ",".join(rows), schema_name=sn)
        aggs = ", ".join(f"SUM(c{i}) AS s{i}, MAX(c{i}) AS m{i}, COUNT(c{i}) AS n{i}" for i in range(n))
        _parity(client, sn, f"SELECT g, {aggs} FROM t GROUP BY g")
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Runtime per-worker group cap (dedicated low-cap server)
# ---------------------------------------------------------------------------


def test_group_cap_aborts(adhoc_group_cap_server):
    client = adhoc_group_cap_server
    sn = "gc" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE t (pk BIGINT PRIMARY KEY, g BIGINT NOT NULL)", schema_name=sn)
        # 100 distinct groups; with the cap at 4 per worker, some worker exceeds it.
        vals = ", ".join(f"({i}, {i})" for i in range(1, 101))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        with pytest.raises(Exception) as ei:
            _rows(client, sn, "SELECT g, COUNT(*) AS c FROM t GROUP BY g")
        assert "CREATE VIEW" in str(ei.value)
        # The worker keeps serving afterwards.
        rows = _rows(client, sn, "SELECT COUNT(*) AS c FROM t")
        assert rows[0]["c"] == 100
    finally:
        _cleanup(client, sn, "t")


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
        schema_name=sn,
    )
    vals = ", ".join(f"({i}, {i % 16}, {i % 2})" for i in range(1, 49))
    client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)


def test_tied_cut_is_worker_count_independent(seamed_server):
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
        per_count[workers] = {k: [tuple(r) for r in _rows(client, sn, q)] for k, q in queries.items()}

    for k in queries:
        assert per_count["2"][k] == per_count["4"][k], (
            f"`{queries[k]}` differs across worker counts:\n"
            f"  W=2: {per_count['2'][k]}\n  W=4: {per_count['4'][k]}"
        )
    # Pin the GROUP BY answer outright, so the test still catches a regression if
    # two worker counts ever happen to agree on a wrong one. A single
    # non-nullable integer group column keys on the order-preserving route key,
    # so the tie breaks ascending by `g` — ASC in both directions, because
    # tiebreak keys are always ASC. (DISTINCT over two columns keys on a digest:
    # deterministic, but arbitrary with respect to the data, so only the
    # cross-worker-count equality above is asserted for it.)
    for k in ("asc", "desc"):
        assert [r[0] for r in per_count["4"][k]] == [0, 1, 2, 3], per_count["4"][k]


def test_ordered_tie_parity_with_a_view(client):
    """The ad-hoc cut and a view's cut return the *same* tied rows. The two keys
    are not the same bytes — a view over a single non-nullable integer group
    column keys on that column's 8-byte OPK, not on a U128 `_group_pk` — but
    `route_key` is order-preserving, so the fold's U128 key induces the identical
    order, and the client's identity tiebreak reproduces the worker's."""
    sn = "op" + _uid()
    client.create_schema(sn)
    try:
        _tied_groups(client, sn)
        # A single natural group column: the key is the plain route key.
        body = "SELECT g, COUNT(*) AS c FROM t GROUP BY g"
        first = _parity_ordered(
            client, sn, body, "ORDER BY c LIMIT 4", "ORDER BY c DESC LIMIT 4", "ORDER BY c OFFSET 5 LIMIT 6"
        )
        assert [r["g"] for r in first] == [0, 1, 2, 3]
        # A two-column group key: both sides key on the digest, so this is where
        # a divergence between the fold's stamp and the view's would show.
        _parity_ordered(
            client,
            sn,
            "SELECT g, h, COUNT(*) AS c FROM t GROUP BY g, h",
            "ORDER BY c LIMIT 5",
        )
    finally:
        _cleanup(client, sn, "t")


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


def _float_rows():
    rnd = random.Random(11)
    out = []
    for i in range(1, _FLOAT_ROWS + 1):
        # Alternating +/-1e15 and 1e-8 magnitudes: the sums cancel
        # catastrophically, so any reassociation moves the low bits.
        mag = 1e15 if i % 4 == 0 else (-1e15 if i % 4 == 1 else 1e-8)
        out.append(f"({i}, {(i * 37) % _FLOAT_CATS}, {mag * (1 + rnd.random())!r})")
    return out


def _float_table(client, sn):
    client.execute_sql(
        "CREATE TABLE f (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL, x DOUBLE NOT NULL)",
        schema_name=sn,
    )
    client.execute_sql("INSERT INTO f VALUES " + ",".join(_float_rows()), schema_name=sn)


def _bits(rows):
    """Each `s` as its exact bit pattern — `float.hex()` round-trips losslessly,
    so two sums compare equal only if they are the same double."""
    return [r.s.hex() for r in rows]


def _access(client, sn, q):
    """EXPLAIN's `access:` line for `q` — which walk the plan chose. Found by
    prefix rather than by row position, so adding a plan line cannot silently
    make this read a different fact."""
    lines = [r[0] for r in _rows(client, sn, "EXPLAIN " + q)]
    got = [ln for ln in lines if ln.startswith("access: ")]
    assert len(got) == 1, lines
    return got[0]


def test_float_sum_is_stable_for_a_fixed_plan(client):
    """Same query, same data, same worker count, same access path → bit-identical.
    The one reproducibility guarantee the float contract does make."""
    sn = "fs" + _uid()
    client.create_schema(sn)
    try:
        _float_table(client, sn)
        for q in (
            "SELECT SUM(x) AS s FROM f",
            "SELECT cat, SUM(x) AS s FROM f GROUP BY cat",
            "SELECT cat, AVG(x) AS s FROM f GROUP BY cat",
        ):
            first = _bits(_rows(client, sn, q))
            assert first == _bits(_rows(client, sn, q)), f"`{q}` is not repeatable"
    finally:
        _cleanup(client, sn, "f")


def test_index_plan_keeps_the_exact_answer_and_a_repeatable_float_sum(tiny_ddl_chunk_server):
    """`CREATE INDEX` re-plans these reads onto an index-bounded walk, which sorts
    and dedups PKs one chunk at a time. What that owes the caller is pinned here:
    the same rows, and the same aggregates that are exact over them, chunk
    boundaries and all — plus a float SUM that still repeats under the new plan.

    What it does not owe is the full scan's float bits. A float SUM follows the
    summation order and the access path is part of that order, so the indexed and
    unindexed float sums are deliberately never compared: they may legitimately
    agree, and requiring either answer would pin an access-path detail as a promise.

    The 3-row chunk is the point. `test_indices.py::TestIndexBoundPushdown` covers
    the same invariant for integer aggregates at the 65 536-row default, where any
    test range is one chunk and the per-chunk sort is the global one; here each
    worker's share spans several chunks, the only regime in which the walk visits
    rows in an order a PK scan never would."""
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
            assert _access(client, sn, q).startswith("access: full scan"), q
        before_rows = _norm(_rows(client, sn, rows_q))
        before_exact = _items(_rows(client, sn, exact_q))
        # 8 of `_FLOAT_CATS` categories at 4 rows each — coupled to that fixture.
        # 1.6% of the table, a 4x margin inside the worker's 1/16-of-the-slice
        # cost gate, and 8 rows per worker at 4 workers: three 3-row chunks each.
        assert len(before_rows) == 32, before_rows

        client.execute_sql("CREATE INDEX fi ON f (cat)", schema_name=sn)
        # The plan-time choice, which is sink-independent: one access planner
        # serves the rows sink and the fold sink alike, so all three re-plan.
        for q in (rows_q, exact_q, float_q):
            assert _access(client, sn, q).startswith("access: index range on (cat)"), q

        # The walk's own claim: a chunked drain and an unchunked one yield
        # identical multisets. The rows, then the aggregates exact over them —
        # COUNT and an integer SUM fold associatively, and MIN/MAX copy a cell's
        # bits rather than combining them.
        assert _norm(_rows(client, sn, rows_q)) == before_rows
        assert _items(_rows(client, sn, exact_q)) == before_exact

        # The float SUM under the index plan: repeatable, and that alone.
        assert _bits(_rows(client, sn, float_q)) == _bits(_rows(client, sn, float_q))
    finally:
        _cleanup(client, sn, "f")
