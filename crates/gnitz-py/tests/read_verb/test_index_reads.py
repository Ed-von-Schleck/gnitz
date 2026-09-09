"""Reads served through a secondary index: point seek, SQL equality and range
lookups, the bound pushed into the walk, the per-partition collect/merge, and
a reply train that spans more than one frame.

The failure mode is silent under-reporting — a key the walk never opens comes
back as "no such row" — so each case asserts the full result set with its
weights, and the bounded build is compared against the unbounded one. A key
answered by two workers is a doubled weight, which a PK list cannot see.
"""

import pytest
from _read import access, bag, rows
from _uid import uid as _uid


def _insert(client, sn, table, values, chunk=1000):
    """Multi-row INSERT of same-width value tuples, split into at-most-`chunk`-row
    statements; a literal 'NULL' passes through."""
    for i in range(0, len(values), chunk):
        vals = ", ".join(
            f"({', '.join(str(v) for v in r)})" for r in values[i:i + chunk])
        client.execute_sql(f"INSERT INTO {table} VALUES {vals}", schema_name=sn)


# ---------------------------------------------------------------------------
# Index seek (the binary verb)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("index_first", [False, True], ids=["backfill", "maintained"])
def test_index_seek_finds_every_key(client, schema_name, index_first):
    """Every indexed value resolves to its own row, whether the index was built
    over existing rows (backfill) or maintained as they arrived.

    The keys span every partition, so the projection maintaining the index has
    to have run on every worker rather than only the one a seed landed on.
    """
    sn = schema_name
    n = 32
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, cust_id BIGINT NOT NULL)",
        schema_name=sn)
    tid, _ = client.resolve_table(sn, "t")
    if index_first:
        client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)
    _insert(client, sn, "t", [(i, i * 100) for i in range(1, n + 1)])
    if not index_first:
        client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)

    for i in range(1, n + 1):
        got = client.seek_by_index(tid, [1], [i * 100])
        assert list(zip(got.pks, got.weights)) == [(i, 1)], f"cust_id={i * 100}"
    # An absent value is an empty answer, not an error.
    assert len(client.seek_by_index(tid, [1], [n * 100 + 1]).pks) == 0


# ---------------------------------------------------------------------------
# Equality through SQL
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("coltype,vals", [
    ("BIGINT", [-5, -1, 0, 10]),
    ("INT", [-100, -1, 0, 100]),
    ("SMALLINT", [-32768, -1, 0, 32767]),
    ("BIGINT UNSIGNED", [0, 1, 9223372036854775808, 18446744073709551615]),
])
def test_indexed_equality_over_the_width_and_sign_range(client, schema_name, coltype, vals):
    """`WHERE col = v` resolves through the index at every width and on both
    sides of zero. The sign-flip in the OPK encoding is what a signed column
    exercises, and a value past i64::MAX what an unsigned one does — a column
    read at the wrong signedness lands on the wrong side of the walk and misses.
    """
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v {coltype} NOT NULL)",
        schema_name=sn)
    _insert(client, sn, "t", [(i, v) for i, v in enumerate(vals, 1)])
    client.execute_sql("CREATE INDEX ON t(v)", schema_name=sn)

    for pk, v in enumerate(vals, 1):
        assert bag(rows(client, sn, f"SELECT pk, v FROM t WHERE v = {v}")) == {(pk, v): 1}
    # A value no row carries answers empty rather than the nearest neighbour.
    assert rows(client, sn, "SELECT pk FROM t WHERE v = 7777") == []


def test_pk_equality_needs_no_index(client, schema_name):
    """A PK point is served by the PK walk itself, with an unrelated index
    present — the two access paths must not be confused for one another."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=sn)
    _insert(client, sn, "t", [(1, 5), (2, 6), (3, 5)])
    client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
    assert bag(rows(client, sn, "SELECT pk, val FROM t WHERE pk = 2")) == {(2, 6): 1}
    # A non-indexed column has no seek key, so the bounded read degrades to a
    # full cursor and the predicate runs server-side — served, not rejected.
    assert bag(rows(client, sn, "SELECT pk FROM t WHERE val = 5")) == {(1,): 1, (3,): 1}


def test_indexed_equality_with_a_residual(client, schema_name):
    """An index seek combined with a residual AND predicate filters correctly."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, cust_id BIGINT NOT NULL, "
        "region BIGINT NOT NULL)", schema_name=sn)
    _insert(client, sn, "t", [(1, 42, 100), (2, 99, 200), (3, 42, 200)])
    client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)
    assert bag(rows(client, sn,
                    "SELECT pk FROM t WHERE cust_id = 42 AND region = 100")) == {(1,): 1}


# ---------------------------------------------------------------------------
# Ranges through SQL
# ---------------------------------------------------------------------------


@pytest.fixture
def ranged(client, schema_name):
    """`t (pk, x INT)` with `x = (pk - 8) * 5` over pks 1..15 — x from -35 to 35,
    so a range spans zero and the rows scatter over every partition. Read-only:
    every case below is a SELECT."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x INT NOT NULL)",
        schema_name=sn)
    _insert(client, sn, "t", [(pk, (pk - 8) * 5) for pk in range(1, 16)])
    client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)
    return sn


@pytest.mark.parametrize("predicate,keep", [
    ("x > 0", lambda x: x > 0),
    ("x >= 0", lambda x: x >= 0),
    ("x < 0", lambda x: x < 0),
    ("x <= 0", lambda x: x <= 0),
    ("0 < x", lambda x: x > 0),                      # flipped orientation
    ("x BETWEEN -10 AND 10", lambda x: -10 <= x <= 10),
    ("x NOT BETWEEN -10 AND 10", lambda x: not -10 <= x <= 10),
    ("x > -10 AND x < 10", lambda x: -10 < x < 10),
    ("x >= -35 AND x <= -35", lambda x: x == -35),   # single-value inclusive band
    # Both ends on the same side: the tighter one binds, whichever is written
    # first — order-independence is what proves no packed-native compare.
    ("x > -10 AND x > 10", lambda x: x > 10),
    ("x > 10 AND x > -10", lambda x: x > 10),
    # An out-of-type-range bound saturates rather than wrapping: provably empty
    # above I32::MAX, unbounded below it.
    ("x > 3000000000", lambda x: False),
    ("x < 3000000000", lambda x: True),
])
def test_index_range_walks_the_signed_interval(client, ranged, predicate, keep):
    """A range over a signed indexed column returns exactly the contiguous
    interval, boundary inclusivity and all. Signed values are the discriminating
    case: the OPK sign-flip is what makes `OPK(-5) < OPK(5)`, so a raw unsigned
    read of the key inverts the interval and answers an empty or complementary
    set."""
    want = {(pk,): 1 for pk in range(1, 16) if keep((pk - 8) * 5)}
    assert bag(rows(client, ranged, f"SELECT pk FROM t WHERE {predicate}")) == want


def test_range_over_a_composite_index(client, schema_name):
    """On index (a, b), `a = 5 AND b > 10` is served by the composite range scan,
    NOT a bare `a = 5` prefix seek + residual: a row at (5, 0) must be absent,
    and an (8, 11) row never enters the candidate set (the scan stops < a=8)."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "a BIGINT NOT NULL, b BIGINT NOT NULL)", schema_name=sn)
    _insert(client, sn, "t", [(1, 5, 0), (2, 5, 20), (3, 5, 11), (4, 8, 11)])
    client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=sn)
    assert bag(rows(client, sn, "SELECT * FROM t WHERE a = 5 AND b > 10")) == \
        {(2, 5, 20): 1, (3, 5, 11): 1}


def test_max_arity_composite_descriptor_crosses_the_inline_cap(client, schema_name):
    """A composite index at PK_LIST_MAX_COLS = 4 carrying a two-sided range on
    the last column (n_eq = 3) produces an 82-byte descriptor — over the 64-byte
    `seek_pk_extra` and 80-byte `PkBuf` caps — so the request must travel by the
    explicit-blob send path rather than inline, and still reach every worker.

    The EXPLAIN check is what makes this a descriptor test: without it a
    fallback to a full scan would answer the same rows and pass silently.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, d BIGINT NOT NULL)",
        schema_name=sn)
    values = [(pk, 1, 2, 3, d) for pk, d in enumerate(range(60), 1)]
    values += [(61, 1, 2, 4, 30), (62, 2, 2, 3, 30)]   # decoys on c and on a
    _insert(client, sn, "t", values)
    client.execute_sql("CREATE INDEX ON t(a, b, c, d)", schema_name=sn)

    q = "SELECT pk FROM t WHERE a = 1 AND b = 2 AND c = 3 AND d > 10 AND d < 50"
    assert access(client, sn, q).startswith("access: index range on (a, b, c, d)"), \
        access(client, sn, q)
    assert bag(rows(client, sn, q)) == \
        {(pk,): 1 for pk, _, _, _, d in values[:60] if 10 < d < 50}


def test_residual_conjuncts_bind_and_filter(client, schema_name):
    """Conjuncts the range cannot absorb are applied after the walk. `BETWEEN`
    and `NOT BETWEEN` in that position are the regression guard for the
    `Expr::Between` binder arm, which used to fail to bind rather than filter."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "x BIGINT NOT NULL, y BIGINT NOT NULL)", schema_name=sn)
    _insert(client, sn, "t", [(1, 10, 5), (2, 20, 50), (3, 30, 1)])
    client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)
    q = lambda s: bag(rows(client, sn, f"SELECT pk FROM t WHERE {s}"))
    assert q("x > 5 AND y = 5") == {(1,): 1}
    assert q("x > 5 AND y BETWEEN 1 AND 9") == {(1,): 1, (3,): 1}
    assert q("x > 5 AND y NOT BETWEEN 1 AND 9") == {(2,): 1}
    # No index on y at all: no seek key, so the whole predicate is the residual.
    assert q("y > 5") == {(2,): 1}


# ---------------------------------------------------------------------------
# Per-partition collect / merge
# ---------------------------------------------------------------------------


def test_nonunique_collect_matches_the_scan_reference(client, schema_name):
    """The merged index result equals a scan-and-filter reference over the same
    data, and reflects net weights after a retraction — mandatory for any index
    access path, since consolidation is what the merge owes its caller."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "x BIGINT NOT NULL, y BIGINT NOT NULL)", schema_name=sn)
    # 200 rows scattered across workers; 10 rows per x value.
    _insert(client, sn, "t", [(i, i % 20, i * 3) for i in range(200)])
    client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)

    tid, _ = client.resolve_table(sn, "t")
    ref = bag(client.scan(tid))
    assert len(ref) == 200
    q = lambda s: bag(rows(client, sn, f"SELECT * FROM t WHERE {s}"))
    assert q("x = 7") == {r: w for r, w in ref.items() if r[1] == 7}
    assert q("x > 15") == {r: w for r, w in ref.items() if r[1] > 15}

    client.execute_sql("DELETE FROM t WHERE pk IN (7, 27, 47)", schema_name=sn)
    assert q("x = 7") == {r: w for r, w in ref.items() if r[1] == 7 and r[0] not in (7, 27, 47)}


def test_anticorrelated_runs_merge_by_value_then_src_pk(client, schema_name):
    """A secondary index's PK is the pair ``(indexed_value, src_pk)``, and the
    read cursor's N-way merge must order runs the way storage sorted each one —
    by column order — not by a raw u128 view of the key bytes, which orders by
    ``(src_pk, value)`` because src_pk lands in the high half.

    Correlated data sorts identically under both orders and so proves nothing.
    Here value falls as pk rises, which is what makes the two orders disagree:
    under the wrong one the merge seeks the wrong run's head and the lookup
    misses. One INSERT per row, so each row is its own run.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
    n = 6
    want = [(pk, (n + 1 - pk) * 100) for pk in range(1, n + 1)]
    for pk, val in want:
        client.execute_sql(f"INSERT INTO t VALUES ({pk}, {val})", schema_name=sn)
    for pk, val in want:
        assert bag(rows(client, sn, f"SELECT * FROM t WHERE val = {val}")) == {(pk, val): 1}


# ---------------------------------------------------------------------------
# Reply trains that span more than one frame
# ---------------------------------------------------------------------------


def test_chunked_seek_and_range_replies_return_full_set(reply_frame_budget_server):
    """A per-worker reply that spans several 16 KiB frames (see the fixture)
    still returns every row, with payload intact across the chunk boundaries and
    net weights after a retraction."""
    client = reply_frame_budget_server
    sn = "cx" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "x BIGINT NOT NULL, y BIGINT NOT NULL)", schema_name=sn)
        # ~32 KB of matching rows per worker per value at 4 workers: every
        # worker's train is several frames past the 16 KiB budget.
        n = 4_000
        _insert(client, sn, "t", [(i, i % 2, i * 7) for i in range(n)])
        client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)

        # Full-row values, so payload integrity through the boundaries is
        # asserted rather than just the key set.
        assert bag(rows(client, sn, "SELECT * FROM t WHERE x = 0")) == \
            {(i, 0, i * 7): 1 for i in range(0, n, 2)}
        assert bag(rows(client, sn, "SELECT pk FROM t WHERE x >= 0")) == \
            {(i,): 1 for i in range(n)}

        client.execute_sql("DELETE FROM t WHERE pk IN (1, 3, 5)", schema_name=sn)
        assert bag(rows(client, sn, "SELECT pk FROM t WHERE x = 1")) == \
            {(i,): 1 for i in range(7, n, 2)}
    finally:
        client.drop_schema(sn)


def test_unique_index_over_a_long_text_table_past_one_frame(reply_frame_budget_server):
    """CREATE UNIQUE INDEX warms its cold filters from a whole-table scan, so a
    table with a long TEXT column reaches that scan's frame budget on rows the
    user never asked to read. Every frame carries a heap compacted to its own
    rows, so the DDL completes and the index it builds enforces uniqueness."""
    client = reply_frame_budget_server
    sn = "ux" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "u BIGINT NOT NULL, body TEXT NOT NULL)", schema_name=sn)
        # 200 bytes of heap per row: 4 000 rows is ~800 KB, far past the
        # 16 KiB budget on every worker.
        body = "z" * 200
        n = 4_000
        _insert(client, sn, "t", [(i, i, f"'row-{i}-{body}'") for i in range(n)])

        client.execute_sql("CREATE UNIQUE INDEX ON t(u)", schema_name=sn)
        with pytest.raises(Exception):
            client.execute_sql("INSERT INTO t VALUES (99999, 7, 'dup')", schema_name=sn)
        client.execute_sql(f"INSERT INTO t VALUES (99999, {n}, 'new')", schema_name=sn)
        assert bag(rows(client, sn, "SELECT pk, u FROM t WHERE u = 17")) == {(17, 17): 1}
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# The bound pushed into a backfill
# ---------------------------------------------------------------------------


@pytest.fixture
def bounded(client, schema_name):
    """`t (pk, g, v, ind)` over 2000 rows: `ind` takes 11 values, so `ind = 4` is
    ~9% of the table and `ind < 2` ~18% — both inside the selectivity gate that
    decides whether a bound is worth opening. Read-only across the cases that
    share it."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL,"
        " v BIGINT NOT NULL, ind BIGINT NOT NULL)", schema_name=sn)
    _insert(client, sn, "t", [(i, i % 7, i * 3, i % 11) for i in range(2000)])
    return sn


@pytest.mark.parametrize("q", [
    "SELECT g, SUM(v) AS s FROM t WHERE ind = 4 GROUP BY g",
    "SELECT g, COUNT(*) AS c FROM t WHERE ind < 2 GROUP BY g",
])
def test_a_bounded_walk_returns_what_the_unbounded_one_returns(client, bounded, q):
    """An index changes which walk the planner picks, never the answer. The
    expectation is the same query run before the index existed, so it is the
    engine's own unbounded answer rather than a hand-written constant."""
    sn = bounded
    unbounded = bag(rows(client, sn, q))
    assert unbounded, "the fixture must match some rows"
    client.execute_sql("CREATE INDEX ON t(ind)", schema_name=sn)
    assert bag(rows(client, sn, q)) == unbounded


def test_a_bounded_view_backfill_matches_and_then_maintains(client, bounded):
    """The bound is consulted only at backfill, so a view built over an indexed
    WHERE materialises what the unindexed build did and then maintains
    identically: a push failing the predicate is dropped by the Filter, one
    passing it lands."""
    sn = bounded
    body = "SELECT g, SUM(v) AS s FROM t WHERE ind = 4 GROUP BY g"
    client.execute_sql(f"CREATE VIEW v_plain AS {body}", schema_name=sn)
    plain = bag(rows(client, sn, "SELECT * FROM v_plain"))

    client.execute_sql("CREATE INDEX ON t(ind)", schema_name=sn)
    client.execute_sql(f"CREATE VIEW v_bound AS {body}", schema_name=sn)
    assert bag(rows(client, sn, "SELECT * FROM v_bound")) == plain

    _insert(client, sn, "t", [(2000, 0, 100, 5), (2001, 0, 1000, 4)])
    after = {g: s for (g, s), _ in bag(rows(client, sn, "SELECT * FROM v_bound")).items()}
    before = {g: s for (g, s), _ in plain.items()}
    assert after[0] == before[0] + 1000, \
        "the matching push must maintain the bounded view; the other must not"
    assert {g: s for g, s in after.items() if g != 0} == \
        {g: s for g, s in before.items() if g != 0}


def test_a_dropped_index_falls_back_to_a_full_scan(client, bounded):
    """A plan that named an index which is later dropped still answers — the
    engine degrades to a full scan rather than losing rows."""
    sn = bounded
    q = "SELECT g, COUNT(*) AS c FROM t WHERE ind = 3 GROUP BY g"
    client.execute_sql("CREATE INDEX ON t(ind)", schema_name=sn)
    bounded_answer = bag(rows(client, sn, q))
    client.execute_sql(f"DROP INDEX {sn}__t__idx_ind", schema_name=sn)
    assert bag(rows(client, sn, q)) == bounded_answer


def test_a_null_indexed_value_is_returned_by_neither_build(client, schema_name):
    """A NULL-valued row is absent from the index, so a bounded scan can never
    return it — and under 3VL the Filter drops it too. Both builds must agree."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL,"
        " ind BIGINT)", schema_name=sn)
    _insert(client, sn, "t", [(i, i % 3, i % 5) for i in range(300)]
            + [(300 + i, i % 3, "NULL") for i in range(30)])
    q = "SELECT g, COUNT(*) AS c FROM t WHERE ind >= 0 GROUP BY g"
    unbounded = bag(rows(client, sn, q))

    client.execute_sql("CREATE INDEX ON t(ind)", schema_name=sn)
    assert bag(rows(client, sn, q)) == unbounded
    assert sum(c for _, c in unbounded) == 300, "the 30 NULL rows are in neither"


def test_a_replicated_global_aggregate_grounds_on_an_empty_range(client, schema_name):
    """A replicated base's global COUNT(*) is the shape that reaches
    `backfill_view`, and an unmatched WHERE makes its range provably empty. The
    ground row must still be minted: COUNT(*) = 0, at weight 1, one row."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, ind BIGINT NOT NULL)"
        " WITH (replicated = true)", schema_name=sn)
    _insert(client, sn, "t", [(i, i % 5) for i in range(100)])
    client.execute_sql("CREATE INDEX ON t(ind)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW mv AS SELECT COUNT(*) AS c FROM t WHERE ind = 999", schema_name=sn)
    assert bag(rows(client, sn, "SELECT * FROM mv")) == {(0,): 1}
