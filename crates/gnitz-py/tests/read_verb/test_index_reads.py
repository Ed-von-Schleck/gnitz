"""Reads served through a secondary index: point seek, SQL equality and range
lookups, the bound pushed into the walk, and the per-partition collect/merge.

The failure mode is silent under-reporting — a key the walk never opens comes
back as "no such row" — so each case asserts the full result set with its
weights. A key answered by two workers is a doubled weight, which a PK list
cannot see.

A SQL index walk whose conjuncts the server predicate also carries is optional:
a worker trades it for a full scan when the range covers more than a small
fraction of its rows, and that scan returns the same answer — so a small table
passes every assertion here without ever opening the index. Each table that is
meant to be walked is padded with `PAD` rows no case's predicate selects: a NULL
indexed cell, which the index does not hold, or an out-of-range value.
"""

import pytest
from _read import access, bag, rows, scanned
from _sql import insert

PAD = 1000


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
    insert(client, sn, "t", [(i, i * 100) for i in range(1, n + 1)])
    if not index_first:
        client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)

    for i in range(1, n + 1):
        got = client.seek_by_index(tid, [1], [i * 100])
        assert bag(got, "pk") == {(i,): 1}, f"cust_id={i * 100}"
    # An absent value is an empty answer, not an error.
    assert len(client.seek_by_index(tid, [1], [n * 100 + 1])) == 0


def test_index_seek_takes_a_negative_key(client, schema_name):
    """A key value is the column's own integer, sign included: a negative
    `BIGINT` key packs to the two's-complement word the column stores."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, delta BIGINT NOT NULL); "
        "CREATE INDEX ON t(delta)", schema_name=sn)
    tid, _ = client.resolve_table(sn, "t")
    insert(client, sn, "t", [(1, -5), (2, 5), (3, -7)])
    assert bag(client.seek_by_index(tid, [1], [-5]), "pk", "delta") == {(1, -5): 1}
    assert bag(client.seek_by_index(tid, [1], [5]), "pk", "delta") == {(2, 5): 1}


# ---------------------------------------------------------------------------
# Equality and ranges through SQL
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
    client.execute_sql(f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v {coltype})",
                       schema_name=sn)
    insert(client, sn, "t", [(i, v) for i, v in enumerate(vals, 1)]
           + [(-i, None) for i in range(1, PAD + 1)])
    client.execute_sql("CREATE INDEX ON t(v)", schema_name=sn)

    for pk, v in enumerate(vals, 1):
        assert bag(rows(client, sn, f"SELECT pk, v FROM t WHERE v = {v}")) == {(pk, v): 1}
    # A value no row carries answers empty rather than the nearest neighbour.
    assert rows(client, sn, "SELECT pk FROM t WHERE v = 7777") == []


@pytest.fixture(scope="module")
def ranged(module_schema):
    """`ranged (pk, x INT)` with `x = (pk - 8) * 5` over pks 1..15 — x from -35
    to 35, so a range spans zero — plus NULL padding. Read-only."""
    conn, sn = module_schema
    conn.execute_sql("CREATE TABLE ranged (pk BIGINT NOT NULL PRIMARY KEY, x INT)",
                     schema_name=sn)
    insert(conn, sn, "ranged", [(pk, (pk - 8) * 5) for pk in range(1, 16)]
           + [(-i, None) for i in range(1, PAD + 1)])
    conn.execute_sql("CREATE INDEX ON ranged(x)", schema_name=sn)
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
    assert bag(rows(client, ranged, f"SELECT pk FROM ranged WHERE {predicate}")) == want


def test_range_over_a_composite_index(client, schema_name):
    """On index (a, b), `a = 5 AND b > 10` is served by the composite range scan,
    NOT a bare `a = 5` prefix seek + residual: a row at (5, 0) must be absent,
    and an (8, 11) row never enters the candidate set (the scan stops < a=8)."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "a BIGINT NOT NULL, b BIGINT NOT NULL)", schema_name=sn)
    insert(client, sn, "t", [(1, 5, 0), (2, 5, 20), (3, 5, 11), (4, 8, 11)]
           + [(-i, 100, i) for i in range(1, PAD + 1)])
    client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=sn)
    assert bag(rows(client, sn, "SELECT * FROM t WHERE a = 5 AND b > 10")) == \
        {(2, 5, 20): 1, (3, 5, 11): 1}


def test_max_arity_composite_descriptor_crosses_the_inline_cap(client, schema_name):
    """A composite index at the maximum index arity carrying a two-sided range on
    the last column produces a descriptor too wide to travel inline, so the
    request takes the explicit-blob send path and must still reach every worker.

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
    insert(client, sn, "t", values + [(-i, 9, 2, 3, 30) for i in range(1, PAD + 1)])
    client.execute_sql("CREATE INDEX ON t(a, b, c, d)", schema_name=sn)

    q = "SELECT pk FROM t WHERE a = 1 AND b = 2 AND c = 3 AND d > 10 AND d < 50"
    assert access(client, sn, q).startswith("access: index range on (a, b, c, d)")
    assert bag(rows(client, sn, q)) == \
        {(pk,): 1 for pk, _, _, _, d in values[:60] if 10 < d < 50}


def test_residual_conjuncts_bind_and_filter(client, schema_name):
    """Conjuncts the walk cannot absorb — on another column, or `BETWEEN` / `NOT
    BETWEEN` — are applied after it, and a predicate on an unindexed column is
    the whole residual."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT, y BIGINT NOT NULL)",
        schema_name=sn)
    insert(client, sn, "t", [(1, 10, 5), (2, 20, 50), (3, 30, 1), (4, 10, 7)]
           + [(-i, None, 0) for i in range(1, PAD + 1)])
    client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)

    def q(s):
        return bag(rows(client, sn, f"SELECT pk FROM t WHERE {s}"))

    assert q("x = 10 AND y = 5") == {(1,): 1}
    assert q("x > 5 AND y BETWEEN 1 AND 9") == {(1,): 1, (3,): 1, (4,): 1}
    assert q("x > 5 AND y NOT BETWEEN 1 AND 9") == {(2,): 1}
    assert q("y > 5") == {(2,): 1, (4,): 1}


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
    # 2000 rows scattered across workers; 10 rows per x value.
    insert(client, sn, "t", [(i, i % 200, i * 3) for i in range(2000)])
    client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)

    ref = bag(scanned(client, sn, "t"))
    assert len(ref) == 2000

    def q(s):
        return bag(rows(client, sn, f"SELECT * FROM t WHERE {s}"))

    assert q("x = 7") == {r: w for r, w in ref.items() if r[1] == 7}
    assert q("x >= 195") == {r: w for r, w in ref.items() if r[1] >= 195}

    client.execute_sql("DELETE FROM t WHERE pk IN (7, 207, 407)", schema_name=sn)
    assert q("x = 7") == {r: w for r, w in ref.items() if r[1] == 7 and r[0] not in (7, 207, 407)}


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
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "CREATE INDEX ON t(val)", schema_name=sn)
    insert(client, sn, "t", [(-i, -i) for i in range(1, PAD + 1)])
    n = 6
    want = [(pk, (n + 1 - pk) * 100) for pk in range(1, n + 1)]
    for pk, val in want:
        client.execute_sql(f"INSERT INTO t VALUES ({pk}, {val})", schema_name=sn)
    for pk, val in want:
        assert bag(rows(client, sn, f"SELECT * FROM t WHERE val = {val}")) == {(pk, val): 1}


# ---------------------------------------------------------------------------
# The bound pushed into a read and a backfill
# ---------------------------------------------------------------------------


# `(pk, g, v, ind)`: `ind` spans 50 values and is NULL on every twentieth row,
# so each case's range is a few percent of the table.
_BOUNDED = [(i, i % 7, i * 3, None if i % 20 == 19 else i % 50) for i in range(2000)]


@pytest.fixture
def bounded(client, schema_name):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL,"
        " v BIGINT NOT NULL, ind BIGINT)", schema_name=schema_name)
    insert(client, schema_name, "t", _BOUNDED)
    return schema_name


@pytest.mark.parametrize("where,keep", [
    ("ind = 4", lambda ind: ind == 4),
    ("ind < 2", lambda ind: ind < 2),
    # 49 is a value NULL cells sit among: a NULL cell is absent from the index
    # and dropped by the Filter under 3VL, so neither walk returns it.
    ("ind >= 48", lambda ind: ind >= 48),
])
def test_an_index_changes_the_walk_never_the_answer(client, bounded, where, keep):
    """The same grouped read before an index exists, through it, and after it is
    dropped: the plan's access line moves, the rows do not."""
    sn = bounded
    q = f"SELECT g, COUNT(*) AS c, SUM(v) AS s FROM t WHERE {where} GROUP BY g"
    groups = {}
    for _, g, v, ind in _BOUNDED:
        if ind is not None and keep(ind):
            c, s = groups.get(g, (0, 0))
            groups[g] = (c + 1, s + v)
    want = {(g, c, s): 1 for g, (c, s) in groups.items()}

    assert access(client, sn, q).startswith("access: full scan")
    assert bag(rows(client, sn, q)) == want
    client.execute_sql("CREATE INDEX ON t(ind)", schema_name=sn)
    assert access(client, sn, q).startswith("access: index range on (ind)")
    assert bag(rows(client, sn, q)) == want
    client.execute_sql(f"DROP INDEX {sn}__t__idx_ind", schema_name=sn)
    assert access(client, sn, q).startswith("access: full scan")
    assert bag(rows(client, sn, q)) == want


def test_a_bounded_view_backfill_matches_and_then_maintains(client, bounded):
    """The bound is consulted only at backfill, so a view built over an indexed
    WHERE materialises what the unindexed build did and then maintains
    identically: a push failing the predicate is dropped by the Filter, one
    passing it lands."""
    sn = bounded
    body = "SELECT g, SUM(v) AS s FROM t WHERE ind = 4 GROUP BY g"
    client.execute_sql(f"CREATE VIEW v_plain AS {body}", schema_name=sn)
    plain = bag(rows(client, sn, "SELECT * FROM v_plain"))

    client.execute_sql(f"CREATE INDEX ON t(ind); CREATE VIEW v_bound AS {body}", schema_name=sn)
    assert bag(rows(client, sn, "SELECT * FROM v_bound")) == plain

    insert(client, sn, "t", [(2000, 0, 100, 5), (2001, 0, 1000, 4)])
    (s0,) = [s for g, s in plain if g == 0]
    assert bag(rows(client, sn, "SELECT * FROM v_bound")) == \
        {**{k: 1 for k in plain if k[0] != 0}, (0, s0 + 1000): 1}


def test_a_replicated_global_aggregate_grounds_on_an_empty_range(client, schema_name):
    """A replicated base's global COUNT(*) view backfills over a provably-empty
    index range. The ground row must still be minted: COUNT(*) = 0, at weight 1,
    one row."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, ind BIGINT NOT NULL)"
        " WITH (replicated = true)", schema_name=sn)
    insert(client, sn, "t", [(i, i % 5) for i in range(100)])
    client.execute_sql(
        "CREATE INDEX ON t(ind); "
        "CREATE VIEW mv AS SELECT COUNT(*) AS c FROM t WHERE ind = 999", schema_name=sn)
    assert bag(rows(client, sn, "SELECT * FROM mv")) == {(0,): 1}


# ---------------------------------------------------------------------------
# Which index a composite WHERE binds, and what stays a residual
# ---------------------------------------------------------------------------


@pytest.fixture
def abc(client, schema_name):
    """`t (pk, a, b, c)` with three rows, `b` NOT NULL."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        "b BIGINT NOT NULL, c BIGINT NOT NULL)", schema_name=sn)
    insert(client, sn, "t", [(10, 1, 100, 7), (20, 1, 200, 8), (30, 2, 100, 9)]
           + [(-i, 100, i, i) for i in range(1, PAD + 1)])
    return sn


@pytest.mark.parametrize("indexes,drop,where,walk,want", [
    # The full key.
    (["(a, b)"], None, "a = 1 AND b = 200", "(a, b)", [20]),
    # Out-of-order WHERE binds each value to the index's declared column, not to
    # AST order.
    (["(a, b)"], None, "b = 200 AND a = 1", "(a, b)", [20]),
    # A leading prefix over (a, b) with b NOT NULL is served by the index.
    (["(a, b)"], None, "a = 1", "(a, b)", [10, 20]),
    # Both indexes pin (a, b) to the same point, so the tie falls to arity: the
    # narrower walk wins.
    (["(a, b)", "(a, b, c)"], None, "a = 1 AND b = 200", "(a, b)", [20]),
    # `a = 1` is consumed by the walk and `a > 5` stays a residual — excluded by
    # the consumed conjunct's physical index, not by column, so it survives and
    # (since a = 1) matches nothing. The companion proves that is the residual
    # filtering rather than the walk returning nothing.
    (["(a)"], None, "a = 1 AND a > 5", "(a)", []),
    (["(a)"], None, "a = 1 AND a > 0", "(a)", [10, 20]),
    # DROP INDEX matches the exact column list, so the single-column index is
    # left serving.
    (["(a)", "(a, b)"], "idx_a_b", "a = 1", "(a)", [10, 20]),
], ids=["full-key", "out-of-order", "leading-prefix", "tiebreak-arity",
        "residual-empty", "residual-kept", "drop-exact-list"])
def test_which_composite_index_a_where_binds(client, abc, indexes, drop, where, walk, want):
    """The walk the planner picks and the rows it returns, asserted together: a
    row bag alone passes with every index dropped, and an access line alone
    passes on a walk that loses rows."""
    for cols in indexes:
        client.execute_sql(f"CREATE INDEX ON t{cols}", schema_name=abc)
    if drop:
        client.execute_sql(f"DROP INDEX {abc}__t__{drop}", schema_name=abc)
    q = f"SELECT pk FROM t WHERE {where}"
    assert access(client, abc, q).startswith(f"access: index range on {walk}")
    assert bag(rows(client, abc, q)) == {(pk,): 1 for pk in want}


def test_a_nullable_trailing_prefix_is_not_served_by_the_index(client, schema_name):
    """A leading-prefix WHERE over (a, b) with `b` NULLABLE would silently drop
    the (1, NULL) row, so the index must not serve it — the read scans the base
    instead and DOES return that row. The full key still uses the index and
    finds the non-null one."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        "b BIGINT, c BIGINT NOT NULL)", schema_name=sn)
    insert(client, sn, "t", [(10, 1, 100, 7), (20, 1, 200, 8), (30, 2, 100, 9), (40, 1, None, 11)]
           + [(-i, 100, i, i) for i in range(1, PAD + 1)])
    client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=sn)

    q = "SELECT pk FROM t WHERE a = 1"
    assert access(client, sn, q).startswith("access: full scan")
    assert bag(rows(client, sn, q)) == {(10,): 1, (20,): 1, (40,): 1}
    q = "SELECT pk FROM t WHERE a = 1 AND b = 200"
    assert access(client, sn, q).startswith("access: index range on (a, b)")
    assert bag(rows(client, sn, q)) == {(20,): 1}


# ---------------------------------------------------------------------------
# An index over a PK column
# ---------------------------------------------------------------------------


_U64_MAX = 18446744073709551615


@pytest.fixture
def pk_indexed(client, schema_name):
    """A compound-PK table whose WHERE leaves the LEADING PK column free, so
    neither case below has a PK-range plan to fall back on."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
        "v BIGINT NOT NULL, PRIMARY KEY (a, b))", schema_name=sn)
    insert(client, sn, "t", [(1000 + i, 0, 0) for i in range(PAD)])
    return sn


def test_an_index_over_a_trailing_pk_column_serves_the_where(client, pk_indexed):
    """A secondary index may name a PK column, and the planner treats it as any
    other index column."""
    sn = pk_indexed
    insert(client, sn, "t", [(1, 10, 100), (2, 10, 200), (3, 20, 300)])
    client.execute_sql("CREATE INDEX ON t(b, v)", schema_name=sn)

    q = "SELECT * FROM t WHERE b = 10 AND v = 200"
    assert access(client, sn, q).startswith("access: index range on (b, v)")
    assert bag(rows(client, sn, q)) == {(2, 10, 200): 1}
    # A range on the same column bounds the same index.
    assert bag(rows(client, sn, "SELECT * FROM t WHERE b > 10")) == {(3, 20, 300): 1}


def test_a_wide_literal_on_an_indexed_pk_column_is_servable(client, pk_indexed):
    """The literal overflows i64, so the predicate VM has no form for the
    conjunct: only an index walk that consumes it byte-exactly can serve this
    WHERE, and the PK rung cannot bound it (nothing pins `a`)."""
    sn = pk_indexed
    insert(client, sn, "t", [(1, _U64_MAX, 100), (2, 5, 200)])
    client.execute_sql("CREATE INDEX ON t(b)", schema_name=sn)

    q = f"SELECT * FROM t WHERE b = {_U64_MAX}"
    assert access(client, sn, q).startswith("access: index range on (b)")
    assert bag(rows(client, sn, q)) == {(1, _U64_MAX, 100): 1}
