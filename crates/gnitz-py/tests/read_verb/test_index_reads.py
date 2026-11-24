"""Reads served through a secondary index: point seek, SQL equality and range
lookups, the bound pushed into the walk, and the per-partition collect/merge.

The failure mode is silent under-reporting — a key the walk never opens comes
back as "no such row" — so each case asserts the full result set with its
weights. A key answered by two workers is a doubled weight, which a PK list
cannot see.

A worker trades an index walk for a full scan narrowed to the walk's rows when
the range covers more than a small fraction of its rows, so a small table passes
every assertion here without ever opening the index. Each table that is meant to
be walked is padded with `PAD` rows no case's predicate selects: a NULL indexed
cell, which the index does not hold, or an out-of-range value.
"""

import uuid
from datetime import date, datetime
from decimal import Decimal

import pytest
from gnitz import ColumnDef, Schema, TypeCode, ZSetBatch
from _read import access, bag, rows
from _shapes import grouped
from _sql import insert

PAD = 1000
_U64_MAX = 2**64 - 1


# ---------------------------------------------------------------------------
# Index seek (the binary verb)
# ---------------------------------------------------------------------------


def test_index_seek_refuses_keys_its_columns_cannot_take(client):
    """Key values the named columns cannot take are refused before any request:
    none, more than the columns, or one for a column no key can be."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, delta BIGINT NOT NULL, s TEXT NOT NULL)")
    tid, schema = client.resolve_table("t")
    for cols, keys, msg in [([1], [], "key value count"),
                            ([1], [5, 5], "key value count"),
                            ([2], ["x"], "cannot be a key column")]:
        with pytest.raises(ValueError, match=msg):
            client.seek_by_index(tid, schema, cols, keys)


_DAY = date(2024, 2, 29)

# (column type, stored literal, keys that find it, (key, error) it refuses)
_TYPED_KEYS = [
    ("TINYINT UNSIGNED", "7", [7], [(300, OverflowError)]),
    ("BIGINT UNSIGNED", "7", [7], [(-1, OverflowError)]),
    ("DATE", "DATE '2024-02-29'", [_DAY, datetime(2024, 2, 29)],
     [(datetime(2024, 2, 29, 13), ValueError)]),
    ("TIMESTAMP", "TIMESTAMP '2024-02-29 00:00:00'", [datetime(2024, 2, 29), _DAY], []),
    ("DECIMAL(10, 2)", "5.00", [5, Decimal("5.00"), "5.00", 5.0, "5.000"],
     [(Decimal("5.004"), ValueError), (5.0049999, ValueError)]),
    # Negative: the key is the column's own integer, sign included.
    ("BIGINT", "-7", [-7], [(str(uuid.UUID(int=7)), TypeError)]),
]


@pytest.mark.parametrize("col_type,stored,finds,refused", _TYPED_KEYS,
                         ids=[t[0].split("(")[0].lower().replace(" ", "-") for t in _TYPED_KEYS])
def test_index_seek_reads_a_key_as_its_columns_type(client, col_type, stored, finds, refused):
    """A key the column holds exactly finds its row; one it cannot hold raises
    rather than reaching another row."""
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v {col_type} NOT NULL); "
        f"CREATE INDEX ON t(v); INSERT INTO t VALUES (1, {stored})")
    tid, schema = client.resolve_table("t")
    for key in finds:
        assert bag(client.seek_by_index(tid, schema, [1], [key]), "pk") == {(1,): 1}, repr(key)
    for key, error in refused:
        with pytest.raises(error):
            client.seek_by_index(tid, schema, [1], [key])


def test_a_decimal_key_past_the_columns_range_reaches_no_row(client):
    """A key that rounds onto the column's largest value names no row of it: it
    raises instead of reading, or deleting, the row keyed at that edge."""
    schema = Schema([ColumnDef("k", TypeCode.DECIMAL, scale=2)], [0])
    tid = client.create_table("t", schema)
    edge = Decimal("92233720368547758.07")
    client.push(tid, ZSetBatch(schema).append(k=edge))
    assert bag(client.seek(tid, schema, edge), "k") == {(edge,): 1}
    for verb in (client.seek, lambda t, s, k: client.delete(t, s, [k])):
        with pytest.raises(OverflowError):
            verb(tid, schema, Decimal("92233720368547758.074"))
    assert bag(client.scan(tid, schema), "k") == {(edge,): 1}


# ---------------------------------------------------------------------------
# Equality and ranges through SQL
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("coltype,vals,mid", [
    ("BIGINT", [-2**63, -35, -1, 0, 1, 35, 2**63 - 1], 0),
    ("INT", [-2**31, -35, -1, 0, 1, 35, 2**31 - 1], 0),
    ("SMALLINT", [-2**15, -35, -1, 0, 1, 35, 2**15 - 1], 0),
    ("TINYINT", [-2**7, -35, -1, 0, 1, 35, 2**7 - 1], 0),
    ("HUGEINT", [-2**127, -35, -1, 0, 1, 35, 2**127 - 1], 0),
    ("BIGINT UNSIGNED", [0, 1, 35, 2**63, _U64_MAX], 2**63),
    ("UINT128", [0, 1, 35, 2**64, 2**127, 2**128 - 1], 2**127),
])
def test_an_index_walks_every_width_and_sign(client, coltype, vals, mid):
    """An index keys a column at the column's own width, so a walk over it
    follows that width's order from the type's minimum to its maximum, across
    the point its key encoding flips: zero for a signed column, 2^63 for an
    unsigned one. A column read at the wrong width or signedness lands on the
    wrong side of the walk and misses; a raw unsigned read of a signed key
    inverts an interval spanning zero."""
    client.execute_sql(f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x {coltype})")
    insert(client, "t", list(enumerate(vals, 1)) + [(-i, None) for i in range(1, PAD + 1)])
    client.execute_sql("CREATE INDEX ON t(x)")
    for predicate, keep in [
        *((f"x = {v}", lambda x, v=v: x == v) for v in vals),
        # A value no row carries answers empty rather than the nearest neighbour.
        ("x = 2", lambda x: False),
        (f"x < {mid}", lambda x: x < mid),
        (f"x >= {mid}", lambda x: x >= mid),
        ("x >= 1 AND x <= 35", lambda x: 1 <= x <= 35),
        ("x > 1 AND x < 35", lambda x: 1 < x < 35),
        (f"{mid} < x", lambda x: x > mid),
        # Both ends on the same side: the tighter one binds, whichever is written
        # first.
        ("x > 1 AND x > 35", lambda x: x > 35),
        ("x > 35 AND x > 1", lambda x: x > 35),
    ]:
        q = f"SELECT pk, x FROM t WHERE {predicate}"
        assert access(client, q) == "index range on (x)", predicate
        assert bag(rows(client, q)) == {(pk, x): 1 for pk, x in enumerate(vals, 1) if keep(x)}, predicate


@pytest.mark.parametrize("predicate,walk,keep", [
    # Provably empty above I32::MAX.
    ("x > 3000000000", "index range on (x)", lambda x: False),
    # The whole domain: an index walk would bound nothing, so the scan serves
    # it, and its NULL rows still fail the compare.
    ("x < 3000000000", "full scan", lambda x: True),
])
def test_an_out_of_type_range_bound_saturates(client, predicate, walk, keep):
    """A bound past the column's type saturates rather than wrapping."""
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x INT)")
    insert(client, "t", [(pk, (pk - 8) * 5) for pk in range(1, 16)]
           + [(-i, None) for i in range(1, PAD + 1)])
    client.execute_sql("CREATE INDEX ON t(x)")
    q = f"SELECT pk FROM t WHERE {predicate}"
    assert access(client, q) == walk
    assert bag(rows(client, q)) == {(pk,): 1 for pk in range(1, 16) if keep((pk - 8) * 5)}


# ---------------------------------------------------------------------------
# Per-partition collect / merge
# ---------------------------------------------------------------------------


def test_anticorrelated_runs_merge_by_value_then_src_pk(client):
    """A secondary index's PK is the pair `(indexed_value, src_pk)`, and the
    read cursor's N-way merge must order runs the way storage sorted each one.
    Value falls as pk rises, so a merge ordering by `(src_pk, value)` instead
    seeks the wrong run's head and misses. One INSERT per row, so each row is
    its own run."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "CREATE INDEX ON t(val)")
    insert(client, "t", [(-i, -i) for i in range(1, PAD + 1)])
    n = 6
    want = [(pk, (n + 1 - pk) * 100) for pk in range(1, n + 1)]
    for pk, val in want:
        client.execute_sql(f"INSERT INTO t VALUES ({pk}, {val})")
    for pk, val in want:
        assert bag(rows(client, f"SELECT * FROM t WHERE val = {val}")) == {(pk, val): 1}


# ---------------------------------------------------------------------------
# The bound pushed into a read and a backfill
# ---------------------------------------------------------------------------


# `(pk, g, v, ind)`: `ind` spans 50 values and is NULL on every twentieth row,
# so each case's range is a few percent of the table.
_BOUNDED = [(i, i % 7, i * 3, None if i % 20 == 19 else i % 50) for i in range(2000)]


@pytest.fixture
def bounded(client):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL,"
        " v BIGINT NOT NULL, ind BIGINT)")
    insert(client, "t", _BOUNDED)


@pytest.mark.parametrize("owner", ["t", "w"], ids=["table", "view"])
def test_an_index_changes_the_walk_never_the_answer(client, bounded, owner):
    """The same grouped reads before an index exists, through it, after a DELETE
    retracts rows it holds, and after it is dropped: the plan's access line
    moves, the rows do not — the merged walk owes its caller net weights. The
    view projects the table's PK away, so its key is a hidden column the index
    entry names."""
    if owner == "w":
        client.execute_sql("CREATE VIEW w AS SELECT g, v, ind FROM t")
    wheres = {
        "ind = 4": lambda ind: ind == 4,
        "ind < 2": lambda ind: ind < 2,
        # 49 is a value NULL cells sit among: a NULL cell is absent from the index
        # and dropped by the Filter under 3VL, so neither walk returns it.
        "ind >= 48": lambda ind: ind >= 48,
    }
    live = list(_BOUNDED)

    def check(walk):
        for where, keep in wheres.items():
            q = f"SELECT g, COUNT(*) AS c, SUM(v) AS s FROM {owner} WHERE {where} GROUP BY g"
            assert access(client, q) == walk, q
            assert bag(rows(client, q)) == grouped(
                [r for r in live if r[3] is not None and keep(r[3])],
                lambda r: r[1], lambda r: 1, lambda r: r[2]), q

    check("full scan")
    client.execute_sql(f"CREATE INDEX by_ind ON {owner}(ind)")
    check("index range on (ind)")
    # Rows each range holds: `ind` is `pk % 50`.
    gone = (4, 54, 0, 1, 48, 98)
    client.execute_sql(f"DELETE FROM t WHERE pk IN {gone}")
    live = [r for r in live if r[0] not in gone]
    check("index range on (ind)")
    client.execute_sql("DROP INDEX by_ind")
    check("full scan")


@pytest.mark.parametrize("index_first", [False, True], ids=["backfill", "maintained"])
def test_a_view_index_follows_its_aggregate(client, index_first):
    """An index on an aggregate view's output column walks the view's current
    rows: an UPDATE that moves one group's sum retracts the old entry and adds
    the new one, whether the index was built over the backfill or before it."""
    n = 2000
    view = "CREATE VIEW agg AS SELECT k, SUM(x) AS s FROM t GROUP BY k; CREATE INDEX ON agg(s)"
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT NOT NULL)")
    if index_first:
        client.execute_sql(view)
    # Two rows per group, summing to the distinct 3k + 1.
    insert(client, "t", [(2 * k, k, k) for k in range(n)] + [(2 * k + 1, k, 2 * k + 1) for k in range(n)])
    if not index_first:
        client.execute_sql(view)

    k = 700
    old, new = 3 * k + 1, 10 * n
    vid, schema = client.resolve_table("agg")
    assert access(client, f"SELECT k FROM agg WHERE s = {old}") == "index range on (s)"

    def finds(s):
        got = bag(rows(client, f"SELECT k FROM agg WHERE s = {s}"))
        assert bag(client.seek_by_index(vid, schema, [1], [s]), "k") == got, s
        return got

    assert finds(old) == {(k,): 1}
    client.execute_sql(f"UPDATE t SET x = {new - k} WHERE pk = {2 * k + 1}")
    assert finds(old) == {}
    assert finds(new) == {(k,): 1}


def test_a_bounded_view_backfill_matches_and_then_maintains(client, bounded):
    """The bound is consulted only at backfill, so a view built over an indexed
    WHERE materialises what the unindexed build did and then maintains
    identically: a push failing the predicate is dropped by the Filter, one
    passing it lands."""
    body = "SELECT g, SUM(v) AS s FROM t WHERE ind = 4 GROUP BY g"
    client.execute_sql(f"CREATE VIEW v_plain AS {body}")
    client.execute_sql(f"CREATE INDEX ON t(ind); CREATE VIEW v_bound AS {body}")

    def check(src):
        want = grouped([r for r in src if r[3] == 4], lambda r: r[1], lambda r: r[2])
        for v in ("v_plain", "v_bound"):
            assert bag(rows(client, f"SELECT * FROM {v}")) == want, v

    check(_BOUNDED)
    added = [(2000, 0, 100, 5), (2001, 0, 1000, 4)]
    insert(client, "t", added)
    check(_BOUNDED + added)


def test_a_replicated_global_aggregate_grounds_on_an_empty_range(client):
    """A replicated base's global COUNT(*) view backfills over a provably-empty
    index range. The ground row must still be minted: COUNT(*) = 0, at weight 1,
    one row."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, ind BIGINT NOT NULL)"
        " WITH (replicated = true)")
    insert(client, "t", [(i, i % 5) for i in range(100)])
    client.execute_sql(
        "CREATE INDEX ON t(ind); "
        "CREATE VIEW mv AS SELECT COUNT(*) AS c FROM t WHERE ind = 999")
    assert bag(rows(client, "SELECT * FROM mv")) == {(0,): 1}


# ---------------------------------------------------------------------------
# Which index a composite WHERE binds, and what stays a residual
# ---------------------------------------------------------------------------


@pytest.fixture
def abc(client):
    """`t (pk, a, b, c, d)` with four rows, `b` NOT NULL."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        "b BIGINT NOT NULL, c BIGINT NOT NULL, d BIGINT NOT NULL)")
    insert(client, "t", [(10, 1, 100, 7, 1), (20, 1, 200, 8, 2), (25, 1, 200, 8, 60),
                         (30, 2, 100, 9, 3)]
           + [(-i, 100, i, i, i) for i in range(1, PAD + 1)])


@pytest.mark.parametrize("index,where,want", [
    # Out-of-order WHERE binds each value to the index's declared column, not to
    # AST order.
    ("(a, b)", "b = 200 AND a = 1", [20, 25]),
    # A range on the trailing column bounds the walk past the pinned prefix, up
    # to the widest index there is.
    ("(a, b)", "a = 1 AND b > 100", [20, 25]),
    ("(a, b, c, d)", "a = 1 AND b = 200 AND c = 8 AND d > 0 AND d < 50", [20]),
    # `a = 1` is consumed by the walk and `a > 5` stays a residual — excluded by
    # the consumed conjunct's physical index, not by column, so it survives and
    # (since a = 1) matches nothing. The companion proves that is the residual
    # filtering rather than the walk returning nothing.
    ("(a)", "a = 1 AND a > 5", []),
    ("(a)", "a = 1 AND a > 0", [10, 20, 25]),
    # Conjuncts on a column the walk does not cover filter after it.
    ("(a)", "a = 1 AND d = 2", [20]),
    ("(a)", "a = 1 AND c NOT BETWEEN 8 AND 9", [10]),
], ids=["out-of-order", "trailing-range", "widest-trailing-range", "residual-empty",
        "residual-kept", "residual-column", "residual-not-between"])
def test_which_rows_a_composite_walk_returns(client, abc, index, where, want):
    """The walk and the rows it returns, asserted together: a row bag alone
    passes with every index dropped, and an access line alone passes on a walk
    that loses rows."""
    client.execute_sql(f"CREATE INDEX ON t{index}")
    q = f"SELECT pk FROM t WHERE {where}"
    assert access(client, q) == f"index range on {index}"
    assert bag(rows(client, q)) == {(pk,): 1 for pk in want}


@pytest.mark.parametrize("nullable", [False, True], ids=["not-null", "nullable"])
def test_a_leading_prefix_is_walked_only_over_not_null_columns(client, nullable):
    """A walk over (a, b) holds no row NULL in `b`, so a prefix pinning `a` alone
    is served by the index only where `b` is NOT NULL. Where it is nullable the
    SQL read scans the base and does return the (1, NULL) row, and the binary
    seek refuses key values that stop short; the full key walks either way."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        f"b BIGINT{'' if nullable else ' NOT NULL'}, c BIGINT NOT NULL)")
    insert(client, "t", [(10, 1, 100, 7), (20, 1, 200, 8), (30, 2, 100, 9)]
           + ([(40, 1, None, 11)] if nullable else [])
           + [(-i, 100, i, i) for i in range(1, PAD + 1)])
    client.execute_sql("CREATE INDEX ON t(a, b)")
    tid, schema = client.resolve_table("t")

    q = "SELECT pk FROM t WHERE a = 1"
    want = {(10,): 1, (20,): 1, **({(40,): 1} if nullable else {})}
    assert access(client, q) == ("full scan" if nullable else "index range on (a, b)")
    assert bag(rows(client, q)) == want
    if nullable:
        with pytest.raises(ValueError, match="nullable"):
            client.seek_by_index(tid, schema, [1, 2], [1])
    else:
        assert bag(client.seek_by_index(tid, schema, [1, 2], [1]), "pk") == want

    q = "SELECT pk FROM t WHERE a = 1 AND b = 200"
    assert access(client, q) == "index range on (a, b)"
    assert bag(rows(client, q)) == {(20,): 1}
    assert bag(client.seek_by_index(tid, schema, [1, 2], [1, 200]), "pk") == {(20,): 1}


# ---------------------------------------------------------------------------
# An index over a PK column
# ---------------------------------------------------------------------------


@pytest.fixture
def pk_indexed(client):
    """A compound-PK table whose WHERE leaves the LEADING PK column free, so
    neither case below has a PK-range plan to fall back on."""
    client.execute_sql(
        "CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
        "v BIGINT NOT NULL, PRIMARY KEY (a, b))")
    insert(client, "t", [(1000 + i, 0, 0) for i in range(PAD)])


def test_an_index_over_a_trailing_pk_column_serves_the_where(client, pk_indexed):
    """A secondary index may name a PK column, and the planner treats it as any
    other index column."""
    insert(client, "t", [(1, 10, 100), (2, 10, 200), (3, 20, 300)])
    client.execute_sql("CREATE INDEX ON t(b, v)")

    q = "SELECT * FROM t WHERE b = 10 AND v = 200"
    assert access(client, q) == "index range on (b, v)"
    assert bag(rows(client, q)) == {(2, 10, 200): 1}
    # A range on the same column bounds the same index.
    q = "SELECT * FROM t WHERE b > 10"
    assert access(client, q) == "index range on (b, v)"
    assert bag(rows(client, q)) == {(3, 20, 300): 1}


def test_a_wide_literal_on_an_indexed_pk_column_is_servable(client, pk_indexed):
    """The literal overflows i64, so the predicate VM has no form for the
    conjunct: only an index walk that consumes it byte-exactly can serve this
    WHERE, and the PK rung cannot bound it (nothing pins `a`)."""
    insert(client, "t", [(1, _U64_MAX, 100), (2, 5, 200)])
    client.execute_sql("CREATE INDEX ON t(b)")

    q = f"SELECT * FROM t WHERE b = {_U64_MAX}"
    assert access(client, q) == "index range on (b)"
    assert bag(rows(client, q)) == {(1, _U64_MAX, 100): 1}
