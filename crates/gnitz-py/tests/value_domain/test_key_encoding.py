"""The order-preserving key across its columns: plain unsigned `memcmp` over the
encoded bytes *is* the typed lexicographic key order at every arity.

A compound key is its columns packed tightly in PK-list order, so an earlier
column dominates a later one whatever their values. Adversarial high-half values
are what distinguish that walk from a compare that treats the packed bytes as
one wide integer with the later column most significant. A single column's
order, the sign flip, is asserted per type beside the cross-encoder key test.

Order is asserted through range predicates, never through a sorted read: a
range on a key column is lowered to a walk over the encoded bytes, so the rows
it returns are a direct statement about the byte order the encoder produced.

Asserted on weights throughout: the failure a mis-encoded key produces is a
retraction that seeks a key no row carries, which leaves the old row behind at
weight 1 beside the new one — and a row set reads that as correct.
"""

import operator
from decimal import Decimal

import pytest
import gnitz
from _read import access, bag, rows, scanned
from _sql import insert, values

U64_MAX = 18446744073709551615


# ---------------------------------------------------------------------------
# Compound keys: the column-by-column walk
# ---------------------------------------------------------------------------

# (`a` type, `b` type, rows as (a, b, payload)). In the lowest `a` group `b`
# carries its type's maximum, so a compare treating the packed key as one wide
# integer would invert the order the leading column dictates.
_COMPOUND = [
    pytest.param("BIGINT UNSIGNED", "BIGINT UNSIGNED",
                 [(2, 1, 21), (1, U64_MAX, 1399), (1, 5, 15), (1, 0, 10), (2, 0, 20)], id="stride16"),
    pytest.param("BIGINT", "BIGINT UNSIGNED",
                 [(2, 0, 20), (-3, U64_MAX, 399), (-3, 0, 30), (-1, 5, 15), (2, 1, 21)],
                 id="stride16-signed"),
    pytest.param("BIGINT UNSIGNED", "INT UNSIGNED",
                 [(1, 0, 10), (1, 4294967295, 11), (1, 7, 17), (2, 0, 20)], id="stride12"),
]


@pytest.mark.parametrize("a_sql,b_sql,seed", _COMPOUND)
def test_an_earlier_key_column_dominates_a_later_one(client, a_sql, b_sql, seed):
    """Rows arrive scrambled and must consolidate into exactly the tuples
    written, each at weight 1 — a mis-ordered key groups two distinct rows and
    their weights accumulate against the wrong element. `b` is declared before
    `a`, so a pack following declaration order rather than the PK list would
    interleave the groups, and a range within the lowest group walks a
    contiguous run of the packed key.

    The group's two extremes are retracted, one by SQL and one by the client's
    `delete`, which packs the key tuple itself at `pk_stride` — the members'
    widths summed with no padding. A key packed at the wrong offset, width or
    order names a row that does not exist, and the retraction is then dropped
    without an error.
    """
    client.execute_sql(
        f"CREATE TABLE t (b {b_sql} NOT NULL, payload BIGINT, a {a_sql} NOT NULL, "
        f"PRIMARY KEY (a, b)); INSERT INTO t (a, b, payload) VALUES {values(seed)}")
    tid, schema = client.resolve_table("t")
    assert bag(scanned(client, "t"), "a", "b", "payload") == dict.fromkeys(seed, 1)

    lo = min(a for a, _, _ in seed)
    least, most = min(b for a, b, _ in seed if a == lo), max(b for a, b, _ in seed if a == lo)
    assert bag(rows(client, f"SELECT b, payload FROM t WHERE a = {lo} AND b > {least}")) == \
        {(b, p): 1 for a, b, p in seed if a == lo and b > least}

    client.execute_sql(f"DELETE FROM t WHERE a = {lo} AND b = {most}")
    client.delete(tid, schema, [(lo, least)])
    assert bag(scanned(client, "t"), "a", "b", "payload") == \
        {r: 1 for r in seed if r[:2] not in ((lo, least), (lo, most))}


def test_a_wide_key_distinguishes_two_rows_that_share_their_first_sixteen_bytes(
        client):
    """Past 16 bytes the comparison is a byte walk rather than a packed compare,
    and routing hashes the whole key rather than a widened word. Two rows
    agreeing on `(a, b)` and differing only in `c` therefore exercise both at
    once: under several workers they can land on different workers, so a key
    hashed on its first 16 bytes alone would probe the wrong one and the seek
    would come back empty rather than wrong. The leading column is signed, so the
    flip rides that same byte walk.
    """
    seed = [(-3, 1, 1, 311), (-3, 1, 2, 312), (-3, U64_MAX, 0, 1399),
            (-3, 2, 1, 321), (2, 1, 1, 211)]
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT UNSIGNED NOT NULL, "
        "c BIGINT UNSIGNED NOT NULL, payload BIGINT, PRIMARY KEY (a, b, c))")
    insert(client, "t", seed)

    assert bag(scanned(client, "t"), "a", "b", "c", "payload") == {r: 1 for r in seed}
    assert bag(rows(client, "SELECT payload FROM t WHERE a < 0")) == \
        {(p,): 1 for a, _, _, p in seed if a < 0}

    # Each prefix-sharing sibling resolves to its own row, never the other.
    for a, b, c, p in seed:
        assert bag(rows(client,
                        f"SELECT payload FROM t WHERE a = {a} AND b = {b} AND c = {c}")) \
            == {(p,): 1}
    # A key sharing the 16-byte prefix but absent in `c` misses, rather than
    # matching the sibling that shares the prefix.
    assert bag(rows(client,
                    "SELECT payload FROM t WHERE a = -3 AND b = 1 AND c = 99")) == {}

    # The retraction takes the sibling and leaves the other at weight 1, by SQL
    # and by the client's `delete`, which packs the 24-byte key itself.
    client.execute_sql("DELETE FROM t WHERE a = -3 AND b = 1 AND c = 2")
    client.delete(*client.resolve_table("t"), [(-3, U64_MAX, 0)])
    assert bag(scanned(client, "t"), "a", "b", "c", "payload") == \
        {r: 1 for r in seed if r[:3] not in ((-3, 1, 2), (-3, U64_MAX, 0))}


@pytest.mark.parametrize("pk_ddl,cols,keys", [
    pytest.param("a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
                 "c BIGINT UNSIGNED NOT NULL", "a, b, c",
                 [(1, 1, 1), (1, 1, 2), (3, 4, 5)], id="source-24"),
    pytest.param("a UUID NOT NULL", "a",
                 [("00000000-0000-0000-0000-000000000001",),
                  ("00000000-0000-0000-0000-000000000002",),
                  ("ffffffff-ffff-ffff-ffff-ffffffffffff",)], id="source-16"),
])
def test_an_index_key_grows_with_its_sources_key_and_stays_exact(
        client, pk_ddl, cols, keys):
    """An index row is keyed by the indexed column followed by the whole source
    key, so its own key is 8 + the source's — 32 and 24 here, both past the
    packed-compare width. An index key truncated at the narrow width would fold
    the two rows sharing payload 100 into one entry and lose a row, and the
    `access:` line is what stops a full-scan fallback passing silently.
    """
    client.execute_sql(
        f"CREATE TABLE src ({pk_ddl}, payload BIGINT NOT NULL, PRIMARY KEY ({cols})); "
        "CREATE INDEX ON src (payload)")
    # The first two rows share payload 100; the third carries 200.
    insert(client, "src", [(*k, p) for k, p in zip(keys, [100, 100, 200])])

    q = f"SELECT {cols} FROM src WHERE payload = 100"
    assert access(client, q) == "index range on (payload)"
    assert bag(rows(client, q)) == {keys[0]: 1, keys[1]: 1}
    assert bag(rows(client, f"SELECT {cols} FROM src WHERE payload = 200")) == \
        {keys[2]: 1}


# ---------------------------------------------------------------------------
# A literal against a key: the walk and the filter agree
#
# A key column's predicate is served by a walk over its OPK bytes, the same
# predicate on a non-key column by the filter program. A literal outside the
# column's type, or between two of its values, must mean the same on both.
# ---------------------------------------------------------------------------

_OPS = [("=", operator.eq), ("<>", operator.ne), ("<", operator.lt),
        ("<=", operator.le), (">", operator.gt), (">=", operator.ge)]

# (key type, keys, literals outside the type or between two of its values)
_LITERALS = [
    pytest.param("BIGINT UNSIGNED", [0, 1, 2**63, U64_MAX],
                 ["-5", "-1", "0", str(U64_MAX), str(U64_MAX + 1)], id="u64-outside"),
    pytest.param("BIGINT", [-2, -1, 0, 1, 2, 3], ["1.5", "-1.5"], id="i64-fraction"),
    pytest.param("DECIMAL(6, 2)", [Decimal(i - 6) / 4 for i in range(12)],
                 ["1.255", "-0.505", "1.25"], id="decimal-finer"),
]


@pytest.mark.parametrize("sql,keys,lits", _LITERALS)
def test_a_literal_bounds_a_key_walk_as_it_bounds_the_filter(client, sql, keys, lits):
    """A key column's predicate is served by a walk over its encoded bytes, the
    same predicate on a non-key column by the filter program. A literal outside
    the column's type, or between two of its values, must mean the same on
    both: `=` misses rather than rounding onto a neighbour, and every range cuts
    between the two values the literal falls between. A literal the column
    holds rides alongside as the control."""
    client.execute_sql(f"CREATE TABLE t (k {sql} NOT NULL PRIMARY KEY, c {sql} NOT NULL)")
    insert(client, "t", [(k, k) for k in keys])
    assert access(client, f"SELECT k FROM t WHERE k < {lits[0]}") == "pk range walk"
    for lit in lits:
        for op, holds in _OPS:
            want = {(k,): 1 for k in keys if holds(k, Decimal(lit))}
            for col in ("k", "c"):
                q = f"SELECT k FROM t WHERE {col} {op} {lit}"
                assert bag(rows(client, q)) == want, q


def test_a_smallint_unique_index_refuses_a_value_a_second_push_repeats(client):
    """A 2-byte index key enforces across pushes at both ends of its range and
    on both sides of zero: each seeded value is refused under a new PK, and a
    value no row holds lands."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val SMALLINT NOT NULL); "
        "CREATE UNIQUE INDEX ON t(val)")
    seed = list(zip([1, 7, 13, 1000, 99999, 123456, 777777, 8888888, 73501234, 901234567],
                    [-32768, -1, 0, 1, 32767, 7, -7, 100, -100, 255]))
    insert(client, "t", seed)
    for pk, val in seed:
        with pytest.raises(gnitz.GnitzIntegrityError, match="Unique index violation on '[^']*' column 'val'"):
            insert(client, "t", [(pk + 1, val)])
    insert(client, "t", [(5, 8), (6, -8)])
    assert bag(scanned(client, "t")) == dict.fromkeys(seed + [(5, 8), (6, -8)], 1)
