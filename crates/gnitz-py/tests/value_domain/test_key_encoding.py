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
import uuid
from decimal import Decimal

import pytest
import gnitz
from _read import access, bag, rows, scanned
from _sql import insert, values

U64_MAX = 18446744073709551615


# ---------------------------------------------------------------------------
# Compound keys: the column-by-column walk
# ---------------------------------------------------------------------------

# (key types in PK order, rows as (*key, payload)). In the lowest leading group
# the second column carries its type's maximum, so a compare treating the
# packed key as one wide integer would invert the order the leading column
# dictates.
_COMPOUND = [
    pytest.param(["BIGINT UNSIGNED", "BIGINT UNSIGNED"],
                 [(2, 1, 21), (1, U64_MAX, 1399), (1, 5, 15), (1, 0, 10), (2, 0, 20)], id="stride16"),
    pytest.param(["BIGINT", "BIGINT UNSIGNED"],
                 [(2, 0, 20), (-3, U64_MAX, 399), (-3, 0, 30), (-1, 5, 15), (2, 1, 21)],
                 id="stride16-signed"),
    pytest.param(["BIGINT UNSIGNED", "INT UNSIGNED"],
                 [(1, 0, 10), (1, 4294967295, 11), (1, 7, 17), (2, 0, 20)], id="stride12"),
    # Two rows agree on their first 16 bytes and differ only in `c`.
    pytest.param(["BIGINT", "BIGINT UNSIGNED", "BIGINT UNSIGNED"],
                 [(-3, 1, 1, 311), (-3, 1, 2, 312), (-3, U64_MAX, 0, 1399), (-3, 2, 1, 321), (2, 1, 1, 211)],
                 id="stride24"),
]


@pytest.mark.parametrize("types,seed", _COMPOUND)
def test_an_earlier_key_column_dominates_a_later_one(client, types, seed):
    """Rows arrive scrambled and must consolidate into exactly the tuples
    written, each at weight 1 — a mis-ordered key groups two distinct rows and
    their weights accumulate against the wrong element. The key columns are
    declared in reverse, so a pack following declaration order rather than the
    PK list would interleave the groups, and a range within the lowest group
    walks a contiguous run of the packed key.

    Each row is probed by its whole key, and a key absent only in its last
    column misses rather than matching a sibling. Past 16 bytes the comparison
    is a byte walk rather than a packed compare, and routing hashes the whole
    key rather than a widened word: under several workers two rows sharing
    their first 16 bytes can land on different workers, so a key hashed on that
    prefix alone would probe the wrong one.

    The group's two extremes are retracted, one by SQL and one by the client's
    `delete`, which packs the key tuple itself at `pk_stride` — the members'
    widths summed with no padding. A key packed at the wrong offset, width or
    order names a row that does not exist, and the retraction is then dropped
    without an error.
    """
    key = "abc"[:len(types)]
    decls = [f"{c} {t} NOT NULL" for c, t in reversed(list(zip(key, types)))]
    decls.insert(1, "payload BIGINT")
    client.execute_sql(
        f"CREATE TABLE t ({', '.join(decls)}, PRIMARY KEY ({', '.join(key)})); "
        f"INSERT INTO t ({', '.join(key)}, payload) VALUES {values(seed)}")
    assert bag(scanned(client, "t"), *key, "payload") == dict.fromkeys(seed, 1)

    def at(k):
        return bag(rows(client, "SELECT payload FROM t WHERE "
                        + " AND ".join(f"{c} = {v}" for c, v in zip(key, k))))

    for *k, p in seed:
        assert at(k) == {(p,): 1}, k
    lo = min(r[0] for r in seed)
    group = sorted(r[:-1] for r in seed if r[0] == lo)
    assert at((*group[0][:-1], 99)) == {}
    assert bag(rows(client, f"SELECT payload FROM t WHERE a = {lo} AND b > {group[0][1]}")) == \
        {(r[-1],): 1 for r in seed if r[0] == lo and r[1] > group[0][1]}

    client.execute_sql(
        "DELETE FROM t WHERE " + " AND ".join(f"{c} = {v}" for c, v in zip(key, group[-1])))
    client.delete(*client.resolve_table("t"), [group[0]])
    assert bag(scanned(client, "t"), *key, "payload") == \
        {r: 1 for r in seed if r[:-1] not in (group[0], group[-1])}


# Rows no predicate below selects, so the index is the walk the store takes:
# it walks an index only over a table many times the index's estimated length.
_PAD = 1000


@pytest.mark.parametrize("pk_ddl,cols,keys,pad", [
    pytest.param("a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
                 "c BIGINT UNSIGNED NOT NULL", "a, b, c",
                 [(1, 1, 1), (1, 1, 2), (3, 4, 5)], lambda i: (10 + i, 0, 0), id="source-24"),
    pytest.param("a UUID NOT NULL", "a",
                 [("00000000-0000-0000-0000-000000000001",),
                  ("00000000-0000-0000-0000-000000000002",),
                  ("ffffffff-ffff-ffff-ffff-ffffffffffff",)],
                 lambda i: (str(uuid.UUID(int=10 + i)),), id="source-16"),
])
def test_an_index_key_grows_with_its_sources_key_and_stays_exact(
        client, pk_ddl, cols, keys, pad):
    """An index row is keyed by the indexed column followed by the whole source
    key, so its own key is 8 + the source's — 32 and 24 here, both past the
    packed-compare width. An index key truncated at the narrow width would fold
    the two rows sharing payload 100 into one entry and lose a row.
    """
    client.execute_sql(
        f"CREATE TABLE src ({pk_ddl}, payload BIGINT NOT NULL, PRIMARY KEY ({cols})); "
        "CREATE INDEX ON src (payload)")
    # The first two rows share payload 100; the third carries 200.
    insert(client, "src", [(*k, p) for k, p in zip(keys, [100, 100, 200])]
           + [(*pad(i), 1000 + i) for i in range(_PAD)])

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


@pytest.mark.parametrize("sql,vals", [
    ("SMALLINT", [-32768, -1, 0, 1, 32767]),
    ("INT", [-2147483648, -1, 0, 1, 2147483647]),
    ("BIGINT UNSIGNED", [0, 1, 2**63, U64_MAX]),
], ids=["i16", "i32", "u64"])
def test_a_unique_index_refuses_a_value_a_second_push_repeats(client, sql, vals):
    """An index key at each width enforces across pushes at both ends of its
    range and on both sides of its sign bit: each seeded value is refused under
    a new PK, and a value no row holds lands."""
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val {sql} NOT NULL); "
        "CREATE UNIQUE INDEX ON t(val)")
    seed = list(enumerate(vals))
    insert(client, "t", seed)
    for pk, val in seed:
        with pytest.raises(gnitz.GnitzIntegrityError, match="Unique index violation on '[^']*' column 'val'"):
            insert(client, "t", [(pk + 100, val)])
    insert(client, "t", [(200, 8)])
    assert bag(scanned(client, "t")) == dict.fromkeys(seed + [(200, 8)], 1)
