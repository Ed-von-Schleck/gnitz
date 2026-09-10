"""The order-preserving key: plain unsigned `memcmp` over the encoded bytes *is*
the typed lexicographic key order, at every width, sign and arity.

**Order is asserted through range predicates, never through a sorted read.** A
range on a key column is lowered to a walk over the OPK bytes, so the rows it
returns are a direct statement about the byte order the encoder produced. Sorting
the scanned keys in Python and comparing to `sorted(values)` — the shape this
file replaces — is invariant to whatever order the engine produced and proves
only that the key decodes.

Two properties carry every case:

  * **Sign.** A signed column has its high bit flipped, so `-1` sorts below `0`
    rather than above every positive. A range spanning zero is the one read that
    separates the two.
  * **Arity.** A compound key is its columns packed tightly in PK-list order, so
    an earlier column dominates a later one whatever their values. Adversarial
    high-half values are what distinguish that walk from a compare that treats
    the packed bytes as one wide integer with the later column most significant.

`gnitz-wire`'s own tests sweep the encoder over every PK-eligible type. What an
end-to-end test adds is that a SQL range predicate lowers to that byte walk,
which is not per-width — hence the narrowest and widest of each sign, not the
full ladder.

Asserted on weights throughout: the failure a mis-encoded key produces is a
retraction that seeks a key no row carries, which leaves the old row behind at
weight 1 beside the new one — and a row set reads that as correct.
"""

import pytest
from _read import access, bag, rows, scanned

U64_MAX = 18446744073709551615


def _seed(client, sn, ddl, rows_sql):
    client.execute_sql(ddl, schema_name=sn)
    client.execute_sql(f"INSERT INTO t VALUES {rows_sql}", schema_name=sn)


# ---------------------------------------------------------------------------
# Single-column keys: the sign flip
# ---------------------------------------------------------------------------

# (SQL spelling, keys spanning the type's range and zero). Each list holds the
# minimum, -1, 0 and the maximum, so the range below cuts between -1 and 0 —
# the one boundary a missing sign flip moves.
_SIGNED_KEYS = [
    ("TINYINT", [-128, -1, 0, 127]),
    ("BIGINT", [-9223372036854775808, -1, 0, 9223372036854775807]),
]

_UNSIGNED_KEYS = [
    ("TINYINT UNSIGNED", [0, 1, 127, 128, 255]),
    ("BIGINT UNSIGNED", [0, 1, 2**63, U64_MAX]),
]


@pytest.mark.parametrize("pk_sql,keys", _SIGNED_KEYS,
                         ids=[t[0].lower() for t in _SIGNED_KEYS])
def test_a_signed_key_range_cuts_at_zero_not_at_the_wrap(
        client, schema_name, pk_sql, keys):
    """Without the sign flip a negative key encodes above every positive one, so
    `k >= 0` would return the negatives and `k < 0` nothing. The two halves are
    asserted as exact complements, which no single range can fake."""
    sn = schema_name
    _seed(client, sn,
          f"CREATE TABLE t (k {pk_sql} NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
          ", ".join(f"({k}, {i})" for i, k in enumerate(keys)))

    neg = {(k, i) for i, k in enumerate(keys) if k < 0}
    pos = {(k, i) for i, k in enumerate(keys) if k >= 0}
    assert bag(rows(client, sn, "SELECT k, v FROM t WHERE k < 0")) == {p: 1 for p in neg}
    assert bag(rows(client, sn, "SELECT k, v FROM t WHERE k >= 0")) == {p: 1 for p in pos}
    # A strict bound at the minimum still admits the other negative, so the order
    # holds inside the negative run too, not merely between the signs.
    assert bag(rows(client, sn, f"SELECT k FROM t WHERE k > {keys[0]}")) == \
        {(k,): 1 for k in keys[1:]}


@pytest.mark.parametrize("pk_sql,keys", _UNSIGNED_KEYS,
                         ids=[t[0].split()[0].lower() + "u" for t in _UNSIGNED_KEYS])
def test_an_unsigned_key_range_orders_past_the_signed_midpoint(
        client, schema_name, pk_sql, keys):
    """The mirror case: an unsigned column must *not* flip. A spurious flip
    would sort the values above the signed midpoint below the ones under it, so
    the cut is taken there."""
    sn = schema_name
    mid = 1 << (keys[-1].bit_length() - 1)
    _seed(client, sn,
          f"CREATE TABLE t (k {pk_sql} NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
          ", ".join(f"({k}, {i})" for i, k in enumerate(keys)))

    assert bag(rows(client, sn, f"SELECT k FROM t WHERE k >= {mid}")) == \
        {(k,): 1 for k in keys if k >= mid}
    assert bag(rows(client, sn, f"SELECT k FROM t WHERE k < {mid}")) == \
        {(k,): 1 for k in keys if k < mid}


# ---------------------------------------------------------------------------
# Compound keys: the column-by-column walk
# ---------------------------------------------------------------------------

# (PK column DDL, rows as (a, b, payload), the (a, b) to retract). `b` carries
# its type's maximum, so a compare treating the packed key as one wide integer
# would invert the order the leading column dictates.
_COMPOUND = [
    pytest.param(
        "a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL",
        [(2, 1, 21), (1, U64_MAX, 1399), (1, 5, 15), (1, 0, 10), (2, 0, 20)],
        (1, U64_MAX), id="stride16"),
    pytest.param(
        "a BIGINT NOT NULL, b BIGINT UNSIGNED NOT NULL",
        [(2, 0, 20), (-3, U64_MAX, 399), (-3, 0, 30), (-1, 5, 15), (2, 1, 21)],
        (-3, U64_MAX), id="stride16-signed"),
    pytest.param(
        "a BIGINT UNSIGNED NOT NULL, b INT UNSIGNED NOT NULL",
        [(1, 0, 10), (1, 4294967295, 11), (1, 7, 17), (2, 0, 20)],
        (1, 4294967295), id="stride12"),
]


@pytest.mark.parametrize("cols,seed,retract", _COMPOUND)
def test_an_earlier_key_column_dominates_a_later_one(
        client, schema_name, cols, seed, retract):
    """Rows arrive scrambled and must consolidate into exactly the tuples
    written, each at weight 1 — a mis-ordered key groups two distinct rows and
    their weights accumulate against the wrong element. The retracted row is the
    one a compare stopping at the leading column would fold against its sibling.
    """
    sn = schema_name
    _seed(client, sn,
          f"CREATE TABLE t ({cols}, payload BIGINT, PRIMARY KEY (a, b))",
          ", ".join(f"({a}, {b}, {p})" for a, b, p in seed))

    assert bag(scanned(client, sn, "t"), "a", "b", "payload") == {r: 1 for r in seed}
    client.execute_sql(f"DELETE FROM t WHERE a = {retract[0]} AND b = {retract[1]}",
                       schema_name=sn)
    assert bag(scanned(client, sn, "t"), "a", "b", "payload") == \
        {r: 1 for r in seed if r[:2] != retract}


def test_a_range_on_a_later_key_column_confines_within_its_prefix(client, schema_name):
    """Pinning the leading column and ranging on the next one walks a contiguous
    run of the packed key. The PK list and the column order disagree below, so a
    pack following declaration order would interleave the two groups instead.
    """
    sn = schema_name
    seed = [(1, 0, 1), (1, 5, 2), (1, U64_MAX, 3), (2, 0, 4), (2, 5, 5)]
    client.execute_sql(
        "CREATE TABLE t (b BIGINT UNSIGNED NOT NULL, payload BIGINT, "
        "a BIGINT UNSIGNED NOT NULL, PRIMARY KEY (a, b))", schema_name=sn)
    _, schema = client.resolve_table(sn, "t")
    assert schema.pk_indices == [2, 0], "the PK list order, not the column order"
    client.execute_sql(
        "INSERT INTO t (a, b, payload) VALUES " +
        ", ".join(f"({a}, {b}, {p})" for a, b, p in seed), schema_name=sn)

    assert bag(rows(client, sn, "SELECT b, payload FROM t WHERE a = 1 AND b > 0")) == \
        {(b, p): 1 for a, b, p in seed if a == 1 and b > 0}
    # The whole leading group, including its extreme trailing value.
    assert bag(rows(client, sn, "SELECT b FROM t WHERE a = 1")) == \
        {(b,): 1 for a, b, _ in seed if a == 1}


def test_a_wide_key_distinguishes_two_rows_that_share_their_first_sixteen_bytes(
        client, schema_name):
    """Past 16 bytes the comparison is a byte walk rather than a packed compare,
    and routing hashes the whole key rather than a widened word. Two rows
    agreeing on `(a, b)` and differing only in `c` therefore exercise both at
    once: under several workers they can land on different workers, so a key
    hashed on its first 16 bytes alone would probe the wrong one and the seek
    would come back empty rather than wrong. The leading column is signed, so the
    flip rides that same byte walk.
    """
    sn = schema_name
    seed = [(-3, 1, 1, 311), (-3, 1, 2, 312), (-3, U64_MAX, 0, 1399),
            (-3, 2, 1, 321), (2, 1, 1, 211)]
    _seed(client, sn,
          "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT UNSIGNED NOT NULL, "
          "c BIGINT UNSIGNED NOT NULL, payload BIGINT, PRIMARY KEY (a, b, c))",
          ", ".join(f"({a}, {b}, {c}, {p})" for a, b, c, p in seed))

    assert bag(scanned(client, sn, "t"), "a", "b", "c", "payload") == {r: 1 for r in seed}
    assert bag(rows(client, sn, "SELECT payload FROM t WHERE a < 0")) == \
        {(p,): 1 for a, _, _, p in seed if a < 0}

    # Each prefix-sharing sibling resolves to its own row, never the other.
    for a, b, c, p in seed:
        assert bag(rows(client, sn,
                        f"SELECT payload FROM t WHERE a = {a} AND b = {b} AND c = {c}")) \
            == {(p,): 1}
    # A key sharing the 16-byte prefix but absent in `c` misses, rather than
    # matching the sibling that shares the prefix.
    assert bag(rows(client, sn,
                    "SELECT payload FROM t WHERE a = -3 AND b = 1 AND c = 99")) == {}

    # The retraction takes the sibling and leaves the other at weight 1.
    client.execute_sql("DELETE FROM t WHERE a = -3 AND b = 1 AND c = 2", schema_name=sn)
    assert bag(scanned(client, sn, "t"), "a", "b", "c", "payload") == \
        {r: 1 for r in seed if r[:3] != (-3, 1, 2)}


@pytest.mark.parametrize("widths,keys", [
    ((8, 8), [(1, 2), (3, 4), (5, 6)]),
    ((8, 8, 8), [(1, 1, 1), (1, 1, 2), (2, 2, 2)]),
], ids=["stride16", "stride24"])
def test_a_retraction_names_its_row_by_the_same_packed_key(client, schema_name,
                                                           widths, keys):
    """The delete verb ships the key the client packs, at `pk_stride` — the
    members' widths summed with no padding. A key packed at the wrong offset,
    width or order names a row that does not exist, and the retraction is then
    dropped without an error. Stride 24 is past the width a single packed compare
    covers, so the engine locates it by a byte walk instead.
    """
    sn = schema_name
    cols = "abc"[:len(widths)]
    ddl_cols = ", ".join(f"{c} BIGINT UNSIGNED NOT NULL" for c in cols)
    _seed(client, sn,
          f"CREATE TABLE t ({ddl_cols}, payload BIGINT, PRIMARY KEY ({', '.join(cols)}))",
          ", ".join("(" + ", ".join(str(x) for x in k) + f", {i})"
                    for i, k in enumerate(keys)))
    tid, schema = client.resolve_table(sn, "t")

    gone = keys[1]
    packed = b"".join(v.to_bytes(w, "little") for v, w in zip(gone, widths))
    client.delete(tid, schema, [packed])

    assert bag(scanned(client, sn, "t"), *cols, "payload") == \
        {k + (i,): 1 for i, k in enumerate(keys) if k != gone}


@pytest.mark.parametrize("pk_ddl,pk_list,keys", [
    pytest.param("a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
                 "c BIGINT UNSIGNED NOT NULL", "(a, b, c)",
                 [(1, 1, 1), (1, 1, 2), (3, 4, 5)], id="source-24"),
    pytest.param("a UUID NOT NULL", "(a)",
                 [("00000000-0000-0000-0000-000000000001",),
                  ("00000000-0000-0000-0000-000000000002",),
                  ("ffffffff-ffff-ffff-ffff-ffffffffffff",)], id="source-16"),
])
def test_an_index_key_grows_with_its_sources_key_and_stays_exact(
        client, schema_name, pk_ddl, pk_list, keys):
    """An index row is keyed by the indexed column followed by the whole source
    key, so its own key is 8 + the source's — 32 and 24 here, both past the
    packed-compare width. An index key truncated at the narrow width would fold
    the two rows sharing payload 100 into one entry and lose a row, and the
    `access:` line is what stops a full-scan fallback passing silently.
    """
    sn = schema_name
    cols = ", ".join(pk_list.strip("()").split(", "))
    client.execute_sql(
        f"CREATE TABLE src ({pk_ddl}, payload BIGINT NOT NULL, PRIMARY KEY {pk_list})",
        schema_name=sn)
    client.execute_sql("CREATE INDEX ON src (payload)", schema_name=sn)
    # The first two rows share payload 100; the third carries 200.
    client.execute_sql(
        "INSERT INTO src VALUES " + ", ".join(
            "(" + ", ".join(repr(x) if isinstance(x, str) else str(x) for x in k)
            + f", {p})" for k, p in zip(keys, [100, 100, 200])),
        schema_name=sn)

    q = f"SELECT {cols} FROM src WHERE payload = 100"
    assert access(client, sn, q).startswith("access: index range on (payload)"), \
        access(client, sn, q)
    assert bag(rows(client, sn, q)) == {keys[0]: 1, keys[1]: 1}
    assert bag(rows(client, sn, f"SELECT {cols} FROM src WHERE payload = 200")) == \
        {keys[2]: 1}
