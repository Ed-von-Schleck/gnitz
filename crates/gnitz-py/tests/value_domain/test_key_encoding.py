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

Asserted on weights throughout: the failure a mis-encoded key produces is a
retraction that seeks a key no row carries, which leaves the old row behind at
weight 1 beside the new one — and a row set reads that as correct.
"""

import pytest
from _read import bag, rows, scanned

U64_MAX = 18446744073709551615


def _seed(client, sn, ddl, rows_sql, table="t"):
    client.execute_sql(ddl, schema_name=sn)
    client.execute_sql(f"INSERT INTO {table} VALUES {rows_sql}", schema_name=sn)


# ---------------------------------------------------------------------------
# Single-column keys: the sign flip
# ---------------------------------------------------------------------------

# (SQL spelling, keys spanning the type's range and zero). Each list holds the
# minimum, -1, 0 and the maximum, so the range below cuts between -1 and 0 —
# the one boundary a missing sign flip moves.
_SIGNED_KEYS = [
    ("TINYINT", [-128, -1, 0, 127]),
    ("SMALLINT", [-32768, -1, 0, 32767]),
    ("INT", [-2147483648, -1, 0, 2147483647]),
    ("BIGINT", [-9223372036854775808, -1, 0, 9223372036854775807]),
]

_UNSIGNED_KEYS = [
    ("TINYINT UNSIGNED", [0, 1, 127, 128, 255]),
    ("SMALLINT UNSIGNED", [0, 1, 32768, 65535]),
    ("INT UNSIGNED", [0, 1, 2**31, 2**32 - 1]),
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
    # A bound between the two negatives confines to the upper one, so the order
    # holds inside the negative run too, not merely between the signs.
    assert bag(rows(client, sn, "SELECT k FROM t WHERE k > -1")) == \
        {(k,): 1 for k in keys if k > -1}


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


def test_the_minimum_signed_literal_addresses_its_own_row(client, schema_name):
    """`-9223372036854775808` has no positive counterpart, so a parser negating
    a parsed magnitude overflows on it rather than producing the value. The row
    it names must be the row it deletes."""
    sn = schema_name
    _seed(client, sn,
          "CREATE TABLE t (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
          "(-9223372036854775808, 1), (0, 2)")
    client.execute_sql("DELETE FROM t WHERE k = -9223372036854775808", schema_name=sn)
    assert bag(scanned(client, sn, "t"), "k", "v") == {(0, 2): 1}


# ---------------------------------------------------------------------------
# Compound keys: the column-by-column walk
# ---------------------------------------------------------------------------

# (PK column DDL, rows as (a, b, payload), the (a, b) to retract). `b` carries
# its type's maximum so that a compare treating the packed key as one wide
# integer with the later column most significant would invert the order; the
# leading column must win regardless. The signed variant sets the leading column
# negative, so the sign flip and the column walk have to be right at once.
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
    written, each at weight 1 — a mis-ordered key groups two distinct rows
    together and their weights accumulate against the wrong element.

    The retraction then removes the row whose *later* column is extreme: it is
    the one a compare that stopped at the leading column would fold against its
    sibling, leaving a ghost behind.
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


@pytest.mark.parametrize("lead_sql,lead_vals", [
    ("BIGINT UNSIGNED", [1, 2]),
    ("BIGINT", [-3, 2]),
], ids=["unsigned-lead", "signed-lead"])
def test_a_range_on_a_later_key_column_confines_within_its_prefix(
        client, schema_name, lead_sql, lead_vals):
    """Pinning the leading column and ranging on the next one walks a contiguous
    run of the packed key. It is the read that shows the columns are packed in
    list order and compared in it — a transposed pack answers with the other
    group's rows."""
    sn = schema_name
    lo, hi = lead_vals
    seed = [(lo, 0, 1), (lo, 5, 2), (lo, U64_MAX, 3), (hi, 0, 4), (hi, 5, 5)]
    _seed(client, sn,
          f"CREATE TABLE t (a {lead_sql} NOT NULL, b BIGINT UNSIGNED NOT NULL, "
          "payload BIGINT, PRIMARY KEY (a, b))",
          ", ".join(f"({a}, {b}, {p})" for a, b, p in seed))

    assert bag(rows(client, sn, f"SELECT b, payload FROM t WHERE a = {lo} AND b > 0")) == \
        {(b, p): 1 for a, b, p in seed if a == lo and b > 0}
    # The whole leading group, including its extreme trailing value.
    assert bag(rows(client, sn, f"SELECT b FROM t WHERE a = {lo}")) == \
        {(b,): 1 for a, b, _ in seed if a == lo}


def test_a_wide_key_distinguishes_two_rows_that_share_their_first_sixteen_bytes(
        client, schema_name):
    """Past 16 bytes the comparison is a byte walk rather than a packed compare,
    and routing hashes the whole key rather than a widened word. Two rows
    agreeing on `(a, b)` and differing only in `c` therefore exercise both at
    once: under several workers they can land on different workers, so a key
    hashed on its first 16 bytes alone would send the probe to the wrong one and
    the seek would come back empty rather than wrong.
    """
    sn = schema_name
    seed = [(1, 1, 1, 111), (1, 1, 2, 112), (1, 2, 1, 121), (2, 1, 1, 211)]
    _seed(client, sn,
          "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, c BIGINT UNSIGNED, "
          "payload BIGINT, PRIMARY KEY (a, b, c))",
          ", ".join(f"({a}, {b}, {c}, {p})" for a, b, c, p in seed))

    assert bag(scanned(client, sn, "t"), "a", "b", "c", "payload") == {r: 1 for r in seed}

    # Each prefix-sharing sibling resolves to its own row, never the other.
    for a, b, c, p in seed:
        assert bag(rows(client, sn,
                        f"SELECT payload FROM t WHERE a = {a} AND b = {b} AND c = {c}")) \
            == {(p,): 1}
    # A key sharing the 16-byte prefix but absent in `c` misses, rather than
    # matching the sibling that shares the prefix.
    assert bag(rows(client, sn,
                    "SELECT payload FROM t WHERE a = 1 AND b = 1 AND c = 99")) == {}

    # The retraction takes the sibling and leaves the other at weight 1.
    client.execute_sql("DELETE FROM t WHERE a = 1 AND b = 1 AND c = 2", schema_name=sn)
    assert bag(scanned(client, sn, "t"), "a", "b", "c", "payload") == \
        {r: 1 for r in seed if r[:3] != (1, 1, 2)}


def test_a_wide_signed_key_flips_its_leading_column_past_sixteen_bytes(
        client, schema_name):
    """The sign flip and the wide byte walk at once, which neither the stride-16
    signed case nor the unsigned stride-24 case reaches alone."""
    sn = schema_name
    seed = [(2, 0, 0, 200), (-3, U64_MAX, 0, 1399), (-3, 0, U64_MAX, 309),
            (-3, 0, 0, 300), (-1, 1, 1, 111)]
    _seed(client, sn,
          "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT UNSIGNED NOT NULL, "
          "c BIGINT UNSIGNED NOT NULL, payload BIGINT, PRIMARY KEY (a, b, c))",
          ", ".join(f"({a}, {b}, {c}, {p})" for a, b, c, p in seed))

    assert bag(scanned(client, sn, "t"), "a", "b", "c", "payload") == {r: 1 for r in seed}
    assert bag(rows(client, sn, "SELECT payload FROM t WHERE a < 0")) == \
        {(p,): 1 for a, _, _, p in seed if a < 0}

    client.execute_sql(f"DELETE FROM t WHERE a = -3 AND b = {U64_MAX} AND c = 0",
                       schema_name=sn)
    assert bag(scanned(client, sn, "t"), "a", "b", "c", "payload") == \
        {r: 1 for r in seed if r[:3] != (-3, U64_MAX, 0)}


def test_the_key_region_is_its_columns_packed_tightly_in_list_order(
        client, schema_name):
    """`pk_stride` is the sum of the members' encoded widths with no padding, and
    the client surfaces the region as those bytes. Asserting the width and
    decoding both halves is what pins that a later member sits at the offset the
    earlier ones leave, rather than at an aligned one."""
    sn = schema_name
    _seed(client, sn,
          "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, payload BIGINT, "
          "PRIMARY KEY (a, b))",
          "(1, 2, 10), (3, 4, 20)")
    tid, _ = client.resolve_table(sn, "t")

    pks = client.scan(tid).pks
    assert all(isinstance(p, bytes) and len(p) == 16 for p in pks), pks
    assert sorted((int.from_bytes(p[:8], "little"), int.from_bytes(p[8:], "little"))
                  for p in pks) == [(1, 2), (3, 4)]


@pytest.mark.parametrize("widths,keys", [
    ((8, 8), [(1, 2), (3, 4), (5, 6)]),
    ((8, 8, 8), [(1, 1, 1), (1, 1, 2), (2, 2, 2)]),
], ids=["stride16", "stride24"])
def test_a_retraction_names_its_row_by_the_same_packed_key(client, schema_name,
                                                           widths, keys):
    """The binding builds the key the delete verb ships, so a row is retracted by
    the bytes the client packs rather than by a value the engine re-derives. It
    is the same contract as the layout above, read from the write side: a key
    packed at the wrong offset or width names a row that does not exist, and the
    retraction is then dropped without an error.

    The stride-24 case matters separately because its key is past the width a
    single packed compare covers, so the engine locates it by a byte walk.
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
