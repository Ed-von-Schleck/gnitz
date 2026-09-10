"""NULL as a value: which bit records it, and what it refuses to equal.

A NULL payload cell is **zero-filled and the null bitmap is the sole truth**, so
a test only proves something read that bitmap if the zero would have answered
differently. Every case here therefore pairs a NULL against the type's own zero —
the integer `0`, the empty string — rather than against an arbitrary value: a
predicate over a NULL that a zero would also fail proves nothing. The families
whose zero is not an integer are `test_value_round_trip.py`'s.

The bitmap is indexed by *payload* position, which is the column's index minus
the number of PK columns before it. For a PK declared as a prefix that is the
same as "minus the PK count", so a prefix table cannot tell a correct
implementation from one that subtracts a constant. Only a key interleaved with
the payload separates them.

The connectives themselves are one per-row program whatever the column type —
the width is consumed entirely by the load that fills the register, and
`AND`/`OR`/`NOT` never see a column — so they are stated once over the full 3×3
pattern table in `scalar_expression/test_three_valued_logic.py`. What varies here
is the *type*: each width has its own load and its own bit, and a width-specific
path could obey the rule for BIGINT and not for TINYINT.
"""

import pytest
from _read import bag, rows, scanned

# (SQL spelling, the type's minimum, its maximum).
_WIDTHS = [
    ("TINYINT", -128, 127),
    ("SMALLINT", -32768, 32767),
    ("INT", -2147483648, 2147483647),
    ("BIGINT", -9223372036854775808, 9223372036854775807),
    ("TINYINT UNSIGNED", 0, 255),
    ("SMALLINT UNSIGNED", 0, 65535),
    ("INT UNSIGNED", 0, 4294967295),
    ("BIGINT UNSIGNED", 0, 18446744073709551615),
]


# ---------------------------------------------------------------------------
# The null bit, per width and per position
# ---------------------------------------------------------------------------


def test_a_null_is_distinct_from_its_types_zero_at_every_width(client, schema_name):
    """`v = 0` must admit the written zero and exclude the NULL, though the
    NULL's payload cell holds the same zero bytes. `IS NULL` and `IS NOT NULL`
    are asserted as exact complements, and `v > 0` rides alongside because the
    ordering load is the other genuinely per-width part of the read."""
    sn = schema_name
    cols = [f"v{i}" for i in range(len(_WIDTHS))]
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, " +
        ", ".join(f"{c} {sql}" for c, (sql, _, _) in zip(cols, _WIDTHS)) + ")",
        schema_name=sn)
    # One row per value class, every column at once. An unsigned minimum *is*
    # zero, so those two rows agree there and differ in every signed column.
    nulls = [None] * len(_WIDTHS)
    zeros = [0] * len(_WIDTHS)
    minima = [lo for _, lo, _ in _WIDTHS]
    maxima = [hi for _, _, hi in _WIDTHS]
    seed = [nulls, zeros, minima, maxima]
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(
            f"({i}, " + ", ".join("NULL" if v is None else str(v) for v in r) + ")"
            for i, r in enumerate(seed)),
        schema_name=sn)

    for j, (sql, _lo, _hi) in enumerate(_WIDTHS):
        col = [r[j] for r in seed]
        for pred, keep in (("= 0", lambda v: v == 0),
                           ("> 0", lambda v: v is not None and v > 0),
                           ("IS NULL", lambda v: v is None),
                           ("IS NOT NULL", lambda v: v is not None)):
            want = {(i,): 1 for i, v in enumerate(col) if keep(v)}
            assert bag(rows(client, sn,
                            f"SELECT pk FROM t WHERE {cols[j]} {pred}")) == want, \
                f"{sql} {pred}"


def test_the_null_bit_follows_payload_position_around_an_interleaved_key(
        client, schema_name):
    """The PK columns sit *between* the payload columns, so a bitmap index that
    subtracted the PK count rather than the PK columns *before* each one maps
    `p1` to a negative and `p3` past the end. Writing all eight NULL patterns at
    once shows a mis-indexed bit as a NULL that moved to a neighbouring column
    rather than as a missing row.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (p1 BIGINT, a BIGINT UNSIGNED NOT NULL, p2 BIGINT, "
        "b BIGINT UNSIGNED NOT NULL, p3 BIGINT, PRIMARY KEY (a, b))",
        schema_name=sn)
    _, schema = client.resolve_table(sn, "t")
    assert schema.pk_indices == [1, 3]

    # All eight NULL patterns over (p1, p2, p3), each under its own key.
    seed = []
    for i in range(8):
        vals = [None if i >> b & 1 else (b + 1) * 10 for b in range(3)]
        seed.append((vals[0], i, vals[1], i, vals[2]))
    client.execute_sql(
        "INSERT INTO t (p1, a, p2, b, p3) VALUES " +
        ", ".join("(" + ", ".join("NULL" if v is None else str(v) for v in r) + ")"
                  for r in seed),
        schema_name=sn)

    assert bag(scanned(client, sn, "t"), "p1", "a", "p2", "b", "p3") == \
        {r: 1 for r in seed}

    # A projection that reorders the payload rebuilds the bitmap, so the bits
    # must be re-indexed rather than copied across.
    client.execute_sql(
        "CREATE VIEW v AS SELECT b, a, p3, p2, p1 FROM t", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "b", "a", "p3", "p2", "p1") == \
        {(r[3], r[1], r[4], r[2], r[0]): 1 for r in seed}


# ---------------------------------------------------------------------------
# A NULL key equals nothing, including another NULL
# ---------------------------------------------------------------------------


@pytest.fixture
def lr(client, schema_name):
    """`l(id, fk)` and `r(k, rk, name)`, both join keys nullable, with a real
    `0` key present on the right.

    `0` is the value that matters: a NULL key reindexed to a synthetic key of
    zero would collide with it, which is a wrong *match* rather than a missing
    one — the failure a "NULL matched nothing" assertion alone cannot see.
    """
    client.execute_sql(
        "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT)",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, rk BIGINT, name BIGINT NOT NULL)",
        schema_name=schema_name)
    return schema_name


def test_a_null_key_matches_neither_a_zero_nor_another_null(client, lr):
    """Both directions at once: the left NULL must miss the right `0`, and two
    NULLs — one on each side — must miss each other. Only the real pair matches,
    at weight 1.

    The key here is `r.rk`, a plain column the exchange builds a key for; the
    left-join case below joins `r.k`, the key region itself — two routes to the
    same collision.
    """
    sn = lr
    client.execute_sql(
        "CREATE VIEW v AS SELECT l.id AS id, r.name AS name FROM l JOIN r ON l.fk = r.rk",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO r VALUES (100, 0, 900), (101, NULL, 901), (102, 7, 907)",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO l VALUES (1, NULL), (2, 0), (3, 7), (4, NULL)", schema_name=sn)

    # id 2 matches the real 0; id 3 matches 7. The two NULL left rows match
    # neither the real 0 nor the NULL right row.
    assert bag(scanned(client, sn, "v"), "id", "name") == {(2, 900): 1, (3, 907): 1}


def test_a_left_join_null_fills_a_null_key_once_beside_a_real_zero(client, lr):
    """The preserved row whose key is NULL and the one whose key is `0` reindex
    to the same synthetic key, so they are the pair that would merge or cancel if
    the null were not carried separately. Each must appear exactly once — the
    `0` row matched, the NULL row filled — and the weights are the assertion,
    because a double-emit and a correct emit hold the same rows."""
    sn = lr
    client.execute_sql(
        "CREATE VIEW v AS SELECT l.id AS id, r.name AS name "
        "FROM l LEFT JOIN r ON l.fk = r.k", schema_name=sn)
    client.execute_sql("INSERT INTO r VALUES (0, NULL, 900)", schema_name=sn)
    client.execute_sql("INSERT INTO l VALUES (1, NULL), (2, 0)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "name") == {(1, None): 1, (2, 900): 1}


@pytest.mark.parametrize("kind", ["JOIN", "LEFT JOIN"])
def test_a_key_crossing_the_null_boundary_retracts_and_readmits_exactly(
        client, lr, kind):
    """A key toggling NULL ↔ real moves the row between the match branch and the
    bypass branch, which must be exact complements at every tick. A stale verdict
    leaves the old row at weight 1 beside the new one.

    Going back to NULL is a delete plus an insert because `SET col = NULL` is not
    expressible — orthogonal to the key path under test.
    """
    sn = lr
    client.execute_sql(
        f"CREATE VIEW v AS SELECT l.id AS id, r.name AS name "
        f"FROM l {kind} r ON l.fk = r.k", schema_name=sn)
    client.execute_sql("INSERT INTO r VALUES (5, NULL, 905)", schema_name=sn)
    unmatched = {(1, None): 1} if kind == "LEFT JOIN" else {}

    client.execute_sql("INSERT INTO l VALUES (1, NULL)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id", "name") == unmatched

    client.execute_sql("UPDATE l SET fk = 5 WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id", "name") == {(1, 905): 1}

    client.execute_sql("DELETE FROM l WHERE id = 1", schema_name=sn)
    client.execute_sql("INSERT INTO l VALUES (1, NULL)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id", "name") == unmatched

    client.execute_sql("DELETE FROM l WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id", "name") == {}


def test_a_null_string_key_matches_neither_the_empty_string_nor_another_null(
        client, schema_name):
    """A string key routes by the hash of its content, and the empty string
    hashes what a NULL's absent content would. They must stay distinct — the
    string counterpart of the NULL-versus-zero collision above."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, sk TEXT)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, rk TEXT, name BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT l.id AS id, r.name AS name FROM l JOIN r ON l.sk = r.rk",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO r VALUES (1, '', 900), (2, 'x', 901), (3, NULL, 902)",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO l VALUES (1, NULL), (2, ''), (3, 'x')", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "name") == {(2, 900): 1, (3, 901): 1}
