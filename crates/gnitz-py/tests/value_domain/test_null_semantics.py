"""NULL as a value: which bit records it, and what it refuses to equal.

A NULL payload cell is **zero-filled and the null bitmap is the sole truth**, so
a test only proves something read that bitmap if the zero would have answered
differently. That is why every case here pairs a NULL against the type's own zero
— the integer `0`, the empty string, the epoch date — rather than against an
arbitrary value: a predicate over a NULL that a zero would also fail proves
nothing.

The bitmap is indexed by *payload* position, which is the column's index minus
the number of PK columns before it. For a PK declared as a prefix that is the
same as "minus the PK count", so a prefix table cannot tell a correct
implementation from one that subtracts a constant. Only a key interleaved with
the payload separates them.

The connectives themselves are one per-row program whatever the column type and
are stated once in `scalar_expression/test_three_valued_logic.py`. What varies
here is the *type*: each width has its own load and its own bit, and a
width-specific path could obey the rule for BIGINT and not for TINYINT.
"""

import pytest
from _read import bag, scanned

# (SQL spelling, the type's zero, a value above it). The unsigned zeros are the
# minimum of their range, so `> 0` and `= 0` are the two useful cuts on both.
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

_IDS = ["i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64"]


# ---------------------------------------------------------------------------
# The null bit, per width and per position
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("col_sql,lo,hi", _WIDTHS, ids=_IDS)
def test_a_null_is_distinct_from_its_types_zero_at_every_width(
        client, schema_name, col_sql, lo, hi):
    """`v = 0` must admit the written zero and exclude the NULL, though the
    NULL's payload cell holds the same zero bytes. `v IS NULL` and `v IS NOT
    NULL` are then asserted as exact complements, so a bitmap read that was
    merely inverted fails too."""
    sn = schema_name
    # An unsigned type's minimum *is* zero, so the written values are deduped —
    # two rows at the same value would make the `v = 0` assertion below ambiguous.
    written = [0] + [v for v in (lo, hi) if v != 0]
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v {col_sql})",
        schema_name=sn)
    client.execute_sql("CREATE VIEW zero AS SELECT pk FROM t WHERE v = 0",
                       schema_name=sn)
    client.execute_sql("CREATE VIEW isnull AS SELECT pk FROM t WHERE v IS NULL",
                       schema_name=sn)
    client.execute_sql("CREATE VIEW notnull AS SELECT pk FROM t WHERE v IS NOT NULL",
                       schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (0, NULL), " +
        ", ".join(f"({i}, {v})" for i, v in enumerate(written, start=1)),
        schema_name=sn)

    assert bag(scanned(client, sn, "zero"), "pk") == {(1,): 1}
    assert bag(scanned(client, sn, "isnull"), "pk") == {(0,): 1}
    assert bag(scanned(client, sn, "notnull"), "pk") == \
        {(i,): 1 for i in range(1, len(written) + 1)}


@pytest.mark.parametrize("col_sql,_lo,hi", _WIDTHS, ids=_IDS)
def test_the_connectives_stay_three_valued_at_every_width(
        client, schema_name, col_sql, _lo, hi):
    """`NOT` is what separates unknown from FALSE: both fail to admit a row under
    OR and under AND, so only the negation tells them apart — `NOT FALSE` is TRUE
    and `NOT unknown` is still unknown. A zero-filled NULL read without the
    bitmap would be FALSE, and would therefore be admitted by the `NOT` view.

    The threshold is 0 rather than the type's minimum, because a minimum reached
    through a negated literal is one past what the register file holds for the
    widest signed type and is refused as a comparison outright.
    """
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a {col_sql}, b {col_sql})",
        schema_name=sn)
    for name, pred in (("ored", "a > 0 OR b > 0"), ("anded", "a > 0 AND b > 0"),
                       ("negated", "NOT (a > 0)")):
        client.execute_sql(f"CREATE VIEW {name} AS SELECT pk FROM t WHERE {pred}",
                           schema_name=sn)
    # 1: NULL/TRUE  2: TRUE/NULL  3: NULL/FALSE  4: FALSE/NULL  5: TRUE/TRUE
    # 6: FALSE/FALSE
    client.execute_sql(
        f"INSERT INTO t VALUES (1, NULL, {hi}), (2, {hi}, NULL), (3, NULL, 0), "
        f"(4, 0, NULL), (5, {hi}, {hi}), (6, 0, 0)", schema_name=sn)

    # OR is TRUE wherever one side definitely is, whatever the other is.
    assert bag(scanned(client, sn, "ored"), "pk") == {(1,): 1, (2,): 1, (5,): 1}
    # AND needs both definite, so an unknown operand withholds the row.
    assert bag(scanned(client, sn, "anded"), "pk") == {(5,): 1}
    # The disconfirming read: only the rows whose `a` is definitely not above 0.
    assert bag(scanned(client, sn, "negated"), "pk") == {(4,): 1, (6,): 1}


def test_the_null_bit_follows_payload_position_around_an_interleaved_key(
        client, schema_name):
    """The PK columns sit *between* the payload columns here, so the payload
    positions are 0, 1, 2 for columns 0, 2, 4. Subtracting the PK count instead
    of the number of PK columns *before* each one would map them to 0, 1, 2 only
    by accident of arity — and mapping `p1` to a negative or `p3` past the end is
    what a constant subtraction produces on this shape.

    Every NULL pattern across the three payload columns is written at once, so a
    bit written at the wrong index shows up as a NULL that moved to a
    neighbouring column rather than as a missing row.
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
    at weight 1."""
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


# ---------------------------------------------------------------------------
# A narrow payload compares at its declared width
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("col_sql,lo,hi", _WIDTHS, ids=_IDS)
def test_a_narrow_payload_retracts_against_its_own_width(
        client, schema_name, col_sql, lo, hi):
    """A retraction is matched on (key, payload), so the payload comparison must
    read the column at its declared width. Reading a 1- or 2-byte column as 8
    bytes takes the neighbouring bytes with it, and the retraction then matches
    nothing — leaving the old row behind at weight 1, which is what the weights
    catch and a row set does not.

    The extremes are used because that is where a too-wide read picks up bits
    that a mid-range value leaves zero.
    """
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v {col_sql} NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE VIEW dist AS SELECT * FROM t", schema_name=sn)
    client.execute_sql(
        f"INSERT INTO t VALUES (1, {lo}), (2, {hi})", schema_name=sn)
    assert bag(scanned(client, sn, "dist"), "pk", "v") == {(1, lo): 1, (2, hi): 1}

    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
    assert bag(scanned(client, sn, "dist"), "pk", "v") == {(2, hi): 1}

    # An UPDATE is a retraction of the old payload plus an insert of the new, so
    # it exercises the same comparison and must leave exactly one row.
    client.execute_sql(f"UPDATE t SET v = {lo} WHERE pk = 2", schema_name=sn)
    assert bag(scanned(client, sn, "dist"), "pk", "v") == {(2, lo): 1}
