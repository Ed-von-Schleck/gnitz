"""One program, every surface that compiles one.

A scalar expression is lowered once and run by one evaluator, so a view
projection, a view WHERE, an ad-hoc SELECT, an ad-hoc WHERE, a GROUP BY key, a
DML residual and a SET right-hand side cannot disagree about what an expression
means. That is the fact this file exists for; what an individual operator
*computes* belongs to the per-class files beside it.

The surfaces are not one code path reached seven ways: a view filter is compiled
into a maintained circuit and evaluated per worker partition, an ad-hoc read
compiles a residual against a scan, and DML compiles a program the client ships
as a blob. Agreement across them is a property only a running cluster answers.
"""

import pytest
import gnitz
from _read import bag, rows, scanned

U64_HIGH = 2**63 + 5  # past the signed boundary: a negative i64 bit pattern
U64_MAX = 2**64 - 1

# One row per interesting sign and width, by pk. `s` is the one nullable column,
# so a NULL anywhere else is the expression's own.
_ROWS = {1: "(1, -9, 'alpha', 0.5, 5)", 2: f"(2, 3, 'beta', 1.5, {U64_HIGH})",
         3: "(3, -1, '100%', 2.7, 9)", 4: "(4, 0, NULL, 1e300, 0)"}


@pytest.fixture
def surf(client, schema_name):
    """`t (pk, i, s, f, u)` holding `_ROWS`."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL, "
        "s TEXT, f DOUBLE NOT NULL, u BIGINT UNSIGNED NOT NULL)", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES " + ", ".join(_ROWS.values()),
                       schema_name=schema_name)
    return schema_name


# Each predicate against the pks it selects. Every operand class is here: an
# integer-only interpreter could serve none of the string, float or unsigned
# ones.
_PREDICATES = {
    "ABS(i) > 5": {1},
    # pk 3 divides by zero: NULL, which a predicate does not keep.
    "100 / (i + 1) > 1": {2, 4},
    # The binder const-folds a null test on a NOT NULL column to a true literal,
    # which the compiler reports as "no filter" — every row must still go.
    "i IS NOT NULL": {1, 2, 3, 4},
    "f > 1.0": {2, 3, 4},
    # A failed conversion is an ordinary NULL: 1e300 has no INT image, and
    # U64_HIGH no SMALLINT one, so neither row passes either spelling.
    "CAST(f AS INT) = 2": {3},
    "CAST(u AS SMALLINT) <> 7": {1, 3, 4},
    # U64_HIGH reads negative under a signed compare.
    "u > 100": {2},
    "s = 'alpha' AND pk = 1": {1},
    "UPPER(s) = 'BETA'": {2},
    "s < 'b'": {1, 3},
    "s ILIKE 'A%'": {1},
    "s NOT LIKE 'al%'": {2, 3},
    r"s LIKE '100\%'": {3},
    "s IN ('beta', 'x')": {2},
    "s NOT IN ('beta', 'x')": {1, 3},
}


def test_every_surface_selects_the_same_rows(client, surf):
    """For each predicate: what the maintained filter holds, the ad-hoc WHERE
    returns and the DELETE residual removes — after which the view retracts
    exactly those rows, and re-admits them when they return."""
    sn = surf
    views = {p: f"v{n}" for n, p in enumerate(_PREDICATES)}
    for p, v in views.items():
        client.execute_sql(f"CREATE VIEW {v} AS SELECT pk FROM t WHERE {p}", schema_name=sn)

    for p, want in _PREDICATES.items():
        selected = {(k,): 1 for k in want}
        assert bag(scanned(client, sn, views[p])) == selected, p
        assert bag(rows(client, sn, f"SELECT pk FROM t WHERE {p}")) == selected, p

        client.execute_sql(f"DELETE FROM t WHERE {p}", schema_name=sn)
        assert bag(scanned(client, sn, "t"), "pk") == {(k,): 1 for k in _ROWS.keys() - want}, p
        assert bag(scanned(client, sn, views[p])) == {}, p
        client.execute_sql("INSERT INTO t VALUES " + ", ".join(_ROWS[k] for k in sorted(want)),
                           schema_name=sn)


def test_a_value_and_a_group_key_evaluate_alike_across_a_retraction(client, surf):
    """The same expressions as a maintained projection and an ad-hoc one, and as
    a written group key maintained and ad hoc. A retraction under a computed
    group key has to leave the old group as well as join the new one; a stale
    group survives at weight 1 and a row count over the groups cannot see it."""
    sn = surf
    proj = "SELECT pk, ABS(i) AS a, LEFT(s, 1) AS k, f * 2 AS f2, u / 2 AS h FROM t"
    grouped = "SELECT LEFT(s, 1) AS k, COUNT(*) AS c, SUM(i) AS si FROM t GROUP BY LEFT(s, 1)"
    client.execute_sql(f"CREATE VIEW vp AS {proj}", schema_name=sn)
    client.execute_sql(f"CREATE VIEW vg AS {grouped}", schema_name=sn)

    unchanged = {(3, 1, "1", 5.4, 4): 1, (4, 0, None, 2e300, 0): 1}
    assert bag(scanned(client, sn, "vp")) == bag(rows(client, sn, proj)) == {
        (1, 9, "a", 1.0, 2): 1, (2, 3, "b", 3.0, U64_HIGH // 2): 1, **unchanged}
    assert bag(scanned(client, sn, "vg")) == bag(rows(client, sn, grouped)) == {
        ("a", 1, -9): 1, ("b", 1, 3): 1, ("1", 1, -1): 1, (None, 1, 0): 1}

    client.execute_sql("UPDATE t SET s = 'gamma', i = 9 WHERE pk = 1", schema_name=sn)
    client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
    assert bag(scanned(client, sn, "vp")) == bag(rows(client, sn, proj)) == {
        (1, 9, "g", 1.0, 2): 1, **unchanged}
    assert bag(scanned(client, sn, "vg")) == bag(rows(client, sn, grouped)) == {
        ("g", 1, 9): 1, ("1", 1, -1): 1, (None, 1, 0): 1}


def test_a_set_right_hand_side_computes_over_every_column_it_can_read(client, schema_name):
    """SET compiles the same program with the target column's class as its sink.
    A nullable source resolves it with nullability on, so a NULL source writes
    NULL rather than the filler zeros read back as a real 0; the PK region is
    readable through the client adapter like any other column; and an unsigned
    operand divides in the unsigned domain."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, "
        "s TEXT NOT NULL, u BIGINT UNSIGNED NOT NULL)", schema_name=sn)
    client.execute_sql(f"INSERT INTO t VALUES (41, NULL, '  pad  ', {U64_HIGH}), "
                       "(7, 5, 'keep', 4)", schema_name=sn)

    client.execute_sql("UPDATE t SET a = a + pk, s = TRIM(s), u = u / 2", schema_name=sn)
    assert bag(scanned(client, sn, "t")) == {
        (41, None, "pad", U64_HIGH // 2): 1,   # NULL + 41 stays NULL, not 41
        (7, 12, "keep", 2): 1,
    }


def test_a_wide_literal_is_checked_against_its_target_before_any_row(client, schema_name):
    """An integer past `i64` has no register slot, so a residual naming one
    cannot be compiled at all, and an assignment out of the target's range is
    refused at plan time — even when the statement would touch no row, since
    deferring the check would make it succeed silently on an empty match and
    wrap two's-complement on a non-empty one. Nothing is written: no wrapped
    value, and no row inserted by the upsert.

    SET parses its literal against the *target's* type rather than against the
    register file, so the top of the unsigned range is writable on both
    assignment surfaces."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
        "v BIGINT UNSIGNED NOT NULL)", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=sn)

    for stmt in (f"DELETE FROM t WHERE v = {U64_MAX}",
                 f"UPDATE t SET v = {U64_MAX + 1} WHERE pk = 999",
                 "UPDATE t SET v = -1 WHERE pk = 999",
                 f"INSERT INTO t VALUES (2, 1) ON CONFLICT (pk) DO UPDATE SET v = {U64_MAX + 1}"):
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(stmt, schema_name=sn)
    assert bag(scanned(client, sn, "t")) == {(1, 100): 1}

    client.execute_sql(
        f"INSERT INTO t VALUES (1, 1) ON CONFLICT (pk) DO UPDATE SET v = {U64_MAX}",
        schema_name=sn)
    assert bag(scanned(client, sn, "t")) == {(1, U64_MAX): 1}
    client.execute_sql(f"UPDATE t SET v = {U64_MAX - 1}", schema_name=sn)
    assert bag(scanned(client, sn, "t")) == {(1, U64_MAX - 1): 1}
