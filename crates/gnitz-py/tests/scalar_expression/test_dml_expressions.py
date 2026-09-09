"""DML expressions run through the one shared evaluator.

UPDATE/DELETE residual predicates and SET right-hand sides compile to the same
program a `CREATE VIEW` WHERE compiles to, and are evaluated by the same VM. This
file pins what that buys (string, float, `IS NOT NULL` and unsigned residuals
that no client interpreter could serve) and what it costs (the VM's 64-register
cap, now reachable from DML).
"""

import pytest
import gnitz
from _caps import first_rejected

U64_HIGH = 2**63 + 5  # crosses the signed boundary: negative as an i64




def _scan_map(client, tid):
    """{pk: row} over the positive-weight rows."""
    return {row.pk: row for row in client.scan(tid)}


def _table(client, sn, ddl, rows=()):
    client.execute_sql(ddl, schema_name=sn)
    tid, _ = client.resolve_table(sn, "t")
    for values in rows:
        client.execute_sql(f"INSERT INTO t VALUES {values}", schema_name=sn)
    return tid


# ---------------------------------------------------------------------------
# Newly servable residual shapes
# ---------------------------------------------------------------------------


def test_update_with_string_residual(client, schema_name):
    """`WHERE pk = k AND strcol = 'x'` — a string conjunct in a residual, which
    the deleted int-only interpreter rejected outright."""
    sn = schema_name
    tid = _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, v BIGINT NOT NULL)",
        [("(1, 'alpha', 10)"), ("(2, 'beta', 20)")],
    )
    res = client.execute_sql(
        "UPDATE t SET v = 99 WHERE pk = 1 AND s = 'alpha'", schema_name=sn
    )
    assert res[0]["count"] == 1
    rows = _scan_map(client, tid)
    assert rows[1].v == 99 and rows[2].v == 20

    # A non-matching string conjunct touches nothing.
    res = client.execute_sql(
        "UPDATE t SET v = 0 WHERE pk = 2 AND s = 'alpha'", schema_name=sn
    )
    assert res[0]["count"] == 0
    assert _scan_map(client, tid)[2].v == 20


def test_delete_with_float_residual(client, schema_name):
    """A float column in a residual predicate — every float was rejected before."""
    sn = schema_name
    tid = _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
        [("(1, 0.5)"), ("(2, 1.5)"), ("(3, 2.5)")],
    )
    res = client.execute_sql("DELETE FROM t WHERE f > 1.0", schema_name=sn)
    assert res[0]["count"] == 2
    assert list(_scan_map(client, tid).keys()) == [1]


def test_delete_is_not_null_on_non_nullable_column_deletes_every_row(
    client, schema_name
):
    """The binder const-folds `IS NOT NULL` on a NOT NULL column to a true
    literal, which the compiler reports as "no filter" — every row must still be
    deleted, not zero."""
    sn = schema_name
    tid = _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        [("(1, 10)"), ("(2, 20)"), ("(3, 30)")],
    )
    res = client.execute_sql("DELETE FROM t WHERE v IS NOT NULL", schema_name=sn)
    assert res[0]["count"] == 3
    assert _scan_map(client, tid) == {}


def test_residual_div_mod_by_zero_filters_like_a_view(client, schema_name):
    """A zero divisor is SQL NULL, so the row is excluded — the engine VM's own
    `div_like` rule, which the residual now runs verbatim."""
    sn = schema_name
    tid = _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, d BIGINT NOT NULL, v BIGINT NOT NULL)",
        [("(1, 0, 10)"), ("(2, 2, 20)"), ("(3, 0, 30)")],
    )
    client.execute_sql(
        "CREATE VIEW vw AS SELECT pk, v FROM t WHERE 100 / d > 1", schema_name=sn
    )
    view_rows = client.execute_sql("SELECT pk FROM vw", schema_name=sn)[0]["rows"]
    view_pks = sorted(r.pk for r in view_rows)

    res = client.execute_sql("DELETE FROM t WHERE 100 / d > 1", schema_name=sn)
    assert res[0]["count"] == 1
    survivors = sorted(_scan_map(client, tid).keys())
    assert view_pks == [2], "the view keeps only the non-zero divisor"
    assert survivors == [1, 3], "the residual must agree with the view"
    client.execute_sql("DROP VIEW vw", schema_name=sn)


def test_unsigned_64bit_column_compares_and_divides_as_unsigned(client, schema_name):
    """A `BIGINT UNSIGNED` value past 2^63 has a negative i64 bit pattern. The
    compiled path tracks per-register U64-ness, so an ordered comparison and an
    integer division both take the unsigned arm — DML now agrees with a view."""
    sn = schema_name
    tid = _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "u BIGINT UNSIGNED NOT NULL, v BIGINT NOT NULL)",
        [(f"(1, {U64_HIGH}, 0)"), ("(2, 7, 0)")],
    )
    # Residual: `u > 100` is TRUE for 2^63+5 under unsigned comparison.
    res = client.execute_sql("UPDATE t SET v = 1 WHERE u > 100", schema_name=sn)
    assert res[0]["count"] == 1
    rows = _scan_map(client, tid)
    assert rows[1].v == 1 and rows[2].v == 0

    # SET: `u / 2` is the unsigned quotient, not the signed one.
    client.execute_sql("UPDATE t SET v = u / 2 WHERE pk = 1", schema_name=sn)
    assert _scan_map(client, tid)[1].v == U64_HIGH // 2


# ---------------------------------------------------------------------------
# SET right-hand sides
# ---------------------------------------------------------------------------


def test_set_numeric_over_a_nullable_column(client, schema_name):
    """A nullable source resolves the program with nullability on: a NULL source
    must write NULL, not the filler zeros read back as a real 0."""
    sn = schema_name
    tid = _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT)",
        [("(1, 0, 5)"), ("(2, 0, NULL)")],
    )
    res = client.execute_sql("UPDATE t SET a = b + 1", schema_name=sn)
    assert res[0]["count"] == 2
    rows = _scan_map(client, tid)
    assert rows[1].a == 6
    assert rows[2].a is None, "NULL + 1 must stay NULL"


def test_set_reads_the_pk_column(client, schema_name):
    """`SET v = pk + 1` — the PK region read through the client adapter, which no
    other SET test exercises."""
    sn = schema_name
    tid = _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        [("(41, 0)"), ("(7, 0)")],
    )
    client.execute_sql("UPDATE t SET v = pk + 1", schema_name=sn)
    rows = _scan_map(client, tid)
    assert rows[41].v == 42 and rows[7].v == 8


def test_set_int_column_from_float_column_rejects(client, schema_name):
    """A float-valued RHS into an integer column errors rather than storing the
    raw f64 bit pattern."""
    sn = schema_name
    _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, n BIGINT NOT NULL, f DOUBLE NOT NULL)",
        [("(1, 0, 1.5)")],
    )
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("UPDATE t SET n = f", schema_name=sn)


# ---------------------------------------------------------------------------
# Signed VALUES cells
# ---------------------------------------------------------------------------


def test_signed_null_writes_null_not_zero(client, schema_name):
    """`+NULL` is the NULL it spells. The sign used to hide the literal from the
    null check while the writer saw through it, so the cell was written zero with
    the null bit left clear — read back as a real 0."""
    sn = schema_name
    tid = _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT)",
        [("(1, +NULL)"), ("(2, -NULL)"), ("(3, NULL)")],
    )
    rows = _scan_map(client, tid)
    for pk in (1, 2, 3):
        assert rows[pk].v is None, f"pk {pk} must read back NULL, not 0"


def test_signed_null_into_a_not_null_column_is_rejected(client, schema_name):
    """The same defect skipped the NOT NULL check entirely."""
    sn = schema_name
    _table(client, sn, "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("INSERT INTO t VALUES (1, +NULL)", schema_name=sn)


def test_a_parenthesised_or_plus_signed_cell_is_a_constant(client, schema_name):
    """One decoder serves every constant position, so a PK slot and a payload
    slot accept the same spellings."""
    sn = schema_name
    tid = _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        [("(+1, (2))"), ("((3), +4)")],
    )
    rows = _scan_map(client, tid)
    assert rows[1].v == 2 and rows[3].v == 4


@pytest.mark.parametrize("cell", ["-'abc'", "+'abc'"])
def test_a_signed_string_cell_is_rejected(client, schema_name, cell):
    """Either sign over a string used to write `abc`, discarded silently."""
    sn = schema_name
    _table(client, sn, "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)")
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(f"INSERT INTO t VALUES (1, {cell})", schema_name=sn)


# ---------------------------------------------------------------------------
# ON CONFLICT DO UPDATE
# ---------------------------------------------------------------------------


def test_do_update_set_string_from_excluded(client, schema_name):
    """`SET s = EXCLUDED.s` on a STRING column: the string classifier path, which
    the existing conflict tests (all integer columns) never reach."""
    sn = schema_name
    tid = _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
        [("(1, 'old')")],
    )
    client.execute_sql(
        "INSERT INTO t VALUES (1, 'new'), (2, 'fresh') "
        "ON CONFLICT (pk) DO UPDATE SET s = EXCLUDED.s",
        schema_name=sn,
    )
    rows = _scan_map(client, tid)
    assert rows[1].s == "new" and rows[2].s == "fresh"


def test_do_update_with_null_into_not_null_column_errors(client, schema_name):
    """A NULL in the incoming VALUES under a NOT NULL column must be rejected
    before the merge reads it. The merged batch is the only thing this plan
    pushes, so an unvalidated incoming NULL would be read back as a real 0 and
    silently committed."""
    sn = schema_name
    tid = _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        [("(1, 10)")],
    )
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(
            "INSERT INTO t VALUES (1, NULL) ON CONFLICT (pk) DO UPDATE SET v = EXCLUDED.v",
            schema_name=sn,
        )
    assert _scan_map(client, tid)[1].v == 10, "no silent 0 committed"


# ---------------------------------------------------------------------------
# The VM's 64-register cap, now reachable from DML
# ---------------------------------------------------------------------------


def test_residual_register_cap(client, schema_name):
    """A residual past the register cap is rejected naming the limit, and every
    shorter one is served. The builder folds identical instructions, so the
    conjuncts share nothing: `v * k < k + 1` over distinct odd `k`."""
    sn = schema_name
    _table(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, s TEXT NOT NULL)",
        [("(1, 0, 'a')")],
    )

    def update(k):
        conjuncts = " AND ".join(f"v * {2 * i + 1} < {2 * i + 2}" for i in range(k))
        res = client.execute_sql(f"UPDATE t SET v = 1 WHERE {conjuncts}", schema_name=sn)
        assert res[0]["count"] == 1, f"k={k}: the row satisfies every conjunct"

    first_rejected(update, range(1, 64))

    def delete_in(n):
        items = ", ".join(f"'x{i}'" for i in range(n))
        client.execute_sql(f"DELETE FROM t WHERE s IN ({items})", schema_name=sn)

    first_rejected(delete_in, range(1, 64))
