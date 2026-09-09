"""A secondary index as an access structure: creating one, dropping one, and the
composite and PK-column forms.

An index changes which walk the planner picks, never the answer, so the DDL
here is paired with an `EXPLAIN` of the access path it opens. What an index
does to a *read* is in `read_verb/test_index_reads.py`; what UNIQUE refuses is
in `admissibility/`.
"""

import gnitz
import pytest
from _read import access, bag, rows


def _idx_rows(client, tid):
    """The live `IDX_TAB` rows owned by `tid`, as `(source_cols, flags)`."""
    batch = client.scan(gnitz.IDX_TAB)
    assert batch.schema is not None
    return [(gnitz.unpack_pk_cols(sc), f)
            for w, o, sc, f in zip(batch.weights, batch.scalars("owner_id"),
                                   batch.scalars("source_col_idx"), batch.scalars("flags"))
            if w > 0 and o == tid]


# ---------------------------------------------------------------------------
# The catalog row an index DDL writes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("unique", [False, True])
def test_create_index_writes_its_catalog_row(client, schema_name, unique):
    """`CREATE INDEX` answers with the id it allocated and leaves one `IDX_TAB`
    row naming the owner, the packed source-column list, and — for UNIQUE — bit 0
    of `flags`."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, cust_id BIGINT NOT NULL)",
        schema_name=sn)
    res = client.execute_sql(
        f"CREATE {'UNIQUE ' if unique else ''}INDEX ON t(cust_id)", schema_name=sn)
    assert len(res) == 1 and res[0]["type"] == "IndexCreated"
    assert res[0]["index_id"] > 0

    tid, _ = client.resolve_table(sn, "t")
    assert _idx_rows(client, tid) == [([1], 1 if unique else 0)]


def test_drop_index_retracts_its_catalog_row(client, schema_name):
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
    tid, _ = client.resolve_table(sn, "t")
    assert _idx_rows(client, tid)

    res = client.execute_sql(f"DROP INDEX {sn}__t__idx_val", schema_name=sn)
    assert res[0]["type"] == "Dropped"
    assert _idx_rows(client, tid) == []


def test_index_ids_are_allocated_monotonically(client, schema_name):
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        "b BIGINT NOT NULL)", schema_name=sn)
    id1 = client.execute_sql("CREATE INDEX ON t(a)", schema_name=sn)[0]["index_id"]
    id2 = client.execute_sql("CREATE INDEX ON t(b)", schema_name=sn)[0]["index_id"]
    assert id2 > id1


@pytest.mark.parametrize("coltype", ["FLOAT", "DOUBLE", "VARCHAR", "TEXT"])
def test_an_index_column_must_be_a_fixed_width_integer(client, schema_name, coltype):
    """IEEE-754 breaks the byte-equal key contract and a string has no fixed
    width, so neither can carry the order-preserving key an index walk needs."""
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, c {coltype} NOT NULL)",
        schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("CREATE INDEX ON t(c)", schema_name=sn)


# ---------------------------------------------------------------------------
# Composite indexes
# ---------------------------------------------------------------------------


@pytest.fixture
def abc(client, schema_name):
    """`t (pk, a, b, c)` with three rows, `b` NOT NULL."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        "b BIGINT NOT NULL, c BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (10, 1, 100, 7), (20, 1, 200, 8), (30, 2, 100, 9)",
        schema_name=sn)
    return sn


@pytest.mark.parametrize("where,want", [
    ("a = 1 AND b = 200", [20]),
    # Out-of-order WHERE binds each value to the index's declared column, not to
    # AST order.
    ("b = 200 AND a = 1", [20]),
    # A leading prefix over (a, b) with b NOT NULL is served by the index.
    ("a = 1", [10, 20]),
])
def test_a_composite_index_serves_its_key_and_its_leading_prefix(client, abc, where, want):
    client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=abc)
    assert bag(rows(client, abc, f"SELECT pk FROM t WHERE {where}")) == {(pk,): 1 for pk in want}


def test_the_tiebreak_prefers_the_tighter_index(client, abc):
    """Both indexes pin (a, b) to the same point, so the tie falls to arity: the
    narrower (a, b) walk wins, and it returns the right row."""
    client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=abc)
    client.execute_sql("CREATE INDEX ON t(a, b, c)", schema_name=abc)
    q = "SELECT pk FROM t WHERE a = 1 AND b = 200"
    assert access(client, abc, q) == (
        "access: index range on (a, b) — may be traded for a full scan "
        "on low selectivity")
    assert bag(rows(client, abc, q)) == {(20,): 1}


def test_a_nullable_trailing_prefix_is_not_served_by_the_index(client, schema_name):
    """A leading-prefix WHERE over (a, b) with `b` NULLABLE would silently drop
    the (1, NULL) row, so the index must not serve it — the on-demand executor
    scans the base instead and DOES return that row. This asserts the property
    the old "raises a clean non-indexed error" check was only a proxy for: pk=40
    must not go missing."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        "b BIGINT, c BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (10, 1, 100, 7), (20, 1, 200, 8), (30, 2, 100, 9), "
        "(40, 1, NULL, 11)", schema_name=sn)
    client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=sn)

    assert bag(rows(client, sn, "SELECT pk FROM t WHERE a = 1")) == \
        {(10,): 1, (20,): 1, (40,): 1}
    # The full key still uses the index and finds the non-null row.
    assert bag(rows(client, sn, "SELECT pk FROM t WHERE a = 1 AND b = 200")) == {(20,): 1}


def test_a_non_equality_on_the_consumed_column_stays_a_residual(client, abc):
    """`a = 1 AND a > 5` consumes the equality via the index and keeps `a > 5` as
    a residual — the residual is excluded by the consumed conjunct's physical
    index, not by column, so `a > 5` survives and (since a=1) matches no row."""
    client.execute_sql("CREATE INDEX ON t(a)", schema_name=abc)
    assert rows(client, abc, "SELECT pk FROM t WHERE a = 1 AND a > 5") == []


def test_drop_index_matches_the_exact_column_list(client, abc):
    """Dropping the composite (a, b) leaves the single-column (a) index serving."""
    client.execute_sql("CREATE INDEX ON t(a)", schema_name=abc)
    client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=abc)
    client.execute_sql(f"DROP INDEX {abc}__t__idx_a_b", schema_name=abc)
    assert bag(rows(client, abc, "SELECT pk FROM t WHERE a = 1")) == {(10,): 1, (20,): 1}


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
    return sn


def test_an_index_over_a_trailing_pk_column_serves_the_where(client, pk_indexed):
    """A secondary index may name a PK column, and the planner treats it as any
    other index column."""
    sn = pk_indexed
    client.execute_sql(
        "INSERT INTO t VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300)", schema_name=sn)
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
    client.execute_sql(
        f"INSERT INTO t VALUES (1, {_U64_MAX}, 100), (2, 5, 200)", schema_name=sn)
    client.execute_sql("CREATE INDEX ON t(b)", schema_name=sn)

    q = f"SELECT * FROM t WHERE b = {_U64_MAX}"
    assert access(client, sn, q) == "access: index range on (b) — exact walk, never traded"
    assert bag(rows(client, sn, q)) == {(1, _U64_MAX, 100): 1}
