"""Which SQL spellings name a type, what each stores as, and which of them a key
may be made of.

One table drives the whole file. A spelling is a `TypeCode` plus a verdict on
whether it is PK-eligible, and the engine derives everything else — stride,
routing, comparison — from those two facts, so stating them once per spelling is
the whole catalog contract. `is_pk_eligible` is the integer scalars and nothing
else: IEEE-754 breaks the byte-equal key contract (±0.0 differ byte-wise but
compare equal, NaN has no canonical bit pattern) and a variable-length string has
no fixed stride to compare at.

DATE, TIMESTAMP and DECIMAL are PK-eligible because each stores as a *signed*
integer — I32, I64 and a scaled I64 — which is why their keys sign-flip like any
other signed column rather than being a category of their own.
"""

import pytest
import gnitz
from _read import bag, scanned

TC = gnitz.TypeCode

# (SQL spelling, stored TypeCode, may be a PK column). Every spelling the
# planner accepts appears exactly once; the aliases sit beside what they alias
# so a spelling that silently stopped aliasing is visible as a changed row.
_SPELLINGS = [
    ("TINYINT", TC.I8, True),
    ("SMALLINT", TC.I16, True),
    ("INT", TC.I32, True),
    ("INTEGER", TC.I32, True),
    ("BIGINT", TC.I64, True),
    ("TINYINT UNSIGNED", TC.U8, True),
    ("SMALLINT UNSIGNED", TC.U16, True),
    ("INT UNSIGNED", TC.U32, True),
    ("BIGINT UNSIGNED", TC.U64, True),
    # FLOAT is the single-precision spelling; REAL and DOUBLE PRECISION are both
    # double, so REAL is *not* F32 despite the SQL-standard reading.
    ("FLOAT", TC.F32, False),
    ("DOUBLE", TC.F64, False),
    ("DOUBLE PRECISION", TC.F64, False),
    ("REAL", TC.F64, False),
    ("VARCHAR(255)", TC.STRING, False),
    ("TEXT", TC.STRING, False),
    ("CHAR(10)", TC.STRING, False),
    # Precision past 18 digits cannot be a scaled I64, so it widens to U128 —
    # which is also why a scale is refused there (see the rejections below).
    ("DECIMAL(38,0)", TC.U128, True),
    ("DECIMAL(39,0)", TC.U128, True),
    ("NUMERIC(38,0)", TC.U128, True),
    ("DECIMAL(10,2)", TC.DECIMAL, True),
    ("UUID", TC.UUID, True),
    ("DATE", TC.DATE, True),
    ("TIMESTAMP", TC.TIMESTAMP, True),
]

_IDS = [s.replace(" ", "_").replace("(", "").replace(")", "").replace(",", "_")
        for s, _, _ in _SPELLINGS]


@pytest.mark.parametrize("sql,tc,_pk_ok", _SPELLINGS, ids=_IDS)
def test_a_spelling_stores_as_its_type_code_in_a_payload_column(
        client, schema_name, sql, tc, _pk_ok):
    """The catalog round-trips the declared type, so a later read decodes the
    column at the width it was written at."""
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, v {sql})",
        schema_name=schema_name)
    _, schema = client.resolve_table(schema_name, "t")
    assert schema.columns[1].type_code == tc


@pytest.mark.parametrize("sql,tc,pk_ok", _SPELLINGS, ids=_IDS)
def test_only_an_integer_scalar_may_be_a_key_column(
        client, schema_name, sql, tc, pk_ok):
    """A PK column keeps its declared type rather than widening to U64, so the
    key's stride is the declared width; a type that cannot be compared as raw
    bytes is refused at CREATE TABLE rather than at the first ingest."""
    ddl = f"CREATE TABLE t (pk {sql} NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"
    if not pk_ok:
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(ddl, schema_name=schema_name)
        return
    client.execute_sql(ddl, schema_name=schema_name)
    _, schema = client.resolve_table(schema_name, "t")
    assert schema.columns[schema.pk_indices[0]].type_code == tc


# ---------------------------------------------------------------------------
# Spellings and key shapes the catalog refuses
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ddl,message", [
    # BOOLEAN has no TypeCode; the error has to name what to write instead,
    # because "unsupported type" alone leaves the author guessing.
    ("flag BOOLEAN NOT NULL", "TINYINT"),
    # Past 18 digits the column is a 128-bit integer rather than a scaled i64,
    # and that has nowhere to keep a scale — so the rule is stated on the
    # precision, which is what the author wrote, not on the scale.
    ("v DECIMAL(38,2) NOT NULL", "precision must be 1..=18"),
    ("v DECIMAL NOT NULL", "DECIMAL"),
    # An offset would have to survive the wire and the sort as a separate field.
    ("z TIMESTAMP WITH TIME ZONE", "time zone"),
], ids=["boolean", "wide-decimal-with-scale", "bare-decimal", "timestamptz"])
def test_an_unsupported_column_type_is_refused_by_name(
        client, schema_name, ddl, message):
    """Each rejection names the type or the rule, not an internal enum: the
    author can only act on a message that says which spelling to reach for."""
    with pytest.raises(gnitz.GnitzError, match=message):
        client.execute_sql(
            f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, {ddl})",
            schema_name=schema_name)


@pytest.mark.parametrize("cols,pk,message", [
    # PK_LIST_MAX_COLS. The count is a codec constant that may move, so the
    # message is matched on the word it must carry, not on the number.
    ("a TINYINT UNSIGNED, b TINYINT UNSIGNED, c TINYINT UNSIGNED, "
     "d TINYINT UNSIGNED, e TINYINT UNSIGNED", "(a, b, c, d, e)", "at most"),
    ("a TEXT, b INT UNSIGNED", "(a, b)", "(?i)string"),
    ("a BIGINT UNSIGNED, b BIGINT UNSIGNED", "(a, a)", "(?i)duplicate"),
], ids=["over-arity", "string-member", "duplicate-member"])
def test_a_compound_key_shape_the_codec_cannot_hold_is_refused(
        client, schema_name, cols, pk, message):
    """The PK region is one tightly packed byte string compared by `memcmp`, so
    an over-long list, a variable-width member and a repeated member each have no
    encoding at all — refused at DDL, never at ingest."""
    with pytest.raises(gnitz.GnitzError, match=message):
        client.execute_sql(
            f"CREATE TABLE t ({cols}, payload BIGINT, PRIMARY KEY {pk})",
            schema_name=schema_name)


def test_a_key_may_be_declared_inline_or_as_a_clause_but_not_both(
        client, schema_name):
    """Two PRIMARY KEY declarations name two different key shapes with no rule
    for which wins, so the statement is refused rather than resolved."""
    with pytest.raises(gnitz.GnitzError, match="Multiple PRIMARY KEY"):
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED PRIMARY KEY, b BIGINT UNSIGNED, "
            "PRIMARY KEY (a, b))", schema_name=schema_name)


def test_the_key_list_order_is_the_sort_order_not_the_column_order(
        client, schema_name):
    """`pk_indices` is the PK-list order, which is the order the packed key is
    compared in — so a table whose PRIMARY KEY names its columns in neither
    declaration order nor sorted order still sorts by the list it named.

    Rows are chosen so the two orders disagree: under `(a, b)` the sequence
    would be (1,9), (2,1); under the declared `(b, a)` it is (2,1), (1,9).
    """
    client.execute_sql(
        "CREATE TABLE t (payload BIGINT, a BIGINT UNSIGNED, b BIGINT UNSIGNED, "
        "PRIMARY KEY (b, a))", schema_name=schema_name)
    _, schema = client.resolve_table(schema_name, "t")
    assert schema.pk_indices == [2, 1]

    client.execute_sql("INSERT INTO t (a, b, payload) VALUES (1, 9, 19), (2, 1, 21)",
                       schema_name=schema_name)
    # `b` leads the key, so a range on it confines to one contiguous run.
    assert bag(scanned(client, schema_name, "t"), "a", "b", "payload") == {
        (1, 9, 19): 1, (2, 1, 21): 1}
