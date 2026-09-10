"""Which SQL spellings name a type, what each stores as, and which of them a key
may be made of.

One table drives the whole file. A spelling is a `TypeCode` plus a verdict on
whether it is PK-eligible, and the engine derives everything else — stride,
routing, comparison — from those two facts. `is_pk_eligible` is the integer
scalars and nothing else: IEEE-754 breaks the byte-equal key contract (±0.0
differ byte-wise but compare equal, NaN has no canonical bit pattern) and a
variable-length string has no fixed stride to compare at.

DATE, TIMESTAMP and DECIMAL are PK-eligible because each stores as a *signed*
integer — I32, I64 and a scaled I64 — which is why their keys sign-flip like any
other signed column rather than being a category of their own.

What is asserted here is the catalog round trip: `CREATE TABLE` → catalog →
`resolve_table` hands the declared type back unchanged. The spelling-to-TypeCode
map, the aliases and the DDL refusals belong to the SQL binder and are swept
there.
"""

import pytest
import gnitz

TC = gnitz.TypeCode

# (SQL spelling, stored TypeCode, may be a PK column). The aliases sit beside
# what they alias, so a spelling that silently stopped aliasing is a changed row.
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
    # Precision past 18 digits cannot be a scaled I64, so it widens to U128.
    ("DECIMAL(38,0)", TC.U128, True),
    ("DECIMAL(39,0)", TC.U128, True),
    ("NUMERIC(38,0)", TC.U128, True),
    ("DECIMAL(10,2)", TC.DECIMAL, True),
    ("UUID", TC.UUID, True),
    ("DATE", TC.DATE, True),
    ("TIMESTAMP", TC.TIMESTAMP, True),
]


def test_every_spelling_stores_as_its_type_code_in_a_payload_column(client, schema_name):
    """One column per spelling, so a retyped spelling is a changed entry beside
    the others. The name rides along: a list with the right types in the wrong
    order would otherwise pass."""
    cols = [f"c{i}" for i in range(len(_SPELLINGS))]
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, " +
        ", ".join(f"{c} {sql}" for c, (sql, _, _) in zip(cols, _SPELLINGS)) + ")",
        schema_name=schema_name)

    _, schema = client.resolve_table(schema_name, "t")
    assert [(c.name, c.type_code) for c in schema.columns[1:]] == \
        [(c, tc) for c, (_, tc, _) in zip(cols, _SPELLINGS)]


def test_only_an_integer_scalar_may_be_a_key_column(client, schema_name):
    """A PK column keeps its declared type rather than widening to U64, so the
    key's stride is the declared width; a type that cannot be compared as raw
    bytes is refused at CREATE TABLE rather than at the first ingest."""
    for i, (sql, tc, pk_ok) in enumerate(_SPELLINGS):
        ddl = f"CREATE TABLE t{i} (pk {sql} NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"
        if not pk_ok:
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(ddl, schema_name=schema_name)
            continue
        client.execute_sql(ddl, schema_name=schema_name)
        _, schema = client.resolve_table(schema_name, f"t{i}")
        assert schema.columns[schema.pk_indices[0]].type_code == tc, sql
