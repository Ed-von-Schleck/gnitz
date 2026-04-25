"""SQL type system tests.

Covers:
  - DDL: every SQL type that maps to a TypeCode can be used in CREATE TABLE
  - Round-trip: INSERT boundary values for every type, scan and verify
  - U128 (DECIMAL(38,0) / DECIMAL(39,0)): full lifecycle including large values
  - Type errors: BOOLEAN, non-zero-scale DECIMAL, DATE, U128 in view expressions
  - PK coercion: signed integer PK columns get coerced to unsigned
"""

import random
import math
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


# Boundary constants
U8_MAX  = 255
I8_MIN, I8_MAX   = -128, 127
U16_MAX = 65535
I16_MIN, I16_MAX = -32768, 32767
U32_MAX = 4294967295
I32_MIN, I32_MAX = -2147483648, 2147483647
U64_MAX = 18446744073709551615
I64_MIN, I64_MAX = -9223372036854775808, 9223372036854775807
U128_MAX = (1 << 128) - 1
U64_OVERFLOW = U64_MAX + 1  # first value that requires pk_hi != 0


def _cleanup(client, sn, *table_names):
    for t in table_names:
        try:
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _scan_pks(client, tid):
    """Return sorted list of pk values for all positive-weight rows."""
    return sorted(row.pk for row in client.scan(tid) if row.weight > 0)


def _scan_map(client, tid):
    """Return {pk: row} for all positive-weight rows."""
    return {row.pk: row for row in client.scan(tid) if row.weight > 0}


# ---------------------------------------------------------------------------
# TestSqlTypeDDL — every mappable type can be used in CREATE TABLE
# ---------------------------------------------------------------------------

class TestSqlTypeDDL:
    """CREATE TABLE succeeds for every supported SQL type and resolves to the
    expected TypeCode in the stored schema."""

    def _make(self, client, sn, col_type_sql):
        client.create_schema(sn)
        client.execute_sql(
            f"CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, v {col_type_sql} NOT NULL)",
            schema_name=sn,
        )
        _, schema = client.resolve_table(sn, "t")
        # col 0 is pk (U64), col 1 is v
        return schema.columns[1].type_code

    def test_tinyint(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "TINYINT")
            assert tc == gnitz.TypeCode.I8
        finally:
            _cleanup(client, sn, "t")

    def test_tinyint_unsigned(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "TINYINT UNSIGNED")
            assert tc == gnitz.TypeCode.U8
        finally:
            _cleanup(client, sn, "t")

    def test_smallint(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "SMALLINT")
            assert tc == gnitz.TypeCode.I16
        finally:
            _cleanup(client, sn, "t")

    def test_smallint_unsigned(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "SMALLINT UNSIGNED")
            assert tc == gnitz.TypeCode.U16
        finally:
            _cleanup(client, sn, "t")

    def test_int(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "INT")
            assert tc == gnitz.TypeCode.I32
        finally:
            _cleanup(client, sn, "t")

    def test_integer(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "INTEGER")
            assert tc == gnitz.TypeCode.I32
        finally:
            _cleanup(client, sn, "t")

    def test_int_unsigned(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "INT UNSIGNED")
            assert tc == gnitz.TypeCode.U32
        finally:
            _cleanup(client, sn, "t")

    def test_bigint(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "BIGINT")
            assert tc == gnitz.TypeCode.I64
        finally:
            _cleanup(client, sn, "t")

    def test_bigint_unsigned(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "BIGINT UNSIGNED")
            assert tc == gnitz.TypeCode.U64
        finally:
            _cleanup(client, sn, "t")

    def test_float(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "FLOAT")
            assert tc == gnitz.TypeCode.F32
        finally:
            _cleanup(client, sn, "t")

    def test_double(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "DOUBLE")
            assert tc == gnitz.TypeCode.F64
        finally:
            _cleanup(client, sn, "t")

    def test_double_precision(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "DOUBLE PRECISION")
            assert tc == gnitz.TypeCode.F64
        finally:
            _cleanup(client, sn, "t")

    def test_real(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "REAL")
            assert tc == gnitz.TypeCode.F64
        finally:
            _cleanup(client, sn, "t")

    def test_varchar(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "VARCHAR(255)")
            assert tc == gnitz.TypeCode.STRING
        finally:
            _cleanup(client, sn, "t")

    def test_text(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "TEXT")
            assert tc == gnitz.TypeCode.STRING
        finally:
            _cleanup(client, sn, "t")

    def test_char(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "CHAR(10)")
            assert tc == gnitz.TypeCode.STRING
        finally:
            _cleanup(client, sn, "t")

    def test_decimal_38_0(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "DECIMAL(38,0)")
            assert tc == gnitz.TypeCode.U128
        finally:
            _cleanup(client, sn, "t")

    def test_decimal_39_0(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "DECIMAL(39,0)")
            assert tc == gnitz.TypeCode.U128
        finally:
            _cleanup(client, sn, "t")

    def test_numeric_38_0(self, client):
        sn = "s" + _uid()
        try:
            tc = self._make(client, sn, "NUMERIC(38,0)")
            assert tc == gnitz.TypeCode.U128
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# TestSqlTypeRoundTrip — INSERT boundary values, scan and verify correctness
# ---------------------------------------------------------------------------

class TestSqlTypeRoundTrip:
    """For each type: CREATE TABLE, INSERT rows with boundary values, scan and
    verify the retrieved values match exactly."""

    def _setup(self, client, sn, col_sql):
        client.create_schema(sn)
        client.execute_sql(
            f"CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, v {col_sql} NOT NULL)",
            schema_name=sn,
        )
        tid, _ = client.resolve_table(sn, "t")
        return tid

    def test_tinyint_boundaries(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "TINYINT")
            client.execute_sql(
                f"INSERT INTO t VALUES (1, {I8_MIN}), (2, 0), (3, {I8_MAX})",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[1].v == I8_MIN
            assert rows[2].v == 0
            assert rows[3].v == I8_MAX
        finally:
            _cleanup(client, sn, "t")

    def test_tinyint_unsigned_boundaries(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "TINYINT UNSIGNED")
            client.execute_sql(
                f"INSERT INTO t VALUES (1, 0), (2, 127), (3, {U8_MAX})",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[1].v == 0
            assert rows[2].v == 127
            assert rows[3].v == U8_MAX
        finally:
            _cleanup(client, sn, "t")

    def test_smallint_boundaries(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "SMALLINT")
            client.execute_sql(
                f"INSERT INTO t VALUES (1, {I16_MIN}), (2, 0), (3, {I16_MAX})",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[1].v == I16_MIN
            assert rows[2].v == 0
            assert rows[3].v == I16_MAX
        finally:
            _cleanup(client, sn, "t")

    def test_smallint_unsigned_boundaries(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "SMALLINT UNSIGNED")
            client.execute_sql(
                f"INSERT INTO t VALUES (1, 0), (2, {U16_MAX})",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[1].v == 0
            assert rows[2].v == U16_MAX
        finally:
            _cleanup(client, sn, "t")

    def test_int_boundaries(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "INT")
            client.execute_sql(
                f"INSERT INTO t VALUES (1, {I32_MIN}), (2, 0), (3, {I32_MAX})",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[1].v == I32_MIN
            assert rows[2].v == 0
            assert rows[3].v == I32_MAX
        finally:
            _cleanup(client, sn, "t")

    def test_int_unsigned_boundaries(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "INT UNSIGNED")
            client.execute_sql(
                f"INSERT INTO t VALUES (1, 0), (2, {U32_MAX})",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[1].v == 0
            assert rows[2].v == U32_MAX
        finally:
            _cleanup(client, sn, "t")

    def test_bigint_boundaries(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "BIGINT")
            client.execute_sql(
                f"INSERT INTO t VALUES (1, {I64_MIN}), (2, 0), (3, {I64_MAX})",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[1].v == I64_MIN
            assert rows[2].v == 0
            assert rows[3].v == I64_MAX
        finally:
            _cleanup(client, sn, "t")

    def test_bigint_unsigned_boundaries(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "BIGINT UNSIGNED")
            client.execute_sql(
                f"INSERT INTO t VALUES (1, 0), (2, {U64_MAX})",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[1].v == 0
            assert rows[2].v == U64_MAX
        finally:
            _cleanup(client, sn, "t")

    def test_float_values(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "FLOAT")
            client.execute_sql(
                "INSERT INTO t VALUES (1, 0.0), (2, 1.5), (3, -3.14)",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[1].v == pytest.approx(0.0)
            assert rows[2].v == pytest.approx(1.5)
            assert rows[3].v == pytest.approx(-3.14, abs=1e-5)
        finally:
            _cleanup(client, sn, "t")

    def test_double_values(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "DOUBLE")
            client.execute_sql(
                "INSERT INTO t VALUES (1, 0.0), (2, 1.23456789), (3, -9.99e10)",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[1].v == pytest.approx(0.0)
            assert rows[2].v == pytest.approx(1.23456789)
            assert rows[3].v == pytest.approx(-9.99e10)
        finally:
            _cleanup(client, sn, "t")

    def test_varchar_values(self, client):
        sn = "s" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, v VARCHAR(255) NOT NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            # Empty string, short inline, long heap-allocated (>12 chars)
            client.execute_sql(
                "INSERT INTO t VALUES (1, ''), (2, 'hello'), (3, 'this_is_a_longer_string')",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[1].v == ""
            assert rows[2].v == "hello"
            assert rows[3].v == "this_is_a_longer_string"
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# TestNullRoundTrip — nullable columns for representative types
# ---------------------------------------------------------------------------

class TestNullRoundTrip:
    """Verify that NULL survives an INSERT→scan round-trip for a selection of
    types.  The null bitmap has a distinct ColData branch per type family
    (Fixed / Strings / U128s), so each family needs at least one test."""

    def _setup(self, client, sn, col_sql):
        client.create_schema(sn)
        client.execute_sql(
            f"CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, v {col_sql} NULL)",
            schema_name=sn,
        )
        tid, _ = client.resolve_table(sn, "t")
        return tid

    def test_null_tinyint(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "TINYINT")
            client.execute_sql("INSERT INTO t VALUES (1, NULL)", schema_name=sn)
            rows = _scan_map(client, tid)
            assert rows[1].v is None
        finally:
            _cleanup(client, sn, "t")

    def test_null_bigint(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "BIGINT")
            client.execute_sql("INSERT INTO t VALUES (1, NULL)", schema_name=sn)
            rows = _scan_map(client, tid)
            assert rows[1].v is None
        finally:
            _cleanup(client, sn, "t")

    def test_null_double(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "DOUBLE")
            client.execute_sql("INSERT INTO t VALUES (1, NULL)", schema_name=sn)
            rows = _scan_map(client, tid)
            assert rows[1].v is None
        finally:
            _cleanup(client, sn, "t")

    def test_null_varchar(self, client):
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "VARCHAR(255)")
            client.execute_sql("INSERT INTO t VALUES (1, NULL)", schema_name=sn)
            rows = _scan_map(client, tid)
            assert rows[1].v is None
        finally:
            _cleanup(client, sn, "t")

    def test_null_and_non_null_same_column(self, client):
        """NULL and non-NULL values in the same column coexist correctly."""
        sn = "s" + _uid()
        try:
            tid = self._setup(client, sn, "INT")
            client.execute_sql(
                "INSERT INTO t VALUES (1, 42), (2, NULL), (3, -7)",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[1].v == 42
            assert rows[2].v is None
            assert rows[3].v == -7
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# TestOutOfRange — INSERT values that exceed the column type's range are
# rejected at the SQL layer (parse failure in append_value_to_col)
# ---------------------------------------------------------------------------

class TestOutOfRange:
    """Values that cannot be represented in the column's wire type must be
    rejected with a clear error, not silently truncated."""

    def _setup(self, client, sn, col_sql):
        client.create_schema(sn)
        client.execute_sql(
            f"CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, v {col_sql} NOT NULL)",
            schema_name=sn,
        )

    def test_tinyint_unsigned_overflow(self, client):
        """256 does not fit in TINYINT UNSIGNED (max 255)."""
        sn = "s" + _uid()
        try:
            self._setup(client, sn, "TINYINT UNSIGNED")
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (1, 256)", schema_name=sn)
        finally:
            _cleanup(client, sn, "t")

    def test_tinyint_unsigned_negative(self, client):
        """-1 does not fit in TINYINT UNSIGNED."""
        sn = "s" + _uid()
        try:
            self._setup(client, sn, "TINYINT UNSIGNED")
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (1, -1)", schema_name=sn)
        finally:
            _cleanup(client, sn, "t")

    def test_tinyint_signed_overflow(self, client):
        """128 does not fit in TINYINT (max 127)."""
        sn = "s" + _uid()
        try:
            self._setup(client, sn, "TINYINT")
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (1, 128)", schema_name=sn)
        finally:
            _cleanup(client, sn, "t")

    def test_tinyint_signed_underflow(self, client):
        """-129 does not fit in TINYINT (min -128)."""
        sn = "s" + _uid()
        try:
            self._setup(client, sn, "TINYINT")
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (1, -129)", schema_name=sn)
        finally:
            _cleanup(client, sn, "t")

    def test_smallint_overflow(self, client):
        """32768 does not fit in SMALLINT (max 32767)."""
        sn = "s" + _uid()
        try:
            self._setup(client, sn, "SMALLINT")
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (1, 32768)", schema_name=sn)
        finally:
            _cleanup(client, sn, "t")

    def test_int_unsigned_overflow(self, client):
        """4294967296 does not fit in INT UNSIGNED (max 4294967295)."""
        sn = "s" + _uid()
        try:
            self._setup(client, sn, "INT UNSIGNED")
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "INSERT INTO t VALUES (1, 4294967296)", schema_name=sn
                )
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# TestU128 — DECIMAL(38,0) full lifecycle
# ---------------------------------------------------------------------------

class TestU128:
    """Test U128 support via DECIMAL(38,0) end-to-end."""

    _CREATE_PK = (
        "CREATE TABLE t (pk DECIMAL(38,0) NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"
    )
    _CREATE_COL = (
        "CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
        "big_id DECIMAL(38,0) NOT NULL, note VARCHAR(64) NOT NULL)"
    )

    # --- DDL ---

    def test_u128_pk_create(self, client):
        """CREATE TABLE with DECIMAL(38,0) PK stores TypeCode.U128."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            _, schema = client.resolve_table(sn, "t")
            assert schema.columns[schema.pk_index].type_code == gnitz.TypeCode.U128
        finally:
            _cleanup(client, sn, "t")

    def test_decimal_39_0_also_accepted(self, client):
        """DECIMAL(39,0) (full u128 range) is also accepted."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk DECIMAL(39,0) NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            _, schema = client.resolve_table(sn, "t")
            assert schema.columns[schema.pk_index].type_code == gnitz.TypeCode.U128
        finally:
            _cleanup(client, sn, "t")

    def test_u128_non_pk_column(self, client):
        """DECIMAL(38,0) as a non-PK column stores TypeCode.U128."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_COL, schema_name=sn)
            _, schema = client.resolve_table(sn, "t")
            # Find big_id column
            col_names = [c.name for c in schema.columns]
            idx = col_names.index("big_id")
            assert schema.columns[idx].type_code == gnitz.TypeCode.U128
        finally:
            _cleanup(client, sn, "t")

    # --- INSERT with small pk (fits in u64) ---

    def test_insert_small_u128_pk(self, client):
        """INSERT a U128 PK value that fits within u64."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql("INSERT INTO t VALUES (42, 100)", schema_name=sn)
            rows = _scan_map(client, tid)
            assert 42 in rows
            assert rows[42].v == 100
        finally:
            _cleanup(client, sn, "t")

    def test_insert_u64_max_pk(self, client):
        """INSERT a U128 PK equal to u64::MAX (boundary: hi=0)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql(
                f"INSERT INTO t VALUES ({U64_MAX}, 1)", schema_name=sn
            )
            rows = _scan_map(client, tid)
            assert U64_MAX in rows
        finally:
            _cleanup(client, sn, "t")

    def test_insert_large_u128_pk(self, client):
        """INSERT a U128 PK larger than u64::MAX (requires pk_hi != 0)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            pk = U64_OVERFLOW  # 2**64
            client.execute_sql(
                f"INSERT INTO t VALUES ({pk}, 7)", schema_name=sn
            )
            rows = _scan_map(client, tid)
            assert pk in rows
            assert rows[pk].v == 7
        finally:
            _cleanup(client, sn, "t")

    def test_insert_u128_max_pk(self, client):
        """INSERT a U128 PK equal to u128::MAX (maximum possible value)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql(
                f"INSERT INTO t VALUES ({U128_MAX}, 99)", schema_name=sn
            )
            rows = _scan_map(client, tid)
            assert U128_MAX in rows
            assert rows[U128_MAX].v == 99
        finally:
            _cleanup(client, sn, "t")

    def test_insert_multiple_u128_pks(self, client):
        """INSERT multiple rows with a mix of small and large U128 PKs."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            pks = [0, 1, U64_MAX, U64_OVERFLOW, U128_MAX]
            for pk in pks:
                client.execute_sql(
                    f"INSERT INTO t VALUES ({pk}, {pk % 1000})", schema_name=sn
                )
            rows = _scan_map(client, tid)
            assert set(rows.keys()) == set(pks)
        finally:
            _cleanup(client, sn, "t")

    # --- SELECT (pk-seek) ---

    def test_select_small_u128_pk(self, client):
        """SELECT WHERE pk = <small u128> performs a pk seek."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn
            )
            res = client.execute_sql("SELECT * FROM t WHERE pk = 2", schema_name=sn)
            assert res[0]["type"] == "Rows"
            rows = list(res[0]["rows"])
            assert len(rows) == 1
            assert rows[0].v == 20
        finally:
            _cleanup(client, sn, "t")

    def test_select_large_u128_pk(self, client):
        """SELECT WHERE pk = <u128 > u64::MAX> performs a correct pk seek."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            pk = U64_OVERFLOW
            client.execute_sql(
                f"INSERT INTO t VALUES ({pk}, 42)", schema_name=sn
            )
            res = client.execute_sql(
                f"SELECT * FROM t WHERE pk = {pk}", schema_name=sn
            )
            assert res[0]["type"] == "Rows"
            rows = list(res[0]["rows"])
            assert len(rows) == 1
            assert rows[0].v == 42
        finally:
            _cleanup(client, sn, "t")

    def test_select_u128_max_pk(self, client):
        """SELECT WHERE pk = u128::MAX finds the row."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            client.execute_sql(
                f"INSERT INTO t VALUES ({U128_MAX}, 55)", schema_name=sn
            )
            res = client.execute_sql(
                f"SELECT * FROM t WHERE pk = {U128_MAX}", schema_name=sn
            )
            assert res[0]["type"] == "Rows"
            rows = list(res[0]["rows"])
            assert len(rows) == 1
            assert rows[0].v == 55
        finally:
            _cleanup(client, sn, "t")

    def test_select_u128_pk_not_found(self, client):
        """SELECT WHERE pk = <absent u128> returns 0 rows."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
            res = client.execute_sql(
                f"SELECT * FROM t WHERE pk = {U64_OVERFLOW}", schema_name=sn
            )
            assert res[0]["type"] == "Rows"
            assert len(res[0]["rows"]) == 0
        finally:
            _cleanup(client, sn, "t")

    # --- ON CONFLICT ---

    def test_on_conflict_do_nothing_u128_pk(self, client):
        """ON CONFLICT DO NOTHING with U128 PK: duplicate is silently skipped."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            pk = U64_OVERFLOW
            client.execute_sql(
                f"INSERT INTO t VALUES ({pk}, 1)", schema_name=sn
            )
            res = client.execute_sql(
                f"INSERT INTO t VALUES ({pk}, 999) ON CONFLICT (pk) DO NOTHING",
                schema_name=sn,
            )
            assert res[0]["count"] == 0  # no new rows
            rows = _scan_map(client, tid)
            assert rows[pk].v == 1  # original value preserved
        finally:
            _cleanup(client, sn, "t")

    def test_on_conflict_do_update_u128_pk(self, client):
        """ON CONFLICT DO UPDATE with U128 PK: existing row gets updated."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            pk = U64_OVERFLOW
            client.execute_sql(
                f"INSERT INTO t VALUES ({pk}, 1)", schema_name=sn
            )
            client.execute_sql(
                f"INSERT INTO t VALUES ({pk}, 999) "
                f"ON CONFLICT (pk) DO UPDATE SET v = EXCLUDED.v",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert rows[pk].v == 999  # updated
        finally:
            _cleanup(client, sn, "t")

    # --- DELETE ---

    def test_delete_u128_pk(self, client):
        """DELETE WHERE pk = <u128> removes the row."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            pk = U64_OVERFLOW
            client.execute_sql(f"INSERT INTO t VALUES ({pk}, 1)", schema_name=sn)
            res = client.execute_sql(
                f"DELETE FROM t WHERE pk = {pk}", schema_name=sn
            )
            assert res[0]["count"] == 1
            rows = _scan_map(client, tid)
            assert pk not in rows
        finally:
            _cleanup(client, sn, "t")

    def test_delete_large_u128_pk(self, client):
        """DELETE WHERE pk = u128::MAX removes the row."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql(
                f"INSERT INTO t VALUES ({U128_MAX}, 1)", schema_name=sn
            )
            client.execute_sql(
                f"DELETE FROM t WHERE pk = {U128_MAX}", schema_name=sn
            )
            rows = _scan_map(client, tid)
            assert U128_MAX not in rows
        finally:
            _cleanup(client, sn, "t")

    # --- UPDATE ---

    def test_update_u128_pk(self, client):
        """UPDATE WHERE pk = <u128> modifies the row."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            pk = U64_OVERFLOW
            client.execute_sql(f"INSERT INTO t VALUES ({pk}, 1)", schema_name=sn)
            res = client.execute_sql(
                f"UPDATE t SET v = 999 WHERE pk = {pk}", schema_name=sn
            )
            assert res[0]["count"] == 1
            rows = _scan_map(client, tid)
            assert rows[pk].v == 999
        finally:
            _cleanup(client, sn, "t")

    # --- U128 non-PK column ---

    def test_u128_non_pk_insert_and_scan(self, client):
        """U128 non-PK column: INSERT and scan round-trip."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_COL, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            pk1, big1 = 1, U64_OVERFLOW
            pk2, big2 = 2, U128_MAX
            client.execute_sql(
                f"INSERT INTO t VALUES ({pk1}, {big1}, 'first')", schema_name=sn
            )
            client.execute_sql(
                f"INSERT INTO t VALUES ({pk2}, {big2}, 'second')", schema_name=sn
            )
            rows = _scan_map(client, tid)
            assert rows[pk1].big_id == big1
            assert rows[pk2].big_id == big2
        finally:
            _cleanup(client, sn, "t")

    def test_u128_non_pk_zero_value(self, client):
        """U128 non-PK column stores 0 correctly."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_COL, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql(
                "INSERT INTO t VALUES (1, 0, 'zero')", schema_name=sn
            )
            rows = _scan_map(client, tid)
            assert rows[1].big_id == 0
        finally:
            _cleanup(client, sn, "t")

    def test_u128_nullable_non_pk(self, client):
        """Nullable U128 non-PK column accepts NULL."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
                "big_id DECIMAL(38,0) NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql(
                "INSERT INTO t VALUES (1, NULL)", schema_name=sn
            )
            rows = _scan_map(client, tid)
            assert rows[1].big_id is None
        finally:
            _cleanup(client, sn, "t")

    # --- Views and expressions ---

    def test_view_on_table_with_u128_pk_no_expression(self, client):
        """CREATE VIEW on a table with U128 PK works when U128 column
        is not referenced in a WHERE or computed projection."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            # Filter on the non-U128 column only — U128 pk not in expression
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE v > 10",
                schema_name=sn,
            )
            client.execute_sql(
                f"INSERT INTO t VALUES ({U64_OVERFLOW}, 5), ({U64_OVERFLOW + 1}, 15)",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "v")
            rows = list(client.scan(vid))
            positive = [r for r in rows if r.weight > 0]
            assert len(positive) == 1  # only v=15 passes
        finally:
            _cleanup(client, sn, "t", "v")

    def test_view_using_u128_col_in_filter_raises(self, client):
        """CREATE VIEW with U128 column in WHERE expression must fail with a clear error."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_COL, schema_name=sn)
            with pytest.raises(gnitz.GnitzError) as exc_info:
                client.execute_sql(
                    "CREATE VIEW v AS SELECT * FROM t WHERE big_id > 0",
                    schema_name=sn,
                )
            assert "U128" in str(exc_info.value)
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# TestUUID — UUID type end-to-end
# ---------------------------------------------------------------------------

class TestUUID:
    """Test UUID type (TypeCode 13) end-to-end."""

    _CREATE_PK = "CREATE TABLE t (pk UUID NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"
    _CREATE_COL = (
        "CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, uid UUID NOT NULL)"
    )

    UUID_A = '550e8400-e29b-41d4-a716-446655440000'
    UUID_B = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
    UUID_V7_EARLY = '01935000-0000-7000-8000-000000000001'
    UUID_V7_LATE  = '01935001-0000-7000-8000-000000000001'

    # --- TypeCode constant ---

    def test_typecode_uuid_constant(self, client):
        assert gnitz.TypeCode.UUID == 13

    # --- DDL ---

    def test_uuid_type_creates_typecode_uuid(self, client):
        """CREATE TABLE with UUID PK stores TypeCode.UUID."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            _, schema = client.resolve_table(sn, "t")
            assert schema.columns[schema.pk_index].type_code == gnitz.TypeCode.UUID
        finally:
            _cleanup(client, sn, "t")

    def test_uuid_non_pk_column_type(self, client):
        """UUID as a non-PK payload column stores TypeCode.UUID."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_COL, schema_name=sn)
            _, schema = client.resolve_table(sn, "t")
            col_names = [c.name for c in schema.columns]
            idx = col_names.index("uid")
            assert schema.columns[idx].type_code == gnitz.TypeCode.UUID
        finally:
            _cleanup(client, sn, "t")

    def test_decimal_38_0_unaffected(self, client):
        """DECIMAL(38,0) still maps to TypeCode.U128 (not UUID), returns Python int."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk DECIMAL(38,0) NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            _, schema = client.resolve_table(sn, "t")
            assert schema.columns[schema.pk_index].type_code == gnitz.TypeCode.U128
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql("INSERT INTO t VALUES (42, 1)", schema_name=sn)
            rows = _scan_map(client, tid)
            assert 42 in rows
            assert isinstance(42, int)
        finally:
            _cleanup(client, sn, "t")

    # --- INSERT ---

    def test_uuid_pk_insert_string_literal(self, client):
        """INSERT with a UUID string literal PK succeeds."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql(
                f"INSERT INTO t VALUES ('{self.UUID_A}', 1)", schema_name=sn
            )
            rows = _scan_map(client, tid)
            assert self.UUID_A in rows
        finally:
            _cleanup(client, sn, "t")

    def test_uuid_non_pk_insert_string_literal(self, client):
        """UUID non-PK column accepts a UUID string literal."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_COL, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql(
                f"INSERT INTO t VALUES (1, '{self.UUID_A}')", schema_name=sn
            )
            rows = _scan_map(client, tid)
            assert rows[1].uid == self.UUID_A
        finally:
            _cleanup(client, sn, "t")

    def test_uuid_decimal_literal_still_valid(self, client):
        """Decimal integer literal is also accepted for a UUID non-PK column."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_COL, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            rows = _scan_map(client, tid)
            # 42 = 0x2a → UUID format: 00000000-0000-0000-0000-00000000002a
            assert rows[1].uid == '00000000-0000-0000-0000-00000000002a'
        finally:
            _cleanup(client, sn, "t")

    def test_uuid_invalid_string_rejected(self, client):
        """INSERT with an invalid UUID string raises GnitzError."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "INSERT INTO t VALUES ('not-a-uuid', 1)", schema_name=sn
                )
        finally:
            _cleanup(client, sn, "t")

    # --- Output format ---

    def test_uuid_output_is_string(self, client):
        """Scanning a UUID PK table returns pk as a Python str, not int."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            client.execute_sql(
                f"INSERT INTO t VALUES ('{self.UUID_A}', 1)", schema_name=sn
            )
            tid, _ = client.resolve_table(sn, "t")
            rows = [r for r in client.scan(tid) if r.weight > 0]
            assert len(rows) == 1
            assert isinstance(rows[0].pk, str)
        finally:
            _cleanup(client, sn, "t")

    def test_uuid_roundtrip(self, client):
        """UUID PK INSERT + scan returns the same UUID string."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql(
                f"INSERT INTO t VALUES ('{self.UUID_A}', 10)", schema_name=sn
            )
            rows = _scan_map(client, tid)
            assert self.UUID_A in rows
            assert rows[self.UUID_A].v == 10
        finally:
            _cleanup(client, sn, "t")

    def test_uuid_v7_ordering(self, client):
        """Two UUIDv7 values inserted in reverse order scan in ascending numeric order."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_PK, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            # Insert later UUID first
            client.execute_sql(
                f"INSERT INTO t VALUES ('{self.UUID_V7_LATE}', 2)", schema_name=sn
            )
            client.execute_sql(
                f"INSERT INTO t VALUES ('{self.UUID_V7_EARLY}', 1)", schema_name=sn
            )
            rows = [r for r in client.scan(tid) if r.weight > 0]
            pks = [r.pk for r in rows]
            assert pks == [self.UUID_V7_EARLY, self.UUID_V7_LATE]
        finally:
            _cleanup(client, sn, "t")

    # --- FK ---

    def test_uuid_fk_valid(self, client):
        """UUID FK column → UUID PK parent: valid UUID value accepted."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id UUID NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                f"INSERT INTO parent VALUES ('{self.UUID_A}')", schema_name=sn
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT UNSIGNED NOT NULL PRIMARY KEY,"
                f"  uid UUID NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                f"INSERT INTO child VALUES (1, '{self.UUID_A}')", schema_name=sn
            )
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_uuid_fk_invalid(self, client):
        """UUID FK column → non-existent parent UUID raises GnitzError."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id UUID NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT UNSIGNED NOT NULL PRIMARY KEY,"
                "  uid UUID NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.execute_sql(
                    f"INSERT INTO child VALUES (1, '{self.UUID_B}')", schema_name=sn
                )
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_uuid_fk_null_nullable(self, client):
        """Nullable UUID FK column: NULL value accepted without a matching parent."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id UUID NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT UNSIGNED NOT NULL PRIMARY KEY,"
                "  uid UUID REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO child VALUES (1, NULL)", schema_name=sn
            )
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_uuid_fk_type_mismatch_rejected(self, client):
        """UUID FK column referencing a U64 PK parent fails at DDL time."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT UNSIGNED NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE TABLE child ("
                    "  cid BIGINT UNSIGNED NOT NULL PRIMARY KEY,"
                    "  uid UUID NOT NULL REFERENCES parent(id)"
                    ")",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, "child", "parent")

    # --- Views ---

    def test_uuid_view_expression_error(self, client):
        """UUID column used in a view WHERE expression gives a meaningful error."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE_COL, schema_name=sn)
            with pytest.raises(gnitz.GnitzError) as exc_info:
                client.execute_sql(
                    "CREATE VIEW v AS SELECT * FROM t WHERE uid = 1",
                    schema_name=sn,
                )
            assert "UUID" in str(exc_info.value) or "U128" in str(exc_info.value)
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# TestTypeErrors — unsupported types and invalid values give clear errors
# ---------------------------------------------------------------------------

class TestTypeErrors:
    def test_boolean_rejected_with_hint(self, client):
        """BOOLEAN gives an error mentioning TINYINT(1)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            with pytest.raises(gnitz.GnitzError) as exc_info:
                client.execute_sql(
                    "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, flag BOOLEAN NOT NULL)",
                    schema_name=sn,
                )
            assert "TINYINT" in str(exc_info.value)
        finally:
            _cleanup(client, sn)

    def test_date_type_rejected(self, client):
        """DATE type is not supported."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, d DATE NOT NULL)",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn)

    def test_decimal_nonzero_scale_rejected(self, client):
        """DECIMAL(38,2) (non-zero scale) is not supported."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v DECIMAL(38,2) NOT NULL)",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn)

    def test_decimal_bare_rejected(self, client):
        """DECIMAL with no precision is not supported."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v DECIMAL NOT NULL)",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn)

    def test_u128_in_view_where_gives_clear_error(self, client):
        """U128 column used in a view expression gives a meaningful error."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
                "big DECIMAL(38,0) NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError) as exc_info:
                client.execute_sql(
                    "CREATE VIEW v AS SELECT * FROM t WHERE big = 1",
                    schema_name=sn,
                )
            assert "U128" in str(exc_info.value)
        finally:
            _cleanup(client, sn, "t")

    def test_u128_non_pk_no_index_gives_clear_error(self, client):
        """SELECT WHERE on a U128 non-PK column (no index) gives a helpful error.
        The engine rejects the query before evaluation because the column has no
        index; the error message guides the user to CREATE INDEX or CREATE VIEW."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
                "big DECIMAL(38,0) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError) as exc_info:
                # No index on big → error before residual filter even runs
                client.execute_sql(
                    "SELECT * FROM t WHERE big = 42", schema_name=sn
                )
            err = str(exc_info.value)
            assert "non-indexed" in err or "CREATE INDEX" in err or "CREATE VIEW" in err
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# TestPKCoercion — PK type rules: only BIGINT (→U64) and DECIMAL(38,0) (→U128)
# ---------------------------------------------------------------------------

class TestPKCoercion:
    """The server accepts only U64 and U128 primary keys.
    BIGINT is coerced to U64 by the planner. DECIMAL(38,0) maps to U128.
    Narrower integer types (TINYINT, SMALLINT, INT) are rejected even after
    the planner coerces them to their unsigned counterparts (U8, U16, U32)."""

    def _create_table_with_pk(self, client, sn, pk_sql):
        client.create_schema(sn)
        client.execute_sql(
            f"CREATE TABLE t (pk {pk_sql} NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )

    def test_tinyint_pk_rejected(self, client):
        """TINYINT PK is rejected: server only accepts U64 or U128."""
        sn = "s" + _uid()
        try:
            with pytest.raises(gnitz.GnitzError):
                self._create_table_with_pk(client, sn, "TINYINT")
        finally:
            _cleanup(client, sn)

    def test_smallint_pk_rejected(self, client):
        """SMALLINT PK is rejected: server only accepts U64 or U128."""
        sn = "s" + _uid()
        try:
            with pytest.raises(gnitz.GnitzError):
                self._create_table_with_pk(client, sn, "SMALLINT")
        finally:
            _cleanup(client, sn)

    def test_int_pk_rejected(self, client):
        """INT PK is rejected: server only accepts U64 or U128."""
        sn = "s" + _uid()
        try:
            with pytest.raises(gnitz.GnitzError):
                self._create_table_with_pk(client, sn, "INT")
        finally:
            _cleanup(client, sn)

    def test_bigint_pk_coerced_to_u64(self, client):
        """Signed BIGINT PK is coerced to U64 by the planner."""
        sn = "s" + _uid()
        try:
            self._create_table_with_pk(client, sn, "BIGINT")
            _, schema = client.resolve_table(sn, "t")
            assert schema.columns[schema.pk_index].type_code == gnitz.TypeCode.U64
        finally:
            _cleanup(client, sn, "t")

    def test_decimal_38_0_pk_stays_u128(self, client):
        """DECIMAL(38,0) PK maps to U128 (already unsigned, no coercion needed)."""
        sn = "s" + _uid()
        try:
            self._create_table_with_pk(client, sn, "DECIMAL(38,0)")
            _, schema = client.resolve_table(sn, "t")
            assert schema.columns[schema.pk_index].type_code == gnitz.TypeCode.U128
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# TestMixedTypeTable — tables with multiple different column types
# ---------------------------------------------------------------------------

class TestMixedTypeTable:
    """A table with many different column types stores and retrieves all values
    correctly in a single INSERT/scan cycle."""

    def test_all_numeric_types_in_one_table(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk      BIGINT UNSIGNED NOT NULL PRIMARY KEY,"
                "  ti      TINYINT NOT NULL,"
                "  tiu     TINYINT UNSIGNED NOT NULL,"
                "  si      SMALLINT NOT NULL,"
                "  siu     SMALLINT UNSIGNED NOT NULL,"
                "  i       INT NOT NULL,"
                "  iu      INT UNSIGNED NOT NULL,"
                "  bi      BIGINT NOT NULL,"
                "  biu     BIGINT UNSIGNED NOT NULL,"
                "  f       FLOAT NOT NULL,"
                "  d       DOUBLE NOT NULL,"
                "  big128  DECIMAL(38,0) NOT NULL"
                ")",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            big_val = U64_OVERFLOW
            client.execute_sql(
                f"INSERT INTO t VALUES "
                f"(1, {I8_MIN}, {U8_MAX}, {I16_MIN}, {U16_MAX}, "
                f"{I32_MIN}, {U32_MAX}, {I64_MIN}, {U64_MAX}, "
                f"1.5, 2.5, {big_val})",
                schema_name=sn,
            )
            rows = _scan_map(client, tid)
            assert len(rows) == 1
            r = rows[1]
            assert r.ti  == I8_MIN
            assert r.tiu == U8_MAX
            assert r.si  == I16_MIN
            assert r.siu == U16_MAX
            assert r.i   == I32_MIN
            assert r.iu  == U32_MAX
            assert r.bi  == I64_MIN
            assert r.biu == U64_MAX
            assert r.f   == pytest.approx(1.5)
            assert r.d   == pytest.approx(2.5)
            assert r.big128 == big_val
        finally:
            _cleanup(client, sn, "t")
