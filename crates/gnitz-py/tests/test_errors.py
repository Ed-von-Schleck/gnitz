import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, tables=()):
    for t in tables:
        try:
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def test_push_to_nonexistent_target(client):
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=10)
    with pytest.raises(gnitz.GnitzError):
        client.push(99999, batch)


def test_scan_nonexistent_target(client):
    with pytest.raises(gnitz.GnitzError):
        client.scan(99999)


# ---------------------------------------------------------------------------
# Schema mismatch — validate_schema_match in executor.rs is called on every
# buffered push that includes a schema descriptor.  No test previously sent
# a deliberately wrong schema.
# ---------------------------------------------------------------------------

class TestSchemaMismatch:
    """Push batches whose schema disagrees with the stored table schema."""

    def _make_table(self, client, sn):
        """Two-column table: pk BIGINT PK, val BIGINT."""
        client.create_schema(sn)
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        tid, _ = client.resolve_table(sn, "t")
        return tid

    def test_wrong_column_count(self, client):
        """Batch with one column fewer than the table schema must be rejected."""
        sn = "err" + _uid()
        try:
            tid = self._make_table(client, sn)
            wrong_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
            wrong_schema = gnitz.Schema(wrong_cols)
            batch = gnitz.ZSetBatch(wrong_schema)
            batch.append(pk=1)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, batch)
        finally:
            _cleanup(client, sn, tables=["t"])

    def test_wrong_pk_index(self, client):
        """Batch that declares pk_index=1 when the table uses pk_index=0 must be rejected."""
        sn = "err" + _uid()
        try:
            tid = self._make_table(client, sn)
            # Same types, but primary_key flag swapped: pk_index becomes 1.
            wrong_cols = [
                gnitz.ColumnDef("pk",  gnitz.TypeCode.U64),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64, primary_key=True),
            ]
            wrong_schema = gnitz.Schema(wrong_cols)
            batch = gnitz.ZSetBatch(wrong_schema)
            batch.append(pk=1, val=42)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, batch)
        finally:
            _cleanup(client, sn, tables=["t"])

    def test_wrong_column_type(self, client):
        """Batch where val is F64 instead of I64 must be rejected."""
        sn = "err" + _uid()
        try:
            tid = self._make_table(client, sn)
            wrong_cols = [
                gnitz.ColumnDef("pk",  gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.F64),   # wrong: table has I64
            ]
            wrong_schema = gnitz.Schema(wrong_cols)
            batch = gnitz.ZSetBatch(wrong_schema)
            batch.append(pk=1, val=3.14)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, batch)
        finally:
            _cleanup(client, sn, tables=["t"])

    def test_wrong_nullable(self, client):
        """Batch that marks a NOT NULL column as nullable must be rejected."""
        sn = "err" + _uid()
        try:
            tid = self._make_table(client, sn)  # val BIGINT NOT NULL (nullable=0)
            wrong_cols = [
                gnitz.ColumnDef("pk",  gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64, is_nullable=True),  # wrong
            ]
            wrong_schema = gnitz.Schema(wrong_cols)
            batch = gnitz.ZSetBatch(wrong_schema)
            batch.append(pk=1, val=42)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, batch)
        finally:
            _cleanup(client, sn, tables=["t"])


# ---------------------------------------------------------------------------
# Schema construction limits
# ---------------------------------------------------------------------------

class TestSchemaColumnLimit:
    """Schema must reject invalid column counts — guards the u64 null bitmask."""

    def test_zero_columns_raises(self):
        with pytest.raises(ValueError):
            gnitz.Schema([])

    def test_exactly_65_columns_ok(self):
        """1 PK + 64 payload = 65 total fills the u64 null bitmask exactly."""
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
        cols += [gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64, is_nullable=True)
                 for i in range(64)]
        s = gnitz.Schema(cols)
        assert len(s.columns) == 65

    def test_66_columns_raises(self):
        """1 PK + 65 payload shifts by 64 bits — must be rejected."""
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
        cols += [gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64, is_nullable=True)
                 for i in range(65)]
        with pytest.raises(ValueError, match="65"):
            gnitz.Schema(cols)

    def test_100_columns_raises(self):
        cols = [gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64) for i in range(100)]
        with pytest.raises(ValueError):
            gnitz.Schema(cols)
