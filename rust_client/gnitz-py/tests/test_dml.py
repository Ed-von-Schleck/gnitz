import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _setup(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "t", cols)
    return sn, tid, schema


def test_push_and_scan(client):
    sn, tid, schema = _setup(client)
    batch = gnitz.ZSetBatch(schema)
    for i in range(1, 6):
        batch.append(pk=i, val=i * 10)
    client.push(tid, batch)
    result = client.scan(tid)
    assert len(result) == 5
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_scan_empty_table(client):
    sn, tid, schema = _setup(client)
    result = client.scan(tid)
    assert len(result) == 0
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_delete_rows(client):
    sn, tid, schema = _setup(client)
    batch = gnitz.ZSetBatch(schema)
    for pk, val in [(1, 10), (2, 20), (3, 30)]:
        batch.append(pk=pk, val=val)
    client.push(tid, batch)
    client.delete(tid, schema, [2])
    result = client.scan(tid)
    pks = sorted(row.pk for row in result if row.weight > 0)
    assert pks == [1, 3]
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_nullable_string_columns(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("s", gnitz.TypeCode.STRING, is_nullable=True)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "strs", cols)
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, s="hello")
    batch.append(pk=2, s=None)
    batch.append(pk=3, s="world")
    client.push(tid, batch)
    result = client.scan(tid)
    assert len(result) == 3
    client.drop_table(sn, "strs")
    client.drop_schema(sn)


def test_scan_values_correct(client):
    """Verify pushed values round-trip through scan correctly."""
    sn, tid, schema = _setup(client)
    rows_in = [(i, i * 100) for i in range(1, 11)]
    batch = gnitz.ZSetBatch(schema)
    for pk, val in rows_in:
        batch.append(pk=pk, val=val)
    client.push(tid, batch)
    result = client.scan(tid)
    assert len(result) == 10
    pairs = sorted((row.pk, row.val) for row in result)
    assert pairs == rows_in
    client.drop_table(sn, "t")
    client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Helpers for SQL UPDATE / DELETE tests
# ---------------------------------------------------------------------------

_CREATE_T3 = (
    "CREATE TABLE t "
    "(pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL, cat_id BIGINT NOT NULL)"
)
_INSERT_3ROWS = "INSERT INTO t VALUES (1, 100, 10), (2, 200, 20), (3, 300, 30)"


def _rows_map(client, tid):
    """Scan table and return {pk: row} dict (only positive-weight rows)."""
    return {row.pk: row for row in client.scan(tid) if row.weight > 0}


def _drop_idx_and_table(client, sn, idx_name):
    for sql in [f"DROP INDEX {idx_name}", "DROP TABLE t"]:
        try:
            client.execute_sql(sql, schema_name=sn)
        except Exception:
            pass
    client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestUpdateSQL
# ---------------------------------------------------------------------------

class TestUpdateSQL:
    def test_update_pk_literal_set(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = 99 WHERE pk = 1", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 1

            rows = _rows_map(client, tid)
            assert rows[1].val == 99
            assert rows[2].val == 200
            assert rows[3].val == 300
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_pk_expr_set(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = val * 2 WHERE pk = 2", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 1

            rows = _rows_map(client, tid)
            assert rows[2].val == 400
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_unique_index_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        idx = f"{sn}__t__idx_cat_id"
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(cat_id)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = 0 WHERE cat_id = 20", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 1

            rows = _rows_map(client, tid)
            assert rows[1].val == 100
            assert rows[2].val == 0
            assert rows[3].val == 300
        finally:
            _drop_idx_and_table(client, sn, idx)

    def test_update_nonunique_index_scan(self, client):
        """Non-unique index → falls through to full scan; both matching rows updated."""
        sn = "s" + _uid()
        client.create_schema(sn)
        idx = f"{sn}__t__idx_cat_id"
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 100, 10), (2, 200, 10), (3, 300, 99)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(cat_id)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = 0 WHERE cat_id = 10", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 2

            rows = _rows_map(client, tid)
            assert rows[1].val == 0
            assert rows[2].val == 0
            assert rows[3].val == 300
        finally:
            _drop_idx_and_table(client, sn, idx)

    def test_update_full_scan_predicate(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = 0 WHERE val > 150", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 2

            rows = _rows_map(client, tid)
            assert rows[1].val == 100
            assert rows[2].val == 0
            assert rows[3].val == 0
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_no_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = 42", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 3

            rows = _rows_map(client, tid)
            assert all(r.val == 42 for r in rows.values())
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_pk_column_rejects(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 100, 10)", schema_name=sn)

            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("UPDATE t SET pk = 999 WHERE pk = 1", schema_name=sn)
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_row_not_found(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 100, 10)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = 99 WHERE pk = 9999", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 0

            rows = _rows_map(client, tid)
            assert rows[1].val == 100
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestDeleteSQL
# ---------------------------------------------------------------------------

class TestDeleteSQL:
    def test_delete_pk_eq(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 1

            rows = _rows_map(client, tid)
            assert 1 in rows and 3 in rows and 2 not in rows
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_delete_pk_in(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("DELETE FROM t WHERE pk IN (1, 3)", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 2

            rows = _rows_map(client, tid)
            assert list(rows.keys()) == [2]
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_delete_unique_index(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        idx = f"{sn}__t__idx_cat_id"
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(cat_id)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("DELETE FROM t WHERE cat_id = 20", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 1

            rows = _rows_map(client, tid)
            assert 1 in rows and 3 in rows and 2 not in rows
        finally:
            _drop_idx_and_table(client, sn, idx)

    def test_delete_nonunique_index_scan(self, client):
        """Non-unique index → falls through to full scan; all matching rows deleted."""
        sn = "s" + _uid()
        client.create_schema(sn)
        idx = f"{sn}__t__idx_cat_id"
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 100, 10), (2, 200, 10), (3, 300, 99)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(cat_id)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("DELETE FROM t WHERE cat_id = 10", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 2

            rows = _rows_map(client, tid)
            assert list(rows.keys()) == [3]
        finally:
            _drop_idx_and_table(client, sn, idx)

    def test_delete_full_scan(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("DELETE FROM t WHERE val > 150", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 2

            rows = _rows_map(client, tid)
            assert list(rows.keys()) == [1]
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_delete_no_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("DELETE FROM t", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 3

            assert len(client.scan(tid)) == 0
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_delete_updates_index(self, client):
        """DELETE retracts the source row; secondary index seek returns empty after delete."""
        sn = "s" + _uid()
        client.create_schema(sn)
        idx = f"{sn}__t__idx_cat_id"
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 100, 10)", schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(cat_id)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            # Confirm row is found via index before delete
            before = client.seek_by_index(tid, col_idx=2, key=10)
            assert before.batch is not None and len(before.batch.pk_lo) == 1

            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)

            # After delete the index should return empty
            after = client.seek_by_index(tid, col_idx=2, key=10)
            empty = after.batch is None or len(after.batch.pk_lo) == 0
            assert empty
        finally:
            _drop_idx_and_table(client, sn, idx)

    def test_update_then_view_reflects(self, client):
        """UPDATE delta propagates through a DBSP view circuit."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val > 50",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "v")

            # Insert after view creation so rows flow through the circuit
            client.execute_sql("INSERT INTO t VALUES (1, 100), (2, 30)", schema_name=sn)

            # View should contain only row 1 (val=100 > 50)
            assert len(client.scan(vid)) == 1

            # Update row 2: val 30 → 200; delta passes the view filter
            client.execute_sql("UPDATE t SET val = 200 WHERE pk = 2", schema_name=sn)

            # View should now contain both rows
            assert len(client.scan(vid)) == 2
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestStringEdgeCases
# ---------------------------------------------------------------------------

class TestStringEdgeCases:

    def _setup(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("label", gnitz.TypeCode.STRING, is_nullable=True)]
        schema = gnitz.Schema(cols)
        tid = client.create_table(sn, "strs", cols)
        return sn, tid, schema

    def test_string_empty(self, client):
        """Push row with empty string; verify empty string round-trips."""
        sn, tid, schema = self._setup(client)
        try:
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, label="")
            client.push(tid, batch)
            rows = [r for r in client.scan(tid) if r.weight > 0]
            assert len(rows) == 1
            assert rows[0].label == ""
        finally:
            client.drop_table(sn, "strs")
            client.drop_schema(sn)

    def test_string_12byte_boundary(self, client):
        """Push row with exactly 12-char string (inline boundary); verify round-trip."""
        sn, tid, schema = self._setup(client)
        try:
            s12 = "abcdefghijkl"  # exactly 12 bytes
            assert len(s12) == 12
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, label=s12)
            client.push(tid, batch)
            rows = [r for r in client.scan(tid) if r.weight > 0]
            assert len(rows) == 1
            assert rows[0].label == s12
        finally:
            client.drop_table(sn, "strs")
            client.drop_schema(sn)

    def test_string_long(self, client):
        """Push row with string > 12 chars (heap allocation); verify round-trip."""
        sn, tid, schema = self._setup(client)
        try:
            long_s = "this_is_a_longer_string_value"
            assert len(long_s) > 12
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, label=long_s)
            client.push(tid, batch)
            rows = [r for r in client.scan(tid) if r.weight > 0]
            assert len(rows) == 1
            assert rows[0].label == long_s
        finally:
            client.drop_table(sn, "strs")
            client.drop_schema(sn)

    def test_string_null(self, client):
        """Push row with null label; verify row.label is None in scan result."""
        sn, tid, schema = self._setup(client)
        try:
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, label=None)
            client.push(tid, batch)
            rows = [r for r in client.scan(tid) if r.weight > 0]
            assert len(rows) == 1
            assert rows[0].label is None
        finally:
            client.drop_table(sn, "strs")
            client.drop_schema(sn)
