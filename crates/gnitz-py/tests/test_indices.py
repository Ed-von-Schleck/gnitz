"""Secondary index tests: DDL, seek, SQL-layer indexed lookups, and integrity."""
import os
import pytest
import gnitz
from uuid import uuid4

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))


def _sn():
    """Unique schema name for test isolation."""
    return "idx" + uuid4().hex[:8]


def _drop_all(client, sn, tables=(), views=(), indices=()):
    """Drop tables, views, and indices before dropping schema."""
    for idx in indices:
        try:
            client.execute_sql(f"DROP INDEX {idx}", schema_name=sn)
        except Exception:
            pass
    for v in views:
        try:
            client.execute_sql(f"DROP VIEW {v}", schema_name=sn)
        except Exception:
            pass
    for t in tables:
        try:
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    client.drop_schema(sn)


def _table_has_index(client, sn, table):
    """True if any live IdxTab row names `table` as its owner."""
    from gnitz.core import IDX_TAB
    _, batch_obj, _ = client._client.scan(IDX_TAB)
    if batch_obj is None:
        return False
    tid, _ = client.resolve_table(sn, table)
    for i in range(len(batch_obj.pks)):
        if batch_obj.weights[i] <= 0:
            continue
        if batch_obj.columns[1][i] == tid:
            return True
    return False


# ---------------------------------------------------------------------------
# TestIndexDdl
# ---------------------------------------------------------------------------

class TestIndexDdl:
    def test_create_index(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, cust_id BIGINT NOT NULL)",
                schema_name=sn,
            )
            results = client.execute_sql(
                "CREATE INDEX ON t(cust_id)",
                schema_name=sn,
            )
            assert len(results) == 1
            r = results[0]
            assert r["type"] == "IndexCreated"
            assert r["index_id"] > 0

            # Verify IdxTab row exists with correct owner_id and source_col_idx
            from gnitz.core import IDX_TAB
            raw = client._client.scan(IDX_TAB)
            schema_obj, batch_obj, _ = raw
            assert batch_obj is not None
            found = False
            tid, _ = client.resolve_table(sn, "t")
            for i in range(len(batch_obj.pks)):
                if batch_obj.weights[i] <= 0:
                    continue
                owner_id = batch_obj.columns[1][i] if batch_obj.columns[1] else None
                src_col  = batch_obj.columns[3][i] if batch_obj.columns[3] else None
                if owner_id == tid and src_col == 1:
                    found = True
                    break
            assert found, "IdxTab row not found"
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_cust_id"],
                      tables=["t"])

    def test_create_unique_index(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            results = client.execute_sql(
                "CREATE UNIQUE INDEX ON t(val)",
                schema_name=sn,
            )
            assert results[0]["type"] == "IndexCreated"
            # is_unique flag should be 1 in IdxTab
            from gnitz.core import IDX_TAB
            raw = client._client.scan(IDX_TAB)
            _, batch_obj, _ = raw
            assert batch_obj is not None
            tid, _ = client.resolve_table(sn, "t")
            for i in range(len(batch_obj.pks)):
                if batch_obj.weights[i] <= 0:
                    continue
                owner_id = batch_obj.columns[1][i]
                if owner_id == tid:
                    is_unique = batch_obj.columns[5][i]
                    assert is_unique == 1
                    break
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_drop_index(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
            index_name = f"{sn}__t__idx_val"
            results = client.execute_sql(f"DROP INDEX {index_name}", schema_name=sn)
            assert results[0]["type"] == "Dropped"

            # Verify row is gone from IdxTab
            from gnitz.core import IDX_TAB
            _, batch_obj, _ = client._client.scan(IDX_TAB)
            if batch_obj is not None:
                tid, _ = client.resolve_table(sn, "t")
                for i in range(len(batch_obj.pks)):
                    if batch_obj.weights[i] <= 0:
                        continue
                    assert batch_obj.columns[1][i] != tid, "IdxTab row should be gone"
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_create_index_float_col(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, price FLOAT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE INDEX ON t(price)", schema_name=sn)
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_create_index_string_col(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, name VARCHAR NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE INDEX ON t(name)", schema_name=sn)
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_create_index_nonexistent_col(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE INDEX ON t(ghost)", schema_name=sn)
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_drop_nonexistent_index(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "DROP INDEX nonexistent__t__idx_col", schema_name=sn
                )
        finally:
            client.drop_schema(sn)

    def test_index_id_increments(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            r1 = client.execute_sql("CREATE INDEX ON t(a)", schema_name=sn)
            r2 = client.execute_sql("CREATE INDEX ON t(b)", schema_name=sn)
            id1 = r1[0]["index_id"]
            id2 = r2[0]["index_id"]
            assert id1 != id2
            assert id2 > id1
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_a", f"{sn}__t__idx_b"],
                      tables=["t"])


# ---------------------------------------------------------------------------
# TestIndexSeek  — raw API, bypasses the SQL layer
# ---------------------------------------------------------------------------

class TestIndexSeek:
    def _setup(self, client, sn):
        """Create schema and table t(pk BIGINT PK, cust_id BIGINT). Returns tid."""
        client.create_schema(sn)
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, cust_id BIGINT NOT NULL)",
            schema_name=sn,
        )
        tid, _ = client.resolve_table(sn, "t")
        return tid

    def test_seek_hit(self, client):
        sn = _sn()
        try:
            tid = self._setup(client, sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 42), (2, 99)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)

            result = client.seek_by_index(tid, col_idx=1, key=42)
            assert result.batch is not None
            assert len(result.batch.pks) == 1
            assert result.batch.pks[0] == 1
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_cust_id"],
                      tables=["t"])

    def test_seek_miss(self, client):
        sn = _sn()
        try:
            tid = self._setup(client, sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 42), (2, 99)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)

            result = client.seek_by_index(tid, col_idx=1, key=999)
            assert result.batch is None or len(result.batch.pks) == 0
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_cust_id"],
                      tables=["t"])

    def test_seek_backfill(self, client):
        """Index created after rows exist — must backfill correctly."""
        sn = _sn()
        try:
            tid = self._setup(client, sn)
            client.execute_sql(
                "INSERT INTO t VALUES (10, 77), (20, 88)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)

            result = client.seek_by_index(tid, col_idx=1, key=77)
            assert result.batch is not None
            assert len(result.batch.pks) == 1
            assert result.batch.pks[0] == 10
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_cust_id"],
                      tables=["t"])

    def test_seek_after_insert(self, client):
        """Rows inserted after index creation are visible via seek."""
        sn = _sn()
        try:
            tid = self._setup(client, sn)
            client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (5, 55)", schema_name=sn)

            result = client.seek_by_index(tid, col_idx=1, key=55)
            assert result.batch is not None
            assert len(result.batch.pks) == 1
            assert result.batch.pks[0] == 5
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_cust_id"],
                      tables=["t"])


# ---------------------------------------------------------------------------
# TestIndexSql  — SQL-layer indexed SELECT
# ---------------------------------------------------------------------------

class TestIndexSql:
    def test_select_where_indexed_col(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, cust_id BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 42), (2, 99)", schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)

            results = client.execute_sql(
                "SELECT * FROM t WHERE cust_id = 42", schema_name=sn
            )
            assert results[0]["type"] == "Rows"
            rows = results[0]["rows"]
            assert len(rows.batch.pks) == 1
            assert rows.batch.pks[0] == 1
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_cust_id"],
                      tables=["t"])

    def test_select_where_pk(self, client):
        """PK-based seek still works after index infrastructure is present."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (7, 100)", schema_name=sn)

            results = client.execute_sql(
                "SELECT * FROM t WHERE pk = 7", schema_name=sn
            )
            assert results[0]["type"] == "Rows"
            assert results[0]["rows"].batch.pks[0] == 7
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_select_nonindexed_col_rejects(self, client):
        """WHERE on a column with no index raises an error."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("SELECT * FROM t WHERE val = 5", schema_name=sn)
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_select_indexed_with_residual_filter(self, client):
        """Index seek combined with a residual AND predicate filters correctly."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, cust_id BIGINT NOT NULL, region BIGINT NOT NULL)",
                schema_name=sn,
            )
            # Only pk=1 has cust_id=42; pk=2 has a different cust_id
            client.execute_sql(
                "INSERT INTO t VALUES (1, 42, 100), (2, 99, 200)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)

            results = client.execute_sql(
                "SELECT * FROM t WHERE cust_id = 42 AND region = 100",
                schema_name=sn,
            )
            assert results[0]["type"] == "Rows"
            rows = results[0]["rows"]
            assert len(rows.batch.pks) == 1
            assert rows.batch.pks[0] == 1
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_cust_id"],
                      tables=["t"])

    def test_select_indexed_no_match(self, client):
        """Seek on an indexed column returns empty Rows when key is absent."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)

            results = client.execute_sql(
                "SELECT * FROM t WHERE val = 999", schema_name=sn
            )
            assert results[0]["type"] == "Rows"
            assert len(results[0]["rows"].batch.pks) == 0
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_select_where_indexed_col_negative_i64(self, client):
        """WHERE bigint_col = -N must use the index and find the correct row."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, score BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, -5), (2, -1), (3, 0), (4, 10)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(score)", schema_name=sn)

            for pk, val in [(1, -5), (2, -1), (3, 0), (4, 10)]:
                results = client.execute_sql(
                    f"SELECT * FROM t WHERE score = {val}", schema_name=sn
                )
                assert results[0]["type"] == "Rows", f"expected Rows for score={val}"
                rows = results[0]["rows"]
                assert len(rows.batch.pks) == 1, f"expected 1 row for score={val}"
                assert rows.batch.pks[0] == pk, f"wrong pk for score={val}"
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_score"],
                      tables=["t"])

    def test_select_where_indexed_col_negative_i32(self, client):
        """WHERE int_col = -N must use the index and find the correct row (I32)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, score INT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, -100), (2, -1), (3, 0), (4, 100)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(score)", schema_name=sn)

            for pk, val in [(1, -100), (2, -1), (3, 0), (4, 100)]:
                results = client.execute_sql(
                    f"SELECT * FROM t WHERE score = {val}", schema_name=sn
                )
                assert results[0]["type"] == "Rows", f"expected Rows for score={val}"
                rows = results[0]["rows"]
                assert len(rows.batch.pks) == 1, f"expected 1 row for score={val}"
                assert rows.batch.pks[0] == pk, f"wrong pk for score={val}"
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_score"],
                      tables=["t"])

    def test_select_where_indexed_col_negative_miss(self, client):
        """WHERE bigint_col = -N returns empty when the value is absent."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)

            results = client.execute_sql(
                "SELECT * FROM t WHERE val = -99", schema_name=sn
            )
            assert results[0]["type"] == "Rows"
            assert len(results[0]["rows"].batch.pks) == 0
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])


# ---------------------------------------------------------------------------
# TestIndexIntegrity
# ---------------------------------------------------------------------------

class TestIndexIntegrity:
    def test_unique_index_violation(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (2, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_column_unique_constraint_enforced(self, client):
        """A column-level UNIQUE in CREATE TABLE creates a unique index and
        rejects duplicate values end-to-end (the silent-discard defect)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, val BIGINT UNIQUE)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO u VALUES (1, 1)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO u VALUES (2, 1)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__u__idx_val"],
                      tables=["u"])

    def test_unique_index_anticorrelated_multi_source(self, client):
        """Unique-constraint check must hold across an anti-correlated,
        multi-source index.

        The duplicate check seeks the indexed value in the secondary index,
        whose PK is compound ``(value, src_pk)``. With several distinct values
        inserted one-per-push (anti-correlated: value falls as pk rises), the
        check merges N runs. If that merge orders by the raw u128 PK
        (``(src_pk, value)``) instead of column order (``(value, src_pk)``),
        the seek for a smaller value lands on a larger value's run, reports
        "absent", and a duplicate slips through. Every duplicate here must be
        rejected.
        """
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY,"
                " val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            n = 6
            vals = [(n - i) * 100 for i in range(n)]  # 600,500,...,100
            for i, val in enumerate(vals):
                client.execute_sql(
                    f"INSERT INTO t VALUES ({i + 1}, {val})", schema_name=sn)
            # Re-inserting any existing value (with a fresh pk) must be
            # rejected — including the smallest values, which under a u128
            # merge would be masked by a larger value's run.
            for j, val in enumerate(vals):
                try:
                    client.execute_sql(
                        f"INSERT INTO t VALUES ({1000 + j}, {val})",
                        schema_name=sn,
                    )
                    raise AssertionError(
                        f"duplicate val={val} was accepted — unique check "
                        f"missed it (index merge order bug)"
                    )
                except gnitz.GnitzError:
                    pass  # expected: duplicate rejected
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_create_view_with_filter(self, client):
        """CREATE VIEW with a WHERE clause succeeds."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            results = client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val > 10",
                schema_name=sn,
            )
            assert results[0]["type"] == "ViewCreated"
        finally:
            _drop_all(client, sn, views=["v"], tables=["t"])

    def test_scan_view(self, client):
        """Rows inserted into a source table appear in an associated view."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val > 10",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 5)", schema_name=sn)

            vid, _ = client.resolve_table(sn, "v")
            result = client.scan(vid)
            assert result.batch is not None
            assert len(result.batch.pks) == 1
        finally:
            _drop_all(client, sn, views=["v"], tables=["t"])

    def test_unique_cross_partition(self, client):
        """Two separate INSERTs with different PKs but same indexed value must fail."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                # pk=1000000 likely lands on a different partition than pk=1
                client.execute_sql("INSERT INTO t VALUES (1000000, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_batch_internal_duplicate(self, client):
        """Single INSERT with two rows sharing the same indexed value must fail."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "INSERT INTO t VALUES (1, 42), (2, 42)", schema_name=sn
                )
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_upsert_same_value_allowed(self, client):
        """UPSERT with same indexed value must succeed (not a violation)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            # Re-insert same PK with same value via explicit UPSERT
            client.execute_sql(
                "INSERT INTO t VALUES (1, 42) "
                "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
                schema_name=sn,
            )
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_upsert_change_value(self, client):
        """UPSERT that changes the indexed value must succeed."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 99) "
                "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
                schema_name=sn,
            )
            # Verify final state
            tid, _ = client.resolve_table(sn, "t")
            result = client.scan(tid)
            assert result.batch is not None
            assert len(result.batch.pks) == 1
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_upsert_to_existing_value(self, client):
        """UPSERT pk=1 to val=99 when pk=2 already holds val=99 must fail."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, 99)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                # UPSERT pk=1 to val=99 — conflicts with pk=2
                client.execute_sql("INSERT INTO t VALUES (1, 99)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_update_to_existing_value(self, client):
        """UPDATE pk=1 SET val=99 when pk=2 already holds val=99 must fail."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, 99)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "UPDATE t SET val = 99 WHERE pk = 1", schema_name=sn
                )
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_concurrent_buffered(self, server):
        """Two back-to-back inserts with the same unique value must fail.

        The second insert arrives before any scan forces a flush, so both
        writes would be buffered without this fix — the TOCTOU window.
        With the fix, flush_pending_for_tid drains the first insert before
        validating the second.
        """
        sn = _sn()
        with gnitz.connect(server) as c1, gnitz.connect(server) as c2:
            c1.create_schema(sn)
            try:
                c1.execute_sql(
                    "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                    schema_name=sn,
                )
                c1.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
                # First insert via c1 — may be buffered, not yet flushed to workers.
                c1.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
                # Second insert via c2 — must detect the conflict.
                with pytest.raises(gnitz.GnitzError):
                    c2.execute_sql("INSERT INTO t VALUES (2, 42)", schema_name=sn)
            finally:
                _drop_all(c1, sn,
                          indices=[f"{sn}__t__idx_val"],
                          tables=["t"])

    def test_unique_null_not_violation(self, client):
        """Multiple NULLs in a unique-indexed nullable column must not conflict."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, NULL)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, NULL)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_after_delete(self, client):
        """Inserting a value freed by a prior DELETE must succeed."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            # val=42 is now free
            client.execute_sql("INSERT INTO t VALUES (2, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_multi_index(self, client):
        """Table with two unique indices; violation on first index is caught."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(b)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 10, 20)", schema_name=sn)
            # Unique on both a and b
            client.execute_sql("INSERT INTO t VALUES (2, 11, 21)", schema_name=sn)
            # Violates unique on a
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (3, 10, 22)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_a", f"{sn}__t__idx_b"],
                      tables=["t"])

    def test_unique_upsert_intra_batch_duplicate_new_value(self, client):
        """Two UPSERT rows (both existing PKs) set the same NEW unique value in
        one batch. The new value is absent from committed storage, so the
        per-row holder seek never fires; only the in-batch duplicate check can
        catch it. Must raise — otherwise the unique index is silently corrupted
        with two rows holding the same value."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                # Both pk=1 and pk=2 already exist; both UPSERT to val=99,
                # which is not present in storage.
                client.execute_sql(
                    "INSERT INTO t VALUES (1, 99), (2, 99) "
                    "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
                    schema_name=sn,
                )
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])


# ---------------------------------------------------------------------------
# TestIndexReadBarrier
# ---------------------------------------------------------------------------

class TestIndexReadBarrier:
    """Validates that index seeks after pushes see fresh data.

    The server fires pending ticks before index seeks to ensure derived
    index views are up-to-date.
    """

    def test_index_seek_immediately_after_push(self, client):
        """Push a row, then immediately seek by index — must find it."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY,"
                " val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 42)", schema_name=sn)
            results = client.execute_sql(
                "SELECT * FROM t WHERE val = 42", schema_name=sn)
            assert results[0]["type"] == "Rows"
            rows = results[0]["rows"]
            assert len(rows.batch.pks) == 1
            assert rows.batch.pks[0] == 1
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_index_seek_multiple_pushes(self, client):
        """Push several rows, seek by index for each — all found."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY,"
                " val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
            for i in range(5):
                client.execute_sql(
                    f"INSERT INTO t VALUES ({i + 1}, {(i + 1) * 100})",
                    schema_name=sn,
                )
            for i in range(5):
                results = client.execute_sql(
                    f"SELECT * FROM t WHERE val = {(i + 1) * 100}",
                    schema_name=sn,
                )
                assert results[0]["type"] == "Rows"
                rows = results[0]["rows"]
                assert len(rows.batch.pks) == 1
                assert rows.batch.pks[0] == i + 1
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_index_seek_anticorrelated_multi_source(self, client):
        """Index seek must hold when the indexed value and source PK are
        ANTI-correlated across many pushes.

        A secondary index has a compound PK ``(indexed_value, src_pk)``. The
        read-cursor's N-way merge must order it the same way storage sorts
        each run — column order ``(value, src_pk)`` — not by the raw u128
        view of the PK bytes (which would order by ``(src_pk, value)`` because
        src_pk lands in the high 64 bits). Correlated data (value rising with
        pk) hides the difference; anti-correlated data (value falling as pk
        rises) makes the two orders disagree, so a wrong merge order seeks the
        wrong run's head and the lookup misses.
        """
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY,"
                " val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
            n = 6
            # Each INSERT is a separate push -> a separate index run, so the
            # lookup below merges N anti-correlated sources.
            for i in range(n):
                pk = i + 1
                val = (n - i) * 100  # pk=1->600, pk=2->500, ... pk=6->100
                client.execute_sql(
                    f"INSERT INTO t VALUES ({pk}, {val})", schema_name=sn)
            for i in range(n):
                pk = i + 1
                val = (n - i) * 100
                results = client.execute_sql(
                    f"SELECT * FROM t WHERE val = {val}", schema_name=sn)
                assert results[0]["type"] == "Rows"
                rows = results[0]["rows"]
                assert len(rows.batch.pks) == 1, \
                    f"val={val} (pk={pk}) not found — index merge order bug"
                assert rows.batch.pks[0] == pk, \
                    f"val={val} resolved to pk={rows.batch.pks[0]}, expected {pk}"
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])


# ---------------------------------------------------------------------------
# TestCreateUniqueIndexValidation
#
# CREATE UNIQUE INDEX over a table that ALREADY holds rows must validate
# uniqueness globally (across all workers) on the master BEFORE broadcasting
# the index. Uniqueness is global, but the per-worker backfill only sees one
# partition: without the master pre-flight a within-partition duplicate would
# fatally _exit every affected worker, and a cross-partition duplicate would be
# silently accepted. Distinct from TestIndexIntegrity, which creates the index
# FIRST and exercises the INSERT-time enforcement path.
# ---------------------------------------------------------------------------

class TestCreateUniqueIndexValidation:
    def test_within_partition_duplicate_rejected_cluster_survives(self, client):
        """Many rows share one non-PK value across consecutive PKs, so several
        land on the SAME worker (pigeonhole over the worker count). CREATE
        UNIQUE INDEX must return a clean error — and crucially every worker must
        stay alive and answer a follow-up scan. (Pre-fix: each worker holding a
        within-partition duplicate _exits, wedging the cluster.)"""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            # 50 rows, all val=42, PKs 1..50. With >1 worker, pigeonhole forces
            # at least two duplicates onto one worker; with 1 worker all are.
            rows = ", ".join(f"({pk}, 42)" for pk in range(1, 51))
            client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=sn)

            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)

            # Cluster survived: a full-table scan fans out to every worker and
            # must return all 50 rows.
            tid, _ = client.resolve_table(sn, "t")
            result = client.scan(tid)
            assert result.batch is not None
            assert len(result.batch.pks) == 50, "all workers must answer the scan"
            # No phantom constraint: the index was never created.
            assert not _table_has_index(client, sn, "t")
            # And the write path is still healthy across workers.
            client.execute_sql("INSERT INTO t VALUES (51, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_cross_partition_duplicate_rejected(self, client):
        """Rows sharing one non-PK value spread across a wide PK range land on
        different workers; the duplicate is invisible to any single worker's
        backfill but must be caught by the master pre-flight. The index must NOT
        exist afterward and a fresh duplicate INSERT must still be permitted (no
        phantom constraint). (Pre-fix: silently succeeds.)"""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            spread = [1, 7, 13, 1000, 99999, 123456, 777777, 8888888, 73501234, 901234567]
            rows = ", ".join(f"({pk}, 42)" for pk in spread)
            client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=sn)

            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)

            assert not _table_has_index(client, sn, "t"), \
                "no index may exist after a rejected CREATE UNIQUE INDEX"
            # No phantom constraint: another duplicate value is still accepted.
            client.execute_sql("INSERT INTO t VALUES (2, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_column_succeeds_and_enforces(self, client):
        """CREATE UNIQUE INDEX on an all-distinct column succeeds; a subsequent
        duplicate INSERT is rejected by the steady-state path, and an indexed
        lookup returns the right row."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            rows = ", ".join(f"({pk}, {pk * 10})" for pk in range(1, 31))
            client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=sn)

            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")

            # Steady-state enforcement: re-inserting an existing value fails.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (999, 100)", schema_name=sn)
            # A distinct value still inserts.
            client.execute_sql("INSERT INTO t VALUES (999, 99999)", schema_name=sn)

            # Indexed lookup returns the correct row.
            results = client.execute_sql(
                "SELECT * FROM t WHERE val = 200", schema_name=sn)
            assert results[0]["type"] == "Rows"
            rows = results[0]["rows"]
            assert len(rows.batch.pks) == 1
            assert rows.batch.pks[0] == 20
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_multiple_nulls_allowed(self, client):
        """A nullable non-PK column with several NULLs is valid under SQL UNIQUE;
        CREATE UNIQUE INDEX must succeed, and a later duplicate non-NULL value is
        still rejected."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                schema_name=sn,
            )
            # Several NULLs plus distinct non-NULL values, spread across workers.
            client.execute_sql(
                "INSERT INTO t VALUES (1, NULL), (5, NULL), (9, NULL), "
                "(100000, 10), (200000, 20), (300000, 30)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")

            # More NULLs are still fine after creation.
            client.execute_sql("INSERT INTO t VALUES (13, NULL)", schema_name=sn)
            # A duplicate non-NULL value is rejected.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (400000, 10)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_empty_table_succeeds_and_enforces(self, client):
        """CREATE UNIQUE INDEX on an empty table succeeds and enforces
        uniqueness on later inserts."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")

            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (1000000, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_non_unique_index_on_duplicate_column_succeeds(self, client):
        """The pre-flight is gated on is_unique: a NON-unique index over a
        column full of duplicates must still be created without error."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            rows = ", ".join(f"({pk}, 42)" for pk in range(1, 41))
            client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=sn)
            # Non-unique index over an all-duplicate column: no validation.
            results = client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
            assert results[0]["type"] == "IndexCreated"
            assert _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_larger_table_passing_and_failing_keep_cluster_alive(self, client):
        """A larger table (rows fanned across all workers) for both a passing
        and a failing CREATE UNIQUE INDEX; the cluster must stay alive either
        way. (The drain-all-frames-per-worker wedge safety on a multi-frame scan
        train is exercised directly by the drain_index_scan Rust unit test; the
        256 MiB W2M frame ceiling makes a true multi-frame scan impractical to
        provoke from E2E, so this guards the realistic large-table path.)"""
        sn = _sn()
        client.create_schema(sn)
        n = 3000
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            # Distinct values across a wide PK spread → all workers populated.
            batch = ", ".join(f"({pk}, {pk})" for pk in range(1, n + 1))
            client.execute_sql(f"INSERT INTO t VALUES {batch}", schema_name=sn)

            # Passing: all-distinct → index created.
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")
            client.execute_sql(f"DROP INDEX {sn}__t__idx_val", schema_name=sn)

            # Failing: introduce one duplicate, then CREATE must reject and the
            # cluster must remain alive.
            client.execute_sql(f"INSERT INTO t VALUES ({n + 1}, 1)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)

            tid, _ = client.resolve_table(sn, "t")
            result = client.scan(tid)
            assert result.batch is not None
            assert len(result.batch.pks) == n + 1, "all workers must answer post-failure"
            assert not _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_concurrent_inserts_during_create(self, server):
        """A steady INSERT stream into the owner table concurrent with CREATE
        UNIQUE INDEX. All streamed values are distinct, so — whatever the
        interleaving — the catalog write lock orders every INSERT strictly
        before or after the pre-flight+backfill snapshot, the index must be
        created and enforce, no row may be lost from it, and no worker may
        wedge. Guards the snapshot-vs-backfill ordering."""
        import threading
        sn = _sn()
        with gnitz.connect(server) as c_writer, gnitz.connect(server) as c_ddl:
            c_ddl.create_schema(sn)
            try:
                c_ddl.execute_sql(
                    "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                    schema_name=sn,
                )
                # Seed so the pre-flight scan has data on every worker.
                seed = ", ".join(f"({i}, {i})" for i in range(1, 201))
                c_ddl.execute_sql(f"INSERT INTO t VALUES {seed}", schema_name=sn)

                stop = threading.Event()
                errors = []

                def insert_stream():
                    v = 1000
                    try:
                        while not stop.is_set() and v < 5000:
                            c_writer.execute_sql(
                                f"INSERT INTO t VALUES ({v}, {v})", schema_name=sn)
                            v += 1
                    except Exception as e:  # noqa: BLE001 — surfaced via `errors`
                        errors.append(e)

                th = threading.Thread(target=insert_stream)
                th.start()
                try:
                    c_ddl.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
                finally:
                    stop.set()
                    th.join(timeout=60)
                assert not th.is_alive(), "insert stream did not stop"
                assert not errors, f"streaming inserts errored: {errors}"
                assert _table_has_index(c_ddl, sn, "t")

                # Index enforces a seeded value, and the cluster is alive.
                with pytest.raises(gnitz.GnitzError):
                    c_ddl.execute_sql("INSERT INTO t VALUES (888888, 1)", schema_name=sn)
                # A brand-new distinct value still inserts (no spurious reject).
                c_ddl.execute_sql("INSERT INTO t VALUES (888888, 888888)", schema_name=sn)
            finally:
                _drop_all(c_ddl, sn,
                          indices=[f"{sn}__t__idx_val"],
                          tables=["t"])


# ---------------------------------------------------------------------------
# TestAtomicUniqueTransfers
#
# A single delta batch may rearrange unique values among rows so the post-batch
# state is unique, even though validation runs pre-apply against committed
# storage. The distributed validator must accept atomic transfers/swaps/bulk
# shifts while rejecting genuine and forged-retraction duplicates, and must
# reject a same-value insert on a non-unique_pk table (which it previously
# misclassified as a same-PK upsert and silently accepted).
# ---------------------------------------------------------------------------

class TestAtomicUniqueTransfers:
    def _raw_table(self, client, sn, cols, unique_pk=True):
        """Create a raw table named `t` + a SQL unique index on `val`.
        Returns (tid, schema)."""
        schema = gnitz.Schema(cols)
        tid = client.create_table(sn, "t", cols, unique_pk=unique_pk)
        client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
        return tid, schema

    def test_sql_bulk_shift_accepted(self, client):
        """`UPDATE t SET val = val + 1` on a dense sequence ships one batch of
        same-PK +1 upserts with no retraction; the holder of each new value is
        the previous row, itself upserted off it. Must succeed (form 2)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT UNIQUE)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)", schema_name=sn)
            client.execute_sql("UPDATE t SET val = val + 1", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            rows = sorted((r.pk, r.val) for r in client.scan(tid) if r.weight > 0)
            assert rows == [(1, 2), (2, 3), (3, 4)], rows
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_sql_bulk_swap_accepted(self, client):
        """`UPDATE t SET val = 3 - val` swaps two rows' unique values in one batch
        of same-PK +1 upserts. Must succeed (form 2)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT UNIQUE)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 1), (2, 2)", schema_name=sn)
            client.execute_sql("UPDATE t SET val = 3 - val", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            rows = sorted((r.pk, r.val) for r in client.scan(tid) if r.weight > 0)
            assert rows == [(1, 2), (2, 1)], rows
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_raw_transfer_accepted(self, client):
        """One batch {retract P1/5, insert P2/5} moves a unique value to a fresh
        PK; the committed holder P1 releases it via explicit retraction. Must
        succeed (form 1)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            tid, schema = self._raw_table(client, sn, cols)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=5)
            client.push(tid, b)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=5, weight=-1)
            b.append(pk=2, val=5, weight=1)
            client.push(tid, b)  # must succeed
            rows = sorted((r.pk, r.val) for r in client.scan(tid) if r.weight > 0)
            assert rows == [(2, 5)], rows
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_raw_genuine_duplicate_rejected(self, client):
        """A fresh-PK insertion of a still-held value with no retraction freeing
        it is a genuine duplicate — rejected."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            tid, schema = self._raw_table(client, sn, cols)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=5)
            client.push(tid, b)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=3, val=5, weight=1)  # no retraction frees val=5
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, b)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_raw_forged_retraction_rejected(self, client):
        """The exemption keys on (holder PK, value): a retraction naming a
        non-holder must not let a real duplicate through."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            tid, schema = self._raw_table(client, sn, cols)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=5)  # only pk=1 holds 5
            client.push(tid, b)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=3, val=5, weight=-1)  # forged: pk=3 does not hold 5
            b.append(pk=2, val=5, weight=1)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, b)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_raw_forged_retraction_freed_by_upserted_holder_rejected(self, client):
        """`{P2/7@+1, P4/6@+1, P3/6@-1}` over committed `P2/6`: P4/6 is a fresh
        insert routed to the verify by 6 ∈ retracted_vals. The holder P2 is
        upserted (to 7), but P4 is not an upsert (is_upsert gate) and the
        retraction names P3, so neither exemption fires — rejected."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            tid, schema = self._raw_table(client, sn, cols)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=2, val=6)  # committed holder of 6
            client.push(tid, b)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=2, val=7, weight=1)   # P2 upserted off 6
            b.append(pk=4, val=6, weight=1)   # fresh insert of 6
            b.append(pk=3, val=6, weight=-1)  # forged retraction names P3
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, b)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_non_unique_pk_same_value_rejected(self, client):
        """A second live row at an already-held PK and value on a non-unique_pk
        table must be rejected (form 3); the distributed validator previously
        misclassified it as a same-PK upsert and committed two live holders."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("id", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64),
                    gnitz.ColumnDef("data", gnitz.TypeCode.I64)]
            tid, schema = self._raw_table(client, sn, cols, unique_pk=False)
            b = gnitz.ZSetBatch(schema)
            b.append(id=1, val=5, data=100)
            client.push(tid, b)
            b = gnitz.ZSetBatch(schema)
            b.append(id=1, val=5, data=200, weight=1)  # second live row at val=5
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, b)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])


# ---------------------------------------------------------------------------
# TestUniqueIndexCreatePreflight
#
# CREATE UNIQUE INDEX over PRE-EXISTING data runs a global pre-flight: each
# worker sorts its partition's index keys and streams them to the master,
# which k-way-merges the sorted streams and rejects on any adjacent-equal
# pair — catching within-partition and cross-partition duplicates alike
# before anything is committed or broadcast. These tests plant data BEFORE
# the CREATE (the steady-state INSERT-path tests above plant it after).
# ---------------------------------------------------------------------------

class TestUniqueIndexCreatePreflight:
    def _bigint_table(self, client, sn, val_type="BIGINT NOT NULL"):
        client.execute_sql(
            f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val {val_type})",
            schema_name=sn,
        )

    def _compound_pk_table(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL,"
            " payload BIGINT, PRIMARY KEY (a, b))",
            schema_name=sn,
        )

    def _insert_rows(self, client, sn, rows, chunk=500):
        """Multi-row INSERT of [(pk, val), ...]; 'NULL' passes through."""
        for i in range(0, len(rows), chunk):
            values = ", ".join(f"({pk}, {val})" for pk, val in rows[i:i + chunk])
            client.execute_sql(f"INSERT INTO t VALUES {values}", schema_name=sn)

    def test_preexisting_cross_partition_duplicate_rejected(self, client):
        """One value held by many PKs spans partitions (and, by pigeonhole
        over <=8 workers, repeats within at least one); the merge must reject
        either way, and no index may be created."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            self._insert_rows(client, sn, [(pk, 42) for pk in range(1, 9)])
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t"), \
                "failed pre-flight must not create the index"
            # The failed DDL must leave the table fully usable.
            client.execute_sql("INSERT INTO t VALUES (100, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_preexisting_duplicate_among_many_rows_rejected(self, client):
        """A single planted duplicate pair among hundreds of distinct values
        is found by the merge."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            rows = [(pk, pk * 10) for pk in range(1, 301)]
            rows.append((1000, 1500))  # duplicates val of pk=150
            self._insert_rows(client, sn, rows)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_create_succeeds_at_scale_then_enforces(self, client):
        """A few thousand distinct values across all workers pass the
        pre-flight; the very first post-create INSERT of a duplicate is
        rejected (the merge's seed must be complete, never truncated) while a
        fresh value is accepted."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            self._insert_rows(client, sn, [(pk, pk * 3 + 1) for pk in range(1, 2001)])
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                # duplicates val of pk=500 (500*3+1)
                client.execute_sql("INSERT INTO t VALUES (9001, 1501)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (9002, 0)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_preexisting_nulls_allowed(self, client):
        """SQL UNIQUE permits many NULLs: NULL keys never enter any worker's
        key stream, so a NULL-heavy column passes and stays NULL-insertable."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn, val_type="BIGINT")
            rows = [(pk, "NULL") for pk in range(1, 9)]
            rows += [(pk, pk) for pk in range(100, 108)]
            self._insert_rows(client, sn, rows)
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (200, NULL)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_pk_column_unique_index_short_circuits(self, client):
        """A unique index on the sole PK column of a unique_pk table is
        trivially satisfiable (PK uniqueness is enforced on every ingest
        path): the create succeeds without scanning and the table keeps
        working."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            self._insert_rows(client, sn, [(pk, pk) for pk in range(1, 50)])
            client.execute_sql("CREATE UNIQUE INDEX ON t(pk)", schema_name=sn)
            assert _table_has_index(client, sn, "t")
            client.execute_sql("INSERT INTO t VALUES (1000, 1000)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_pk"], tables=["t"])

    def test_pk_column_on_non_unique_pk_table_takes_full_path(self, client):
        """A table created WITHOUT unique_pk runs no PK-uniqueness enforcement,
        so a sole-PK-column unique index is NOT trivially unique: two live
        rows sharing pk=1 must fail the pre-flight."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            schema = gnitz.Schema(cols)
            tid = client.create_table(sn, "t", cols, unique_pk=False)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=10)
            b.append(pk=1, val=20)  # second live row, same PK, distinct payload
            b.append(pk=2, val=30)
            client.push(tid, b)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(pk)", schema_name=sn)
            assert not _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_compound_pk_member_duplicates_rejected(self, client):
        """A member of a compound PK is not trivially unique: duplicate `a`
        values across distinct (a, b) rows must fail the pre-flight."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._compound_pk_table(client, sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, 10), (1, 2, 20), (3, 1, 30)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)
            assert not _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_compound_pk_member_distinct_accepted(self, client):
        """Distinct values in a compound-PK member pass the full-path scan."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._compound_pk_table(client, sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, 10), (2, 2, 20), (3, 1, 30)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (2, 9, 90)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a"], tables=["t"])

    def test_signed_negative_duplicate_rejected(self, client):
        """Equal negative values must collide despite the merge's u128 order
        not being monotonic in signed value (equal value => equal key is what
        the verdict needs)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            self._insert_rows(client, sn, [(1, -5), (2, 300), (3, -5)])
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_signed_distinct_negatives_accepted_then_enforced(self, client):
        """Distinct negative values pass; the post-create filter then rejects a
        re-insert of one of them (native-key round-trip, no sign confusion)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            self._insert_rows(client, sn, [(1, -1), (2, -2), (3, -3), (4, 0)])
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (10, -2)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (11, -9)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_multi_frame_key_train(self, unique_preflight_frame_server):
        """With the debug seam shrinking frames to 7 keys, every worker streams
        a multi-frame continuation train; the merge must stay exact across
        frame boundaries — both verdicts. No-op against a release server (the
        seam compiles out), where this degenerates to single-frame trains."""
        client = unique_preflight_frame_server
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            self._insert_rows(client, sn, [(pk, pk * 7) for pk in range(1, 201)])
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")
            client.execute_sql(f"DROP INDEX {sn}__t__idx_val", schema_name=sn)

            # Plant one duplicate pair on far-apart PKs and re-create: the
            # equal keys are adjacent in the merged stream wherever the frame
            # boundaries fall.
            client.execute_sql("INSERT INTO t VALUES (1000, 700)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_worker_fault_mid_preflight(self, unique_preflight_fault_server):
        """An injected worker fault during the pre-flight scan must surface as
        a client error with no index created, no filter seeded, and every
        worker drained (not wedged): the table stays fully usable, and the
        PK short-circuit — which never fans out — still succeeds. No-op
        against a release server (the seam compiles out)."""
        client = unique_preflight_fault_server
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            self._insert_rows(client, sn, [(pk, pk) for pk in range(1, 33)])
            try:
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
                seam_active = False  # release server: seam compiled out
            except gnitz.GnitzError:
                seam_active = True
            if seam_active:
                assert not _table_has_index(client, sn, "t")
                # No filter was seeded and no index exists: a duplicate
                # value must be accepted, proving no partial constraint
                # leaked out of the failed DDL.
                client.execute_sql("INSERT INTO t VALUES (100, 1)", schema_name=sn)
                # All workers answer a full scan: nobody is wedged on a
                # half-drained pre-flight train.
                tid, _ = client.resolve_table(sn, "t")
                rows = [r for r in client.scan(tid) if r.weight > 0]
                assert len(rows) == 33
                # The PK short-circuit returns before any fan-out, so it
                # succeeds even while every worker's scan path is faulted.
                client.execute_sql("CREATE UNIQUE INDEX ON t(pk)", schema_name=sn)
                assert _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_pk", f"{sn}__t__idx_val"],
                      tables=["t"])
