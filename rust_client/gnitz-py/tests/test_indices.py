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
            for i in range(len(batch_obj.pk_lo)):
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
            for i in range(len(batch_obj.pk_lo)):
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
                for i in range(len(batch_obj.pk_lo)):
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
            assert len(result.batch.pk_lo) == 1
            assert result.batch.pk_lo[0] == 1
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
            assert result.batch is None or len(result.batch.pk_lo) == 0
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
            assert len(result.batch.pk_lo) == 1
            assert result.batch.pk_lo[0] == 10
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
            assert len(result.batch.pk_lo) == 1
            assert result.batch.pk_lo[0] == 5
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
            assert len(rows.batch.pk_lo) == 1
            assert rows.batch.pk_lo[0] == 1
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
            assert results[0]["rows"].batch.pk_lo[0] == 7
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
            assert len(rows.batch.pk_lo) == 1
            assert rows.batch.pk_lo[0] == 1
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
            assert len(results[0]["rows"].batch.pk_lo) == 0
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
            assert len(result.batch.pk_lo) == 1
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
            # Re-insert same PK with same value — UPSERT, not a violation
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
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
            client.execute_sql("INSERT INTO t VALUES (1, 99)", schema_name=sn)
            # Verify final state
            tid, _ = client.resolve_table(sn, "t")
            result = client.scan(tid)
            assert result.batch is not None
            assert len(result.batch.pk_lo) == 1
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    @pytest.mark.xfail(
        _NUM_WORKERS > 1,
        reason="Row modification (INSERT-as-UPSERT or UPDATE) changing a unique-indexed "
               "column to a value held by a row on a different worker is not caught. "
               "The pre-push check skips existing PKs, and the worker-level check only "
               "sees its local index partition. Requires index re-partitioning or a "
               "two-phase protocol to fix.",
    )
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

    @pytest.mark.xfail(
        _NUM_WORKERS > 1,
        reason="UPDATE changing a unique-indexed column to a value held by a row on a "
               "different worker is not caught. Same root cause as "
               "test_unique_upsert_to_existing_value — UPDATE is implemented as a "
               "push with the same PK (weight=+1), triggering the UPSERT path.",
    )
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
            assert len(rows.batch.pk_lo) == 1
            assert rows.batch.pk_lo[0] == 1
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
                assert len(rows.batch.pk_lo) == 1
                assert rows.batch.pk_lo[0] == i + 1
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])
