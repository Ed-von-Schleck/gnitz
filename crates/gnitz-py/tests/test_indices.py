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
    from gnitz import IDX_TAB
    batch_obj = client.scan(IDX_TAB).batch
    if batch_obj is None:
        return False
    tid, _ = client.resolve_table(sn, table)
    for i in range(len(batch_obj.pks)):
        if batch_obj.weights[i] <= 0:
            continue
        if batch_obj.columns[1][i] == tid:
            return True
    return False


def _insert_rows(client, sn, rows, chunk=500):
    """Multi-row INSERT into table `t` of same-width value tuples, split into
    at-most-`chunk`-row statements; a literal 'NULL' passes through."""
    for i in range(0, len(rows), chunk):
        values = ", ".join(
            f"({', '.join(str(v) for v in r)})" for r in rows[i:i + chunk])
        client.execute_sql(f"INSERT INTO t VALUES {values}", schema_name=sn)


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

            # Verify IdxTab row exists with correct owner_id and source_cols
            # (the packed column-list u64, decoded via the shared codec).
            from gnitz import IDX_TAB, unpack_pk_cols
            batch_obj = client.scan(IDX_TAB).batch
            assert batch_obj is not None
            found = False
            tid, _ = client.resolve_table(sn, "t")
            for i in range(len(batch_obj.pks)):
                if batch_obj.weights[i] <= 0:
                    continue
                owner_id = batch_obj.columns[1][i] if batch_obj.columns[1] else None
                src_col  = batch_obj.columns[3][i] if batch_obj.columns[3] else None
                if owner_id == tid and unpack_pk_cols(src_col) == [1]:
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
            from gnitz import IDX_TAB
            batch_obj = client.scan(IDX_TAB).batch
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
            from gnitz import IDX_TAB
            batch_obj = client.scan(IDX_TAB).batch
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

            result = client.seek_by_index(tid, [1], [42])
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

            result = client.seek_by_index(tid, [1], [999])
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

            result = client.seek_by_index(tid, [1], [77])
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

            result = client.seek_by_index(tid, [1], [55])
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

    def test_select_nonindexed_col_served_by_executor(self, client):
        """WHERE on a column with no index is SERVED, not rejected.

        No index means no thin access path, so the query routes to the on-demand
        executor (compiled to a DBSP circuit, run once over the committed base
        snapshot, streamed back). This used to raise a "no usable index" error.
        """
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 6), (3, 5)", schema_name=sn)
            assert _result_pks(client.execute_sql(
                "SELECT * FROM t WHERE val = 5", schema_name=sn)) == [1, 3]
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

    def test_non_unique_pk_weight2_row_rejected(self, client):
        """A single fresh-value row at weight=2 on a non-unique_pk table is two
        live instances — the same violation as two +1 rows of the value, which
        the in-batch check rejects; the consolidated encoding must not slip."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("id", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            tid, schema = self._raw_table(client, sn, cols, unique_pk=False)
            b = gnitz.ZSetBatch(schema)
            b.append(id=1, val=5, weight=2)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, b)
            # The same row at unit weight passes.
            b = gnitz.ZSetBatch(schema)
            b.append(id=1, val=5, weight=1)
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

    def test_preexisting_cross_partition_duplicate_rejected(self, client):
        """One value held by many PKs spans partitions (and, by pigeonhole
        over <=8 workers, repeats within at least one); the merge must reject
        either way, and no index may be created."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            _insert_rows(client, sn, [(pk, 42) for pk in range(1, 9)])
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
            _insert_rows(client, sn, rows)
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
            _insert_rows(client, sn, [(pk, pk * 3 + 1) for pk in range(1, 2001)])
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
            _insert_rows(client, sn, rows)
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
            _insert_rows(client, sn, [(pk, pk) for pk in range(1, 50)])
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
            _insert_rows(client, sn, [(1, -5), (2, 300), (3, -5)])
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
            _insert_rows(client, sn, [(1, -1), (2, -2), (3, -3), (4, 0)])
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
            _insert_rows(client, sn, [(pk, pk * 7) for pk in range(1, 201)])
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
            _insert_rows(client, sn, [(pk, pk) for pk in range(1, 33)])
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

    def _non_unique_pk_table(self, client, sn):
        """Raw table `t` without unique_pk enforcement. Returns (tid, schema)."""
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tid = client.create_table(sn, "t", cols, unique_pk=False)
        return tid, gnitz.Schema(cols)

    def test_preexisting_consolidated_duplicate_rejected(self, client):
        """The identical (PK, payload) row pushed twice consolidates into ONE
        weight-2 row — two live instances of its key that never surface as two
        rows. The pre-flight must reject it, exactly as the same insert is
        rejected once the index exists."""
        sn = _sn()
        client.create_schema(sn)
        try:
            tid, schema = self._non_unique_pk_table(client, sn)
            for _ in range(2):
                b = gnitz.ZSetBatch(schema)
                b.append(pk=1, val=10)
                client.push(tid, b)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_preexisting_weight2_row_rejected(self, client):
        """A single push can carry weight=2 directly — the same consolidated
        state as pushing the row twice; the pre-flight must reject it."""
        sn = _sn()
        client.create_schema(sn)
        try:
            tid, schema = self._non_unique_pk_table(client, sn)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=10, weight=2)
            client.push(tid, b)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t")
            # Control: distinct values on the same kind of table still pass.
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=10, weight=-2)  # clear the duplicate
            b.append(pk=2, val=20)
            client.push(tid, b)
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_unique_pk_weight2_push_collapses_to_one_instance(self, client):
        """On a unique_pk table a pushed weight ≥ 2 is the row repeated: it
        must collapse to ONE live instance (repeated upsert of itself), a
        single delete must remove it entirely, and the sole-PK-column unique
        index stays trivially creatable (the PK short-circuit's premise)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            schema = gnitz.Schema(cols)
            tid = client.create_table(sn, "t", cols, unique_pk=True)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=10, weight=2)
            client.push(tid, b)
            rows = [(r.pk, r.weight) for r in client.scan(tid) if r.weight > 0]
            assert rows == [(1, 1)], rows
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=10, weight=-1)
            client.push(tid, b)
            assert [r for r in client.scan(tid) if r.weight > 0] == []
            client.execute_sql("CREATE UNIQUE INDEX ON t(pk)", schema_name=sn)
            assert _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_pk"], tables=["t"])

    def test_preflight_spill_large_partition_bounded_and_correct(
            self, unique_preflight_spill_server):
        """With a tiny spill budget, a CREATE UNIQUE INDEX over a table whose
        per-worker partition far exceeds the budget drives the external merge
        sort (many spill runs + a k-way merge over the runs). On all-distinct
        data the index is created and then enforces uniqueness on a fresh
        INSERT; a genuinely new value is still admitted."""
        client = unique_preflight_spill_server
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            # 2000 distinct vals over a wide PK spread → hundreds of spans per
            # worker, far past the 256-byte (32-span) budget → many spilled runs.
            n = 2000
            _insert_rows(client, sn, [(i * 7 + 1, i) for i in range(n)])
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")
            # The created index enforces uniqueness: val=5 is already present.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    f"INSERT INTO t VALUES ({n * 10}, 5)", schema_name=sn)
            # A genuinely new value is admitted.
            client.execute_sql(
                f"INSERT INTO t VALUES ({n * 10 + 1}, {n + 1})", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_preflight_spill_rejects_duplicate_across_runs(
            self, unique_preflight_spill_server):
        """A pre-existing duplicate value buried in a large partition (spilled
        across many external-sort runs, on the same worker or across partitions)
        is still caught by the pre-flight: the CREATE is rejected, no index
        remains, and a fresh duplicate INSERT is still permitted (no phantom
        constraint from the aborted DDL)."""
        client = unique_preflight_spill_server
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            n = 2000
            rows = [(i * 7 + 1, i) for i in range(n)]
            # Repeat the first row's val on a far-away PK appended last, so the
            # two equal spans are buried among many runs / partitions and only
            # the sorted merge brings them adjacent.
            rows.append((n * 10 + 3, 0))
            _insert_rows(client, sn, rows)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t"), \
                "no index may exist after a rejected spill-scale CREATE UNIQUE INDEX"
            # No phantom constraint: a fresh duplicate is still allowed.
            client.execute_sql(
                f"INSERT INTO t VALUES ({n * 10 + 4}, 0)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])


# ---------------------------------------------------------------------------
# TestCompositeIndex — multi-column secondary indexes
# ---------------------------------------------------------------------------

def _pks(results):
    """Sorted PKs of a Rows result."""
    assert results[0]["type"] == "Rows", results[0]
    return sorted(row.pk for row in results[0]["rows"])


class TestCompositeIndex:
    """CREATE INDEX (a, b), full-key and out-of-order WHERE lookups,
    leading-prefix seeks, the nullable-trailing-prefix guard, DROP-INDEX
    exact-list matching, and the composite-UNIQUE / limit rejections."""

    def _setup(self, client, sn, b_nullable=False):
        b_decl = "b BIGINT" if b_nullable else "b BIGINT NOT NULL"
        client.execute_sql(
            f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            f"a BIGINT NOT NULL, {b_decl}, c BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES "
            "(10, 1, 100, 7), (20, 1, 200, 8), (30, 2, 100, 9)",
            schema_name=sn,
        )

    def test_full_key_and_out_of_order(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=sn)

            # Full-key seek a=1 AND b=200 -> pk 20.
            assert _pks(client.execute_sql(
                "SELECT * FROM t WHERE a = 1 AND b = 200", schema_name=sn)) == [20]

            # Out-of-order WHERE binds each value to the index's declared column,
            # not AST order: b=200 AND a=1 must return the same row.
            assert _pks(client.execute_sql(
                "SELECT * FROM t WHERE b = 200 AND a = 1", schema_name=sn)) == [20]
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_leading_prefix_served_by_index(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            self._setup(client, sn)  # b NOT NULL
            client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=sn)

            # Leading-prefix WHERE a=1 over index (a, b) (b non-nullable) is
            # served by the index and returns both a=1 rows.
            assert _pks(client.execute_sql(
                "SELECT * FROM t WHERE a = 1", schema_name=sn)) == [10, 20]
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_tiebreak_prefers_tighter_index(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(a, b, c)", schema_name=sn)

            # With both (a,b) and (a,b,c), WHERE a=1 AND b=200 must still return
            # the correct row regardless of which index the planner picks.
            assert _pks(client.execute_sql(
                "SELECT * FROM t WHERE a = 1 AND b = 200", schema_name=sn)) == [20]
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_nullable_trailing_prefix_not_used(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            self._setup(client, sn, b_nullable=True)
            # Add a row with NULL b that the prefix predicate a=1 still matches.
            client.execute_sql(
                "INSERT INTO t VALUES (40, 1, NULL, 11)", schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=sn)

            # Leading-prefix WHERE a=1 over (a, b) with b nullable is NOT served by
            # the index -- it would silently drop the (1, NULL) row. It is instead
            # served by the on-demand executor, which scans the base and so DOES
            # return that row. This asserts the real property the old "raises a
            # clean non-indexed error" check was only a proxy for: pk=40 (a=1,
            # b=NULL) must not go missing.
            assert _pks(client.execute_sql(
                "SELECT * FROM t WHERE a = 1", schema_name=sn)) == [10, 20, 40]

            # The full key still uses the index and finds the non-null row.
            assert _pks(client.execute_sql(
                "SELECT * FROM t WHERE a = 1 AND b = 200", schema_name=sn)) == [20]
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_residual_non_equality_on_prefix_column(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("CREATE INDEX ON t(a)", schema_name=sn)

            # WHERE a = 1 AND a > 5 consumes the equality a=1 via the index and
            # keeps a > 5 as a residual filter — the residual is excluded by the
            # consumed conjunct's physical index, not by column, so a > 5 survives
            # and (since a=1) matches no row.
            res = client.execute_sql(
                "SELECT * FROM t WHERE a = 1 AND a > 5", schema_name=sn)
            assert res[0]["type"] == "Rows"
            assert len(res[0]["rows"]) == 0
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_drop_index_exact_list(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("CREATE INDEX ON t(a)", schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=sn)

            # Drop only the composite (a, b); the single-column (a) index survives
            # and still serves WHERE a = 1.
            client.execute_sql(f"DROP INDEX {sn}__t__idx_a_b", schema_name=sn)
            assert _pks(client.execute_sql(
                "SELECT * FROM t WHERE a = 1", schema_name=sn)) == [10, 20]
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_composite_unique_created_and_enforced(self, client):
        """Composite UNIQUE (a, b) is created (all (a, b) pairs distinct) and
        enforced end-to-end: a duplicate (a, b) is rejected, rows sharing only
        `a` or only `b` are admitted (the trailing/leading column distinguishes
        them — the regression a u128-leading-column truncation would have
        falsely merged), and the composite seek returns exactly one row."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            # (a, b) pairs are all distinct: (1,100), (1,200), (2,100).
            client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=sn)

            # A duplicate (a, b) = (1, 100) is rejected by the steady-state check.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (40, 1, 100, 1)", schema_name=sn)

            # A row sharing only `a` (a=1, b=999) is a distinct composite → admitted;
            # likewise one sharing only `b` (a=9, b=100).
            client.execute_sql("INSERT INTO t VALUES (41, 1, 999, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (42, 9, 100, 1)", schema_name=sn)

            # The composite seek (broadcast-and-merge) returns exactly one row.
            assert _pks(client.execute_sql(
                "SELECT * FROM t WHERE a = 1 AND b = 200", schema_name=sn)) == [20]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_create_index_limits(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, s VARCHAR NOT NULL)",
                schema_name=sn,
            )
            # Index on a STRING column is rejected.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE INDEX ON t(s)", schema_name=sn)
            # Duplicate column in the index list is rejected.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE INDEX ON t(a, a)", schema_name=sn)
        finally:
            _drop_all(client, sn, tables=["t"])


# ---------------------------------------------------------------------------
# TestCompositeUniqueIndex
# ---------------------------------------------------------------------------

class TestCompositeUniqueIndex:
    """Composite UNIQUE (a, b): both DDL entry points, distributed create-time
    pre-flight, distributed insert-time enforcement, NULL-distinctness, the
    trivial-PK short-circuit, and atomic composite-value transfer. Multi-worker
    coverage comes from GNITZ_WORKERS=4 in the E2E run."""

    def test_table_level_unique_constraint_entry_point(self, client):
        """CREATE TABLE ... UNIQUE (a, b) (table constraint) creates and enforces
        the composite index — the removed table-level planner gate."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL, UNIQUE (a, b))",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 5, 1)", schema_name=sn)
            # Distinct composite admitted; duplicate rejected.
            client.execute_sql("INSERT INTO t VALUES (2, 5, 2)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (3, 5, 1)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_create_preflight_rejects_cross_partition_duplicate(self, client):
        """A pre-existing duplicate (a, b) spread across a wide PK range is
        invisible to any single worker's backfill but caught by the master
        composite pre-flight. The index must NOT exist afterward, and a fresh
        duplicate INSERT must still be permitted (no phantom constraint)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            spread = [1, 7, 13, 1000, 99999, 123456, 777777, 8888888, 73501234, 901234567]
            # All share (a, b) = (5, 9): a cross-partition composite duplicate.
            rows = ", ".join(f"({pk}, 5, 9)" for pk in spread)
            client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=sn)
            assert not _table_has_index(client, sn, "t"), \
                "no index may exist after a rejected composite CREATE UNIQUE INDEX"
            client.execute_sql("INSERT INTO t VALUES (2, 5, 9)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_create_preflight_accepts_leading_column_collision(self, client):
        """Many rows sharing the leading column `a` but with distinct trailing
        `b` are all distinct composites — CREATE UNIQUE INDEX (a, b) succeeds
        (the regression a u128 leading-column truncation would have falsely
        rejected), then enforces correctly."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            spread = [1, 1000, 99999, 8888888, 901234567]
            # All share a=5 but every b is distinct → distinct composites.
            rows = ", ".join(f"({pk}, 5, {i})" for i, pk in enumerate(spread))
            client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=sn)
            # A genuine duplicate (a=5, b=0) is still rejected.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (10, 5, 0)", schema_name=sn)
            # A distinct trailing column is admitted.
            client.execute_sql("INSERT INTO t VALUES (11, 5, 999)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_insert_duplicate_on_different_worker_rejected(self, client):
        """An insert-time duplicate (a, b) committed on a DIFFERENT worker than
        the holder must be rejected (the distributed cross-worker check). Wide
        PK spread scatters holders across workers."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=sn)
            spread = [1, 1000, 99999, 8888888, 901234567]
            for i, pk in enumerate(spread):
                client.execute_sql(f"INSERT INTO t VALUES ({pk}, 5, {i})", schema_name=sn)
            # Re-insert each (a, b) on a fresh PK (likely a different worker) —
            # all must be rejected.
            for i in range(len(spread)):
                with pytest.raises(gnitz.GnitzError):
                    client.execute_sql(
                        f"INSERT INTO t VALUES ({2000 + i}, 5, {i})", schema_name=sn)
            # A fully fresh composite is still admitted.
            client.execute_sql("INSERT INTO t VALUES (3000, 6, 0)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_null_distinctness(self, client):
        """A row NULL in ANY indexed column is not indexed and never collides:
        multiple (NULL, b), (a, NULL), and (NULL, NULL) rows are all admitted,
        while a fully non-null (a, b) still enforces."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, NULL, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, NULL, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (3, 5, NULL)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (4, 5, NULL)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (5, NULL, NULL)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (6, NULL, NULL)", schema_name=sn)
            # A fully non-null (a, b) still enforces.
            client.execute_sql("INSERT INTO t VALUES (7, 5, 1)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (8, 5, 1)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_trivial_pk_shortcircuit(self, client):
        """UNIQUE over a table whose PK is exactly those columns (any order) is
        created without a duplicate error and admits rows the PK already
        permits — the trivial-uniqueness short-circuit (group_cols_eq_pk)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, "
                "v BIGINT NOT NULL, PRIMARY KEY (a, b))",
                schema_name=sn,
            )
            # Duplicate v across distinct (a, b) PKs — a per-column unique would
            # choke, but the composite over the PK columns can never collide.
            client.execute_sql("INSERT INTO t VALUES (1, 2, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 3, 100)", schema_name=sn)
            # UNIQUE on the PK columns in the reverse order still short-circuits.
            client.execute_sql("CREATE UNIQUE INDEX ON t(b, a)", schema_name=sn)
            # A new (a, b) the PK permits is admitted.
            client.execute_sql("INSERT INTO t VALUES (2, 3, 100)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_b_a"], tables=["t"])

    def test_composite_bulk_shift_accepted(self, client):
        """UPDATE t SET b = b + 1 over a dense (a, b) sequence under a composite
        UNIQUE (a, b) ships same-PK upserts whose new composite value is held by
        the previous (also-upserted) row — exercising the upsert verify's
        decode-span -> native composite holder seek. Must succeed."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL, UNIQUE (a, b))",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 7, 1), (2, 7, 2), (3, 7, 3)", schema_name=sn)
            client.execute_sql("UPDATE t SET b = b + 1", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            rows = sorted((r.pk, r.a, r.b) for r in client.scan(tid) if r.weight > 0)
            assert rows == [(1, 7, 2), (2, 7, 3), (3, 7, 4)], rows
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])


# ---------------------------------------------------------------------------
# TestIndexRangeSql — ordered range scans over a secondary index (direct SELECT)
# ---------------------------------------------------------------------------

def _result_pks(result):
    """Sorted list of PKs from a SELECT Rows result (positive weight only)."""
    assert result[0]["type"] == "Rows"
    return sorted(row.pk for row in result[0]["rows"] if row.weight > 0)


def _result_rows(result):
    """Sorted list of full-row value tuples (schema order, PK first) from a
    SELECT Rows result (positive weight only)."""
    assert result[0]["type"] == "Rows"
    return sorted(tuple(row) for row in result[0]["rows"] if row.weight > 0)


class TestIndexRangeSql:
    def test_range_open_ended(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 0), (2, 10), (3, 20), (4, 30)",
                               schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)
            q = lambda s: _result_pks(client.execute_sql(s, schema_name=sn))
            assert q("SELECT * FROM t WHERE x > 10") == [3, 4]
            assert q("SELECT * FROM t WHERE x >= 10") == [2, 3, 4]
            assert q("SELECT * FROM t WHERE x < 20") == [1, 2]
            assert q("SELECT * FROM t WHERE x <= 20") == [1, 2, 3]
            assert q("SELECT * FROM t WHERE 10 < x") == [3, 4]   # flipped orientation
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])

    def test_range_between(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 0), (2, 10), (3, 20), (4, 30)",
                               schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)
            q = lambda s: _result_pks(client.execute_sql(s, schema_name=sn))
            assert q("SELECT * FROM t WHERE x BETWEEN 10 AND 20") == [2, 3]
            # NOT BETWEEN is not a contiguous interval, so no index seek serves it.
            # The on-demand executor does, and must get the complement exactly
            # right (it used to be a clean error rather than mis-served).
            assert q("SELECT * FROM t WHERE x NOT BETWEEN 10 AND 20") == [1, 4]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])

    def test_range_signed_between(self, client):
        """The cff7c58 payoff: a signed range returns the contiguous signed
        interval (OPK(-5) < OPK(5)), not an inverted/empty set."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x INT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, -10), (2, -5), (3, 0), (4, 5), (5, 10)",
                schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)
            q = lambda s: _result_pks(client.execute_sql(s, schema_name=sn))
            assert q("SELECT * FROM t WHERE x BETWEEN -5 AND 5") == [2, 3, 4]
            assert q("SELECT * FROM t WHERE x > -5") == [3, 4, 5]
            assert q("SELECT * FROM t WHERE x < 0") == [1, 2]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])

    def test_range_composite_served_by_range(self, client):
        """On index (a, b), `a = 5 AND b > 10` is served by the composite range
        scan, NOT a bare `a = 5` prefix seek + residual: a row at (5, 0) must be
        absent, and an (8, 11) row never enters the candidate (scan stops < a=8)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 5, 0), (2, 5, 20), (3, 5, 11), (4, 8, 11)",
                schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(a, b)", schema_name=sn)
            assert _result_rows(client.execute_sql(
                "SELECT * FROM t WHERE a = 5 AND b > 10", schema_name=sn)) == \
                [(2, 5, 20), (3, 5, 11)]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_range_nonindexed_served_by_executor(self, client):
        """A range predicate on a non-indexed column has no thin access path, so
        it is served by the on-demand executor rather than rejected."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 6), (3, 4)", schema_name=sn)
            assert _result_pks(client.execute_sql(
                "SELECT * FROM t WHERE x > 5", schema_name=sn)) == [2]
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_range_residual_conjunct(self, client):
        """A non-range residual conjunct is applied after the range scan."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "x BIGINT NOT NULL, y BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 100), (2, 20, 200), (3, 30, 100)",
                schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)
            # x > 5 (range) AND y = 100 (residual) → pks 1 and 3.
            assert _result_pks(client.execute_sql(
                "SELECT * FROM t WHERE x > 5 AND y = 100", schema_name=sn)) == [1, 3]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])

    def test_residual_between_binds(self, client):
        """`x > 5 AND y BETWEEN 1 AND 9` (only x indexed): the y BETWEEN residual
        now binds and filters (regression guard for the Expr::Between binder arm)
        instead of failing to bind. NOT BETWEEN likewise filters."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "x BIGINT NOT NULL, y BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 5), (2, 20, 50), (3, 30, 1)",
                schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)
            q = lambda s: _result_pks(client.execute_sql(s, schema_name=sn))
            assert q("SELECT * FROM t WHERE x > 5 AND y BETWEEN 1 AND 9") == [1, 3]
            assert q("SELECT * FROM t WHERE x > 5 AND y NOT BETWEEN 1 AND 9") == [2]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])

    def test_range_redundant_same_side(self, client):
        """`x > 5 AND x > 10` keeps the first end as the bound and trims the slack
        via the residual; order-independent (proves no packed-native compare)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
                schema_name=sn)
            vals = ",".join(f"({i}, {i})" for i in range(1, 16))
            client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)
            q = lambda s: _result_pks(client.execute_sql(s, schema_name=sn))
            assert q("SELECT * FROM t WHERE x > 5 AND x > 10") == list(range(11, 16))
            assert q("SELECT * FROM t WHERE x > 10 AND x > 5") == list(range(11, 16))
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])

    def test_range_out_of_range_saturation(self, client):
        """An out-of-type-range bound saturates: `x > 3e9` on an INT (I32) column
        is provably empty; `x < 3e9` saturates to unbounded and returns all rows."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x INT NOT NULL)",
                schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 1), (2, 100), (3, 1000)",
                               schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)
            q = lambda s: _result_pks(client.execute_sql(s, schema_name=sn))
            assert q("SELECT * FROM t WHERE x > 3000000000") == []
            assert q("SELECT * FROM t WHERE x < 3000000000") == [1, 2, 3]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])


# ---------------------------------------------------------------------------
# TestIndexCollectMerge — broadcast-and-merge reply correctness: the merged
# result of a non-unique seek / ordered range scan equals a scan-and-filter
# reference, including after retractions (net weights).
# ---------------------------------------------------------------------------

class TestIndexCollectMerge:
    def test_nonunique_collect_matches_scan_reference_with_retraction(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "x BIGINT NOT NULL, y BIGINT NOT NULL)", schema_name=sn)
            # 200 rows scattered across workers; 10 rows per x value.
            vals = ",".join(f"({i}, {i % 20}, {i * 3})" for i in range(200))
            client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)

            # Scan-and-filter reference from the full table scan.
            tid, _ = client.resolve_table(sn, "t")
            ref = {r.pk: (r.x, r.y) for r in client.scan(tid) if r.weight > 0}
            assert len(ref) == 200
            q = lambda s: _result_rows(client.execute_sql(s, schema_name=sn))

            eq_expect = sorted((pk, x, y) for pk, (x, y) in ref.items() if x == 7)
            assert q("SELECT * FROM t WHERE x = 7") == eq_expect
            rng_expect = sorted((pk, x, y) for pk, (x, y) in ref.items() if x > 15)
            assert q("SELECT * FROM t WHERE x > 15") == rng_expect

            # Retract part of the result set; the merged reply must reflect
            # net weights (mandatory for any index access path).
            client.execute_sql("DELETE FROM t WHERE pk IN (7, 27, 47)", schema_name=sn)
            eq_after = [t for t in eq_expect if t[0] not in (7, 27, 47)]
            assert q("SELECT * FROM t WHERE x = 7") == eq_after
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])


# ---------------------------------------------------------------------------
# TestChunkedReplyTrains — multi-frame worker reply trains for non-unique
# seeks and ordered range scans, forced small via the GNITZ_REPLY_FRAME_BUDGET
# debug seam (16 KiB per frame). Against a release server the seam is a no-op
# and this degrades to a normal single-frame merge test.
# ---------------------------------------------------------------------------

class TestChunkedReplyTrains:
    def test_chunked_seek_and_range_replies_return_full_set(self, reply_frame_budget_server):
        client = reply_frame_budget_server
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "x BIGINT NOT NULL, y BIGINT NOT NULL)", schema_name=sn)
            # ~100 KB of matching base rows per worker per value: each worker's
            # reply spans several 16 KiB frames (byte-back-pressured by the ring,
            # not capped by any frame count — see the fixture).
            n = 20_000
            _insert_rows(client, sn, [(i, i % 2, i * 7) for i in range(n)], chunk=1000)
            client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)
            q = lambda s: _result_pks(client.execute_sql(s, schema_name=sn))

            # Non-unique point seek over a chunked train per worker.
            assert q("SELECT * FROM t WHERE x = 1") == list(range(1, n, 2))
            # Ordered range scan returning every row.
            assert q("SELECT * FROM t WHERE x >= 0") == list(range(n))

            # Payload integrity through chunk boundaries: full-row values.
            rows = _result_rows(client.execute_sql(
                "SELECT * FROM t WHERE x = 0", schema_name=sn))
            assert rows == [(i, 0, i * 7) for i in range(0, n, 2)]

            # Retraction across a chunked result reflects net weights.
            client.execute_sql("DELETE FROM t WHERE pk IN (1, 3, 5)", schema_name=sn)
            assert q("SELECT * FROM t WHERE x = 1") == list(range(7, n, 2))
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])


# ---------------------------------------------------------------------------
# TestIndexBoundPushdown — the index bound pushed into a circuit's backfill scan
# ---------------------------------------------------------------------------
#
# The bound is a physical access hint: the circuit's Filter carries the full
# predicate either way, so a bounded build and an unbounded one must agree on
# rows AND weights. Each test therefore compares against the same query planned
# without an index. Run at GNITZ_WORKERS=4 these also guard the co-location
# claim the bound rests on — an index circuit shadows only its own worker's base
# slice, so a bounded scan that needed a cross-worker gather would lose rows.


def _grouped(client, sn, sql):
    """Sorted (group, aggregate) tuples from a grouped SELECT."""
    return _result_rows(client.execute_sql(sql, schema_name=sn))


class TestIndexBoundPushdown:
    def test_groupby_bounded_matches_unbounded(self, client):
        """An ad-hoc GROUP BY over an indexed WHERE returns exactly what the
        same query returns with no index to bound on."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL,"
                " v BIGINT NOT NULL, ind BIGINT NOT NULL)", schema_name=sn)
            n = 2000
            _insert_rows(client, sn,
                         [(i, i % 7, i * 3, i % 11) for i in range(n)], chunk=500)
            q = "SELECT g, SUM(v) AS s FROM t WHERE ind = 4 GROUP BY g"
            unbounded = _grouped(client, sn, q)
            assert unbounded, "the fixture must match some rows"

            client.execute_sql("CREATE INDEX ON t(ind)", schema_name=sn)
            assert _grouped(client, sn, q) == unbounded, \
                "a bounded backfill scan must not change the aggregate"

            # A range bound, likewise. 1/11 of rows per `ind` value, so
            # `ind < 2` is ~2/11 — inside the selectivity gate.
            qr = "SELECT g, COUNT(*) AS c FROM t WHERE ind < 2 GROUP BY g"
            assert _grouped(client, sn, qr) == sorted(
                (i % 7, sum(1 for k in range(n) if k % 7 == i % 7 and k % 11 < 2))
                for i in range(7))
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_ind"], tables=["t"])

    def test_create_view_bounded_matches_unbounded(self, client):
        """A CREATE VIEW backfill over an indexed WHERE materialises exactly
        what the unindexed build materialises."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL,"
                " v BIGINT NOT NULL, ind BIGINT NOT NULL)", schema_name=sn)
            n = 2000
            _insert_rows(client, sn,
                         [(i, i % 7, i * 3, i % 11) for i in range(n)], chunk=500)
            # Built BEFORE the index exists: no bound.
            client.execute_sql(
                "CREATE VIEW v_plain AS SELECT g, SUM(v) AS s FROM t"
                " WHERE ind = 4 GROUP BY g", schema_name=sn)
            plain = _grouped(client, sn, "SELECT * FROM v_plain")

            client.execute_sql("CREATE INDEX ON t(ind)", schema_name=sn)
            # Built AFTER: its backfill takes the bounded scan.
            client.execute_sql(
                "CREATE VIEW v_bound AS SELECT g, SUM(v) AS s FROM t"
                " WHERE ind = 4 GROUP BY g", schema_name=sn)
            assert _grouped(client, sn, "SELECT * FROM v_bound") == plain

            # Steady state: a push FAILING the index predicate must be dropped by
            # the Filter, and one PASSING it must land — the bound is consulted
            # only at backfill, so maintenance is unchanged.
            _insert_rows(client, sn, [(n, 0, 100, 5), (n + 1, 0, 1000, 4)])
            after = dict(_grouped(client, sn, "SELECT * FROM v_bound"))
            assert after[0] == dict(plain)[0] + 1000, \
                "the matching push must maintain the bounded view; the other must not"
        finally:
            _drop_all(client, sn, views=["v_bound", "v_plain"],
                      indices=[f"{sn}__t__idx_ind"], tables=["t"])

    def test_null_indexed_column_returned_by_neither_build(self, client):
        """A NULL-valued row is absent from the index, so a bounded scan can
        never return it — and under 3VL the Filter drops it too. Both builds
        must agree."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL,"
                " ind BIGINT)", schema_name=sn)
            rows = [(i, i % 3, i % 5) for i in range(300)]
            rows += [(300 + i, i % 3, "NULL") for i in range(30)]
            _insert_rows(client, sn, rows)
            q = "SELECT g, COUNT(*) AS c FROM t WHERE ind >= 0 GROUP BY g"
            unbounded = _grouped(client, sn, q)

            client.execute_sql("CREATE INDEX ON t(ind)", schema_name=sn)
            assert _grouped(client, sn, q) == unbounded
            # The NULL rows are in neither: 300 non-NULL rows over 3 groups.
            assert sum(c for _, c in unbounded) == 300
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_ind"], tables=["t"])

    def test_dropped_index_falls_back_to_full_scan(self, client):
        """A view whose plan named an index that is later dropped still builds
        correctly — the engine degrades to a full scan."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL,"
                " ind BIGINT NOT NULL)", schema_name=sn)
            _insert_rows(client, sn, [(i, i % 4, i % 9) for i in range(400)])
            client.execute_sql("CREATE INDEX ON t(ind)", schema_name=sn)
            q = "SELECT g, COUNT(*) AS c FROM t WHERE ind = 3 GROUP BY g"
            bounded = _grouped(client, sn, q)

            client.execute_sql(f"DROP INDEX {sn}__t__idx_ind", schema_name=sn)
            assert _grouped(client, sn, q) == bounded, \
                "dropping the index must fall back to a full scan, not lose rows"
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_replicated_global_aggregate_grounds_on_empty_range(self, client):
        """A replicated base's global COUNT(*) is the shape that reaches
        `backfill_view`, and an unmatched WHERE makes its range provably empty.
        The ground row must still be minted: COUNT(*) = 0, one row."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, ind BIGINT NOT NULL)"
                " WITH (replicated = true)", schema_name=sn)
            _insert_rows(client, sn, [(i, i % 5) for i in range(100)])
            client.execute_sql("CREATE INDEX ON t(ind)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW mv AS SELECT COUNT(*) AS c FROM t WHERE ind = 999",
                schema_name=sn)
            res = client.execute_sql("SELECT * FROM mv", schema_name=sn)
            rows = [r for r in res[0]["rows"] if r.weight > 0]
            assert len(rows) == 1, "an empty range must still seed the ground row"
            assert rows[0][0] == 0
        finally:
            _drop_all(client, sn, views=["mv"],
                      indices=[f"{sn}__t__idx_ind"], tables=["t"])


class TestNonUniquePkIndexGather:
    """A non-`unique_pk` table lets one PK carry several live payloads that
    differ in an indexed column, so an index entry denotes (indexed span, PK),
    not the PK alone. Every index-served path (point seek, range) must return
    the same rows and weights as the index-free scan — the by-PK gather lost
    range rows and mis-resolved / double-counted point seeks."""

    def _seed(self, client, sn):
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tid = client.create_table(sn, "t", cols, unique_pk=False)
        # Six PKs (spread across the W=4 partitions), each carrying two payloads
        # with distinct vals — the multi-payload-per-PK shape the by-PK gather
        # mishandles. The master merges the per-worker partial sets by raw append.
        b = gnitz.ZSetBatch(schema)
        for pk, val in [(1, 5), (1, 7), (2, 6), (2, 8), (3, 5), (3, 9),
                        (4, 7), (4, 6), (5, 8), (5, 5), (6, 9), (6, 7)]:
            b.append(pk=pk, val=val)
        client.push(tid, b)
        return tid

    def test_seek_and_range_match_index_free_scan(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            self._seed(client, sn)
            queries = [
                "SELECT * FROM t WHERE val = 5",         # point, matches 2 PKs
                "SELECT * FROM t WHERE val = 7",         # point, one payload of a 2-payload PK
                "SELECT * FROM t WHERE val BETWEEN 5 AND 7",  # range spanning duplicate groups
                "SELECT * FROM t WHERE val >= 8",        # open-ended range
            ]
            # Reference: the index-free full scan + filter (served by the executor).
            want = {q: _result_rows(client.execute_sql(q, schema_name=sn)) for q in queries}
            # Each query must return a non-trivial multiset — otherwise the
            # comparison below could pass vacuously.
            assert all(len(v) >= 2 for v in want.values())

            client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
            for q in queries:
                got = _result_rows(client.execute_sql(q, schema_name=sn))
                assert got == want[q], f"index path diverged from the scan for: {q}"
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])
