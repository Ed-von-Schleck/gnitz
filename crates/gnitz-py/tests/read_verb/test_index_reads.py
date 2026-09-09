"""Reads served through a secondary index: point seek, SQL equality and range
lookups, the bound pushed into the walk, the per-partition collect/merge, and
a reply train that spans more than one frame.

The failure mode is silent under-reporting — a key the walk never opens comes
back as "no such row" — so each case asserts the full result set, and the
bounded build is compared against the unbounded one.
"""

import pytest
from uuid import uuid4



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




def _insert_rows(client, sn, rows, chunk=500):
    """Multi-row INSERT into table `t` of same-width value tuples, split into
    at-most-`chunk`-row statements; a literal 'NULL' passes through."""
    for i in range(0, len(rows), chunk):
        values = ", ".join(
            f"({', '.join(str(v) for v in r)})" for r in rows[i:i + chunk])
        client.execute_sql(f"INSERT INTO t VALUES {values}", schema_name=sn)




def _result_pks(result):
    """Sorted list of PKs from a SELECT Rows result (positive weight only)."""
    assert result[0]["type"] == "Rows"
    return sorted(row.pk for row in result[0]["rows"])


def _result_rows(result):
    """Sorted list of full-row value tuples (schema order, PK first) from a
    SELECT Rows result (positive weight only)."""
    assert result[0]["type"] == "Rows"
    return sorted(tuple(row) for row in result[0]["rows"])

def _grouped(client, sn, sql):
    """Sorted (group, aggregate) tuples from a grouped SELECT."""
    return _result_rows(client.execute_sql(sql, schema_name=sn))


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
            assert result.schema is not None
            assert len(result.pks) == 1
            assert result.pks[0] == 1
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
            assert result.schema is None or len(result.pks) == 0
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
            assert result.schema is not None
            assert len(result.pks) == 1
            assert result.pks[0] == 10
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_cust_id"],
                      tables=["t"])

    def test_seek_after_insert(self, client):
        """Rows inserted after index creation are visible via seek — for keys
        spanning every partition, so the projection that maintains the index has
        to have run on every worker rather than only the one the seed landed on.
        """
        sn = _sn()
        try:
            tid = self._setup(client, sn)
            client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)
            n = 32
            client.execute_sql(
                "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 100})" for i in range(1, n + 1)),
                schema_name=sn)

            for i in range(1, n + 1):
                result = client.seek_by_index(tid, [1], [i * 100])
                assert list(result.pks) == [i], f"cust_id={i * 100} not found via index"
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_cust_id"],
                      tables=["t"])

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
            assert len(rows.pks) == 1
            assert rows.pks[0] == 1
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
            assert results[0]["rows"].pks[0] == 7
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_select_nonindexed_col_served_by_read_path(self, client):
        """WHERE on a column with no index is SERVED, not rejected.

        No index means no seek key, so the bounded read degrades to a full cursor
        and the predicate runs server-side. Served directly — no circuit.
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
            assert len(rows.pks) == 1
            assert rows.pks[0] == 1
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
            assert len(results[0]["rows"].pks) == 0
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
                assert len(rows.pks) == 1, f"expected 1 row for score={val}"
                assert rows.pks[0] == pk, f"wrong pk for score={val}"
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
                assert len(rows.pks) == 1, f"expected 1 row for score={val}"
                assert rows.pks[0] == pk, f"wrong pk for score={val}"
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
            assert len(results[0]["rows"].pks) == 0
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

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

    def test_range_nonindexed_served_by_read_path(self, client):
        """A range predicate on a non-indexed column has no seek key, so it is
        served by the read-spec predicate (full cursor + predicate), not rejected."""
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
            ref = {r.pk: (r.x, r.y) for r in client.scan(tid)}
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

    def test_unique_index_over_a_long_text_table_past_one_frame(
            self, reply_frame_budget_server):
        """CREATE UNIQUE INDEX warms its cold filters from an UNPROJECTED
        whole-table scan, so a table with a long TEXT column reaches that scan's
        frame budget on rows the user never asked to read. Every frame carries a
        heap compacted to its own rows, so the DDL completes and the index it
        builds enforces uniqueness."""
        client = reply_frame_budget_server
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "u BIGINT NOT NULL, body TEXT NOT NULL)", schema_name=sn)
            # 200 bytes of heap per row: 4 000 rows is ~800 KB, far past the
            # 16 KiB budget on every worker.
            body = "z" * 200
            n = 4_000
            _insert_rows(client, sn,
                         [(i, i, f"'row-{i}-{body}'") for i in range(n)], chunk=500)

            client.execute_sql("CREATE UNIQUE INDEX ON t(u)", schema_name=sn)
            # The index is live: a duplicate is refused, a fresh value accepted.
            with pytest.raises(Exception):
                client.execute_sql("INSERT INTO t VALUES (99999, 7, 'dup')",
                                   schema_name=sn)
            client.execute_sql(f"INSERT INTO t VALUES (99999, {n}, 'new')",
                               schema_name=sn)
            # And it still serves reads over the long-TEXT rows it indexes.
            rows = _result_rows(client.execute_sql(
                "SELECT pk, u FROM t WHERE u = 17", schema_name=sn))
            assert rows == [(17, 17)]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_u"], tables=["t"])

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
            rows = list(res[0]["rows"])
            assert len(rows) == 1, "an empty range must still seed the ground row"
            assert rows[0][0] == 0
        finally:
            _drop_all(client, sn, views=["mv"],
                      indices=[f"{sn}__t__idx_ind"], tables=["t"])
