"""Retraction and incremental correctness tests.

Views are defined in SQL; the engine's operators are reached through the planner.
"""

import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


# ---------------------------------------------------------------------------
# TestFilterRetraction
# ---------------------------------------------------------------------------

class TestFilterRetraction:

    def test_delete_propagates_through_filter(self, client):
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
            tid, t_schema = client.resolve_table(sn, "t")
            vid, _ = client.resolve_table(sn, "v")

            client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=sn)
            assert len(list(client.scan(vid))) == 1

            # Retract using the table's actual schema, not a hand-built U64 one.
            # BIGINT PKs are stored as signed I64; a U64-typed batch would encode
            # a different order-preserving PK image and never consolidate.
            batch = gnitz.ZSetBatch(t_schema)
            batch.append(pk=1, val=100, _weight=-1)
            client.push(tid, batch)

            assert len(list(client.scan(vid))) == 0
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_delete_outside_filter_no_effect(self, client):
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
            tid, t_schema = client.resolve_table(sn, "t")
            vid, _ = client.resolve_table(sn, "v")

            client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
            assert len(list(client.scan(vid))) == 0

            batch = gnitz.ZSetBatch(t_schema)
            batch.append(pk=1, val=10, _weight=-1)
            client.push(tid, batch)

            assert len(list(client.scan(vid))) == 0
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestJoinRetraction
# ---------------------------------------------------------------------------

class TestJoinRetraction:

    def _setup(self, client, sn):
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, label BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT a.pk, a.val, b.label FROM a JOIN b ON a.pk = b.pk",
            schema_name=sn,
        )
        a_tid, a_schema = client.resolve_table(sn, "a")
        b_tid, b_schema = client.resolve_table(sn, "b")
        vid, _ = client.resolve_table(sn, "v")
        return a_tid, a_schema, b_tid, b_schema, vid

    def test_delete_propagates_through_join(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            a_tid, a_schema, b_tid, b_schema, vid = self._setup(client, sn)

            client.execute_sql("INSERT INTO a VALUES (1, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 999)", schema_name=sn)
            assert len(list(client.scan(vid))) == 1

            batch = gnitz.ZSetBatch(a_schema)
            batch.append(pk=1, val=100, _weight=-1)
            client.push(a_tid, batch)

            assert len(list(client.scan(vid))) == 0
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE a", "DROP TABLE b"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_trace_update_removes_join_output(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            a_tid, a_schema, b_tid, b_schema, vid = self._setup(client, sn)

            client.execute_sql("INSERT INTO a VALUES (1, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 999)", schema_name=sn)
            assert len(list(client.scan(vid))) == 1

            batch = gnitz.ZSetBatch(b_schema)
            batch.append(pk=1, label=999, _weight=-1)
            client.push(b_tid, batch)

            assert len(list(client.scan(vid))) == 0
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE a", "DROP TABLE b"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestViewCascade
# ---------------------------------------------------------------------------

class TestViewCascade:

    def test_cascade_insert(self, client):
        """T → filter view V1 → passthrough view V2; push matching row; verify both."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10",
                schema_name=sn,
            )
            v1_id, v1_schema = client.resolve_table(sn, "v1")

            # V2 is a passthrough view on top of V1
            v2_id = client.create_view(sn, "v2", v1_id, v1_schema)

            client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=sn)

            assert len(list(client.scan(v1_id))) == 1
            assert len(list(client.scan(v2_id))) == 1
        finally:
            for obj in ["v2", "v1", "t"]:
                try:
                    if obj.startswith("v"):
                        client.execute_sql(f"DROP VIEW {obj}", schema_name=sn)
                    else:
                        client.execute_sql(f"DROP TABLE {obj}", schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_cascade_delete(self, client):
        """T → V1 (filter) → V2 (passthrough); retract row; both views empty."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10",
                schema_name=sn,
            )
            v1_id, v1_schema = client.resolve_table(sn, "v1")
            v2_id = client.create_view(sn, "v2", v1_id, v1_schema)

            tid, t_schema = client.resolve_table(sn, "t")
            client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=sn)
            assert len(list(client.scan(v1_id))) == 1

            batch = gnitz.ZSetBatch(t_schema)
            batch.append(pk=1, val=100, _weight=-1)
            client.push(tid, batch)

            assert len(list(client.scan(v1_id))) == 0
            assert len(list(client.scan(v2_id))) == 0
        finally:
            for obj in ["v2", "v1", "t"]:
                try:
                    if obj.startswith("v"):
                        client.execute_sql(f"DROP VIEW {obj}", schema_name=sn)
                    else:
                        client.execute_sql(f"DROP TABLE {obj}", schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestNullThroughOperators
# ---------------------------------------------------------------------------

class TestNullThroughOperators:

    def test_null_through_filter(self, client):
        """IS NOT NULL filter excludes null-val rows."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val IS NOT NULL",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "v")

            client.execute_sql("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)",
                               schema_name=sn)

            positive = list(client.scan(vid))
            assert len(positive) == 2
            pks = sorted(r.pk for r in positive)
            assert pks == [1, 3]
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_null_through_join(self, client):
        """Nullable column in left table is preserved in join output."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, score BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.pk, a.val, b.score FROM a JOIN b ON a.pk = b.pk",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "v")

            client.execute_sql("INSERT INTO a VALUES (1, NULL)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 42)", schema_name=sn)

            positive = list(client.scan(vid))
            assert len(positive) == 1
            assert positive[0]["val"] is None
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE a", "DROP TABLE b"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_count_vs_sum_with_nulls(self, client):
        """COUNT(*) = 3, SUM(val) = sum of non-null values when one val is NULL."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, val BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_count AS SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_sum AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp",
                schema_name=sn,
            )
            count_vid, _ = client.resolve_table(sn, "v_count")
            sum_vid, _ = client.resolve_table(sn, "v_sum")

            # 3 rows: val=10, val=20, val=NULL
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 1, NULL)",
                schema_name=sn,
            )

            # Visible layout: row[0]=grp, row[1]=agg (cnt / total).
            count_rows = list(client.scan(count_vid))
            assert len(count_rows) == 1
            assert count_rows[0][1] == 3

            sum_rows = list(client.scan(sum_vid))
            assert len(sum_rows) == 1
            assert sum_rows[0][1] == 30  # 10+20, null excluded
        finally:
            for sql in ["DROP VIEW v_count", "DROP VIEW v_sum", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestStringThroughPipeline
# ---------------------------------------------------------------------------

class TestStringThroughPipeline:

    def test_string_through_join(self, client):
        """String name column is preserved in join output."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE left_t (pk BIGINT NOT NULL PRIMARY KEY, name VARCHAR NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE right_t (pk BIGINT NOT NULL PRIMARY KEY, score BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT left_t.pk, left_t.name, right_t.score "
                "FROM left_t JOIN right_t ON left_t.pk = right_t.pk",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "v")

            client.execute_sql("INSERT INTO left_t VALUES (1, 'Alice')", schema_name=sn)
            client.execute_sql("INSERT INTO right_t VALUES (1, 100)", schema_name=sn)

            rows = list(client.scan(vid))
            assert len(rows) == 1
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE left_t", "DROP TABLE right_t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestLargeBatch
# ---------------------------------------------------------------------------

class TestLargeBatch:

    def test_large_batch_through_join(self, client):
        """1000 rows in each table; join output has 1000 matching rows."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, a_val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, b_val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.pk, a.a_val, b.b_val "
                "FROM a JOIN b ON a.pk = b.pk",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "v")

            n = 1000
            a_vals = ",".join(f"({i}, {i * 10})" for i in range(1, n + 1))
            b_vals = ",".join(f"({i}, {i * 100})" for i in range(1, n + 1))
            client.execute_sql(f"INSERT INTO a VALUES {a_vals}", schema_name=sn)
            client.execute_sql(f"INSERT INTO b VALUES {b_vals}", schema_name=sn)

            rows = list(client.scan(vid))
            assert len(rows) == n
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE a", "DROP TABLE b"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)
