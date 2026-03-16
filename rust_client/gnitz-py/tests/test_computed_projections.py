"""E2E tests: computed projections in CREATE VIEW.

Run:
    cd rust_client/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_computed_projections.py -v --tb=short
"""
import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn):
    """Best-effort cleanup of schema and its contents."""
    for name in ["v", "t"]:
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
        try:
            client.execute_sql(f"DROP TABLE {name}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _scan_dicts(client, tid):
    """Scan and return list of dicts."""
    return client.scan(tid).mappings()


# ---------------------------------------------------------------------------
# TestComputedProjections
# ---------------------------------------------------------------------------

class TestComputedProjections:
    def test_add_columns(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, a + b AS total FROM t",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 20), (2, 30, 40)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            vals = sorted([(r["id"], r["total"]) for r in rows])
            assert vals == [(1, 30), (2, 70)]
        finally:
            _cleanup(client, sn)

    def test_multiply_literal(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, price BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, price * 2 AS doubled FROM t",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 50), (2, 100)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            vals = sorted([(r["id"], r["doubled"]) for r in rows])
            assert vals == [(1, 100), (2, 200)]
        finally:
            _cleanup(client, sn)

    def test_mixed_arith(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, (a + b) * c AS result FROM t",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 2, 3, 4)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert rows[0]["result"] == 20  # (2+3)*4
        finally:
            _cleanup(client, sn)

    def test_comparison_expr(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, val > 3 AS positive FROM t",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 5), (2, 1)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            vals = {r["id"]: r["positive"] for r in rows}
            assert vals[1] == 1
            assert vals[2] == 0
        finally:
            _cleanup(client, sn)

    def test_float_expr(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, price DOUBLE NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, price * 1.1 AS adjusted FROM t",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 100.0)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            # price * 1.1 = 110.0
            assert abs(rows[0]["adjusted"] - 110.0) < 0.01
        finally:
            _cleanup(client, sn)

    def test_pk_auto_prepend(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            # PK (id) not in projection — should be auto-prepended
            client.execute_sql(
                "CREATE VIEW v AS SELECT val + 1 AS v FROM t",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            vals = sorted([(r["id"], r["v"]) for r in rows])
            assert vals == [(1, 11), (2, 21)]
        finally:
            _cleanup(client, sn)

    def test_computed_pk_auto_prepend(self, client):
        """When only computed exprs are given, PK is auto-prepended."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            # id * 2 is computed — PK (id) auto-prepended as PassThrough
            client.execute_sql(
                "CREATE VIEW v AS SELECT id * 2 AS doubled FROM t",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            vals = sorted([(r["id"], r["doubled"]) for r in rows])
            assert vals == [(1, 2), (2, 4)]
        finally:
            _cleanup(client, sn)

    def test_projection_after_insert(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, a + b AS total FROM t",
                schema_name=sn,
            )
            # First insert
            client.execute_sql("INSERT INTO t VALUES (1, 5, 10)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 1 and rows[0]["total"] == 15

            # Second insert
            client.execute_sql("INSERT INTO t VALUES (2, 20, 30)", schema_name=sn)
            rows = _scan_dicts(client, vid)
            vals = sorted([(r["id"], r["total"]) for r in rows])
            assert vals == [(1, 15), (2, 50)]
        finally:
            _cleanup(client, sn)

    def test_string_passthrough(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL, a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, name, a + b AS total FROM t",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 'alice', 10, 20)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert rows[0]["name"] == "alice"
            assert rows[0]["total"] == 30
        finally:
            _cleanup(client, sn)

    def test_mixed_passthrough(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL, a BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, val, a * 2 AS doubled FROM t",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 100, 5), (2, 200, 10)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            vals = sorted([(r["id"], r["val"], r["doubled"]) for r in rows])
            assert vals == [(1, 100, 10), (2, 200, 20)]
        finally:
            _cleanup(client, sn)


# ---------------------------------------------------------------------------
# TestFloatExpressions
# ---------------------------------------------------------------------------

class TestFloatExpressions:
    def test_float_arith(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL, f DOUBLE NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, i + f AS result FROM t",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 10, 2.5)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert abs(rows[0]["result"] - 12.5) < 0.01
        finally:
            _cleanup(client, sn)

    def test_mixed_multiply(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, i * 1.5 AS result FROM t",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert abs(rows[0]["result"] - 15.0) < 0.01
        finally:
            _cleanup(client, sn)

    def test_float_neg(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, -f AS neg_f FROM t",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 3.14)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert abs(rows[0]["neg_f"] + 3.14) < 0.01
        finally:
            _cleanup(client, sn)

    def test_float_cmp(self, client):
        """WHERE with float comparison (non-negative values)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, f FROM t WHERE f > 1.5",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 3.0), (2, 0.5), (3, 2.0)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            ids = sorted([r["id"] for r in rows])
            assert ids == [1, 3]  # f=3.0 > 1.5, f=2.0 > 1.5
        finally:
            _cleanup(client, sn)

    def test_pure_int_unchanged(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, a + b AS total FROM t",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 100, 200)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert rows[0]["total"] == 300
        finally:
            _cleanup(client, sn)

    def test_pure_float_no_cast(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a DOUBLE NOT NULL, b DOUBLE NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, a + b AS total FROM t",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 1.5, 2.5)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert abs(rows[0]["total"] - 4.0) < 0.01
        finally:
            _cleanup(client, sn)


# ---------------------------------------------------------------------------
# TestNullPropagation
# ---------------------------------------------------------------------------

class TestNullPropagation:
    def test_null_plus_int(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, a + b AS total FROM t",
                schema_name=sn,
            )
            # Insert with a = NULL using explicit NULL
            client.execute_sql(
                "INSERT INTO t VALUES (1, NULL, 10)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert rows[0]["total"] is None
        finally:
            _cleanup(client, sn)

    def test_null_comparison(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT)",
                schema_name=sn,
            )
            # WHERE a > 5 — NULL rows should be filtered out
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, a FROM t WHERE a > 5",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, NULL)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 1
            assert rows[0]["id"] == 1
        finally:
            _cleanup(client, sn)

    def test_non_null_computes(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, a + b AS total FROM t",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 5, 10)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert rows[0]["total"] == 15
        finally:
            _cleanup(client, sn)

    def test_is_null_filter(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT)",
                schema_name=sn,
            )
            # IS NULL / IS NOT NULL are never themselves NULL
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, a FROM t WHERE a IS NULL",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, NULL)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 1
            assert rows[0]["id"] == 2
        finally:
            _cleanup(client, sn)

    def test_null_chain(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT, c BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, a + b + c AS total FROM t",
                schema_name=sn,
            )
            # b = NULL
            client.execute_sql(
                "INSERT INTO t VALUES (1, 5, NULL, 10)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert rows[0]["total"] is None
        finally:
            _cleanup(client, sn)

    def test_null_filter_eq_zero(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT)",
                schema_name=sn,
            )
            # WHERE a = 0 with a NULL should filter the row out
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, a FROM t WHERE a = 0",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, NULL)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 1
            assert rows[0]["id"] == 1
        finally:
            _cleanup(client, sn)
