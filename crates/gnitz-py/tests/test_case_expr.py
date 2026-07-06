"""E2E tests: CASE / COALESCE / NULLIF, and the folded-in U64 type-preservation fix.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_case_expr.py -v --tb=short
"""
import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, *names):
    for name in names or ("v2", "v", "t"):
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


def _dicts(client, vid):
    """Current (positive-state) view rows as dicts."""
    return client.scan(vid).mappings()


# ---------------------------------------------------------------------------
# CASE / COALESCE / NULLIF in computed projections
# ---------------------------------------------------------------------------

class TestCaseProjection:
    def test_searched_case(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, "
                "CASE WHEN a > 10 THEN 100 WHEN a > 0 THEN 10 ELSE 0 END AS bucket FROM t",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 5), (3, -3)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            got = sorted((r["id"], r["bucket"]) for r in _dicts(client, vid))
            assert got == [(1, 100), (2, 10), (3, 0)]
        finally:
            _cleanup(client, sn)

    def test_simple_case_operand(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, CASE a WHEN 1 THEN 111 WHEN 2 THEN 222 ELSE 999 END AS m FROM t",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 1), (2, 2), (3, 7)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            got = sorted((r["id"], r["m"]) for r in _dicts(client, vid))
            assert got == [(1, 111), (2, 222), (3, 999)]
        finally:
            _cleanup(client, sn)

    def test_case_no_else_is_null(self, client):
        """CASE without ELSE returns NULL for the unmatched rows (round-trips to client)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, CASE WHEN a > 10 THEN a END AS big FROM t",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 5)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            got = {r["id"]: r["big"] for r in _dicts(client, vid)}
            assert got == {1: 20, 2: None}
        finally:
            _cleanup(client, sn)

    def test_coalesce_projection(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, COALESCE(a, b, 0) AS c FROM t",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 7, 9), (2, NULL, 9), (3, NULL, NULL)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            got = sorted((r["id"], r["c"]) for r in _dicts(client, vid))
            assert got == [(1, 7), (2, 9), (3, 0)]
        finally:
            _cleanup(client, sn)

    def test_nullif_projection(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, NULLIF(a, 0) AS n FROM t",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 0)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            got = {r["id"]: r["n"] for r in _dicts(client, vid)}
            assert got == {1: 5, 2: None}  # NULLIF(0, 0) → NULL
        finally:
            _cleanup(client, sn)


# ---------------------------------------------------------------------------
# CASE / COALESCE in WHERE and HAVING
# ---------------------------------------------------------------------------

class TestCasePredicate:
    def test_case_in_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, kind BIGINT NOT NULL)",
                schema_name=sn,
            )
            # keep rows where (kind=1 ? a>10 : a>100)
            client.execute_sql(
                "CREATE VIEW v AS SELECT id FROM t "
                "WHERE (CASE WHEN kind = 1 THEN a > 10 ELSE a > 100 END)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 20, 1), (2, 20, 2), (3, 200, 2), (4, 5, 1)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            got = sorted(r["id"] for r in _dicts(client, vid))
            assert got == [1, 3]
        finally:
            _cleanup(client, sn)

    def test_coalesce_in_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT)",
                schema_name=sn,
            )
            # COALESCE(a, 0) > 5 — a NULL row behaves like 0 (excluded)
            client.execute_sql(
                "CREATE VIEW v AS SELECT id FROM t WHERE COALESCE(a, 0) > 5",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 2)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            got = sorted(r["id"] for r in _dicts(client, vid))
            assert got == [1]
        finally:
            _cleanup(client, sn)

    def test_case_in_having(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            # HAVING over an aggregate result via CASE
            client.execute_sql(
                "CREATE VIEW v AS SELECT g, SUM(v) AS s FROM t GROUP BY g "
                "HAVING (CASE WHEN SUM(v) > 100 THEN 1 ELSE 0 END) = 1",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, 60), (2, 1, 60), (3, 2, 10)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            got = {r["g"]: r["s"] for r in _dicts(client, vid)}
            assert got == {1: 120}  # group 2 (sum 10) filtered out
        finally:
            _cleanup(client, sn)


# ---------------------------------------------------------------------------
# UPDATE ... SET col = CASE (client-side row evaluator)
# ---------------------------------------------------------------------------

class TestCaseDml:
    def test_update_set_case(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, grade BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 95, 0), (2, 70, 0), (3, 40, 0)",
                schema_name=sn,
            )
            client.execute_sql(
                "UPDATE t SET grade = CASE WHEN a >= 90 THEN 3 WHEN a >= 60 THEN 2 ELSE 1 END",
                schema_name=sn,
            )
            tid = client.resolve_table(sn, "t")[0]
            got = sorted((r.id, r.grade) for r in client.scan(tid) if r.weight > 0)
            assert got == [(1, 3), (2, 2), (3, 1)]
        finally:
            _cleanup(client, sn)


# ---------------------------------------------------------------------------
# Folded-in U64 type-preservation fix (arithmetic + CASE + SUM)
# ---------------------------------------------------------------------------

class TestU64Preservation:
    def test_u64_add_downstream_compare(self, client):
        """SELECT u64_a + u64_b AS v (value >= 2^63) read by a downstream WHERE v > 100.

        Both operands are 2^62 (fit in i64 for the SQL literal); their sum is
        2^63, whose i64 bit pattern is negative. A signed downstream compare drops
        the row (the bug); the U64-preserving type makes the compare unsigned.
        """
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE VIEW v AS SELECT id, a + b AS v FROM t", schema_name=sn)
            client.execute_sql("CREATE VIEW v2 AS SELECT id FROM v WHERE v > 100", schema_name=sn)
            half = 2 ** 62
            client.execute_sql(f"INSERT INTO t VALUES (1, {half}, {half}), (2, 1, 1)", schema_name=sn)
            v2id = client.resolve_table(sn, "v2")[0]
            got = sorted(r["id"] for r in _dicts(client, v2id))
            # id=1: v = 2^63 (>100 unsigned). id=2: v = 2 (<100).
            assert got == [1], f"U64 sum >= 2^63 must survive downstream v > 100; got {got}"
        finally:
            _cleanup(client, sn, "v2", "v", "t")

    def test_u64_case_downstream_compare(self, client):
        """CASE ... THEN (u64 arithmetic) ... END preserves U64 for a downstream compare."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, flag BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, CASE WHEN flag > 0 THEN a + b ELSE 0 END AS c FROM t",
                schema_name=sn,
            )
            client.execute_sql("CREATE VIEW v2 AS SELECT id FROM v WHERE c > 100", schema_name=sn)
            half = 2 ** 62
            client.execute_sql(
                f"INSERT INTO t VALUES (1, {half}, {half}, 1), (2, {half}, {half}, 0)",
                schema_name=sn,
            )
            v2id = client.resolve_table(sn, "v2")[0]
            got = sorted(r["id"] for r in _dicts(client, v2id))
            # id=1: flag>0 → c = 2^63 (>100 unsigned). id=2: flag=0 → c=0.
            assert got == [1], f"U64-carrying CASE must survive downstream c > 100; got {got}"
        finally:
            _cleanup(client, sn, "v2", "v", "t")

    def test_sum_u64_downstream_compare(self, client):
        """SUM(u64) with a group sum >= 2^63 read by a downstream WHERE s > 100."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, u BIGINT UNSIGNED NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE VIEW v AS SELECT g, SUM(u) AS s FROM t GROUP BY g", schema_name=sn)
            client.execute_sql("CREATE VIEW v2 AS SELECT g FROM v WHERE s > 100", schema_name=sn)
            half = 2 ** 62
            # group 1: two rows of 2^62 → sum 2^63 (>100 unsigned). group 2: sum 3.
            client.execute_sql(
                f"INSERT INTO t VALUES (1, 1, {half}), (2, 1, {half}), (3, 2, 3)",
                schema_name=sn,
            )
            v2id = client.resolve_table(sn, "v2")[0]
            got = sorted(r["g"] for r in _dicts(client, v2id))
            assert got == [1], f"SUM(u64) group sum >= 2^63 must survive downstream s > 100; got {got}"
        finally:
            _cleanup(client, sn, "v2", "v", "t")

    def test_narrow_unsigned_unchanged(self, client):
        """Regression guard: narrow unsigned (U32) sums stay < 2^63 and compare normally."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, "
                "a INT UNSIGNED NOT NULL, b INT UNSIGNED NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE VIEW v AS SELECT id, a + b AS v FROM t", schema_name=sn)
            client.execute_sql("CREATE VIEW v2 AS SELECT id FROM v WHERE v > 100", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 3000000000, 3000000000), (2, 10, 10)", schema_name=sn)
            v2id = client.resolve_table(sn, "v2")[0]
            got = sorted(r["id"] for r in _dicts(client, v2id))
            # id=1: 6e9 > 100. id=2: 20 < 100.
            assert got == [1]
        finally:
            _cleanup(client, sn, "v2", "v", "t")
