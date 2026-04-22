"""E2E regression tests for narrow integer payload comparisons and SQL 3VL.

Two gap areas covered:
  1. TestNarrowTypeViewOps: DELETE/UPDATE through views exercises
     compare_cursor_payload_to_batch_row for TINYINT/SMALLINT/INT payloads.
     Before the fix, any DELETE/UPDATE on a narrow-typed payload column would
     panic inside the DISTINCT operator.
  2. TestSqlThreeValuedLogic: NULL OR TRUE = TRUE and NULL AND FALSE = FALSE
     in WHERE clauses (SQL 3VL correctness for BOOL_OR / BOOL_AND).
"""
import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _scan_map(client, tid):
    return {row.pk: row for row in client.scan(tid) if row.weight > 0}


def _cleanup(client, sn, *names):
    """Drop objects by name (tries VIEW then TABLE for each), then drop schema."""
    for name in names:
        for stmt in [f"DROP VIEW {name}", f"DROP TABLE {name}"]:
            try:
                client.execute_sql(stmt, schema_name=sn)
            except Exception:
                pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Type parameters: (sql_type, lo, mid, hi)
# ---------------------------------------------------------------------------

NARROW_SIGNED = [
    pytest.param("TINYINT",  -128, 0, 127,              id="i8"),
    pytest.param("SMALLINT", -32768, 0, 32767,          id="i16"),
    pytest.param("INT",      -2147483648, 0, 2147483647, id="i32"),
]

NARROW_UNSIGNED = [
    pytest.param("TINYINT UNSIGNED",  0, 127, 255,          id="u8"),
    pytest.param("SMALLINT UNSIGNED", 0, 32767, 65535,      id="u16"),
    pytest.param("INT UNSIGNED",      0, 2147483647, 4294967295, id="u32"),
]

ALL_NARROW = NARROW_SIGNED + NARROW_UNSIGNED


# ---------------------------------------------------------------------------
# TestNarrowTypeViewOps
# ---------------------------------------------------------------------------

class TestNarrowTypeViewOps:
    """Routes DELETE/UPDATE deltas through a view's DISTINCT operator for every
    narrow integer type.

    The code path exercised: op_distinct → compare_cursor_payload_to_batch_row.
    Before the fix, this function called i64::from_le_bytes on a 4/2/1-byte
    slice (TINYINT/SMALLINT/INT), which always panicked.
    """

    def _setup(self, client, sn, col_type_sql):
        client.create_schema(sn)
        client.execute_sql(
            f"CREATE TABLE t "
            f"(pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, v {col_type_sql} NOT NULL)",
            schema_name=sn,
        )
        # View must be created before data is inserted so it processes all deltas.
        client.execute_sql("CREATE VIEW vw AS SELECT * FROM t", schema_name=sn)
        tid, _ = client.resolve_table(sn, "t")
        vid, _ = client.resolve_table(sn, "vw")
        return tid, vid

    @pytest.mark.parametrize("col_type,lo,mid,hi", ALL_NARROW)
    def test_delete_retraction_through_view(self, client, col_type, lo, mid, hi):
        """DELETE after INSERT sends a retraction delta through op_distinct.

        The trace holds the INSERT element; the retraction delta has the same
        (PK, payload), triggering compare_cursor_payload_to_batch_row on the
        narrow integer payload column.
        """
        sn = "s" + _uid()
        try:
            _, vid = self._setup(client, sn, col_type)
            client.execute_sql(
                f"INSERT INTO t VALUES (1, {lo}), (2, {mid}), (3, {hi})",
                schema_name=sn,
            )
            client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
            rows = _scan_map(client, vid)
            assert set(rows.keys()) == {1, 3}, (
                f"after DELETE pk=2 expected {{1,3}} in view, got {set(rows.keys())}"
            )
            assert rows[1].v == lo
            assert rows[3].v == hi
        finally:
            _cleanup(client, sn, "vw", "t")

    @pytest.mark.parametrize("col_type,lo,mid,hi", ALL_NARROW)
    def test_update_retraction_through_view(self, client, col_type, lo, mid, hi):
        """UPDATE = retract-old + insert-new; retraction triggers distinct comparison.

        The retraction delta element has the narrow integer payload. Before the
        fix, compare_cursor_payload_to_batch_row would panic on I8/I16/I32.
        """
        sn = "s" + _uid()
        try:
            _, vid = self._setup(client, sn, col_type)
            client.execute_sql(
                f"INSERT INTO t VALUES (1, {lo}), (2, {mid})",
                schema_name=sn,
            )
            client.execute_sql(
                f"UPDATE t SET v = {hi} WHERE pk = 1",
                schema_name=sn,
            )
            rows = _scan_map(client, vid)
            assert rows[1].v == hi, (
                f"after UPDATE pk=1 v→{hi}: expected v={hi}, got v={rows[1].v}"
            )
            assert rows[2].v == mid
        finally:
            _cleanup(client, sn, "vw", "t")

    @pytest.mark.parametrize("col_type,lo,mid,hi", ALL_NARROW)
    def test_filtered_view_narrow_type(self, client, col_type, lo, mid, hi):
        """View with WHERE v >= mid correctly filters narrow integer payload rows."""
        sn = "s" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                f"CREATE TABLE t "
                f"(pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, v {col_type} NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                f"CREATE VIEW vw AS SELECT * FROM t WHERE v >= {mid}",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "vw")
            client.execute_sql(
                f"INSERT INTO t VALUES (1, {lo}), (2, {mid}), (3, {hi})",
                schema_name=sn,
            )
            rows = _scan_map(client, vid)
            assert 1 not in rows, (
                f"pk=1 (v={lo}) must not pass WHERE v >= {mid}"
            )
            assert 2 in rows and rows[2].v == mid
            assert 3 in rows and rows[3].v == hi
        finally:
            _cleanup(client, sn, "vw", "t")


# ---------------------------------------------------------------------------
# TestSqlThreeValuedLogic
# ---------------------------------------------------------------------------

class TestSqlThreeValuedLogic:
    """Validates SQL 3-valued logic for OR and AND in view WHERE clauses.

    Before the fix, BOOL_OR and BOOL_AND propagated NULL whenever either
    operand was NULL, violating:
      - NULL OR  TRUE  = TRUE   (must include row)
      - TRUE  OR NULL  = TRUE   (must include row)
      - NULL AND FALSE = FALSE  (excluded either way, but correct reason)
    The most observable gap: rows that should appear in a view (NULL OR TRUE)
    were silently dropped.
    """

    def _make_or_view(self, client, sn):
        client.create_schema(sn)
        client.execute_sql(
            "CREATE TABLE t "
            "(pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, a BIGINT NULL, b BIGINT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW vw AS SELECT * FROM t WHERE a > 0 OR b > 0",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "vw")
        return vid

    def test_null_or_true_includes_row(self, client):
        """NULL OR TRUE = TRUE: row with a=NULL, b=5 must appear in the view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            vid = self._make_or_view(client, sn)
            # pk=1: a=NULL, b=5  → NULL  OR TRUE  = TRUE  → included
            # pk=2: a=NULL, b=-1 → NULL  OR FALSE = NULL  → excluded
            # pk=3: a=1, b=1     → TRUE  OR TRUE  = TRUE  → included (control)
            client.execute_sql(
                "INSERT INTO t VALUES (1, NULL, 5), (2, NULL, -1), (3, 1, 1)",
                schema_name=sn,
            )
            rows = _scan_map(client, vid)
            assert 1 in rows, "pk=1 (a=NULL, b=5): NULL OR TRUE must = TRUE → row included"
            assert 2 not in rows, "pk=2 (a=NULL, b=-1): NULL OR FALSE must = NULL → row excluded"
            assert 3 in rows, "pk=3 (a=1, b=1): TRUE OR TRUE control"
        finally:
            _cleanup(client, sn, "vw", "t")

    def test_true_or_null_includes_row(self, client):
        """TRUE OR NULL = TRUE: row with a=1, b=NULL must appear in the view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            vid = self._make_or_view(client, sn)
            # pk=1: a=1,  b=NULL → TRUE  OR NULL  = TRUE  → included
            # pk=2: a=-1, b=NULL → FALSE OR NULL  = NULL  → excluded
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, NULL), (2, -1, NULL)",
                schema_name=sn,
            )
            rows = _scan_map(client, vid)
            assert 1 in rows, "pk=1 (a=1, b=NULL): TRUE OR NULL must = TRUE → row included"
            assert 2 not in rows, "pk=2 (a=-1, b=NULL): FALSE OR NULL must = NULL → row excluded"
        finally:
            _cleanup(client, sn, "vw", "t")

    def test_all_null_excluded(self, client):
        """NULL OR NULL = NULL: row with both columns NULL must not appear."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            vid = self._make_or_view(client, sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, NULL, NULL), (2, 1, 1)",
                schema_name=sn,
            )
            rows = _scan_map(client, vid)
            assert 1 not in rows, "pk=1 (a=NULL, b=NULL): NULL OR NULL must = NULL → excluded"
            assert 2 in rows, "pk=2 (a=1, b=1): TRUE OR TRUE control"
        finally:
            _cleanup(client, sn, "vw", "t")

    def test_and_all_combinations(self, client):
        """AND 3VL: only TRUE AND TRUE includes a row; all other combos exclude."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t "
                "(pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, a BIGINT NULL, b BIGINT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW vw AS SELECT * FROM t WHERE a > 0 AND b > 0",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "vw")
            # pk=1: TRUE  AND TRUE  = TRUE  → included
            # pk=2: NULL  AND TRUE  = NULL  → excluded
            # pk=3: FALSE AND NULL  = FALSE → excluded (key 3VL case: not NULL)
            # pk=4: NULL  AND NULL  = NULL  → excluded
            # pk=5: TRUE  AND NULL  = NULL  → excluded
            # pk=6: FALSE AND FALSE = FALSE → excluded
            client.execute_sql(
                "INSERT INTO t VALUES "
                "(1, 1, 1), (2, NULL, 1), (3, -1, NULL), "
                "(4, NULL, NULL), (5, 1, NULL), (6, -1, -1)",
                schema_name=sn,
            )
            rows = _scan_map(client, vid)
            assert set(rows.keys()) == {1}, (
                f"only pk=1 (TRUE AND TRUE) must pass; got {set(rows.keys())}"
            )
        finally:
            _cleanup(client, sn, "vw", "t")

    @pytest.mark.parametrize("col_type,lo,mid,hi", ALL_NARROW)
    def test_null_or_true_narrow_nullable(self, client, col_type, lo, mid, hi):
        """NULL OR TRUE = TRUE holds for nullable narrow integer payload columns.

        Parametrized over all 6 narrow types to catch any type-specific path
        in the expression evaluator that might not obey 3VL.
        """
        sn = "s" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                f"CREATE TABLE t "
                f"(pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
                f"a {col_type} NULL, b {col_type} NULL)",
                schema_name=sn,
            )
            # Threshold = lo so that only values strictly above lo are "true".
            # For signed:   lo=-128  → hi=127  passes, lo=-128  does not.
            # For unsigned: lo=0     → hi=255  passes, lo=0     does not.
            client.execute_sql(
                f"CREATE VIEW vw AS SELECT * FROM t WHERE a > {lo} OR b > {lo}",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "vw")
            # pk=1: a=NULL, b=hi → NULL OR TRUE  = TRUE  → included
            # pk=2: a=hi,   b=NULL → TRUE OR NULL  = TRUE  → included
            # pk=3: a=NULL, b=lo → NULL OR FALSE = NULL  → excluded
            # pk=4: a=lo,   b=NULL → FALSE OR NULL = NULL → excluded
            client.execute_sql(
                f"INSERT INTO t VALUES "
                f"(1, NULL, {hi}), (2, {hi}, NULL), "
                f"(3, NULL, {lo}), (4, {lo}, NULL)",
                schema_name=sn,
            )
            rows = _scan_map(client, vid)
            assert 1 in rows, (
                f"pk=1 (a=NULL, b={hi}): NULL OR TRUE must = TRUE → included"
            )
            assert 2 in rows, (
                f"pk=2 (a={hi}, b=NULL): TRUE OR NULL must = TRUE → included"
            )
            assert 3 not in rows, (
                f"pk=3 (a=NULL, b={lo}): NULL OR FALSE must = NULL → excluded"
            )
            assert 4 not in rows, (
                f"pk=4 (a={lo}, b=NULL): FALSE OR NULL must = NULL → excluded"
            )
        finally:
            _cleanup(client, sn, "vw", "t")
