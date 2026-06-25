"""E2E tests for UNION ALL, UNION, SELECT DISTINCT.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_set_ops.py -v --tb=short
"""
import random
import gnitz
import _oracle as oracle

def _uid():
    return str(random.randint(100000, 999999))


class TestSetOps:
    def _setup_two_tables(self, client, sn):
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )

    def test_union_all(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a UNION ALL SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO a VALUES (1, 10), (2, 20)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO b VALUES (3, 30), (4, 40)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == 4
            vals = sorted(r["val"] for r in rows)
            assert vals == [10, 20, 30, 40]

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_union_all_identical_rows_keep_both(self, client):
        """UNION ALL of two tables that each hold the same (pk, val) row keeps
        both copies (weight +2). The per-side branch discriminator in the
        synthetic hash PK is what stops the two identical rows from collapsing
        to one under unique-PK enforcement at the view sink."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a UNION ALL SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Same content on both sides.
            client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 10), (2, 20)", schema_name=sn)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            total_weight = sum(r.weight for r in rows)
            assert total_weight == 4, f"expected total weight 4, got {total_weight}"
            vals = sorted(r["val"] for r in rows for _ in range(r.weight))
            assert vals == [10, 10, 20, 20], f"got {vals}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_union_distinct(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a UNION SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Insert rows with overlapping values but different PKs.
            # NOTE: This test does not truly exercise UNION vs UNION ALL dedup
            # because dedup is by (PK, payload) — since all 4 rows have different
            # PKs, they are all distinct regardless. A true dedup test would require
            # projection-based UNION (e.g., SELECT val FROM a UNION SELECT val FROM b)
            # which is not yet supported.
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10), (2, 20)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO b VALUES (3, 10), (4, 30)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == 4

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_union_all_retraction(self, client):
        """Deleting a row from one source retracts it from the UNION ALL view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a UNION ALL SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO a VALUES (1, 10), (2, 20)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO b VALUES (3, 30)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 3

            client.execute_sql("DELETE FROM a WHERE pk = 1", schema_name=sn)
            rows = client.scan(vid)
            assert len(rows) == 2, f"expected 2 rows after delete, got {rows}"
            vals = sorted(r["val"] for r in rows)
            assert vals == [20, 30]

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_except_basic(self, client):
        """EXCEPT view excludes rows whose PK is present in the right table."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a EXCEPT SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Insert b FIRST so I(B) is populated when ΔA arrives
            client.execute_sql(
                "INSERT INTO b VALUES (2, 20)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            vals = sorted(r["val"] for r in rows)
            assert vals == [10, 30], f"expected [10, 30], got {vals}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_except_retraction(self, client):
        """Deleting a row from b adds it back to the EXCEPT view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a EXCEPT SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Insert b first so I(B) is populated when ΔA arrives
            client.execute_sql(
                "INSERT INTO b VALUES (2, 20)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10), (2, 20)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            vals = sorted(r["val"] for r in rows)
            assert vals == [10], f"expected [10], got {vals}"

            # Delete the exclusion row — (2, 20) should now appear in v
            client.execute_sql("DELETE FROM b WHERE pk = 2", schema_name=sn)
            rows = client.scan(vid)
            vals = sorted(r["val"] for r in rows)
            assert vals == [10, 20], f"expected [10, 20] after delete, got {vals}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_intersect_basic(self, client):
        """INTERSECT view contains only rows present in both tables."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a INTERSECT SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)",
                schema_name=sn,
            )
            # Only (2, 20) is shared (same pk AND same val)
            client.execute_sql(
                "INSERT INTO b VALUES (2, 20), (4, 40)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            vals = sorted(r["val"] for r in rows)
            assert vals == [20], f"expected [20], got {vals}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_select_distinct_view(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT DISTINCT * FROM t",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == 3

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_distinct_retraction(self, client):
        """DISTINCT view retracts correctly when a row is deleted."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT DISTINCT * FROM t",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 3

            # Delete one row — DISTINCT view should drop it
            client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
            rows = client.scan(vid)
            vals = sorted(r["val"] for r in rows)
            assert vals == [10, 30], f"expected [10, 30] after delete, got {vals}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_distinct_update_view(self, client):
        """UPDATE on a table with a SELECT DISTINCT view reflects the new value."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT DISTINCT * FROM t",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=sn)
            rows = client.scan(vid)
            assert len(rows) == 2

            # Update pk=1: old value should retract, new value should appear
            client.execute_sql("UPDATE t SET val = 99 WHERE pk = 1", schema_name=sn)
            rows = client.scan(vid)
            vals = sorted(r["val"] for r in rows)
            assert vals == [20, 99], f"expected [20, 99] after update, got {vals}"
        finally:
            client.drop_schema(sn)


class TestSetOpsDifferential:
    """Lock-in: the two-distinct-table set-ops, projected on `val` so dedup and
    weight actually matter, checked against the from-scratch oracle after each
    epoch (validates the oracle against the known-good two-table path)."""

    def _setup(self, client, sn, op):
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
        client.execute_sql(
            f"CREATE VIEW v AS SELECT val FROM a {op} SELECT val FROM b", schema_name=sn)
        return client.resolve_table(sn, "v")[0]

    def _run(self, client, op):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            vid = self._setup(client, sn, op)
            a_state, b_state = {}, {}

            def check(ctx):
                left = oracle.oracle_filter_project(a_state, None, ["val"])
                right = oracle.oracle_filter_project(b_state, None, ["val"])
                exp = oracle.oracle_setop(op, left, right)
                oracle.assert_view_matches(client, vid, ["val"], exp, ctx=ctx)

            # Overlapping values across the two tables so dedup/weight matters.
            client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn)
            oracle.apply_insert(a_state, "pk", [
                {"pk": 1, "val": 10}, {"pk": 2, "val": 20}, {"pk": 3, "val": 30}])
            check(f"{op} after-a")

            client.execute_sql("INSERT INTO b VALUES (4, 20), (5, 30), (6, 40)", schema_name=sn)
            oracle.apply_insert(b_state, "pk", [
                {"pk": 4, "val": 20}, {"pk": 5, "val": 30}, {"pk": 6, "val": 40}])
            check(f"{op} after-b")

            client.execute_sql("DELETE FROM b WHERE pk = 4", schema_name=sn)
            oracle.apply_delete(b_state, "pk", [4])
            check(f"{op} after-delete-b")

            client.execute_sql("UPDATE a SET val = 99 WHERE pk = 1", schema_name=sn)
            oracle.apply_update(a_state, "pk", 1, {"val": 99})
            check(f"{op} after-update-a")
        finally:
            client.drop_schema(sn)

    def test_except_differential(self, client):
        self._run(client, "EXCEPT")

    def test_intersect_differential(self, client):
        self._run(client, "INTERSECT")

    def test_union_differential(self, client):
        self._run(client, "UNION")

    def test_union_all_differential(self, client):
        self._run(client, "UNION ALL")


class TestDistinctDifferential:
    """SELECT DISTINCT projected so real dedup happens (many rows share one value),
    checked against the from-scratch oracle after every epoch. DISTINCT is the
    non-linear boundary-crossing operator (DBSP Prop 4.7) and had no differential
    test: a value stays at weight 1 while any row carries it, vanishes only when
    the last is retracted, and an UPDATE that moves a row's value crosses an exit
    and an entry boundary in a single epoch."""

    def test_distinct_single_col_churn(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql("CREATE VIEW v AS SELECT DISTINCT g FROM t", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            t_state = {}

            def check(ctx):
                exp = oracle.oracle_distinct(
                    oracle.oracle_filter_project(t_state, None, ["g"]))
                oracle.assert_view_matches(client, vid, ["g"], exp, ctx=ctx)

            # g=1 and g=2 each carried by two rows; g=3 by one.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 3)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "g": 1}, {"pk": 2, "g": 1}, {"pk": 3, "g": 2},
                {"pk": 4, "g": 2}, {"pk": 5, "g": 3}])
            check("after-insert")

            # Drop one of g=1's two carriers: g=1 stays (boundary NOT crossed).
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [1])
            check("after-delete-one-of-two")

            # Drop g=3's only carrier: g=3 vanishes (exit boundary crossed).
            client.execute_sql("DELETE FROM t WHERE pk = 5", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [5])
            check("after-delete-last-carrier")

            # Move g=1's last carrier (pk=2) to a brand-new g=4: g=1 exits and g=4
            # enters in one epoch — both boundaries cross simultaneously.
            client.execute_sql("UPDATE t SET g = 4 WHERE pk = 2", schema_name=sn)
            oracle.apply_update(t_state, "pk", 2, {"g": 4})
            check("after-update-cross-both-boundaries")

            # Re-introduce g=3.
            client.execute_sql("INSERT INTO t VALUES (6, 3)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 6, "g": 3}])
            check("after-reinsert")
        finally:
            client.drop_schema(sn)

    def test_distinct_two_col_churn(self, client):
        """DISTINCT over two columns: dedup keys on the whole (a, b) pair. An
        UPDATE to one component moves the pair, and the pair survives only while a
        carrier remains."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql("CREATE VIEW v AS SELECT DISTINCT a, b FROM t", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            t_state = {}

            def check(ctx):
                exp = oracle.oracle_distinct(
                    oracle.oracle_filter_project(t_state, None, ["a", "b"]))
                oracle.assert_view_matches(client, vid, ["a", "b"], exp, ctx=ctx)

            # (1,1) carried twice; (1,2) and (2,1) once each. (1,1) and (1,2) share
            # `a`, (1,1) and (2,1) share `b` — so dedup must key on the pair.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 1)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "a": 1, "b": 1}, {"pk": 2, "a": 1, "b": 1},
                {"pk": 3, "a": 1, "b": 2}, {"pk": 4, "a": 2, "b": 1}])
            check("after-insert")

            # Drop one carrier of (1,1): pair stays.
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [1])
            check("after-delete-one-carrier")

            # Move (1,2)'s carrier to (2,1): (1,2) exits, (2,1) already present so
            # its weight stays 1 (a second carrier, not a second row).
            client.execute_sql("UPDATE t SET b = 1, a = 2 WHERE pk = 3", schema_name=sn)
            oracle.apply_update(t_state, "pk", 3, {"a": 2, "b": 1})
            check("after-update-onto-existing-pair")

            # Remove pk=4 — (2,1) still carried by pk=3 → stays.
            client.execute_sql("DELETE FROM t WHERE pk = 4", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [4])
            check("after-delete-other-carrier")
        finally:
            client.drop_schema(sn)


class TestSetOpNullability:
    """Set-op output column nullability is the union of both inputs. A NOT NULL
    left paired with a nullable right that emits NULL must produce a nullable
    output column, so the reader checks the null bitmap instead of reading 0."""

    def test_union_all_right_null_reads_back_null(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            # Left val NOT NULL; right val nullable.
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a UNION ALL SELECT * FROM b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql("INSERT INTO a VALUES (1, 10)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (2, NULL)", schema_name=sn)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            vals = sorted((r["val"] is None, r["val"]) for r in rows)
            # One concrete 10 from the left, one NULL (not 0) from the right.
            assert (False, 10) in vals, f"left value missing: {vals}"
            assert any(r["val"] is None for r in rows), \
                f"right NULL must read back as NULL, not 0: {[r['val'] for r in rows]}"
            assert 0 not in [r["val"] for r in rows], "NULL must not be read as 0"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            client.drop_schema(sn)
