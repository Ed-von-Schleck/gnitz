"""E2E tests for UNION ALL, UNION, SELECT DISTINCT.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_set_ops.py -v --tb=short
"""
import random
import pytest
import gnitz
import _oracle as oracle

def _uid():
    return str(random.randint(100000, 999999))


def _create_ab_tables(client, sn):
    """Create the two single-PK `a`/`b` tables every set-op test runs against."""
    client.execute_sql(
        "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)


class TestSetOps:
    def _setup_two_tables(self, client, sn):
        _create_ab_tables(client, sn)

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

    def test_view_rejects_silently_dropped_clauses(self, client):
        """Every CREATE VIEW shape (simple, grouped, join, set-op, DISTINCT) reads only a
        hand-picked subset of the SELECT. A clause a shape does not consume must be rejected,
        not silently dropped — a dropped clause runs a different query than the caller wrote."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE u (pk BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)", schema_name=sn
            )

            def rejects(sql, msg):
                with pytest.raises(gnitz.GnitzError, match=msg):
                    client.execute_sql(sql, schema_name=sn)

            # --- DISTINCT ON: single SELECT, set-op branch, and the degenerate shape ---
            # (degenerate `DISTINCT ON (a) a` == `DISTINCT a` but is rejected ON PURPOSE —
            # not worth an ON-list/projection equivalence check.)
            for sql in (
                "CREATE VIEW v AS SELECT DISTINCT ON (a) a, b FROM t",
                "CREATE VIEW v AS SELECT DISTINCT ON (a) a FROM t UNION SELECT b FROM t",
                "CREATE VIEW v AS SELECT DISTINCT ON (a) a FROM t",
            ):
                rejects(sql, "DISTINCT ON is not supported")

            # --- GROUP BY / HAVING dropped in the DISTINCT-view and set-op-branch paths ---
            rejects("CREATE VIEW v AS SELECT DISTINCT a FROM t GROUP BY a", "GROUP BY is not supported")
            rejects("CREATE VIEW v AS SELECT DISTINCT a FROM t HAVING a > 0", "HAVING is not supported")
            rejects(
                "CREATE VIEW v AS SELECT a FROM t GROUP BY a UNION ALL SELECT b FROM t",
                "GROUP BY is not supported",
            )
            # HAVING on a non-grouped (simple) view: no reduce, the predicate never runs.
            rejects("CREATE VIEW v AS SELECT a FROM t HAVING a > 0", "HAVING is not supported")

            # --- PREWHERE (dropped filter) and TOP (dropped limit), across shapes ---
            rejects("CREATE VIEW v AS SELECT DISTINCT a FROM t PREWHERE a > 5", "PREWHERE is not supported")
            rejects("CREATE VIEW v AS SELECT a FROM t PREWHERE a > 5", "PREWHERE is not supported")
            rejects(
                "CREATE VIEW v AS SELECT a, COUNT(*) FROM t PREWHERE a > 5 GROUP BY a",
                "PREWHERE is not supported",
            )
            rejects("CREATE VIEW v AS SELECT DISTINCT TOP 5 a FROM t", "TOP is not supported")

            # --- exotic clauses now each report by name (QUALIFY on DISTINCT, SORT BY on simple) ---
            rejects(
                "CREATE VIEW v AS SELECT DISTINCT a FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a) = 1",
                "QUALIFY is not supported",
            )
            rejects("CREATE VIEW v AS SELECT a FROM t SORT BY a", "SORT BY is not supported")

            # --- a top-level WHERE on a JOIN view has no builder and would be dropped ---
            rejects(
                "CREATE VIEW v AS SELECT t.a FROM t JOIN u ON t.pk = u.pk WHERE t.a > 5",
                "WHERE is not supported",
            )

            # --- GROUP BY ALL must be classified, never routed to the simple path and dropped ---
            rejects("CREATE VIEW v AS SELECT a FROM t GROUP BY ALL", "GROUP BY")

            # --- FETCH dropped at the view front door (Query-level row-limit) ---
            rejects("CREATE VIEW v AS SELECT a FROM t FETCH FIRST 5 ROWS ONLY", "FETCH is not supported")

            # --- plain branch DISTINCT (caller check; would leak dups under UNION ALL) ---
            rejects(
                "CREATE VIEW v AS SELECT DISTINCT a FROM t UNION ALL SELECT b FROM t",
                "DISTINCT on the .* branch is not supported",
            )

            # --- every honored shape still compiles (positive controls) ---
            ok = {
                "vsimple": "SELECT a, b FROM t WHERE a > 0",
                "vdistinct": "SELECT DISTINCT a, b FROM t",
                "vgroup": "SELECT a, COUNT(*) FROM t GROUP BY a",
                "vjoin": "SELECT t.a, u.c FROM t JOIN u ON t.pk = u.pk",
                "vsetop": "SELECT a FROM t UNION ALL SELECT b FROM t",
            }
            for name, body in ok.items():
                client.execute_sql(f"CREATE VIEW {name} AS {body}", schema_name=sn)
                client.resolve_table(sn, name)  # registered (raises if absent)
                client.execute_sql(f"DROP VIEW {name}", schema_name=sn)

            client.execute_sql("DROP TABLE u", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)


class TestBagSetOps:
    """EXCEPT ALL / INTERSECT ALL preserve bag multiplicity: per row
    max(0, cL−cR) and min(cL, cR), vs the deduplicating EXCEPT/INTERSECT.
    Multiplicity comes from distinct-PK base rows projected onto a shared value
    column. Expected multisets are hand-computed (independent of the oracle)."""

    def _two_tables(self, client, sn):
        _create_ab_tables(client, sn)

    def test_except_all_arithmetic(self, client):
        """left {10:3, 20:1}, right {10:1, 30:2} ⇒ EXCEPT ALL ⇒ {10:2, 20:1}.
        Row 30 (cL=0, cR=2) drives positive_part's integral net-NEGATIVE (A−B=−2)
        and must still be absent — the one behavior no set-op `distinct` exercises
        (every existing `distinct` input is base-positive)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT val FROM a EXCEPT ALL SELECT val FROM b", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql("INSERT INTO a VALUES (1,10),(2,10),(3,10),(4,20)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (5,10),(6,30),(7,30)", schema_name=sn)

            oracle.assert_view_matches(
                client, vid, ["val"], {(10,): 2, (20,): 1}, ctx="except_all_arith")
        finally:
            client.drop_schema(sn)

    def test_intersect_all_arithmetic(self, client):
        """left {10:3, 20:1}, right {10:1, 30:2} ⇒ INTERSECT ALL ⇒ {10:1}.
        20 (cR=0) and 30 (cL=0) both drop out; 10 carries min(3,1)=1."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT val FROM a INTERSECT ALL SELECT val FROM b", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql("INSERT INTO a VALUES (1,10),(2,10),(3,10),(4,20)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (5,10),(6,30),(7,30)", schema_name=sn)

            oracle.assert_view_matches(
                client, vid, ["val"], {(10,): 1}, ctx="intersect_all_arith")
        finally:
            client.drop_schema(sn)

    def test_bag_retraction(self, client):
        """Delete the single right '10': EXCEPT ALL's 10-count rises 2→3,
        INTERSECT ALL's 10-count falls 1→0. Pins positive_part's boundary emit
        under a negative delta on both bag operators at once."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v_exc AS SELECT val FROM a EXCEPT ALL SELECT val FROM b", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v_int AS SELECT val FROM a INTERSECT ALL SELECT val FROM b", schema_name=sn)
            exc = client.resolve_table(sn, "v_exc")[0]
            inter = client.resolve_table(sn, "v_int")[0]

            client.execute_sql("INSERT INTO a VALUES (1,10),(2,10),(3,10),(4,20)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (5,10),(6,30),(7,30)", schema_name=sn)
            oracle.assert_view_matches(client, exc, ["val"], {(10,): 2, (20,): 1}, ctx="exc-before")
            oracle.assert_view_matches(client, inter, ["val"], {(10,): 1}, ctx="int-before")

            # Delete the lone right-side 10 (pk=5): right 10-count 1→0.
            client.execute_sql("DELETE FROM b WHERE pk = 5", schema_name=sn)
            oracle.assert_view_matches(client, exc, ["val"], {(10,): 3, (20,): 1}, ctx="exc-after")
            oracle.assert_view_matches(client, inter, ["val"], {}, ctx="int-after")
        finally:
            client.drop_schema(sn)

    def test_set_vs_bag_divergence(self, client):
        """cL=2, cR=1 for value 10: set EXCEPT removes it entirely (→ absent);
        bag EXCEPT ALL keeps max(0, 2−1)=1. The two views are now distinct code
        paths over the same inputs."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._two_tables(client, sn)
            client.execute_sql(
                "CREATE VIEW v_set AS SELECT val FROM a EXCEPT SELECT val FROM b", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v_bag AS SELECT val FROM a EXCEPT ALL SELECT val FROM b", schema_name=sn)
            v_set = client.resolve_table(sn, "v_set")[0]
            v_bag = client.resolve_table(sn, "v_bag")[0]

            client.execute_sql("INSERT INTO a VALUES (1,10),(2,10)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (3,10)", schema_name=sn)

            oracle.assert_view_matches(client, v_set, ["val"], {}, ctx="set EXCEPT (10 fully removed)")
            oracle.assert_view_matches(client, v_bag, ["val"], {(10,): 1}, ctx="bag EXCEPT ALL (one 10 survives)")
        finally:
            client.drop_schema(sn)

    def test_quantifier_dispatch(self, client):
        """EXCEPT ALL / INTERSECT ALL now compile (bag semantics); BY NAME set
        operations are rejected, not silently aligned positionally. A rejected
        CREATE registers no view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._two_tables(client, sn)

            # BY NAME (any operator/quantifier) is unimplemented → rejected.
            rejected = {
                "union_all_by_name": "SELECT val FROM a UNION ALL BY NAME SELECT val FROM b",
                "intersect_by_name": "SELECT val FROM a INTERSECT BY NAME SELECT val FROM b",
                "except_by_name": "SELECT val FROM a EXCEPT BY NAME SELECT val FROM b",
            }
            for name, body in rejected.items():
                with pytest.raises(Exception):
                    client.execute_sql(f"CREATE VIEW rej_{name} AS {body}", schema_name=sn)
                with pytest.raises(Exception):
                    client.resolve_table(sn, f"rej_{name}")  # no view registered

            # Bag ALL forms and the deduplicating forms all create successfully.
            ok = {
                "ok_except_all": "SELECT val FROM a EXCEPT ALL SELECT val FROM b",
                "ok_intersect_all": "SELECT val FROM a INTERSECT ALL SELECT val FROM b",
                "ok_union_all": "SELECT val FROM a UNION ALL SELECT val FROM b",
                "ok_except": "SELECT val FROM a EXCEPT SELECT val FROM b",
                "ok_intersect": "SELECT val FROM a INTERSECT SELECT val FROM b",
            }
            for name, body in ok.items():
                client.execute_sql(f"CREATE VIEW {name} AS {body}", schema_name=sn)
                client.resolve_table(sn, name)  # registered (raises if absent)
                client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        finally:
            client.drop_schema(sn)


class TestSetOpsDifferential:
    """Lock-in: the two-distinct-table set-ops, projected on `val` so dedup and
    weight actually matter, checked against the from-scratch oracle after each
    epoch (validates the oracle against the known-good two-table path)."""

    def _setup(self, client, sn, op):
        _create_ab_tables(client, sn)
        client.execute_sql(
            f"CREATE VIEW v AS SELECT val FROM a {op} SELECT val FROM b", schema_name=sn)
        return client.resolve_table(sn, "v")[0]

    def _assert_setop(self, client, vid, op, a_state, b_state, ctx):
        left = oracle.oracle_filter_project(a_state, None, ["val"])
        right = oracle.oracle_filter_project(b_state, None, ["val"])
        exp = oracle.oracle_setop(op, left, right)
        oracle.assert_view_matches(client, vid, ["val"], exp, ctx=ctx)

    def _run(self, client, op):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            vid = self._setup(client, sn, op)
            a_state, b_state = {}, {}

            def check(ctx):
                self._assert_setop(client, vid, op, a_state, b_state, ctx)

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

    def test_except_all_differential(self, client):
        self._run(client, "EXCEPT ALL")

    def test_intersect_all_differential(self, client):
        self._run(client, "INTERSECT ALL")

    def _run_membership_flip(self, client, op):
        """Drive a single projected value across the set-membership boundary in
        BOTH directions, including the tick where positive_part's pre-image
        integral goes negative (da − db = −1 while it stays clamped at 0). Both
        EXCEPT and INTERSECT DISTINCT route through positive_part, so its
        boundary emits must net exactly per the from-scratch oracle each tick."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            vid = self._setup(client, sn, op)
            a_state, b_state = {}, {}

            def check(ctx):
                self._assert_setop(client, vid, op, a_state, b_state, ctx)

            # a has val=10 (da=1), b empty (db=0): da − db = 1.
            client.execute_sql("INSERT INTO a VALUES (1, 10)", schema_name=sn)
            oracle.apply_insert(a_state, "pk", [{"pk": 1, "val": 10}])
            check(f"{op} a-only")

            # b gains val=10 (db=1): da − db = 0.
            client.execute_sql("INSERT INTO b VALUES (2, 10)", schema_name=sn)
            oracle.apply_insert(b_state, "pk", [{"pk": 2, "val": 10}])
            check(f"{op} both")

            # Retract a's only val=10 (da=0) while b still has it (db=1): the
            # positive_part input integral for val=10 is now da − db = −1.
            client.execute_sql("DELETE FROM a WHERE pk = 1", schema_name=sn)
            oracle.apply_delete(a_state, "pk", [1])
            check(f"{op} a-empty-negative-integral")

            # Retract b's val=10 too (db=0): da − db back to 0.
            client.execute_sql("DELETE FROM b WHERE pk = 2", schema_name=sn)
            oracle.apply_delete(b_state, "pk", [2])
            check(f"{op} both-empty")

            # Re-introduce val=10 on a only (da=1, db=0): da − db = 1 again.
            client.execute_sql("INSERT INTO a VALUES (3, 10)", schema_name=sn)
            oracle.apply_insert(a_state, "pk", [{"pk": 3, "val": 10}])
            check(f"{op} a-only-again")
        finally:
            client.drop_schema(sn)

    def test_except_membership_flip(self, client):
        self._run_membership_flip(client, "EXCEPT")

    def test_intersect_membership_flip(self, client):
        self._run_membership_flip(client, "INTERSECT")


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
