"""E2E tests for scalar aggregate subquery views (correlated + uncorrelated) and
(Step 4) ANY/ALL quantifiers.

Every scenario asserts weights, not just row presence.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_scalar_subquery.py -v --tb=short
"""
import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, *names):
    for name in names:
        for stmt in (f"DROP VIEW {name}", f"DROP TABLE {name}"):
            try:
                client.execute_sql(stmt, schema_name=sn)
                break
            except Exception:
                pass
    client.drop_schema(sn)


def _rows(client, vid):
    """List of (row, weight) for positive net-weight rows keyed by all columns."""
    agg = {}
    for row in client.scan(vid):
        if row.weight == 0:
            continue
        key = tuple(sorted((c, getattr(row, c)) for c in row._fields if c != "weight")) \
            if hasattr(row, "_fields") else None
        agg[key if key is not None else id(row)] = agg.get(key, 0) + row.weight
    return agg


def _col_weights(client, vid, *cols):
    """{(col values…): net_weight} over positive-weight rows."""
    out = {}
    for row in client.scan(vid):
        if row.weight == 0:
            continue
        key = tuple(getattr(row, c) for c in cols)
        out[key] = out.get(key, 0) + row.weight
    return {k: w for k, w in out.items() if w != 0}


_CREATE_A = "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)"
_CREATE_B = "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL)"


class TestCorrelatedScalar:
    def _setup(self, client, sn):
        client.execute_sql(_CREATE_A, schema_name=sn)
        client.execute_sql(_CREATE_B, schema_name=sn)

    def test_projected_count_bug(self, client):
        """A projected correlated COUNT reads 0 (not NULL, not dropped) for an
        outer row with no inner matches, and transitions with balanced weights."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c FROM a",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            # No inner rows: every outer row shows c = 0.
            assert _col_weights(client, vid, "id", "c") == {(1, 0): 1, (2, 0): 1}
            # First inner match for k=10 -> id 1 transitions 0 -> 1.
            client.execute_sql("INSERT INTO b VALUES (1, 10, 5)", schema_name=sn)
            assert _col_weights(client, vid, "id", "c") == {(1, 1): 1, (2, 0): 1}
            # Second match for k=10 -> c = 2.
            client.execute_sql("INSERT INTO b VALUES (2, 10, 6)", schema_name=sn)
            assert _col_weights(client, vid, "id", "c") == {(1, 2): 1, (2, 0): 1}
            # Delete both -> back to 0.
            client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
            client.execute_sql("DELETE FROM b WHERE id = 2", schema_name=sn)
            assert _col_weights(client, vid, "id", "c") == {(1, 0): 1, (2, 0): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_projected_sum_null_over_empty(self, client):
        """A projected correlated SUM is NULL over an empty group."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id, (SELECT SUM(w) FROM b WHERE b.k = a.k) AS s FROM a",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 10, 5), (2, 10, 6)", schema_name=sn)
            # id 1 (k=10) -> SUM = 11; id 2 (k=20) -> SUM = NULL.
            assert _col_weights(client, vid, "id", "s") == {(1, 11): 1, (2, None): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_correlated_count_in_where(self, client):
        """A correlated COUNT in a WHERE comparison filters rows and moves with
        the group aggregate."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.v FROM a "
                "WHERE (SELECT COUNT(*) FROM b WHERE b.k = a.k) >= 2",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            assert _col_weights(client, vid, "v") == {}
            client.execute_sql("INSERT INTO b VALUES (1, 10, 5), (2, 10, 6)", schema_name=sn)
            assert _col_weights(client, vid, "v") == {(100,): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")


class TestUncorrelatedScalar:
    def _setup(self, client, sn):
        client.execute_sql(_CREATE_A, schema_name=sn)
        client.execute_sql(_CREATE_B, schema_name=sn)

    def test_uncorrelated_max_range(self, client):
        """`a.v < (SELECT MAX(w) FROM b)` — pure-range INNER join against the
        one-row global aggregate, correct at W>1 (broadcast relay)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id FROM a WHERE a.v < (SELECT MAX(w) FROM b)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)", schema_name=sn)
            # Empty b -> MAX = NULL -> a.v < NULL is UNKNOWN -> no rows.
            assert _col_weights(client, vid, "id") == {}
            client.execute_sql("INSERT INTO b VALUES (1, 1, 25)", schema_name=sn)
            # MAX = 25 -> rows with v < 25: id 1 (10), id 2 (20).
            assert _col_weights(client, vid, "id") == {(1,): 1, (2,): 1}
            client.execute_sql("INSERT INTO b VALUES (2, 1, 100)", schema_name=sn)
            # MAX = 100 -> all three.
            assert _col_weights(client, vid, "id") == {(1,): 1, (2,): 1, (3,): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_uncorrelated_count_eq(self, client):
        """`a.v = (SELECT COUNT(*) FROM b)` — equi join against the global COUNT,
        whose ground row is a real 0 (so v = 0 matches an empty b)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT c.id FROM c WHERE c.v = (SELECT COUNT(*) FROM b)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO c VALUES (1, 0), (2, 1), (3, 2)", schema_name=sn)
            # Empty b -> COUNT = 0 -> only c.v = 0 matches (id 1).
            assert _col_weights(client, vid, "id") == {(1,): 1}
            client.execute_sql("INSERT INTO b VALUES (1, 1, 5)", schema_name=sn)
            # COUNT = 1 -> id 2.
            assert _col_weights(client, vid, "id") == {(2,): 1}
        finally:
            _cleanup(client, sn, "v", "c", "a", "b")


class TestAuditCases:
    """Cases the external audit flagged: self-correlation (E), COALESCE fold (D),
    IS NULL fold (Item 3), multiple subqueries with unique synthetic names (B)."""

    def _setup(self, client, sn):
        client.execute_sql(_CREATE_A, schema_name=sn)
        client.execute_sql(_CREATE_B, schema_name=sn)

    def test_self_correlated_subquery(self, client):
        """Issue E: outer and inner are the same base table (different aliases).
        Supported via the Gᵢ reduce (a distinct source id)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a1.id FROM a a1 "
                "WHERE a1.v >= (SELECT SUM(a2.v) FROM a a2 WHERE a2.k = a1.k)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # k=10: id1 v=100, id2 v=5, SUM=105. id1:100>=105 no; id2:5>=105 no.
            # k=20: id3 v=7, SUM=7. id3:7>=7 yes.
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 10, 5), (3, 20, 7)", schema_name=sn)
            assert _col_weights(client, vid, "id") == {(3,): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_coalesce_fold_count_nonlast(self, client):
        """Issue D: COALESCE((SELECT COUNT..), 5) folds to the COUNT (never NULL)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id, "
                "COALESCE((SELECT COUNT(*) FROM b WHERE b.k = a.k), 5) AS c FROM a",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 10, 5)", schema_name=sn)
            # id1 (k=10): COUNT=1 -> COALESCE(1,5)=1. id2 (k=20): COUNT=0 -> COALESCE(0,5)=0 (never 5).
            assert _col_weights(client, vid, "id", "c") == {(1, 1): 1, (2, 0): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_count_is_null_fold(self, client):
        """Item 3: (SELECT COUNT..) IS NULL folds to FALSE; IS NOT NULL to TRUE."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id FROM a "
                "WHERE (SELECT COUNT(*) FROM b WHERE b.k = a.k) IS NOT NULL",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            # COUNT is never NULL -> IS NOT NULL is always TRUE -> all rows.
            assert _col_weights(client, vid, "id") == {(1,): 1, (2,): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_two_correlated_subqueries(self, client):
        """Issue B: two scalar subqueries stack with distinct synthetic names."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id, "
                "(SELECT SUM(w) FROM b WHERE b.k = a.k) AS sb, "
                "(SELECT SUM(x) FROM c WHERE c.k = a.k) AS sc FROM a",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 10, 7)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (1, 10, 9)", schema_name=sn)
            assert _col_weights(client, vid, "id", "sb", "sc") == {(1, 7, 9): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b", "c")

    def test_backfill(self, client):
        """View created after data: backfill matches incremental."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 10, 5), (2, 10, 6)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c FROM a",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            assert _col_weights(client, vid, "id", "c") == {(1, 2): 1, (2, 0): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")


class TestQuantifiers:
    """ANY/ALL quantified comparisons (Step 4)."""

    def _setup(self, client, sn):
        client.execute_sql(_CREATE_A, schema_name=sn)
        # b.w is NOT NULL so it can be an ANY/ALL set column.
        client.execute_sql(_CREATE_B, schema_name=sn)

    def test_eq_any_is_in(self, client):
        """`x = ANY (SELECT k FROM b)` behaves as `x IN (SELECT k FROM b)`."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.v FROM a WHERE a.k = ANY (SELECT k FROM b)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 20, 5)", schema_name=sn)
            assert _col_weights(client, vid, "v") == {(200,): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_ne_all_is_not_in(self, client):
        """`x <> ALL (SELECT k FROM b)` behaves as `x NOT IN (SELECT k FROM b)`."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.v FROM a WHERE a.k <> ALL (SELECT k FROM b)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 20, 5)", schema_name=sn)
            assert _col_weights(client, vid, "v") == {(100,): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_correlated_lt_any(self, client):
        """`a.v < ANY (SELECT w FROM b WHERE b.k=a.k)` ⟺ a.v < MAX(w per k).
        ANY over an empty group is FALSE (row excluded)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id FROM a "
                "WHERE a.v < ANY (SELECT w FROM b WHERE b.k = a.k)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 5), (2, 10, 50), (3, 20, 1)", schema_name=sn)
            # b for k=10: w in {8, 20} -> MAX=20. id1 v=5 <20 yes; id2 v=50<20 no.
            # k=20 empty -> ANY(∅)=FALSE -> id3 out.
            client.execute_sql("INSERT INTO b VALUES (1, 10, 8), (2, 10, 20)", schema_name=sn)
            assert _col_weights(client, vid, "id") == {(1,): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_correlated_ge_all(self, client):
        """`a.v >= ALL (SELECT w FROM b WHERE b.k=a.k)` ⟺ a.v >= MAX(w per k).
        ALL over an empty group is TRUE (row included)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id FROM a "
                "WHERE a.v >= ALL (SELECT w FROM b WHERE b.k = a.k)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # k=10: w in {8,20} -> MAX=20. id1 v=25>=20 yes; id2 v=10>=20 no.
            # k=20 empty -> ALL(∅)=TRUE -> id3 in.
            client.execute_sql("INSERT INTO a VALUES (1, 10, 25), (2, 10, 10), (3, 20, 7)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 10, 8), (2, 10, 20)", schema_name=sn)
            assert _col_weights(client, vid, "id") == {(1,): 1, (3,): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_negated_any_over_empty(self, client):
        """`NOT (a.v < ANY (empty))` is TRUE — the existence conjunct makes the
        empty-group edge a definite FALSE that negates to TRUE."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id FROM a "
                "WHERE NOT (a.v < ANY (SELECT w FROM b WHERE b.k = a.k))",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # No b rows: every group empty -> ANY FALSE -> NOT FALSE = TRUE -> all rows.
            client.execute_sql("INSERT INTO a VALUES (1, 10, 5), (2, 20, 9)", schema_name=sn)
            assert _col_weights(client, vid, "id") == {(1,): 1, (2,): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_uncorrelated_lt_any(self, client):
        """`a.v < ANY (SELECT w FROM b)` ⟺ a.v < global MAX(w)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id FROM a WHERE a.v < ANY (SELECT w FROM b)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)", schema_name=sn)
            assert _col_weights(client, vid, "id") == {}  # empty b -> ANY false
            client.execute_sql("INSERT INTO b VALUES (1, 1, 25)", schema_name=sn)
            assert _col_weights(client, vid, "id") == {(1,): 1, (2,): 1}  # < 25
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_rejections(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            # = ALL unsupported.
            with pytest.raises(Exception):
                client.execute_sql(
                    "CREATE VIEW bad AS SELECT a.v FROM a WHERE a.k = ALL (SELECT k FROM b)",
                    schema_name=sn,
                )
            # <> ANY unsupported.
            with pytest.raises(Exception):
                client.execute_sql(
                    "CREATE VIEW bad AS SELECT a.v FROM a WHERE a.k <> ANY (SELECT k FROM b)",
                    schema_name=sn,
                )
            # uncorrelated ALL unsupported.
            with pytest.raises(Exception):
                client.execute_sql(
                    "CREATE VIEW bad AS SELECT a.v FROM a WHERE a.v < ALL (SELECT w FROM b)",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, "bad", "a", "b")


class TestMoreScalar:
    def _setup(self, client, sn):
        client.execute_sql(_CREATE_A, schema_name=sn)
        client.execute_sql(_CREATE_B, schema_name=sn)

    def test_projected_min_max_avg(self, client):
        """Projected MIN/MAX/AVG (bare column refs; NULL over an empty group)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id, "
                "(SELECT MIN(w) FROM b WHERE b.k = a.k) AS mn, "
                "(SELECT MAX(w) FROM b WHERE b.k = a.k) AS mx, "
                "(SELECT AVG(w) FROM b WHERE b.k = a.k) AS av FROM a",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 0), (2, 20, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 10, 4), (2, 10, 10)", schema_name=sn)
            # k=10: min 4, max 10, avg 7.0. k=20: all NULL.
            assert _col_weights(client, vid, "id", "mn", "mx", "av") == {
                (1, 4, 10, 7.0): 1,
                (2, None, None, None): 1,
            }
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_min_incremental_retraction(self, client):
        """A correlated MIN retracts the current extremum when it leaves the group
        (exercises the reduce MIN retraction under the scalar chain)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id, (SELECT MIN(w) FROM b WHERE b.k = a.k) AS mn FROM a",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 10, 3), (2, 10, 8)", schema_name=sn)
            assert _col_weights(client, vid, "id", "mn") == {(1, 3): 1}
            # Delete the current min (w=3) -> MIN becomes 8.
            client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
            assert _col_weights(client, vid, "id", "mn") == {(1, 8): 1}
            # Delete the last row -> MIN becomes NULL.
            client.execute_sql("DELETE FROM b WHERE id = 2", schema_name=sn)
            assert _col_weights(client, vid, "id", "mn") == {(1, None): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_correlated_scalar_under_or_and_case(self, client):
        """A correlated scalar substitutes to a column and works under OR and in
        CASE (any boolean/value position)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id, "
                "CASE WHEN (SELECT COUNT(*) FROM b WHERE b.k = a.k) > 0 THEN 1 ELSE 0 END AS hit "
                "FROM a WHERE a.v > 0 OR (SELECT SUM(w) FROM b WHERE b.k = a.k) > 100",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 5), (2, 20, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 10, 7)", schema_name=sn)
            # id1: a.v=5>0 -> in; hit: k=10 count>0 -> 1.
            # id2: a.v=0, SUM(k=20)=NULL>100 UNKNOWN -> out.
            assert _col_weights(client, vid, "id", "hit") == {(1, 1): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_correlated_and_uncorrelated_mix(self, client):
        """A view mixing a correlated projected COUNT and an uncorrelated WHERE
        comparison (LEFT join + INNER join in one H)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c "
                "FROM a WHERE a.v < (SELECT MAX(w) FROM b)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 5), (2, 20, 50)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 10, 8), (2, 10, 30)", schema_name=sn)
            # global MAX(w)=30. id1 v=5<30 in, count(k=10)=2. id2 v=50<30 no.
            assert _col_weights(client, vid, "id", "c") == {(1, 2): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_uncorrelated_backfill(self, client):
        """Uncorrelated comparison view created after data (backfill)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("INSERT INTO a VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 1, 25)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id FROM a WHERE a.v < (SELECT MAX(w) FROM b)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            assert _col_weights(client, vid, "id") == {(1,): 1, (2,): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_count_is_null_uncorrelated_folds(self, client):
        """An uncorrelated COUNT under IS NOT NULL folds to TRUE (never a segment)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id FROM a WHERE (SELECT COUNT(*) FROM b) IS NOT NULL",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 1), (2, 20, 2)", schema_name=sn)
            assert _col_weights(client, vid, "id") == {(1,): 1, (2,): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")


class TestScalarRejections:
    def _setup(self, client, sn):
        client.execute_sql(_CREATE_A, schema_name=sn)
        client.execute_sql(_CREATE_B, schema_name=sn)

    def test_rejections(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            bad = [
                # inner GROUP BY
                "SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k=a.k GROUP BY b.w) FROM a",
                # non-aggregate scalar
                "SELECT a.id, (SELECT b.w FROM b WHERE b.k=a.k) FROM a",
                # range correlation with aggregate
                "SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k < a.k) FROM a",
                # uncorrelated scalar in projection
                "SELECT a.id, (SELECT COUNT(*) FROM b) FROM a",
                # uncorrelated <> comparison
                "SELECT a.id FROM a WHERE a.v <> (SELECT COUNT(*) FROM b)",
                # join inner
                "SELECT a.id, (SELECT COUNT(*) FROM b JOIN a a2 ON b.k=a2.k WHERE b.k=a.k) FROM a",
            ]
            for sql in bad:
                with pytest.raises(Exception):
                    client.execute_sql(f"CREATE VIEW bad AS {sql}", schema_name=sn)
                # ensure a partial chain didn't linger
                try:
                    client.execute_sql("DROP VIEW bad", schema_name=sn)
                except Exception:
                    pass
        finally:
            _cleanup(client, sn, "bad", "a", "b")

    def test_join_from_outer_rejects_subquery(self, client):
        """A JOIN-FROM view keeps today's rejection of a WHERE subquery."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            with pytest.raises(Exception):
                client.execute_sql(
                    "CREATE VIEW bad AS SELECT a.id FROM a JOIN b ON a.k=b.k "
                    "WHERE a.v < (SELECT MAX(w) FROM b b2)",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, "bad", "a", "b")


class TestInnerLocal:
    def test_inner_local_filter(self, client):
        """An inner-local WHERE conjunct (references only the inner) filters Gᵢ."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_A, schema_name=sn)
            client.execute_sql(_CREATE_B, schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id, "
                "(SELECT COUNT(*) FROM b WHERE b.k = a.k AND b.w > 5) AS c FROM a",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 0)", schema_name=sn)
            # b for k=10: w in {3, 8, 20}. Only w>5 counted -> 2.
            client.execute_sql("INSERT INTO b VALUES (1, 10, 3), (2, 10, 8), (3, 10, 20)", schema_name=sn)
            assert _col_weights(client, vid, "id", "c") == {(1, 2): 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")
