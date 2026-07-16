"""E2E tests for ad-hoc SELECT ORDER BY / OFFSET / LIMIT (the client-side sink).

The sink sorts BEFORE projection (so a non-projected ORDER BY key resolves) and
counts LIMIT/OFFSET by logical **multiplicity** (summed weight), not Z-set
entries. Run with GNITZ_WORKERS=4 — the view-scan paths (UNION ALL, GROUP BY)
are distributed, and the determinism guarantee is that the returned logical bag
is a function of the data, independent of worker count.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_order_by.py -v --tb=short
"""
import random

import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _rows(client, sn, q):
    """Execute a direct SELECT and return its result rows in result (sorted) order."""
    res = client.execute_sql(q, schema_name=sn)[0]
    assert res["type"] == "Rows", f"expected Rows, got {res['type']}"
    return list(res["rows"])


def _cleanup(client, sn, *names):
    for name in names:
        for kind in ("VIEW", "TABLE"):
            try:
                client.execute_sql(f"DROP {kind} {name}", schema_name=sn)
            except Exception:
                pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Base-table ORDER BY / OFFSET / LIMIT
# ---------------------------------------------------------------------------


class TestBaseTableOrderBy:
    def _setup(self, client, sn, rows):
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)",
            schema_name=sn,
        )
        vals = ",".join(f"({pk}, {v})" if v is not None else f"({pk}, NULL)" for pk, v in rows)
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

    def test_order_by_asc_desc(self, client):
        sn = "ob" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn, [(1, 30), (2, 10), (3, 20)])
            asc = _rows(client, sn, "SELECT * FROM t ORDER BY v")
            assert [r.id for r in asc] == [2, 3, 1]
            desc = _rows(client, sn, "SELECT * FROM t ORDER BY v DESC")
            assert [r.id for r in desc] == [1, 3, 2]
        finally:
            _cleanup(client, sn, "t")

    def test_order_by_nulls_default_and_explicit(self, client):
        sn = "ob" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn, [(1, 10), (2, None), (3, 20)])
            # ASC default → NULLS LAST.
            asc = _rows(client, sn, "SELECT * FROM t ORDER BY v")
            assert [r.id for r in asc] == [1, 3, 2]
            # DESC default → NULLS FIRST (placement absolute, not value-flipped).
            desc = _rows(client, sn, "SELECT * FROM t ORDER BY v DESC")
            assert [r.id for r in desc] == [2, 3, 1]
            # Explicit NULLS FIRST on ASC overrides the default.
            nf = _rows(client, sn, "SELECT * FROM t ORDER BY v ASC NULLS FIRST")
            assert [r.id for r in nf] == [2, 1, 3]
        finally:
            _cleanup(client, sn, "t")

    def test_order_by_multikey(self, client):
        sn = "ob" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, 30), (2, 1, 10), (3, 2, 5)",
                schema_name=sn,
            )
            rows = _rows(client, sn, "SELECT * FROM t ORDER BY a, b")
            assert [r.id for r in rows] == [2, 1, 3]
            rows = _rows(client, sn, "SELECT * FROM t ORDER BY a ASC, b DESC")
            assert [r.id for r in rows] == [1, 2, 3]
        finally:
            _cleanup(client, sn, "t")

    def test_offset_and_limit_window(self, client):
        sn = "ob" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn, [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)])
            # OFFSET 1 LIMIT 2 over an ASC id sort.
            rows = _rows(client, sn, "SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 1")
            assert [r.id for r in rows] == [2, 3]
            # OFFSET past the end → empty.
            rows = _rows(client, sn, "SELECT * FROM t ORDER BY id OFFSET 99")
            assert rows == []
            # LIMIT 0 → empty.
            rows = _rows(client, sn, "SELECT * FROM t ORDER BY id LIMIT 0")
            assert rows == []
            # MySQL `LIMIT off, lim` form.
            rows = _rows(client, sn, "SELECT * FROM t ORDER BY id LIMIT 1, 2")
            assert [r.id for r in rows] == [2, 3]
        finally:
            _cleanup(client, sn, "t")

    def test_limit_by_still_rejected(self, client):
        sn = "ob" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn, [(1, 10)])
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("SELECT * FROM t LIMIT 1 BY v", schema_name=sn)
        finally:
            _cleanup(client, sn, "t")

    def test_limit_offset_expression_rejected(self, client):
        """A non-literal LIMIT/OFFSET is a clean error, never a silent degrade:
        `LIMIT 1+1` must not return every row, `OFFSET 1+1` must not skip 0."""
        sn = "ob" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn, [(1, 10), (2, 20), (3, 30)])
            with pytest.raises(gnitz.GnitzError, match="LIMIT"):
                client.execute_sql("SELECT * FROM t LIMIT 1+1", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="OFFSET"):
                client.execute_sql("SELECT * FROM t LIMIT 1 OFFSET 1+1", schema_name=sn)
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Thin-path top-N: the `pk IN (…)` fetch-cap must be disabled under ORDER BY
# ---------------------------------------------------------------------------


class TestThinPathTopN:
    def test_pk_in_order_by_limit_returns_true_smallest(self, client):
        """`WHERE pk IN (…) ORDER BY v LIMIT n` returns the true n smallest by v,
        not the first n keys in IN-list order — proving `row_cap` is disabled
        under ORDER BY (else it fetches pk=1,2 and sorts only those)."""
        sn = "tp" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            # Smallest v sit at the LARGEST pks.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 50), (2, 40), (3, 30), (4, 20), (5, 10)",
                schema_name=sn,
            )
            rows = _rows(client, sn, "SELECT * FROM t WHERE pk IN (1, 2, 3, 4, 5) ORDER BY v LIMIT 2")
            assert [(r.pk, r.v) for r in rows] == [(5, 10), (4, 20)]
            # With OFFSET too.
            rows = _rows(client, sn, "SELECT * FROM t WHERE pk IN (1, 2, 3, 4, 5) ORDER BY v LIMIT 2 OFFSET 1")
            assert [(r.pk, r.v) for r in rows] == [(4, 20), (3, 30)]
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Sort BEFORE projection: an ORDER BY key absent from the projected columns
# ---------------------------------------------------------------------------


class TestSortBeforeProjection:
    def _setup(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)", schema_name=sn)

    def test_order_by_non_projected_column(self, client):
        sn = "sp" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            # `v` is not in the projection but still orders the result.
            rows = _rows(client, sn, "SELECT id FROM t ORDER BY v")
            assert [r.id for r in rows] == [2, 3, 1]
        finally:
            _cleanup(client, sn, "t")

    def test_order_by_alias_and_source_name(self, client):
        sn = "sp" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            # Order by the output alias.
            rows = _rows(client, sn, "SELECT id, v AS foo FROM t ORDER BY foo")
            assert [r.id for r in rows] == [2, 3, 1]
            # Order by the source column name even when it was aliased in the output.
            rows = _rows(client, sn, "SELECT id, v AS foo FROM t ORDER BY v DESC")
            assert [r.id for r in rows] == [1, 3, 2]
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Views: multiplicity-correct LIMIT and determinism of the summed-weight bag
# ---------------------------------------------------------------------------


def _weight_bag(rows, key):
    """{key(row) -> summed weight} over positive-weight rows — aggregates entries
    so the bag is invariant to how a logical multiplicity is split across
    workers/entries."""
    out = {}
    for r in rows:
        if r.weight > 0:
            out[key(r)] = out.get(key(r), 0) + r.weight
    return out


class TestViewMultiplicity:
    def _union_all_view(self, client, sn):
        for name in ("a", "b"):
            client.execute_sql(
                f"CREATE TABLE {name} (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM a UNION ALL SELECT * FROM b",
            schema_name=sn,
        )
        # Both sides identical → each (pk, val) row has logical multiplicity 2.
        client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (1, 10), (2, 20)", schema_name=sn)

    def test_union_all_limit_counts_logical_rows(self, client):
        """LIMIT counts logical rows (summed weight), not Z-set entries. With
        val=10 at multiplicity 2, `ORDER BY val LIMIT 2` returns exactly those 2
        logical rows — an entry-counting LIMIT would spill into val=20."""
        sn = "vm" + _uid()
        client.create_schema(sn)
        try:
            self._union_all_view(client, sn)

            rows = _rows(client, sn, "SELECT * FROM v ORDER BY val LIMIT 2")
            assert sum(r.weight for r in rows) == 2, "LIMIT 2 = 2 logical rows"
            assert all(r.val == 10 for r in rows), "only the smallest value survives"

            # LIMIT 3 straddles the boundary: 2 logical val=10 + 1 logical val=20.
            rows = _rows(client, sn, "SELECT * FROM v ORDER BY val LIMIT 3")
            bag = _weight_bag(rows, lambda r: r.val)
            assert bag == {10: 2, 20: 1}, f"boundary entry clipped to weight 1: {bag}"
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_union_all_offset_and_limit_inside_multiplicity(self, client):
        """OFFSET and LIMIT landing inside the weight-2 val=10 group."""
        sn = "vm" + _uid()
        client.create_schema(sn)
        try:
            self._union_all_view(client, sn)
            # window [1, 2) over sorted logical rows [10,10,20,20] → one val=10.
            rows = _rows(client, sn, "SELECT * FROM v ORDER BY val LIMIT 1 OFFSET 1")
            assert sum(r.weight for r in rows) == 1
            assert all(r.val == 10 for r in rows)
        finally:
            _cleanup(client, sn, "v", "a", "b")


class TestViewOrdering:
    def test_groupby_view_order_by_hidden_and_positional(self, client):
        """A GROUP BY view carries a hidden `_group_pk`; positional ORDER BY must
        target the first VISIBLE column, and ORDER BY over the aggregate works."""
        sn = "vo" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT cat, COUNT(*) AS cnt FROM orders GROUP BY cat",
                schema_name=sn,
            )
            # cat 10 ×3, cat 20 ×1, cat 30 ×2.
            client.execute_sql(
                "INSERT INTO orders VALUES (1,10),(2,10),(3,10),(4,20),(5,30),(6,30)",
                schema_name=sn,
            )
            # ORDER BY the aggregate, descending, top 2.
            rows = _rows(client, sn, "SELECT * FROM v ORDER BY cnt DESC LIMIT 2")
            assert [(r.cat, r.cnt) for r in rows] == [(10, 3), (30, 2)]
            # Positional: column 1 is the visible `cat` (not the hidden _group_pk).
            rows = _rows(client, sn, "SELECT * FROM v ORDER BY 1")
            assert [r.cat for r in rows] == [10, 20, 30]
        finally:
            _cleanup(client, sn, "v", "orders")

    def test_join_view_order_by_limit_deterministic_bag(self, client):
        """ORDER BY + LIMIT over a fan-out join view (non-unique join key) returns
        a summed-weight bag that is a function of the data — the same at any
        worker count (run the suite at W=1 and W=4)."""
        sn = "vo" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE fact (fid BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, amt BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE dim (did BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, label BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT fact.amt AS amt, dim.label AS label "
                "FROM fact JOIN dim ON fact.k = dim.k",
                schema_name=sn,
            )
            # Two fact rows share join key k=1 with one dim row (label 100); one
            # fact with k=2 joins label 200.
            client.execute_sql(
                "INSERT INTO fact VALUES (1,1,10),(2,1,10),(3,2,30)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO dim VALUES (1,1,100),(2,2,200)", schema_name=sn)

            # Full ordered bag (no cut): amt 10 appears twice (both fact k=1 rows).
            rows = _rows(client, sn, "SELECT * FROM v ORDER BY amt")
            full_bag = _weight_bag(rows, lambda r: (r.amt, r.label))
            assert full_bag == {(10, 100): 2, (30, 200): 1}, f"full bag: {full_bag}"

            # LIMIT 2 over ORDER BY amt keeps only the two amt=10 logical rows.
            rows = _rows(client, sn, "SELECT * FROM v ORDER BY amt LIMIT 2")
            cut_bag = _weight_bag(rows, lambda r: (r.amt, r.label))
            assert cut_bag == {(10, 100): 2}, f"cut bag: {cut_bag}"
        finally:
            _cleanup(client, sn, "v", "fact", "dim")
