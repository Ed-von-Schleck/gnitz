"""A secondary index as an access structure: creating one, dropping one, and the
composite and PK-column forms.

An index changes which walk the planner picks, never the answer, so the DDL
here is paired with an `EXPLAIN` of the access path it opens. What an index
does to a *read* is in `read_verb/test_index_reads.py`; what UNIQUE refuses is
in `admissibility/`.
"""

import pytest
import gnitz
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





def _pks(results):
    """Sorted PKs of a Rows result."""
    assert results[0]["type"] == "Rows", results[0]
    return sorted(row.pk for row in results[0]["rows"])


def _access(client, sn, q):
    """EXPLAIN's `access:` line for `q` — which walk the plan chose. Found by
    prefix rather than by row position, so adding a plan line cannot silently
    make this read a different fact."""
    res = client.execute_sql("EXPLAIN " + q, schema_name=sn)
    assert res[0]["type"] == "Rows", res[0]
    got = [r[0] for r in res[0]["rows"] if r[0].startswith("access: ")]
    assert len(got) == 1, res[0]["rows"]
    return got[0]



def _result_rows(result):
    """Sorted list of full-row value tuples (schema order, PK first) from a
    SELECT Rows result (positive weight only)."""
    assert result[0]["type"] == "Rows"
    return sorted(tuple(row) for row in result[0]["rows"])



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
            from gnitz._native import IDX_TAB, unpack_pk_cols
            batch_obj = client.scan(IDX_TAB)
            assert batch_obj.schema is not None
            tid, _ = client.resolve_table(sn, "t")
            owners = batch_obj.scalars("owner_id")
            srcs = batch_obj.scalars("source_col_idx")
            found = any(w > 0 and o == tid and unpack_pk_cols(sc) == [1]
                        for w, o, sc in zip(batch_obj.weights, owners, srcs))
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
            # The unique bit (bit 0 of IdxTab's packed `flags`) should be set.
            from gnitz._native import IDX_TAB
            batch_obj = client.scan(IDX_TAB)
            assert batch_obj.schema is not None
            tid, _ = client.resolve_table(sn, "t")
            flags = [f for w, o, f in zip(batch_obj.weights,
                                          batch_obj.scalars("owner_id"),
                                          batch_obj.scalars("flags"))
                     if w > 0 and o == tid]
            assert flags and flags[0] & 1
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
            from gnitz._native import IDX_TAB
            batch_obj = client.scan(IDX_TAB)
            if batch_obj is not None:
                tid, _ = client.resolve_table(sn, "t")
                live = [o for w, o in zip(batch_obj.weights, batch_obj.scalars("owner_id")) if w > 0]
                assert tid not in live, "IdxTab row should be gone"
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

            # Both indexes pin (a, b) to the same point, so the tie falls to
            # arity: the narrower (a, b) walk wins — the fact the name claims —
            # and it returns the right row.
            q = "SELECT * FROM t WHERE a = 1 AND b = 200"
            assert _access(client, sn, q) == (
                "access: index range on (a, b) — may be traded for a full scan "
                "on low selectivity")
            assert _pks(client.execute_sql(q, schema_name=sn)) == [20]
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

class TestPkColumnIndex:
    """A secondary index may name a PK column, and the planner treats it as any
    other index column. Both shapes below have no PK-range plan — the WHERE
    leaves the LEADING PK column free — and the second has no index-free plan
    either, so the index walk is the only plan it has."""

    _CREATE = (
        "CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
        "v BIGINT NOT NULL, PRIMARY KEY (a, b))"
    )

    def test_index_over_a_trailing_pk_column_serves_the_where(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE, schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300)",
                schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(b, v)", schema_name=sn)

            # An equality on the index's leading column (a PK column) pins it,
            # and `v` completes the key.
            q = "SELECT * FROM t WHERE b = 10 AND v = 200"
            assert _access(client, sn, q).startswith("access: index range on (b, v)")
            assert _result_rows(client.execute_sql(q, schema_name=sn)) == [(2, 10, 200)]

            # A range on the same column bounds the same index.
            assert _result_rows(client.execute_sql(
                "SELECT * FROM t WHERE b > 10", schema_name=sn)) == [(3, 20, 300)]
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_wide_literal_on_an_indexed_pk_column_is_servable(self, client):
        """The literal overflows i64, so the predicate VM has no form for the
        conjunct: only an index walk that consumes it byte-exactly can serve
        this WHERE, and the PK rung cannot bound it (nothing pins `a`)."""
        u64_max = 18446744073709551615
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(self._CREATE, schema_name=sn)
            client.execute_sql(
                f"INSERT INTO t VALUES (1, {u64_max}, 100), (2, 5, 200)",
                schema_name=sn)
            client.execute_sql("CREATE INDEX ON t(b)", schema_name=sn)

            q = f"SELECT * FROM t WHERE b = {u64_max}"
            assert _access(client, sn, q) == (
                "access: index range on (b) — exact walk, never traded")
            assert _result_rows(client.execute_sql(q, schema_name=sn)) == [
                (1, u64_max, 100)]
        finally:
            _drop_all(client, sn, tables=["t"])
