"""End-to-end tests for compound (multi-column) PRIMARY KEY tables.

Covers schema acceptance/rejection (planner gate from
`plans/compound-pk-planner-gate.md`), INSERT/SELECT/DELETE round-trips,
byte-path PK delivery, and named-projection through `apply_projection`.

The Python `Schema` shim still exposes a single `pk_index` (it calls
`pk_index_single()` under the hood), so the introspection API is not
exercised here — the tests interact through SQL DDL/DML only.
"""

import os
import random
import pytest
import gnitz


_NEEDS_MULTI = pytest.mark.skipif(
    int(os.environ.get("GNITZ_WORKERS", "1")) < 2,
    reason="requires GNITZ_WORKERS>=2",
)


def _uid() -> str:
    return str(random.randint(100_000, 999_999))


def _cleanup(client, sn, *tables):
    for t in tables:
        try:
            client.drop_table(sn, t)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Schema acceptance: legal compound forms must not raise.
# ---------------------------------------------------------------------------


def test_compound_pk_two_u64_accepted(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, payload BIGINT, "
            "PRIMARY KEY (a, b))",
            schema_name=sn,
        )
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_four_u32_accepted(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a INT UNSIGNED, b INT UNSIGNED, c INT UNSIGNED, d INT UNSIGNED, "
            "payload BIGINT, PRIMARY KEY (a, b, c, d))",
            schema_name=sn,
        )
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_uuid_plus_companion_rejected_by_stride(client):
    """`(UUID, U64)` = stride 24, rejected. Sanity check that mixed-width
    compound PKs trip the stride membership rule."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "CREATE TABLE t (a UUID, b BIGINT UNSIGNED, PRIMARY KEY (a, b))",
                schema_name=sn,
            )
        assert "stride" in str(exc.value).lower()
    finally:
        _cleanup(client, sn)


# ---------------------------------------------------------------------------
# Schema rejection
# ---------------------------------------------------------------------------


def test_compound_pk_five_columns_rejected(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "CREATE TABLE t (a TINYINT UNSIGNED, b TINYINT UNSIGNED, c TINYINT UNSIGNED, "
                "d TINYINT UNSIGNED, e TINYINT UNSIGNED, PRIMARY KEY (a, b, c, d, e))",
                schema_name=sn,
            )
        assert "at most 4" in str(exc.value)
    finally:
        _cleanup(client, sn)


def test_compound_pk_stride_12_rejected(client):
    """(U64, U32) = stride 12, not in {1,2,4,8,16}."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "CREATE TABLE t (a BIGINT UNSIGNED, b INT UNSIGNED, PRIMARY KEY (a, b))",
                schema_name=sn,
            )
        assert "stride" in str(exc.value).lower()
    finally:
        _cleanup(client, sn)


def test_compound_pk_stride_24_rejected(client):
    """Three U64s = stride 24."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, c BIGINT UNSIGNED, "
                "PRIMARY KEY (a, b, c))",
                schema_name=sn,
            )
        assert "stride" in str(exc.value).lower()
    finally:
        _cleanup(client, sn)


def test_compound_pk_string_column_rejected(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "CREATE TABLE t (a TEXT, b INT UNSIGNED, PRIMARY KEY (a, b))",
                schema_name=sn,
            )
        msg = str(exc.value)
        # Error names the offending column so the user can act on it.
        assert "String" in msg or "string" in msg.lower()
    finally:
        _cleanup(client, sn)


def test_compound_pk_duplicate_column_rejected(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, PRIMARY KEY (a, a))",
                schema_name=sn,
            )
        msg = str(exc.value).lower()
        assert "duplicate" in msg
    finally:
        _cleanup(client, sn)


def test_compound_pk_mixed_inline_and_table_level_rejected(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "CREATE TABLE t (a BIGINT UNSIGNED PRIMARY KEY, b BIGINT UNSIGNED, "
                "PRIMARY KEY (a, b))",
                schema_name=sn,
            )
        assert "Multiple PRIMARY KEY" in str(exc.value)
    finally:
        _cleanup(client, sn)


# ---------------------------------------------------------------------------
# View / Index / FK against compound-PK source rejected
# ---------------------------------------------------------------------------


def _make_compound_table(client, sn, name="src"):
    client.execute_sql(
        f"CREATE TABLE {name} (a BIGINT UNSIGNED, b BIGINT UNSIGNED, payload BIGINT, "
        f"PRIMARY KEY (a, b))",
        schema_name=sn,
    )


def test_view_over_compound_pk_rejected(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn)
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql("CREATE VIEW v AS SELECT * FROM src", schema_name=sn)
        assert "compound" in str(exc.value).lower()
    finally:
        _cleanup(client, sn, "src")


def test_view_distinct_over_compound_pk_rejected(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn)
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "CREATE VIEW v AS SELECT DISTINCT a, b FROM src", schema_name=sn,
            )
        assert "compound" in str(exc.value).lower()
    finally:
        _cleanup(client, sn, "src")


def test_view_group_by_over_compound_pk_rejected(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn)
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "CREATE VIEW v AS SELECT a, COUNT(*) AS n FROM src GROUP BY a",
                schema_name=sn,
            )
        assert "compound" in str(exc.value).lower()
    finally:
        _cleanup(client, sn, "src")


def test_index_on_compound_pk_narrow_source_via_sql(client):
    """Compound source PK that fits within 8 bytes ⇒ index PK ≤ 16, which the
    cursor handles natively. (U32, U32) = stride 8 → index PK = 16."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE src (a INT UNSIGNED, b INT UNSIGNED, payload BIGINT, "
            "PRIMARY KEY (a, b))",
            schema_name=sn,
        )
        client.execute_sql("CREATE INDEX idx ON src (payload)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO src (a, b, payload) VALUES (1, 1, 100), (1, 2, 200), (3, 4, 100)",
            schema_name=sn,
        )
        # `WHERE payload = 200` resolves via the secondary index. The
        # SQL planner currently builds a wildcard projection over
        # compound-PK tables, so we ask for `*`.
        results = client.execute_sql(
            "SELECT * FROM src WHERE payload = 200", schema_name=sn,
        )
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 2, 200)]
    finally:
        _cleanup(client, sn, "src")


def test_fk_references_compound_pk_rejected(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "parent")
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "CREATE TABLE chi (cid BIGINT PRIMARY KEY, "
                "ref_a BIGINT UNSIGNED REFERENCES parent(a))",
                schema_name=sn,
            )
        assert "compound" in str(exc.value).lower()
    finally:
        _cleanup(client, sn, "parent", "chi")


def test_on_conflict_target_column_rejected_on_compound_pk(client):
    """`ON CONFLICT (a) DO NOTHING` against PK(a, b) hits the
    `validate_conflict_target` guard before any `pk_index_single()`
    access — a clean SQL error, not a server-side panic."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "conf_t")
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "INSERT INTO conf_t (a, b, payload) VALUES (1, 2, 3) "
                "ON CONFLICT (a) DO NOTHING",
                schema_name=sn,
            )
        assert "compound" in str(exc.value).lower()
    finally:
        _cleanup(client, sn, "conf_t")


# ---------------------------------------------------------------------------
# End-to-end DML round-trips
# ---------------------------------------------------------------------------


def test_compound_pk_insert_distinct_rows_round_trip(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)",
            schema_name=sn,
        )
        # Read back via SELECT *. Named projection through
        # `apply_projection` rebuilds the schema as single-PK (see
        # `plans/apply-projection-compound-pk.md`), so we go through
        # the wildcard branch that returns the source batch unchanged.
        # This exercises compound-PK byte decoding: the second PK
        # column lives in bytes 8..16 of each row's PK region.
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 1, 10), (1, 2, 20), (2, 1, 30)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_insert_duplicate_tuple_rejected(client):
    """Same (a, b) tuple twice → conflict; sharing one column is fine."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        client.execute_sql("INSERT INTO t (a, b, payload) VALUES (1, 1, 10)", schema_name=sn)
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(
                "INSERT INTO t (a, b, payload) VALUES (1, 1, 99)", schema_name=sn,
            )
        # Different tuple → accepted.
        client.execute_sql("INSERT INTO t (a, b, payload) VALUES (1, 2, 20)", schema_name=sn)
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_on_conflict_no_target_do_nothing(client):
    """No-target arms (`ON CONFLICT DO NOTHING` / `DO UPDATE`) don't
    touch `pk_index_single()` — should accept compound-PK tables."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES (1, 1, 10) ON CONFLICT DO NOTHING",
            schema_name=sn,
        )
        # Same tuple again → DO NOTHING; no error.
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES (1, 1, 999) ON CONFLICT DO NOTHING",
            schema_name=sn,
        )
    finally:
        _cleanup(client, sn, "t")


@_NEEDS_MULTI
def test_compound_pk_multi_worker_partition_routing(client):
    """Multi-worker correctness: 20 rows distributed across workers
    via compound-PK partition routing must all survive a scan, in
    the right (a, b, payload) shape."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        expected = [(i, (i * 7) % 11, i * 100) for i in range(20)]
        values = ", ".join(f"({a}, {b}, {p})" for (a, b, p) in expected)
        client.execute_sql(f"INSERT INTO t (a, b, payload) VALUES {values}", schema_name=sn)
        # SELECT * sidesteps apply_projection's single-PK assumption
        # (see plans/apply-projection-compound-pk.md).
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == sorted(expected)
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Binding-layer (Python) introspection + DML by raw PK bytes
# ---------------------------------------------------------------------------


def test_compound_pk_resolve_preserves_order(client):
    """`resolve_table` must return a Schema whose `pk_indices` reflects the
    PRIMARY KEY column order from the DDL — not just set membership.

    Here the payload column comes first in the table definition, and the
    PRIMARY KEY clause lists `(b, a)` — i.e. sort by b first, then a.
    The Schema returned to Python must round-trip that as
    `pk_indices == [2, 1]`.
    """
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (payload BIGINT, a BIGINT UNSIGNED, b BIGINT UNSIGNED, "
            "PRIMARY KEY (b, a))",
            schema_name=sn,
        )
        _, schema = client.resolve_table(sn, "t")
        assert schema.pk_indices == [2, 1]
        # Compound schema: pk_index getter must raise.
        with pytest.raises(ValueError):
            _ = schema.pk_index
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_scan_returns_bytes_pks(client):
    """Scanning a compound-PK table must return PK column as `list[bytes]`,
    one packed PK per row (LE-encoded in column-list order)."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES (1, 2, 10), (3, 4, 20)",
            schema_name=sn,
        )
        tid, _ = client.resolve_table(sn, "t")
        sr = client.scan(tid)
        pks = sr.batch.pks
        assert len(pks) == 2
        # Each packed PK is 16 bytes: 8 (a, LE) || 8 (b, LE).
        for p in pks:
            assert isinstance(p, bytes)
            assert len(p) == 16
        decoded = sorted(
            (int.from_bytes(p[:8], "little"), int.from_bytes(p[8:], "little"))
            for p in pks
        )
        assert decoded == [(1, 2), (3, 4)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_scalars_on_pk_columns(client):
    """`scan_result.scalars(pk_col_idx)` must work on PK columns of a
    compound-PK table (`PkColumn::Bytes` variant)."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES (1, 5, 100), (2, 6, 200)",
            schema_name=sn,
        )
        tid, _ = client.resolve_table(sn, "t")
        sr = client.scan(tid)
        a_vals = sorted(sr.scalars("a"))
        b_vals = sorted(sr.scalars("b"))
        assert a_vals == [1, 2]
        assert b_vals == [5, 6]
    finally:
        _cleanup(client, sn, "t")


def test_delete_single_pk_u64_accepts_int(client):
    """`client.delete(...)` on a single-PK U64 table accepts plain int PKs.
    The binding must adopt the schema's PK stride so the resulting tuple
    matches the `PkColumn::U64s` variant in `push_tuple`."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT UNSIGNED PRIMARY KEY, v BIGINT)",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t (pk, v) VALUES (1, 10), (2, 20), (3, 30)",
            schema_name=sn,
        )
        tid, schema = client.resolve_table(sn, "t")
        client.delete(tid, schema, [1, 3])
        sr = client.scan(tid)
        seen = sorted((row.pk, row.v) for row in sr)
        assert seen == [(2, 20)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_delete_by_bytes(client):
    """`client.delete(...)` on a compound-PK table accepts packed PK bytes."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)",
            schema_name=sn,
        )
        tid, schema = client.resolve_table(sn, "t")
        # Delete (1, 2): 16 bytes packed.
        pk_bytes = (1).to_bytes(8, "little") + (2).to_bytes(8, "little")
        client.delete(tid, schema, [pk_bytes])
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 1, 10), (2, 1, 30)]
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Named projection through apply_projection (see plans/apply-projection-compound-pk.md)
# ---------------------------------------------------------------------------


def test_compound_pk_full_projection(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES (1, 2, 100), (3, 4, 200)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT a, b, payload FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 2, 100), (3, 4, 200)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_subset_projection(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES (1, 2, 100), (3, 4, 200)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT a, payload FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 100), (3, 200)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_reorder_projection(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES (1, 2, 100), (3, 4, 200)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT b, a, payload FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.b, row.a, row.payload) for row in rows_result["rows"])
        assert seen == [(2, 1, 100), (4, 3, 200)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_payload_reorder_with_nulls(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, "
            "p1 BIGINT, p2 BIGINT, PRIMARY KEY (a, b))",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t (a, b, p1, p2) VALUES (1, 2, NULL, 100), (3, 4, 200, NULL)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT a, b, p2, p1 FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.p2, row.p1) for row in rows_result["rows"])
        assert seen == [(1, 2, 100, None), (3, 4, None, 200)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_pk_and_payload_reorder_with_nulls(client):
    """Both PK columns AND payload columns reordered: exercises the
    `!pk_preserved && !payload_preserved` combination — both rebuild
    loops fire in the same call."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, "
            "p1 BIGINT, p2 BIGINT, PRIMARY KEY (a, b))",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t (a, b, p1, p2) VALUES (1, 2, NULL, 100), (3, 4, 200, NULL)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT b, a, p2, p1 FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.b, row.a, row.p2, row.p1) for row in rows_result["rows"])
        assert seen == [(2, 1, 100, None), (4, 3, None, 200)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_no_pk_projection_rejected(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql("SELECT payload FROM t", schema_name=sn)
        assert "must project at least one PRIMARY KEY column" in str(exc.value)
    finally:
        _cleanup(client, sn, "t")
