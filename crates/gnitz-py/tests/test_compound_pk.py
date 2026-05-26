"""End-to-end tests for compound (multi-column) PRIMARY KEY tables.

Covers schema acceptance/rejection (planner gate), INSERT/SELECT/DELETE
round-trips, byte-path PK delivery, and named-projection through
`apply_projection`.

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


_UUID_1 = "00000000-0000-0000-0000-000000000001"
_UUID_2 = "00000000-0000-0000-0000-000000000002"


def test_compound_pk_uuid_plus_companion_accepted_round_trip(client):
    """`(UUID, U64)` = stride 24: a wide, mixed-width compound PK. Round-trips
    through the byte-path accessors. Two rows share the same UUID (the first 16
    bytes of the packed PK) and differ only in the trailing U64 — this drives
    the wide `compare_pk_bytes` tie-break past byte 16."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a UUID, b BIGINT UNSIGNED, payload BIGINT, "
            "PRIMARY KEY (a, b))",
            schema_name=sn,
        )
        client.execute_sql(
            f"INSERT INTO t (a, b, payload) VALUES "
            f"('{_UUID_1}', 1, 10), ('{_UUID_1}', 2, 20), ('{_UUID_2}', 1, 30)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [
            (_UUID_1, 1, 10), (_UUID_1, 2, 20), (_UUID_2, 1, 30),
        ]
        # Delete the row sharing UUID_1 but with the trailing-U64 tie-break — the
        # other UUID_1 row must survive (distinct only past byte 16).
        client.execute_sql(
            f"DELETE FROM t WHERE a = '{_UUID_1}' AND b = 2", schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [(_UUID_1, 1, 10), (_UUID_2, 1, 30)]
    finally:
        _cleanup(client, sn, "t")


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


def test_compound_pk_stride_12_accepted_round_trip(client):
    """(U64, U32) = stride 12: narrow (≤ 16) but non-power-of-two. Widens to a
    `u128` fast key via `widen_pk_le`'s zero-extend arm. Round-trips, including
    rows that share the leading U64 and differ only in the trailing U32."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED, b INT UNSIGNED, payload BIGINT, "
            "PRIMARY KEY (a, b))",
            schema_name=sn,
        )
        u32max = 4294967295  # u32::MAX — adversarial trailing value
        client.execute_sql(
            f"INSERT INTO t (a, b, payload) VALUES "
            f"(1, 0, 10), (1, {u32max}, 11), (1, 7, 17), (2, 0, 20)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 0, 10), (1, 7, 17), (1, u32max, 11), (2, 0, 20)]
        # Delete a row that shares col a but differs only in the trailing U32.
        client.execute_sql(
            f"DELETE FROM t WHERE a = 1 AND b = {u32max}", schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 0, 10), (1, 7, 17), (2, 0, 20)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_stride_24_accepted_round_trip(client):
    """Three U64s = stride 24: wide (> 16), routes through the byte-path
    accessors. Round-trips, including rows that share the first 16 bytes
    (cols a, b) and differ only in the trailing U64 (col c) — driving the wide
    `compare_pk_bytes` tie-break past byte 16."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, c BIGINT UNSIGNED, "
            "payload BIGINT, PRIMARY KEY (a, b, c))",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t (a, b, c, payload) VALUES "
            "(1, 1, 1, 111), (1, 1, 2, 112), (1, 2, 1, 121), (2, 1, 1, 211)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.c, row.payload) for row in rows_result["rows"])
        assert seen == [
            (1, 1, 1, 111), (1, 1, 2, 112), (1, 2, 1, 121), (2, 1, 1, 211),
        ]
        # Delete the row that shares (a, b) = (1, 1) with another but differs in
        # the trailing U64 — only the byte-16+ tie-break distinguishes them.
        client.execute_sql(
            "DELETE FROM t WHERE a = 1 AND b = 1 AND c = 2", schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.c, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 1, 1, 111), (1, 2, 1, 121), (2, 1, 1, 211)]
    finally:
        _cleanup(client, sn, "t")


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


def test_index_on_stride24_pk_source_via_sql(client):
    """Secondary index on a stride-24 (three-U64) compound-PK table. The index
    PK is `indexed_col + source_pk` = 8 + 24 = 32 bytes, well past the 16-byte
    narrow cap, so the index table itself uses the wide byte-path accessors.
    `ops/index.rs` sizes the composite key against MAX_PK_BYTES, so this is
    reachable once the gate is lifted; the lookup must round-trip correctly."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE src (a BIGINT UNSIGNED, b BIGINT UNSIGNED, c BIGINT UNSIGNED, "
            "payload BIGINT, PRIMARY KEY (a, b, c))",
            schema_name=sn,
        )
        client.execute_sql("CREATE INDEX idx ON src (payload)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO src (a, b, c, payload) VALUES "
            "(1, 1, 1, 100), (1, 1, 2, 200), (3, 4, 5, 100)",
            schema_name=sn,
        )
        # `WHERE payload = 200` resolves via the wide-source secondary index.
        results = client.execute_sql(
            "SELECT * FROM src WHERE payload = 200", schema_name=sn,
        )
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.c, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 1, 2, 200)]
        # A value shared by two distinct wide PKs must return both.
        results = client.execute_sql(
            "SELECT * FROM src WHERE payload = 100", schema_name=sn,
        )
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.c, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 1, 1, 100), (3, 4, 5, 100)]
    finally:
        _cleanup(client, sn, "src")


def test_index_on_uuid_pk_source_via_sql(client):
    """Secondary index on a single-column 16-byte (UUID) PK table. The source PK
    is the maximum narrow width (16), so the index PK is `indexed_col + source_pk`
    = 8 + 16 = 24 bytes — a wide index PK over a narrow source. Guards the wide
    index-key path for the maximal-narrow-source case."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE src (id UUID NOT NULL PRIMARY KEY, payload BIGINT)",
            schema_name=sn,
        )
        client.execute_sql("CREATE INDEX idx ON src (payload)", schema_name=sn)
        client.execute_sql(
            f"INSERT INTO src (id, payload) VALUES "
            f"('{_UUID_1}', 100), ('{_UUID_2}', 200)",
            schema_name=sn,
        )
        results = client.execute_sql(
            "SELECT * FROM src WHERE payload = 200", schema_name=sn,
        )
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.id, row.payload) for row in rows_result["rows"])
        assert seen == [(_UUID_2, 200)]
    finally:
        _cleanup(client, sn, "src")


def test_unique_index_on_compound_pk_column(client):
    """UNIQUE INDEX on a single column of a compound PK. Two rows share that
    column's value but differ in the rest of the PK (so they are distinct PKs,
    a legal compound-key insert) — yet they violate UNIQUE on the indexed
    column. Pre-fix the validation keyed on the whole packed (a, b) key, so the
    duplicate `a` was silently accepted."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE src (a INT UNSIGNED, b INT UNSIGNED, PRIMARY KEY (a, b))",
            schema_name=sn,
        )
        client.execute_sql("CREATE UNIQUE INDEX ON src (a)", schema_name=sn)
        # (1,1) and (1,2): distinct PKs, same a=1 → UNIQUE(a) violation.
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(
                "INSERT INTO src (a, b) VALUES (1, 1), (1, 2)", schema_name=sn,
            )
    finally:
        _cleanup(client, sn, "src")


def test_unique_index_on_compound_pk_column_distinct_ok(client):
    """Counterpart to the violation test: distinct values on the indexed PK
    column must ingest cleanly (no false positive from the byte-slice key)."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE src (a INT UNSIGNED, b INT UNSIGNED, PRIMARY KEY (a, b))",
            schema_name=sn,
        )
        client.execute_sql("CREATE UNIQUE INDEX ON src (a)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO src (a, b) VALUES (1, 1), (2, 1), (3, 9)", schema_name=sn,
        )
        tid, _ = client.resolve_table(sn, "src")
        rows = sorted((row.a, row.b) for row in client.scan(tid) if row.weight > 0)
        assert rows == [(1, 1), (2, 1), (3, 9)]
    finally:
        _cleanup(client, sn, "src")


def test_fk_references_compound_pk_rejected(client):
    """A FK targeting a non-UNIQUE column of a compound-PK parent is rejected:
    a compound PK has no lone PK column, so the column qualifies only via its
    own UNIQUE index. `a` (a PK member, no separate UNIQUE index) and `payload`
    (a plain column) both fail."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "parent")
        for col in ("a", "payload"):
            with pytest.raises(gnitz.GnitzError) as exc:
                client.execute_sql(
                    f"CREATE TABLE chi (cid BIGINT PRIMARY KEY, "
                    f"ref BIGINT UNSIGNED REFERENCES parent({col}))",
                    schema_name=sn,
                )
            assert "unique" in str(exc.value).lower()
    finally:
        _cleanup(client, sn, "parent", "chi")


def test_fk_references_compound_pk_unique_col_enforced(client):
    """A FK targeting a UNIQUE-indexed column of a compound-PK parent is
    accepted and enforced end to end: matching child INSERT succeeds,
    non-matching is rejected, and deleting the referenced parent is blocked."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        # Narrow (U32, U32) compound PK so the email index PK (key + source PK)
        # stays within the 16-byte index-PK limit.
        client.execute_sql(
            "CREATE TABLE parent (a INT UNSIGNED, b INT UNSIGNED, "
            "email BIGINT UNSIGNED NOT NULL, PRIMARY KEY (a, b))",
            schema_name=sn,
        )
        client.execute_sql("CREATE UNIQUE INDEX ON parent(email)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE chi (cid BIGINT PRIMARY KEY, "
            "ref BIGINT UNSIGNED REFERENCES parent(email))",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO parent (a, b, email) VALUES (1, 1, 1000)", schema_name=sn
        )
        # Matching referenced value → accepted.
        client.execute_sql("INSERT INTO chi (cid, ref) VALUES (1, 1000)", schema_name=sn)
        # Non-matching referenced value → rejected.
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("INSERT INTO chi (cid, ref) VALUES (2, 9999)", schema_name=sn)
        # Deleting the referenced parent row is blocked by RESTRICT.
        parent_tid, parent_schema = client.resolve_table(sn, "parent")
        pk_bytes = (1).to_bytes(4, "little") + (1).to_bytes(4, "little")
        with pytest.raises(gnitz.GnitzError):
            client.delete(parent_tid, parent_schema, [pk_bytes])
    finally:
        _cleanup(client, sn, "parent", "chi")


def test_fk_references_single_pk_column_enforced(client):
    """Regression: a FK targeting a single-PK parent's PK column is accepted
    and enforced (the long-standing form)."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, name BIGINT)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE c (cid BIGINT PRIMARY KEY, "
            "ref BIGINT UNSIGNED REFERENCES p(pid))",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO p (pid, name) VALUES (5, 50)", schema_name=sn)
        client.execute_sql("INSERT INTO c (cid, ref) VALUES (1, 5)", schema_name=sn)
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("INSERT INTO c (cid, ref) VALUES (2, 99)", schema_name=sn)
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("DELETE FROM p WHERE pid = 5", schema_name=sn)
    finally:
        _cleanup(client, sn, "p", "c")


def test_fk_references_single_pk_unique_col_enforced(client):
    """A FK targeting a non-PK UNIQUE column of a single-PK parent is accepted
    and enforced — the form newly enabled by the unified admission rule. It
    exercises the non-PK runtime branch on a single-PK parent."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, code BIGINT UNSIGNED NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE c (cid BIGINT PRIMARY KEY, "
            "ref BIGINT UNSIGNED REFERENCES p(code))",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO p (pid, code) VALUES (1, 1000)", schema_name=sn)
        client.execute_sql("INSERT INTO c (cid, ref) VALUES (1, 1000)", schema_name=sn)
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("INSERT INTO c (cid, ref) VALUES (2, 9999)", schema_name=sn)
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("DELETE FROM p WHERE pid = 1", schema_name=sn)
    finally:
        _cleanup(client, sn, "p", "c")


def test_fk_multi_column_against_compound_pk_rejected(client):
    """A multi-column FK is out of scope and rejected."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "parent")
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "CREATE TABLE chi (x BIGINT UNSIGNED, y BIGINT UNSIGNED, "
                "cid BIGINT PRIMARY KEY, "
                "FOREIGN KEY (x, y) REFERENCES parent (a, b))",
                schema_name=sn,
            )
        assert "multi-column" in str(exc.value).lower()
    finally:
        _cleanup(client, sn, "parent", "chi")


def test_fk_parent_delete_with_null_referenced_value(client):
    """A NULL referenced value can be referenced by no child, so RESTRICT must
    not block deleting a parent row whose UNIQUE column is NULL. Exercises the
    storage-resolved RESTRICT path on a nullable UNIQUE column."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, code BIGINT UNSIGNED)",
            schema_name=sn,
        )
        client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE c (cid BIGINT PRIMARY KEY, "
            "ref BIGINT UNSIGNED REFERENCES p(code))",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO p (pid, code) VALUES (1, NULL)", schema_name=sn)
        # No child references NULL → the delete must succeed.
        client.execute_sql("DELETE FROM p WHERE pid = 1", schema_name=sn)
        results = client.execute_sql("SELECT * FROM p", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        assert [row for row in rows_result["rows"]] == []
    finally:
        _cleanup(client, sn, "p", "c")


def test_fk_update_referenced_unique_value_restricted(client):
    """Updating a referenced non-PK UNIQUE value is blocked by RESTRICT while a
    child still references the old value; updating an unreferenced value
    succeeds."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, code BIGINT UNSIGNED NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE c (cid BIGINT PRIMARY KEY, "
            "ref BIGINT UNSIGNED REFERENCES p(code))",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO p (pid, code) VALUES (1, 1000), (2, 2000)", schema_name=sn
        )
        client.execute_sql("INSERT INTO c (cid, ref) VALUES (1, 1000)", schema_name=sn)
        # Referenced value (code=1000) still referenced → UPDATE rejected.
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("UPDATE p SET code = 1500 WHERE pid = 1", schema_name=sn)
        # Unreferenced value (code=2000) → UPDATE succeeds.
        client.execute_sql("UPDATE p SET code = 2500 WHERE pid = 2", schema_name=sn)
        results = client.execute_sql("SELECT * FROM p", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.pid, row.code) for row in rows_result["rows"])
        assert seen == [(1, 1000), (2, 2500)]
    finally:
        _cleanup(client, sn, "p", "c")


def test_fk_referenced_unique_index_drop_protected(client):
    """A UNIQUE index targeted by a FK is load-bearing and cannot be dropped;
    once the FK is gone the drop succeeds."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, code BIGINT UNSIGNED NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE c (cid BIGINT PRIMARY KEY, "
            "ref BIGINT UNSIGNED REFERENCES p(code))",
            schema_name=sn,
        )
        idx_name = f"{sn}__p__idx_code"
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(f"DROP INDEX {idx_name}", schema_name=sn)
        assert "integrity" in str(exc.value).lower()
        # Remove the FK, then the index can be dropped.
        client.execute_sql("DROP TABLE c", schema_name=sn)
        client.execute_sql(f"DROP INDEX {idx_name}", schema_name=sn)
    finally:
        _cleanup(client, sn, "p", "c")


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
        # `apply_projection` rebuilds the schema as single-PK, so we go
        # through the wildcard branch that returns the source batch unchanged.
        # This exercises compound-PK byte decoding: the second PK
        # column lives in bytes 8..16 of each row's PK region.
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 1, 10), (1, 2, 20), (2, 1, 30)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_adversarial_second_column_and_delete_consolidation(client):
    """Compound-PK ingest consolidation with second-column values that would
    invert the order if the packed key were compared as one wide integer
    (col b in the high half). The consolidation sort's order-preserving key
    must order column-by-column (a dominates b), and DELETE retractions must
    fold cleanly. Scan order isn't globally sorted under multiple workers, so
    we assert the surviving (a, b, payload) *set* — the grouping/folding the
    consolidation sort is responsible for."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        umax = 18446744073709551615  # u64::MAX — a huge col-b value
        # Inserted scrambled. Under a naive packed-u128 compare treating b as
        # the high half, (1, umax) would sort after (2, 0); column a must win.
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES "
            f"(2, 1, 21), (1, {umax}, 1399), (1, 5, 15), (1, 0, 10), (2, 0, 20)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [
            (1, 0, 10), (1, 5, 15), (1, umax, 1399), (2, 0, 20), (2, 1, 21),
        ]
        # Retract the group-a=1 max-b row; the next consolidation must fold it.
        client.execute_sql(
            f"DELETE FROM t WHERE a = 1 AND b = {umax}", schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 0, 10), (1, 5, 15), (2, 0, 20), (2, 1, 21)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_signed_first_column_adversarial_consolidation(client):
    """Compound PK whose FIRST column is signed (BIGINT) with negative values,
    second column unsigned (BIGINT UNSIGNED). Combines the two ordering
    dimensions the order-preserving key must get right at once: the signed
    bias-flip (negatives sort before positives, not after as raw LE bytes
    would place them) AND the column-by-column walk (column a dominates b).

    No other e2e exercises a signed column inside a compound PK — the signed
    suites use single-column PKs, the compound suites use unsigned columns.
    Distinct (a, b) tuples must round-trip, and a DELETE retraction on a
    negative-a row must fold cleanly across consolidation. Scan order isn't
    globally sorted under multiple workers, so we assert the surviving
    (a, b, payload) *set* — the grouping/folding the consolidation sort owns.
    """
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT UNSIGNED NOT NULL, "
            "payload BIGINT, PRIMARY KEY (a, b))",
            schema_name=sn,
        )
        umax = 18446744073709551615  # u64::MAX — adversarial unsigned col-b value
        # Scrambled, with negatives. Under a raw-LE-byte compare, -3 (0xFFFF…FD)
        # would sort after +2, and (a=-3, b=umax) would sort before (a=-3, b=0);
        # the order-preserving key must place a=-3 first (signed) and order
        # within a by b (column walk). Includes a shared col-a across distinct b.
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES "
            f"(2, 0, 20), (-3, {umax}, 399), (-3, 0, 30), (-1, 5, 15), (2, 1, 21)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [
            (-3, 0, 30), (-3, umax, 399), (-1, 5, 15), (2, 0, 20), (2, 1, 21),
        ]
        # Retract a negative-a row; the next consolidation must fold it against
        # its insert (PK equality over the signed-compound key).
        client.execute_sql(
            f"DELETE FROM t WHERE a = -3 AND b = {umax}", schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [(-3, 0, 30), (-1, 5, 15), (2, 0, 20), (2, 1, 21)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_stride24_adversarial_consolidation(client):
    """Stride-24 (three-U64) ingest consolidation with adversarial high-half
    values in the second and third columns. The wide byte-path order-preserving
    key must order column-by-column (a dominates b dominates c); a naive compare
    treating later columns as more significant would invert the order. DELETE
    retractions over the wide key must fold cleanly. Scan order isn't globally
    sorted under multiple workers, so we assert the surviving (a, b, c, payload)
    *set* — the grouping/folding the consolidation sort owns."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, c BIGINT UNSIGNED, "
            "payload BIGINT, PRIMARY KEY (a, b, c))",
            schema_name=sn,
        )
        umax = 18446744073709551615  # u64::MAX in the high columns
        client.execute_sql(
            "INSERT INTO t (a, b, c, payload) VALUES "
            f"(2, 0, 0, 200), (1, {umax}, 0, 1900), (1, 0, {umax}, 109), "
            f"(1, 0, 0, 100), (1, {umax}, {umax}, 1999)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.c, row.payload) for row in rows_result["rows"])
        assert seen == [
            (1, 0, 0, 100), (1, 0, umax, 109), (1, umax, 0, 1900),
            (1, umax, umax, 1999), (2, 0, 0, 200),
        ]
        # Retract the all-max-tail row; the next consolidation must fold it.
        client.execute_sql(
            f"DELETE FROM t WHERE a = 1 AND b = {umax} AND c = {umax}", schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.c, row.payload) for row in rows_result["rows"])
        assert seen == [
            (1, 0, 0, 100), (1, 0, umax, 109), (1, umax, 0, 1900), (2, 0, 0, 200),
        ]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_stride24_signed_first_column_adversarial_consolidation(client):
    """Stride-24 compound PK whose FIRST column is signed (BIGINT) with negative
    values, the remaining two unsigned. Combines, at full wide width, the signed
    bias-flip (negatives sort before positives) with the column-by-column walk
    (a dominates b dominates c). Distinct tuples must round-trip and a DELETE on
    a negative-a row must fold across consolidation. Asserts the surviving set."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT UNSIGNED NOT NULL, "
            "c BIGINT UNSIGNED NOT NULL, payload BIGINT, PRIMARY KEY (a, b, c))",
            schema_name=sn,
        )
        umax = 18446744073709551615
        client.execute_sql(
            "INSERT INTO t (a, b, c, payload) VALUES "
            f"(2, 0, 0, 200), (-3, {umax}, 0, 1399), (-3, 0, {umax}, 309), "
            f"(-1, 5, 5, 155), (2, 1, 1, 211)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.c, row.payload) for row in rows_result["rows"])
        assert seen == [
            (-3, 0, umax, 309), (-3, umax, 0, 1399), (-1, 5, 5, 155),
            (2, 0, 0, 200), (2, 1, 1, 211),
        ]
        client.execute_sql(
            f"DELETE FROM t WHERE a = -3 AND b = {umax} AND c = 0", schema_name=sn,
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.c, row.payload) for row in rows_result["rows"])
        assert seen == [
            (-3, 0, umax, 309), (-1, 5, 5, 155), (2, 0, 0, 200), (2, 1, 1, 211),
        ]
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
        # SELECT * sidesteps apply_projection's single-PK assumption.
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == sorted(expected)
    finally:
        _cleanup(client, sn, "t")


@_NEEDS_MULTI
def test_compound_pk_stride24_multi_worker_partition_routing(client):
    """Multi-worker correctness for a wide (stride-24) PK: 20 rows distributed
    across workers via the byte-path partition routing must all survive a scan
    in the right (a, b, c, payload) shape. Exercises wide-PK hashing/routing,
    which the narrow stride-16 routing test does not reach."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, c BIGINT UNSIGNED, "
            "payload BIGINT, PRIMARY KEY (a, b, c))",
            schema_name=sn,
        )
        expected = [(i, (i * 7) % 11, (i * 13) % 17, i * 100) for i in range(20)]
        values = ", ".join(f"({a}, {b}, {c}, {p})" for (a, b, c, p) in expected)
        client.execute_sql(
            f"INSERT INTO t (a, b, c, payload) VALUES {values}", schema_name=sn
        )
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted(
            (row.a, row.b, row.c, row.payload) for row in rows_result["rows"]
        )
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


def test_compound_pk_stride24_delete_by_bytes(client):
    """`client.delete(...)` on a wide (stride-24, three-U64) PK table accepts
    a 24-byte packed PK. Exercises the binding's `PkColumn::Bytes` push path at
    a width past the narrow cap."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, c BIGINT UNSIGNED, "
            "payload BIGINT, PRIMARY KEY (a, b, c))",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t (a, b, c, payload) VALUES "
            "(1, 1, 1, 10), (1, 1, 2, 20), (2, 1, 1, 30)",
            schema_name=sn,
        )
        tid, schema = client.resolve_table(sn, "t")
        # Delete (1, 1, 2): 24 bytes packed (8 || 8 || 8, LE).
        pk_bytes = (
            (1).to_bytes(8, "little")
            + (1).to_bytes(8, "little")
            + (2).to_bytes(8, "little")
        )
        client.delete(tid, schema, [pk_bytes])
        results = client.execute_sql("SELECT * FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.c, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 1, 1, 10), (2, 1, 1, 30)]
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Named projection through apply_projection
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


# ---------------------------------------------------------------------------
# FK / UNIQUE enforcement regressions (wide-PK + signed-PK + PK-as-FK + DDL)
# ---------------------------------------------------------------------------


def test_fk_on_pk_column_enforced(client):
    """Child table whose FK column is also its own PK column: INSERT with a
    non-existent parent value must be rejected without crashing the server.
    An FK column that is also a PK column has no payload slot, so the
    parent-existence check must read its value from the PK region, not via the
    payload index."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE c (cid BIGINT UNSIGNED PRIMARY KEY REFERENCES p(pid))",
            schema_name=sn)
        client.execute_sql("INSERT INTO p (pid) VALUES (1)", schema_name=sn)
        client.execute_sql("INSERT INTO c (cid) VALUES (1)", schema_name=sn)
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("INSERT INTO c (cid) VALUES (99)", schema_name=sn)
    finally:
        _cleanup(client, sn, "p", "c")


def test_fk_restrict_with_signed_pk_source(client):
    """FK RESTRICT must catch references from child rows with negative PK values.
    The signed-suffix seek bug made negative-PK rows invisible to the parent-delete
    restriction check, silently creating dangling references."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE parent (pid BIGINT UNSIGNED PRIMARY KEY)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE child ("
            "  cid BIGINT PRIMARY KEY, "
            "  fk_val BIGINT UNSIGNED NOT NULL, "
            "  FOREIGN KEY (fk_val) REFERENCES parent(pid)"
            ")", schema_name=sn)
        client.execute_sql("INSERT INTO parent (pid) VALUES (1)", schema_name=sn)
        client.execute_sql("INSERT INTO child (cid, fk_val) VALUES (-5, 1)", schema_name=sn)
        client.execute_sql("INSERT INTO child (cid, fk_val) VALUES (-1, 1)", schema_name=sn)
        # Parent delete must be rejected: child rows with negative PKs reference it.
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("DELETE FROM parent WHERE pid = 1", schema_name=sn)
    finally:
        _cleanup(client, sn, "parent", "child")


def test_unique_index_enforced_for_negative_pk_source(client):
    """Unique index seek must find rows with negative source-table PK values.
    The signed-suffix bug caused them to be skipped, allowing duplicate inserts."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, val BIGINT UNSIGNED)",
            schema_name=sn)
        # Explicit CREATE UNIQUE INDEX — the inline UNIQUE column constraint is
        # not translated into an index by the planner.
        client.execute_sql("CREATE UNIQUE INDEX ON t (val)", schema_name=sn)
        client.execute_sql("INSERT INTO t (id, val) VALUES (-5, 42)", schema_name=sn)
        client.execute_sql("INSERT INTO t (id, val) VALUES (-1, 99)", schema_name=sn)
        # Duplicate val=42 must be rejected (the existing row has a negative PK).
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("INSERT INTO t (id, val) VALUES (1, 42)", schema_name=sn)
    finally:
        _cleanup(client, sn, "t")


def test_unique_index_on_string_rejected(client):
    """Creating a UNIQUE index on a STRING column must be rejected cleanly: the
    u128 index key cannot represent a variable-length string without collision,
    so the distributed uniqueness check would silently bypass or falsely reject."""
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, name VARCHAR(255))",
            schema_name=sn,
        )
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("CREATE UNIQUE INDEX ON t (name)", schema_name=sn)
    finally:
        _cleanup(client, sn, "t")
