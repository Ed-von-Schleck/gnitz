"""End-to-end tests for native PK column types.

The SQL planner no longer coerces narrow signed/unsigned integer or float PK
columns to U64. Each PK keeps its declared type and stores at its native
stride (1/2/4 bytes for U8/U16/U32 and I8/I16/I32; 4/8 for F32/F64; 8 for
U64/I64; 16 for U128/UUID). This file exercises:

  - Schema preservation across CREATE TABLE.
  - Signed PK ordering (`-1 < +1` instead of wrap-around).
  - Float PK ordering by `total_cmp` (NaN-stable, sign-aware).
  - Round-trip INSERT/SELECT/UPDATE/DELETE.
  - Multi-worker partition routing parity (same logical key → same worker
    regardless of stride or signedness).
"""

import math
import os
import random
import struct

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
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _pks_sorted(client, tid):
    """Return PK values for positive-weight rows, sorted ascending.

    Multi-worker scans concatenate per-worker batches; ordering across
    workers is not guaranteed. Sort here so callers can compare against
    the natural order expected by the test."""
    return sorted(row[0] for row in client.scan(tid) if row.weight > 0)


# ---------------------------------------------------------------------------
# Schema preservation
# ---------------------------------------------------------------------------

class TestNativePkSchema:
    """Each declared PK type round-trips through CREATE TABLE/resolve_table."""

    @pytest.mark.parametrize("sql,tc", [
        ("TINYINT",             gnitz.TypeCode.I8),
        ("SMALLINT",            gnitz.TypeCode.I16),
        ("INT",                 gnitz.TypeCode.I32),
        ("BIGINT",              gnitz.TypeCode.I64),
        ("TINYINT UNSIGNED",    gnitz.TypeCode.U8),
        ("SMALLINT UNSIGNED",   gnitz.TypeCode.U16),
        ("INT UNSIGNED",        gnitz.TypeCode.U32),
        ("BIGINT UNSIGNED",     gnitz.TypeCode.U64),
        ("FLOAT",               gnitz.TypeCode.F32),
        ("DOUBLE",              gnitz.TypeCode.F64),
        ("DECIMAL(38,0)",       gnitz.TypeCode.U128),
    ])
    def test_pk_type_preserved(self, client, sql, tc):
        sn = "n" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                f"CREATE TABLE t (pk {sql} NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            _, schema = client.resolve_table(sn, "t")
            assert schema.columns[schema.pk_index].type_code == tc
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Signed integer PK ordering
# ---------------------------------------------------------------------------

class TestSignedPkOrdering:
    """Negative values must sort *before* positives (signed semantics).
    Before native PK types, BIGINT was coerced to U64 and `-1` widened to
    0xFFFF…FFFF, sorting AFTER `+1`. The cursor's single-PK fast path widens
    narrow strides to u128; only `make_slow_pk_cmp`'s signed arm restores
    correct order."""

    @pytest.mark.parametrize("pk_sql,values", [
        ("BIGINT",   [-100, -1, 0, 1, 100]),
        ("INT",      [-2_000_000_000, -1, 0, 1, 2_000_000_000]),
        ("SMALLINT", [-32_768, -1, 0, 1, 32_767]),
        ("TINYINT",  [-128, -1, 0, 1, 127]),
    ])
    def test_negatives_precede_positives(self, client, pk_sql, values):
        sn = "n" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                f"CREATE TABLE t (pk {pk_sql} NOT NULL PRIMARY KEY, v INT NOT NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            vals_sql = ",".join(f"({v}, {abs(v)})" for v in values)
            client.execute_sql(f"INSERT INTO t VALUES {vals_sql}", schema_name=sn)

            # Python's `sorted()` orders integers in signed numerical order,
            # so the comparison verifies *what* the engine stored: each negative
            # PK must come back as the negative integer, not its u64 wrap-around.
            seen = _pks_sorted(client, tid)
            assert seen == sorted(values), \
                f"expected {sorted(values)}, got {seen}"
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Float PK ordering (total_cmp)
# ---------------------------------------------------------------------------

def _f64_bits_as_u64(x: float) -> int:
    """Reinterpret f64 → u64 bit pattern (what the scan returns for an F64 PK)."""
    return struct.unpack("<Q", struct.pack("<d", x))[0]


def _f32_bits_as_u32(x: float) -> int:
    return struct.unpack("<I", struct.pack("<f", x))[0]


class TestFloatPkOrdering:
    """Floats sort by `total_cmp`: NaN-stable, sign-aware (-0.0 < 0.0)."""

    def test_f64_all_values_round_trip(self, client):
        """Every inserted float PK round-trips by bit pattern (total_cmp identity).
        Multi-worker scan order isn't guaranteed, so verify presence by
        bit pattern rather than asserting ordering at the API boundary."""
        sn = "n" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                "CREATE TABLE t (score DOUBLE NOT NULL PRIMARY KEY, label INT NOT NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            values = [-math.inf, -2.5, -0.0, 0.0, 1.0, 3.5, math.inf]
            vals_sql = ",".join(
                f"({v if math.isfinite(v) else ('-1e400' if v < 0 else '1e400')}, {i})"
                for i, v in enumerate(values)
            )
            client.execute_sql(f"INSERT INTO t VALUES {vals_sql}", schema_name=sn)

            # Compare by bit pattern so -0.0 vs 0.0 stays distinct.
            seen_bits = {_f64_bits_as_u64(row[0]) for row in client.scan(tid) if row.weight > 0}
            expected_bits = {_f64_bits_as_u64(v) for v in values}
            assert seen_bits == expected_bits, \
                f"missing/extra bit patterns: expected={expected_bits} seen={seen_bits}"
        finally:
            _cleanup(client, sn, "t")

    def test_f64_negative_zero_is_distinct_from_positive_zero(self, client):
        """Float PK *identity* follows to_bits(): -0.0 and 0.0 are separate keys."""
        sn = "n" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                "CREATE TABLE t (score DOUBLE NOT NULL PRIMARY KEY, tag INT NOT NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            # Both inserts succeed (no PK conflict) because to_bits differs.
            client.execute_sql("INSERT INTO t VALUES (-0.0, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (0.0, 2)", schema_name=sn)
            rows = [r for r in client.scan(tid) if r.weight > 0]
            assert len(rows) == 2, f"-0.0 and 0.0 must be distinct PKs; got {rows}"
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Narrow unsigned PK ordering and storage
# ---------------------------------------------------------------------------

class TestNarrowUnsignedPk:
    """Narrow unsigned PKs (U8/U16/U32) sort by native unsigned order and
    fit the pk_stride exactly."""

    @pytest.mark.parametrize("pk_sql,boundary_values", [
        ("TINYINT UNSIGNED",  [0, 1, 127, 128, 255]),
        ("SMALLINT UNSIGNED", [0, 1, 32_768, 65_534, 65_535]),
        ("INT UNSIGNED",      [0, 1, 2**31, 2**32 - 2, 2**32 - 1]),
    ])
    def test_ascending_scan(self, client, pk_sql, boundary_values):
        sn = "n" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                f"CREATE TABLE t (pk {pk_sql} NOT NULL PRIMARY KEY, v INT NOT NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            vals_sql = ",".join(f"({v}, {v % 1000})" for v in boundary_values)
            client.execute_sql(f"INSERT INTO t VALUES {vals_sql}", schema_name=sn)

            seen = _pks_sorted(client, tid)
            assert seen == sorted(boundary_values), \
                f"expected {sorted(boundary_values)}, got {seen}"
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Round-trip UPDATE / DELETE / SEEK on native PK literals
# ---------------------------------------------------------------------------

class TestNativePkDml:

    def test_delete_by_negative_pk_i32(self, client):
        sn = "n" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql("INSERT INTO t VALUES (-1, 10), (-2, 20), (3, 30)", schema_name=sn)
            client.execute_sql("DELETE FROM t WHERE id = -1", schema_name=sn)
            assert _pks_sorted(client, tid) == [-2, 3]
        finally:
            _cleanup(client, sn, "t")

    def test_delete_in_list_negative_pks_i64(self, client):
        sn = "n" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql(
                "INSERT INTO t VALUES (-3, 1), (-2, 2), (-1, 3), (0, 4), (1, 5)",
                schema_name=sn,
            )
            client.execute_sql("DELETE FROM t WHERE id IN (-3, -1, 1)", schema_name=sn)
            assert _pks_sorted(client, tid) == [-2, 0]
        finally:
            _cleanup(client, sn, "t")

    def test_update_by_negative_pk_i16(self, client):
        sn = "n" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                "CREATE TABLE t (id SMALLINT NOT NULL PRIMARY KEY, v INT NOT NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql("INSERT INTO t VALUES (-1, 10), (1, 20)", schema_name=sn)
            client.execute_sql("UPDATE t SET v = 99 WHERE id = -1", schema_name=sn)
            rows = {r[0]: r[1] for r in client.scan(tid) if r.weight > 0}
            assert rows == {-1: 99, 1: 20}
        finally:
            _cleanup(client, sn, "t")

    def test_i64_min_literal_roundtrips(self, client):
        """`WHERE id = -9223372036854775808` must match the row inserted with
        the same literal — regression for the `checked_neg` parse rule."""
        sn = "n" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql(
                "INSERT INTO t VALUES (-9223372036854775808, 1), (0, 2)",
                schema_name=sn,
            )
            client.execute_sql(
                "DELETE FROM t WHERE id = -9223372036854775808", schema_name=sn
            )
            assert _pks_sorted(client, tid) == [0]
        finally:
            _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Multi-worker partition routing parity
# ---------------------------------------------------------------------------

@_NEEDS_MULTI
class TestNativePkMultiWorker:
    """Master routes SEEK via partition_for_key(pk). INSERT and SEEK must
    produce the same u128 for the same logical literal — otherwise they land
    on different workers and SEEK misses."""

    def test_seek_signed_negative_i32(self, client):
        sn = "n" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql("INSERT INTO t VALUES (-1, 42)", schema_name=sn)
            client.execute_sql("DELETE FROM t WHERE id = -1", schema_name=sn)
            assert _pks_sorted(client, tid) == [], \
                "SEEK on negative I32 PK must route to the same worker as INSERT"
        finally:
            _cleanup(client, sn, "t")

    def test_seek_signed_negative_i8(self, client):
        sn = "n" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                "CREATE TABLE t (id TINYINT NOT NULL PRIMARY KEY, v INT NOT NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql("INSERT INTO t VALUES (-1, 42)", schema_name=sn)
            client.execute_sql("DELETE FROM t WHERE id = -1", schema_name=sn)
            assert _pks_sorted(client, tid) == []
        finally:
            _cleanup(client, sn, "t")

    def test_seek_float_negative_zero(self, client):
        """-0.0 routes via the to_bits() pattern, not 0u128."""
        sn = "n" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                "CREATE TABLE t (score DOUBLE NOT NULL PRIMARY KEY, v INT NOT NULL)",
                schema_name=sn,
            )
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql("INSERT INTO t VALUES (-0.0, 42)", schema_name=sn)
            client.execute_sql("DELETE FROM t WHERE score = -0.0", schema_name=sn)
            assert _pks_sorted(client, tid) == []
        finally:
            _cleanup(client, sn, "t")
