"""Signed-128 (`I128`) join-key type: cross-sign `U64 = I64` equijoins.

A cross-sign integer equijoin whose unsigned side is `U64` (`BIGINT UNSIGNED`)
co-partitions through the signed-128 common type `I128`. The join view's
`_join_pk` is then an `I128` column — a hidden key slot (physical, so it still
keys/routes/decodes the view, but excluded from `SELECT *` and not
name-resolvable). `include_hidden=True` surfaces it as a signed Python int.

Two surfaces are tested:

1. A **synthetic** batch round-trip (no server). A real join's matched
   `_join_pk` always lands in the operands' non-negative overlap
   `[0, 2^63-1]`, so signed and unsigned surfacing agree on it. The only way
   to drive an `I128` value with bit 127 set — and prove the signed surfacing
   paths — is to build a batch directly via the native write path. This covers
   the `.pks` accessor (`pk_column_to_pylist`) and the payload column read
   (`rust_batch_columns_to_py`) across the full signed range, including
   negatives.

2. The **real** `U64 = I64` join end to end: co-partition (same worker),
   equality (byte-identical `_join_pk`), no cross-sign false matches, and the
   matched key decoding back to its true non-negative value through the client
   (the wide-PK OPK sign round-trip).
"""
import random

import pytest
import gnitz
from gnitz import ColumnDef, Schema, TypeCode, ZSetBatch


def _uid():
    return str(random.randint(100_000, 999_999))


# ---------------------------------------------------------------------------
# Synthetic surfacing — drives I128 values with bit 127 set (no server)
# ---------------------------------------------------------------------------

I128_MIN = -(2 ** 127)
I128_MAX = (2 ** 127) - 1
I128_VALUES = [I128_MIN, -1, 0, 1, I128_MAX]


def _i128_schema():
    # PK: I128; payload: I128 (the `SELECT _join_pk AS dup` shape).
    return Schema(
        [
            ColumnDef("pk", TypeCode.I128, primary_key=True),
            ColumnDef("dup", TypeCode.I128),
        ],
        pk_index=0,
    )


def test_i128_pk_and_payload_signed_surfacing():
    """An I128 PK and an I128 payload must surface as SIGNED Python ints across
    the full range. A pre-fix run shows 2^128-1 for -1 (unsigned reading)."""
    schema = _i128_schema()
    batch = ZSetBatch(schema)
    for v in I128_VALUES:
        batch.append(pk=v, dup=v)

    # `.pks` → pk_column_to_pylist: the lone I128 PK surfaces signed.
    assert list(batch.pks) == I128_VALUES, (
        f"I128 PK must surface as signed ints; got {list(batch.pks)}"
    )

    # `.columns[1]` → rust_batch_columns_to_py U128s arm: the I128 payload
    # surfaces signed (columns[0] is the PK placeholder).
    dup_col = list(batch.columns[1])
    assert dup_col == I128_VALUES, (
        f"I128 payload must surface as signed ints; got {dup_col}"
    )


# ---------------------------------------------------------------------------
# Real cross-sign U64 = I64 join, end to end
# ---------------------------------------------------------------------------

def _cleanup(client, sn, tables=None, views=None):
    for name in (views or []):
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
    for name in (tables or []):
        try:
            client.execute_sql(f"DROP TABLE {name}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


# A U64 customer_id strictly greater than 2^63 (no signed customer can match it)
# and a negative customers.id (no unsigned order can reference it). Matched keys
# lie in the overlap [0, 2^63-1].
U64_NO_MATCH = (2 ** 63) + 10          # 9223372036854775818
MAX_SIGNED = (2 ** 63) - 1             # 9223372036854775807


def test_cross_sign_u64_i64_join(client):
    """`orders.customer_id BIGINT UNSIGNED` ⋈ `customers.id BIGINT`. The pair
    co-partitions through I128; only logically-equal keys match, and the matched
    `_join_pk` surfaces to the client as its true non-negative value."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, "
            "customer_id BIGINT UNSIGNED NOT NULL, amount BIGINT NOT NULL)",
            schema_name=sn,
        )
        # The join key pair is U64 (orders.customer_id) = I64 (customers.id),
        # whose common type is the signed-128 I128. The view prepends `_join_pk`.
        client.execute_sql(
            "CREATE VIEW v AS SELECT customers.name AS name, orders.amount AS amount "
            "FROM orders JOIN customers ON orders.customer_id = customers.id",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]

        client.execute_sql(
            f"INSERT INTO customers VALUES "
            f"(5, 'Five'), (100, 'Hundred'), (-7, 'NegSeven'), ({MAX_SIGNED}, 'MaxSigned')",
            schema_name=sn,
        )
        client.execute_sql(
            f"INSERT INTO orders VALUES "
            f"(1, 5, 1000), (2, 100, 2000), (4, 5, 4000), (3, {U64_NO_MATCH}, 3000)",
            schema_name=sn,
        )

        # `_join_pk` is a hidden key slot, so `include_hidden=True` surfaces it at
        # its physical position (column 0) for this wide-PK decode tripwire.
        rows = [r for r in client.scan(vid, include_hidden=True) if r.weight > 0]

        # Equality + co-partition: exactly the logically-equal pairs match.
        # customer -7 (signed) matches no unsigned order; order customer_id
        # 2^63+10 (unsigned) matches no signed customer; MaxSigned has no order.
        got = sorted((r["name"], r["amount"]) for r in rows)
        assert got == [
            ("Five", 1000),
            ("Five", 4000),
            ("Hundred", 2000),
        ], f"cross-sign join multiset mismatch: {got}"

        # The `_join_pk` (column 0, surfaced by include_hidden) is an I128 decoded
        # via the wide-PK client decode (decode_wal_block OPK round-trip). The
        # matched keys are non-negative, so the §4.6.1 sign-flip is exactly what
        # makes them decode back to 5 / 100 rather than a large negative number.
        join_pks = sorted(r[0] for r in rows)
        assert join_pks == [5, 5, 100], (
            f"matched _join_pk must surface as its true non-negative value; got {join_pks}"
        )
        # And per-row association is correct: amount 2000 ⇒ key 100.
        by_amount = {r["amount"]: r[0] for r in rows}
        assert by_amount == {1000: 5, 4000: 5, 2000: 100}, by_amount
    finally:
        _cleanup(client, sn, tables=["orders", "customers"], views=["v"])


def test_cross_sign_u64_i64_project_key_column(client):
    """The synthetic `_join_pk` is a hidden key slot, so it is not name-resolvable
    downstream. Keying downstream work on the join key means projecting the
    underlying source key column: a `SELECT customers.id AS cust_id` view exposes
    the join key as a real column that a downstream `SELECT`/`GROUP BY` can name."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, "
            "customer_id BIGINT UNSIGNED NOT NULL, amount BIGINT NOT NULL)",
            schema_name=sn,
        )
        # Project the join key's source column (`customers.id`) as `cust_id`. For a
        # matched pair `customer_id == id`, so `cust_id` carries the same value the
        # hidden `_join_pk` would.
        client.execute_sql(
            "CREATE VIEW v AS SELECT customers.id AS cust_id, orders.amount AS amount "
            "FROM orders JOIN customers ON orders.customer_id = customers.id",
            schema_name=sn,
        )
        # `amount` is kept so the two key-5 rows stay distinct tuples rather than
        # consolidating to a single weight-2 row under Z-set projection.
        client.execute_sql(
            "CREATE VIEW vpk AS SELECT cust_id AS k, amount AS a FROM v",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW vgrp AS SELECT cust_id AS k, COUNT(*) AS n FROM v GROUP BY cust_id",
            schema_name=sn,
        )
        vpk_id = client.resolve_table(sn, "vpk")[0]
        vgrp_id = client.resolve_table(sn, "vgrp")[0]

        client.execute_sql(
            "INSERT INTO customers VALUES (5, 'Five'), (100, 'Hundred')",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO orders VALUES (1, 5, 1000), (2, 100, 2000), (4, 5, 4000)",
            schema_name=sn,
        )

        pk_rows = sorted(r["k"] for r in client.scan(vpk_id) if r.weight > 0)
        assert pk_rows == [5, 5, 100], f"projected key mismatch: {pk_rows}"

        grp = {r["k"]: r["n"] for r in client.scan(vgrp_id) if r.weight > 0}
        assert grp == {5: 2, 100: 1}, f"GROUP BY projected key mismatch: {grp}"
    finally:
        _cleanup(client, sn, tables=["orders", "customers"], views=["v", "vpk", "vgrp"])
