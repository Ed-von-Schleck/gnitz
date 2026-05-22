"""E2E tests: DBSP operators (DISTINCT, JOIN) over SIGNED-integer PKs with
NEGATIVE key values.

A signed PK widens into the u128 cursor key by zero-extension, so a negative
key (e.g. -1 → 0xFFFF…FFFF) sorts AFTER the positives in raw u128 order while
storage sorts it first. Operator trace point-seeks used to compare the raw
u128 and therefore mislocated negative keys, silently dropping retractions or
join matches. The existing operator suites (test_joins, test_aggregates,
test_narrow_types_and_null_logic) only exercise positive / unsigned keys, so
this file fills the negative-signed-key gap end to end.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_signed_pk_operators.py -v --tb=short
"""
import random

import pytest
import gnitz


def _uid():
    return str(random.randint(100_000, 999_999))


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


def _scan_map(client, tid):
    """{pk: row} for positive-weight rows."""
    return {r[0]: r for r in client.scan(tid) if r.weight > 0}


def _scan_dicts(client, tid):
    return [r._asdict() for r in client.scan(tid) if r.weight > 0]


# ---------------------------------------------------------------------------
# DISTINCT view over a signed PK
# ---------------------------------------------------------------------------

class TestDistinctSignedPk:
    """`SELECT * FROM t` routes deltas through op_distinct, whose trace is keyed
    by t's signed PK. A DELETE/UPDATE on a negative PK sends a retraction whose
    seek must land on the matching negative key."""

    @pytest.mark.parametrize("pk_sql", ["BIGINT", "INT", "SMALLINT", "TINYINT"])
    def test_delete_retraction_negative_pk(self, client, pk_sql):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                f"CREATE TABLE t (pk {pk_sql} NOT NULL PRIMARY KEY, v INT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE VIEW vw AS SELECT * FROM t", schema_name=sn)
            vid, _ = client.resolve_table(sn, "vw")

            # Negative keys present; storage order is -3 < -1 < 2.
            client.execute_sql(
                "INSERT INTO t VALUES (-3, 30), (-1, 10), (2, 20)", schema_name=sn
            )
            assert set(_scan_map(client, vid).keys()) == {-3, -1, 2}

            # Retract the negative key -1. The distinct trace seek must find it.
            client.execute_sql("DELETE FROM t WHERE pk = -1", schema_name=sn)
            rows = _scan_map(client, vid)
            assert set(rows.keys()) == {-3, 2}, (
                f"after DELETE pk=-1 expected {{-3, 2}}, got {set(rows.keys())}"
            )
            assert rows[-3].v == 30
            assert rows[2].v == 20
        finally:
            _cleanup(client, sn, tables=["t"], views=["vw"])

    def test_update_retraction_negative_pk(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE VIEW vw AS SELECT * FROM t", schema_name=sn)
            vid, _ = client.resolve_table(sn, "vw")

            client.execute_sql(
                "INSERT INTO t VALUES (-5, 1), (-1, 2), (3, 3)", schema_name=sn
            )
            # UPDATE = retract-old + insert-new; the retraction seeks pk=-1.
            client.execute_sql("UPDATE t SET v = 99 WHERE pk = -1", schema_name=sn)
            rows = _scan_map(client, vid)
            assert set(rows.keys()) == {-5, -1, 3}
            assert rows[-1].v == 99, f"expected v=99 at pk=-1, got {rows[-1].v}"
            assert rows[-5].v == 1
            assert rows[3].v == 3
        finally:
            _cleanup(client, sn, tables=["t"], views=["vw"])


# ---------------------------------------------------------------------------
# INNER JOIN whose build (trace) side is keyed by a negative signed PK
# ---------------------------------------------------------------------------

class TestJoinSignedKey:
    """`orders JOIN customers ON orders.cid = customers.id` accumulates the
    customers side into a trace keyed by customers.id (a signed PK). When orders
    arrive as a delta, the join seeks that trace by the negative key."""

    def test_inner_join_negative_key(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amount BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid, customers.name AS name, orders.amount AS amount "
                "FROM orders JOIN customers ON orders.cid = customers.id",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Build the customers trace first (negative ids), then stream orders
            # as a delta so the join takes the delta-trace seek path.
            client.execute_sql(
                "INSERT INTO customers VALUES (-10, 'Alice'), (-20, 'Bob'), (30, 'Carol')",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO orders VALUES (1, -10, 100), (2, -20, 200), (3, -10, 300), (4, 30, 400)",
                schema_name=sn,
            )
            rows = _scan_dicts(client, vid)
            got = sorted((r["oid"], r["name"], r["amount"]) for r in rows)
            assert got == [
                (1, "Alice", 100),
                (2, "Bob", 200),
                (3, "Alice", 300),
                (4, "Carol", 400),
            ], f"join over negative keys mismatched: {got}"
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    def test_inner_join_negative_key_incremental_retract(self, client):
        """Delete an order referencing a negative key; the retraction must seek
        the customers trace at that negative key and remove exactly one join row."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amount BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid, customers.name AS name "
                "FROM orders JOIN customers ON orders.cid = customers.id",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO customers VALUES (-1, 'Neg'), (1, 'Pos')", schema_name=sn
            )
            client.execute_sql(
                "INSERT INTO orders VALUES (10, -1, 1), (11, -1, 2), (12, 1, 3)",
                schema_name=sn,
            )
            assert len(_scan_dicts(client, vid)) == 3

            # Retract one order on the negative key.
            client.execute_sql("DELETE FROM orders WHERE id = 11", schema_name=sn)
            rows = _scan_dicts(client, vid)
            got = sorted((r["oid"], r["name"]) for r in rows)
            assert got == [(10, "Neg"), (12, "Pos")], (
                f"after deleting order 11 expected oids {{10,12}}, got {got}"
            )
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])
