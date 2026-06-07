"""E2E tests: co-partitioning of NARROW integer keys (I8/I16/I32 and their
unsigned twins) used as JOIN keys and GROUP BY keys.

Repartitioning hashes a routing key per row. A single-PK column routes by the
raw widened value (`get_pk`), but a single non-PK column routed by
`extract_group_key`, whose fast path used to cover only U64/I64/U128 — narrow
ints fell into the `mix64` hash loop. So the same logical value routed to one
worker as a PK and a DIFFERENT worker as a non-PK column, breaking JOIN
co-partitioning (matches silently dropped) and GROUP BY routing for narrow
keys under multiple workers. The existing operator suites only use 8-byte
(BIGINT) keys, so this gap went uncaught.

These tests use values chosen to spread across workers (incl. negatives) and
assert every row participates.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_narrow_key_copartition.py -v --tb=short
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


def _dicts(client, tid):
    return [r._asdict() for r in client.scan(tid) if r.weight > 0]


# Narrow integer SQL types and a value spread that covers several workers.
# Signed types include negatives; unsigned use only non-negative values.
_SIGNED = [
    ("TINYINT", [-100, -1, 0, 1, 5, 100]),
    ("SMALLINT", [-9000, -1, 0, 1, 5, 9000]),
    ("INT", [-2_000_000_000, -1, 0, 1, 5, 2_000_000_000]),
]
_UNSIGNED = [
    ("TINYINT UNSIGNED", [0, 1, 5, 200]),
    ("SMALLINT UNSIGNED", [0, 1, 5, 60000]),
    ("INT UNSIGNED", [0, 1, 5, 4_000_000_000]),
]

# Cross-sign twins: (customers PK type, orders column type, keys). One side is
# signed, the other unsigned with the unsigned operand ≤ 4 bytes; the pair
# promotes to the narrowest signed type holding both ranges. Keys are
# non-negative and representable on BOTH sides, and include values above the
# signed midpoint to exercise zero-extension of the unsigned operand.
_CROSS_SIGN = [
    ("INT UNSIGNED", "BIGINT", [0, 1, 5, 4_000_000_000]),
    ("BIGINT", "INT UNSIGNED", [0, 1, 5, 4_000_000_000]),
    ("SMALLINT", "TINYINT UNSIGNED", [0, 1, 5, 127]),
    ("INT", "SMALLINT UNSIGNED", [0, 1, 5, 60000]),
]


class TestNarrowJoinKey:
    """`orders JOIN customers ON orders.cid = customers.id`: customers.id is a
    narrow PK (routes by raw value); orders.cid is a narrow non-PK column
    (routes by extract_group_key). Every order must find its customer."""

    @pytest.mark.parametrize("key_type,keys", _SIGNED + _UNSIGNED)
    def test_join_on_narrow_key(self, client, key_type, keys):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                f"CREATE TABLE customers (id {key_type} NOT NULL PRIMARY KEY, tag BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                f"CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid {key_type} NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid, customers.tag AS tag "
                "FROM orders JOIN customers ON orders.cid = customers.id",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            cust_sql = ",".join(f"({k}, {1000 + i})" for i, k in enumerate(keys))
            client.execute_sql(f"INSERT INTO customers VALUES {cust_sql}", schema_name=sn)
            # One order per customer key; order id encodes the key index.
            ord_sql = ",".join(f"({i}, {k})" for i, k in enumerate(keys))
            client.execute_sql(f"INSERT INTO orders VALUES {ord_sql}", schema_name=sn)

            rows = _dicts(client, vid)
            got = sorted(r["oid"] for r in rows)
            assert got == sorted(range(len(keys))), (
                f"{key_type}: every order must join its customer; got {got}"
            )
            # Verify each order joined the RIGHT customer (correct tag).
            by_oid = {r["oid"]: r["tag"] for r in rows}
            for i in range(len(keys)):
                assert by_oid[i] == 1000 + i, f"{key_type}: order {i} joined wrong customer"
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    @pytest.mark.parametrize("cust_type,ord_type,keys", _CROSS_SIGN)
    def test_join_on_cross_sign_key(self, client, cust_type, ord_type, keys):
        """Cross-sign join: customers.id (PK, routes by raw value) and orders.cid
        (non-PK, routes by extract_group_key) differ in sign class. Both sides
        promote to a common signed type and must route the same value to the same
        worker, so every order still finds its customer under multiple workers."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                f"CREATE TABLE customers (id {cust_type} NOT NULL PRIMARY KEY, tag BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                f"CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid {ord_type} NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid, customers.tag AS tag "
                "FROM orders JOIN customers ON orders.cid = customers.id",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            cust_sql = ",".join(f"({k}, {1000 + i})" for i, k in enumerate(keys))
            client.execute_sql(f"INSERT INTO customers VALUES {cust_sql}", schema_name=sn)
            ord_sql = ",".join(f"({i}, {k})" for i, k in enumerate(keys))
            client.execute_sql(f"INSERT INTO orders VALUES {ord_sql}", schema_name=sn)

            rows = _dicts(client, vid)
            got = sorted(r["oid"] for r in rows)
            assert got == sorted(range(len(keys))), (
                f"{cust_type}={ord_type}: every order must join its customer; got {got}"
            )
            by_oid = {r["oid"]: r["tag"] for r in rows}
            for i in range(len(keys)):
                assert by_oid[i] == 1000 + i, (
                    f"{cust_type}={ord_type}: order {i} joined wrong customer"
                )
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    def test_join_on_narrow_key_incremental(self, client):
        """Stream orders as a delta after customers exist (delta-trace path),
        then retract one; the negative-key match must be maintained."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE customers (id INT NOT NULL PRIMARY KEY, tag BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid INT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid, customers.tag AS tag "
                "FROM orders JOIN customers ON orders.cid = customers.id",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO customers VALUES (-1, 10), (1, 20), (-7, 30)", schema_name=sn
            )
            client.execute_sql(
                "INSERT INTO orders VALUES (100, -1), (101, -1), (102, 1), (103, -7)",
                schema_name=sn,
            )
            assert sorted(r["oid"] for r in _dicts(client, vid)) == [100, 101, 102, 103]

            client.execute_sql("DELETE FROM orders WHERE id = 101", schema_name=sn)
            assert sorted(r["oid"] for r in _dicts(client, vid)) == [100, 102, 103]
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])


class TestNarrowGroupByKey:
    """GROUP BY a narrow non-PK integer column. Routing the group key must agree
    with where the aggregate is stored, or rows for one group land on multiple
    workers and the aggregate is wrong / the incremental retraction misfires."""

    @pytest.mark.parametrize("key_type,keys", _SIGNED)
    def test_group_by_narrow_key_sum(self, client, key_type, keys):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, grp {key_type} NOT NULL, amount BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT grp, SUM(amount) AS total, COUNT(*) AS cnt FROM t GROUP BY grp",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Two rows per group → SUM must combine both (would split if the two
            # rows routed to different workers than the group's aggregate).
            pk = 0
            vals = []
            for g in keys:
                vals.append(f"({pk}, {g}, 10)")
                pk += 1
                vals.append(f"({pk}, {g}, 5)")
                pk += 1
            client.execute_sql(f"INSERT INTO t VALUES {','.join(vals)}", schema_name=sn)

            rows = {r["grp"]: r for r in _dicts(client, vid)}
            assert set(rows.keys()) == set(keys), (
                f"{key_type}: one group per distinct key; got {set(rows.keys())}"
            )
            for g in keys:
                assert rows[g]["total"] == 15, f"{key_type}: SUM for grp={g} must be 15"
                assert rows[g]["cnt"] == 2, f"{key_type}: COUNT for grp={g} must be 2"
        finally:
            _cleanup(client, sn, tables=["t"], views=["v"])

    def test_group_by_narrow_key_incremental_retract(self, client):
        """Delete one row of a two-row group on a negative key; SUM must drop to
        the remaining row (exercises the aggregate's incremental retraction
        seek, keyed by the narrow group value)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, grp INT NOT NULL, amount BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT grp, SUM(amount) AS total FROM t GROUP BY grp",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO t VALUES (1, -5, 100), (2, -5, 50), (3, 7, 30)", schema_name=sn
            )
            rows = {r["grp"]: r["total"] for r in _dicts(client, vid)}
            assert rows == {-5: 150, 7: 30}, f"initial sums wrong: {rows}"

            client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
            rows = {r["grp"]: r["total"] for r in _dicts(client, vid)}
            assert rows == {-5: 100, 7: 30}, f"after delete sums wrong: {rows}"
        finally:
            _cleanup(client, sn, tables=["t"], views=["v"])
