"""E2E tests: a top-level WHERE over a join view.

INNER `FROM a JOIN b ON a.k=b.k WHERE p` ≡ `ON (a.k=b.k AND p)`, so the WHERE folds
into the residual filter over the join output. OUTER (LEFT/RIGHT/FULL) equi joins
apply the WHERE as a post-null-fill 3VL filter over the merged output (preserved-side
predicates keep null-filled rows; right-side predicates drop them). A WHERE over an
OUTER range/band join is rejected.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_join_where.py -v --tb=short
"""
import random
import pytest


def _uid():
    return str(random.randint(100000, 999999))


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


def _rows(client, sn, view):
    vid = client.resolve_table(sn, view)[0]
    return [r._asdict() for r in client.scan(vid) if r.weight > 0]


def _setup(client, sn):
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amount BIGINT NOT NULL)",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL, tier BIGINT NOT NULL)",
        schema_name=sn,
    )


class TestJoinWhere:
    def test_where_filters_inner_join_one_sided(self, client):
        """WHERE on a left-table column keeps only matched rows passing the predicate."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _setup(client, sn)
            # View created first, then data (backfill + incrementality both exercised).
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid, customers.name AS nm "
                "FROM orders JOIN customers ON orders.cid = customers.id "
                "WHERE orders.amount > 150",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO customers VALUES (10, 'Alice', 1), (20, 'Bob', 2)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300)",
                schema_name=sn,
            )
            rows = _rows(client, sn, "v")
            # Only orders 2 (200) and 3 (300) pass amount > 150; order 1 (100) is dropped.
            got = sorted((r["oid"], r["nm"]) for r in rows)
            assert got == [(2, "Bob"), (3, "Alice")], got
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    def test_where_incremental_and_retraction(self, client):
        """Inserts below/above the threshold flow through; a retraction removes its row."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid FROM orders "
                "JOIN customers ON orders.cid = customers.id WHERE orders.amount >= 100",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (10, 'Alice', 1)", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 10, 50)", schema_name=sn)
            assert _rows(client, sn, "v") == [], "below-threshold order excluded"

            client.execute_sql("INSERT INTO orders VALUES (2, 10, 100), (3, 10, 500)", schema_name=sn)
            assert sorted(r["oid"] for r in _rows(client, sn, "v")) == [2, 3]

            # Retract order 3 — it must leave the view.
            client.execute_sql("DELETE FROM orders WHERE id = 3", schema_name=sn)
            assert sorted(r["oid"] for r in _rows(client, sn, "v")) == [2]
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    def test_where_compares_both_sides(self, client):
        """A WHERE conjunct spanning both tables binds through the join output."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid FROM orders "
                "JOIN customers ON orders.cid = customers.id WHERE orders.amount > customers.tier",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO customers VALUES (10, 'Alice', 100), (20, 'Bob', 5)",
                schema_name=sn,
            )
            # o1: amount 50 vs tier 100 -> excluded; o2: 200 vs 5 -> kept; o3: 90 vs 100 -> excluded.
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 50), (2, 20, 200), (3, 10, 90)",
                schema_name=sn,
            )
            assert sorted(r["oid"] for r in _rows(client, sn, "v")) == [2]
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    def test_where_multiple_conjuncts(self, client):
        """`WHERE p AND q` folds both conjuncts into the residual."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid FROM orders "
                "JOIN customers ON orders.cid = customers.id "
                "WHERE orders.amount > 100 AND customers.tier = 1",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO customers VALUES (10, 'Alice', 1), (20, 'Bob', 2)",
                schema_name=sn,
            )
            # o1: amount 200, tier 1 -> kept; o2: amount 200, tier 2 -> excluded (tier);
            # o3: amount 50, tier 1 -> excluded (amount).
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 200), (2, 20, 200), (3, 10, 50)",
                schema_name=sn,
            )
            assert sorted(r["oid"] for r in _rows(client, sn, "v")) == [1]
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    def test_where_on_left_join_preserved_side(self, client):
        """LEFT JOIN + WHERE on a preserved (left) column keeps null-filled rows that pass."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid, customers.name AS nm "
                "FROM orders LEFT JOIN customers ON orders.cid = customers.id "
                "WHERE orders.amount > 150",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (10, 'Alice', 1)", schema_name=sn)
            # o1 matched Alice amount 200>150 -> kept; o2 unmatched amount 300>150 -> kept (nm=NULL);
            # o3 matched amount 100 -> dropped; o4 unmatched amount 50 -> dropped.
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 200), (2, 99, 300), (3, 10, 100), (4, 99, 50)",
                schema_name=sn,
            )
            got = sorted((r["oid"], r["nm"]) for r in _rows(client, sn, "v"))
            assert got == [(1, "Alice"), (2, None)], got
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    def test_where_on_left_join_right_side_drops_nullfill(self, client):
        """WHERE on a right (non-preserved) column drops null-filled rows: NULL cmp = UNKNOWN."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid FROM orders "
                "LEFT JOIN customers ON orders.cid = customers.id WHERE customers.tier > 0",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (10, 'Alice', 5)", schema_name=sn)
            # o1 matched tier 5>0 -> kept; o2 unmatched tier=NULL, NULL>0=UNKNOWN -> dropped.
            client.execute_sql("INSERT INTO orders VALUES (1, 10, 100), (2, 99, 100)", schema_name=sn)
            assert sorted(r["oid"] for r in _rows(client, sn, "v")) == [1]
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    def test_where_on_left_join_is_null_keeps_unmatched(self, client):
        """`WHERE right.col IS NULL` keeps null-filled (unmatched) rows — anti-join via LEFT JOIN."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid FROM orders "
                "LEFT JOIN customers ON orders.cid = customers.id WHERE customers.id IS NULL",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (10, 'Alice', 5)", schema_name=sn)
            # o1 matched (customers.id=10, not null) -> dropped; o2 unmatched (id=NULL) -> kept.
            client.execute_sql("INSERT INTO orders VALUES (1, 10, 100), (2, 99, 100)", schema_name=sn)
            assert sorted(r["oid"] for r in _rows(client, sn, "v")) == [2]
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    def test_where_on_full_join(self, client):
        """FULL JOIN + WHERE filters the post-null-fill output on both sides' null-fills."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT orders.id AS oid, customers.id AS cid FROM orders "
                "FULL JOIN customers ON orders.cid = customers.id WHERE orders.id IS NOT NULL",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO customers VALUES (10, 'Alice', 5), (30, 'Carol', 9)", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 10, 100), (2, 99, 100)", schema_name=sn)
            # FULL join rows: (o1,c10) matched; (o2,NULL) left-only; (NULL,c30) right-only.
            # WHERE orders.id IS NOT NULL keeps only rows with a real order -> o1, o2.
            got = sorted((r["oid"], r["cid"]) for r in _rows(client, sn, "v"))
            assert got == [(1, 10), (2, None)], got
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    def test_where_on_outer_range_join_rejected(self, client):
        """WHERE over an OUTER range/band join is not supported."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _setup(client, sn)
            with pytest.raises(Exception) as ei:
                client.execute_sql(
                    "CREATE VIEW v AS SELECT orders.id AS oid FROM orders "
                    "LEFT JOIN customers ON orders.amount < customers.tier WHERE orders.amount > 10",
                    schema_name=sn,
                )
            assert "range" in str(ei.value).lower() or "not yet supported" in str(ei.value), str(ei.value)
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])
