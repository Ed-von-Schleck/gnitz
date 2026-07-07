"""E2E tests: equijoins in CREATE VIEW.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_joins.py -v --tb=short
"""
import os
import random
import pytest
import gnitz
import _oracle as oracle

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)


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


def _scan_dicts(client, tid):
    return [r._asdict() for r in client.scan(tid) if r.weight > 0]


class TestJoins:
    def test_inner_join_int_key(self, client):
        """Basic equijoin on integer key."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amount BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM orders JOIN customers ON orders.cid = customers.id",
                schema_name=sn,
            )
            # Insert customers first, then orders
            client.execute_sql(
                "INSERT INTO customers VALUES (10, 'Alice'), (20, 'Bob')",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            # 3 orders, each matched to a customer
            assert len(rows) == 3, f"expected 3 rows, got {len(rows)}: {rows}"
            # Check that customer name is present
            names = sorted([r["name"] for r in rows])
            assert names == ["Alice", "Alice", "Bob"]
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    def test_inner_join_no_match(self, client):
        """Rows without matches are excluded."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t1 JOIN t2 ON t1.fk = t2.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t2 VALUES (10, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO t1 VALUES (1, 99)", schema_name=sn)  # no match
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 0, f"expected 0 rows, got {rows}"
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    def test_join_incremental_update(self, client):
        """Insert into one table, then the other — join updates incrementally."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t1 JOIN t2 ON t1.fk = t2.id",
                schema_name=sn,
            )
            # Insert into t1 first — no matches yet
            client.execute_sql("INSERT INTO t1 VALUES (1, 10)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 0

            # Now insert matching row into t2 — join should produce result
            client.execute_sql("INSERT INTO t2 VALUES (10, 100)", schema_name=sn)
            rows = _scan_dicts(client, vid)
            assert len(rows) == 1, f"expected 1 row after t2 insert, got {rows}"
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    def test_join_cross_rejects(self, client):
        """CROSS JOIN should be rejected."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            with pytest.raises(Exception):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT * FROM t1 CROSS JOIN t2",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    def test_inner_join_left_delete_retraction(self, client):
        """Deleting a left-side row retracts its matched rows from the view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t1 JOIN t2 ON t1.fk = t2.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t2 VALUES (10, 100)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO t1 VALUES (1, 10), (2, 10)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 2, f"expected 2 rows before delete, got {rows}"

            client.execute_sql("DELETE FROM t1 WHERE id = 1", schema_name=sn)
            rows = _scan_dicts(client, vid)
            assert len(rows) == 1, f"expected 1 row after left-side delete, got {rows}"
            # The remaining row should have val=100 from t2 (both t1 rows matched t2 pk=10)
            assert rows[0]["val"] == 100
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    def test_inner_join_right_delete_retraction(self, client):
        """Deleting a right-side row retracts all matched rows from the view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t1 JOIN t2 ON t1.fk = t2.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t2 VALUES (10, 100)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO t1 VALUES (1, 10), (2, 10)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 2, f"expected 2 rows before delete, got {rows}"

            client.execute_sql("DELETE FROM t2 WHERE id = 10", schema_name=sn)
            rows = _scan_dicts(client, vid)
            assert len(rows) == 0, f"expected 0 rows after right-side delete, got {rows}"
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    def test_join_non_equi_range_accepted(self, client):
        """A non-equi (range) join condition is now supported: it compiles to a
        DeltaTraceRange join whose output PK is the source-PK pair (t1.id, t2.id).
        (See tests/test_workers.py::TestRangeJoin for full distributed coverage.)"""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            # Previously rejected; now a valid pure range join.
            client.execute_sql(
                "CREATE VIEW v AS SELECT t1.a AS a, t2.b AS b FROM t1 JOIN t2 ON t1.a < t2.b",
                schema_name=sn,
            )
            t1 = [(1, 10), (2, 30), (3, 50)]   # (id, a)
            t2 = [(1, 20), (2, 40)]            # (id, b)
            client.execute_sql("INSERT INTO t1 VALUES " + ",".join(f"({i},{a})" for i, a in t1), schema_name=sn)
            client.execute_sql("INSERT INTO t2 VALUES " + ",".join(f"({i},{b})" for i, b in t2), schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            # The view's pair-PK columns are hidden synthetic keys; surface them
            # with include_hidden so (r[0], r[1]) = (t1.id, t2.id).
            pairs = {(r[0], r[1]) for r in client.scan(vid, include_hidden=True)
                     if r.weight > 0}
            want = {(ai, bi) for (ai, a) in t1 for (bi, b) in t2 if a < b}
            assert pairs == want, f"range join: got {pairs}, want {want}"
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    def test_inner_join_string_payload(self, client):
        """Join with VARCHAR payload columns: verify blob data correct in output."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY, "
                "cat_id BIGINT NOT NULL, label VARCHAR(200) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE categories (id BIGINT NOT NULL PRIMARY KEY, "
                "name VARCHAR(200) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM items "
                "JOIN categories ON items.cat_id = categories.id",
                schema_name=sn,
            )
            # Insert categories first
            client.execute_sql(
                "INSERT INTO categories VALUES (100, 'Electronics'), (200, 'Books')",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO items VALUES (1, 100, 'Laptop'), (2, 200, 'Novel'), "
                "(3, 100, 'Phone')",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            assert len(rows) == 3, f"expected 3 rows, got {len(rows)}"
            labels = sorted([r["label"] for r in rows])
            assert labels == ["Laptop", "Novel", "Phone"]
            cats = sorted([r["name"] for r in rows])
            assert cats == ["Books", "Electronics", "Electronics"]
        finally:
            _cleanup(client, sn, tables=["items", "categories"], views=["v"])

    def test_many_to_many_join(self, client):
        """Both sides have multiple rows matching same key — verify cross-product."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, a BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t1 JOIN t2 ON t1.fk = t2.fk",
                schema_name=sn,
            )
            # 2 left rows with fk=10, 3 right rows with fk=10
            client.execute_sql(
                "INSERT INTO t2 VALUES (101, 10, 1), (102, 10, 2), (103, 10, 3)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t1 VALUES (1, 10, 100), (2, 10, 200)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            # 2 left x 3 right = 6 output rows
            assert len(rows) == 6, f"expected 6 cross-product rows, got {len(rows)}"
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v"])

    def test_inner_join_differential_oracle(self, client):
        """Lock-in: the canonical two-distinct-table inner join, checked against
        the from-scratch oracle after each epoch (validates the oracle against a
        known-good path)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amount BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, cname BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS "
                "SELECT orders.id AS oid, orders.amount AS amt, "
                "customers.id AS cid, customers.cname AS cname "
                "FROM orders JOIN customers ON orders.cid = customers.id",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            project = ["oid", "amt", "cid", "cname"]
            orders, customers = {}, {}

            def expected():
                return oracle.oracle_equijoin(
                    left=orders, lwhere=None, lkey="cid", lproj=["id", "amount"],
                    right=customers, rwhere=None, rkey="id", rproj=["id", "cname"],
                    out_cols=project,
                )

            def check(ctx):
                oracle.assert_view_matches(client, vid, project, expected(), ctx=ctx)

            client.execute_sql("INSERT INTO customers VALUES (10, 111), (20, 222)", schema_name=sn)
            oracle.apply_insert(customers, "id", [{"id": 10, "cname": 111}, {"id": 20, "cname": 222}])
            check("after-customers")

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300)", schema_name=sn)
            oracle.apply_insert(orders, "id", [
                {"id": 1, "cid": 10, "amount": 100},
                {"id": 2, "cid": 20, "amount": 200},
                {"id": 3, "cid": 10, "amount": 300}])
            check("after-orders")

            client.execute_sql("DELETE FROM customers WHERE id = 10", schema_name=sn)
            oracle.apply_delete(customers, "id", [10])
            check("after-delete-customer")
        finally:
            _cleanup(client, sn, tables=["orders", "customers"], views=["v"])

    @_NEEDS_MULTI
    def test_inner_join_wide_u64_pks_multiworker(self, client):
        """Inner join on BIGINT PK columns distributed across multiple workers.

        Both tables use U64 PKs (narrow physical representation). Rows are
        spread across workers by hash-partitioning. Verifies that exchange
        routing, narrow PK encode/decode, and join produce the correct
        cross-matched result with no duplicates or missing rows.
        """
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE left_t "
                "(id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, lval BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE right_t "
                "(id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, rval BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS "
                "SELECT left_t.id AS lid, right_t.id AS rid, left_t.lval, right_t.rval "
                "FROM left_t JOIN right_t ON left_t.fk = right_t.fk",
                schema_name=sn,
            )
            # Use a range of PKs that span multiple hash buckets / workers.
            n = 20
            left_vals = ", ".join(f"({i}, {i % 5}, {i * 10})" for i in range(1, n + 1))
            right_vals = ", ".join(f"({i + 100}, {i % 5}, {i * 100})" for i in range(1, n + 1))
            client.execute_sql(f"INSERT INTO left_t VALUES {left_vals}", schema_name=sn)
            client.execute_sql(f"INSERT INTO right_t VALUES {right_vals}", schema_name=sn)

            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            # 5 fk groups × (4 left × 4 right matches) = 80 rows.
            assert len(rows) == 80, f"expected 80 join rows, got {len(rows)}"
            # Every left row (lid 1..n) must appear in the output.
            lids = {r["lid"] for r in rows}
            assert lids == set(range(1, n + 1)), f"unexpected lids: {lids}"
        finally:
            _cleanup(client, sn, tables=["left_t", "right_t"], views=["v"])

    def test_inner_join_composite_key(self, client):
        """Composite (k=2) equijoin `ON a.x = b.x AND a.y = b.y`. Only rows that
        agree on BOTH key columns join; the view's PK is the 2-column synthetic
        `_join_pk`. Includes an incremental insert that completes a pair."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, "
                "y BIGINT NOT NULL, av BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, "
                "y BIGINT NOT NULL, bv BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x, a.y, a.av, b.bv "
                "FROM a JOIN b ON a.x = b.x AND a.y = b.y",
                schema_name=sn,
            )
            vid, vschema = client.resolve_table(sn, "v")
            assert vschema.pk_indices == [0, 1], "k=2 join PK is the two _join_pk columns"

            # (10,100) and (30,300) match; (20,*) differs in y → no match.
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10, 100, 1), (2, 20, 200, 2), (3, 30, 300, 3)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO b VALUES (1, 10, 100, 11), (2, 20, 999, 22), (3, 30, 300, 33)",
                schema_name=sn,
            )
            rows = _scan_dicts(client, vid)
            assert sorted((r["x"], r["y"], r["av"], r["bv"]) for r in rows) == [
                (10, 100, 1, 11), (30, 300, 3, 33),
            ], "INNER k=2 join keeps only rows agreeing on both key columns"

            # Incremental: a b-row completing the (20,200) pair must join in.
            client.execute_sql("INSERT INTO b VALUES (4, 20, 200, 44)", schema_name=sn)
            rows = _scan_dicts(client, vid)
            assert sorted((r["x"], r["y"], r["av"], r["bv"]) for r in rows) == [
                (10, 100, 1, 11), (20, 200, 2, 44), (30, 300, 3, 33),
            ], "incremental INNER join admits the newly-completed composite-key pair"
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    @_NEEDS_MULTI
    def test_inner_join_composite_key_multiworker(self, client):
        """Composite (k=2) equijoin across multiple workers: the k-wide reindex
        and exchange must co-locate rows that agree on the full (x, y) key. Both
        sides are spread across hash buckets; every key value yields one match."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, "
                "y BIGINT NOT NULL, av BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, "
                "y BIGINT NOT NULL, bv BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x, a.y, a.av, b.bv "
                "FROM a JOIN b ON a.x = b.x AND a.y = b.y",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            n = 20
            a_vals = ", ".join(f"({i}, {i % 5}, {i % 7}, {i})" for i in range(1, n + 1))
            # b carries the SAME (x, y) keys but distinct ids/payloads.
            b_vals = ", ".join(f"({i + 100}, {i % 5}, {i % 7}, {i * 10})" for i in range(1, n + 1))
            client.execute_sql(f"INSERT INTO a VALUES {a_vals}", schema_name=sn)
            client.execute_sql(f"INSERT INTO b VALUES {b_vals}", schema_name=sn)

            rows = _scan_dicts(client, vid)
            # Recompute the expected INNER join over the full (x, y) key.
            a_rows = [(i % 5, i % 7, i) for i in range(1, n + 1)]
            b_rows = [(i % 5, i % 7, i * 10) for i in range(1, n + 1)]
            expected = sorted(
                (ax, ay, av, bv)
                for (ax, ay, av) in a_rows
                for (bx, by, bv) in b_rows
                if ax == bx and ay == by
            )
            got = sorted((r["x"], r["y"], r["av"], r["bv"]) for r in rows)
            assert got == expected, "multi-worker composite join must match a full recompute"
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    @_NEEDS_MULTI
    def test_inner_join_cross_width_int_bigint(self, client):
        """Cross-width SAME-SIGN equijoin: INT (I32) key = BIGINT (I64) key. The
        planner promotes the pair to the wider type I64, OPK-encodes both sides'
        key into the I64 slot, and the exchange co-partitions equal numeric values
        across workers. Covers matching, non-matching, NEGATIVE values (signed
        sign-extension), a duplicate key spanning workers, and a retraction."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            # left.k is INT (I32); right.k is BIGINT (I64).
            client.execute_sql(
                "CREATE TABLE lt (id BIGINT NOT NULL PRIMARY KEY, k INT NOT NULL, lv BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE rt (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, rv BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT lt.id AS lid, rt.id AS rid, lt.k AS lk, lt.lv, rt.rv "
                "FROM lt JOIN rt ON lt.k = rt.k",
                schema_name=sn,
            )
            vid, vschema = client.resolve_table(sn, "v")
            # The synthetic _join_pk is the promoted common type I64 (8 bytes).
            assert vschema.columns[0].type_code == gnitz.TypeCode.I64, \
                f"_join_pk must be promoted to I64, got {vschema.columns[0].type_code}"

            # Keys: 5 matches, a negative key (-7) on both sides, and a duplicate
            # key (3) that must co-locate. id 4 / 999 are non-matching.
            client.execute_sql(
                "INSERT INTO lt VALUES (1, 3, 10), (2, -7, 20), (3, 3, 30), "
                "(4, 100, 40), (5, 0, 50)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO rt VALUES (101, 3, 1000), (102, -7, 2000), (103, 0, 3000), "
                "(999, 555, 9000)",
                schema_name=sn,
            )

            def got():
                return sorted((r["lk"], r["lv"], r["rv"]) for r in _scan_dicts(client, vid))

            # k=3 matches lt{1,3} × rt{101} = 2 rows; k=-7 matches lt{2}×rt{102};
            # k=0 matches lt{5}×rt{103}; k=100,555 unmatched.
            assert got() == sorted([
                (3, 10, 1000), (3, 30, 1000), (-7, 20, 2000), (0, 50, 3000),
            ]), f"cross-width INT=BIGINT join mismatch: {got()}"

            # Retraction: DELETE the duplicate-key left row id=3 → its join row drops.
            client.execute_sql("DELETE FROM lt WHERE id = 3", schema_name=sn)
            assert got() == sorted([
                (3, 10, 1000), (-7, 20, 2000), (0, 50, 3000),
            ]), f"retraction must drop the deleted row's join output: {got()}"

            # Incremental: a new right row completing k=100 must join in.
            client.execute_sql("INSERT INTO rt VALUES (104, 100, 4000)", schema_name=sn)
            assert got() == sorted([
                (3, 10, 1000), (-7, 20, 2000), (0, 50, 3000), (100, 40, 4000),
            ]), f"incremental cross-width match must appear: {got()}"
        finally:
            _cleanup(client, sn, tables=["lt", "rt"], views=["v"])

    @_NEEDS_MULTI
    def test_inner_join_cross_width_u32_u64(self, client):
        """Cross-width unsigned equijoin: INT UNSIGNED (U32) = BIGINT UNSIGNED
        (U64), promoted to U64. Exercises large values near the U32 ceiling so
        zero-extension into the wider slot is checked across workers."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE lt (id BIGINT NOT NULL PRIMARY KEY, k INT UNSIGNED NOT NULL, lv BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE rt (id BIGINT NOT NULL PRIMARY KEY, k BIGINT UNSIGNED NOT NULL, rv BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT lt.k AS lk, lt.lv, rt.rv "
                "FROM lt JOIN rt ON lt.k = rt.k",
                schema_name=sn,
            )
            vid, vschema = client.resolve_table(sn, "v")
            assert vschema.columns[0].type_code == gnitz.TypeCode.U64, \
                f"_join_pk must be promoted to U64, got {vschema.columns[0].type_code}"

            big = 4294967295   # u32::MAX
            client.execute_sql(
                f"INSERT INTO lt VALUES (1, 7, 10), (2, {big}, 20), (3, 0, 30)",
                schema_name=sn,
            )
            client.execute_sql(
                f"INSERT INTO rt VALUES (101, 7, 100), (102, {big}, 200), (103, 12345, 300)",
                schema_name=sn,
            )
            got = sorted((r["lk"], r["lv"], r["rv"]) for r in _scan_dicts(client, vid))
            assert got == sorted([(7, 10, 100), (big, 20, 200)]), \
                f"cross-width U32=U64 join mismatch: {got}"
        finally:
            _cleanup(client, sn, tables=["lt", "rt"], views=["v"])

    @_NEEDS_MULTI
    def test_inner_join_overlapping_key_cross_width(self, client):
        """Overlapping key `ON a.x = b.p AND a.x = b.q`: the `a` side reindexes
        `[x, x]` into two _join_pk slots. With a cross-width promotion on one slot,
        the two slots carry distinct targets and the scatter must mirror the
        trace packer slot-for-slot. A row joins only when b.p == b.q == a.x."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            # a.x is INT (I32); b.p, b.q are BIGINT (I64). Both pairs promote to I64.
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x INT NOT NULL, av BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, p BIGINT NOT NULL, "
                "q BIGINT NOT NULL, bv BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x AS ax, a.av, b.bv "
                "FROM a JOIN b ON a.x = b.p AND a.x = b.q",
                schema_name=sn,
            )
            vid, vschema = client.resolve_table(sn, "v")
            assert vschema.pk_indices == [0, 1], "overlapping key → 2-slot _join_pk"

            client.execute_sql(
                "INSERT INTO a VALUES (1, 5, 10), (2, 8, 20), (3, -4, 30)",
                schema_name=sn,
            )
            # b rows: (5,5) matches a.x=5; (8,9) p!=q → no match; (-4,-4) matches.
            client.execute_sql(
                "INSERT INTO b VALUES (101, 5, 5, 100), (102, 8, 9, 200), (103, -4, -4, 300)",
                schema_name=sn,
            )
            got = sorted((r["ax"], r["av"], r["bv"]) for r in _scan_dicts(client, vid))
            assert got == sorted([(5, 10, 100), (-4, 30, 300)]), \
                f"overlapping cross-width key join mismatch: {got}"
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    @_NEEDS_MULTI
    def test_left_join_cross_width_nullable_key(self, client):
        """LEFT JOIN with a NULLABLE cross-width key exercises the sibling-Map path
        (null-key bypass + not-null match side both reindex at the promoted width).
        A NULL key emits NULL right columns; a present key joins; both co-partition."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            # a.x is a NULLABLE INT (I32); b.k is BIGINT (I64). Promote to I64.
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x INT, av BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, bv BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, a.av, b.bv "
                "FROM a LEFT JOIN b ON a.x = b.k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO b VALUES (101, 5, 100), (102, -3, 200)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO a VALUES (1, 5, 10), (2, NULL, 20), (3, -3, 30), (4, 77, 40)",
                schema_name=sn,
            )
            # id1 → match bv=100; id3 → match bv=200; id2 (NULL key) and id4 (no
            # match) → NULL right (bv None).
            got = sorted((r["aid"], r["av"], r["bv"]) for r in _scan_dicts(client, vid))
            assert got == sorted([
                (1, 10, 100), (2, 20, None), (3, 30, 200), (4, 40, None),
            ]), f"LEFT JOIN cross-width nullable key mismatch: {got}"
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    def test_inner_join_cross_sign_key(self, client):
        """Cross-sign equijoin: INT UNSIGNED (U32) key = BIGINT (I64) key, promoted
        to the signed common type I64. Equal numeric values pack byte-identically
        (unsigned zero-extends, signed sign-extends) so they co-partition and join;
        a value above i32::MAX exercises the unsigned-only region, and a NEGATIVE
        BIGINT (with no unsigned counterpart) must never match. Inserts happen after
        CREATE VIEW (delta-trace + scatter path); runs at the suite default
        GNITZ_WORKERS=4 so the cross-worker co-partition path is covered."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            # lt.k is INT UNSIGNED (U32); rt.k is BIGINT (I64). Promote to I64.
            client.execute_sql(
                "CREATE TABLE lt (id BIGINT NOT NULL PRIMARY KEY, k INT UNSIGNED NOT NULL, lv BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE rt (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, rv BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT lt.k AS lk, lt.lv, rt.rv "
                "FROM lt JOIN rt ON lt.k = rt.k",
                schema_name=sn,
            )
            vid, vschema = client.resolve_table(sn, "v")
            assert vschema.columns[0].type_code == gnitz.TypeCode.I64, \
                f"_join_pk must be promoted to I64, got {vschema.columns[0].type_code}"

            big = 4_000_000_000   # > i32::MAX, fits in U32 and I64
            client.execute_sql(
                f"INSERT INTO lt VALUES (1, 7, 10), (2, {big}, 20), (3, 0, 30), (4, 12345, 40)",
                schema_name=sn,
            )
            # rt: matches for 7 and big; a NEGATIVE key -7 (no unsigned twin) and
            # 999999 (no left twin) must not join.
            client.execute_sql(
                f"INSERT INTO rt VALUES (101, 7, 100), (102, {big}, 200), "
                "(103, -7, 300), (104, 999999, 400)",
                schema_name=sn,
            )

            def got():
                return sorted((r["lk"], r["lv"], r["rv"]) for r in _scan_dicts(client, vid))

            assert got() == sorted([(7, 10, 100), (big, 20, 200)]), \
                f"cross-sign U32=I64 join mismatch: {got()}"

            # Incremental: a new right row completing k=0 joins in; a negative right
            # row must never collide with the unsigned 0 (distinct I64 OPK keys).
            client.execute_sql("INSERT INTO rt VALUES (105, 0, 500), (106, -1, 600)", schema_name=sn)
            assert got() == sorted([(7, 10, 100), (big, 20, 200), (0, 30, 500)]), \
                f"incremental cross-sign match must appear and negatives must not: {got()}"
        finally:
            _cleanup(client, sn, tables=["lt", "rt"], views=["v"])

    def test_join_cross_sign_rejected(self, client):
        """The surviving cross-sign reject: a key whose UNSIGNED side is 128-bit
        (DECIMAL(38,0) = BIGINT, i.e. U128 = I64) would need a signed-256 type that
        does not exist, so CREATE VIEW fails with a clear planner error. (Cross-sign
        pairs whose unsigned side is U8/U16/U32 promote to a wider signed type, and
        U64 promotes to the signed-128 type I128 — both accepted; see
        test_inner_join_cross_sign_key and test_i128_cross_sign_join.)"""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k DECIMAL(38,0) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(Exception):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.k",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    def test_inner_join_compound_pk_source_dml(self, client):
        """Equijoin over compound-PK source tables, driven by INSERT/UPDATE/DELETE.
        Both sources have `PRIMARY KEY (k1, k2)` / `(j1, j2)`; the join key is a
        composite `(x, y)` drawn from non-PK columns. The compound source PK rides
        as payload (k1, k2). After every DML tick the incremental view must equal a
        full recompute. Under `make e2e` this runs at GNITZ_WORKERS=4, so the
        compound source PK and the (x, y) join key are exchange-routed independently."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, "
                "x BIGINT NOT NULL, y BIGINT NOT NULL, av BIGINT NOT NULL, "
                "PRIMARY KEY (k1, k2))",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (j1 BIGINT NOT NULL, j2 BIGINT NOT NULL, "
                "x BIGINT NOT NULL, y BIGINT NOT NULL, bv BIGINT NOT NULL, "
                "PRIMARY KEY (j1, j2))",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.k1, a.k2, a.av, b.bv "
                "FROM a JOIN b ON a.x = b.x AND a.y = b.y",
                schema_name=sn,
            )
            vid, vschema = client.resolve_table(sn, "v")
            assert vschema.pk_indices == [0, 1], "k=2 join PK is the two _join_pk columns"

            # Mirror table state in Python; recompute the join after each tick.
            a_state = {}  # (k1, k2) -> (x, y, av)
            b_state = {}  # (j1, j2) -> (x, y, bv)

            def expected():
                return sorted(
                    (k1, k2, av, bv)
                    for (k1, k2), (ax, ay, av) in a_state.items()
                    for (_j1, _j2), (bx, by, bv) in b_state.items()
                    if ax == bx and ay == by
                )

            def got():
                return sorted(
                    (r["k1"], r["k2"], r["av"], r["bv"]) for r in _scan_dicts(client, vid)
                )

            # Tick 1: initial inserts.
            client.execute_sql(
                "INSERT INTO a VALUES (1, 1, 10, 100, 1), (2, 2, 20, 200, 2), (3, 3, 30, 300, 3)",
                schema_name=sn,
            )
            a_state.update({(1, 1): (10, 100, 1), (2, 2): (20, 200, 2), (3, 3): (30, 300, 3)})
            client.execute_sql(
                "INSERT INTO b VALUES (5, 5, 10, 100, 11), (6, 6, 20, 999, 22), (7, 7, 30, 300, 33)",
                schema_name=sn,
            )
            b_state.update({(5, 5): (10, 100, 11), (6, 6): (20, 999, 22), (7, 7): (30, 300, 33)})
            assert got() == expected(), "initial INSERT join"

            # Tick 2: UPDATE a payload column (av) — retract+insert, same key.
            client.execute_sql("UPDATE a SET av = 111 WHERE k1 = 1 AND k2 = 1", schema_name=sn)
            a_state[(1, 1)] = (10, 100, 111)
            assert got() == expected(), "UPDATE a payload"

            # Tick 3: UPDATE b's join key (y) so the (20, *) pair now matches.
            client.execute_sql("UPDATE b SET y = 200 WHERE j1 = 6 AND j2 = 6", schema_name=sn)
            b_state[(6, 6)] = (20, 200, 22)
            assert got() == expected(), "UPDATE b join key completes a pair"

            # Tick 4: DELETE a source row.
            client.execute_sql("DELETE FROM a WHERE k1 = 3 AND k2 = 3", schema_name=sn)
            del a_state[(3, 3)]
            assert got() == expected(), "DELETE a row drops its join output"

            # Tick 5: DELETE a b source row.
            client.execute_sql("DELETE FROM b WHERE j1 = 5 AND j2 = 5", schema_name=sn)
            del b_state[(5, 5)]
            assert got() == expected(), "DELETE b row drops its join output"
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    def test_inner_join_wide_compound_pk_source(self, client):
        """Equijoin over a wide (3-column, 24-byte) compound source PK. Several
        source rows share the first 16 OPK bytes (`(k1, k2)`) and differ only past
        byte 16 (`k3`); they must survive ingest/scan as distinct and join
        independently. Under `make e2e` (GNITZ_WORKERS=4) this exercises
        multi-worker exchange of the wide source PK end-to-end."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, k3 BIGINT NOT NULL, "
                "fk BIGINT NOT NULL, av BIGINT NOT NULL, PRIMARY KEY (k1, k2, k3))",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, bv BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.k1, a.k2, a.k3, a.av, b.bv "
                "FROM a JOIN b ON a.fk = b.fk",
                schema_name=sn,
            )
            vid, vschema = client.resolve_table(sn, "v")
            assert vschema.pk_indices == [0], "single-key join PK is the lone _join_pk"

            a_state = {}  # (k1, k2, k3) -> (fk, av)
            b_state = {}  # id -> (fk, bv)

            def expected():
                return sorted(
                    (k1, k2, k3, av, bv)
                    for (k1, k2, k3), (afk, av) in a_state.items()
                    for _id, (bfk, bv) in b_state.items()
                    if afk == bfk
                )

            def got():
                return sorted(
                    (r["k1"], r["k2"], r["k3"], r["av"], r["bv"]) for r in _scan_dicts(client, vid)
                )

            # (1,1,1), (1,1,2), (1,1,3) share the first 16 OPK bytes; differ past byte 16.
            client.execute_sql(
                "INSERT INTO a VALUES (1, 1, 1, 7, 11), (1, 1, 2, 7, 22), (1, 1, 3, 9, 33)",
                schema_name=sn,
            )
            a_state.update({(1, 1, 1): (7, 11), (1, 1, 2): (7, 22), (1, 1, 3): (9, 33)})
            client.execute_sql("INSERT INTO b VALUES (1, 7, 70), (2, 9, 90)", schema_name=sn)
            b_state.update({1: (7, 70), 2: (9, 90)})
            assert got() == expected(), "wide compound source PK: rows sharing 16-byte prefix join distinctly"

            # UPDATE one tie-break sibling's payload, DELETE another.
            client.execute_sql("UPDATE a SET av = 222 WHERE k1 = 1 AND k2 = 1 AND k3 = 2", schema_name=sn)
            a_state[(1, 1, 2)] = (7, 222)
            assert got() == expected(), "UPDATE one tie-break sibling"

            client.execute_sql("DELETE FROM a WHERE k1 = 1 AND k2 = 1 AND k3 = 1", schema_name=sn)
            del a_state[(1, 1, 1)]
            assert got() == expected(), "DELETE one tie-break sibling leaves the others intact"
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])
