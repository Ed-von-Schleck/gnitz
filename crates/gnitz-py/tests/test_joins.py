"""E2E tests: equijoins in CREATE VIEW.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_joins.py -v --tb=short
"""
import os
from collections import Counter

import pytest
import gnitz
import _oracle as oracle
from _uid import uid as _uid

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)




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
    return client.scan(tid).mappings()


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

    def test_global_join_is_the_plain_join(self, client):
        """ClickHouse `GLOBAL JOIN` names the whole-relation evaluation a DBSP
        join already does, so it is accepted and yields the plain join's rows."""
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
                "CREATE VIEW v_plain AS SELECT t1.id, t2.val FROM t1 JOIN t2 ON t1.fk = t2.id",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_global AS SELECT t1.id, t2.val FROM t1 GLOBAL JOIN t2 ON t1.fk = t2.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t2 VALUES (10, 100), (20, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 99)", schema_name=sn)

            def rows(view):
                vid = client.resolve_table(sn, view)[0]
                return sorted((r["id"], r["val"]) for r in _scan_dicts(client, vid))

            assert rows("v_plain") == [(1, 100), (2, 200)]
            assert rows("v_global") == rows("v_plain")
        finally:
            _cleanup(client, sn, tables=["t1", "t2"], views=["v_plain", "v_global"])

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
            pairs = {(r[0], r[1]) for r in client.scan(vid, include_hidden=True)}
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


class TestJoinFormDesugars:
    """The join spellings that carry their keys somewhere other than a literal
    `ON` clause: a comma-separated FROM keyed by the WHERE, `USING (c)`, and
    `NATURAL`. All three compile to the same equi-join, so each is checked by
    weight-multiset against the `ON` form it desugars to."""

    ROWS_T = [(i, i % 3, i % 2) for i in range(1, 13)]  # (id, k, a)
    ROWS_U = [(i, i % 3, 100 + i) for i in range(1, 13)]  # (id, k, w)

    def _setup(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL)",
            schema_name=sn,
        )

    def _insert(self, client, sn):
        client.execute_sql(
            "INSERT INTO t VALUES " + ",".join(f"({a},{b},{c})" for a, b, c in self.ROWS_T), schema_name=sn
        )
        client.execute_sql(
            "INSERT INTO u VALUES " + ",".join(f"({a},{b},{c})" for a, b, c in self.ROWS_U), schema_name=sn
        )

    def _expect_on_k(self):
        exp = Counter()
        for (ti, tk, _) in self.ROWS_T:
            for (ui, uk, _) in self.ROWS_U:
                if tk == uk:
                    exp[(ti, ui)] += 1
        return exp

    def test_comma_join_is_keyed_by_the_where(self, client):
        """`FROM a, b WHERE a.k = b.k` carries no ON at all: the equality reaches
        the join's key classification from the WHERE, which is the whole of what
        makes the comma form expressible."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT t.id AS tid, u.id AS uid FROM t, u WHERE t.k = u.k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            self._insert(client, sn)
            oracle.assert_view_matches(client, vid, ["tid", "uid"], self._expect_on_k())
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v"])

    def test_comma_join_without_a_predicate_is_refused(self, client):
        """No ON, no keying WHERE: a keyless product, which is not supported."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            with pytest.raises(gnitz.GnitzError, match="at least one equijoin or range predicate"):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT t.id AS tid, u.id AS uid FROM t, u", schema_name=sn
                )
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v"])

    def test_where_equality_becomes_a_second_key_column(self, client):
        """`ON p WHERE q` and `ON (p AND q)` are the same rows for an INNER join,
        and now the same plan: the WHERE conjunct is classified as a key rather
        than left as a post-join filter. Checked by weight, not row presence."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW split AS SELECT t.id AS tid, u.id AS uid FROM t JOIN u ON t.k = u.k WHERE t.a = u.w",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW fused AS SELECT t.id AS tid, u.id AS uid FROM t JOIN u ON t.k = u.k AND t.a = u.w",
                schema_name=sn,
            )
            ids = [client.resolve_table(sn, n)[0] for n in ("split", "fused")]
            self._insert(client, sn)
            exp = Counter()
            for (ti, tk, ta) in self.ROWS_T:
                for (ui, uk, uw) in self.ROWS_U:
                    if tk == uk and ta == uw:
                        exp[(ti, ui)] += 1
            for vid, name in zip(ids, ("split", "fused")):
                oracle.assert_view_matches(client, vid, ["tid", "uid"], exp, name)
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["split", "fused"])

    def test_where_conjunct_that_cannot_key_stays_a_filter(self, client):
        """A cross-table equality over FLOAT columns cannot be a join key. Written
        in the WHERE it must stay a residual filter, not turn a working query into
        an error — the promotion is a better plan, never a requirement."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE tf (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, f DOUBLE NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE uf (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, f DOUBLE NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT tf.id AS tid, uf.id AS uid FROM tf JOIN uf ON tf.k = uf.k WHERE tf.f = uf.f",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO tf VALUES (1, 1, 1.5), (2, 1, 2.5)", schema_name=sn)
            client.execute_sql("INSERT INTO uf VALUES (10, 1, 1.5), (11, 1, 9.5)", schema_name=sn)
            oracle.assert_view_matches(client, vid, ["tid", "uid"], Counter({(1, 10): 1}))
        finally:
            _cleanup(client, sn, tables=["tf", "uf"], views=["v"])

    def test_using_merges_the_named_column(self, client):
        """`USING (k)` equates the two copies and merges them into one output
        column: `SELECT *` emits `k` once, and the qualified `u.k` still reaches
        the right side's own copy."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT t.id AS tid, u.id AS uid FROM t JOIN u USING (k)", schema_name=sn
            )
            client.execute_sql("CREATE VIEW star AS SELECT * FROM t JOIN u USING (k)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW qual AS SELECT t.id AS tid, k AS merged, u.k AS right_k FROM t JOIN u USING (k)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            star = client.resolve_table(sn, "star")[0]
            qual = client.resolve_table(sn, "qual")[0]
            self._insert(client, sn)
            oracle.assert_view_matches(client, vid, ["tid", "uid"], self._expect_on_k())

            names = [c for c in client.scan(star).mappings()[0].keys()]
            assert names.count("k") == 1, f"USING merges `k` into one output column, got {names}"

            # The merged column and the right side's own copy are equal on every
            # matched row, which is what makes the merge a pass-through.
            for row in client.scan(qual).mappings():
                assert row["merged"] == row["right_k"], row
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v", "star", "qual"])

    def test_natural_join_uses_every_shared_name(self, client):
        """`t` and `u` share `id` and `k`, so NATURAL keys on both — not on `k`
        alone, which is what a `USING (k)` would have said."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT id AS shared_id, a, w FROM t NATURAL JOIN u", schema_name=sn
            )
            vid = client.resolve_table(sn, "v")[0]
            self._insert(client, sn)
            exp = Counter()
            for (ti, tk, ta) in self.ROWS_T:
                for (ui, uk, uw) in self.ROWS_U:
                    if ti == ui and tk == uk:
                        exp[(ti, ta, uw)] += 1
            oracle.assert_view_matches(client, vid, ["shared_id", "a", "w"], exp)
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v"])

    def test_left_join_using_null_fills_and_keeps_the_left_copy(self, client):
        """The merged column's value is the PRESERVED side's copy, so an unmatched
        left row carries its own `k` while every `u` column is null-filled."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT t.id AS tid, k AS merged, u.w AS w FROM t LEFT JOIN u USING (k)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO t VALUES (1, 7, 0), (2, 8, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO u VALUES (10, 7, 70)", schema_name=sn)
            oracle.assert_view_matches(
                client, vid, ["tid", "merged", "w"], Counter({(1, 7, 70): 1, (2, 8, None): 1})
            )
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v"])

    def test_full_join_column_merge_is_refused(self, client):
        """FULL preserves both sides, so its merged column would be
        COALESCE(l, r) — a computed expression a join projection cannot carry."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            for sql in (
                "CREATE VIEW v AS SELECT t.id AS tid FROM t FULL OUTER JOIN u USING (k)",
                "CREATE VIEW v AS SELECT t.id AS tid FROM t NATURAL FULL OUTER JOIN u",
            ):
                with pytest.raises(gnitz.GnitzError, match="COALESCE"):
                    client.execute_sql(sql, schema_name=sn)
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v"])

    def test_natural_join_without_a_shared_name_is_refused(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE TABLE r (rid BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn
            )
            with pytest.raises(gnitz.GnitzError, match="share no column name"):
                client.execute_sql("CREATE VIEW v AS SELECT x, y FROM l NATURAL JOIN r", schema_name=sn)
        finally:
            _cleanup(client, sn, tables=["l", "r"], views=["v"])

    def test_using_names_a_column_neither_side_has(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            with pytest.raises(gnitz.GnitzError, match="JOIN USING: column 'nope' not found"):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT t.id AS tid FROM t JOIN u USING (nope)", schema_name=sn
                )
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v"])


class TestCommaJoinSpine:
    """A comma-separated FROM folds left-deep, so every step's key has to be
    found in one WHERE sitting above the whole spine — including the keys of
    steps the top join cannot see across its own two sides."""

    def _setup(self, client, sn):
        for t in ("a", "b", "c"):
            client.execute_sql(
                f"CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, j BIGINT NOT NULL)",
                schema_name=sn,
            )

    def _rows(self):
        return {
            "a": [(1, 1, 0), (2, 2, 0), (3, 1, 0)],
            "b": [(10, 1, 5), (11, 2, 6), (12, 2, 5)],
            "c": [(20, 0, 5), (21, 0, 6)],
        }

    def _insert(self, client, sn):
        for t, rows in self._rows().items():
            client.execute_sql(
                f"INSERT INTO {t} VALUES " + ",".join(f"({i},{k},{j})" for i, k, j in rows), schema_name=sn
            )

    def _expect(self):
        r = self._rows()
        exp = Counter()
        for (ai, ak, _) in r["a"]:
            for (bi, bk, bj) in r["b"]:
                if ak != bk:
                    continue
                for (ci, _, cj) in r["c"]:
                    if bj == cj:
                        exp[(ai, bi, ci)] += 1
        return exp

    def test_three_way_comma_join(self, client):
        """`a.k = b.k` keys the inner `(a, b)` step; the outer step sees only
        `b.j = c.j` across its own sides."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS ai, b.id AS bi, c.id AS ci "
                "FROM a, b, c WHERE a.k = b.k AND b.j = c.j",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            self._insert(client, sn)
            oracle.assert_view_matches(client, vid, ["ai", "bi", "ci"], self._expect())
        finally:
            _cleanup(client, sn, tables=["a", "b", "c"], views=["v"])

    def test_three_way_comma_join_matches_the_explicit_form(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW explicit AS SELECT a.id AS ai, b.id AS bi, c.id AS ci "
                "FROM a JOIN b ON a.k = b.k JOIN c ON b.j = c.j",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW commaform AS SELECT a.id AS ai, b.id AS bi, c.id AS ci "
                "FROM a, b, c WHERE a.k = b.k AND b.j = c.j",
                schema_name=sn,
            )
            ids = {n: client.resolve_table(sn, n)[0] for n in ("explicit", "commaform")}
            self._insert(client, sn)
            for name, vid in ids.items():
                oracle.assert_view_matches(client, vid, ["ai", "bi", "ci"], self._expect(), name)
        finally:
            _cleanup(client, sn, tables=["a", "b", "c"], views=["explicit", "commaform"])

    def test_mixed_comma_and_explicit_join(self, client):
        """The comma binds loosest, so `FROM a JOIN b ON …, c` is `((a ⋈ b) , c)`
        and the WHERE keys only the outer step."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS ai, b.id AS bi, c.id AS ci "
                "FROM a JOIN b ON a.k = b.k, c WHERE b.j = c.j",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            self._insert(client, sn)
            oracle.assert_view_matches(client, vid, ["ai", "bi", "ci"], self._expect())
        finally:
            _cleanup(client, sn, tables=["a", "b", "c"], views=["v"])

    def test_a_left_only_filter_still_applies_when_pushed_down(self, client):
        """A conjunct naming only the left input is pushed to the step below, so
        the rows it removes must be gone from the result all the same."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS ai, b.id AS bi, c.id AS ci "
                "FROM a, b, c WHERE a.k = b.k AND b.j = c.j AND a.id > 1",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            self._insert(client, sn)
            exp = Counter({k: w for k, w in self._expect().items() if k[0] > 1})
            oracle.assert_view_matches(client, vid, ["ai", "bi", "ci"], exp)
        finally:
            _cleanup(client, sn, tables=["a", "b", "c"], views=["v"])

    def test_comma_join_inside_a_derived_table_and_a_cte(self, client):
        """Both wrap a body through the same bind, so the comma form reaches them
        without a rule of its own."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW d AS SELECT ai, bi FROM "
                "(SELECT a.id AS ai, b.id AS bi FROM a, b WHERE a.k = b.k) x",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW w AS WITH x AS (SELECT a.id AS ai, b.id AS bi FROM a, b WHERE a.k = b.k) "
                "SELECT ai, bi FROM x",
                schema_name=sn,
            )
            ids = {n: client.resolve_table(sn, n)[0] for n in ("d", "w")}
            self._insert(client, sn)
            r = self._rows()
            exp = Counter()
            for (ai, ak, _) in r["a"]:
                for (bi, bk, _) in r["b"]:
                    if ak == bk:
                        exp[(ai, bi)] += 1
            for name, vid in ids.items():
                oracle.assert_view_matches(client, vid, ["ai", "bi"], exp, name)
        finally:
            _cleanup(client, sn, tables=["a", "b", "c"], views=["d", "w"])


class TestJoinDesugarEdges:
    """The orientations and compositions the first pass of the desugars did not
    exercise: the mirror of LEFT, a merge chained across three relations, a
    grouped body reading a merged column, and the range-only comma join."""

    def _setup(self, client, sn, tables=("t", "u")):
        for name in tables:
            client.execute_sql(
                f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
                schema_name=sn,
            )

    def test_right_join_using_keeps_the_right_copy(self, client):
        """The merged column is the PRESERVED side's, so for RIGHT it is the right
        input's — an unmatched right row carries its own `k` while every left
        column is null-filled. The mirror of the LEFT case, and the one that would
        break if the merge always kept the left."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT u.id AS uid, k AS merged, t.v AS tv FROM t RIGHT JOIN u USING (k)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO t VALUES (1, 7, 70)", schema_name=sn)
            client.execute_sql("INSERT INTO u VALUES (10, 7, 0), (11, 8, 0)", schema_name=sn)
            oracle.assert_view_matches(
                client, vid, ["uid", "merged", "tv"], Counter({(10, 7, 70): 1, (11, 8, None): 1})
            )
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v"])

    def test_natural_join_chains_across_three_relations(self, client):
        """The second step intersects against the *visible* left names, so the
        column the first step merged away cannot pair a second time."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn, ("a", "b", "c"))
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, k, v FROM a NATURAL JOIN b NATURAL JOIN c", schema_name=sn
            )
            vid = client.resolve_table(sn, "v")[0]
            for name in ("a", "b", "c"):
                client.execute_sql(f"INSERT INTO {name} VALUES (1, 5, 9), (2, 6, 9)", schema_name=sn)
            # All three share every column name, so NATURAL keys on all three and
            # the output is one row per fully-matching triple.
            oracle.assert_view_matches(client, vid, ["id", "k", "v"], Counter({(1, 5, 9): 1, (2, 6, 9): 1}))
            names = list(client.scan(vid).mappings()[0].keys())
            assert sorted(names) == ["id", "k", "v"], f"each shared name survives once, got {names}"
        finally:
            _cleanup(client, sn, tables=["a", "b", "c"], views=["v"])

    def test_grouped_body_over_a_merged_column(self, client):
        """A GROUP BY names the merged column, which resolves through the same
        scope the projection does — the grouped tail takes no separate route."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT k, COUNT(*) AS n FROM t JOIN u USING (k) GROUP BY k", schema_name=sn
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO t VALUES (1, 5, 0), (2, 5, 0), (3, 6, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO u VALUES (10, 5, 0), (11, 6, 0), (12, 6, 0)", schema_name=sn)
            # k=5: 2 t-rows × 1 u-row = 2; k=6: 1 × 2 = 2.
            oracle.assert_view_matches(client, vid, ["k", "n"], Counter({(5, 2): 1, (6, 2): 1}))
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v"])

    def test_range_only_comma_join(self, client):
        """A comma join whose WHERE carries only a range comparison keys on the
        range slot alone — `n_eq == 0 && has_range` satisfies the arity rule — and
        takes the broadcast pure-range path. Checked by weight at W=4, since that
        path replicates one side."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT t.id AS tid, u.id AS uid FROM t, u WHERE t.v < u.v", schema_name=sn
            )
            vid = client.resolve_table(sn, "v")[0]
            rt = [(i, 0, i) for i in range(1, 7)]
            ru = [(i, 0, 2 * i) for i in range(1, 7)]
            client.execute_sql(
                "INSERT INTO t VALUES " + ",".join(f"({a},{b},{c})" for a, b, c in rt), schema_name=sn
            )
            client.execute_sql(
                "INSERT INTO u VALUES " + ",".join(f"({a},{b},{c})" for a, b, c in ru), schema_name=sn
            )
            exp = Counter()
            for (ti, _, tv) in rt:
                for (ui, _, uv) in ru:
                    if tv < uv:
                        exp[(ti, ui)] += 1
            oracle.assert_view_matches(client, vid, ["tid", "uid"], exp)
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v"])

    def test_using_on_a_name_the_left_carries_twice_is_ambiguous(self, client):
        """Three relations, all with `k`: by the third step the left side has two
        visible `k`, and `USING (k)` names neither. The error has to say
        *ambiguous* — "not found" would send the reader looking for a column that
        is there twice."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn, ("a", "b", "c"))
            with pytest.raises(gnitz.GnitzError, match="'k' is ambiguous"):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT a.id AS ai FROM a JOIN b ON a.id = b.id JOIN c USING (k)",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, tables=["a", "b", "c"], views=["v"])

    def test_a_merged_name_is_no_longer_ambiguous(self, client):
        """The mirror of the case above: once the first step MERGED `k`, the left
        side carries one visible `k`, so the third relation pairs with it."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn, ("a", "b", "c"))
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS ai, c.id AS ci FROM a JOIN b USING (k) JOIN c USING (k)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            for name in ("a", "b", "c"):
                client.execute_sql(f"INSERT INTO {name} VALUES (1, 5, 0), (2, 6, 0)", schema_name=sn)
            exp = Counter()
            for ai, ak in [(1, 5), (2, 6)]:
                for _bi, bk in [(1, 5), (2, 6)]:
                    for ci, ck in [(1, 5), (2, 6)]:
                        if ak == bk and ak == ck:
                            exp[(ai, ci)] += 1
            oracle.assert_view_matches(client, vid, ["ai", "ci"], exp)
        finally:
            _cleanup(client, sn, tables=["a", "b", "c"], views=["v"])

    def test_self_join_with_using(self, client):
        """The same relation on both sides: the merge resolves its names before
        the right alias enters the scope, and the lowering's self-collision
        wrapper still sees two distinct sources."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT x.id AS xid, y.id AS yid FROM t x JOIN t y USING (k)", schema_name=sn
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO t VALUES (1, 5, 0), (2, 5, 0), (3, 6, 0)", schema_name=sn)
            rows = [(1, 5), (2, 5), (3, 6)]
            exp = Counter()
            for (xi, xk) in rows:
                for (yi, yk) in rows:
                    if xk == yk:
                        exp[(xi, yi)] += 1
            oracle.assert_view_matches(client, vid, ["xid", "yid"], exp)
        finally:
            _cleanup(client, sn, tables=["t"], views=["v"])

    def test_promotion_across_a_null_filled_left_input(self, client):
        """The subtlest promotion: an INNER step whose left is a LEFT JOIN, and a
        WHERE conjunct spanning the null-fillable side and the right relation.
        Promoting it to a key changes how a NULL is handled — a NULL equi-join key
        matches nothing, where a residual `NULL = x` is filtered — so the two must
        agree, and the rows a null-fill produced must not leak."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn, ("a", "b", "c"))
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS ai, c.id AS ci "
                "FROM a LEFT JOIN b ON a.k = b.k JOIN c ON a.id = c.id WHERE b.v = c.v",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # a=1 matches b (v=9); a=2 matches no b, so b.v is null-filled.
            client.execute_sql("INSERT INTO a VALUES (1, 5, 0), (2, 99, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (10, 5, 9)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (1, 0, 9), (2, 0, 9)", schema_name=sn)
            # Only a=1 has a non-NULL b.v, and it equals c.v for c.id = 1.
            # a=2's null-filled b.v must match nothing at all.
            oracle.assert_view_matches(client, vid, ["ai", "ci"], Counter({(1, 1): 1}))
        finally:
            _cleanup(client, sn, tables=["a", "b", "c"], views=["v"])

    def test_using_merges_several_columns_at_once(self, client):
        """`USING (k, v)` is two key pairs and two merges in one step — the loop,
        not the single-name case every other test exercises."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT t.id AS tid, u.id AS uid, k, v FROM t JOIN u USING (k, v)",
                schema_name=sn,
            )
            star = "CREATE VIEW star AS SELECT * FROM t JOIN u USING (k, v)"
            client.execute_sql(star, schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            sid = client.resolve_table(sn, "star")[0]
            rt = [(1, 5, 50), (2, 5, 51), (3, 6, 60)]
            ru = [(10, 5, 50), (11, 6, 60), (12, 6, 61)]
            client.execute_sql(
                "INSERT INTO t VALUES " + ",".join(f"({a},{b},{c})" for a, b, c in rt), schema_name=sn
            )
            client.execute_sql(
                "INSERT INTO u VALUES " + ",".join(f"({a},{b},{c})" for a, b, c in ru), schema_name=sn
            )
            exp = Counter()
            for (ti, tk, tv) in rt:
                for (ui, uk, uv) in ru:
                    if tk == uk and tv == uv:
                        exp[(ti, ui, tk, tv)] += 1
            oracle.assert_view_matches(client, vid, ["tid", "uid", "k", "v"], exp)
            names = list(client.scan(sid).mappings()[0].keys())
            assert names.count("k") == 1 and names.count("v") == 1, f"both merge once, got {names}"
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v", "star"])

    def test_using_over_a_view_and_a_cte(self, client):
        """The merge resolves names off whatever `resolve_table_factor` handed the
        scope, so a view or a CTE pairs exactly as a base table does."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("CREATE VIEW tv AS SELECT id, k FROM t", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS WITH cu AS (SELECT id AS uid, k FROM u) "
                "SELECT tv.id AS tid, cu.uid AS uid, k FROM tv JOIN cu USING (k)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO t VALUES (1, 5, 0), (2, 6, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO u VALUES (10, 5, 0), (11, 7, 0)", schema_name=sn)
            oracle.assert_view_matches(client, vid, ["tid", "uid", "k"], Counter({(1, 10, 5): 1}))
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v", "tv"])

    def test_distinct_over_a_merged_column(self, client):
        """The DISTINCT tail takes its own lowering arm; it reads the same merged
        scope the grouped and plain projections do."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT DISTINCT k FROM t JOIN u USING (k)", schema_name=sn
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO t VALUES (1, 5, 0), (2, 5, 0), (3, 6, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO u VALUES (10, 5, 0), (11, 5, 0), (12, 6, 0)", schema_name=sn)
            # k=5 pairs 2×2 and k=6 pairs 1×1; DISTINCT collapses each to weight 1.
            oracle.assert_view_matches(client, vid, ["k"], Counter({(5,): 1, (6,): 1}))
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v"])

    def test_comma_join_as_a_set_operation_side(self, client):
        """A set-op side binds as any relational body, so the comma form reaches
        it for the same reason it reaches a derived table and a CTE — no rule of
        its own, and none needed."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT t.id AS x FROM t, u WHERE t.k = u.k "
                "UNION ALL SELECT id AS x FROM u",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO t VALUES (1, 5, 0), (2, 9, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO u VALUES (10, 5, 0)", schema_name=sn)
            # left branch: t.id=1 pairs with u; right branch: u.id=10.
            oracle.assert_view_matches(client, vid, ["x"], Counter({(1,): 1, (10,): 1}))
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["v"])

    def test_cross_join_is_the_comma_join_spelled_out(self, client):
        """`CROSS JOIN` states no keys of its own, exactly as the comma does — so
        a WHERE keys it the same way, and the two spellings of one query cannot
        disagree. Bare, with nothing to key it, both are still refused by the one
        arity rule."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW x AS SELECT t.id AS tid, u.id AS uid FROM t CROSS JOIN u WHERE t.k = u.k",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW c AS SELECT t.id AS tid, u.id AS uid FROM t, u WHERE t.k = u.k",
                schema_name=sn,
            )
            ids = {n: client.resolve_table(sn, n)[0] for n in ("x", "c")}
            client.execute_sql("INSERT INTO t VALUES (1, 5, 0), (2, 6, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO u VALUES (10, 5, 0), (11, 5, 0)", schema_name=sn)
            exp = Counter({(1, 10): 1, (1, 11): 1})
            for name, vid in ids.items():
                oracle.assert_view_matches(client, vid, ["tid", "uid"], exp, name)

            # Bare, both spellings hit the same rule with the same message.
            for body in ("FROM t CROSS JOIN u", "FROM t, u"):
                with pytest.raises(gnitz.GnitzError, match="at least one equijoin or range predicate"):
                    client.execute_sql(
                        f"CREATE VIEW bad AS SELECT t.id AS tid {body}", schema_name=sn
                    )
        finally:
            _cleanup(client, sn, tables=["t", "u"], views=["x", "c", "bad"])
