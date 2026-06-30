"""E2E tests for RIGHT and FULL OUTER JOIN (equi + band).

RIGHT/FULL fall out of LEFT by symmetry: RIGHT emits the mirror null-fill `ν_B`
(unmatched right rows, NULL-filled on the left columns), FULL emits both `ν_A` and
`ν_B`. Pure-range RIGHT/FULL is rejected at the planner — see
crates/gnitz-sql/tests/planner_join.rs.

Assertions are per-row `.weight`, not just row presence (a Z-set engine's
correctness lives in the weights).

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_right_full_join.py -v --tb=short
"""
import random
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _pos(client, vid):
    """Positive-weight rows as a list of gnitz rows."""
    return [r for r in client.scan(vid) if r.weight > 0]


def _weighted(client, vid):
    """All rows (including retractions) as (dict, weight) pairs."""
    return [(r._asdict(), r.weight) for r in client.scan(vid)]


class TestEquiRightFull:
    def _setup(self, client, sn):
        # orders.customer_id is the (nullable-free) left join key; customers.pk the
        # right key. Both payloads are NOT NULL so the nullability widening is
        # observable (a right-only row decodes NULL orders without a panic).
        client.execute_sql(
            "CREATE TABLE orders (pk BIGINT NOT NULL PRIMARY KEY, "
            "customer_id BIGINT NOT NULL, amount BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE customers (pk BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL)",
            schema_name=sn,
        )

    def _view(self, client, sn, jt):
        client.execute_sql(
            f"CREATE VIEW v AS SELECT customers.pk AS cust, customers.name AS cname, "
            f"orders.pk AS oid, orders.amount AS amt "
            f"FROM orders {jt} JOIN customers ON orders.customer_id = customers.pk",
            schema_name=sn,
        )
        return client.resolve_table(sn, "v")[0]

    def test_right_join_match_and_nullfill(self, client):
        """RIGHT preserves the right (customers): a matched customer carries the
        order payload; an unmatched customer is NULL-filled on the left columns; an
        unmatched left (order) row is dropped."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            vid = self._view(client, sn, "RIGHT")
            client.execute_sql("INSERT INTO customers VALUES (100, 1), (200, 2)", schema_name=sn)
            # order 1 → customer 100 (match); order 3 → customer 999 (no customer, dropped).
            client.execute_sql("INSERT INTO orders VALUES (1, 100, 50), (3, 999, 70)", schema_name=sn)

            rows = _pos(client, vid)
            by_cust = {r["cust"]: r for r in rows}
            assert set(by_cust) == {100, 200}, f"customer 999's order is dropped (RIGHT): {rows}"
            assert all(r.weight == 1 for r in rows), f"weights: {[(r['cust'], r.weight) for r in rows]}"
            # Customer 100 matched → order payload present.
            assert by_cust[100]["oid"] == 1 and by_cust[100]["amt"] == 50
            # Customer 200 unmatched → NULL left columns.
            assert by_cust[200]["oid"] is None and by_cust[200]["amt"] is None
        finally:
            _drop(client, sn, ["orders", "customers"], ["v"])

    def test_right_join_many_to_one_weights(self, client):
        """A right row matched by multiple left rows yields one inner row per match
        (weight 1 each); an unmatched right row null-fills exactly once."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            vid = self._view(client, sn, "RIGHT")
            client.execute_sql("INSERT INTO customers VALUES (100, 1), (200, 2)", schema_name=sn)
            # customer 100 matched by orders 1 AND 2; customer 200 unmatched.
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 100, 50), (2, 100, 60), (3, 999, 70)", schema_name=sn
            )
            rows = _pos(client, vid)
            # 2 inner rows (customer 100 × orders 1,2) + 1 null-fill (customer 200) = 3.
            assert len(rows) == 3, f"expected 3, got {rows}"
            assert all(r.weight == 1 for r in rows)
            matched_100 = sorted(r["oid"] for r in rows if r["cust"] == 100)
            assert matched_100 == [1, 2], "customer 100 matched by both orders"
            nullfill = [r for r in rows if r["cust"] == 200]
            assert len(nullfill) == 1 and nullfill[0]["amt"] is None, "customer 200 null-fills once"
        finally:
            _drop(client, sn, ["orders", "customers"], ["v"])

    def test_full_join_both_directions(self, client):
        """FULL = inner ∪ ν_A ∪ ν_B: a matched pair, a left-only row (NULL right), and
        a right-only row (NULL left), each at weight 1."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            vid = self._view(client, sn, "FULL")
            client.execute_sql("INSERT INTO customers VALUES (100, 1), (200, 2)", schema_name=sn)
            # order 1 → customer 100 (match); order 3 → customer 999 (left-only).
            client.execute_sql("INSERT INTO orders VALUES (1, 100, 50), (3, 999, 70)", schema_name=sn)

            rows = _pos(client, vid)
            assert all(r.weight == 1 for r in rows), f"weights: {rows}"
            # Matched: customer 100 + order 1.
            matched = [r for r in rows if r["cust"] == 100 and r["oid"] == 1]
            assert len(matched) == 1 and matched[0]["amt"] == 50
            # Right-only: customer 200, NULL order.
            right_only = [r for r in rows if r["cust"] == 200]
            assert len(right_only) == 1 and right_only[0]["oid"] is None
            # Left-only: order 3 (customer_id 999), NULL customer.
            left_only = [r for r in rows if r["oid"] == 3]
            assert len(left_only) == 1 and left_only[0]["cust"] is None and left_only[0]["cname"] is None
            assert len(rows) == 3, f"inner ∪ ν_A ∪ ν_B = 3 rows, got {rows}"
        finally:
            _drop(client, sn, ["orders", "customers"], ["v"])

    def test_right_join_retraction(self, client):
        """Deleting a matching left row re-emits the right row's null-fill once;
        deleting the right row retracts both inner and null-fill contributions."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            vid = self._view(client, sn, "RIGHT")
            client.execute_sql("INSERT INTO customers VALUES (100, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO orders VALUES (1, 100, 50)", schema_name=sn)
            rows = _pos(client, vid)
            assert len(rows) == 1 and rows[0]["amt"] == 50, "matched inner row"

            # Delete the order → customer 100 becomes unmatched → null-fill appears once.
            client.execute_sql("DELETE FROM orders WHERE pk = 1", schema_name=sn)
            rows = _pos(client, vid)
            assert len(rows) == 1 and rows[0]["amt"] is None, f"null-fill re-emitted: {rows}"

            # Delete the customer → nothing left.
            client.execute_sql("DELETE FROM customers WHERE pk = 100", schema_name=sn)
            assert _pos(client, vid) == [], "right row gone → view empty"
        finally:
            _drop(client, sn, ["orders", "customers"], ["v"])

    def test_right_join_nullable_right_key_null_row(self, client):
        """A right row whose (nullable) join key is NULL matches nothing (SQL 3VL) and
        still null-fills, via b_all re-keying the unfiltered right input."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE l (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lv BIGINT NOT NULL)",
                schema_name=sn,
            )
            # r.k is the nullable right join key.
            client.execute_sql(
                "CREATE TABLE r (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT, rv BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT r.pk AS rid, r.rv AS rv, l.lv AS lv "
                "FROM l RIGHT JOIN r ON l.k = r.k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO l VALUES (1, 7, 70)", schema_name=sn)
            # r1 matches (k=7); r2 has a NULL key → never matches → null-fills.
            client.execute_sql("INSERT INTO r VALUES (1, 7, 1), (2, NULL, 2)", schema_name=sn)
            rows = _pos(client, vid)
            by_rid = {r["rid"]: r for r in rows}
            assert all(r.weight == 1 for r in rows)
            assert by_rid[1]["lv"] == 70, "r1 matched l"
            assert by_rid[2]["lv"] is None, "r2 (NULL key) null-fills"
        finally:
            _drop(client, sn, ["l", "r"], ["v"])

    def test_right_join_bag_valued_right_view(self, client):
        """RIGHT over a bag-valued (non-unique) right side — a UNION ALL view whose two
        source rows project to the same key, summing to weight 2 on one identity. The
        weight-exact positive_part subtracts the RAW matched multiplicity, so a matched
        weight-2 right row produces NO spurious null-fill (the retired distinct form
        would leak a weight-1 fill); an unmatched weight-2 right row null-fills at
        weight 2."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE src (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE other (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
                schema_name=sn,
            )
            # u groups by content hash of g: two src rows with g=5 collapse to one
            # identity with weight 2 (UNION ALL does not dedup).
            client.execute_sql(
                "CREATE VIEW u AS SELECT g FROM src UNION ALL SELECT g FROM other",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE lt (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT u.g AS g, lt.k AS k FROM lt RIGHT JOIN u ON lt.k = u.g",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # src has two g=5 rows → u has (g=5) at weight 2. lt has one k=5 → matches.
            client.execute_sql("INSERT INTO src VALUES (1, 5), (2, 5)", schema_name=sn)
            client.execute_sql("INSERT INTO lt VALUES (1, 5)", schema_name=sn)
            rows = _weighted(client, vid)
            # All output rows are matched (k=5 present): two inner rows (weight 2 from the
            # bag), zero null-fill. No row may carry a NULL k.
            assert all(d["k"] is not None for (d, w) in rows if w > 0), (
                f"matched weight-2 right row must NOT leak a null-fill: {rows}"
            )
            total = sum(w for (d, w) in rows)
            assert total == 2, f"two matched contributions (bag weight 2), got {total}: {rows}"

            # Now make it unmatched: delete the left row → u's g=5 (weight 2) null-fills.
            client.execute_sql("DELETE FROM lt WHERE pk = 1", schema_name=sn)
            rows = _pos(client, vid)
            assert len(rows) == 1 and rows[0]["k"] is None and rows[0].weight == 2, (
                f"unmatched bag-valued right row null-fills at weight 2: {rows}"
            )
        finally:
            _drop(client, sn, ["lt", "src", "other"], ["v", "u"])


class TestBandRightFull:
    def _setup(self, client, sn):
        # a.k = b.k AND a.lo <= b.t : band join (n_eq = 1).
        client.execute_sql(
            "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
            schema_name=sn,
        )

    def _view(self, client, sn, jt):
        client.execute_sql(
            f"CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
            f"FROM a {jt} JOIN b ON a.k = b.k AND a.lo <= b.t",
            schema_name=sn,
        )
        return client.resolve_table(sn, "v")[0]

    def test_band_right_join(self, client):
        """Band RIGHT preserves b: a b-row with a qualifying a is matched; one with no
        qualifying a null-fills (NULL aid)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            vid = self._view(client, sn, "RIGHT")
            # a: (id, k, lo). b: (id, k, t). Band a.k=b.k AND a.lo<=b.t.
            client.execute_sql("INSERT INTO a VALUES (1, 7, 10), (2, 7, 100)", schema_name=sn)
            # b1 (k=7,t=50): matched by a1 (lo=10<=50), not a2 (lo=100>50).
            # b2 (k=7,t=5):  matched by neither (10>5, 100>5) → null-fill.
            # b3 (k=9,t=99): different k → null-fill.
            client.execute_sql("INSERT INTO b VALUES (1, 7, 50), (2, 7, 5), (3, 9, 99)", schema_name=sn)
            rows = _pos(client, vid)
            assert all(r.weight == 1 for r in rows), f"weights: {rows}"
            by_bid = {}
            for r in rows:
                by_bid.setdefault(r["bid"], []).append(r["aid"])
            assert sorted(by_bid[1]) == [1], "b1 matched only by a1"
            assert by_bid[2] == [None], "b2 unmatched → null-fill"
            assert by_bid[3] == [None], "b3 (other k) unmatched → null-fill"
        finally:
            _drop(client, sn, ["a", "b"], ["v"])

    def test_band_full_join(self, client):
        """Band FULL: a left-only a (no qualifying b), a right-only b (no qualifying a),
        and a matched pair, each weight 1."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            vid = self._view(client, sn, "FULL")
            # a1 (k=7,lo=10) matched by b1 (t=50). a2 (k=8,lo=10) has no b → left-only.
            client.execute_sql("INSERT INTO a VALUES (1, 7, 10), (2, 8, 10)", schema_name=sn)
            # b1 (k=7,t=50) matched by a1. b2 (k=9,t=50) has no a → right-only.
            client.execute_sql("INSERT INTO b VALUES (1, 7, 50), (2, 9, 50)", schema_name=sn)
            rows = _pos(client, vid)
            assert all(r.weight == 1 for r in rows), f"weights: {rows}"
            pairs = {(r["aid"], r["bid"]) for r in rows}
            assert pairs == {(1, 1), (2, None), (None, 2)} and len(rows) == 3, (
                f"inner ∪ ν_A ∪ ν_B, one row each: {[(r['aid'], r['bid'], r.weight) for r in rows]}"
            )
        finally:
            _drop(client, sn, ["a", "b"], ["v"])

    def test_band_full_pair_pk_zero_both_sides(self, client):
        """FULL band with a left row a.id=0 and a right row b.id=0 that match nothing:
        both produce pair-PK (0, 0) (the absent side's PK packs to the synthetic 0) but
        are TWO distinct (PK, payload) elements (the null bitmaps differ). Both must
        survive consolidation — a join view is not unique_pk."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            vid = self._view(client, sn, "FULL")
            # a.id=0, k=1: no b with k=1 → left-only, pair-PK (a.id=0, b.id→0) = (0,0).
            client.execute_sql("INSERT INTO a VALUES (0, 1, 10)", schema_name=sn)
            # b.id=0, k=2: no a with k=2 → right-only, pair-PK (a.id→0, b.id=0) = (0,0).
            client.execute_sql("INSERT INTO b VALUES (0, 2, 50)", schema_name=sn)
            rows = _pos(client, vid)
            assert all(r.weight == 1 for r in rows), f"weights: {rows}"
            pairs = {(r["aid"], r["bid"]) for r in rows}
            assert pairs == {(0, None), (None, 0)} and len(rows) == 2, (
                f"two distinct rows both at pair-PK (0,0) must both survive: "
                f"{[(r['aid'], r['bid'], r.weight) for r in rows]}"
            )
        finally:
            _drop(client, sn, ["a", "b"], ["v"])

    def test_band_right_join_duplicate_payload_weights(self, client):
        """Re-key fidelity: a band RIGHT where b rows share a payload but differ in PK,
        matched by multiple a rows — every (a,b) pair is one weight-1 row, no over/under
        counting from the pair-PK re-key."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            vid = self._view(client, sn, "RIGHT")
            # a1,a2 both (k=7, lo=10). b1,b2 both (k=7, t=50) — duplicate payloads.
            client.execute_sql("INSERT INTO a VALUES (1, 7, 10), (2, 7, 10)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 7, 50), (2, 7, 50)", schema_name=sn)
            rows = _pos(client, vid)
            # 2 b-rows × 2 qualifying a-rows = 4 matched pairs, each weight 1.
            assert all(r.weight == 1 for r in rows), f"weights: {rows}"
            pairs = sorted((r["aid"], r["bid"]) for r in rows)
            assert pairs == [(1, 1), (1, 2), (2, 1), (2, 2)], f"all 4 pairs once: {pairs}"
        finally:
            _drop(client, sn, ["a", "b"], ["v"])


def _drop(client, sn, tables, views):
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
    try:
        client.drop_schema(sn)
    except Exception:
        pass
