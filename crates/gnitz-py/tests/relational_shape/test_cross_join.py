"""E2E tests: the keyless (cross) join in CREATE VIEW.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/relational_shape/test_cross_join.py -v --tb=short

A cross join broadcasts each delta and pairs it, on every worker, with that
worker's slice of the other side, so every check here is a weight-multiset
comparison: a row-set check would pass a W× duplication silently.
"""
from collections import Counter

import gnitz
import _oracle as oracle
from _serverproc import NEEDS_MULTI
from _uid import uid as _uid

def _cleanup(client, sn):
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _product(rows_t, rows_u, pred=None):
    return Counter({(t[0], u[0]): 1 for t in rows_t for u in rows_u if pred is None or pred(t, u)})


def _vid(client, sn, name):
    return client.resolve_table(sn, name)[0]


class TestCrossJoin:
    ROWS_T = [(i, i % 3) for i in range(1, 8)]  # (id, v)
    ROWS_U = [(10 * i, i % 2) for i in range(1, 6)]  # (id, w)

    def _setup(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn
        )
        client.execute_sql(
            "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL)", schema_name=sn
        )

    def _insert(self, client, sn, rows_t=None, rows_u=None):
        rows_t = self.ROWS_T if rows_t is None else rows_t
        rows_u = self.ROWS_U if rows_u is None else rows_u
        if rows_t:
            client.execute_sql(
                "INSERT INTO t VALUES " + ",".join(f"({a},{b})" for a, b in rows_t), schema_name=sn
            )
        if rows_u:
            client.execute_sql(
                "INSERT INTO u VALUES " + ",".join(f"({a},{b})" for a, b in rows_u), schema_name=sn
            )

    def test_every_keyless_spelling_is_the_product(self, client):
        """`CROSS JOIN`, the comma, and a `JOIN … ON` whose predicate compares no
        column across the two sides are one keyless INNER step; a residual on
        it filters the product."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            cases = {
                "cx": ("FROM t CROSS JOIN u", None),
                "cm": ("FROM t, u", None),
                "c1": ("FROM t INNER JOIN u ON 1 = 1", None),
                "cr": ("FROM t JOIN u ON t.v <> u.w", lambda t, u: t[1] != u[1]),
                "cw": ("FROM t, u WHERE t.v <> u.w", lambda t, u: t[1] != u[1]),
                "cf": ("FROM t CROSS JOIN u WHERE t.v = 0", lambda t, u: t[1] == 0),
            }
            for name, (body, _) in cases.items():
                client.execute_sql(
                    f"CREATE VIEW {name} AS SELECT t.id AS tid, u.id AS uid {body}", schema_name=sn
                )
            self._insert(client, sn)
            for name, (_, pred) in cases.items():
                exp = _product(self.ROWS_T, self.ROWS_U, pred)
                oracle.assert_view_matches(client, _vid(client, sn, name), ["tid", "uid"], exp, name)
        finally:
            _cleanup(client, sn)

    def test_star_projects_both_sides_and_no_pair_pk(self, client):
        """`SELECT *` over a cross join yields every user column of both sides
        and none of the hidden pair-PK slots."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (tid BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE TABLE u (uid BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL)", schema_name=sn
            )
            client.execute_sql("CREATE VIEW v AS SELECT * FROM t CROSS JOIN u", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 6)", schema_name=sn)
            client.execute_sql("INSERT INTO u VALUES (7, 'x'), (8, 'yy')", schema_name=sn)
            vid = _vid(client, sn, "v")
            rows = client.scan(vid).mappings()
            assert rows and all(set(r) == {"tid", "v", "uid", "name"} for r in rows), rows
            exp = Counter({(1, 5, 7, "x"): 1, (1, 5, 8, "yy"): 1, (2, 6, 7, "x"): 1, (2, 6, 8, "yy"): 1})
            oracle.assert_view_matches(client, vid, ["tid", "v", "uid", "name"], exp)
        finally:
            _cleanup(client, sn)

    def test_incremental_changes_on_both_sides(self, client):
        """Each delta, on either side, adds or retracts exactly the pairs it
        forms with the other side's current rows: insert, delete and update
        (a retraction plus an insertion under one PK) on each side in turn."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT t.id AS tid, t.v AS tv, u.id AS uid, u.w AS uw FROM t CROSS JOIN u",
                schema_name=sn,
            )
            vid = _vid(client, sn, "v")
            t = {}
            u = {}

            def check(ctx):
                exp = Counter({(ti, tv, ui, uw): 1 for ti, tv in t.items() for ui, uw in u.items()})
                oracle.assert_view_matches(client, vid, ["tid", "tv", "uid", "uw"], exp, ctx)

            check("both empty")
            client.execute_sql("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)", schema_name=sn)
            t.update({1: 1, 2: 2, 3: 3})
            check("t alone pairs with nothing")
            client.execute_sql("INSERT INTO u VALUES (10, 0), (20, 0)", schema_name=sn)
            u.update({10: 0, 20: 0})
            check("first product")
            client.execute_sql("INSERT INTO t VALUES (4, 4)", schema_name=sn)
            t[4] = 4
            check("a t row pairs with every u row")
            client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
            del t[2]
            check("a t deletion retracts its pairs")
            client.execute_sql("INSERT INTO u VALUES (30, 1)", schema_name=sn)
            u[30] = 1
            check("a u row pairs with every t row")
            client.execute_sql("UPDATE u SET w = 9 WHERE id = 10", schema_name=sn)
            u[10] = 9
            check("a u update swaps its pairs")
            client.execute_sql("UPDATE t SET v = 7 WHERE id = 3", schema_name=sn)
            t[3] = 7
            check("a t update swaps its pairs")
            client.execute_sql("DELETE FROM u WHERE id = 20", schema_name=sn)
            del u[20]
            check("a u deletion retracts its pairs")
            client.execute_sql("DELETE FROM u", schema_name=sn)
            u.clear()
            check("an emptied side empties the product")
            client.execute_sql("INSERT INTO u VALUES (40, 4)", schema_name=sn)
            u[40] = 4
            check("and refilling it rebuilds the product")
        finally:
            _cleanup(client, sn)

    def test_backfill_over_existing_rows(self, client):
        """A view created over populated tables backfills the whole product, and
        keeps maintaining it afterwards."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            self._insert(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT t.id AS tid, u.id AS uid FROM t, u", schema_name=sn
            )
            vid = _vid(client, sn, "v")
            oracle.assert_view_matches(client, vid, ["tid", "uid"], _product(self.ROWS_T, self.ROWS_U), "backfill")
            client.execute_sql("INSERT INTO u VALUES (99, 1)", schema_name=sn)
            rows_u = self.ROWS_U + [(99, 1)]
            oracle.assert_view_matches(client, vid, ["tid", "uid"], _product(self.ROWS_T, rows_u), "after")
        finally:
            _cleanup(client, sn)

    def test_compound_pks_and_string_payload(self, client):
        """The output PK is the pair of source PKs at their own arity — a
        two-column PK on one side gives a three-slot pair — and the payload
        rides through untouched, strings included."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v TEXT NOT NULL, PRIMARY KEY (a, b))",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT t.a, t.b, t.v, u.id AS uid, u.name FROM t CROSS JOIN u",
                schema_name=sn,
            )
            rows_t = [(1, 1, "one-one"), (1, 2, "one-two"), (2, 1, "two-one-" + "x" * 40)]
            rows_u = [(7, "seven"), (8, "eight-" + "y" * 40)]
            client.execute_sql(
                "INSERT INTO t VALUES " + ",".join(f"({a},{b},'{v}')" for a, b, v in rows_t), schema_name=sn
            )
            client.execute_sql(
                "INSERT INTO u VALUES " + ",".join(f"({i},'{n}')" for i, n in rows_u), schema_name=sn
            )
            exp = Counter({(a, b, v, i, n): 1 for a, b, v in rows_t for i, n in rows_u})
            oracle.assert_view_matches(client, _vid(client, sn, "v"), ["a", "b", "v", "uid", "name"], exp)
        finally:
            _cleanup(client, sn)

    def test_self_product(self, client):
        """`FROM t a, t b` pairs a table with itself: the second copy is wrapped
        in a segment so the two inputs are distinct sources, and the product
        holds every ordered pair including each row with itself."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS x, b.id AS y FROM t a, t b", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW w AS SELECT a.id AS x, b.id AS y FROM t a CROSS JOIN t b WHERE a.id <> b.id",
                schema_name=sn,
            )
            self._insert(client, sn, rows_u=[])
            vid, wid = _vid(client, sn, "v"), _vid(client, sn, "w")
            oracle.assert_view_matches(client, vid, ["x", "y"], _product(self.ROWS_T, self.ROWS_T), "v")
            oracle.assert_view_matches(
                client, wid, ["x", "y"], _product(self.ROWS_T, self.ROWS_T, lambda a, b: a[0] != b[0]), "w"
            )
            client.execute_sql("DELETE FROM t WHERE id = 4", schema_name=sn)
            rows = [r for r in self.ROWS_T if r[0] != 4]
            oracle.assert_view_matches(client, vid, ["x", "y"], _product(rows, rows), "v after delete")
            oracle.assert_view_matches(
                client, wid, ["x", "y"], _product(rows, rows, lambda a, b: a[0] != b[0]), "w after delete"
            )
        finally:
            _cleanup(client, sn)

    def test_three_way_product(self, client):
        """`FROM a, b, c` is two keyless steps: the inner product is cut into a
        segment whose pair-PK becomes the left key of the outer one."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            for name in ("a", "b", "c"):
                client.execute_sql(
                    f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn
                )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid, c.id AS cid FROM a, b, c", schema_name=sn
            )
            client.execute_sql("INSERT INTO a VALUES (1, 0), (2, 0), (3, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (10, 0), (20, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (100, 0), (200, 0), (300, 0), (400, 0)", schema_name=sn)
            vid = _vid(client, sn, "v")
            exp = Counter({(x, y, z): 1 for x in (1, 2, 3) for y in (10, 20) for z in (100, 200, 300, 400)})
            oracle.assert_view_matches(client, vid, ["aid", "bid", "cid"], exp)
            client.execute_sql("DELETE FROM b WHERE id = 10", schema_name=sn)
            exp = Counter({(x, 20, z): 1 for x in (1, 2, 3) for z in (100, 200, 300, 400)})
            oracle.assert_view_matches(client, vid, ["aid", "bid", "cid"], exp, "after delete")
        finally:
            _cleanup(client, sn)

    def test_weights_multiply_over_a_bag_valued_side(self, client):
        """A side whose rows carry weight 2 (a `UNION ALL` of two copies) pairs at
        weight 2: the join multiplies weights, it does not count matches."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a1 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE TABLE a2 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW bag AS SELECT k FROM a1 UNION ALL SELECT k FROM a2", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT bag.k AS k, u.id AS uid FROM bag CROSS JOIN u", schema_name=sn
            )
            client.execute_sql("INSERT INTO a1 VALUES (1, 5), (2, 6)", schema_name=sn)
            client.execute_sql("INSERT INTO a2 VALUES (1, 5)", schema_name=sn)
            client.execute_sql("INSERT INTO u VALUES (10, 0), (20, 0)", schema_name=sn)
            exp = Counter({(5, 10): 2, (5, 20): 2, (6, 10): 1, (6, 20): 1})
            oracle.assert_view_matches(client, _vid(client, sn, "v"), ["k", "uid"], exp)
        finally:
            _cleanup(client, sn)

    def test_a_grouped_view_over_the_product(self, client):
        """A view over a cross view reads its pair-PK-keyed rows like any other
        relation: per-t counts are the other side's size."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW x AS SELECT t.id AS tid, u.w AS uw FROM t CROSS JOIN u", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW g AS SELECT tid, COUNT(*) AS n, SUM(uw) AS s FROM x GROUP BY tid", schema_name=sn
            )
            self._insert(client, sn)
            sw = sum(w for _, w in self.ROWS_U)
            exp = Counter({(ti, len(self.ROWS_U), sw): 1 for ti, _ in self.ROWS_T})
            oracle.assert_view_matches(client, _vid(client, sn, "g"), ["tid", "n", "s"], exp)
            client.execute_sql("DELETE FROM u WHERE id = 10", schema_name=sn)
            rows_u = [r for r in self.ROWS_U if r[0] != 10]
            sw = sum(w for _, w in rows_u)
            exp = Counter({(ti, len(rows_u), sw): 1 for ti, _ in self.ROWS_T})
            oracle.assert_view_matches(client, _vid(client, sn, "g"), ["tid", "n", "s"], exp, "after delete")
        finally:
            _cleanup(client, sn)

    @NEEDS_MULTI
    def test_replicated_sides(self, client):
        """A replicated side holds every row on every worker. Its delta relays
        single-sourced and the trace filter partitions it like a keyed side, so
        the product carries no W× duplication — for one replicated side, and
        for two, where the view itself is replicated and runs correct-local."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL) WITH (replicated = true)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE r2 (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL) WITH (replicated = true)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE k (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql("CREATE VIEW rk AS SELECT r.id AS a, k.id AS b FROM r CROSS JOIN k", schema_name=sn)
            client.execute_sql("CREATE VIEW kr AS SELECT k.id AS a, r.id AS b FROM k CROSS JOIN r", schema_name=sn)
            client.execute_sql("CREATE VIEW rr AS SELECT r.id AS a, r2.id AS b FROM r, r2", schema_name=sn)
            rows_r = [(i, 0) for i in range(1, 6)]
            rows_r2 = [(i, 0) for i in range(50, 53)]
            rows_k = [(i, 0) for i in range(10, 17)]
            for name, rows in (("r", rows_r), ("r2", rows_r2), ("k", rows_k)):
                client.execute_sql(
                    f"INSERT INTO {name} VALUES " + ",".join(f"({a},{b})" for a, b in rows), schema_name=sn
                )
            oracle.assert_view_matches(client, _vid(client, sn, "rk"), ["a", "b"], _product(rows_r, rows_k), "rk")
            oracle.assert_view_matches(client, _vid(client, sn, "kr"), ["a", "b"], _product(rows_k, rows_r), "kr")
            oracle.assert_view_matches(client, _vid(client, sn, "rr"), ["a", "b"], _product(rows_r, rows_r2), "rr")
            client.execute_sql("DELETE FROM r WHERE id = 3", schema_name=sn)
            rows_r = [r for r in rows_r if r[0] != 3]
            oracle.assert_view_matches(client, _vid(client, sn, "rk"), ["a", "b"], _product(rows_r, rows_k), "rk-del")
            oracle.assert_view_matches(client, _vid(client, sn, "rr"), ["a", "b"], _product(rows_r, rows_r2), "rr-del")
        finally:
            _cleanup(client, sn)


def test_cross_view_survives_restart(own_server):
    """The two traces checkpoint like any join's: after a restart the view
    resumes and a delta on either side pairs against the recovered other side."""
    sock_path = own_server.sock_path
    own_server.start()
    conn = gnitz.connect(sock_path)
    sn = "cross_persist"
    conn.create_schema(sn)
    conn.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
    conn.execute_sql("CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL)", schema_name=sn)
    conn.execute_sql("CREATE VIEW v AS SELECT t.id AS tid, u.id AS uid FROM t CROSS JOIN u", schema_name=sn)
    vid = _vid(conn, sn, "v")
    rows_t = [(i, 0) for i in range(1, 5)]
    rows_u = [(i, 0) for i in range(10, 13)]
    conn.execute_sql("INSERT INTO t VALUES " + ",".join(f"({a},{b})" for a, b in rows_t), schema_name=sn)
    conn.execute_sql("INSERT INTO u VALUES " + ",".join(f"({a},{b})" for a, b in rows_u), schema_name=sn)
    oracle.assert_view_matches(conn, vid, ["tid", "uid"], _product(rows_t, rows_u), "before restart")
    conn.close()

    own_server.restart()
    conn = gnitz.connect(sock_path)
    assert _vid(conn, sn, "v") == vid
    oracle.assert_view_matches(conn, vid, ["tid", "uid"], _product(rows_t, rows_u), "after restart")
    conn.execute_sql("INSERT INTO u VALUES (13, 0)", schema_name=sn)
    conn.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
    rows_u.append((13, 0))
    rows_t = [r for r in rows_t if r[0] != 2]
    oracle.assert_view_matches(conn, vid, ["tid", "uid"], _product(rows_t, rows_u), "after restart deltas")
    conn.close()
