"""The inner equijoin: the graph an ON key compiles to, and the spellings that
compile to the same graph.

The output PK is the synthetic `_join_pk`, one slot per key pair, so each
source's own PK rides through as payload and a join view's identity is the key
rather than either input's key. Output weight is the *product* of the input
weights, which is why every assertion here is a weighted bag: a many-to-many key
is exactly where a duplicated delta or a doubled reply frame hides, and a row
count reads both as correct.

Outer, keyless, range and band shapes have their own files; they appear here
only where a spelling's rule differs by orientation (the USING merge keeps the
*preserved* side's copy).
"""
from collections import Counter

from _read import bag, scanned

# (statement, table, {pk: row}); a `None` row deletes the pk. Rows are
# l: (k, x, y, lv), r: (x, y, name), c: (x, y, cv) under the key (k1, k2, k3).
_CHURN = [
    # r(40) shares r(10)'s (x, y), so the two-column key is many-to-many.
    ("INSERT INTO r VALUES (10, 1, 100, 'Alice'), (20, 2, 200, 'Bob'), (30, 7, 7, 'Cy'), "
     "(40, 1, 100, 'Dup')",
     "r", {10: (1, 100, "Alice"), 20: (2, 200, "Bob"), 30: (7, 7, "Cy"), 40: (1, 100, "Dup")}),
    # l(4)'s key 99 names no r; l(2) agrees with r(20) on x alone; l(5)'s x fills
    # both slots of r(30)'s (x, x).
    ("INSERT INTO l VALUES (1, 10, 1, 100, 100), (2, 20, 2, 999, 200), (3, 10, 1, 100, 300), "
     "(4, 99, 5, 5, 400), (5, 30, 7, 0, 500)",
     "l", {1: (10, 1, 100, 100), 2: (20, 2, 999, 200), 3: (10, 1, 100, 300),
           4: (99, 5, 5, 400), 5: (30, 7, 0, 500)}),
    # Three rows whose 24-byte key shares its first 16 bytes.
    ("INSERT INTO c VALUES (1, 1, 1, 1, 100, 11), (1, 1, 2, 1, 100, 22), (1, 1, 3, 2, 200, 33)",
     "c", {(1, 1, 1): (1, 100, 11), (1, 1, 2): (1, 100, 22), (1, 1, 3): (2, 200, 33)}),
    ("UPDATE l SET lv = 999 WHERE id = 1", "l", {1: (10, 1, 100, 999)}),
    ("UPDATE l SET k = 20 WHERE id = 3", "l", {3: (20, 1, 100, 300)}),
    ("UPDATE c SET cv = 222 WHERE k1 = 1 AND k2 = 1 AND k3 = 2", "c", {(1, 1, 2): (1, 100, 222)}),
    ("UPDATE r SET name = 'Bobby' WHERE id = 20", "r", {20: (2, 200, "Bobby")}),
    # A right-side re-key completes l(2)'s pair.
    ("UPDATE r SET y = 999 WHERE id = 20", "r", {20: (2, 999, "Bobby")}),
    ("DELETE FROM r WHERE id = 10", "r", {10: None}),
    ("DELETE FROM c WHERE k1 = 1 AND k2 = 1 AND k3 = 1", "c", {(1, 1, 1): None}),
    ("DELETE FROM l WHERE id = 2", "l", {2: None}),
]


def test_a_delta_on_either_side_joins_the_other_sides_trace(client, schema_name):
    """Whichever input an epoch's delta arrives on, it joins the *other* input's
    pre-epoch integral: insert, payload update, re-key and delete on each side in
    turn. `one` keys a plain column against the key region, with a TEXT payload
    crossing through the blob heap; `two` is a k=2 key, where only rows agreeing
    on both columns join and the k-wide reindex co-locates them; `twin` fills both
    of its INT-to-BIGINT promoted slots from one column, so the scatter must mirror
    the trace packer slot for slot; and `wide` carries a three-column source PK as
    payload, whose tie-break siblings must maintain independently."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x INT NOT NULL, "
        "y BIGINT NOT NULL, lv BIGINT NOT NULL); "
        "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL, "
        "name TEXT NOT NULL); "
        "CREATE TABLE c (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, k3 BIGINT NOT NULL, "
        "x BIGINT NOT NULL, y BIGINT NOT NULL, cv BIGINT NOT NULL, PRIMARY KEY (k1, k2, k3)); "
        "CREATE VIEW one AS SELECT l.id AS lid, l.lv AS lv, r.id AS rid, r.name AS name "
        "FROM l JOIN r ON l.k = r.id; "
        "CREATE VIEW two AS SELECT l.id AS lid, r.id AS rid FROM l JOIN r ON l.x = r.x AND l.y = r.y; "
        "CREATE VIEW twin AS SELECT l.id AS lid, r.id AS rid FROM l JOIN r ON l.x = r.x AND l.x = r.y; "
        "CREATE VIEW wide AS SELECT c.k1, c.k2, c.k3, c.cv, r.id AS rid "
        "FROM c JOIN r ON c.x = r.x AND c.y = r.y", schema_name=sn)

    state = {"l": {}, "r": {}, "c": {}}
    for sql, table, changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        for pk, row in changes.items():
            if row is None:
                del state[table][pk]
            else:
                state[table][pk] = row
        l, r, c = state["l"], state["r"], state["c"]

        assert bag(scanned(client, sn, "one"), "lid", "lv", "rid", "name") == {
            (li, lv, ri, name): 1 for li, (k, _, _, lv) in l.items()
            for ri, (_, _, name) in r.items() if k == ri}, sql
        assert bag(scanned(client, sn, "two"), "lid", "rid") == {
            (li, ri): 1 for li, (_, x, y, _) in l.items() for ri, (rx, ry, _) in r.items()
            if (x, y) == (rx, ry)}, sql
        assert bag(scanned(client, sn, "twin"), "lid", "rid") == {
            (li, ri): 1 for li, (_, x, _, _) in l.items() for ri, (rx, ry, _) in r.items()
            if x == rx == ry}, sql
        assert bag(scanned(client, sn, "wide"), "k1", "k2", "k3", "cv", "rid") == {
            (*key, cv, ri): 1 for key, (x, y, cv) in c.items() for ri, (rx, ry, _) in r.items()
            if (x, y) == (rx, ry)}, sql


def test_every_spelling_of_one_join_compiles_to_one_graph(client, schema_name):
    """The key can arrive from an ON, from the WHERE above a keyless step, or
    from a USING, and none of them is a different graph. A WHERE equality over an
    INNER join is promoted to a second key where it can be one and stays a
    residual where it cannot (a float), so the promotion never turns a working
    query into an error. A comma FROM folds left-deep, so the one WHERE above the
    spine must key the inner step too; a comma binds loosest, so `JOIN … ON …, c`
    keys only the outer step from the WHERE; a range-only WHERE keys the comma on
    its range slot alone. A derived table, a set-op branch and a CTE bind their
    bodies through the same binder, and a merged USING column resolves in the
    grouped and DISTINCT tails as in a plain projection."""
    sn = schema_name
    pairs = "SELECT t.id AS tid, u.id AS uid"
    spine = "SELECT t.id AS tid, u.id AS uid, c.id AS cid"
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL, "
        "n BIGINT NOT NULL, f DOUBLE NOT NULL); "
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL, "
        "n BIGINT NOT NULL, f DOUBLE NOT NULL, j BIGINT NOT NULL); "
        "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, j BIGINT NOT NULL); "
        "CREATE VIEW tv AS SELECT id, k FROM t; "
        f"CREATE VIEW k_on AS {pairs} FROM t JOIN u ON t.k = u.k; "
        f"CREATE VIEW k_global AS {pairs} FROM t GLOBAL JOIN u ON t.k = u.k; "
        f"CREATE VIEW k_comma AS {pairs} FROM t, u WHERE t.k = u.k; "
        f"CREATE VIEW k_cross AS {pairs} FROM t CROSS JOIN u WHERE t.k = u.k; "
        f"CREATE VIEW k_using AS {pairs} FROM t JOIN u USING (k); "
        f"CREATE VIEW k_derived AS SELECT tid, uid FROM ({pairs} FROM t, u WHERE t.k = u.k) x; "
        "CREATE VIEW k_cte AS WITH cu AS (SELECT id AS uid, k FROM u) "
        "SELECT tv.id AS tid, cu.uid AS uid FROM tv JOIN cu USING (k); "
        f"CREATE VIEW kn_split AS {pairs} FROM t JOIN u ON t.k = u.k WHERE t.n = u.n; "
        f"CREATE VIEW kn_fused AS {pairs} FROM t JOIN u ON t.k = u.k AND t.n = u.n; "
        f"CREATE VIEW kf_residual AS {pairs} FROM t JOIN u ON t.k = u.k WHERE t.f = u.f; "
        f"CREATE VIEW range_comma AS {pairs} FROM t, u WHERE t.a < u.w; "
        "CREATE VIEW branch AS SELECT t.id AS x FROM t, u WHERE t.k = u.k UNION ALL SELECT id AS x FROM u; "
        "CREATE VIEW grouped AS SELECT k, COUNT(*) AS n FROM t JOIN u USING (k) GROUP BY k; "
        "CREATE VIEW distinct_k AS SELECT DISTINCT k FROM t JOIN u USING (k); "
        f"CREATE VIEW spine_comma AS {spine} FROM t, u, c WHERE t.k = u.k AND u.j = c.j; "
        f"CREATE VIEW spine_explicit AS {spine} FROM t JOIN u ON t.k = u.k JOIN c ON u.j = c.j; "
        f"CREATE VIEW spine_mixed AS {spine} FROM t JOIN u ON t.k = u.k, c WHERE u.j = c.j",
        schema_name=sn)

    t, u, c = {}, {}, {}
    for sql, table, changes in [
        ("INSERT INTO c VALUES (20, 5), (21, 6)", c, {20: 5, 21: 6}),
        ("INSERT INTO u VALUES (10, 1, 5, 7, 1.5, 5), (11, 2, 1, 9, 2.5, 6), (12, 2, 6, 8, 3.5, 5)",
         u, {10: (1, 5, 7, 1.5, 5), 11: (2, 1, 9, 2.5, 6), 12: (2, 6, 8, 3.5, 5)}),
        # k=1 is one-to-one, k=2 one-to-many, and t(3) is a second row at k=1.
        ("INSERT INTO t VALUES (1, 1, 1, 7, 1.5), (2, 2, 2, 8, 2.5), (3, 1, 3, 7, 9.5)",
         t, {1: (1, 1, 7, 1.5), 2: (2, 2, 8, 2.5), 3: (1, 3, 7, 9.5)}),
        ("UPDATE t SET k = 2 WHERE id = 1", t, {1: (2, 1, 7, 1.5)}),
        ("DELETE FROM u WHERE id = 12", u, {12: None}),
    ]:
        client.execute_sql(sql, schema_name=sn)
        for pk, row in changes.items():
            if row is None:
                del table[pk]
            else:
                table[pk] = row
        keyed = [(ti, ui) for ti, tr in t.items() for ui, ur in u.items() if tr[0] == ur[0]]

        for name in ("k_on", "k_global", "k_comma", "k_cross", "k_using", "k_derived", "k_cte"):
            assert bag(scanned(client, sn, name), "tid", "uid") == dict.fromkeys(keyed, 1), (sql, name)
        for name in ("kn_split", "kn_fused"):
            assert bag(scanned(client, sn, name), "tid", "uid") == {
                p: 1 for p in keyed if t[p[0]][2] == u[p[1]][2]}, (sql, name)
        assert bag(scanned(client, sn, "kf_residual"), "tid", "uid") == {
            p: 1 for p in keyed if t[p[0]][3] == u[p[1]][3]}, sql
        assert bag(scanned(client, sn, "range_comma"), "tid", "uid") == {
            (ti, ui): 1 for ti, tr in t.items() for ui, ur in u.items() if tr[1] < ur[1]}, sql
        assert bag(scanned(client, sn, "branch"), "x") == \
            Counter((ti,) for ti, _ in keyed) + Counter((ui,) for ui in u), sql
        assert bag(scanned(client, sn, "grouped"), "k", "n") == {
            kn: 1 for kn in Counter(t[ti][0] for ti, _ in keyed).items()}, sql
        assert bag(scanned(client, sn, "distinct_k"), "k") == {(t[ti][0],): 1 for ti, _ in keyed}, sql
        for name in ("spine_comma", "spine_explicit", "spine_mixed"):
            assert bag(scanned(client, sn, name), "tid", "uid", "cid") == {
                (ti, ui, ci): 1 for ti, ui in keyed for ci, j in c.items() if u[ui][4] == j}, (sql, name)


def test_natural_and_using_merge_each_shared_name_into_one(client, schema_name):
    """NATURAL keys on every name the two sides' *visible* columns share, so a
    pair agreeing on only some of them does not join; chained, a name the first
    step merged is already gone from the left, so it survives the chain once.
    NATURAL over no shared name is a CROSS JOIN. `USING (k, v)` is two key pairs
    and two merges, and the merged column carries the matched right copy's value;
    under an outer join it carries the *preserved* side's copy, since an unmatched
    preserved row keeps a key of its own while the other copy is null-filled.
    Once a first step merged `k`, a third relation's `USING (k)` pairs with it."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE na (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL); "
        "CREATE TABLE nb (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL); "
        "CREATE TABLE nc (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL); "
        "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL); "
        "CREATE TABLE r (rid BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL); "
        "CREATE VIEW pair AS SELECT id, k, v FROM na NATURAL JOIN nb; "
        "CREATE VIEW chain AS SELECT * FROM na NATURAL JOIN nb NATURAL JOIN nc; "
        "CREATE VIEW product AS SELECT x, y FROM l NATURAL JOIN r; "
        "CREATE VIEW both_merged AS SELECT na.id AS aid, nb.id AS bid, k AS merged, "
        "nb.k AS right_k, v FROM na JOIN nb USING (k, v); "
        "CREATE VIEW left_merged AS SELECT na.id AS pid, k AS merged, nb.id AS oid "
        "FROM na LEFT JOIN nb USING (k); "
        "CREATE VIEW right_merged AS SELECT nb.id AS pid, k AS merged, na.id AS oid "
        "FROM na RIGHT JOIN nb USING (k); "
        "CREATE VIEW three AS SELECT na.id AS ai, nc.id AS ci FROM na JOIN nb USING (k) JOIN nc USING (k); "
        "INSERT INTO l VALUES (1, 10), (2, 20); "
        "INSERT INTO r VALUES (7, 70), (8, 80), (9, 90); "
        "INSERT INTO nc VALUES (1, 5, 9), (6, 6, 9)", schema_name=sn)
    assert bag(scanned(client, sn, "product"), "x", "y") == {
        (x, y): 1 for x in (10, 20) for y in (70, 80, 90)}

    na = {1: (5, 9), 2: (6, 9), 3: (7, 9), 4: (8, 0)}
    nb = {1: (5, 9), 2: (6, 9), 3: (0, 9), 5: (8, 1)}
    nc = {1: (5, 9), 6: (6, 9)}
    for sql, changes in [
        # nb(3) shares only `id` and `v` with na(3); nb(5) shares only `k` with na(4).
        ("INSERT INTO na VALUES (1, 5, 9), (2, 6, 9), (3, 7, 9), (4, 8, 0); "
         "INSERT INTO nb VALUES (1, 5, 9), (2, 6, 9), (3, 0, 9), (5, 8, 1)", {}),
        ("UPDATE nb SET k = 7 WHERE id = 3", {3: (7, 9)}),
    ]:
        client.execute_sql(sql, schema_name=sn)
        nb.update(changes)

        assert bag(scanned(client, sn, "pair"), "id", "k", "v") == {
            (i, *kv): 1 for i, kv in na.items() if nb.get(i) == kv}, sql
        chain = scanned(client, sn, "chain")
        assert bag(chain, "id", "k", "v") == {
            (i, *kv): 1 for i, kv in na.items() if nb.get(i) == kv == nc.get(i)}, sql
        assert all(set(row._fields) == {"id", "k", "v"} for row in chain), sql
        assert bag(scanned(client, sn, "both_merged"), "aid", "bid", "merged", "right_k", "v") == {
            (ai, bi, k, k, v): 1 for ai, (k, v) in na.items() for bi, bkv in nb.items()
            if bkv == (k, v)}, sql
        for name, pres, other in (("left_merged", na, nb), ("right_merged", nb, na)):
            assert bag(scanned(client, sn, name), "pid", "merged", "oid") == Counter(
                (pi, pk, oi) for pi, (pk, _) in pres.items()
                for oi in ([oi for oi, (ok, _) in other.items() if ok == pk] or [None])), (sql, name)
        assert bag(scanned(client, sn, "three"), "ai", "ci") == Counter(
            (ai, ci) for ai, (k, _) in na.items() for bk, _ in nb.values() if bk == k
            for ci, (ck, _) in nc.items() if ck == k), sql
