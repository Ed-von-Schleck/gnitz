"""The two-way inner equijoin: the graph an ON key compiles to, and the
spellings that compile to the same graph.

The output PK is the synthetic `_join_pk`, one slot per key pair, so each
source's own PK rides through as payload and a join view's identity is the key
rather than either input's key. Output weight is the *product* of the input
weights, which is why every assertion here is a weighted bag: a many-to-many key
is exactly where a duplicated delta or a doubled reply frame hides, and a row
count reads both as correct.

INNER only. LEFT/RIGHT/FULL, CROSS and range/band shapes have their own files;
they appear here only where a spelling's rule differs by orientation (the USING
merge keeps the *preserved* side's copy).

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/relational_shape/test_joins.py
"""
from collections import Counter

import gnitz
import pytest
import _oracle as oracle
from _read import bag, scanned


def _tu(client, sn):
    """`t(id, k, a)` and `u(id, k, w)` — the pair the spelling tests join. They
    share `id` and `k`, which is what NATURAL keys on and `USING (k)` merges."""
    for name, payload in (("t", "a"), ("u", "w")):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, "
            f"{payload} BIGINT NOT NULL)",
            schema_name=sn,
        )


def _fill_tu(client, sn):
    """t and u seeded so k=1 is one-to-one, k=2 is one-to-many, and t(3) is a
    second row at k=1 — the smallest data that separates the product from a
    row set."""
    client.execute_sql("INSERT INTO t VALUES (1, 1, 0), (2, 2, 0), (3, 1, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO u VALUES (10, 1, 0), (11, 2, 0), (12, 2, 0)", schema_name=sn)


_TU_PAIRS = {(1, 10): 1, (3, 10): 1, (2, 11): 1, (2, 12): 1}


# ── the join itself ────────────────────────────────────────────────────────


def test_only_matching_pairs_reach_the_output(client, schema_name):
    """An equijoin emits one row per (left, right) pair agreeing on the key and
    nothing for a key with no counterpart. The right side's VARCHAR payload
    crosses the join through the blob heap and arrives verbatim."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, cid BIGINT NOT NULL, amount BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, name VARCHAR(100) NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT orders.id AS oid, orders.amount, customers.name "
        "FROM orders JOIN customers ON orders.cid = customers.id", schema_name=sn)
    client.execute_sql("INSERT INTO customers VALUES (10, 'Alice'), (20, 'Bob')", schema_name=sn)
    client.execute_sql(
        "INSERT INTO orders VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300), (4, 99, 400)",
        schema_name=sn)

    assert bag(scanned(client, sn, "v"), "oid", "amount", "name") == {
        (1, 100, "Alice"): 1, (2, 200, "Bob"): 1, (3, 300, "Alice"): 1,
    }, "order 4's key 99 has no customer, so it emits nothing"


def test_output_weight_is_the_product_of_the_input_weights(client, schema_name):
    """Join is bilinear, so a key carrying several rows on each side emits their
    full product and a bag-valued input *multiplies* rather than repeating. The
    left is a UNION ALL view whose two branches project to one identity, giving
    it weight 2 at k=5 — a row-count assertion cannot tell that from weight 1."""
    sn = schema_name
    for name in ("s1", "s2"):
        client.execute_sql(f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, g BIGINT NOT NULL)",
                           schema_name=sn)
    client.execute_sql(
        "CREATE TABLE r (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, val BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE VIEW l AS SELECT g FROM s1 UNION ALL SELECT g FROM s2",
                       schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT l.g, r.val FROM l JOIN r ON l.g = r.k",
                       schema_name=sn)

    # g=5 exists in both branches (weight 2); g=6 in one (weight 1).
    client.execute_sql("INSERT INTO s1 VALUES (1, 5), (2, 6)", schema_name=sn)
    client.execute_sql("INSERT INTO s2 VALUES (1, 5)", schema_name=sn)
    client.execute_sql("INSERT INTO r VALUES (1, 5, 10), (2, 5, 20), (3, 6, 30)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "g", "val") == {
        (5, 10): 2, (5, 20): 2, (6, 30): 1,
    }, "each output weight is w_left x w_right"


def test_a_delta_on_either_side_joins_the_other_sides_trace(client, schema_name):
    """The incremental form is symmetric: whichever input an epoch's delta
    arrives on, it joins the *other* input's pre-epoch integral. Insert, re-key,
    payload-update and delete on each side in turn, each epoch checked against a
    from-scratch oracle that never touches the engine."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, cid BIGINT NOT NULL, amount BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, cname BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT orders.id AS oid, orders.amount AS amt, "
        "customers.id AS cid, customers.cname AS cname "
        "FROM orders JOIN customers ON orders.cid = customers.id", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]

    project = ["oid", "amt", "cid", "cname"]
    orders, customers = {}, {}

    def check(ctx):
        oracle.assert_view_matches(client, vid, project, oracle.oracle_equijoin(
            left=orders, lwhere=None, lkey="cid", lproj=["id", "amount"],
            right=customers, rwhere=None, rkey="id", rproj=["id", "cname"],
            out_cols=project), ctx=ctx)

    client.execute_sql("INSERT INTO customers VALUES (10, 111), (20, 222)", schema_name=sn)
    oracle.apply_insert(customers, "id", [{"id": 10, "cname": 111}, {"id": 20, "cname": 222}])
    check("right side alone")

    client.execute_sql(
        "INSERT INTO orders VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300)", schema_name=sn)
    oracle.apply_insert(orders, "id", [{"id": 1, "cid": 10, "amount": 100},
                                       {"id": 2, "cid": 20, "amount": 200},
                                       {"id": 3, "cid": 10, "amount": 300}])
    check("left delta against the right trace")

    client.execute_sql("UPDATE orders SET amount = 999 WHERE id = 1", schema_name=sn)
    oracle.apply_update(orders, "id", 1, {"amount": 999})
    check("left payload update")

    client.execute_sql("UPDATE orders SET cid = 20 WHERE id = 3", schema_name=sn)
    oracle.apply_update(orders, "id", 3, {"cid": 20})
    check("left re-key moves the row to another group")

    client.execute_sql("UPDATE customers SET cname = 333 WHERE id = 20", schema_name=sn)
    oracle.apply_update(customers, "id", 20, {"cname": 333})
    check("right payload update retracts and re-emits every matched row")

    client.execute_sql("DELETE FROM customers WHERE id = 10", schema_name=sn)
    oracle.apply_delete(customers, "id", [10])
    check("right delete retracts every row it matched")

    client.execute_sql("DELETE FROM orders WHERE id = 2", schema_name=sn)
    oracle.apply_delete(orders, "id", [2])
    check("left delete")


# ── the key: how many slots, and what fills them ───────────────────────────


def test_a_two_column_key_becomes_a_two_slot_join_pk(client, schema_name):
    """`ON a.x = b.x AND a.y = b.y` is a k=2 key: the view's PK is the two
    `_join_pk` slots, only rows agreeing on BOTH columns join, and the k-wide
    reindex co-locates them across workers. Checked against a full recompute
    over 20 rows a side, then re-checked after a row completes a new pair."""
    sn = schema_name
    for name, payload in (("a", "av"), ("b", "bv")):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, x BIGINT NOT NULL, "
            f"y BIGINT NOT NULL, {payload} BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.x, a.y, a.av, b.bv FROM a JOIN b ON a.x = b.x AND a.y = b.y",
        schema_name=sn)
    vid, vschema = client.resolve_table(sn, "v")
    assert vschema.pk_indices == [0, 1], "a k=2 key makes the PK two _join_pk slots"

    # Keys spread over 5x7 (x, y) combinations so both sides span hash buckets.
    a_rows = [(i, i % 5, i % 7, i) for i in range(1, 21)]
    b_rows = [(i + 100, i % 5, i % 7, i * 10) for i in range(1, 21)]

    def expect():
        return Counter((ax, ay, av, bv)
                       for (_ai, ax, ay, av) in a_rows
                       for (_bi, bx, by, bv) in b_rows
                       if (ax, ay) == (bx, by))

    for name, rows in (("a", a_rows), ("b", b_rows)):
        client.execute_sql(f"INSERT INTO {name} VALUES " + ",".join(str(r) for r in rows),
                           schema_name=sn)
    oracle.assert_view_matches(client, vid, ["x", "y", "av", "bv"], expect(), "seed")

    # A key no b row carried yet: the pair completes only when both columns agree.
    a_rows.append((99, 40, 41, 7))
    client.execute_sql("INSERT INTO a VALUES (99, 40, 41, 7)", schema_name=sn)
    oracle.assert_view_matches(client, vid, ["x", "y", "av", "bv"], expect(), "unmatched a")

    b_rows.append((199, 40, 41, 8))
    client.execute_sql("INSERT INTO b VALUES (199, 40, 41, 8)", schema_name=sn)
    oracle.assert_view_matches(client, vid, ["x", "y", "av", "bv"], expect(), "pair completed")


def test_one_key_column_can_fill_two_slots(client, schema_name):
    """`ON a.x = b.p AND a.x = b.q` reindexes `[x, x]` into both `_join_pk`
    slots. The two slots carry distinct targets — `a.x` is INT and `b.p`/`b.q`
    are BIGINT, so each pair promotes independently — so the scatter has to
    mirror the trace packer slot for slot. A row joins only where p = q = x."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT PRIMARY KEY, x INT NOT NULL, av BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT PRIMARY KEY, p BIGINT NOT NULL, q BIGINT NOT NULL, "
        "bv BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.x AS ax, a.av, b.bv FROM a JOIN b ON a.x = b.p AND a.x = b.q",
        schema_name=sn)
    assert client.resolve_table(sn, "v")[1].pk_indices == [0, 1]

    client.execute_sql("INSERT INTO a VALUES (1, 5, 10), (2, 8, 20), (3, -4, 30)", schema_name=sn)
    # (5,5) and (-4,-4) agree with an a.x; (8,9) has p != q so it matches nothing.
    client.execute_sql(
        "INSERT INTO b VALUES (101, 5, 5, 100), (102, 8, 9, 200), (103, -4, -4, 300)",
        schema_name=sn)

    assert bag(scanned(client, sn, "v"), "ax", "av", "bv") == {
        (5, 10, 100): 1, (-4, 30, 300): 1,
    }


def test_a_key_pair_with_no_common_type_is_refused(client, schema_name):
    """A key whose unsigned side is 128-bit (`DECIMAL(38,0) = BIGINT`) would need
    a signed 256-bit type to hold both ranges, which does not exist. Narrower
    cross-sign pairs promote instead — value_domain covers those."""
    sn = schema_name
    client.execute_sql("CREATE TABLE a (id BIGINT PRIMARY KEY, k DECIMAL(38,0) NOT NULL)",
                       schema_name=sn)
    client.execute_sql("CREATE TABLE b (id BIGINT PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.k",
                           schema_name=sn)


def test_a_compound_source_pk_rides_through_as_payload(client, schema_name):
    """The output PK is the join key, so each source's own PK becomes payload.
    `a`'s PK is three columns wide and its first three rows share their first 16
    OPK bytes, differing only past byte 16; they must stay distinct through the
    reindex and maintain independently under INSERT / UPDATE / DELETE on either
    side."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, k3 BIGINT NOT NULL, "
        "x BIGINT NOT NULL, y BIGINT NOT NULL, av BIGINT NOT NULL, PRIMARY KEY (k1, k2, k3))",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (j1 BIGINT NOT NULL, j2 BIGINT NOT NULL, x BIGINT NOT NULL, "
        "y BIGINT NOT NULL, bv BIGINT NOT NULL, PRIMARY KEY (j1, j2))", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.k1, a.k2, a.k3, a.av, b.bv "
        "FROM a JOIN b ON a.x = b.x AND a.y = b.y", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]

    a_state, b_state = {}, {}   # (k...) -> (x, y, payload)

    def check(ctx):
        oracle.assert_view_matches(client, vid, ["k1", "k2", "k3", "av", "bv"], Counter(
            (k1, k2, k3, av, bv)
            for (k1, k2, k3), (ax, ay, av) in a_state.items()
            for (bx, by, bv) in b_state.values()
            if (ax, ay) == (bx, by)), ctx=ctx)

    client.execute_sql(
        "INSERT INTO a VALUES (1, 1, 1, 10, 100, 11), (1, 1, 2, 10, 100, 22), "
        "(1, 1, 3, 20, 200, 33)", schema_name=sn)
    a_state.update({(1, 1, 1): (10, 100, 11), (1, 1, 2): (10, 100, 22),
                    (1, 1, 3): (20, 200, 33)})
    client.execute_sql(
        "INSERT INTO b VALUES (5, 5, 10, 100, 70), (6, 6, 20, 999, 80)", schema_name=sn)
    b_state.update({(5, 5): (10, 100, 70), (6, 6): (20, 999, 80)})
    check("rows sharing a 16-byte PK prefix join independently")

    client.execute_sql("UPDATE a SET av = 222 WHERE k1 = 1 AND k2 = 1 AND k3 = 2", schema_name=sn)
    a_state[(1, 1, 2)] = (10, 100, 222)
    check("payload update on one tie-break sibling")

    client.execute_sql("UPDATE b SET y = 200 WHERE j1 = 6 AND j2 = 6", schema_name=sn)
    b_state[(6, 6)] = (20, 200, 80)
    check("re-keying b completes a pair")

    client.execute_sql("DELETE FROM a WHERE k1 = 1 AND k2 = 1 AND k3 = 1", schema_name=sn)
    del a_state[(1, 1, 1)]
    check("deleting one sibling leaves the others intact")


# ── the spellings that compile to one equijoin ─────────────────────────────


@pytest.mark.parametrize("body", [
    "FROM t JOIN u ON t.k = u.k",
    "FROM t GLOBAL JOIN u ON t.k = u.k",       # ClickHouse's name for what DBSP always does
    "FROM t, u WHERE t.k = u.k",
    "FROM t CROSS JOIN u WHERE t.k = u.k",
    "FROM t JOIN u USING (k)",
])
def test_every_spelling_of_one_key_compiles_to_the_same_join(client, schema_name, body):
    """The key can arrive from an ON, from the WHERE above a keyless step, or
    from a USING; none of them is a different graph. A keyless step (comma,
    CROSS) states no keys of its own, so the WHERE keys it exactly as an ON
    would, and the two spellings of one query cannot disagree."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql(f"CREATE VIEW v AS SELECT t.id AS tid, u.id AS uid {body}",
                       schema_name=sn)
    _fill_tu(client, sn)
    assert bag(scanned(client, sn, "v"), "tid", "uid") == _TU_PAIRS


def test_natural_keys_on_every_shared_name(client, schema_name):
    """NATURAL keys on the intersection of the two sides' *visible* names, not on
    one of them: `a`, `b` and `c` share `id`, `k` and `v`, so all three are keys
    and a pair agreeing on only some of them does not join. Chained, the names
    the first step merged away are already gone from the left, so each survives
    the whole chain exactly once instead of pairing again."""
    sn = schema_name
    for name in ("a", "b", "c"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
            schema_name=sn)
    client.execute_sql("CREATE VIEW pair AS SELECT id, k, v FROM a NATURAL JOIN b", schema_name=sn)
    client.execute_sql("CREATE VIEW chain AS SELECT id, k, v FROM a NATURAL JOIN b NATURAL JOIN c",
                       schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 5, 9), (2, 6, 9), (3, 7, 9)", schema_name=sn)
    # b(3) shares only `id` and `v` with a(3), so the shared `k` keeps them apart.
    client.execute_sql("INSERT INTO b VALUES (1, 5, 9), (2, 6, 9), (3, 0, 9)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (1, 5, 9)", schema_name=sn)

    assert bag(scanned(client, sn, "pair"), "id", "k", "v") == {(1, 5, 9): 1, (2, 6, 9): 1}
    assert bag(scanned(client, sn, "chain"), "id", "k", "v") == {(1, 5, 9): 1}
    names = list(client.scan(client.resolve_table(sn, "chain")[0]).mappings()[0].keys())
    assert sorted(names) == ["id", "k", "v"], f"each shared name survives the chain once: {names}"


def test_natural_without_a_shared_name_is_the_product(client, schema_name):
    """SQL's rule: NATURAL over relations sharing no column name is a CROSS JOIN.
    That makes it a keyless step like any other, so the INNER form is the full
    product."""
    sn = schema_name
    client.execute_sql("CREATE TABLE l (id BIGINT PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE TABLE r (rid BIGINT PRIMARY KEY, y BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT x, y FROM l NATURAL JOIN r", schema_name=sn)
    client.execute_sql("INSERT INTO l VALUES (1, 10), (2, 20)", schema_name=sn)
    client.execute_sql("INSERT INTO r VALUES (7, 70), (8, 80), (9, 90)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "x", "y") == \
        {(x, y): 1 for x in (10, 20) for y in (70, 80, 90)}


def test_using_merges_each_named_column_into_one(client, schema_name):
    """`USING (k, v)` is two key pairs and two merges in one step: `SELECT *`
    emits each named column once, and the merged column and the right side's own
    copy hold the same value on every matched row — which is what makes the
    merge a pass-through rather than a computed column."""
    sn = schema_name
    for name in ("t", "u"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
            schema_name=sn)
    client.execute_sql("CREATE VIEW star AS SELECT * FROM t JOIN u USING (k, v)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW qual AS SELECT t.id AS tid, u.id AS uid, k AS merged, u.k AS right_k, v "
        "FROM t JOIN u USING (k, v)", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 5, 50), (2, 5, 51), (3, 6, 60)", schema_name=sn)
    client.execute_sql("INSERT INTO u VALUES (10, 5, 50), (11, 6, 60), (12, 6, 61)",
                       schema_name=sn)

    assert bag(scanned(client, sn, "qual"), "tid", "uid", "merged", "v") == {
        (1, 10, 5, 50): 1, (3, 11, 6, 60): 1,
    }
    for row in client.scan(client.resolve_table(sn, "qual")[0]).mappings():
        assert row["merged"] == row["right_k"], row
    names = list(client.scan(client.resolve_table(sn, "star")[0]).mappings()[0])
    assert names.count("k") == 1 and names.count("v") == 1, names


@pytest.mark.parametrize("side,preserved", [("LEFT", "t"), ("RIGHT", "u")])
def test_a_merged_column_carries_the_preserved_sides_copy(client, schema_name, side, preserved):
    """USING merges two columns into one, so an outer join has to decide which
    copy survives: it is the preserved side's, since an unmatched preserved row
    still has a key of its own while the other side's copy is null-filled. The
    mirrored pair is the case that would break if the merge always kept the left."""
    sn = schema_name
    _tu(client, sn)
    other = "u" if preserved == "t" else "t"
    client.execute_sql(
        f"CREATE VIEW v AS SELECT {preserved}.id AS pid, k AS merged, "
        f"{other}.id AS oid FROM t {side} JOIN u USING (k)", schema_name=sn)
    # t(1) and u(10) share k=7 and match; t(2) at k=8 and u(11) at k=9 do not.
    client.execute_sql("INSERT INTO t VALUES (1, 7, 0), (2, 8, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO u VALUES (10, 7, 0), (11, 9, 0)", schema_name=sn)

    # The unmatched row's `merged` is its own k (8 for LEFT, 9 for RIGHT), never NULL.
    matched, unmatched = ((1, 7, 10), (2, 8, None)) if preserved == "t" \
        else ((10, 7, 1), (11, 9, None))
    assert bag(scanned(client, sn, "v"), "pid", "merged", "oid") == {matched: 1, unmatched: 1}


def test_a_merged_name_stops_being_ambiguous(client, schema_name):
    """Three relations all carrying `k`. Without a merge the left side of the
    third step has two visible `k` and `USING (k)` names neither — the error has
    to say *ambiguous*, since "not found" would send the reader looking for a
    column that is there twice. Once the first step MERGED `k`, the left carries
    one and the third relation pairs with it."""
    sn = schema_name
    for name in ("a", "b", "c"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
            schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match="'k' is ambiguous"):
        client.execute_sql(
            "CREATE VIEW bad AS SELECT a.id AS ai FROM a JOIN b ON a.id = b.id JOIN c USING (k)",
            schema_name=sn)

    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id AS ai, c.id AS ci FROM a JOIN b USING (k) JOIN c USING (k)",
        schema_name=sn)
    for name in ("a", "b", "c"):
        client.execute_sql(f"INSERT INTO {name} VALUES (1, 5, 0), (2, 6, 0)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "ai", "ci") == {(1, 1): 1, (2, 2): 1}


@pytest.mark.parametrize("sql,message", [
    ("SELECT t.id AS tid FROM t FULL OUTER JOIN u USING (k)", "COALESCE"),
    ("SELECT t.id AS tid FROM t NATURAL FULL OUTER JOIN u", "COALESCE"),
    ("SELECT t.id AS tid FROM t JOIN u USING (nope)", "JOIN USING: column 'nope' not found"),
])
def test_a_merge_the_projection_cannot_carry_is_refused(client, schema_name, sql, message):
    """FULL preserves both sides, so its merged column would be
    `COALESCE(l, r)` — a computed expression, which a join projection cannot
    carry. A USING naming a column neither side has has nothing to merge."""
    sn = schema_name
    _tu(client, sn)
    with pytest.raises(gnitz.GnitzError, match=message):
        client.execute_sql(f"CREATE VIEW v AS {sql}", schema_name=sn)


def test_a_keyless_natural_join_cannot_preserve_a_side(client, schema_name):
    """An outer join needs a key to decide what "unmatched" means, so the keyless
    NATURAL (no shared name) has no outer form."""
    sn = schema_name
    client.execute_sql("CREATE TABLE l (id BIGINT PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE TABLE r (rid BIGINT PRIMARY KEY, y BIGINT NOT NULL)",
                       schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match="may be keyless"):
        client.execute_sql("CREATE VIEW v AS SELECT x, y FROM l NATURAL LEFT JOIN r",
                           schema_name=sn)


# ── where a predicate above the join ends up ───────────────────────────────


def test_a_where_equality_becomes_a_key_and_one_that_cannot_stays_a_residual(
        client, schema_name):
    """For an INNER join `ON p WHERE q` and `ON (p AND q)` are the same rows, so
    a WHERE equality spanning both sides is classified as a second key column
    rather than left as a post-join filter. The promotion is a better plan and
    never a requirement: a FLOAT equality cannot be a join key (IEEE-754 breaks
    the byte-equal key contract), so it stays a residual instead of turning a
    working query into an error."""
    sn = schema_name
    for name in ("t", "u"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, "
            f"n BIGINT NOT NULL, f DOUBLE NOT NULL)", schema_name=sn)
    for name, body in (
        ("split", "ON t.k = u.k WHERE t.n = u.n"),
        ("fused", "ON t.k = u.k AND t.n = u.n"),
    ):
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT t.id AS tid, u.id AS uid FROM t JOIN u {body}",
            schema_name=sn)
    client.execute_sql(
        "CREATE VIEW floats AS SELECT t.id AS tid, u.id AS uid "
        "FROM t JOIN u ON t.k = u.k WHERE t.f = u.f", schema_name=sn)

    client.execute_sql("INSERT INTO t VALUES (1, 1, 7, 1.5), (2, 1, 8, 2.5)", schema_name=sn)
    client.execute_sql("INSERT INTO u VALUES (10, 1, 7, 1.5), (11, 1, 9, 9.5)", schema_name=sn)

    for name in ("split", "fused"):
        assert bag(scanned(client, sn, name), "tid", "uid") == {(1, 10): 1}, name
    assert bag(scanned(client, sn, "floats"), "tid", "uid") == {(1, 10): 1}


def test_a_conjunct_naming_one_side_is_pushed_below_the_join(client, schema_name):
    """A WHERE conjunct naming only one input is pushed to the step below it, so
    the rows it removes have to be gone from the result all the same."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT t.id AS tid, u.id AS uid FROM t, u "
        "WHERE t.k = u.k AND t.id > 1", schema_name=sn)
    _fill_tu(client, sn)

    assert bag(scanned(client, sn, "v"), "tid", "uid") == \
        {k: w for k, w in _TU_PAIRS.items() if k[0] > 1}


def test_the_comma_spine_folds_left_deep_under_one_where(client, schema_name):
    """A comma-separated FROM folds left-deep, so every step's key has to be
    found in the one WHERE sitting above the whole spine — including the inner
    step's key, which the outer join cannot see across its own two sides. The
    comma binds loosest, so `FROM a JOIN b ON …, c` is `((a ⋈ b), c)` and the
    WHERE keys only the outer step. All three spellings are one graph."""
    sn = schema_name
    for name in ("a", "b", "c"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, j BIGINT NOT NULL)",
            schema_name=sn)
    for name, body in (
        ("comma", "FROM a, b, c WHERE a.k = b.k AND b.j = c.j"),
        ("explicit", "FROM a JOIN b ON a.k = b.k JOIN c ON b.j = c.j"),
        ("mixed", "FROM a JOIN b ON a.k = b.k, c WHERE b.j = c.j"),
    ):
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT a.id AS ai, b.id AS bi, c.id AS ci {body}",
            schema_name=sn)
    rows = {"a": [(1, 1, 0), (2, 2, 0), (3, 1, 0)],
            "b": [(10, 1, 5), (11, 2, 6), (12, 2, 5)],
            "c": [(20, 0, 5), (21, 0, 6)]}
    for name, vals in rows.items():
        client.execute_sql(f"INSERT INTO {name} VALUES " + ",".join(str(r) for r in vals),
                           schema_name=sn)

    want = Counter((ai, bi, ci)
                   for (ai, ak, _aj) in rows["a"]
                   for (bi, bk, bj) in rows["b"] if ak == bk
                   for (ci, _ck, cj) in rows["c"] if bj == cj)
    for name in ("comma", "explicit", "mixed"):
        assert bag(scanned(client, sn, name), "ai", "bi", "ci") == dict(want), name


def test_a_desugared_join_binds_at_every_nested_site(client, schema_name):
    """A derived table, a CTE and a set-operation branch all bind their body
    through the one relational binder, so the comma form and the USING merge
    reach them with no rule of their own — and none needed."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql("CREATE VIEW tv AS SELECT id, k FROM t", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW derived AS SELECT tid, uid FROM "
        "(SELECT t.id AS tid, u.id AS uid FROM t, u WHERE t.k = u.k) x", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW cte AS WITH x AS (SELECT t.id AS tid, u.id AS uid FROM t, u WHERE t.k = u.k) "
        "SELECT tid, uid FROM x", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW branch AS SELECT t.id AS x FROM t, u WHERE t.k = u.k "
        "UNION ALL SELECT id AS x FROM u", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW merged AS WITH cu AS (SELECT id AS uid, k FROM u) "
        "SELECT tv.id AS tid, cu.uid AS uid FROM tv JOIN cu USING (k)", schema_name=sn)
    _fill_tu(client, sn)

    for name in ("derived", "cte", "merged"):
        assert bag(scanned(client, sn, name), "tid", "uid") == _TU_PAIRS, name
    # The join branch contributes one x per pair (so t(2) arrives at weight 2);
    # the plain branch contributes every u row once.
    assert bag(scanned(client, sn, "branch"), "x") == \
        {(1,): 1, (2,): 2, (3,): 1, (10,): 1, (11,): 1, (12,): 1}


def test_a_merged_column_resolves_in_the_grouped_and_distinct_tails(client, schema_name):
    """GROUP BY and DISTINCT each take their own lowering arm, and both resolve
    names off the same merged scope the plain projection does."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql(
        "CREATE VIEW grouped AS SELECT k, COUNT(*) AS n FROM t JOIN u USING (k) GROUP BY k",
        schema_name=sn)
    client.execute_sql("CREATE VIEW distinct_k AS SELECT DISTINCT k FROM t JOIN u USING (k)",
                       schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 5, 0), (2, 5, 0), (3, 6, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO u VALUES (10, 5, 0), (11, 6, 0), (12, 6, 0)", schema_name=sn)

    # k=5 pairs 2x1, k=6 pairs 1x2; DISTINCT collapses each bag to weight 1.
    assert bag(scanned(client, sn, "grouped"), "k", "n") == {(5, 2): 1, (6, 2): 1}
    assert bag(scanned(client, sn, "distinct_k"), "k") == {(5,): 1, (6,): 1}


def test_a_comma_join_whose_where_carries_only_a_range(client, schema_name):
    """`n_eq == 0 && has_range` satisfies the key-arity rule, so the comma form
    can key on the range slot alone and take the broadcast pure-range path — the
    one keyless spelling whose WHERE never produces an equality."""
    sn = schema_name
    _tu(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT t.id AS tid, u.id AS uid FROM t, u WHERE t.a < u.w",
        schema_name=sn)
    t_rows = [(i, 0, i) for i in range(1, 7)]
    u_rows = [(i, 0, 2 * i) for i in range(1, 7)]
    client.execute_sql("INSERT INTO t VALUES " + ",".join(str(r) for r in t_rows), schema_name=sn)
    client.execute_sql("INSERT INTO u VALUES " + ",".join(str(r) for r in u_rows), schema_name=sn)

    assert bag(scanned(client, sn, "v"), "tid", "uid") == \
        {(ti, ui): 1 for (ti, _, tv) in t_rows for (ui, _, uv) in u_rows if tv < uv}


def test_promotion_across_a_null_filled_left_input(client, schema_name):
    """An INNER step whose left input is a LEFT JOIN, with a WHERE conjunct
    spanning the null-fillable side and the right relation. Promoting it to a key
    changes how a NULL is treated — a NULL equi-join key matches nothing, where a
    residual `NULL = x` is filtered — so the rows a null-fill produced must not
    leak either way."""
    sn = schema_name
    for name in ("a", "b", "c"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
            schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id AS ai, c.id AS ci "
        "FROM a LEFT JOIN b ON a.k = b.k JOIN c ON a.id = c.id WHERE b.v = c.v", schema_name=sn)
    # a(1) matches b (v=9); a(2) matches no b, so its b.v is null-filled.
    client.execute_sql("INSERT INTO a VALUES (1, 5, 0), (2, 99, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (10, 5, 9)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (1, 0, 9), (2, 0, 9)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "ai", "ci") == {(1, 1): 1}, \
        "a(2)'s null-filled b.v must match nothing at all"
