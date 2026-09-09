"""The non-key predicate over a join: written into the ON as a residual, or into
the WHERE above it.

A conjunct the key classifier cannot use — an inequality, a comparison against a
literal, a second range — becomes a linear Filter over the join's output. For an
INNER join that is also what a top-level WHERE becomes, so `ON (k AND p)` and
`ON k WHERE p` are one plan; the two spellings only part company over an OUTER
join, where an ON residual is refused outright and the WHERE is instead a 3VL
filter over the full-width *post*-null-fill output. That difference is the
subject of the second half of this file: a preserved-side predicate keeps
null-filled rows, a predicate over the other side drops them (NULL cmp is
UNKNOWN), and `IS NULL` selects exactly the unmatched ones.

Ground truth is recomputed in Python from state the test maintains, never
scanned back from the engine, and compared as a weighted bag.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/relational_shape/test_join_residual.py
"""
import gnitz
import pytest
import _oracle as oracle
from _read import bag, scanned


def _ab(client, sn, a_cols, b_cols):
    """`a` and `b`, each with a BIGINT PK named `id` plus the given columns."""
    for name, cols in (("a", a_cols), ("b", b_cols)):
        client.execute_sql(f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, {cols})",
                           schema_name=sn)


def _pairs(a, b, pred):
    """`(a.id, b.id) -> 1` for every pair `pred` accepts. Both PKs are unique and
    every base row is weight 1, so a surviving pair carries weight exactly 1."""
    return {(ar["id"], br["id"]): 1
            for ar in a.values() for br in b.values() if pred(ar, br)}


# ── the residual over an INNER join ────────────────────────────────────────


@pytest.mark.parametrize("pred,want", [
    ("orders.amount > 150", {2, 3}),                              # one side, a literal
    ("orders.amount > customers.tier", {1, 2, 3}),                # both sides
    ("orders.amount > 100 AND customers.tier = 1", {3}),          # two conjuncts
])
def test_an_inner_joins_where_and_its_on_residual_are_one_plan(client, schema_name, pred, want):
    """`FROM a JOIN b ON k WHERE p` and `ON (k AND p)` select the same rows for
    an INNER join, so the WHERE conjunct is folded into the same residual Filter
    rather than left as a separate step above the join."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, cid BIGINT NOT NULL, amount BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE TABLE customers (id BIGINT PRIMARY KEY, tier BIGINT NOT NULL)",
                       schema_name=sn)
    for name, tail in (("on_form", f"AND ({pred})"), ("where_form", f"WHERE {pred}")):
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT orders.id AS oid FROM orders "
            f"JOIN customers ON orders.cid = customers.id {tail}", schema_name=sn)
    client.execute_sql("INSERT INTO customers VALUES (10, 1), (20, 2), (30, 100)", schema_name=sn)
    # o(5)'s key has no customer at all, so no predicate can readmit it.
    client.execute_sql(
        "INSERT INTO orders VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300), "
        "(4, 30, 50), (5, 99, 400)", schema_name=sn)

    expect = {(oid,): 1 for oid in want}
    for name in ("on_form", "where_form"):
        assert bag(scanned(client, sn, name), "oid") == expect, name


def test_the_residual_is_maintained_across_deltas_that_flip_it(client, schema_name):
    """`a.k = b.k AND a.v <> b.v`: the equi rows minus those agreeing on `v`. A
    delta that changes only the residual's outcome — an UPDATE either side, on a
    column that is not the key — must still enter or leave the view, since the
    Filter sits above the join and sees the retract/insert pair."""
    sn = schema_name
    _ab(client, sn, "k BIGINT NOT NULL, v BIGINT NOT NULL", "k BIGINT NOT NULL, v BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
        "FROM a JOIN b ON a.k = b.k AND a.v <> b.v", schema_name=sn)
    a_state, b_state = {}, {}

    def check(ctx):
        want = _pairs(a_state, b_state, lambda ar, br: ar["k"] == br["k"] and ar["v"] != br["v"])
        assert bag(scanned(client, sn, "v"), "aid", "bid") == want, ctx

    client.execute_sql("INSERT INTO b VALUES (10, 10, 5), (11, 10, 8), (12, 20, 9)",
                       schema_name=sn)
    oracle.apply_insert(b_state, "id", [{"id": 10, "k": 10, "v": 5}, {"id": 11, "k": 10, "v": 8},
                                        {"id": 12, "k": 20, "v": 9}])
    client.execute_sql("INSERT INTO a VALUES (1, 10, 5), (2, 10, 7), (3, 20, 9)", schema_name=sn)
    oracle.apply_insert(a_state, "id", [{"id": 1, "k": 10, "v": 5}, {"id": 2, "k": 10, "v": 7},
                                        {"id": 3, "k": 20, "v": 9}])
    # a(1) and b(10) agree on v, as do a(3) and b(12): both pairs are filtered out.
    assert bag(scanned(client, sn, "v"), "aid", "bid") == {(1, 11): 1, (2, 10): 1, (2, 11): 1}
    check("seed")

    client.execute_sql("UPDATE b SET v = 7 WHERE id = 11", schema_name=sn)
    oracle.apply_update(b_state, "id", 11, {"v": 7})
    check("b(11) now equals a(2): the pair leaves")

    client.execute_sql("UPDATE a SET v = 6 WHERE id = 1", schema_name=sn)
    oracle.apply_update(a_state, "id", 1, {"v": 6})
    check("a(1) now differs from b(10): the pair enters")

    client.execute_sql("DELETE FROM a WHERE id = 2", schema_name=sn)
    oracle.apply_delete(a_state, "id", [2])
    check("deleting a row drops the pairs that survived the residual")


@pytest.mark.parametrize("on,pred", [
    ("a.k = b.k AND a.lo < b.hi AND a.x > b.y",
     lambda ar, br: ar["k"] == br["k"] and ar["lo"] < br["hi"] and ar["x"] > br["y"]),
    ("a.lo < b.hi AND a.x > b.y",
     lambda ar, br: ar["lo"] < br["hi"] and ar["x"] > br["y"]),
])
def test_a_second_range_rides_above_a_range_shape_as_a_residual(client, schema_name, on, pred):
    """A join carries at most one range in its physical shape — a band (equality
    prefix plus range) or a pure range — so a second inequality has to be a
    residual over its output. Both physical shapes take one."""
    sn = schema_name
    _ab(client, sn,
        "k BIGINT NOT NULL, lo BIGINT NOT NULL, x BIGINT NOT NULL",
        "k BIGINT NOT NULL, hi BIGINT NOT NULL, y BIGINT NOT NULL")
    client.execute_sql(
        f"CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON {on}",
        schema_name=sn)
    a_state, b_state = {}, {}

    def check(ctx):
        assert bag(scanned(client, sn, "v"), "aid", "bid") == \
            _pairs(a_state, b_state, pred), ctx

    client.execute_sql("INSERT INTO b VALUES (10, 1, 50, 5), (11, 1, 10, 5), (12, 2, 99, 0)",
                       schema_name=sn)
    oracle.apply_insert(b_state, "id", [{"id": 10, "k": 1, "hi": 50, "y": 5},
                                        {"id": 11, "k": 1, "hi": 10, "y": 5},
                                        {"id": 12, "k": 2, "hi": 99, "y": 0}])
    client.execute_sql("INSERT INTO a VALUES (1, 1, 20, 9), (2, 1, 5, 1), (3, 2, 1, 7)",
                       schema_name=sn)
    oracle.apply_insert(a_state, "id", [{"id": 1, "k": 1, "lo": 20, "x": 9},
                                        {"id": 2, "k": 1, "lo": 5, "x": 1},
                                        {"id": 3, "k": 2, "lo": 1, "x": 7}])
    check("seed")

    client.execute_sql("UPDATE a SET x = 0 WHERE id = 1", schema_name=sn)
    oracle.apply_update(a_state, "id", 1, {"x": 0})
    check("a(1) now fails the residual x > y")

    client.execute_sql("DELETE FROM b WHERE id = 12", schema_name=sn)
    oracle.apply_delete(b_state, "id", [12])
    check("after deleting b(12)")


def test_a_string_residual_compares_content(client, schema_name):
    """A VARCHAR residual compares string content, not the German-string
    descriptor bytes: every value here shares a 13-character prefix and so is
    out-of-line, differing only in the heap tail, which a descriptor compare
    would wrongly call equal. A literal operand takes the same opcodes."""
    sn = schema_name
    _ab(client, sn, "k BIGINT NOT NULL, s VARCHAR NOT NULL", "k BIGINT NOT NULL, s VARCHAR NOT NULL")
    client.execute_sql(
        "CREATE VIEW pairwise AS SELECT a.id AS aid, b.id AS bid "
        "FROM a JOIN b ON a.k = b.k AND a.s <> b.s", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW literal AS SELECT a.id AS aid, b.id AS bid "
        "FROM a JOIN b ON a.k = b.k AND a.s < 'commonprefix_M'", schema_name=sn)
    client.execute_sql(
        "INSERT INTO b VALUES (10, 1, 'commonprefix_BBBB'), (11, 1, 'commonprefix_SAME')",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO a VALUES (1, 1, 'commonprefix_AAAA'), (2, 1, 'commonprefix_SAME')",
        schema_name=sn)

    # Only a(2) x b(11) share a value, so only that pair is filtered out.
    assert bag(scanned(client, sn, "pairwise"), "aid", "bid") == \
        {(1, 10): 1, (1, 11): 1, (2, 10): 1}
    # 'commonprefix_AAAA' sorts below the literal, 'commonprefix_SAME' above it.
    assert bag(scanned(client, sn, "literal"), "aid", "bid") == {(1, 10): 1, (1, 11): 1}


def test_a_residual_over_a_null_is_unknown_and_drops_the_pair(client, schema_name):
    """3VL, not two-valued: a comparison against NULL is UNKNOWN and an INNER
    join keeps only TRUE, so the pair is dropped until an UPDATE makes the column
    non-NULL. On a NOT NULL column `IS NULL` instead folds to the constant false
    at compile time, which registers a view that is always empty rather than
    rejecting one."""
    sn = schema_name
    _ab(client, sn, "k BIGINT NOT NULL, v BIGINT, x BIGINT NOT NULL", "k BIGINT NOT NULL, v BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW unknown_drops AS SELECT a.id AS aid, b.id AS bid "
        "FROM a JOIN b ON a.k = b.k AND a.v <> b.v", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW folded AS SELECT a.id AS aid, b.id AS bid "
        "FROM a JOIN b ON a.k = b.k AND a.x IS NULL", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (10, 1, 5)", schema_name=sn)
    client.execute_sql("INSERT INTO a VALUES (1, 1, NULL, 7), (2, 1, 5, 7)", schema_name=sn)

    # a(1): NULL <> 5 is UNKNOWN; a(2): 5 <> 5 is false.
    assert bag(scanned(client, sn, "unknown_drops"), "aid", "bid") == {}
    assert bag(scanned(client, sn, "folded"), "aid", "bid") == {}

    client.execute_sql("UPDATE a SET v = 9 WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "unknown_drops"), "aid", "bid") == {(1, 10): 1}
    assert bag(scanned(client, sn, "folded"), "aid", "bid") == {}


def test_a_residual_whose_operands_do_not_share_a_type_is_refused(client, schema_name):
    """`a.s <> b.n` over a VARCHAR and a BIGINT has no comparison to compile, so
    CREATE must fail rather than emit an integer load over a string descriptor."""
    sn = schema_name
    _ab(client, sn, "k BIGINT NOT NULL, s VARCHAR NOT NULL", "k BIGINT NOT NULL, n BIGINT NOT NULL")
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(
            "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
            "FROM a JOIN b ON a.k = b.k AND a.s <> b.n", schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.resolve_table(sn, "v")


# ── the same predicate over an OUTER join ──────────────────────────────────


def test_an_outer_join_refuses_a_residual_in_its_on(client, schema_name):
    """An ON residual would have to run *before* the null-fill decides which
    preserved rows are unmatched, so the predicate would change match existence
    rather than filter the output. The WHERE form is the expressible one."""
    sn = schema_name
    _ab(client, sn, "k BIGINT NOT NULL, t BIGINT NOT NULL", "k BIGINT NOT NULL, t BIGINT NOT NULL")
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(
            "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
            "FROM a LEFT JOIN b ON a.k = b.k AND a.t <> b.t", schema_name=sn)


def test_a_where_over_a_left_join_filters_after_the_null_fill(client, schema_name):
    """The WHERE is one linear 3VL filter over the full-width post-null-fill
    output. So a predicate over the preserved side keeps null-filled rows that
    pass it; a predicate over the other side drops every null-filled row, since
    its columns are NULL and the comparison is UNKNOWN; and `IS NULL` on an
    other-side column selects exactly the unmatched rows — the anti-join."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, cid BIGINT NOT NULL, amount BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE TABLE customers (id BIGINT PRIMARY KEY, tier BIGINT NOT NULL)",
                       schema_name=sn)
    for name, pred in (("preserved", "orders.amount > 150"),
                       ("other_side", "customers.tier > 0"),
                       ("unmatched", "customers.id IS NULL")):
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT orders.id AS oid, customers.tier AS ct "
            f"FROM orders LEFT JOIN customers ON orders.cid = customers.id WHERE {pred}",
            schema_name=sn)
    client.execute_sql("INSERT INTO customers VALUES (10, 5)", schema_name=sn)
    # o(1) matched and over 150; o(2) unmatched and over; o(3) matched and under;
    # o(4) unmatched and under.
    client.execute_sql(
        "INSERT INTO orders VALUES (1, 10, 200), (2, 99, 300), (3, 10, 100), (4, 99, 50)",
        schema_name=sn)

    assert bag(scanned(client, sn, "preserved"), "oid", "ct") == {(1, 5): 1, (2, None): 1}
    assert bag(scanned(client, sn, "other_side"), "oid", "ct") == {(1, 5): 1, (3, 5): 1}
    assert bag(scanned(client, sn, "unmatched"), "oid", "ct") == {(2, None): 1, (4, None): 1}


def test_a_where_over_a_full_join_reaches_both_sides_null_fills(client, schema_name):
    """FULL preserves both sides, so the post-null-fill output carries rows whose
    left columns are NULL as well as rows whose right ones are. One filter sees
    both."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, cid BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE TABLE customers (id BIGINT PRIMARY KEY)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT orders.id AS oid, customers.id AS cid FROM orders "
        "FULL JOIN customers ON orders.cid = customers.id WHERE orders.id IS NOT NULL",
        schema_name=sn)
    client.execute_sql("INSERT INTO customers VALUES (10), (30)", schema_name=sn)
    client.execute_sql("INSERT INTO orders VALUES (1, 10), (2, 99)", schema_name=sn)

    # The FULL output is (o1,c10) matched, (o2,NULL) left-only and (NULL,c30)
    # right-only; the filter keeps the two carrying a real order.
    assert bag(scanned(client, sn, "v"), "oid", "cid") == {(1, 10): 1, (2, None): 1}


@pytest.mark.parametrize("on", [
    "orders.amount < customers.tier",                                 # pure range
    "orders.cid = customers.id AND orders.amount < customers.tier",   # band
])
def test_a_range_or_band_left_join_takes_the_where_the_same_way(client, schema_name, on):
    """The post-null-fill filter is a property of the outer join, not of the
    physical shape underneath it: a pure-range LEFT (which broadcasts one side)
    and a band LEFT (an equality prefix plus a range) both apply the WHERE the
    same way, and both maintain it through a retraction."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, cid BIGINT NOT NULL, amount BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE TABLE customers (id BIGINT PRIMARY KEY, tier BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql(
        f"CREATE VIEW v AS SELECT orders.id AS oid, customers.tier AS ct "
        f"FROM orders LEFT JOIN customers ON {on} WHERE orders.amount > 15", schema_name=sn)
    client.execute_sql("INSERT INTO customers VALUES (10, 50)", schema_name=sn)
    # o(1) matches on both shapes; o(2) fails the range; o(3) fails the equality
    # prefix (and the range on the pure form); o(4) is dropped by the WHERE.
    client.execute_sql(
        "INSERT INTO orders VALUES (1, 10, 30), (2, 10, 100), (3, 99, 40), (4, 10, 10)",
        schema_name=sn)

    band = "customers.id" in on
    assert bag(scanned(client, sn, "v"), "oid", "ct") == {
        (1, 50): 1, (2, None): 1, (3, None if band else 50): 1,
    }

    client.execute_sql("DELETE FROM orders WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "oid", "ct") == {
        (2, None): 1, (3, None if band else 50): 1,
    }
