"""The equi outer join: LEFT, RIGHT and FULL over an equality key.

There is no fused outer opcode. `LEFT JOIN = inner ∪ null_extend(ν)`, where `ν`
is the unmatched preserved rows at their true multiplicity: per preserved
identity `x` at weight `w_A ≥ 0`, and `S ≥ 0` the summed other-side weight it
matches, `ν(x) = w_A · [S = 0]`. RIGHT is that construction with the two sides
swapped; FULL runs it on both.

Every assertion is a weighted bag. The failure mode of a wrong ν is a spurious
weight-`w−1` null-fill beside a matched weight-`w` row, which a row count and a
`sorted(pks)` list each read as correct.

The band and pure-range orientations build ν differently and live in
test_range_join.py.

Run with GNITZ_WORKERS=4: ν must cancel per worker, before the output exchange.
"""

import pytest
from _read import bag

_COLS = ("oid", "amt", "cust", "cname")


@pytest.fixture
def oc(client, schema_name):
    """Empty `orders(pk, ckey, amount)` / `customers(pk, ckey, name)` in a fresh
    schema, joined on `orders.ckey = customers.ckey`.

    The key is nullable on both sides so a row can be unmatched by 3VL as well as
    by absence; every payload column is NOT NULL, so a NULL reaching a payload is
    the outer join's widening and nothing else.
    """
    client.execute_sql(
        "CREATE TABLE orders (pk BIGINT NOT NULL PRIMARY KEY, ckey BIGINT, "
        "amount BIGINT NOT NULL)", schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE customers (pk BIGINT NOT NULL PRIMARY KEY, ckey BIGINT, "
        "name BIGINT NOT NULL)", schema_name=schema_name)
    return schema_name


def _view(client, sn, kind, cols="orders.pk AS oid, orders.amount AS amt, "
                                "customers.pk AS cust, customers.name AS cname"):
    """`orders <kind> JOIN customers ON ckey`, returning the view id. `kind` is
    the one thing that varies across the orientation cases."""
    client.execute_sql(
        f"CREATE VIEW v AS SELECT {cols} FROM orders {kind} JOIN customers "
        f"ON orders.ckey = customers.ckey", schema_name=sn)
    return client.resolve_table(sn, "v")[0]


# Order 1 matches customer 100. Order 2 matches no customer, order 3 matches
# nothing because its key is NULL; customer 200 and (by its NULL key) customer
# 300 are the mirror pair on the right.
_MATCHED = {(1, 50, 100, 7): 1}
_LEFT_ONLY = {(2, 60, None, None): 1, (3, 70, None, None): 1}
_RIGHT_ONLY = {(None, None, 200, 8): 1, (None, None, 300, 9): 1}


@pytest.mark.parametrize("kind,want", [
    ("INNER", _MATCHED),
    ("LEFT", _MATCHED | _LEFT_ONLY),
    ("RIGHT", _MATCHED | _RIGHT_ONLY),
    ("FULL", _MATCHED | _LEFT_ONLY | _RIGHT_ONLY),
])
def test_an_orientation_emits_the_inner_rows_plus_its_own_side_null_fills(
        client, oc, kind, want):
    """Each orientation is the inner join plus the ν of each side it preserves,
    every row at weight 1. ν reads the *unfiltered* input, so a preserved row
    that matches nothing because its key is NULL null-fills exactly like one
    whose key simply has no partner — INNER, which preserves neither side, drops
    both."""
    vid = _view(client, oc, kind)
    client.execute_sql(
        "INSERT INTO customers VALUES (100, 10, 7), (200, 20, 8), (300, NULL, 9)",
        schema_name=oc)
    client.execute_sql(
        "INSERT INTO orders VALUES (1, 10, 50), (2, 99, 60), (3, NULL, 70)",
        schema_name=oc)

    assert bag(client.scan(vid), *_COLS) == want


def test_a_preserved_row_null_fills_once_however_many_rows_match_its_partner(
        client, oc):
    """ν is `[S = 0]`, not a per-match count: a customer matched by two orders
    yields two inner rows and no null-fill, and each unmatched row on either side
    null-fills exactly once."""
    vid = _view(client, oc, "FULL")
    client.execute_sql(
        "INSERT INTO customers VALUES (100, 10, 7), (200, 20, 8)", schema_name=oc)
    client.execute_sql(
        "INSERT INTO orders VALUES (1, 10, 50), (2, 10, 60), (3, 99, 70)",
        schema_name=oc)

    assert bag(client.scan(vid), *_COLS) == {
        (1, 50, 100, 7): 1,
        (2, 60, 100, 7): 1,
        (3, 70, None, None): 1,
        (None, None, 200, 8): 1,
    }


def test_a_null_fill_retracts_when_its_row_gains_a_match_and_returns_when_it_loses_one(
        client, oc):
    """The whole null-fill life cycle on both preserved sides at once. A row that
    gains a partner has its ν retracted in the same epoch the inner row appears;
    losing the partner re-emits ν. Deleting a *matched* preserved row retracts
    only its inner row: the passthrough `+x` and the matched term's `−x` are
    byte-identical and cancel, so no (x, NULL) tombstone survives."""
    vid = _view(client, oc, "FULL")

    def pairs():
        return bag(client.scan(vid), "oid", "cust")

    client.execute_sql("INSERT INTO orders VALUES (1, 10, 50)", schema_name=oc)
    client.execute_sql("INSERT INTO customers VALUES (200, 20, 8)", schema_name=oc)
    assert pairs() == {(1, None): 1, (None, 200): 1}, "neither side has a partner"

    client.execute_sql("INSERT INTO customers VALUES (100, 10, 7)", schema_name=oc)
    assert pairs() == {(1, 100): 1, (None, 200): 1}, "order 1's ν retracts"

    client.execute_sql("INSERT INTO orders VALUES (2, 20, 60)", schema_name=oc)
    assert pairs() == {(1, 100): 1, (2, 200): 1}, "customer 200's ν retracts"

    client.execute_sql("DELETE FROM customers WHERE pk = 100", schema_name=oc)
    assert pairs() == {(1, None): 1, (2, 200): 1}, "order 1's ν returns"

    client.execute_sql("DELETE FROM orders WHERE pk = 2", schema_name=oc)
    assert pairs() == {(1, None): 1, (None, 200): 1}, "customer 200's ν returns"

    client.execute_sql("DELETE FROM orders WHERE pk = 1", schema_name=oc)
    assert pairs() == {(None, 200): 1}, "deleting an unmatched row retracts its ν"

    client.execute_sql("INSERT INTO orders VALUES (3, 20, 70)", schema_name=oc)
    client.execute_sql("DELETE FROM orders WHERE pk = 3", schema_name=oc)
    assert pairs() == {(None, 200): 1}, "a matched row leaves no ν tombstone"


def test_the_null_fill_is_weight_exact_over_a_bag_valued_preserved_side(
        client, schema_name):
    """A preserved side whose identities carry weight > 1 — a `UNION ALL` view,
    where two source rows collapse onto one `_set_pk` identity of weight 2.

    ν subtracts the RAW matched multiplicity `w_A · S` and clamps the result, so
    a matched weight-2 identity reaches `w_A · (1 − S) ≤ 0` and null-fills not at
    all; an unmatched one null-fills at the full weight 2. Clamping the *witness*
    to 1 instead would leak a weight-1 null-fill beside the matched rows.

    Driven on the RIGHT side: the LEFT one is covered by the ν-coarsening cases
    in test_join_payload_pruning.py and test_multiway_join.py.
    """
    sn = schema_name
    for tbl in ("src", "other", "lt"):
        client.execute_sql(
            f"CREATE TABLE {tbl} (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
            schema_name=sn)
    client.execute_sql(
        "CREATE VIEW u AS SELECT g FROM src UNION ALL SELECT g FROM other",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT u.g AS g, lt.pk AS lid FROM lt RIGHT JOIN u ON lt.g = u.g",
        schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]

    # g=5 twice (one matched weight-2 identity), g=9 twice (unmatched).
    client.execute_sql("INSERT INTO src VALUES (1, 5), (2, 5), (3, 9), (4, 9)",
                       schema_name=sn)
    client.execute_sql("INSERT INTO lt VALUES (1, 5)", schema_name=sn)
    assert bag(client.scan(vid), "g", "lid") == {(5, 1): 2, (9, None): 2}

    # Retract the only match: the matched identity flips to a full weight-2 ν.
    client.execute_sql("DELETE FROM lt WHERE pk = 1", schema_name=sn)
    assert bag(client.scan(vid), "g", "lid") == {(5, None): 2, (9, None): 2}


def test_a_composite_key_null_fills_on_the_whole_key(client, schema_name):
    """A k=2 equality key: ν subtracts on both key columns, so a left row that
    agrees on only one of them is unmatched and null-fills, and the b-row that
    completes the pair retracts that null-fill."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, "
        "y BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, "
        "y BIGINT NOT NULL, bv BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.x, a.y, b.bv FROM a LEFT JOIN b "
        "ON a.x = b.x AND a.y = b.y", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]

    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
    # b1 agrees with a2 on x only — not a match.
    client.execute_sql("INSERT INTO b VALUES (1, 10, 100, 11), (2, 20, 999, 22)",
                       schema_name=sn)
    assert bag(client.scan(vid), "x", "y", "bv") == {
        (10, 100, 11): 1, (20, 200, None): 1}

    client.execute_sql("INSERT INTO b VALUES (3, 20, 200, 33)", schema_name=sn)
    assert bag(client.scan(vid), "x", "y", "bv") == {
        (10, 100, 11): 1, (20, 200, 33): 1}
