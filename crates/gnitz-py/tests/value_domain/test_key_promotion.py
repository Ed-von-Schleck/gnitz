"""Equal logical keys co-partition, whatever width or signedness each side
declares.

Routing hashes the encoded key bytes, and the two sides of a join reach that
hash by different routes: a PRIMARY KEY column routes by the key region it
already is, a plain column routes by a key the exchange builds for it. Both must
be a pure function of the *logical* value, or the same key lands on two workers
and the match is silently dropped — no error, just missing rows.

When the two sides declare different types the pair first promotes to a common
type wide enough to hold both ranges, and both sides must encode into *that*
type. A cross-sign pair is where this bites: the unsigned side zero-extends and
the signed side sign-extends, so only a shared target makes the two images equal.

The engine's own unit tests sweep the promotion lattice exhaustively over every
type code and worker count. What only an end-to-end test can add is that the
planner picks the common type and the two legs actually meet, so one
representative per arm is the right density rather than the cross-product.

Every case is asserted as a weighted bag over `(key, left tag, right tag)`: a
key claimed by two workers shows up as weight 2, and a dropped match as a
missing row — and a row count sees neither.
"""

import pytest
from _read import bag, scanned
from _serverproc import NEEDS_MULTI

# (left type, right type, keys representable on both sides). One row per arm:
# the identity promotion at each sign, then the cross-sign pairs in both
# directions and at both widths. The keys always include the boundary that
# separates the two encodings: the unsigned maximum for a cross-sign pair, a
# negative for a signed one, and 0 everywhere because it is the value a NULL's
# zero-fill collides with.
_PAIRS = [
    # Same type on both sides takes the identity arm, which is one route
    # whatever the width — the widest of each sign stands for the ladder.
    ("BIGINT", "BIGINT", [-9223372036854775808, -1, 0, 1, 100]),
    ("BIGINT UNSIGNED", "BIGINT UNSIGNED", [0, 1, 2**63, 2**64 - 1]),
    # Cross-sign, in both directions because the two sides reach the hash by
    # different routes. Keys sit above the unsigned side's signed midpoint,
    # where a missing zero-extension shows up.
    ("INT UNSIGNED", "BIGINT", [0, 1, 5, 4_000_000_000]),
    ("BIGINT", "INT UNSIGNED", [0, 1, 5, 4_000_000_000]),
    ("SMALLINT", "TINYINT UNSIGNED", [0, 1, 5, 127]),
]

_IDS = [f"{a}={b}".replace(" ", "").lower() for a, b, _ in _PAIRS]


@pytest.mark.parametrize("cust_sql,ord_sql,keys", _PAIRS, ids=_IDS)
def test_a_join_matches_every_key_across_the_two_declared_types(
        client, schema_name, cust_sql, ord_sql, keys):
    """`customers.id` is a PRIMARY KEY (routed as the key region); `orders.cid`
    is a plain column (routed by the key the exchange builds). Every order must
    reach its own customer, so the tag pairing is asserted rather than the row
    count — a routing slip that paired the right *number* of rows wrongly is
    otherwise invisible."""
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE customers (id {cust_sql} NOT NULL PRIMARY KEY, tag BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        f"CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid {ord_sql} NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT orders.id AS oid, customers.tag AS tag "
        "FROM orders JOIN customers ON orders.cid = customers.id", schema_name=sn)

    client.execute_sql(
        "INSERT INTO customers VALUES " +
        ", ".join(f"({k}, {1000 + i})" for i, k in enumerate(keys)), schema_name=sn)
    client.execute_sql(
        "INSERT INTO orders VALUES " +
        ", ".join(f"({i}, {k})" for i, k in enumerate(keys)), schema_name=sn)

    assert bag(scanned(client, sn, "v"), "oid", "tag") == \
        {(i, 1000 + i): 1 for i in range(len(keys))}

    # A delta after both traces exist takes the seek path rather than the build
    # path, and must route to the same worker the build side landed on.
    client.execute_sql(f"INSERT INTO orders VALUES (99, {keys[-1]})", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "oid", "tag") == \
        {(i, 1000 + i): 1 for i in range(len(keys))} | {(99, 1000 + len(keys) - 1): 1}

    # Retracting it must cancel exactly, leaving no ghost at the promoted key.
    client.execute_sql("DELETE FROM orders WHERE id = 99", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "oid", "tag") == \
        {(i, 1000 + i): 1 for i in range(len(keys))}


@pytest.mark.parametrize("key_sql,keys", [
    ("TINYINT", [-100, -1, 0, 1, 100]),
    ("INT", [-2_000_000_000, -1, 0, 5, 2_000_000_000]),
    ("INT UNSIGNED", [0, 1, 5, 4_000_000_000]),
], ids=["i8", "i32", "u32"])
def test_a_group_by_lands_every_row_of_a_group_on_one_worker(
        client, schema_name, key_sql, keys):
    """Grouping routes by the same key the aggregate is stored under. If the two
    disagreed, one group's rows would be reduced on two workers and each would
    publish a partial — which sums to the right total only by accident, so the
    per-group aggregate is what has to be asserted."""
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, grp {key_sql} NOT NULL, "
        "amount BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT grp, SUM(amount) AS total, COUNT(*) AS n "
        "FROM t GROUP BY grp", schema_name=sn)

    # Two rows per group, so a split group publishes 10 and 5 instead of 15.
    vals = []
    for i, g in enumerate(keys):
        vals += [f"({2 * i}, {g}, 10)", f"({2 * i + 1}, {g}, 5)"]
    client.execute_sql("INSERT INTO t VALUES " + ", ".join(vals), schema_name=sn)
    assert bag(scanned(client, sn, "v"), "grp", "total", "n") == \
        {(g, 15, 2): 1 for g in keys}

    # Retracting one row of a group re-seeks the aggregate by that same key.
    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "grp", "total", "n") == \
        {(g, 15, 2): 1 for g in keys[1:]} | {(keys[0], 10, 1): 1}


@NEEDS_MULTI
def test_a_cross_sign_join_key_surfaces_its_promoted_value(client, schema_name):
    """The `BIGINT UNSIGNED = BIGINT` pair promotes to the signed 128-bit type,
    the only key whose decode has to undo a 16-byte sign flip. Each side holds
    one key the other cannot represent, and neither may match.
    """
    sn = schema_name
    no_match = (2 ** 63) + 10
    max_signed = (2 ** 63) - 1
    client.execute_sql(
        "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, "
        "customer_id BIGINT UNSIGNED NOT NULL, amount BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT customers.name AS name, orders.amount AS amount "
        "FROM orders JOIN customers ON orders.customer_id = customers.id",
        schema_name=sn)

    client.execute_sql(
        f"INSERT INTO customers VALUES (5, 50), (100, 900), (-7, 70), ({max_signed}, 63)",
        schema_name=sn)
    client.execute_sql(
        f"INSERT INTO orders VALUES (1, 5, 1000), (2, 100, 2000), (4, 5, 4000), "
        f"(3, {no_match}, 3000)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "name", "amount") == {
        (50, 1000): 1, (50, 4000): 1, (900, 2000): 1}

    # The hidden key slot rides at the physical key position; decoding it back
    # to 5 / 100 is exactly what the 128-bit sign flip has to undo.
    vid = client.resolve_table(sn, "v")[0]
    got = list(client.scan(vid, include_hidden=True))
    assert bag(got, "_join_pk", "amount") == {(5, 1000): 1, (5, 4000): 1, (100, 2000): 1}
