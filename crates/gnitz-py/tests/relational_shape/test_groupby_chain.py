"""An aggregate or DISTINCT chained to a join: the analytics shape where one
compiled bundle holds a hidden view plus a user-named one.

Two directions, and the file is split along them:

  * the aggregate is the *inner* body — a CTE or a derived table — and the join
    runs over it, so the bundle is `reduce → join`;
  * the aggregate is the *outer* body over a join, so the join compiles to a
    hidden view H and the reduce or distinct runs over it, `join → reduce`. The
    group, aggregate and WHERE columns then resolve against H, and H is pruned to
    the names that outer operator evaluates.

Every assertion is a weighted bag: a chain that emitted a group twice, or left a
retracted row behind at weight 0, would pass a row-set comparison.
"""

import pytest
from _read import bag, scanned


def _orders_customers(client, sn, dim="name VARCHAR(50) NOT NULL"):
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, "
        "amt BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        f"CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, {dim})", schema_name=sn)


# ── The aggregate below the join ──────────────────────────────────────────────


@pytest.mark.parametrize("body", [
    "WITH agg AS (SELECT cid, SUM(amt) AS total FROM orders GROUP BY cid) "
    "SELECT c.name AS nm, agg.total AS tot FROM agg JOIN customers c ON agg.cid = c.id",
    "SELECT c.name AS nm, a.total AS tot "
    "FROM (SELECT cid, SUM(amt) AS total FROM orders GROUP BY cid) a "
    "JOIN customers c ON a.cid = c.id",
], ids=["cte", "derived"])
@pytest.mark.parametrize("prepopulate", [False, True], ids=["incremental", "backfill"])
def test_a_sum_per_group_joined_to_a_dimension_is_maintained(
        client, schema_name, body, prepopulate):
    """The aggregate is the CTE or derived-table body and the join runs over its
    output, so a new fact row must re-aggregate its group and re-join it, and
    retracting a group's last row must drop the joined row entirely. The grouping
    key here is the reduce's natural PK, so no synthetic key is involved."""
    sn = schema_name
    _orders_customers(client, sn)

    def fill():
        client.execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')", schema_name=sn)
        client.execute_sql(
            "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 50), (3, 2, 200)", schema_name=sn)

    if prepopulate:
        fill()
    client.execute_sql(f"CREATE VIEW v AS {body}", schema_name=sn)
    if not prepopulate:
        fill()

    assert bag(scanned(client, sn, "v"), "nm", "tot") == {("Alice", 150): 1, ("Bob", 200): 1}

    client.execute_sql("INSERT INTO orders VALUES (4, 1, 25)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "nm", "tot") == {("Alice", 175): 1, ("Bob", 200): 1}

    client.execute_sql("DELETE FROM orders WHERE id = 3", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "nm", "tot") == {("Alice", 175): 1}


def test_two_aggregate_ctes_join_on_their_group_keys(client, schema_name):
    """Both join inputs are reduce outputs, so the join key is each side's group
    key and a key present in only one side contributes nothing."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, y BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS WITH sa AS (SELECT k, SUM(x) AS sx FROM a GROUP BY k), "
        "sb AS (SELECT k, SUM(y) AS sy FROM b GROUP BY k) "
        "SELECT sa.sx AS sx, sb.sy AS sy FROM sa JOIN sb ON sa.k = sb.k", schema_name=sn)
    # sa: k7 → 30, k9 → 5. sb: k7 → 100, k8 → 200. Only k7 is in both.
    client.execute_sql("INSERT INTO a VALUES (1, 7, 10), (2, 7, 20), (3, 9, 5)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 7, 100), (2, 8, 200)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "sx", "sy") == {(30, 100): 1}


def test_a_filter_cte_feeds_the_group_by(client, schema_name):
    """The reduce's input is a hidden filter view rather than a base table, so
    rows the CTE drops never reach the fold."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, "
        "amt BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS WITH big AS (SELECT id, cid, amt FROM orders WHERE amt > 50) "
        "SELECT cid, COUNT(*) AS n FROM big GROUP BY cid", schema_name=sn)
    client.execute_sql(
        "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 30), (3, 2, 200), (4, 2, 60)",
        schema_name=sn)
    assert bag(scanned(client, sn, "v"), "cid", "n") == {(1, 1): 1, (2, 2): 1}


def test_dropping_the_chained_view_retires_its_hidden_reduce(client, schema_name):
    """The bundle's hidden reduce segment is retired with the user-named view, so
    both source tables are free to drop straight after. `orders` is reached only
    through that hidden segment, and a table under a live view refuses to
    drop — which is what makes the refusal beforehand the precondition and the
    success afterwards the fact."""
    sn = schema_name
    _orders_customers(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS WITH agg AS (SELECT cid, SUM(amt) AS total FROM orders GROUP BY cid) "
        "SELECT c.name AS nm, agg.total AS tot FROM agg JOIN customers c ON agg.cid = c.id",
        schema_name=sn)
    with pytest.raises(Exception, match="dependency"):
        client.execute_sql("DROP TABLE orders", schema_name=sn)

    client.execute_sql("DROP VIEW v", schema_name=sn)
    client.execute_sql("DROP TABLE orders", schema_name=sn)
    client.execute_sql("DROP TABLE customers", schema_name=sn)


# ── The aggregate or DISTINCT above the join ──────────────────────────────────


@pytest.mark.parametrize("prepopulate", [False, True], ids=["incremental", "backfill"])
def test_a_group_by_over_a_join_aggregates_the_dimensions_column(
        client, schema_name, prepopulate):
    """`SELECT k, agg(x) FROM <join> GROUP BY k` with no explicit CTE: the join
    compiles to a hidden view H and the reduce runs over it, resolving the group
    and aggregate columns against H by unqualified name. Derived from scratch over
    a populated join and maintained per tick, both."""
    sn = schema_name
    _orders_customers(client, sn, dim="region BIGINT NOT NULL")

    def fill():
        client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO orders VALUES (1, 1, 50), (2, 1, 30), (3, 2, 70)", schema_name=sn)

    if prepopulate:
        fill()
    client.execute_sql(
        "CREATE VIEW v AS SELECT region AS reg, SUM(amt) AS total "
        "FROM orders JOIN customers ON orders.cid = customers.id GROUP BY region",
        schema_name=sn)
    if not prepopulate:
        fill()

    assert bag(scanned(client, sn, "v"), "reg", "total") == {(100, 80): 1, (200, 70): 1}

    client.execute_sql("INSERT INTO orders VALUES (4, 2, 5)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg", "total") == {(100, 80): 1, (200, 75): 1}

    client.execute_sql("DELETE FROM orders WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg", "total") == {(100, 30): 1, (200, 75): 1}


def test_a_where_over_a_join_is_consumed_inside_the_hidden_circuit(client, schema_name):
    """The WHERE of a grouped-join body runs inside H, so rows it drops never
    reach the fold — under a fresh insert below and above the threshold, and under
    a retraction of a row that was contributing."""
    sn = schema_name
    _orders_customers(client, sn, dim="region BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW v AS SELECT region AS reg, COUNT(*) AS n, SUM(amt) AS s "
        "FROM orders JOIN customers ON orders.cid = customers.id WHERE amt > 40 GROUP BY region",
        schema_name=sn)
    client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO orders VALUES (1, 1, 50), (2, 1, 30), (3, 2, 70)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg", "n", "s") == {
        (100, 1, 50): 1, (200, 1, 70): 1}

    client.execute_sql("INSERT INTO orders VALUES (4, 1, 10), (5, 1, 60)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg", "n", "s") == {
        (100, 2, 110): 1, (200, 1, 70): 1}

    client.execute_sql("DELETE FROM orders WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg", "n", "s") == {
        (100, 1, 60): 1, (200, 1, 70): 1}


def test_a_qualified_where_column_over_a_join_resolves_through_the_alias_map(
        client, schema_name):
    """`orders.id` is carried by both join inputs, so a WHERE bound by bare name
    over H would be ambiguous; the qualifier has to resolve through the join's
    alias map instead. The same holds for a qualified aggregate argument, which
    binds against the join scope directly rather than through H's pruned names."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, "
        "amt BIGINT NOT NULL)", schema_name=sn)
    # customers carries `id` and `amt` too, so both bare names are ambiguous.
    client.execute_sql(
        "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL, "
        "amt BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW vw AS SELECT region AS reg, COUNT(*) AS n "
        "FROM orders JOIN customers ON orders.cid = customers.id WHERE orders.id > 1 "
        "GROUP BY region", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW va AS SELECT SUM(orders.amt) AS s "
        "FROM orders JOIN customers ON orders.cid = customers.id", schema_name=sn)
    client.execute_sql("INSERT INTO customers VALUES (1, 100, 1000), (2, 200, 2000)",
                       schema_name=sn)
    client.execute_sql(
        "INSERT INTO orders VALUES (1, 1, 50), (2, 1, 80), (3, 2, 30)", schema_name=sn)
    # orders.id > 1 keeps o2 (region 100) and o3 (region 200).
    assert bag(scanned(client, sn, "vw"), "reg", "n") == {(100, 1): 1, (200, 1): 1}
    # 50 + 80 + 30; the customers' 1000/2000 must not contribute.
    assert bag(scanned(client, sn, "va"), "s") == {(160,): 1}


def test_a_group_by_and_a_distinct_over_a_three_way_join(client, schema_name):
    """The hidden view H is the whole join tree, not just its last node, so an
    outer reduce or distinct over a fact ⋈ dim ⋈ dim star reads the far
    dimension's column."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, "
        "amt BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT grp, SUM(amt) AS total "
        "FROM a JOIN b ON a.cid = b.id JOIN c ON b.c = c.id GROUP BY grp", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW d AS SELECT DISTINCT grp FROM a JOIN b ON a.cid = b.id JOIN c ON b.c = c.id",
        schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (10, 500)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (500, 7)", schema_name=sn)
    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 10, 50)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "grp", "total") == {(7, 150): 1}
    assert bag(scanned(client, sn, "d"), "grp") == {(7,): 1}

    client.execute_sql("INSERT INTO a VALUES (3, 10, 25)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "grp", "total") == {(7, 175): 1}
    assert bag(scanned(client, sn, "d"), "grp") == {(7,): 1}


@pytest.mark.parametrize("prepopulate", [False, True], ids=["incremental", "backfill"])
def test_a_distinct_over_a_join_dedups_the_projection(client, schema_name, prepopulate):
    """`SELECT DISTINCT … FROM <join>` runs the distinct over H, so several joined
    rows carrying one value collapse to one — and the value leaves only when its
    last carrier does. H is pruned to the projected name."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)",
        schema_name=sn)

    def fill():
        client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200), (3, 300)",
                           schema_name=sn)
        # No order references customer 3, so region 300 is absent at first.
        client.execute_sql("INSERT INTO orders VALUES (1, 1), (2, 1), (3, 2)", schema_name=sn)

    if prepopulate:
        fill()
    client.execute_sql(
        "CREATE VIEW v AS SELECT DISTINCT region AS reg "
        "FROM orders JOIN customers ON orders.cid = customers.id", schema_name=sn)
    if not prepopulate:
        fill()

    assert bag(scanned(client, sn, "v"), "reg") == {(100,): 1, (200,): 1}

    client.execute_sql("INSERT INTO orders VALUES (4, 3)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg") == {(100,): 1, (200,): 1, (300,): 1}

    # Both carriers of region 100 go at once.
    client.execute_sql("DELETE FROM orders WHERE cid = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg") == {(200,): 1, (300,): 1}


def test_a_distinct_over_a_filtered_join_dedups_what_survives_the_where(client, schema_name):
    """The WHERE runs inside H and the distinct over its output, so a second
    above-threshold row for a value adds no row and retracting a value's only
    carrier removes it."""
    sn = schema_name
    _orders_customers(client, sn, dim="region BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW v AS SELECT DISTINCT region AS reg "
        "FROM orders JOIN customers ON orders.cid = customers.id WHERE amt > 40",
        schema_name=sn)
    client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO orders VALUES (1, 1, 50), (2, 1, 30), (3, 2, 70)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg") == {(100,): 1, (200,): 1}

    client.execute_sql("INSERT INTO orders VALUES (4, 1, 90)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg") == {(100,): 1, (200,): 1}

    client.execute_sql("DELETE FROM orders WHERE id = 3", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg") == {(100,): 1}


def test_min_max_over_a_joined_column_is_retained_in_the_pruned_hidden_view(
        client, schema_name):
    """H keeps exactly the names the outer operator evaluates, so an aggregate
    argument survives the pruning — and MIN/MAX, being non-linear, then have to
    track a new extreme and recede when the row holding one is retracted."""
    sn = schema_name
    _orders_customers(client, sn, dim="region BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW v AS SELECT region AS reg, MIN(amt) AS lo, MAX(amt) AS hi "
        "FROM orders JOIN customers ON orders.cid = customers.id GROUP BY region",
        schema_name=sn)
    client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO orders VALUES (1, 1, 50), (2, 1, 80), (3, 2, 30)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg", "lo", "hi") == {
        (100, 50, 80): 1, (200, 30, 30): 1}

    client.execute_sql("INSERT INTO orders VALUES (4, 1, 20)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg", "lo", "hi") == {
        (100, 20, 80): 1, (200, 30, 30): 1}

    client.execute_sql("DELETE FROM orders WHERE id = 2", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg", "lo", "hi") == {
        (100, 20, 50): 1, (200, 30, 30): 1}


def test_a_count_star_over_a_join_collects_no_names_and_keeps_the_wildcard(
        client, schema_name):
    """`COUNT(*)` names no column, so the pruning has an empty set to work from
    and H keeps its wildcard projection — the degenerate case, which must still
    register and count."""
    sn = schema_name
    _orders_customers(client, sn, dim="region BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW v AS SELECT COUNT(*) AS n "
        "FROM orders JOIN customers ON orders.cid = customers.id", schema_name=sn)
    client.execute_sql("INSERT INTO customers VALUES (1, 100), (2, 200)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO orders VALUES (1, 1, 50), (2, 1, 80), (3, 2, 30)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "n") == {(3,): 1}

    client.execute_sql("DELETE FROM orders WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "n") == {(2,): 1}


# ── The aggregate or DISTINCT above an OUTER join ─────────────────────────────


def test_a_group_by_over_a_left_join_groups_the_null_fills_by_where_placement(
        client, schema_name):
    """Over a LEFT JOIN the WHERE decides what happens to the null-filled rows: a
    preserved-side predicate keeps them and they form a NULL group, a right-side
    predicate drops them (a comparison against NULL is UNKNOWN), and
    `right.col IS NULL` keeps exactly the unmatched ones. `id` is carried by both
    sides, so that last qualifier must resolve through the join's alias map.
    Maintained under churn on the preserved side."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, amt BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW vp AS SELECT region AS reg, COUNT(*) AS n "
        "FROM a LEFT JOIN b ON a.k = b.id WHERE a.amt > 40 GROUP BY region", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW vr AS SELECT region AS reg, COUNT(*) AS n "
        "FROM a LEFT JOIN b ON a.k = b.id WHERE region > 0 GROUP BY region", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW vn AS SELECT COUNT(*) AS n "
        "FROM a LEFT JOIN b ON a.k = b.id WHERE b.id IS NULL", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (10, 500)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO a VALUES (1, 10, 50), (2, 99, 70), (3, 10, 30), (4, 88, 20)",
        schema_name=sn)
    # amt > 40 keeps a1 (matched, region 500) and a2 (unmatched, region NULL).
    assert bag(scanned(client, sn, "vp"), "reg", "n") == {(500, 1): 1, (None, 1): 1}
    # region > 0 keeps only the matched a1 and a3.
    assert bag(scanned(client, sn, "vr"), "reg", "n") == {(500, 2): 1}
    assert bag(scanned(client, sn, "vn"), "n") == {(2,): 1}

    client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=sn)
    client.execute_sql("INSERT INTO a VALUES (5, 77, 80)", schema_name=sn)
    assert bag(scanned(client, sn, "vp"), "reg", "n") == {(None, 2): 1}     # a2, a5
    assert bag(scanned(client, sn, "vr"), "reg", "n") == {(500, 1): 1}      # a3 alone
    assert bag(scanned(client, sn, "vn"), "n") == {(3,): 1}                 # a2, a4, a5


def test_a_group_by_over_a_band_left_join_groups_the_range_misses_as_null(
        client, schema_name):
    """A band LEFT JOIN matches on an equality prefix and then a range, so a row
    whose key matches but whose range does not is unmatched and null-fills like
    any other. Under a preserved-side WHERE, and under the retraction of the only
    matched row."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL, "
        "amt BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, hi BIGINT NOT NULL, "
        "region BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT region AS reg, COUNT(*) AS n "
        "FROM a LEFT JOIN b ON a.k = b.k AND a.lo < b.hi WHERE amt > 40 GROUP BY region",
        schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (10, 1, 100, 500)", schema_name=sn)
    # a1 matches (k=1, 50<100); a2 misses on the key; a3 is filtered out by amt.
    client.execute_sql(
        "INSERT INTO a VALUES (1, 1, 50, 70), (2, 2, 10, 80), (3, 1, 50, 30)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg", "n") == {(500, 1): 1, (None, 1): 1}

    client.execute_sql("INSERT INTO a VALUES (4, 3, 5, 100)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg", "n") == {(500, 1): 1, (None, 2): 1}

    client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg", "n") == {(None, 2): 1}


def test_a_distinct_over_a_pure_range_left_join_dedups_matches_and_null_fills(
        client, schema_name):
    """A pure-range LEFT JOIN has no equality prefix at all. Matched rows carry
    the right value and unmatched ones carry NULL, so the distinct over them holds
    one entry per distinct value plus one for NULL, which leaves when its last
    unmatched carrier does."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, lo BIGINT NOT NULL, amt BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, hi BIGINT NOT NULL, "
        "region BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT DISTINCT region AS reg "
        "FROM a LEFT JOIN b ON a.lo < b.hi WHERE amt > 40", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (10, 100, 500)", schema_name=sn)
    # a1 and a3 match (lo < 100); a2 does not.
    client.execute_sql(
        "INSERT INTO a VALUES (1, 50, 70), (2, 200, 80), (3, 60, 90)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg") == {(500,): 1, (None,): 1}

    client.execute_sql("DELETE FROM a WHERE id = 2", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "reg") == {(500,): 1}
