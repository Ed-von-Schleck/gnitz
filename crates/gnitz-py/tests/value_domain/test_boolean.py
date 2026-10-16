"""BOOLEAN: a named type over a stored byte that holds 0 or 1.

It keys, routes and orders as the `U8` it is stored as — FALSE below TRUE — and
is no number: a Python `bool` is the one value a cell takes, and the one a read
hands back. What a BOOLEAN means inside an expression is a per-row program and
lives in `scalar_expression/test_conditions.py`.
"""

from _read import bag, scanned


def test_a_boolean_groups_and_orders(client):
    """A BOOLEAN group key's two groups move weight between them when a flag
    flips, and in its order FALSE is the minimum."""
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, b BOOLEAN NOT NULL, n BIGINT NOT NULL)")
    client.execute_sql("CREATE VIEW per AS SELECT b, COUNT(*) AS c, SUM(n) AS s FROM t GROUP BY b")
    client.execute_sql("CREATE VIEW ends AS SELECT MIN(b) AS lo, MAX(b) AS hi FROM t")
    client.execute_sql("INSERT INTO t VALUES (1, TRUE, 1), (2, TRUE, 2), (3, FALSE, 4)")
    assert bag(scanned(client, "per")) == {(True, 2, 3): 1, (False, 1, 4): 1}
    assert bag(scanned(client, "ends")) == {(False, True): 1}

    client.execute_sql("UPDATE t SET b = NOT b WHERE id = 3")
    assert bag(scanned(client, "per")) == {(True, 3, 7): 1}
    assert bag(scanned(client, "ends")) == {(True, True): 1}


def test_a_boolean_join_key_matches_only_its_own_value(client):
    """Two BOOLEAN columns co-partition on the byte they store, so each row
    meets exactly the rows of its own truth value, at every worker count."""
    client.execute_sql("CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, b BOOLEAN NOT NULL)")
    client.execute_sql("CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, b BOOLEAN NOT NULL, tag TEXT NOT NULL)")
    client.execute_sql("CREATE VIEW j AS SELECT l.id, r.tag FROM l JOIN r ON l.b = r.b")
    client.execute_sql("INSERT INTO r VALUES (1, TRUE, 'yes'), (2, FALSE, 'no')")
    client.execute_sql("INSERT INTO l VALUES (10, TRUE), (11, FALSE), (12, TRUE)")
    assert bag(scanned(client, "j")) == {(10, "yes"): 1, (11, "no"): 1, (12, "yes"): 1}

    client.execute_sql("DELETE FROM r WHERE id = 1")
    assert bag(scanned(client, "j")) == {(11, "no"): 1}
