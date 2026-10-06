"""BOOLEAN: a named type over a stored byte that holds 0 or 1.

It keys, routes and orders as the `U8` it is stored as — FALSE below TRUE — and
is no number: a Python `bool` is the one value a cell takes, and the one a read
hands back. What a BOOLEAN means inside an expression is a per-row program and
lives in `scalar_expression/test_conditions.py`.
"""

import pytest
import gnitz
from _read import bag, rows, scanned


def test_every_literal_spelling_reaches_the_same_stored_value(client):
    """The keywords, the text spellings a BOOLEAN column reads a string as and
    the two literal casts are spellings of two values, and a NULL stays a NULL
    rather than becoming FALSE. A read hands back a `bool`, not the integer it
    is stored as: `1 == True` in Python, so the type is asserted beside the
    value."""
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, b BOOLEAN, c BOOL)")
    _, schema = client.resolve_table("t")
    assert [c.type_code for c in schema.columns[1:]] == [gnitz.TypeCode.BOOLEAN] * 2

    client.execute_sql(
        "INSERT INTO t VALUES (1, TRUE, FALSE), (2, 'yes', 'f'), (3, ' On ', 'NO'), "
        "(4, CAST('t' AS BOOLEAN), CAST(0 AS BOOLEAN)), (5, CAST(7 AS BOOLEAN), '0'), (6, NULL, NULL)")

    got = scanned(client, "t")
    assert bag(got, "id", "b", "c") == {
        (1, True, False): 1, (2, True, False): 1, (3, True, False): 1,
        (4, True, False): 1, (5, True, False): 1, (6, None, None): 1,
    }
    assert {type(v) for r in got for v in (r.b, r.c)} == {bool, type(None)}


def test_a_pushed_cell_is_a_bool_and_nothing_else(client):
    """`push` takes the column's own Python type. An integer is refused where it
    could be read as one — `1` is a number in SQL too."""
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, b BOOLEAN)")
    tid, schema = client.resolve_table("t")
    batch = gnitz.ZSetBatch(schema)
    batch.append(id=1, b=True)
    batch.append(id=2, b=False)
    batch.append(id=3, b=None)
    client.push(tid, batch)
    assert bag(scanned(client, "t"), "id", "b") == {(1, True): 1, (2, False): 1, (3, None): 1}

    for bad in (1, 0, "true", 1.0):
        refused = gnitz.ZSetBatch(schema)
        with pytest.raises(TypeError):
            refused.append(id=9, b=bad)


def test_a_boolean_keys_groups_and_orders(client):
    """A BOOLEAN is a key column like any stored integer: a primary key of two
    values, a group key whose two groups move weight between them when a flag
    flips, and an order in which FALSE is the minimum."""
    client.execute_sql("CREATE TABLE flags (k BOOLEAN NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
    client.execute_sql("INSERT INTO flags VALUES (TRUE, 10), (FALSE, 20)")
    assert bag(rows(client, "SELECT v FROM flags WHERE k = TRUE")) == {(10,): 1}
    assert bag(rows(client, "SELECT v FROM flags WHERE k = FALSE")) == {(20,): 1}
    assert bag(rows(client, "SELECT v FROM flags WHERE k < TRUE")) == {(20,): 1}

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
