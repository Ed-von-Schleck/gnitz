"""The linear body: one relation under a WHERE and a projection.

The base case every other shape in this directory is built on — no segment, no
combine node, no synthetic key. Asserted on weights, because the failure this
shape can have is a delta applied twice or a ghost left at weight 0, and a row
set reads both as correct.
"""

from _read import bag, scanned


def test_a_where_admits_and_retracts_rows_at_weight_one(client, schema_name):
    """A row crossing the predicate enters the view at weight 1 and a row leaving
    it is retracted to nothing, whichever write moves it."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT val FROM t WHERE val > 10", schema_name=sn)

    client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 15), (3, 25)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "val") == {(15,): 1, (25,): 1}

    client.execute_sql("UPDATE t SET val = 30 WHERE pk = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "val") == {(15,): 1, (25,): 1, (30,): 1}

    client.execute_sql("UPDATE t SET val = 0 WHERE pk = 2", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "val") == {(25,): 1, (30,): 1}

    client.execute_sql("DELETE FROM t WHERE pk = 3", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "val") == {(30,): 1}


def test_a_wildcard_expands_through_its_modifiers_and_stays_maintained(client, schema_name):
    """`EXCEPT`/`EXCLUDE` drop a column from the expansion and `RENAME` renames
    one, so the wildcard decides the view's column list before any expression is
    lowered. The dropped column must be absent from the presented row rather than
    merely unread, and the reduced projection maintains like any other linear
    body — a delta applied twice shows up in the weights.

    The two spellings of the drop are one set, so a column named by either is
    gone; naming the same column in both is not an error.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        "b BIGINT NOT NULL, c BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT * EXCEPT (b) RENAME (a AS years) FROM t", schema_name=sn)
    client.execute_sql("CREATE VIEW w AS SELECT * EXCLUDE (b, c) FROM t", schema_name=sn)

    client.execute_sql("INSERT INTO t VALUES (1, 10, 100, 7), (2, 20, 200, 8)",
                       schema_name=sn)
    assert bag(scanned(client, sn, "v"), "pk", "years", "c") == {(1, 10, 7): 1, (2, 20, 8): 1}
    assert bag(scanned(client, sn, "w"), "pk", "a") == {(1, 10): 1, (2, 20): 1}

    # The dropped and renamed names are gone from the presented row, and no
    # extra column arrived with them.
    assert set(next(iter(scanned(client, sn, "v")))._fields) == {"pk", "years", "c"}

    client.execute_sql("INSERT INTO t VALUES (3, 30, 300, 9)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "pk", "years", "c") == {
        (1, 10, 7): 1, (2, 20, 8): 1, (3, 30, 9): 1}
    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "pk", "years", "c") == {(2, 20, 8): 1, (3, 30, 9): 1}
