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
