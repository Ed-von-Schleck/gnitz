"""Upsert and delete-by-PK across a partitioned table.

Which worker owns a PK is the engine's business, so a write addressed by PK has
to reach that worker and only that one. At one worker the routing is a no-op and
these pass without having checked it, which is what the module-level skip
records.
"""

from _serverproc import NEEDS_MULTI

pytestmark = NEEDS_MULTI

_CREATE = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"


def test_upsert_reaches_the_owning_worker(client, schema_name):
    """`ON CONFLICT DO UPDATE` on a committed PK replaces that row wherever it
    lives: one row survives, at the new value and weight 1. A conflict resolved
    on the wrong worker would leave the original beside the new one."""
    client.execute_sql(_CREATE, schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 200) ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
        schema_name=schema_name,
    )

    tid = client.resolve_table(schema_name, "t")[0]
    rows = list(client.scan(tid))
    assert [(r.pk, r.val, r.weight) for r in rows] == [(1, 200, 1)]


def test_delete_by_pk_retracts_on_the_owning_worker_and_in_a_view(client, schema_name):
    """A DELETE addressed by PK retracts on exactly the worker holding it, and
    the retraction reaches a passthrough view fanned out over every worker —
    which is where a delete routed to the wrong partition would show up as a row
    the base table has lost but the view still carries."""
    client.execute_sql(_CREATE, schema_name=schema_name)
    client.execute_sql("CREATE VIEW v AS SELECT * FROM t", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
                       schema_name=schema_name)
    client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=schema_name)

    tid = client.resolve_table(schema_name, "t")[0]
    vid = client.resolve_table(schema_name, "v")[0]
    expected = [(1, 10, 1), (3, 30, 1)]
    assert sorted((r.pk, r.val, r.weight) for r in client.scan(tid)) == expected
    # A view runs no `enforce_unique_pk`, so a doubly-applied tick would keep
    # this row set and double the weights.
    assert sorted((r.pk, r.val, r.weight) for r in client.scan(vid)) == expected
