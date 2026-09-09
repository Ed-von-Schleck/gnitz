"""A DDL ACKed while workers are parked mid-epoch must not leave a worker
serving traffic against a stale catalog.

The `relay_hold_server` seam holds the first exchange relay until a DDL requests
the tick quiesce, so every test here reaches the race by ordering — no sleeps, no
elapsed-time assertions.
"""


import gnitz
from _uid import uid as _uid

PARKED_ROWS = 20_000
PARKED_GROUPS = 8


def _scalar(client, sn, q):
    res = client.execute_sql(q, schema_name=sn)[0]
    assert res["type"] == "Rows", f"expected Rows, got {res['type']}: {res}"
    return list(res["rows"])[0][0]


def _park_workers(client, sn):
    """Leave every worker blocked in its exchange wait behind the held relay.

    The GROUP BY view seeds an exchange, and the push crosses the tick-coalesce
    threshold, so the tick group is emitted before push() returns.
    """
    client.execute_sql(
        "CREATE TABLE src (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a, COUNT(*) AS n FROM src GROUP BY a",
        schema_name=sn)
    tid, schema = client.resolve_table(sn, "src")
    batch = gnitz.ZSetBatch(schema)
    for i in range(PARKED_ROWS):
        batch.append(pk=i, a=i % PARKED_GROUPS)
    client.push(tid, batch)


def _assert_view_intact(client, sn):
    """The view the parked epoch was maintaining still holds every source row
    exactly once. A worker applying a push under a stale catalog corrupts the
    view's weights without necessarily disturbing the base table, so the base
    row count alone would not catch it."""
    assert _scalar(client, sn, "SELECT COUNT(*) FROM v") == PARKED_GROUPS
    assert _scalar(client, sn, "SELECT SUM(n) FROM v") == PARKED_ROWS


def test_create_index_then_indexed_delete(relay_hold_server):
    """CREATE INDEX + SEEK_BY_INDEX: a worker that deferred the IDX_TAB DdlSync
    answers 'No index on cols …', failing the DELETE."""
    c = relay_hold_server
    sn = "s" + _uid()
    c.create_schema(sn)
    c.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)",
        schema_name=sn)
    c.execute_sql("INSERT INTO t VALUES (1, 100), (2, 200)", schema_name=sn)
    _park_workers(c, sn)

    c.execute_sql("CREATE INDEX ix ON t (c)", schema_name=sn)
    c.execute_sql("DELETE FROM t WHERE c = 100", schema_name=sn)

    assert _scalar(c, sn, "SELECT COUNT(*) FROM t") == 1
    _assert_view_intact(c, sn)


def test_create_table_then_transactional_insert(relay_hold_server):
    """CREATE TABLE + a transactional INSERT: COMMIT reports Ok either way, so the
    row count is the only witness that a worker which deferred the TABLE_TAB
    DdlSync did not drop its partition's share of the transaction."""
    c = relay_hold_server
    sn = "s" + _uid()
    c.create_schema(sn)
    _park_workers(c, sn)

    c.execute_sql(
        "CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=sn)

    values = ", ".join(f"({i}, {i * 10})" for i in range(64))
    c.execute_sql("BEGIN", schema_name=sn)
    c.execute_sql(f"INSERT INTO t2 VALUES {values}", schema_name=sn)
    c.execute_sql("COMMIT", schema_name=sn)

    assert _scalar(c, sn, "SELECT COUNT(*) FROM t2") == 64
    _assert_view_intact(c, sn)


def test_create_view_over_a_parked_source(relay_hold_server):
    """CREATE VIEW while an exchange tick is already in flight: the DDL's own
    quiesce has to drain the parked epoch before it registers a second view over
    the same source.

    A view that read the source mid-epoch would double-count the parked rows or
    miss them, so both views are asserted — the one that was already maintaining
    across the park, and the one built during it.
    """
    c = relay_hold_server
    sn = "s" + _uid()
    c.create_schema(sn)
    _park_workers(c, sn)

    c.execute_sql("CREATE VIEW v2 AS SELECT a, COUNT(*) AS n FROM src GROUP BY a",
                  schema_name=sn)

    _assert_view_intact(c, sn)
    assert _scalar(c, sn, "SELECT COUNT(*) FROM v2") == PARKED_GROUPS
    assert _scalar(c, sn, "SELECT SUM(n) FROM v2") == PARKED_ROWS
