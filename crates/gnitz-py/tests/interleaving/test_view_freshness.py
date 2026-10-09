"""A read of a view reflects every commit ACKed before it, and waits only for its
own sources' ticks.

Every read verb that answers "what is current" asks whether the target's
transitive source closure committed at or below the last emitted tick's
watermark, and drains when it did not. A tick fires when a push crosses the
row-coalesce threshold or when a read drains, so a small push sits pending until
the read that needs it; and a read written behind a tick still in flight is
answered by each worker only once its drive has ended.
"""

import gnitz
from _read import bag, scanned
from _serverproc import join_or_fail, spawn

# 4x the 10 000-row coalesce threshold: crossing it fires the auto-tick, and the
# excess is how long that tick is still running when the next statement lands —
# the race window, not a scale knob.
BIG_ROWS = 40_000

_TABLE = "CREATE TABLE {} (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"


def test_a_seek_reflects_every_acked_push_under_concurrent_ddl(client, server):
    """Connection A pushes key k; connection B then seeks it through a view over a
    view, and through the view beneath. Each must return k at weight 1: the
    drain runs off shared master state, so it covers another connection's commit,
    and one drain cascades down the chain. A view runs no `enforce_unique_pk`, so
    a tick applied twice shows as weight 2.

    A third connection churns CREATE/DROP VIEW on an unrelated table throughout.
    The seek releases the catalog read lock to drain and re-takes it; a drain
    under the lock would deadlock against the writer-preferring DDL."""
    for sql in (_TABLE.format("t"), _TABLE.format("t2"),
                "CREATE VIEW v1 AS SELECT * FROM t WHERE val >= 0",
                "CREATE VIEW v2 AS SELECT * FROM v1 WHERE val >= 0"):
        client.execute_sql(sql)
    tid, schema = client.resolve_table("t")
    (v1, v1_schema), (v2, v2_schema) = (client.resolve_table(v) for v in ("v1", "v2"))

    def churn():
        with gnitz.connect(server, schema=client.schema) as c:
            for i in range(20):
                c.execute_sql(f"CREATE VIEW dv{i} AS SELECT * FROM t2 WHERE val >= 0")
                c.execute_sql(f"DROP VIEW dv{i}")

    th = spawn(churn)
    k = 0
    try:
        with gnitz.connect(server) as b:
            # Until the churn is done, so every DDL lands inside the loop.
            while k < 30 or (th.is_alive() and k < 10_000):
                k += 1
                client.push(tid, gnitz.ZSetBatch(schema).append(pk=k, val=k * 10))
                assert bag(b.seek(v2, v2_schema, pk=k), "pk", "val") == {(k, k * 10): 1}, f"v2 seek {k}"
                assert bag(b.seek(v1, v1_schema, pk=k), "pk", "val") == {(k, k * 10): 1}, f"v1 seek {k}"
    finally:
        join_or_fail("the DDL churn hung", th)
    assert bag(scanned(client, "v2"), "pk", "val") == {(i, i * 10): 1 for i in range(1, k + 1)}


def test_a_read_waits_for_its_own_in_flight_tick(client):
    """A push crossing the coalesce threshold ACKs while the tick it fired is
    still running, and the read that follows with nothing in between must see
    every group at weight 1. The pending-tick queue is already empty once the tick
    is dequeued, so a freshness check keyed on it would serve the view with none
    of its groups.

    A second such push fires another tick, and INSERTs run against it: each takes
    the PK-rejection probe burst into workers still evaluating the GROUP BY's
    exchange. Neither may wedge, and the view converges exactly."""
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)")
    client.execute_sql("CREATE VIEW v AS SELECT g, COUNT(*) AS n FROM t GROUP BY g")
    tid, schema = client.resolve_table("t")
    vid, v_schema = client.resolve_table("v")

    def push_range(lo, hi):
        client.push(tid, gnitz.ZSetBatch(schema).extend([{"pk": i, "g": i} for i in range(lo, hi)]))

    push_range(0, BIG_ROWS)
    assert bag(client.scan(vid, v_schema), "g", "n") == {(i, 1): 1 for i in range(BIG_ROWS)}

    push_range(BIG_ROWS, 2 * BIG_ROWS)
    end = 2 * BIG_ROWS + 300
    for lo in range(2 * BIG_ROWS, end, 100):
        client.execute_sql("INSERT INTO t VALUES " + ", ".join(f"({i}, {i})" for i in range(lo, lo + 100)))
    assert bag(client.scan(vid, v_schema), "g", "n") == {(i, 1): 1 for i in range(end)}


def test_an_unrelated_pending_tick_does_not_gate_the_read(client):
    """A read of a clean `v1` must not tick `t2`, which `v1` does not depend on.

    Asserted on the watermark a scan reports, which only a tick moves:
    had the read drained, it would jump past `t2`'s commit."""
    for sql in (_TABLE.format("t1"), _TABLE.format("t2"),
                "CREATE VIEW v1 AS SELECT pk, val FROM t1 WHERE val >= 0",
                "CREATE VIEW v2 AS SELECT val, COUNT(*) AS n FROM t2 GROUP BY val"):
        client.execute_sql(sql)
    (t1, schema), (t2, _) = client.resolve_table("t1"), client.resolve_table("t2")
    (v1, v1_schema), (v2, v2_schema) = (client.resolve_table(v) for v in ("v1", "v2"))

    def push_small(tid):
        # Under the coalesce threshold: no auto-tick, the tid sits pending.
        return client.push(tid, gnitz.ZSetBatch(schema).extend([{"pk": i, "val": i} for i in range(10)]))

    push_small(t1)
    client.scan(v1, v1_schema)        # drains t1; v1 is now clean
    before = client.scan(v1, v1_schema).lsn  # fresh: no drain, so the watermark stands

    push_lsn = push_small(t2)
    after = client.scan(v1, v1_schema).lsn
    assert after == before and after < push_lsn, (
        f"read of a clean view ticked an unrelated relation "
        f"(watermark {before} -> {after}, unrelated push at {push_lsn})")

    # And the drain is not simply broken: v2's own read does absorb it.
    assert client.scan(v2, v2_schema).lsn >= push_lsn, "a read of the dirty view must drain its source"
