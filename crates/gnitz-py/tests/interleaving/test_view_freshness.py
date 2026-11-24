"""A read of a view reflects every commit ACKed before it.

A tick fires when a push crosses the row-coalesce threshold or when a read
drains, so a small push sits pending until the read that needs it; and a read
written behind a tick still in flight is answered by each worker only once its
drive has ended.
"""

import gnitz
import pytest
from _read import bag, scanned
from _schemas import KV, kv
from _serverproc import TICK_COALESCE_ROWS, across
from _sql import insert

# Crossing the coalesce threshold fires the auto-tick, and the excess is how long
# that tick is still running when the next statement lands — the race window,
# not a scale knob.
BIG_ROWS = 4 * TICK_COALESCE_ROWS


def test_a_seek_reflects_every_acked_push_under_concurrent_ddl(client, peer):
    """Connection A pushes key k; connection B then seeks it through a view over a
    view, and through the view beneath. Each must return k at weight 1: the
    drain runs off shared master state, so it covers another connection's commit,
    and one drain cascades down the chain. A view runs no `enforce_unique_pk`, so
    a tick applied twice shows as weight 2.

    A third connection churns CREATE/DROP VIEW on an unrelated table throughout.
    The seek drains before it takes the catalog read lock, so it never holds
    the lock across a drain and the writer-preferring DDL never waits one out."""
    tid = client.create_table("t", KV)
    client.create_table("t2", KV)
    client.execute_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE val >= 0; "
                       "CREATE VIEW v2 AS SELECT * FROM v1 WHERE val >= 0")
    (v1, v1_schema), (v2, v2_schema) = (client.resolve_table(v) for v in ("v1", "v2"))
    churner, b = peer(), peer()

    def churn():
        for i in range(20):
            churner.execute_sql(f"CREATE VIEW dv{i} AS SELECT * FROM t2 WHERE val >= 0; DROP VIEW dv{i}")

    def step(k):
        client.push(tid, kv((k, k * 10)))
        assert bag(b.seek(v2, v2_schema, pk=k)) == {(k, k * 10): 1}, f"v2 seek {k}"
        assert bag(b.seek(v1, v1_schema, pk=k)) == {(k, k * 10): 1}, f"v1 seek {k}"

    n = across(churn, step)
    assert bag(scanned(client, "v2")) == {(i, i * 10): 1 for i in range(n)}


def test_a_read_waits_for_its_own_in_flight_tick(client):
    """A push crossing the coalesce threshold ACKs while the tick it fired is
    still running, and the read that follows with nothing in between must see
    every group at weight 1. The pending-tick queue is already empty once the tick
    is dequeued, so a freshness check keyed on it would serve the view with none
    of its groups.

    A second such push fires another tick, and INSERTs run against it: each takes
    the PK-rejection probe burst into workers still evaluating the GROUP BY's
    exchange. Neither may wedge, and the view converges exactly."""
    tid = client.create_table("t", KV)
    client.execute_sql("CREATE VIEW v AS SELECT val, COUNT(*) AS n FROM t GROUP BY val")
    # Resolved up front: a RESOLVE between a push and its read would give the
    # tick time to finish.
    vid, v_schema = client.resolve_table("v")

    def push_range(lo, hi):
        client.push(tid, kv(*((i, i) for i in range(lo, hi))))

    push_range(0, BIG_ROWS)
    assert bag(client.scan(vid, v_schema)) == {(i, 1): 1 for i in range(BIG_ROWS)}

    push_range(BIG_ROWS, 2 * BIG_ROWS)
    end = 2 * BIG_ROWS + 300
    for lo in range(2 * BIG_ROWS, end, 100):
        insert(client, "t", ((i, i) for i in range(lo, lo + 100)))
    assert bag(client.scan(vid, v_schema)) == {(i, 1): 1 for i in range(end)}


def test_a_failed_tick_fails_its_read_and_stays_pending(own_server):
    """A view tick that fails to emit fails the read waiting on it, rather than
    answering OK over a view that silently stopped advancing, and the next read
    ticks the same delta.

    The CREATE VIEW has nothing pending, emits no tick and so cannot spend the
    one-shot emit failure."""
    with gnitz.connect(own_server.start(extra_env={"GNITZ_INJECT_TICK_EMIT_ERROR": "1"}).target) as client:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
            "CREATE VIEW v AS SELECT pk, val FROM t WHERE val > 5; "
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 1)")

        with pytest.raises(gnitz.GnitzRefusedError):
            scanned(client, "v")

        assert bag(scanned(client, "v")) == {(1, 10): 1, (2, 20): 1}
