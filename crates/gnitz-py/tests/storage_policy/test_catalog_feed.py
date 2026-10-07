"""A delta feed over a system family: a DDL's catalog rows are a round of the
feed like any push's, weights and all."""

import time

from _catalog import schema_id
from _feedviews import FEED, Subscriber, later


def _ddl_feed(client):
    sid = schema_id(client)
    client.execute_sql(
        f"CREATE VIEW ddl_feed WITH (delta = '{FEED}') AS "
        f"SELECT table_id, name FROM _system.tables WHERE schema_id = {sid}")
    sub = Subscriber(client, "ddl_feed")
    sub.bootstrap()
    sub.drain()
    return sub


def _names(batch):
    acc = {}
    for r in batch:
        acc[r.name] = acc.get(r.name, 0) + r._weight
    return {k: w for k, w in acc.items() if w}


def test_a_poll_after_a_ddl_returns_that_ddls_rows_at_their_weights(client):
    sub = _ddl_feed(client)
    assert sub.copy == {}

    client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY)")
    assert _names(sub.poll()) == {"a": 1}
    assert len(sub.poll()) == 0, "a DDL is one round"

    client.execute_sql("ALTER TABLE a RENAME TO b")
    assert _names(sub.poll()) == {"a": -1, "b": 1}

    client.execute_sql("DROP TABLE b")
    assert _names(sub.poll()) == {"b": -1}
    assert sub.copy == {} == sub.scan()

    client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY)")
    sub.assert_converged("after a create")


def test_a_waiting_poll_is_released_by_a_ddl(client, server):
    sub = _ddl_feed(client)
    t0 = time.monotonic()
    assert len(sub.poll(wait=0.3)) == 0
    assert time.monotonic() - t0 >= 0.3, "nothing changed, so the reply was held"

    writer = later(server, client.schema, 0.2, "CREATE TABLE late (id BIGINT NOT NULL PRIMARY KEY)")
    t0 = time.monotonic()
    got = sub.poll(wait=60)
    assert time.monotonic() - t0 < 20, "the DDL released it, not the wait"
    writer.join()
    assert _names(got) == {"late": 1}
    sub.assert_converged("after the waiting poll")
