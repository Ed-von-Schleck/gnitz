"""A delta feed over a system family: a DDL's catalog rows are a round of the
feed like any push's, weights and all."""

import asyncio

from gnitz import aio
from _catalog import schema_id
from _feedviews import Subscriber, mk_feed
from _read import bag


def _ddl_feed(client):
    mk_feed(client, "ddl_feed", f"SELECT table_id, name FROM _system.tables WHERE schema_id = {schema_id(client)}")
    sub = Subscriber(client, "ddl_feed")
    sub.bootstrap()
    sub.drain()
    return sub


def test_a_poll_after_a_ddl_returns_that_ddls_rows_at_their_weights(client):
    sub = _ddl_feed(client)
    assert sub.copy == {}

    client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY)")
    assert bag(sub.poll(), "name") == {("a",): 1}
    assert len(sub.poll()) == 0, "a DDL is one round"

    client.execute_sql("ALTER TABLE a RENAME TO b")
    assert bag(sub.poll(), "name") == {("a",): -1, ("b",): 1}

    client.execute_sql("DROP TABLE b")
    assert bag(sub.poll(), "name") == {("b",): -1}
    assert sub.copy == {} == sub.scan()

    client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY)")
    sub.assert_converged("after a create")


async def test_a_held_sync_is_released_by_a_ddl(client, server):
    sub = _ddl_feed(client)
    async with aio.connect(server, schema=client.schema) as conn:
        await conn.subscribe(sub.vid, sub.schema, sub.cursor)
        held = asyncio.ensure_future(conn.sync(60))
        await asyncio.wait_for(conn.scan(sub.vid, sub.schema), 20)
        assert not held.done(), "nothing changed, so the sync is held"
        client.execute_sql("CREATE TABLE late (id BIGINT NOT NULL PRIMARY KEY)")
        (pushed,) = (await asyncio.wait_for(held, 20)).pushed
    assert (pushed.error, bag(pushed.rows, "name")) == (None, {("late",): 1})
