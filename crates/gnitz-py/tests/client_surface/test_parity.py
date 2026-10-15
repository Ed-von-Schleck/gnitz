"""One client, two ways of waiting: `gnitz.GnitzClient` and
`gnitz.AsyncGnitzClient` run the same verbs, and each has the other's way of
sharing a round trip.

What a verb *does* is owned by the coordinate that tests it through the
blocking client. This suite owns what only the pair has: that the verb set is
one set, that an event loop runs calls in the order they were made, that the
verbs made of several round trips — DDL, SQL, a transaction, a mirror — work
with the loop doing the waiting, and that the blocking client pipelines.
"""

import asyncio

import pytest
import gnitz
from gnitz import aio
from _feedviews import churn, fed_view
from _read import bag, result_rows, rows
from _schemas import KV, SCHEMAS, kv


# ---------------------------------------------------------------------------
# One verb set
# ---------------------------------------------------------------------------

def test_the_two_classes_share_every_verb():
    """Every verb is defined once, on the base both classes inherit; what each
    class adds is its lifecycle. A verb written onto one class alone is the
    drift this pins."""
    def own(cls):
        return {n for n in vars(cls) if not n.startswith("__")}

    base = gnitz.GnitzClient.__mro__[1]
    assert base is gnitz.AsyncGnitzClient.__mro__[1]
    assert own(gnitz.GnitzClient) == {"close", "pipeline"}
    assert own(gnitz.AsyncGnitzClient) == {"aclose"}
    assert {"push", "execute_sql", "mirror_view", "transaction", "create_table"} <= own(base)


# ---------------------------------------------------------------------------
# The verbs of several round trips, on an event loop
# ---------------------------------------------------------------------------

async def test_ddl_sql_and_reads_on_the_loop(aconn):
    """DDL through both surfaces, DML, and every read verb, each awaited: the
    verbs the loop could not run while only a single request could wait."""
    results = await aconn.execute_sql(
        "CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30); "
        "UPDATE t SET val = 21 WHERE pk = 2; "
        "DELETE FROM t WHERE pk = 3")
    assert [r["type"] for r in results] == ["Ddl", "RowsAffected", "RowsAffected", "RowsAffected"]

    tid, schema = await aconn.resolve_table("t")
    assert bag(await aconn.scan(tid, schema)) == {(1, 10): 1, (2, 21): 1}
    assert bag(await aconn.seek(tid, schema, 2)) == {(2, 21): 1}
    assert bag(result_rows(await aconn.execute_sql("SELECT val FROM t WHERE pk = 1"))) == {(10,): 1}

    other = await aconn.create_table("u", KV)
    await aconn.push(other, kv((7, 70)))
    await aconn.delete(other, KV, [7])
    assert bag(await aconn.scan(other, KV)) == {}
    await aconn.drop_table("u")
    with pytest.raises(gnitz.GnitzNotFoundError):
        await aconn.resolve_table("u")


async def test_calls_run_in_the_order_they_were_made(aconn):
    """Every call below is made before the first is awaited. They run in call
    order, one at a time: the push replaces the row the INSERT wrote, and the
    SELECT, made last, reads the push's value. Run out of order, the INSERT
    would find the row and be refused, or the SELECT would read too early."""
    tid = await aconn.create_table("t", KV)
    inserted, _, _, selected = await asyncio.gather(
        aconn.execute_sql("INSERT INTO t VALUES (1, 10)"),
        aconn.push(tid, kv((1, 11))),
        aconn.push(tid, kv((2, 20))),
        aconn.execute_sql("SELECT pk, val FROM t"),
    )
    assert inserted[0] == {"type": "RowsAffected", "count": 1}
    assert bag(result_rows(selected)) == {(1, 11): 1, (2, 20): 1}


async def test_a_transaction_on_the_loop(aconn, client):
    """`async with` commits what its block buffered as one transaction, and an
    exception discards it; the blocking spelling is refused rather than left to
    return a future nothing awaits."""
    tid = await aconn.create_table("t", KV)
    await aconn.push(tid, kv((1, 10)))

    async with aconn.transaction() as txn:
        assert txn is aconn
        assert await txn.push(tid, kv((2, 20))) == 0, "buffered, not sent"
        await txn.delete(tid, KV, [1])
        # Another connection sees none of it before the commit.
        assert bag(client.scan(tid, KV)) == {(1, 10): 1}
    assert bag(client.scan(tid, KV)) == {(2, 20): 1}

    with pytest.raises(RuntimeError, match="boom"):
        async with aconn.transaction():
            await aconn.push(tid, kv((3, 30)))
            raise RuntimeError("boom")
    assert bag(await aconn.scan(tid, KV)) == {(2, 20): 1}

    with pytest.raises(TypeError, match="event loop"):
        with aconn.transaction():
            pass
    with pytest.raises(TypeError, match="blocks"):
        async with client.transaction():
            pass


async def test_a_delta_feed_on_the_loop(aconn, client):
    """The feed verbs by hand: a bootstrap, then a poll that carries the round a
    later push produced — weights and all."""
    fed_view(client)
    churn(client, 1, 50)

    vid, schema = await aconn.resolve_table("f")
    seed, cursor = await aconn.delta_bootstrap(vid, schema)
    assert bag(seed.including_hidden()) == bag(client.scan(vid, schema).including_hidden())

    churn(client, 51, 100)
    delta, cursor2 = await aconn.delta_poll(vid, schema, cursor)
    assert cursor2[0] == cursor[0] and cursor2[1] > cursor[1]
    copy = bag([*seed.including_hidden(), *delta.including_hidden()])
    assert copy == bag(client.scan(vid, schema).including_hidden())


async def test_a_mirror_on_the_loop(aconn, client, server, mirror_dir):
    """A mirrored copy kept by a connection on an event loop: bootstrapped,
    advanced by a poll, read locally — weight-exact against the server, with the
    proof that the copy answered — and made durable by `aclose`, which a
    blocking client reopening the same directory then resumes from."""
    fed_view(client)
    churn(client, 1, 100)

    await aconn.mirror_at(mirror_dir)
    first = await aconn.mirror_view("f")
    assert first.reseeded and first.error is None
    vid = first.view_id
    assert await aconn.mirrored_ids() == [vid]

    churn(client, 101, 200)
    (polled,) = (await aconn.sync()).mirrored
    assert (polled.view_id, polled.reseeded, polled.error) == (vid, False, None)
    cursor = await aconn.cursor(vid)
    assert cursor == polled.cursor and cursor is not None

    before = await aconn.requests_sent
    local = result_rows(await aconn.execute_sql("SELECT * FROM f"))
    assert await aconn.requests_sent == before, "the read was delegated upstream"
    served = rows(client, "SELECT * FROM f")
    assert bag(local) == bag(served) != {}
    assert (await aconn.scan(vid, (await aconn.resolve_table("f"))[1])).lsn is None

    await aconn.checkpoint()
    await aconn.aclose()

    # The exit checkpoint landed and the directory lock is free: a second
    # client resumes the copy at its cursor rather than reading the view whole.
    with gnitz.connect(server, schema=client.schema) as again:
        again.mirror_at(mirror_dir)
        resumed = again.mirror_view("f")
        assert not resumed.reseeded, "the copy was not resumed from the checkpoint"
        assert bag(rows(again, "SELECT * FROM f")) == bag(served)


async def test_aclose_waits_for_the_calls_already_made(server, client):
    """`aclose` runs behind every call made before it, so their replies arrive;
    it is idempotent, and a call made after it is refused through its future
    rather than reconnecting behind the caller's back."""
    tid = client.create_table("t", KV)
    conn = await aio.connect(server, client.schema)
    pushes = [conn.push(tid, kv((i, i))) for i in range(50)]
    closed = conn.aclose()
    late = conn.scan(tid, KV)
    lsns = await asyncio.gather(*pushes)
    assert min(lsns) > 0
    assert await closed is None
    await conn.aclose()
    with pytest.raises(gnitz.GnitzConnectionError, match="connection closed"):
        await late
    assert bag(client.scan(tid, KV)) == {(i, i): 1 for i in range(50)}


async def test_a_push_sends_the_batch_as_it_was_called_with(aconn):
    """A push is submitted when called, behind whatever is already queued; rows
    appended to its batch afterwards belong to the next push."""
    tid = await aconn.create_table("t", KV)
    batch = kv((1, 10))
    held_back = aconn.execute_sql("SELECT pk FROM t")
    pushed = aconn.push(tid, batch)
    batch.append(pk=2, val=20)
    await asyncio.gather(held_back, pushed)
    assert bag(await aconn.scan(tid, KV)) == {(1, 10): 1}
    assert len(batch) == 2


# ---------------------------------------------------------------------------
# Pipelining, on the blocking client
# ---------------------------------------------------------------------------

def test_a_pipeline_defers_the_waits(client):
    """Inside `pipeline()` a one-request verb returns once its request is sent.
    Its `Pending` resolves to what the verb returns outside one, in call order,
    and a read made behind a push in the block sees that push."""
    tid = client.create_table("t", KV)
    with client.pipeline() as piped:
        assert piped is client
        lsns = [client.push(tid, kv((i, i * 10))) for i in range(100)]
        scan = client.scan(tid, KV)
        point = client.seek(tid, KV, 7)
        many = client.scan_many([(tid, KV)])
        assert all(isinstance(p, gnitz.Pending) for p in [*lsns, scan, point, many])
        # A result read inside the block waits for exactly that reply.
        assert bag(point.result()) == {(7, 70): 1}
    values = [p.result() for p in lsns]
    assert min(values) > 0 and values == sorted(values)
    expected = {(i, i * 10): 1 for i in range(100)}
    assert bag(scan.result()) == expected
    assert [bag(r) for r in many.result()] == [expected]
    # Outside the block a verb is a round trip again.
    assert isinstance(client.push(tid, kv((500, 0))), int)


def test_a_pipeline_runs_longer_verbs_in_their_turn(client):
    """A verb of several round trips finishes inside its call, in its place in
    the order — and returns a `Pending` like every other verb there."""
    tid = client.create_table("t", KV)
    with client.pipeline():
        client.push(tid, kv((1, 10)))
        selected = client.execute_sql("SELECT pk, val FROM t")
        client.push(tid, kv((2, 20)))
    assert bag(result_rows(selected.result())) == {(1, 10): 1}
    assert bag(client.scan(tid, KV)) == {(1, 10): 1, (2, 20): 1}


def test_a_pipeline_raises_the_failure_nobody_read(client):
    """Leaving the block raises the first failed reply no `result()` took, so a
    refused push cannot pass silently; one that was read is not raised twice.
    Either way the calls around the failure still happened."""
    tid = client.create_table("t", KV)

    with pytest.raises(gnitz.GnitzNotFoundError):
        with client.pipeline():
            client.push(tid, kv((1, 1)))
            client.push(0xDEAD_BEEF, kv((1, 1)))        # no such relation
            client.push(tid, kv((2, 2)))
    assert bag(client.scan(tid, KV)) == {(1, 1): 1, (2, 2): 1}

    with client.pipeline():
        refused = client.push(0xDEAD_BEEF, kv((1, 1)))
        with pytest.raises(gnitz.GnitzNotFoundError):
            refused.result()
        with pytest.raises(gnitz.GnitzNotFoundError):
            refused.result()


def test_a_pipeline_that_raises_waits_for_nothing(client):
    """A block that raises lets its own exception through; the requests it sent
    still commit, and their replies stay there to be read."""
    tid = client.create_table("t", KV)
    with pytest.raises(RuntimeError, match="boom"):
        with client.pipeline():
            sent = client.push(tid, kv((1, 1)))
            client.push(0xDEAD_BEEF, kv((1, 1)))
            raise RuntimeError("boom")
    assert sent.result() > 0
    assert bag(client.scan(tid, KV)) == {(1, 1): 1}
    with pytest.raises(gnitz.GnitzError, match="already open"):
        with client.pipeline():
            with client.pipeline():
                pass


def test_a_pipeline_past_the_in_flight_cap_makes_room(client):
    """More requests than a connection may hold in flight: the pipeline waits
    for room instead of taking the refusal a connection raises at the cap."""
    with client.pipeline():
        scans = [client.scan(*SCHEMAS) for _ in range(gnitz.MAX_IN_FLIGHT + 200)]
    sizes = {len(s.result()) for s in scans}
    assert len(sizes) == 1 and sizes.pop() > 0


def test_a_pipelined_push_sends_the_batch_as_it_was_called_with(client):
    tid = client.create_table("t", KV)
    batch = kv((1, 10))
    with client.pipeline():
        client.push(tid, batch)
        batch.append(pk=2, val=20)
    assert bag(client.scan(tid, KV)) == {(1, 10): 1}
    client.push(tid, batch)
    assert bag(client.scan(tid, KV)) == {(1, 10): 1, (2, 20): 1}
