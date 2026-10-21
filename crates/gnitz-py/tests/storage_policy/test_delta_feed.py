"""Delta feeds: `CREATE VIEW … WITH (delta = '…')` and the read bound that
walks them.

A fed view keeps its recent deltas in a store of its own, and "what changed
since round N" is one more bound on the read verb that already exists. There is
no accumulator and no server-side cursor: a subscriber holds `(tag, tick)` and
nothing else, and a subscription only has the server issue that read itself.

**Every failure mode here is a weight error.** A row-set comparison tests
nothing — a retraction that never arrived and an insert applied twice both leave
the row set intact — so every convergence assertion below compares the client's
copy against a scan as a multiset of `(row, weight)`.

The mirroring client drives the same feed through its own poll path; its suites
are `client_surface/test_mirror.py` and `crates/gnitz-mirror/tests`, which also
hold how a held sync waits and which rounds move a cursor. These drive the raw
`delta_bootstrap` / `delta_poll` verbs, whose refusals a mirror hides, through
the Python binding.
"""
import asyncio
import os

import pytest
import gnitz
from gnitz import aio
from _catalog import schema_id
from _feedviews import GROUPBY, JOIN, LINEAR, SETOP, Subscriber, base_tables, churn, fed_view, mk_feed
from _read import bag, rows
from _sql import insert


# ── convergence ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "body", [LINEAR, JOIN, GROUPBY, SETOP], ids=["linear", "join", "groupby", "setop"]
)
def test_feed_converges_weight_exact(client, body):
    """Bootstrap, then poll, then be pushed the deltas by sync, then poll again
    from the cursor the last sync handed out: the applied copy equals the view as
    a multiset of (row, weight) at every step. The churn includes an UPDATE and a
    DELETE, so a capture that shipped `+1` with no `-1` fails here where a
    row-set test would pass. The `linear` body carries a >12-byte TEXT payload,
    so the delta rows carry out-of-line strings."""
    fed_view(client, body)
    sub = Subscriber(client, "f")
    churn(client, 1, 200)
    sub.bootstrap()
    sub.assert_converged("after the bootstrap")
    churn(client, 201, 400)
    sub.assert_converged("polled")

    sub.subscribe()
    assert len(sub.sync()) == 0, "nothing was pushed yet"
    for lo in (500, 700):
        churn(client, lo, lo + 100)
        assert len(sub.sync()) > 0, f"the sync through {lo} did not carry the pushes it followed"
        assert len(sub.sync()) == 0, f"the sync through {lo} left part of its rounds behind"
        assert sub.copy == sub.scan(), f"synced through {lo}"

    client.unsubscribe(sub.sub)
    assert client.sync().pushed == []
    churn(client, 900, 1000)
    sub.assert_converged("polled from the subscription's cursor")


# ── which rounds a poll sees ─────────────────────────────────────────────────


def test_a_round_ticked_by_a_ddl_drain_reaches_the_next_poll(client):
    """`CREATE VIEW` drains its source's pending ticks itself, outside the tick
    loop. That round must reach the subscriber like any other, not be gated away
    as "nothing changed"."""
    fed_view(client)
    sub = Subscriber(client, "f")
    client.execute_sql("INSERT INTO t VALUES (1, 100, 'seed')")
    sub.bootstrap()

    # Rows committed but deliberately NOT settled: the next statement's drain is
    # what ticks them.
    insert(client, "t", [(i, i * 3, f"late-{i}") for i in range(2, 40)])
    client.execute_sql("CREATE VIEW second AS SELECT id FROM t WHERE v > 1000")

    # The seed plus ids 4..39, the ones `v > 10` keeps.
    assert sub.assert_converged("the CREATE VIEW drain's round must reach the subscriber") == 37


def test_subscribers_at_one_position_share_one_read(own_server):
    """The steady state of a subscriber is a poll that returns nothing, and the
    master answers it locally, with no SAL group and no worker wakeup: one that
    did write would drive the checkpoint threshold on an idle database, forever.

    A cursor is a round of its own view, so two subscribers standing at one
    position of a feed hold one cursor whatever ticked elsewhere between their
    syncs, and the round that moves the view is read for both at once: the sync
    that finds it writes the SAL what it writes when nothing ticked between."""
    own_server.start()
    sal = os.path.join(own_server.data_dir, "wal.sal")

    def sal_written():
        # The SAL is MAP_SHARED and zero past its last group, so what a
        # request appended is visible through the file at once — to within the
        # zero bytes that group happens to end in.
        with open(sal, "rb") as fh:
            return len(fh.read(4 << 20).rstrip(b"\0"))

    target = own_server.target
    with gnitz.connect(target) as writer, gnitz.connect(target) as a, gnitz.connect(target) as b:
        fed_view(writer)
        writer.execute_sql("CREATE VIEW other AS SELECT id, w FROM u WHERE w > 0")
        writer.execute_sql("INSERT INTO t VALUES (1, 100, 'seed')")
        subs = []
        for client in (a, b):
            sub = Subscriber(client, "f")
            sub.bootstrap()
            subs.append(sub)

        before = sal_written()
        for _ in range(20):
            assert len(subs[0].poll()) == 0
        assert sal_written() == before, "an up-to-date poll must write no SAL bytes"
        for sub in subs:
            sub.subscribe()

        def round_read_by_the_first(key, between):
            """Move the view after `between` ran between the two subscribers'
            syncs; the SAL bytes the first one's next sync writes."""
            subs[0].sync()
            between()
            subs[1].sync()
            assert subs[0].cursor == subs[1].cursor, "one position, one cursor"
            writer.execute_sql(f"INSERT INTO t VALUES ({key}, 200, 'moved')")
            before = sal_written()
            subs[0].sync()
            wrote = sal_written() - before
            # The second subscriber's train was queued by that read.
            before = sal_written()
            subs[1].sync()
            assert sal_written() == before
            for sub in subs:
                assert sub.copy == sub.scan() and len(sub.copy) == key
            return wrote

        def a_round_elsewhere():
            # A round of a table the view does not read.
            writer.execute_sql("INSERT INTO u VALUES (1, 1, 7)")
            assert bag(rows(writer, "SELECT * FROM other")) == {(1, 7): 1}

        alone = round_read_by_the_first(2, lambda: None)
        again = round_read_by_the_first(3, a_round_elsewhere)
        # A second read is a whole group more, several times this slack.
        assert alone > 64 and abs(again - alone) <= 16, f"one read, as with nothing between: {alone} then {again}"


def test_a_poll_ticks_the_push_it_follows_and_loses_no_round(client):
    """A poll issued right after a push ACK carries that push. Over a stream and
    far below the row count that ticks on its own, so only the poll can have
    ticked it."""
    client.execute_sql(
        "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, amount BIGINT NOT NULL) "
        "WITH (stream = true)",
    )
    mk_feed(client, "f", "SELECT kind, SUM(amount) AS total FROM ev GROUP BY kind")
    sub = Subscriber(client, "f")
    sub.bootstrap()

    # The convergence check polls before it scans: a round its poll left
    # behind is ticked by the scan, and the two disagree.
    for r in range(6):
        insert(client, "ev", [(r * 50 + i, i % 4, i + 1) for i in range(50)])
        sub.assert_converged(f"after poll {r}")


# ── held syncs ───────────────────────────────────────────────────────────────


async def test_a_held_sync_leaves_an_async_client_free(client, aconn):
    """A sync held with nothing to report does not hold its client: a scan and a
    subscription submitted beside it are answered and leave it held, and it
    waits for that subscription too — a DDL reaching its view of the catalog
    alone ends the hold, with that DDL's catalog rows."""
    fed_view(client)
    mk_feed(client, "g", f"SELECT table_id, name FROM _system.tables WHERE schema_id = {schema_id(client)}")
    client.execute_sql("INSERT INTO t VALUES (1, 100, 'seed')")
    (fid, f_schema), (gid, g_schema) = client.resolve_table("f"), client.resolve_table("g")
    await aconn.subscribe(fid, f_schema)
    held = asyncio.ensure_future(aconn.sync(60))
    scanned = await asyncio.wait_for(aconn.scan(fid, f_schema), 20)
    assert not held.done(), "the scan left the sync held"
    late, _, cursor = await aconn.subscribe(gid, g_schema)
    assert not held.done(), "subscribing left the sync held"
    client.execute_sql("CREATE TABLE late (id BIGINT NOT NULL PRIMARY KEY)")
    pushed = {p.sub: p for p in (await asyncio.wait_for(held, 20)).pushed}[late]
    assert bag(scanned) == {(1, 100, "seed"): 1}
    assert (pushed.error, bag(pushed.rows, "name")) == (None, {("late",): 1})
    assert pushed.cursor[0] == cursor[0] and pushed.cursor[1] > cursor[1]


async def test_a_cancelled_held_sync_loses_nothing(client, aconn):
    """A held sync that is cancelled hands nothing to nobody: the next sync
    brings what the cancelled one's answer carried, and is not held behind it."""
    fed_view(client)
    vid, schema = client.resolve_table("f")
    sub, _, _ = await aconn.subscribe(vid, schema)
    held = asyncio.ensure_future(aconn.sync(60))
    # Answered behind the sync's request, so the server holds that one.
    await asyncio.wait_for(aconn.scan(vid, schema), 20)
    assert not held.done(), "the scan left the sync held"
    held.cancel()
    insert(client, "t", [(i, 100, "w") for i in range(10)])
    (pushed,) = (await asyncio.wait_for(aconn.sync(60), 20)).pushed
    assert (pushed.sub, pushed.error, bag(pushed.rows)) == (sub, None, {(i, 100, "w"): 1 for i in range(10)})


async def test_a_sync_waiting_for_a_tick_holds_up_no_request(own_server):
    """The writes behind a held sync are answered while it waits for a tick,
    which at the pace set here is ten minutes away; and a sync that finds a
    commit no tick has taken is answered with it once its own wait ends."""
    own_server.start(extra_env={"GNITZ_PATIENT_TICK_GAP_MS": "600000"})
    with gnitz.connect(own_server.target) as client:
        fed_view(client)
        client.execute_sql("INSERT INTO t VALUES (1, 100, 'seed')")
        vid, schema = client.resolve_table("f")
        async with aio.connect(own_server.target) as conn:
            # The read ticks the seed, and the pace counts from that tick.
            await conn.subscribe(vid, schema)
            held = asyncio.ensure_future(conn.sync(60))
            for i in range(2, 6):
                await asyncio.wait_for(conn.execute_sql(f"INSERT INTO t VALUES ({i}, 100, 'w')"), 20)
            assert not held.done(), "no tick has taken the writes"
            # A read of the view ticks at once: no follower asked for it.
            assert bag(rows(client, "SELECT * FROM f")) == {(1, 100, "seed"): 1, **{(i, 100, "w"): 1 for i in range(2, 6)}}
            (pushed,) = (await asyncio.wait_for(held, 20)).pushed
            assert (pushed.error, bag(pushed.rows)) == (None, {(i, 100, "w"): 1 for i in range(2, 6)})

            client.execute_sql("INSERT INTO t VALUES (6, 200, 'late')")
            (pushed,) = (await asyncio.wait_for(conn.sync(0.2), 60)).pushed
            assert (pushed.error, bag(pushed.rows)) == (None, {(6, 200, "late"): 1})


# ── refusals ─────────────────────────────────────────────────────────────────


def test_a_delta_read_of_a_relation_with_no_feed_is_an_error(client):
    """At **every** `after_tick`, zero included: the reply would otherwise promise
    a continuation the server cannot serve."""
    base_tables(client)
    client.execute_sql(f"CREATE VIEW plain AS {LINEAR}")
    vid, schema = client.resolve_table("plain")
    for verb in (
        lambda: client.delta_bootstrap(vid, schema),
        lambda: client.delta_poll(vid, schema, (0, 1)),
    ):
        with pytest.raises(gnitz.GnitzRefusedError, match="carries no delta feed") as e:
            verb()
        # Not the expiry code: a subscriber reacts to that by re-reading at 0,
        # so answering it here would spin one forever against a view that will
        # never have a feed.
        assert not isinstance(e.value, gnitz.GnitzDeltaExpiredError), str(e.value)
