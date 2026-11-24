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
hold how a held sync waits and which rounds move a cursor; how a held sync
leaves an event-loop client free is `client_surface/test_async.py`'s. These
drive the raw `delta_read` verb, whose refusals a mirror hides, through the
Python binding.
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


def test_a_poll_after_a_ddl_returns_that_ddls_rows_at_their_weights(client):
    """A feed over a system family: a DDL's catalog rows are a round of the feed
    like any push's, weights and all."""
    mk_feed(client, "ddl_feed", f"SELECT table_id, name FROM _system.tables WHERE schema_id = {schema_id(client)}")
    sub = Subscriber(client, "ddl_feed")
    sub.bootstrap()
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


def test_subscribers_at_one_position_share_one_read(own_server):
    """The steady state of a subscriber is a poll that returns nothing, and the
    master answers it locally, with no SAL group and no worker wakeup: one that
    did write would drive the checkpoint threshold on an idle database, forever.

    A cursor is a round of its own view, so two subscribers standing at one
    position of a feed hold one cursor whatever ticked elsewhere between their
    syncs, and the round that moves the view is read once for both: the second
    subscriber's sync writes the SAL nothing."""
    own_server.start()
    sal = os.path.join(own_server.data_dir, "wal.sal")

    def sal_written():
        # The SAL is MAP_SHARED and zero past its last group, so what a
        # request appended is visible through the file at once. A fresh server
        # never reaches the checkpoint that would rewind it.
        with open(sal, "rb") as fh:
            return len(fh.read(4 << 20).rstrip(b"\0"))

    target = own_server.target
    with gnitz.connect(target) as writer, gnitz.connect(target) as a, gnitz.connect(target) as b:
        fed_view(writer)
        writer.execute_sql("CREATE VIEW other AS SELECT id, w FROM u WHERE w > 0")
        writer.execute_sql("INSERT INTO t VALUES (1, 100, 'seed')")
        subs = [Subscriber(client, "f") for client in (a, b)]
        for sub in subs:
            sub.bootstrap()

        before = sal_written()
        for _ in range(20):
            assert len(subs[0].poll()) == 0
        assert sal_written() == before, "an up-to-date poll must write no SAL bytes"
        for sub in subs:
            sub.subscribe()

        # The second round is preceded by a round of a table the view does not read.
        for key in (2, 3):
            subs[0].sync()
            if key == 3:
                writer.execute_sql("INSERT INTO u VALUES (1, 1, 7)")
                assert bag(rows(writer, "SELECT * FROM other")) == {(1, 7): 1}
            subs[1].sync()
            assert subs[0].cursor == subs[1].cursor, "one position, one cursor"
            writer.execute_sql(f"INSERT INTO t VALUES ({key}, 200, 'moved')")
            subs[0].sync()
            # The second subscriber's train was queued by that read.
            before = sal_written()
            subs[1].sync()
            assert sal_written() == before, f"the round of key {key} was read again"
            for sub in subs:
                assert sub.copy == sub.scan() and len(sub.copy) == key


# ── a held sync ──────────────────────────────────────────────────────────────


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
    for cursor in (None, (0, 1)):
        with pytest.raises(gnitz.GnitzRefusedError, match="carries no delta feed") as e:
            client.delta_read(vid, schema, cursor)
        # Not the expiry code: a subscriber reacts to that by re-reading at 0,
        # so answering it here would spin one forever against a view that will
        # never have a feed.
        assert not isinstance(e.value, gnitz.GnitzDeltaExpiredError), str(e.value)
