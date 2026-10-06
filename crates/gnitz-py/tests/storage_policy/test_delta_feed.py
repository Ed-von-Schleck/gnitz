"""Delta feeds: `CREATE VIEW … WITH (delta = '…')` and the read bound that
walks them.

A fed view keeps its recent deltas in a store of its own, and "what changed
since round N" is one more bound on the read verb that already exists. There is
no subscription verb, no accumulator and no server-side cursor: a subscriber
holds `(tag, tick)` and nothing else.

**Every failure mode here is a weight error.** A row-set comparison tests
nothing — a retraction that never arrived and an insert applied twice both leave
the row set intact — so every convergence assertion below compares the client's
copy against a scan as a multiset of `(row, weight)`.

The mirroring client drives the same feed through its own poll path; its suites
are `client_surface/test_mirror.py` and `crates/gnitz-mirror/tests`. These drive
the raw `delta_bootstrap` / `delta_poll` verbs, whose refusals a mirror hides.
"""
import hashlib
import os
import time

import pytest
import gnitz
from _feedviews import (
    FEED, GROUPBY, JOIN, LINEAR, SETOP, SWEEP_ENV,
    Subscriber, base_tables, churn, flood, later, mk_feed,
)
from _paths import relation_dir
from _serverproc import NEEDS_MULTI


# ── convergence ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "body", [LINEAR, JOIN, GROUPBY, SETOP], ids=["linear", "join", "groupby", "setop"]
)
def test_feed_converges_weight_exact(client, body):
    """Bootstrap, then poll to the end, and the applied copy equals the view as a
    multiset of (row, weight). The churn includes an UPDATE and a DELETE, so a
    capture that shipped `+1` with no `-1` fails here where a row-set test would
    pass. The `linear` body carries a >12-byte TEXT payload, so the delta rows
    carry out-of-line strings."""
    base_tables(client)
    mk_feed(client, "f", body)
    sub = Subscriber(client, "f")

    churn(client, 1, 200)
    sub.bootstrap()
    sub.assert_converged("after the bootstrap")

    for lo, hi in ((201, 400), (401, 600)):
        churn(client, lo, hi)
        sub.assert_converged(f"through key {hi}")


@NEEDS_MULTI
def test_a_delta_touching_one_worker_only(client):
    """A one-row insert lands on one worker; the other W−1 emit nothing and write
    no delta row. Any design that assembles whole rounds server-side waits
    forever for a round they will never report — and passes at W=1."""
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    sub = Subscriber(client, "f")
    sub.bootstrap()

    for i in range(1, 12):
        client.execute_sql(f"INSERT INTO t VALUES ({i * 1000}, {i * 37}, 'solo-{i:0>20}')")
        sub.assert_converged(f"single-key round {i}")


def test_a_four_column_pk_view_carries_a_feed(client):
    """The stamp is one more PK column, so the widest declarable view PK plus the
    stamp is exactly `MAX_PK_COLUMNS`."""
    client.execute_sql(
        "CREATE TABLE q (a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, d BIGINT NOT NULL, "
        "v BIGINT NOT NULL, PRIMARY KEY (a, b, c, d))",
    )
    mk_feed(client, "f", "SELECT a, b, c, d, v FROM q WHERE v > 1")
    sub = Subscriber(client, "f")
    sub.bootstrap()
    for r in range(4):
        client.execute_sql(
            "INSERT INTO q VALUES " + ",".join(f"({r}, {i}, {i * 2}, {i * 3}, {i + 2})" for i in range(20)),
        )
        sub.assert_converged(f"compound-PK round {r}")


# ── which rounds a poll sees ─────────────────────────────────────────────────


def test_a_round_ticked_by_a_ddl_drain_reaches_the_next_poll(client):
    """`CREATE VIEW` drains its source's pending ticks itself, outside the tick
    loop. That round must reach the subscriber like any other, not be gated away
    as "nothing changed"."""
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    sub = Subscriber(client, "f")
    client.execute_sql("INSERT INTO t VALUES (1, 100, 'seed')")
    sub.bootstrap()
    sub.drain()

    # Rows committed but deliberately NOT settled: the next statement's drain is
    # what ticks them.
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'late-{i}')" for i in range(2, 40)),
    )
    client.execute_sql("CREATE VIEW second AS SELECT id FROM t WHERE v > 1000")

    sub.drain()
    # The seed plus ids 4..39, the ones `v > 10` keeps.
    assert len(sub.copy) == 37 and sub.copy == sub.scan(), "the CREATE VIEW drain's round must reach the subscriber"


def test_an_up_to_date_poll_writes_no_sal_bytes(own_server):
    """The steady state of a subscription is a poll that returns nothing, so that
    is the case that must be cheap: the master answers it locally, with no SAL
    group and no worker wakeup. A poll that did write would drive the checkpoint
    threshold on an idle database, forever."""
    own_server.start()
    sal = os.path.join(own_server.data_dir, "wal.sal")

    def sal_digest():
        # The SAL is MAP_SHARED, so a group write is visible through the file at
        # once. Hashing a generous prefix covers every group an idle server could
        # append.
        with open(sal, "rb") as fh:
            return hashlib.sha256(fh.read(4 << 20)).hexdigest()

    with gnitz.connect(own_server.target) as client:
        base_tables(client)
        mk_feed(client, "f", LINEAR)
        sub = Subscriber(client, "f")
        client.execute_sql("INSERT INTO t VALUES (1, 100, 'x')")
        sub.bootstrap()
        sub.drain()

        before = sal_digest()
        for _ in range(20):
            assert len(sub.poll()) == 0
        assert sal_digest() == before, "an up-to-date poll must write no SAL bytes"


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
    sub.drain()

    for r in range(6):
        client.execute_sql(
            "INSERT INTO ev VALUES " + ",".join(f"({r * 50 + i}, {i % 4}, {i + 1})" for i in range(50)),
        )
        assert len(sub.poll()) > 0, f"poll {r} did not carry the push it followed"
        assert len(sub.poll()) == 0, f"poll {r} left part of its round behind"
        assert sub.copy == sub.scan(), f"after poll {r}"

    sub.assert_converged("after the polls")


def test_a_waiting_poll_is_held_until_a_commit_reaches_the_view(client, server):
    """With `wait`, a poll that has nothing to report is held for the wait, and a
    commit ends it at once with the commit's rows."""
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    sub = Subscriber(client, "f")
    client.execute_sql("INSERT INTO t VALUES (1, 100, 'seed')")
    sub.bootstrap()
    sub.drain()

    t0 = time.monotonic()
    assert len(sub.poll(wait=0.3)) == 0
    assert time.monotonic() - t0 >= 0.3, "nothing changed, so the reply was held"

    writer = later(server, client.schema, 0.2, "INSERT INTO t VALUES (2, 200, 'late')")
    t0 = time.monotonic()
    got = sub.poll(wait=60)
    assert time.monotonic() - t0 < 20, "the commit released it, not the wait"
    writer.join()
    assert len(got) == 1, "the reply is the commit's delta"
    sub.assert_converged("after the waiting poll")

    with pytest.raises(ValueError, match="non-negative"):
        sub.poll(wait=-1)


# ── refused cursors ──────────────────────────────────────────────────────────


def test_a_cursor_below_the_floor_is_refused_as_a_code(own_server):
    """Retention drops the oldest rounds whether or not anyone still reads them,
    and a cursor below what was dropped is refused with `GnitzDeltaExpiredError`
    — a type the subscriber reacts to, not a string it matches. Recovery is the
    read it made on its first day."""
    with gnitz.connect(own_server.start(extra_env=SWEEP_ENV).target) as client:
        base_tables(client)
        mk_feed(client, "f", LINEAR, feed="1 KB")
        sub = Subscriber(client, "f")
        sub.bootstrap()
        sub.drain()

        # Twice the volume measured to reach the first drop, so the margin is
        # the test's and not the box's.
        flood(client, 1, 8_000)

        with pytest.raises(gnitz.GnitzDeltaExpiredError):
            sub.poll()

        # Settle before re-reading rather than going through `assert_converged`:
        # on a 1 KB budget one round of these rows overruns it outright, so a
        # round ticked by that helper's scan is dropped before its poll arrives.
        live = sub.scan()
        sub.bootstrap()
        assert sub.copy == live, "after re-reading at 0"


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


@pytest.mark.parametrize("recreate", ["drop-and-create", "or-replace"])
def test_a_cursor_across_a_recreate_is_rejected(client, recreate):
    """A recreated view takes a fresh id whose rounds come from the same global
    counter, so an unrefused cursor would be answered with the *new* view's deltas
    above the stale round — and a recreated view's backfill never enters a delta
    store, so applying them yields a copy missing everything below it.

    `CREATE OR REPLACE` is the case nothing else would tell a subscriber about:
    unlike the drop, the name still resolves.
    """
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    sub = Subscriber(client, "f")
    client.execute_sql("INSERT INTO t VALUES (1, 100, 'a')")
    sub.bootstrap()
    sub.drain()

    if recreate == "drop-and-create":
        client.execute_sql("DROP VIEW f")
        mk_feed(client, "f", LINEAR)
    else:
        client.execute_sql(
            f"CREATE OR REPLACE VIEW f WITH (delta = '{FEED}') AS {LINEAR}"
        )
    client.execute_sql("INSERT INTO t VALUES (2, 200, 'b')")

    fresh = Subscriber(client, "f")
    assert fresh.vid != sub.vid, "a recreated view takes a fresh id"
    fresh.cursor = sub.cursor
    with pytest.raises(gnitz.GnitzDeltaExpiredError):
        fresh.poll()

    fresh.bootstrap()
    fresh.assert_converged("after re-resolving and bootstrapping")


def test_a_feed_survives_a_restart_on_every_worker(own_server):
    """A restart leaves a fed view fed on every worker — not a catalog that says
    "fed" over no delta store, which every other test reports only as "no rows".

    A cursor held across the restart must be rejected: the delta store is erased
    at open and the boot mints a fresh tag.
    """
    own_server.start()
    with gnitz.connect(own_server.target) as client:
        base_tables(client)
        mk_feed(client, "f", LINEAR)
        sub = Subscriber(client, "f")
        churn(client, 1, 120)
        sub.bootstrap()
        sub.drain()
        stale, vid = sub.cursor, sub.vid

    own_server.restart()
    with gnitz.connect(own_server.target) as client:
        sub = Subscriber(client, "f")
        assert sub.vid == vid, "the view keeps its id across a restart"
        sub.cursor = stale
        with pytest.raises(gnitz.GnitzDeltaExpiredError):
            sub.poll()

        sub.bootstrap()
        sub.assert_converged("after the restart's bootstrap")
        churn(client, 121, 240)
        sub.assert_converged("after post-restart churn")


@NEEDS_MULTI
def test_a_replicated_feed_lives_on_worker_zero_alone(own_server):
    """A replicated view computes its entire result on every worker, so the feed
    is read from one copy: a gather over all W identical stores would hand back
    every row W times — invisible to a row-set comparison. Worker 0 serves it, so
    worker 0 is the only rank that opens a delta store for it. Asserted on the
    directories: the feed itself is per-worker and every other rank's store would
    be written and never read.

    A view over only replicated sources is itself replicated, which is what makes
    this a feed over a replicated placement rather than over a partitioned one.
    """
    own_server.start()
    with gnitz.connect(own_server.target) as client:
        client.execute_sql(
            "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL) "
            "WITH (replicated = true)",
        )
        mk_feed(client, "f", "SELECT id, v FROM r WHERE v > 10")
        sub = Subscriber(client, "f")
        sub.bootstrap()
        for i in range(1, 40):
            client.execute_sql(f"INSERT INTO r VALUES ({i}, {i * 10})")
        sub.assert_converged("over a replicated source")
        client.execute_sql("UPDATE r SET v = v + 5 WHERE id < 10; DELETE FROM r WHERE id > 30")
        sub.assert_converged("retractions over a replicated source")
        # The bootstrap ran before the inserts, so every row in the copy reached
        # it as a retained round through the one rank that serves the feed.
        assert sub.copy, "the feed carried the rounds the churn produced"
        vid = sub.vid

    view_dir = relation_dir(own_server.data_dir, vid)
    feeds = sorted(d for d in os.listdir(view_dir) if d.startswith("delta_w"))
    assert feeds == [f"delta_w0of{own_server.workers}"], f"only worker 0 serves this feed, found {feeds}"
