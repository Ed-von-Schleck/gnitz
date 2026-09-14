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

import pytest
import gnitz
from _feedviews import (
    FEED, GROUPBY, JOIN, LINEAR, SETOP,
    _base_tables, _churn, _flood, _mk_feed, _zset,
)
from _uid import uid as _uid
from _serverproc import NEEDS_MULTI


class Subscriber:
    """A client-side copy of a view, maintained the way the feed intends: one
    bootstrap, then polls, applying weights. It is deliberately not a set — the
    whole content of a delta row is its weight."""

    def __init__(self, client, sn, name):
        self.client = client
        self.vid, self.schema = client.resolve_table(sn, name)
        self.delta_schema = gnitz.delta_reply_schema(self.schema)
        self.copy = {}
        self.cursor = (0, 0)

    def bootstrap(self):
        reply = self.client.delta_bootstrap(self.vid, self.schema)
        # A bootstrap replaces state; it does not add to it.
        self.copy = _zset(reply.rows.including_hidden())
        self.cursor = reply.cursor

    def poll(self):
        # No hand-written tag check: `delta_poll` refuses a foreign cursor
        # itself, so a reply that arrives here is one this copy may apply.
        reply = self.client.delta_poll(self.vid, self.delta_schema, self.cursor)
        for k, w in _zset(reply.rows.including_hidden()).items():
            self.copy[k] = self.copy.get(k, 0) + w
            if self.copy[k] == 0:
                del self.copy[k]
        self.cursor = reply.cursor
        return reply

    def drain(self):
        """Poll until a poll comes back empty."""
        for _ in range(12):
            if len(self.poll().rows) == 0:
                return
        raise AssertionError("feed did not settle in 12 polls")

    def scan(self):
        return _zset(self.client.scan(self.vid).including_hidden())

    def assert_converged(self, what=""):
        """Quiesce, then require the copy to equal the view as a multiset.

        The order matters: a poll does **not** drive a tick and a scan does, so
        a push ACKed but not yet ticked is in the scan and not in the copy — a
        real and intended divergence that the next poll heals. Scanning first
        drains, so the rounds exist before the polls that collect them.
        """
        live = self.scan()
        self.drain()
        assert live or self.copy, f"{what}: both sides are empty, so they agree about nothing"
        assert self.copy == live, what


# ── convergence ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "body", [LINEAR, JOIN, GROUPBY, SETOP], ids=["linear", "join", "groupby", "setop"]
)
def test_feed_converges_weight_exact(client, schema_name, body):
    """Bootstrap, then poll to the end, and the applied copy equals the view as a
    multiset of (row, weight). The churn includes an UPDATE and a DELETE, so a
    capture that shipped `+1` with no `-1` fails here where a row-set test would
    pass. The `linear` body carries a >12-byte TEXT payload, so the delta rows
    carry out-of-line strings."""
    sn = schema_name
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", body)
    sub = Subscriber(client, sn, "f")

    _churn(client, sn, 1, 200)
    sub.bootstrap()
    sub.assert_converged("after the bootstrap")

    for lo, hi in ((201, 400), (401, 600)):
        _churn(client, sn, lo, hi)
        sub.assert_converged(f"through key {hi}")


@NEEDS_MULTI
def test_a_delta_touching_one_worker_only(client, schema_name):
    """A one-row insert lands on one worker; the other W−1 emit nothing and write
    no delta row. Any design that assembles whole rounds server-side waits
    forever for a round they will never report — and passes at W=1."""
    sn = schema_name
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", LINEAR)
    sub = Subscriber(client, sn, "f")
    sub.bootstrap()

    for i in range(1, 12):
        client.execute_sql(f"INSERT INTO t VALUES ({i * 1000}, {i * 37}, 'solo-{i:0>20}')", schema_name=sn)
        sub.assert_converged(f"single-key round {i}")


@NEEDS_MULTI
def test_replicated_source_feed_is_weight_exact(client, schema_name):
    """A replicated view computes its entire result on every worker, so the feed
    is read from one copy. A gather over all W identical stores would hand back
    every row W times — invisible to a row-set comparison, which is why this is a
    weight assertion."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL) WITH (replicated = true)",
        schema_name=sn,
    )
    _mk_feed(client, sn, "f", "SELECT id, v FROM r WHERE v > 3")
    sub = Subscriber(client, sn, "f")
    sub.bootstrap()

    for lo in (1, 41, 81):
        client.execute_sql(
            "INSERT INTO r VALUES " + ",".join(f"({i}, {i * 2})" for i in range(lo, lo + 40)),
            schema_name=sn,
        )
        client.execute_sql(f"UPDATE r SET v = v + 5 WHERE id >= {lo} AND id < {lo + 10}", schema_name=sn)
        sub.assert_converged(f"replicated through {lo + 40}")


def test_a_four_column_pk_view_carries_a_feed(client, schema_name):
    """The stamp is one more PK column, so the widest declarable view PK plus the
    stamp is exactly `MAX_PK_COLUMNS`."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE q (a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, d BIGINT NOT NULL, "
        "v BIGINT NOT NULL, PRIMARY KEY (a, b, c, d))",
        schema_name=sn,
    )
    _mk_feed(client, sn, "f", "SELECT a, b, c, d, v FROM q WHERE v > 1")
    sub = Subscriber(client, sn, "f")
    sub.bootstrap()
    for r in range(4):
        client.execute_sql(
            "INSERT INTO q VALUES " + ",".join(f"({r}, {i}, {i * 2}, {i * 3}, {i + 2})" for i in range(20)),
            schema_name=sn,
        )
        sub.assert_converged(f"compound-PK round {r}")


# ── which rounds a poll sees ─────────────────────────────────────────────────


def test_a_round_ticked_by_a_ddl_drain_reaches_the_next_poll(client, schema_name):
    """`CREATE VIEW` drains its source's pending ticks itself, outside the tick
    loop. That round must reach the subscriber like any other, not be gated away
    as "nothing changed"."""
    sn = schema_name
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", LINEAR)
    sub = Subscriber(client, sn, "f")
    client.execute_sql("INSERT INTO t VALUES (1, 100, 'seed')", schema_name=sn)
    sub.bootstrap()
    sub.drain()

    # Rows committed but deliberately NOT settled: the next statement's drain is
    # what ticks them.
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'late-{i}')" for i in range(2, 40)),
        schema_name=sn,
    )
    client.execute_sql("CREATE VIEW second AS SELECT id FROM t WHERE v > 1000", schema_name=sn)

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

    with gnitz.connect(own_server.sock_path) as client:
        sn = "s" + _uid()
        client.create_schema(sn)
        _base_tables(client, sn)
        _mk_feed(client, sn, "f", LINEAR)
        sub = Subscriber(client, sn, "f")
        client.execute_sql("INSERT INTO t VALUES (1, 100, 'x')", schema_name=sn)
        sub.bootstrap()
        sub.drain()

        before = sal_digest()
        for _ in range(20):
            assert len(sub.poll().rows) == 0
        assert sal_digest() == before, "an up-to-date poll must write no SAL bytes"


def test_a_poll_of_a_stream_fed_view_takes_no_tick_and_loses_no_round(client, schema_name):
    """Every other read of a stream-fed view drains pending ticks, since a stream
    push never advances the published tick. A delta poll answers "what has
    happened", so it drains nothing — else every poll would tick the whole server.

    Observable because a drain is synchronous: a poll issued right after a push
    ACK either already carries that push (it drained) or does not. These rounds
    are far below any row count that triggers a tick on its own, so every poll
    comes back empty — and each round must still arrive on a later poll, as its
    own round rather than folded into its neighbour.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, amount BIGINT NOT NULL) "
        "WITH (stream = true)",
        schema_name=sn,
    )
    _mk_feed(client, sn, "f", "SELECT kind, SUM(amount) AS total FROM ev GROUP BY kind")
    sub = Subscriber(client, sn, "f")
    sub.bootstrap()
    sub.drain()

    for r in range(6):
        client.execute_sql(
            "INSERT INTO ev VALUES " + ",".join(f"({r * 50 + i}, {i % 4}, {i + 1})" for i in range(50)),
            schema_name=sn,
        )
        assert len(sub.poll().rows) == 0, f"poll {r} carried the push it raced — it drove a tick"

    sub.assert_converged("after the undrained polls")


# ── refused cursors ──────────────────────────────────────────────────────────


def test_a_cursor_below_the_floor_is_refused_as_a_code(sweeping_server):
    """Retention drops the oldest rounds whether or not anyone still reads them,
    and a cursor below what was dropped is refused with `GnitzDeltaExpiredError`
    — a type the subscriber reacts to, not a string it matches. Recovery is the
    read it made on its first day."""
    with gnitz.connect(sweeping_server.sock_path) as client:
        sn = "s" + _uid()
        client.create_schema(sn)
        _base_tables(client, sn)
        _mk_feed(client, sn, "f", LINEAR, feed="1 KB")
        sub = Subscriber(client, sn, "f")
        sub.bootstrap()
        sub.drain()

        # Twice the volume measured to reach the first drop, so the margin is
        # the test's and not the box's.
        _flood(client, sn, 1, 8_000)

        with pytest.raises(gnitz.GnitzDeltaExpiredError):
            sub.poll()

        # Settle before re-reading rather than going through `assert_converged`:
        # on a 1 KB budget one round of these rows overruns it outright, so a
        # round ticked by that helper's scan is dropped before its poll arrives.
        live = sub.scan()
        sub.bootstrap()
        assert sub.copy == live, "after re-reading at 0"


def test_a_delta_read_of_a_relation_with_no_feed_is_an_error(client, schema_name):
    """At **every** `after_tick`, zero included: the reply would otherwise promise
    a continuation the server cannot serve."""
    sn = schema_name
    _base_tables(client, sn)
    client.execute_sql(f"CREATE VIEW plain AS {LINEAR}", schema_name=sn)
    vid, schema = client.resolve_table(sn, "plain")
    for verb in (
        lambda: client.delta_bootstrap(vid, schema),
        lambda: client.delta_poll(vid, gnitz.delta_reply_schema(schema), (0, 1)),
    ):
        with pytest.raises(gnitz.GnitzError) as e:
            verb()
        # Not the expiry code: a subscriber reacts to that by re-reading at 0,
        # so answering it here would spin one forever against a view that will
        # never have a feed.
        assert not isinstance(e.value, gnitz.GnitzDeltaExpiredError), str(e.value)


@pytest.mark.parametrize("recreate", ["drop-and-create", "or-replace"])
def test_a_cursor_across_a_recreate_is_rejected(client, schema_name, recreate):
    """A recreated view takes a fresh id whose rounds come from the same global
    counter, so an unrefused cursor would be answered with the *new* view's deltas
    above the stale round — and a recreated view's backfill never enters a delta
    store, so applying them yields a copy missing everything below it.

    `CREATE OR REPLACE` is the case nothing else would tell a subscriber about:
    unlike the drop, the name still resolves.
    """
    sn = schema_name
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", LINEAR)
    sub = Subscriber(client, sn, "f")
    client.execute_sql("INSERT INTO t VALUES (1, 100, 'a')", schema_name=sn)
    sub.bootstrap()
    sub.drain()

    if recreate == "drop-and-create":
        client.execute_sql("DROP VIEW f", schema_name=sn)
        _mk_feed(client, sn, "f", LINEAR)
    else:
        client.execute_sql(
            f"CREATE OR REPLACE VIEW f WITH (delta = '{FEED}') AS {LINEAR}", schema_name=sn
        )
    client.execute_sql("INSERT INTO t VALUES (2, 200, 'b')", schema_name=sn)

    fresh = Subscriber(client, sn, "f")
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
    with gnitz.connect(own_server.sock_path) as client:
        sn = "s" + _uid()
        client.create_schema(sn)
        _base_tables(client, sn)
        _mk_feed(client, sn, "f", LINEAR)
        sub = Subscriber(client, sn, "f")
        _churn(client, sn, 1, 120)
        sub.bootstrap()
        sub.drain()
        stale, vid = sub.cursor, sub.vid

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as client:
        sub = Subscriber(client, sn, "f")
        assert sub.vid == vid, "the view keeps its id across a restart"
        sub.cursor = stale
        with pytest.raises(gnitz.GnitzDeltaExpiredError):
            sub.poll()

        sub.bootstrap()
        sub.assert_converged("after the restart's bootstrap")
        _churn(client, sn, 121, 240)
        sub.assert_converged("after post-restart churn")
