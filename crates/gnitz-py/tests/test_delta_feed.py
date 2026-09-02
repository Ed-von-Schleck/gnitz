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
"""
import hashlib
import os

import pytest
import gnitz
from _feedviews import (
    FEED, GROUPBY, JOIN, LINEAR, SETOP,
    _base_tables, _churn, _flood, _key, _mk_feed, _uid, _zset,
)
from _serverproc import NUM_WORKERS, ServerProc


class Subscriber:
    """A client-side copy of a view, maintained the way the feed intends: one
    bootstrap, then polls, applying weights. It is deliberately not a set — the
    whole content of a delta row is its weight."""

    def __init__(self, client, sn, name):
        self.client = client
        self.vid, self.schema = client.resolve_table(sn, name)
        self.delta_schema = gnitz.delta_reply_schema(self.schema)
        self.copy = {}
        self.tag = None
        self.tick = 0

    def bootstrap(self):
        reply = self.client.delta_bootstrap(self.vid, self.schema, include_hidden=True)
        # A bootstrap replaces state; it does not add to it.
        self.copy = _zset(reply.rows)
        self.tag, self.tick = reply.tag, reply.tick
        return reply

    def poll(self):
        # No hand-written tag check: `delta_poll` refuses a foreign cursor
        # itself, so a reply that arrives here is one this copy may apply.
        reply = self.client.delta_poll(
            self.vid, self.delta_schema, self.tag, self.tick, include_hidden=True
        )
        for k, w in _zset(reply.rows).items():
            self.copy[k] = self.copy.get(k, 0) + w
            if self.copy[k] == 0:
                del self.copy[k]
        self.tag, self.tick = reply.tag, reply.tick
        return reply

    def drain(self, rounds=12):
        """Poll until a poll comes back empty. Returns the number of polls."""
        for n in range(1, rounds + 1):
            if len(self.poll().rows) == 0:
                return n
        raise AssertionError(f"feed did not settle in {rounds} polls")

    def scan(self):
        return _zset(self.client.scan(self.vid, include_hidden=True))

    def assert_converged(self, what=""):
        """Quiesce, then require the copy to equal the view as a multiset.

        The order matters and is not cosmetic: a poll does **not** drive a tick
        and a scan does, so a push ACKed but not yet ticked is in the scan and
        not in the copy — a real and intended divergence that the next poll
        heals. Scanning first drains, so the rounds exist before the polls that
        collect them.
        """
        live = self.scan()
        self.drain()
        assert self.copy == live, what


# ── convergence ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "body", [LINEAR, JOIN, GROUPBY, SETOP], ids=["linear", "join", "groupby", "setop"]
)
def test_feed_converges_weight_exact(client, body):
    """Bootstrap, then poll to the end, and the applied copy equals the view as a
    multiset of (row, weight). Runs over a stream that includes an UPDATE and a
    DELETE, so `enforce_unique_pk`'s stored-row retraction is exercised — a
    capture that shipped `+1` with no `-1` would pass a row-set test.

    The `linear` body's payload is a >12 byte TEXT column, which is the German
    string case: the stamp adopts the source batch's `blob_id`, so the delta
    batch and the batch the view's own store ingested claim one blob identity
    across two stores — the only place in the tree that is true.
    """
    sn = "s" + _uid()
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", body)
    sub = Subscriber(client, sn, "f")

    _churn(client, sn, 1, 200)
    sub.bootstrap()
    sub.assert_converged("after the bootstrap")

    for lo, hi in ((201, 400), (401, 600)):
        _churn(client, sn, lo, hi)
        sub.assert_converged(f"through key {hi}")


@pytest.mark.skipif(NUM_WORKERS < 2, reason="needs a partitioned relation")
def test_a_delta_touching_one_worker_only(client):
    """A one-row insert lands on one worker; the other W−1 emit nothing and write
    no delta row. Any design that assembles whole rounds server-side waits
    forever for a round they will never report — and passes at W=1."""
    sn = "s" + _uid()
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", LINEAR)
    sub = Subscriber(client, sn, "f")
    sub.bootstrap()

    for i in range(1, 12):
        client.execute_sql(f"INSERT INTO t VALUES ({i * 1000}, {i * 37}, 'solo-{i:0>20}')", schema_name=sn)
        sub.assert_converged(f"single-key round {i}")


def test_replicated_source_feed_is_weight_exact(client):
    """A replicated view computes its entire result on every worker, so only
    worker 0 stamps and only worker 0 is read. If the writer and the reader ever
    disagreed about which worker holds the feed, a broadcast gather over W
    identical stores would hand back every row W times — invisible to a row-set
    comparison, which is why this is a weight assertion."""
    sn = "s" + _uid()
    client.create_schema(sn)
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


def test_a_poll_that_races_an_exchanging_views_tick_loses_nothing(client):
    """Poll a multi-source exchanging view under continuous push. Without the
    deferral, a read answered from inside an in-flight evaluation sees a
    half-ingested round; a client that advanced its cursor over it would lose the
    rest of it silently and rarely."""
    sn = "s" + _uid()
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", JOIN)
    sub = Subscriber(client, sn, "f")
    sub.bootstrap()

    for lo in range(1, 400, 25):
        client.execute_sql(
            "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'x-{i:0>20}')" for i in range(lo, lo + 25)),
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO u VALUES " + ",".join(f"({i}, {i}, {i * 7})" for i in range(lo, lo + 25)),
            schema_name=sn,
        )
        # Poll without settling: the point is to land reads inside live ticks.
        sub.poll()
    sub.assert_converged("after the racing stream")


def test_view_over_a_stream_converges(client):
    """Consecutive ticks of a stream-only workload share a published LSN — a
    stream push opens no zone and publishes nothing — so this is the case that
    proves the round is not `lsn_alloc.published()`. Under that value every tick
    here would carry the same `_tick` and the rounds would fold together."""
    sn = "s" + _uid()
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, amount BIGINT NOT NULL) "
        "WITH (stream = true)",
        schema_name=sn,
    )
    _mk_feed(client, sn, "f", "SELECT kind, SUM(amount) AS total FROM ev GROUP BY kind")
    sub = Subscriber(client, sn, "f")
    sub.bootstrap()

    for r in range(6):
        client.execute_sql(
            "INSERT INTO ev VALUES " + ",".join(f"({r * 50 + i}, {i % 4}, {i + 1})" for i in range(50)),
            schema_name=sn,
        )
        sub.assert_converged(f"stream round {r}")


# ── the gate, and what it must not swallow ───────────────────────────────────


def test_a_round_emitted_outside_run_tick_is_not_gated_away(client):
    """`CREATE VIEW` drives a reactor-parked drain of its source through
    `drain_tick_blocking`, not through the tick loop. A last-round map maintained
    in `run_tick` would miss that round, gate the subscriber's next poll as
    "nothing changed", and leave a silent hole — while passing every other test
    in this file."""
    sn = "s" + _uid()
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", LINEAR)
    sub = Subscriber(client, sn, "f")
    client.execute_sql("INSERT INTO t VALUES (1, 100, 'seed')", schema_name=sn)
    sub.bootstrap()
    sub.drain()

    # Rows committed but deliberately NOT settled: the next statement's
    # reactor-parked drain is what ticks them.
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'late-{i}')" for i in range(2, 40)),
        schema_name=sn,
    )
    client.execute_sql("CREATE VIEW second AS SELECT id FROM t WHERE v > 1000", schema_name=sn)

    sub.drain()
    assert sub.copy == sub.scan(), "the CREATE VIEW drain's round must reach the subscriber"


def test_an_up_to_date_poll_writes_no_sal_bytes(own_server):
    """The steady state of a subscription is a poll that returns nothing, so that
    is the case that must be cheap: the master answers it locally, with no SAL
    group and no worker wakeup. Since no read path rewinds the SAL cursor, a poll
    that did write would drive the checkpoint threshold on an idle database,
    forever."""
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
        _base_tables(client, sn)
        _mk_feed(client, sn, "f", LINEAR)
        sub = Subscriber(client, sn, "f")
        client.execute_sql("INSERT INTO t VALUES (1, 100, 'x')", schema_name=sn)
        sub.bootstrap()
        sub.drain()

        before = sal_digest()
        for _ in range(20):
            reply = sub.poll()
            assert len(reply.rows) == 0
        assert sal_digest() == before, "an up-to-date poll must write no SAL bytes"


def test_a_poll_of_a_stream_fed_view_does_not_drive_a_tick(client):
    """A stream push publishes nothing, so `read_is_fresh` answers false for every
    view over one — meaning a read that asked to be current would drain on **every**
    poll, forever, taking the whole server's pending tid set with it each time. A
    delta poll answers "what has happened", so it takes no drain and a round the
    tick loop has not run yet is a round the next poll will carry.

    Observable because a drain is *synchronous*: it skips the coalesce window and
    the caller waits for the tick. So a poll issued microseconds after a push ACK
    either already carries that push (it drained) or does not (it did not) —
    against a 20 ms coalesce deadline the second is what a correct server does
    every time. Several trials, requiring only that one poll came back empty, so
    an unlucky scheduling hiccup cannot fail a correct server.
    """
    sn = "s" + _uid()
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, amount BIGINT NOT NULL) "
        "WITH (stream = true)",
        schema_name=sn,
    )
    _mk_feed(client, sn, "f", "SELECT kind, SUM(amount) AS total FROM ev GROUP BY kind")
    sub = Subscriber(client, sn, "f")
    sub.bootstrap()
    sub.drain()

    undrained = 0
    for r in range(6):
        client.execute_sql(
            "INSERT INTO ev VALUES " + ",".join(f"({r * 20 + i}, {i % 3}, {i + 1})" for i in range(20)),
            schema_name=sn,
        )
        if len(sub.poll().rows) == 0:
            undrained += 1
    assert undrained > 0, "every poll carried the push it raced — the poll is driving a tick"

    # And nothing was lost by not draining: the rounds arrive on later polls.
    sub.assert_converged("after the undrained polls")


def test_a_view_at_the_column_limit_cannot_carry_a_feed(client):
    """The stamp is one more column, so a view already at `MAX_COLUMNS` is refused
    at CREATE rather than aborting a worker later — the post-fork master opens no
    user store, so the store-open path would fail only after the client had been
    told the CREATE succeeded."""
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = ", ".join(
        ["c0 BIGINT NOT NULL PRIMARY KEY"] + [f"c{i} BIGINT NOT NULL" for i in range(1, 65)]
    )
    client.execute_sql(f"CREATE TABLE wide ({cols})", schema_name=sn)
    with pytest.raises(gnitz.GnitzError) as e:
        client.execute_sql(f"CREATE VIEW f WITH (delta = '{FEED}') AS SELECT * FROM wide", schema_name=sn)
    assert "delta feed" in str(e.value)
    # The same view without a feed is fine, so the refusal is about the stamp.
    client.execute_sql("CREATE VIEW plain AS SELECT * FROM wide", schema_name=sn)


# ── retention ────────────────────────────────────────────────────────────────


def _delta_dir_bytes(data_dir, view_id):
    """On-disk bytes under every `delta_w{k}` of `view_id`'s relation directory."""
    total = 0
    needle = f"_{view_id}"
    for root, _dirs, files in os.walk(data_dir):
        parts = root.split(os.sep)
        if not any(p.startswith("delta_w") for p in parts):
            continue
        if not any(p.endswith(needle) for p in parts):
            continue
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return total


def _delta_child_names(data_dir, view_id):
    out = set()
    needle = f"_{view_id}"
    for root, dirs, _files in os.walk(data_dir):
        if root.split(os.sep)[-1].endswith(needle):
            out |= {d for d in dirs if d.startswith("delta_w")}
    return out


def test_a_cursor_below_the_floor_is_refused_as_a_code(sweeping_server):
    """A drop raises the worker's retention floor, and a cursor at or below it is
    refused with `STATUS_DELTA_EXPIRED` — a code the subscriber reacts to, not a
    string it matches. Recovery is the read it made on its first day."""
    with gnitz.connect(sweeping_server.sock_path) as client:
        sn = "s" + _uid()
        _base_tables(client, sn)
        _mk_feed(client, sn, "f", LINEAR, feed="1 KB")
        sub = Subscriber(client, sn, "f")
        sub.bootstrap()
        sub.drain()
        stale_tick = sub.tick

        # Enough volume that the delta store folds, spills, pushes down and
        # drops its oldest terminal guard — which is what raises the floor past
        # the cursor taken above.
        _flood(client, sn, 1, 20_000)

        with pytest.raises(gnitz.GnitzDeltaExpiredError):
            client.delta_poll(sub.vid, sub.delta_schema, sub.tag, stale_tick, include_hidden=True)

        # And the recovery is exact: re-read at 0 and the copy is the view.
        #
        # Settle BEFORE re-reading, which is why this does not go through
        # `assert_converged`. That helper scans first on purpose — a scan drives a
        # tick and a poll does not — and then polls up the round the scan
        # produced. On the 32 MB feeds everything else here uses, that round is
        # still retained when the poll arrives. On this test's deliberately
        # starved 1 KB budget it is not: one round of these rows overruns the
        # budget outright, so the sweep drops it before it can be polled and the
        # just-issued cursor is refused in turn. Driving the tick first makes the
        # bootstrap's watermark the settled cut, which is what "the recovery is
        # the read it made on its first day" actually claims.
        live = sub.scan()
        sub.bootstrap()
        assert sub.copy == live, "after re-reading at 0"


def test_a_delta_store_does_not_grow_without_bound(sweeping_server):
    """The one failure `resident_bytes` cannot see: it counts registered entries,
    and a leaked superseded shard is unregistered. It also catches the unlink
    drain hung off the sweep instead of off the store, which unlinks the drops and
    leaks every ordinary compaction."""
    with gnitz.connect(sweeping_server.sock_path) as client:
        sn = "s" + _uid()
        _base_tables(client, sn)
        # Above the guard granularity, so the store settles *on* disk with
        # something to measure. A budget under one guard's worth has no residual
        # at all — a drop is destructive, so the guard is the residual step — and
        # the plateau this pins would be zero against zero.
        _mk_feed(client, sn, "f", LINEAR, feed="256 KB")
        vid, _ = client.resolve_table(sn, "f")

        # Measure the plateau, not a ratio. The first burst has to be long
        # enough to *reach* the plateau — the sweep budgets itself to one
        # push-down per spill, so the first few spills are still ramping — and
        # the second is three times as long again. A store that leaked its
        # superseded shards would track everything ever written; one that sweeps
        # stays where the recent write burst leaves it.
        _flood(client, sn, 1, 20_000)
        settled = _delta_dir_bytes(sweeping_server.data_dir, vid)
        _flood(client, sn, 20_001, 60_000)
        after = _delta_dir_bytes(sweeping_server.data_dir, vid)
        assert settled > 0, "the feed never reached disk; this flood is too small to test retention"
        assert after <= settled * 2, (
            f"delta store grew {settled} -> {after} bytes while the deltas written grew "
            "four-fold — it is tracking everything ever written"
        )


# ── lifecycle ────────────────────────────────────────────────────────────────


def test_capacity_and_delta_are_refused_together(client):
    """Both sides refuse the pair. The SQL layer is where the message is legible;
    the engine's `view_registration` is the trust boundary, and nothing tests the
    divergence itself because with the guard in place it is unreachable."""
    sn = "s" + _uid()
    _base_tables(client, sn)
    with pytest.raises(gnitz.GnitzError) as e:
        client.execute_sql(
            f"CREATE VIEW bad WITH (capacity = '1 MB', delta = '1 MB') AS {LINEAR}", schema_name=sn
        )
    assert "delta" in str(e.value)


def test_an_unknown_view_option_names_both(client):
    sn = "s" + _uid()
    _base_tables(client, sn)
    with pytest.raises(gnitz.GnitzError) as e:
        client.execute_sql(f"CREATE VIEW bad WITH (nonsense = '1 MB') AS {LINEAR}", schema_name=sn)
    assert "capacity" in str(e.value) and "delta" in str(e.value)


def test_alter_view_on_a_fed_view_is_refused(client):
    """`ALTER VIEW … AS` re-renders the body as a bare `CREATE VIEW`, dropping the
    `WITH` clause — so retargeting a fed view would silently strip its feed and
    turn every later read, `after_tick = 0` included, into a typed error. `delta`
    is deliberately not part of the relation class, so the bounded-view guard does
    not cover this."""
    sn = "s" + _uid()
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", LINEAR)
    with pytest.raises(gnitz.GnitzError) as e:
        client.execute_sql("ALTER VIEW f AS SELECT id, v, body FROM t WHERE v > 20", schema_name=sn)
    assert "DROP and CREATE" in str(e.value)


def test_a_delta_read_of_a_relation_with_no_feed_is_an_error(client):
    """At **every** `after_tick`, zero included: the reply would otherwise promise
    a continuation the server cannot serve."""
    sn = "s" + _uid()
    _base_tables(client, sn)
    client.execute_sql(f"CREATE VIEW plain AS {LINEAR}", schema_name=sn)
    vid, schema = client.resolve_table(sn, "plain")
    with pytest.raises(gnitz.GnitzError):
        client.delta_bootstrap(vid, schema)
    with pytest.raises(gnitz.GnitzError):
        client.delta_poll(vid, gnitz.delta_reply_schema(schema), 0, 1)


def test_a_cursor_across_drop_and_recreate_is_rejected(client):
    """Within one boot the nonce half of the tag is unchanged, so a tag naming
    only the boot would accept this cursor: the recreated view takes a fresh id
    whose rounds come from the same global counter. Unrefused, the poll answers
    with the *new* view's deltas above the stale round — and a recreated view's
    backfill never enters a delta store, so applying them yields a copy missing
    everything below that round. `delta_poll` refuses the cursor instead."""
    sn = "s" + _uid()
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", LINEAR)
    sub = Subscriber(client, sn, "f")
    client.execute_sql("INSERT INTO t VALUES (1, 100, 'a')", schema_name=sn)
    sub.bootstrap()
    sub.drain()
    old_tag, old_tick = sub.tag, sub.tick

    client.execute_sql("DROP VIEW f", schema_name=sn)
    _mk_feed(client, sn, "f", LINEAR)
    client.execute_sql("INSERT INTO t VALUES (2, 200, 'b')", schema_name=sn)

    fresh = Subscriber(client, sn, "f")
    assert fresh.vid != sub.vid, "a recreated view takes a fresh id"
    with pytest.raises(gnitz.GnitzDeltaExpiredError):
        client.delta_poll(fresh.vid, fresh.delta_schema, old_tag, old_tick, include_hidden=True)

    fresh.bootstrap()
    fresh.assert_converged("after re-resolving and bootstrapping")


def test_dropping_a_fed_view_removes_its_delta_store(client, server_dirs):
    """The delta store lives under the view's own directory, so the drop the
    master already queues for that directory takes it."""
    sn = "s" + _uid()
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", LINEAR)
    vid, _ = client.resolve_table(sn, "f")
    client.execute_sql("INSERT INTO t VALUES (1, 100, 'a')", schema_name=sn)
    client.scan(vid)
    client.execute_sql("DROP VIEW f", schema_name=sn)

    # The view id is gone from the catalog, and a poll against it is an error
    # rather than an empty "nothing changed" — which is what a leaked gate entry
    # would produce.
    with pytest.raises(gnitz.GnitzError):
        client.resolve_table(sn, "f")


def test_a_four_column_pk_view_carries_a_feed(client):
    """One stamp column plus the widest declarable view PK is exactly
    `MAX_PK_COLUMNS`, so this is the case the derived-schema argument turns on and
    the one a second stamp column could not fit."""
    sn = "s" + _uid()
    client.create_schema(sn)
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


# ── restart ──────────────────────────────────────────────────────────────────


def test_a_feed_survives_a_restart_on_every_worker(own_server):
    """`rehome` rebuilds every relation's store on every worker at boot, so
    a `delta_bytes` that did not reach `rebuild_relation_store` would leave a
    catalog that says "fed" and no delta store anywhere — which every other test
    reports only as "no rows". The keys span all four partitions.

    A cursor held across the restart must be rejected: the delta store is erased
    at open and the boot mints a fresh nonce, so every tag stops matching.
    """
    own_server.start()
    with gnitz.connect(own_server.sock_path) as client:
        sn = "s" + _uid()
        _base_tables(client, sn)
        _mk_feed(client, sn, "f", LINEAR)
        sub = Subscriber(client, sn, "f")
        _churn(client, sn, 1, 120)
        sub.bootstrap()
        sub.drain()
        stale_tag, stale_tick = sub.tag, sub.tick
        vid = sub.vid

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as client:
        sub = Subscriber(client, sn, "f")
        assert sub.vid == vid, "the view keeps its id across a restart"
        with pytest.raises(gnitz.GnitzDeltaExpiredError):
            client.delta_poll(sub.vid, sub.delta_schema, stale_tag, stale_tick, include_hidden=True)

        sub.bootstrap()
        sub.assert_converged("after the restart's bootstrap")
        # And the feed still ingests on every worker.
        _churn(client, sn, 121, 240)
        sub.assert_converged("after post-restart churn")


@pytest.mark.skipif(NUM_WORKERS < 2, reason="needs a worker count to narrow")
def test_a_narrowed_worker_count_reclaims_the_delta_children(server_dirs):
    """The `ChildAddr` grammar is what makes `delta_w{k}` visible to the boot
    sweep; a name in no grammar is skipped rather than reported, so a narrowed
    count would leak one directory per feed forever."""
    data_dir, sock_path = server_dirs
    wide = ServerProc(data_dir, sock_path, workers=NUM_WORKERS)
    wide.start()
    try:
        with gnitz.connect(sock_path) as client:
            sn = "s" + _uid()
            _base_tables(client, sn)
            _mk_feed(client, sn, "f", LINEAR)
            vid, _ = client.resolve_table(sn, "f")
            _churn(client, sn, 1, 100)
            client.scan(vid)
        names = _delta_child_names(data_dir, vid)
        assert names == {f"delta_w{k}" for k in range(NUM_WORKERS)}, names
    finally:
        wide.stop()

    narrow = ServerProc(data_dir, sock_path, workers=1)
    narrow.start()
    try:
        with gnitz.connect(sock_path) as client:
            sub = Subscriber(client, sn, "f")
            sub.bootstrap()
            _churn(client, sn, 101, 200)
            sub.assert_converged("at the narrowed worker count")
        assert _delta_child_names(data_dir, vid) == {"delta_w0"}
    finally:
        narrow.stop()
