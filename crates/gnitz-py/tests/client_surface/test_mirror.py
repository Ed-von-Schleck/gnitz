"""A mirroring `gnitz.GnitzClient`: a local copy of a view, read in this
interpreter.

What this suite adds over the mirror crate's own — which owns the storage
shapes, the worker counts and the recovery paths — is everything only the
binding has: the pyclass and its lifetime inside one long-lived interpreter, the
error classes, the dict-and-`Row` result shape, and the fixtures that exist here
alone.

**Every correctness assertion is weight-exact, and every one of them asserts
that the answer came from the copy.** Without the second half the suite is
exposed to its own worst failure: a copy that is not valid makes every read
delegate to the server, and a mirror-versus-server comparison then compares the
server against itself and passes.
"""
import os
import signal
import threading

import pytest
import gnitz
from _feedviews import (
    FEED, GROUPBY, JOIN, LINEAR, SETOP,
    _base_tables, _churn, _flood, _key, _mk_feed, _zset,
)
from _read import rows
from _uid import uid as _uid
import _mirrorproc


# ── helpers ──────────────────────────────────────────────────────────────────


def _local(mirror, sn, vid, sql):
    """One read off the copy, with the proof that it was one."""
    assert mirror.mirrors(vid), f"{sql}: the view is not answered locally"
    before = mirror.requests_sent
    got = rows(mirror, sn, sql)
    assert mirror.requests_sent == before, f"{sql}: the read was delegated upstream"
    return got


def _same_zset(what, local_rows, remote_rows):
    """The two replies denote the same Z-set, and it is not the empty one."""
    a, b = _zset(local_rows), _zset(remote_rows)
    assert a or b, f"{what}: both replies are empty, so they agree about nothing"
    assert a == b, f"{what}: the copy and the server disagree"


def _same_sequence(what, local_rows, remote_rows):
    """The two replies are the same rows in the same order.

    Only for a query carrying `ORDER BY`: a multiset comparison is order-blind,
    so it accepts the right rows in the wrong order. A query without one is never
    compared this way — row order is unspecified, and the mirror is one partition
    where the server is W, so the sequences legitimately differ.
    """
    a = [(_key(r), r.weight) for r in local_rows]
    b = [(_key(r), r.weight) for r in remote_rows]
    assert a, f"{what}: the ordered reply is empty, so it agrees about nothing"
    assert a == b, f"{what}: the copy and the server disagree on order or content"


def _quiesce(client, mirror, sn, view="f"):
    """Make the rounds the pushes so far produced exist, then carry them into the
    copy.

    The order is the whole freshness contract and is not interchangeable: a poll
    drives no tick and a read against the server does, so a push ACKed but not
    yet ticked is in the server's answer and not in the copy. Reading first
    drains; one poll then suffices, because a poll covers `(cursor, cut]` whole.
    """
    client.execute_sql(f"SELECT COUNT(*) AS n FROM {view}", schema_name=sn)
    return mirror.poll()


# One read of each shape per body, in the body's own columns. The bodies differ
# in what they project, so a single query list cannot cover them.
_READS = {
    "linear": {
        "plain": [
            "SELECT * FROM f",
            "SELECT id, v FROM f WHERE v > 300",
            "SELECT v, COUNT(*) AS n, SUM(v) AS total FROM f GROUP BY v",
            # A PK point lookup. Only this body can be given one: the other three
            # have a hidden synthetic key, and every column a user can name there
            # is a payload column.
            "SELECT id, v, body FROM f WHERE id = 42",
        ],
        "ordered": "SELECT id, v FROM f ORDER BY v DESC, id ASC LIMIT 17",
    },
    "join": {
        "plain": [
            "SELECT * FROM f",
            "SELECT id, w FROM f WHERE w > 300",
            "SELECT w, COUNT(*) AS n, SUM(w) AS total FROM f GROUP BY w",
        ],
        "ordered": "SELECT id, w FROM f ORDER BY w DESC, id ASC LIMIT 17",
    },
    "groupby": {
        "plain": [
            "SELECT * FROM f",
            "SELECT tid, total FROM f WHERE total > 300",
            "SELECT n, COUNT(*) AS c, SUM(total) AS s FROM f GROUP BY n",
        ],
        "ordered": "SELECT tid, total FROM f ORDER BY total DESC, tid ASC LIMIT 17",
    },
    "setop": {
        "plain": [
            "SELECT * FROM f",
            "SELECT id FROM f WHERE id > 100",
            "SELECT id, COUNT(*) AS c FROM f GROUP BY id",
        ],
        "ordered": "SELECT id FROM f ORDER BY id DESC LIMIT 17",
    },
}
_BODIES = {"linear": LINEAR, "join": JOIN, "groupby": GROUPBY, "setop": SETOP}


def _fed_view(client, sn, body, feed=FEED):
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", body, feed=feed)


# ── P1 · the copy answers, and it is the copy that answers ───────────────────


@pytest.mark.parametrize("kind", list(_BODIES), ids=list(_BODIES))
def test_a_mirrored_read_equals_the_server_read(client, mirror, kind):
    """The differential, over every store shape a view body produces, through
    the binding's own result objects: the dicts, the `Row`s, and the client-side
    finishing over a single-worker reply."""
    sn = "s" + _uid()
    _fed_view(client, sn, _BODIES[kind])
    vid = mirror.mirror_view(sn, "f").view_id

    _churn(client, sn, 1, 200)
    _quiesce(client, mirror, sn)

    for q in _READS[kind]["plain"]:
        _same_zset(q, _local(mirror, sn, vid, q), rows(client, sn, q))
    q = _READS[kind]["ordered"]
    _same_sequence(q, _local(mirror, sn, vid, q), rows(client, sn, q))


# ── P2 · delegation ──────────────────────────────────────────────────────────


def test_an_unmirrored_relation_is_delegated_and_a_forgotten_one_goes_back(client, mirror):
    """Locality's falsifiable other half.

    A read the handle does not hold costs exactly two requests — one resolve and
    one read; without the statement bracket it would cost three — and a view the
    handle has forgotten issues requests again while staying correct.
    """
    sn = "s" + _uid()
    _fed_view(client, sn, LINEAR)
    vid = mirror.mirror_view(sn, "f").view_id
    _churn(client, sn, 1, 60)
    _quiesce(client, mirror, sn)

    before = mirror.requests_sent
    delegated = rows(mirror, sn, "SELECT * FROM t")
    cost = mirror.requests_sent - before
    _same_zset("a delegated read", delegated, rows(client, sn, "SELECT * FROM t"))
    assert cost == 2, f"a delegated scan must cost one resolve and one read, not {cost}"

    mirror.forget_view(vid)
    assert not mirror.mirrors(vid)
    before = mirror.requests_sent
    after_forget = rows(mirror, sn, "SELECT * FROM f")
    assert mirror.requests_sent > before, "a forgotten view must be read upstream again"
    _same_zset("a forgotten view", after_forget, rows(client, sn, "SELECT * FROM f"))


# ── P3 · the handle's life in an interpreter ─────────────────────────────────


def test_the_handles_life_in_one_interpreter(client, server, tmp_path):
    """Open, close, reopen.

    Not the `mirror` fixture: this test owns the clients' lifetimes, which is
    what it is about. Closing a mirroring client releases its copy directory,
    which is what lets the reopen below take it.
    """
    sn = "s" + _uid()
    _fed_view(client, sn, LINEAR)
    _churn(client, sn, 1, 40)
    base = str(tmp_path / "m")

    # Under `with` even for the explicit-close half, so a failing assertion
    # still releases the directory rather than holding it for the reopen below.
    with gnitz.connect(server) as first:
        first.mirror_at(base)
        r = first.mirror_view(sn, "f")
        assert r.reseeded is True, "a first registration bootstraps"

        first.close()
        first.close()  # idempotent — a host must be able to release a client twice
    for call in (lambda: first.poll(), lambda: first.mirrored_ids(), lambda: first.checkpoint()):
        with pytest.raises(gnitz.GnitzError):
            call()

    with gnitz.connect(server) as second:
        second.mirror_at(base)
        again = second.mirror_view(sn, "f")
        assert again.view_id == r.view_id, "the reopen lands on the same server id"
        assert again.reseeded is False, "a reopen resumes from its persisted cursor"
    with pytest.raises(gnitz.GnitzError):
        second.poll()

    # `close_mirror` is the narrower release: the copy goes, the connection
    # stays, and the directory can be attached again — on this client or another.
    with gnitz.connect(server) as third:
        third.mirror_at(base)
        third.close_mirror()
        assert third.mirrored_ids() == [], "the registrations went with the store"
        with pytest.raises(gnitz.GnitzError) as e:
            third.poll()
        assert "mirrors nothing" in str(e.value)
        # The connection is untouched.
        assert rows(third, sn, "SELECT * FROM t")
        third.mirror_at(base)
        assert third.mirror_view(sn, "f").view_id == r.view_id


# ── P4 · errors arrive as the right Python class ─────────────────────────────


def test_every_refusal_names_why(client, mirror):
    """What cannot be mirrored, and the message that says so."""
    sn = "s" + _uid()
    _fed_view(client, sn, LINEAR)
    client.execute_sql(f"CREATE VIEW plain AS {LINEAR}", schema_name=sn)
    client.execute_sql(f"CREATE VIEW bounded WITH (capacity = '1 MB') AS {LINEAR}", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, kind BIGINT NOT NULL) WITH (stream = true)",
        schema_name=sn,
    )

    for name, needle in [
        ("t", "is a table"),
        ("ev", "is a stream"),
        ("plain", "keeps no delta feed"),
        ("bounded", "capacity-bounded"),
        ("nosuchview", "nosuchview"),
    ]:
        with pytest.raises(gnitz.GnitzError) as e:
            mirror.mirror_view(sn, name)
        assert needle in str(e.value), f"mirroring {name!r} said: {e.value}"

    with pytest.raises(gnitz.GnitzError):
        mirror.mirror_view("s" + _uid(), "f")


@pytest.mark.parametrize("case,env,why", [
    ("poison", {"GNITZ_INJECT_INGEST_APPLY_ERROR": "store"},
     "a storage fault during the bootstrap ingest, which leaves no cursor"),
    ("panic", {"GNITZ_INJECT_MIRROR_INGEST_PANIC": "1"},
     "a panic inside the guarded apply of a poll, which leaves the copy gated in "
     "— cursor and registration both stand — so the reads it refuses are exactly "
     "the ones it would otherwise have answered off a copy silently missing rows, "
     "and the exception class a poison raised through the SQL layer must arrive as"),
], ids=["storage-fault", "panicking-apply"])
def test_a_fault_poisons_the_copy_and_the_process_lives(client, server, mirror_dir, case, env, why):
    """The host process surviving a fault is the whole reason the crate is safe
    to link into someone's application.

    In a fresh interpreter because a fault seam is a per-process latch that would
    contaminate every sibling test, and because a Rust panic crossing pyo3 raises
    a `BaseException` the suite should not have to step around. **The seam
    variable goes to the mirror child and nowhere else**: a server that inherited
    it would fail its own ingest and drop the connection, and the test would then
    report a protocol error instead of a poisoning.

    Gated on the *extension's* build, not the server's: these seams live in
    gnitz-mirror and gnitz-store, which the extension links and the server does
    not — `e2e-release` pairs a release server with a debug extension, and
    reading the server's build there would skip a test whose seam is armed.
    """
    if not gnitz.debug_assertions:
        pytest.skip(f"{why} — requires a debug extension")
    sn = "s" + _uid()
    _fed_view(client, sn, LINEAR)
    _churn(client, sn, 1, 40)
    client.execute_sql("SELECT COUNT(*) AS n FROM f", schema_name=sn)

    _mirrorproc.run(case, mirror_dir, server, sn, env=env)


# ── P5 · the poll report ─────────────────────────────────────────────────────


def test_one_poll_reports_every_view(client, mirror):
    """Ten views under one poll, and the shape of the cursor a host reads back.

    The tick is the master's global round counter, so a view's cursor advances
    over rounds that carried it nothing — stated here rather than left for a host
    to infer "my copy changed" from a tick that moved.
    """
    sn = "s" + _uid()
    _base_tables(client, sn)
    ids = {}
    for i in range(10):
        name = f"f{i}"
        _mk_feed(client, sn, name, f"SELECT id, v, body FROM t WHERE v > {i}")
        r = mirror.mirror_view(sn, name)
        assert r.reseeded is True
        ids[name] = r.view_id

    _churn(client, sn, 1, 60)
    first = _quiesce(client, mirror, sn, "f0")
    assert {r.view_id for r in first} == set(ids.values()), "one poll advances every registration"
    assert not any(r.reseeded for r in first), "an ordinary advance is not a reseed"
    assert all(r.error is None for r in first), "nothing failed, so nothing carries a message"
    advanced = {vid: mirror.cursor(vid) for vid in ids.values()}
    assert all(r.cursor == advanced[r.view_id] for r in first), (
        "the report carries the round each copy now answers at, so a host need not ask twice"
    )

    for r in mirror.poll():
        assert not r.reseeded, "an empty poll reseeds nothing"
        assert mirror.cursor(r.view_id) == advanced[r.view_id], (
            "with nothing pushed, a second poll moves no cursor"
        )

    assert mirror.cursor(ids["f0"])[1] == mirror.cursor(ids["f9"])[1], (
        "the round is the master's global counter, shared by every relation"
    )
    assert mirror.cursor(ids["f0"])[0] != mirror.cursor(ids["f9"])[0], "the tag is per-view"

    unknown = max(ids.values()) + 10_000
    assert mirror.cursor(unknown) is None, "a view with no valid copy has no cursor"


def test_one_poll_mixes_a_moved_view_with_idle_ones(client, mirror):
    """A poll over many views where exactly one has moved.

    The all-idle and all-moved polls are covered above and in the mirror crate;
    this is the mixed batch, the only one where the two reply shapes interleave
    on one connection — the moved view's rows arrive as a train the workers
    produced, every idle one's terminal is answered master-locally.
    """
    sn = "s" + _uid()
    _base_tables(client, sn)

    idle = {}
    for i in range(7):
        name = f"g{i}"
        _mk_feed(client, sn, name, f"SELECT id, w FROM u WHERE w > {i}")
        idle[name] = mirror.mirror_view(sn, name).view_id
    _mk_feed(client, sn, "f", "SELECT id, v, body FROM t WHERE v > 5")
    moved = mirror.mirror_view(sn, "f").view_id

    _churn(client, sn, 1, 60)
    for name in list(idle) + ["f"]:
        client.execute_sql(f"SELECT COUNT(*) AS n FROM {name}", schema_name=sn)
    mirror.poll()
    before = {vid: mirror.cursor(vid) for vid in list(idle.values()) + [moved]}
    idle_before = {vid: _zset(_local(mirror, sn, vid, f"SELECT * FROM {n}")) for n, vid in idle.items()}

    # Only `t` is written, so only `f` has a round to carry; `u`'s views are
    # up-to-date and answered by the master-local gate.
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'late-{i:0>20}')" for i in range(9001, 9040)),
        schema_name=sn,
    )
    report = {r.view_id: r for r in _quiesce(client, mirror, sn)}
    assert set(report) == set(before), "one poll reports every registration, moved or not"

    # The claim this test exists for: each report carries its *own* view's tag.
    # A poll correlating replies by arrival rather than by request would hand a
    # view another's cursor, and only a mixed batch can catch it.
    for vid, prev in before.items():
        assert report[vid].cursor[0] == prev[0], f"{vid}: a report carrying another view's tag is a mis-correlation"
        assert report[vid].cursor[1] >= prev[1], f"{vid}: the round never goes backwards"

    q = "SELECT * FROM f"
    _same_zset(q, _local(mirror, sn, moved, q), rows(client, sn, q))
    for name, vid in idle.items():
        q = f"SELECT * FROM {name}"
        local = _local(mirror, sn, vid, q)
        assert _zset(local) == idle_before[vid], f"{name}: nothing was pushed to it, so its copy must be unchanged"
        _same_zset(q, local, rows(client, sn, q))


def test_a_dead_view_fails_its_own_entry_and_stops_nothing_else(client, mirror):
    """A view dropped upstream fails forever, and the poll still reports every
    other one.

    `forget_view` clears it, and the id it needs is in the report rather than
    inside a formatted message.
    """
    sn = "s" + _uid()
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", LINEAR)
    _mk_feed(client, sn, "g", "SELECT id, v, body FROM t WHERE v > 1")
    dead = mirror.mirror_view(sn, "g").view_id
    alive = mirror.mirror_view(sn, "f").view_id
    _churn(client, sn, 1, 40)
    _quiesce(client, mirror, sn)

    client.execute_sql("DROP VIEW g", schema_name=sn)
    _churn(client, sn, 41, 80)
    report = {r.view_id: r for r in _quiesce(client, mirror, sn)}
    assert report[dead].error is not None, "the dropped view carries its own failure"
    assert report[dead].reseeded is False, "a failed view did not reseed"
    assert report[alive].error is None and report[alive].cursor is not None, (
        "the survivor advanced, and the report says to which round"
    )

    mirror.forget_view(dead)
    assert all(r.error is None for r in mirror.poll()), "the poll recovers once it is forgotten"
    q = "SELECT * FROM f"
    _same_zset(q, _local(mirror, sn, alive, q), rows(client, sn, q))


def test_a_reconnect_after_a_restart_reseeds_and_converges(own_server, mirror_on, mirror_dir):
    """A server restart kills the handle's socket and stops its tags continuing.

    `reconnect` attaches a fresh connection to the same copies; the next poll
    reports the reseed, and the differential holds afterwards.
    """
    own_server.start()
    sn = "s" + _uid()
    with gnitz.connect(own_server.sock_path) as client:
        _fed_view(client, sn, LINEAR)
        _churn(client, sn, 1, 60)
        m = mirror_on(mirror_dir, own_server.sock_path)
        vid = m.mirror_view(sn, "f").view_id
        _quiesce(client, m, sn)

    own_server.restart()
    m.reconnect(own_server.sock_path)
    with gnitz.connect(own_server.sock_path) as client:
        _churn(client, sn, 61, 120)
        assert any(r.view_id == vid and r.reseeded for r in _quiesce(client, m, sn)), (
            "a tag that stopped continuing is a reseed"
        )
        _quiesce(client, m, sn)
        q = "SELECT * FROM f"
        _same_zset(q, _local(m, sn, vid, q), rows(client, sn, q))


# ── P6 · a mirror is not read-your-own-writes ────────────────────────────────


def test_a_mirror_is_not_read_your_own_writes(client, mirror):
    """The contract a user is most likely to be surprised by: a write through the
    mirror's own connection is not in the copy until a poll carries its round."""
    sn = "s" + _uid()
    _fed_view(client, sn, LINEAR)
    _churn(client, sn, 1, 40)
    vid = mirror.mirror_view(sn, "f").view_id
    _quiesce(client, mirror, sn)

    q = "SELECT id, v FROM f WHERE id = 7"
    before = _zset(_local(mirror, sn, vid, q))
    mirror.execute_sql("UPDATE t SET v = 999999 WHERE id = 7", schema_name=sn)
    assert _zset(_local(mirror, sn, vid, q)) == before, (
        "the copy answers at its last poll, not at what the write just made true"
    )

    _quiesce(client, mirror, sn)
    after = _zset(_local(mirror, sn, vid, q))
    assert after != before, "a drain must carry the write into the copy"
    _same_zset(q, _local(mirror, sn, vid, q), rows(client, sn, q))


# ── P7 · cursor expiry recovers without the host seeing it ───────────────────


def test_an_expired_cursor_recovers_inside_the_poll(sweeping_server, mirror_on, mirror_dir):
    """The feed's one recovery, run by the handle rather than by the host.

    `GnitzDeltaExpiredError` must not reach the caller: the handle owns that
    recovery, and a host told to bootstrap again has nothing to do with the
    advice.
    """
    sn = "s" + _uid()
    with gnitz.connect(sweeping_server.sock_path) as client:
        _fed_view(client, sn, LINEAR, feed="1 KB")
        _churn(client, sn, 1, 60)
        m = mirror_on(mirror_dir, sweeping_server.sock_path)
        vid = m.mirror_view(sn, "f").view_id
        _quiesce(client, m, sn)

        # Push the worker's retention floor past the mirror's cursor. The floor
        # is crossed an order of magnitude below this; the margin is what keeps
        # it a reseed test rather than a threshold test.
        _flood(client, sn, 61, 5_000)
        client.execute_sql("SELECT COUNT(*) AS n FROM f", schema_name=sn)
        assert any(r.view_id == vid and r.reseeded for r in m.poll()), (
            "an expired cursor is recovered by reseeding"
        )

        _quiesce(client, m, sn)
        q = "SELECT id, v FROM f WHERE id > 4000"
        _same_zset(q, _local(m, sn, vid, q), rows(client, sn, q))


# ── P8 · a host crash ────────────────────────────────────────────────────────


def test_a_killed_host_reopens_at_its_last_checkpoint(client, server, mirror_on, mirror_dir):
    """`SIGKILL` means no destructor and so no exit checkpoint.

    The reopen must land on the last checkpoint and converge from there without
    doubling a weight — which is what a cursor file written out of step with the
    copies produces, and it leaves the row set identical.

    The signal goes to the mirror child, not the server, so the session server
    serves: nothing here kills, stops or restarts one.
    """
    sn = "s" + _uid()
    _fed_view(client, sn, LINEAR)
    _churn(client, sn, 1, 60)

    child = _mirrorproc.spawn("crash", mirror_dir, server, sn)
    try:
        _mirrorproc.wait_for_ready(child)
        os.kill(child.pid, signal.SIGKILL)
    finally:
        child.wait()
        child.stdout.close()
        child.stderr.close()

    m = mirror_on(mirror_dir, server)
    vid = m.mirror_view(sn, "f").view_id
    _quiesce(client, m, sn)
    for q in ("SELECT * FROM f", "SELECT COUNT(*) AS n, SUM(v) AS total FROM f"):
        _same_zset(q, _local(m, sn, vid, q), rows(client, sn, q))


# ── P9 · the mirror does not disturb the process ─────────────────────────────


def test_the_mirrors_own_connection_serves_every_other_statement(client, mirror):
    """DDL, DML and a transaction through the mirroring client, visible to a
    plain one — everything the copy cannot serve runs on the connection it is a
    field of."""
    sn = "s" + _uid()
    client.create_schema(sn)
    mirror.execute_sql(
        "CREATE TABLE k (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn
    )
    mirror.execute_sql("INSERT INTO k VALUES (1, 10), (2, 20)", schema_name=sn)
    mirror.execute_sql("BEGIN", schema_name=sn)
    mirror.execute_sql("INSERT INTO k VALUES (3, 30)", schema_name=sn)
    mirror.execute_sql("COMMIT", schema_name=sn)
    mirror.execute_sql("UPDATE k SET v = 99 WHERE id = 1", schema_name=sn)

    # Against the literal Z-set, not against the server: `k` is a table, which
    # the copy does not hold, so a mirror-versus-server comparison here would be
    # the server compared with itself and would pass however the writes landed.
    assert _zset(rows(client, sn, "SELECT * FROM k")) == {
        (("id", 1), ("v", 99)): 1,
        (("id", 2), ("v", 20)): 1,
        (("id", 3), ("v", 30)): 1,
    }


@pytest.mark.asyncio
async def test_an_open_mirror_leaves_the_rest_of_the_process_alone(client, server, mirror_dir):
    """A `gnitz.aio` transport and a plain client both work in an interpreter
    that has a copy open, and after it is closed."""
    from gnitz import aio

    sn = "s" + _uid()
    _fed_view(client, sn, LINEAR)
    _churn(client, sn, 1, 20)

    with gnitz.connect(server) as m:
        m.mirror_at(mirror_dir)
        vid = m.mirror_view(sn, "f").view_id
        _quiesce(client, m, sn)
        expected = _zset(rows(client, sn, "SELECT * FROM f"))
        assert expected, "the view must hold rows, or the reads below prove nothing"
        async with aio.connect(server) as aconn:
            assert _zset(await aconn.scan(vid)) == expected
        assert _zset(rows(client, sn, "SELECT * FROM f")) == expected

    async with aio.connect(server) as aconn:
        assert _zset(await aconn.scan(vid)) == expected
    assert _zset(rows(client, sn, "SELECT * FROM f")) == expected


# ── P10 · the GIL across a mirror call ──────────────────────────────────────


class _Contender:
    """A thread that does nothing but count, so increments over a window say
    whether the GIL was available to another thread across it.

    Counts, never elapsed time: a clock would measure the call instead.
    """

    def __init__(self):
        self._stop = threading.Event()
        self.count = 0
        self._thread = threading.Thread(target=self._spin, daemon=True)

    def _spin(self):
        while not self._stop.is_set():
            self.count += 1

    def __enter__(self):
        self._thread.start()
        while self.count == 0:
            pass
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()

    def during(self, call):
        """`call()`'s result, and the increments this thread managed while it ran."""
        before = self.count
        out = call()
        return out, self.count - before

    def wins(self, call, n=200):
        """`call()`'s last result, and whether this thread ran inside any of up to
        `n` repeats of it — stopping at the first window it did.

        Dropping the GIL only makes this thread *runnable*: the caller can retake
        it before the OS ever schedules us, losing three windows in four with
        every core busy. Retrying is the floor one window has not got. A call that
        never drops the GIL loses every window whatever the load, so the control
        needs only one.
        """
        out = None
        for _ in range(n):
            out, count = self.during(call)
            if count:
                return out, True
        return out, False


def test_a_mirror_call_releases_the_gil(client, mirror):
    """A mirror method that does work lets the rest of the interpreter run.

    Two calls that work — one that talks to the server, one answered entirely
    off the copy — and one control that only reads memory. The control is what
    makes the other two mean anything: without it the test cannot tell a released
    GIL from ambient scheduling.

    Every window holds one extension call and nothing else, so the control's zero
    is structural — no Python bytecode runs inside it to be preempted — and one
    window settles it. The other two are races against the OS scheduler rather
    than structural, so they retry until won.
    """
    sn = "s" + _uid()
    _fed_view(client, sn, LINEAR)
    _churn(client, sn, 1, 200)

    with _Contender() as spin:
        registered, _ = spin.during(lambda: mirror.mirror_view(sn, "f"))
        vid = registered.view_id

        _churn(client, sn, 201, 400)
        client.execute_sql("SELECT COUNT(*) AS n FROM f", schema_name=sn)
        polled, poll_dropped_gil = spin.wins(mirror.poll)

        before = mirror.requests_sent
        held, read_dropped_gil = spin.wins(
            lambda: rows(mirror, sn, "SELECT * FROM f")
        )
        assert mirror.requests_sent == before, "the mirrored read must be answered off the copy"

        _, during_control = spin.during(lambda: mirror.mirrors(vid))

    assert polled, "the poll must have covered the registered view"
    assert held, "the mirrored read must return rows, or it proves nothing"

    assert poll_dropped_gil, "a poll must drop the GIL across its delta read"
    assert read_dropped_gil, "a mirrored read must drop the GIL across the engine scan"
    assert during_control == 0, (
        f"a metadata getter answers out of memory, so it must never drop the GIL; "
        f"got {during_control} increments"
    )


# ── P11 · a mirrored read with the server stopped ────────────────────────────


def test_the_copy_answers_with_the_server_stopped(own_server, mirror_on, mirror_dir):
    """The feature's actual promise."""
    own_server.start()
    sn = "s" + _uid()
    with gnitz.connect(own_server.sock_path) as client:
        _fed_view(client, sn, LINEAR)
        _churn(client, sn, 1, 60)
        m = mirror_on(mirror_dir, own_server.sock_path)
        vid = m.mirror_view(sn, "f").view_id
        _quiesce(client, m, sn)
        expected = _zset(rows(client, sn, "SELECT * FROM f"))

    own_server.stop()

    assert _zset(_local(m, sn, vid, "SELECT * FROM f")) == expected
    assert _zset(_local(m, sn, vid, "SELECT id, v FROM f WHERE id = 7")) != {}

    # The poll cannot reach the server, and says so per view rather than by
    # raising: the copy is intact and every other view's entry is still worth
    # reading.
    assert all(r.error is not None for r in m.poll()), "a dead connection fails every view's poll"
    assert _zset(_local(m, sn, vid, "SELECT * FROM f")) == expected, (
        "and the copy still answers at the round it reached"
    )
    with pytest.raises(gnitz.GnitzError):
        m.execute_sql("SELECT * FROM t", schema_name=sn)


def test_replacing_a_mirrored_view_invalidates_the_copy(client, mirror):
    """`CREATE OR REPLACE VIEW` retires the view a copy was built from and puts a
    different one under the same name. The replacing client drops its own copy
    because the DDL bundle it just pushed says which view ids it retired — a
    negative VIEW_TAB weight with no positive one at the same id — so it cannot
    go on serving rows from a view that no longer exists. A name lookup would not
    tell it, since the name still resolves."""
    sn = "s" + _uid()
    _base_tables(client, sn)
    _mk_feed(client, sn, "f", LINEAR)
    old_vid = mirror.mirror_view(sn, "f").view_id
    _churn(client, sn, 1, 40)
    _quiesce(client, mirror, sn)
    q = "SELECT * FROM f"
    _same_zset(q, _local(mirror, sn, old_vid, q), rows(client, sn, q))

    # Replace through the mirroring client itself: the copy it holds is the one
    # being retired.
    mirror.execute_sql(
        f"CREATE OR REPLACE VIEW f WITH (delta = '{FEED}') AS SELECT id, v, body FROM t WHERE v > 20",
        schema_name=sn,
    )
    new_vid = client.resolve_table(sn, "f")[0]
    assert new_vid != old_vid, "a replaced view takes a fresh id"

    # The retired copy is gone: nothing answers off it any more.
    assert all(r.view_id != old_vid for r in mirror.poll()), (
        "the retired view left the mirror's poll set"
    )

    # Re-mirroring under the same name picks up the NEW definition and converges.
    fresh_vid = mirror.mirror_view(sn, "f").view_id
    assert fresh_vid == new_vid
    _churn(client, sn, 41, 60)
    _quiesce(client, mirror, sn)
    _same_zset(q, _local(mirror, sn, fresh_vid, q), rows(client, sn, q))
