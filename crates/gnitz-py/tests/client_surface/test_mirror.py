"""A mirroring `gnitz.GnitzClient`: a local copy of a view, read in this
interpreter.

What this suite adds over the mirror crate's own — which owns the storage
shapes, the view bodies, the worker counts and the poll's recovery paths — is
everything only the binding has: the pyclass and its lifetime inside one
long-lived interpreter, the error classes, the poll report and dict-and-`Row`
result shapes, and the GIL.

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
from _feedviews import LINEAR, base_tables, churn, mk_feed
from _read import bag, ordered, rows
import _childproc


# ── helpers ──────────────────────────────────────────────────────────────────


def _local(mirror, vid, sql):
    """One read off the copy, with the proof that it was one."""
    assert mirror.cursor(vid) is not None, f"{sql}: the view is not answered locally"
    before = mirror.requests_sent
    got = rows(mirror, sql)
    assert mirror.requests_sent == before, f"{sql}: the read was delegated upstream"
    return got


def _samebag(what, local_rows, remote_rows):
    """The two replies denote the same Z-set, and it is not the empty one."""
    a, b = bag(local_rows), bag(remote_rows)
    assert a or b, f"{what}: both replies are empty, so they agree about nothing"
    assert a == b, f"{what}: the copy and the server disagree"


def _quiesce(client, mirror):
    """Make the rounds the pushes so far produced exist, then carry them into the
    copy.

    The order is the whole freshness contract: a poll drives no tick and a read
    against the server does, draining every pending table, so a push ACKed but
    not yet ticked is in the server's answer and not in the copy. Reading first
    drains; one poll then suffices, because a poll covers `(cursor, cut]` whole.
    """
    client.execute_sql("SELECT COUNT(*) AS n FROM f")
    return mirror.poll()


def _fed_view(client, body=LINEAR):
    base_tables(client)
    mk_feed(client, "f", body)


# ── the copy answers, and it is the copy that answers ───────────────────────


def test_a_mirrored_read_equals_the_server_read(client, mirror):
    """The differential through the binding's own result objects: the dicts, the
    `Row`s, and the client-side finishing over a single-worker reply. An ordered
    read is compared as a sequence — a multiset comparison accepts the right rows
    in the wrong order."""
    _fed_view(client)
    vid = mirror.mirror_view("f").view_id

    churn(client, 1, 200)
    _quiesce(client, mirror)

    for q in [
        "SELECT * FROM f",
        "SELECT id, v FROM f WHERE v > 300",
        "SELECT v, COUNT(*) AS n, SUM(v) AS total FROM f GROUP BY v",
        "SELECT id, v, body FROM f WHERE id = 42",
    ]:
        _samebag(q, _local(mirror, vid, q), rows(client, q))

    q = "SELECT id, v FROM f ORDER BY v DESC, id ASC LIMIT 17"
    local = ordered(_local(mirror, vid, q))
    assert local, "the ordered reply is empty, so it agrees about nothing"
    assert local == ordered(rows(client, q))


def test_what_the_copy_does_not_hold_is_read_upstream(client, mirror):
    """Locality's falsifiable other half: a relation the handle does not hold,
    and a view it has forgotten, issue requests and stay correct."""
    _fed_view(client)
    vid = mirror.mirror_view("f").view_id
    churn(client, 1, 60)
    _quiesce(client, mirror)

    before = mirror.requests_sent
    _samebag("a table", rows(mirror, "SELECT * FROM t"), rows(client, "SELECT * FROM t"))
    assert mirror.requests_sent > before, "a table the copy does not hold must be read upstream"

    mirror.forget_view(vid)
    assert mirror.cursor(vid) is None
    before = mirror.requests_sent
    _samebag("a forgotten view", rows(mirror, "SELECT * FROM f"), rows(client, "SELECT * FROM f"))
    assert mirror.requests_sent > before, "a forgotten view must be read upstream again"


def test_a_point_read_is_answered_off_the_copy(client, mirror):
    """`seek` and `seek_by_index` over a mirrored view send nothing, carry no
    LSN, and agree with the same read through a client that mirrors nothing."""
    _fed_view(client)
    vid = mirror.mirror_view("f").view_id
    churn(client, 1, 60)
    _quiesce(client, mirror)
    _, schema = mirror.resolve_table("f")
    for what, read in [
        ("seek", lambda c: c.seek(vid, schema, 30)),
        ("seek_by_index over the key", lambda c: c.seek_by_index(vid, schema, [0], [30])),
        ("seek_by_index over a payload column", lambda c: c.seek_by_index(vid, schema, [1], [90])),
    ]:
        before = mirror.requests_sent
        local = read(mirror)
        assert mirror.requests_sent == before, f"{what}: the read was delegated upstream"
        assert local.lsn is None, f"{what}: a local answer carries no LSN"
        remote = read(client)
        assert remote.lsn is not None
        _samebag(what, local, remote)


# ── the handle's life in an interpreter ─────────────────────────────────────


def test_the_handles_life_in_one_interpreter(client, server, tmp_path):
    """Open, close, reopen.

    Not the `mirror` fixture: this test owns the clients' lifetimes, which is
    what it is about. Closing a mirroring client releases its copy directory,
    which is what lets the reopen below take it.
    """
    _fed_view(client)
    churn(client, 1, 40)
    base = str(tmp_path / "m")

    # Under `with` even for the explicit-close half, so a failing assertion
    # still releases the directory rather than holding it for the reopen below.
    with gnitz.connect(server, schema=client.schema) as first:
        first.mirror_at(base)
        r = first.mirror_view("f")
        assert r.reseeded is True, "a first registration bootstraps"

        first.close()
        first.close()  # idempotent — a host must be able to release a client twice
    for call in (first.poll, first.mirrored_ids, first.checkpoint):
        with pytest.raises(gnitz.GnitzConnectionError):
            call()

    with gnitz.connect(server, schema=client.schema) as second:
        second.mirror_at(base)
        again = second.mirror_view("f")
        assert again.view_id == r.view_id, "the reopen lands on the same server id"
        assert again.reseeded is False, "a reopen resumes from its persisted cursor"

    # `close_mirror` is the narrower release: the copy goes, the connection
    # stays, and the directory can be attached again — on this client or another.
    with gnitz.connect(server, schema=client.schema) as third:
        third.mirror_at(base)
        third.close_mirror()
        assert third.mirrored_ids() == [], "the registrations went with the store"
        with pytest.raises(gnitz.GnitzError, match="mirrors nothing"):
            third.poll()
        assert rows(third, "SELECT * FROM t"), "the connection is untouched"
        third.mirror_at(base)
        assert third.mirror_view("f").view_id == r.view_id


# ── errors arrive as the right Python class ─────────────────────────────────


def test_every_refusal_names_why(client, mirror):
    """What cannot be mirrored, and the message that says so."""
    _fed_view(client)
    client.execute_sql(f"CREATE VIEW plain AS {LINEAR}")
    client.execute_sql(f"CREATE VIEW bounded WITH (capacity = '1 MB') AS {LINEAR}")
    client.execute_sql(
        "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, kind BIGINT NOT NULL) WITH (stream = true)",
    )

    for name, needle in [
        ("t", "is a table"),
        ("ev", "is a stream"),
        ("plain", "keeps no delta feed"),
        ("bounded", "capacity-bounded"),
        ("nosuchview", "nosuchview"),
    ]:
        with pytest.raises(gnitz.GnitzError, match=needle):
            mirror.mirror_view(name)

    mirror.schema = "nosuchschema"
    with pytest.raises(gnitz.GnitzNotFoundError):
        mirror.mirror_view("f")


@pytest.mark.parametrize("case,env", [
    ("erase", {"GNITZ_INJECT_INGEST_APPLY_ERROR": "store"}),
    ("panic", {"GNITZ_INJECT_MIRROR_INGEST_PANIC": "1"}),
], ids=["storage-fault", "panicking-apply"])
def test_a_fault_degrades_one_copy_and_the_process_lives(client, server, mirror_dir, case, env):
    """The host process surviving a fault is the whole reason the crate is safe
    to link into someone's application.

    In a fresh interpreter because a fault seam is a per-process latch that would
    contaminate every sibling test, and because a Rust panic crossing pyo3 raises
    a `BaseException` the suite should not have to step around. **The seam
    variable goes to the mirror child and nowhere else**: a server that inherited
    it would fail its own ingest and drop the connection.

    Gated on the *extension's* build, not the server's: these seams live in
    crates the extension links and the server does not.
    """
    if not gnitz.debug_assertions:
        pytest.skip("fault seams require a debug extension")
    _fed_view(client)
    churn(client, 1, 40)
    client.execute_sql("SELECT COUNT(*) AS n FROM f")

    _childproc.run("_mirrorchild", case, mirror_dir, server, client.schema, extra_env=env)


# ── the poll report ─────────────────────────────────────────────────────────


def test_one_poll_reports_every_view(client, mirror):
    """Eight views under one poll, exactly one of which moves, and the shape of
    the report a host reads back.

    The moved view's rows arrive as a train the workers produced while every
    idle one's terminal is answered master-locally, so the two reply shapes
    interleave on one connection: a poll correlating replies by arrival rather
    than by request would hand a view another's tag. The tick is the master's
    global round counter, so a cursor advances over rounds that carried its view
    nothing — stated here rather than left for a host to infer "my copy changed"
    from a tick that moved.
    """
    base_tables(client)
    idle = {}
    for i in range(7):
        name = f"g{i}"
        mk_feed(client, name, f"SELECT id, w FROM u WHERE w > {i}")
        idle[name] = mirror.mirror_view(name).view_id
    mk_feed(client, "f", "SELECT id, v, body FROM t WHERE v > 5")
    moved = mirror.mirror_view("f").view_id

    churn(client, 1, 60)
    _quiesce(client, mirror)
    before = {vid: mirror.cursor(vid) for vid in [*idle.values(), moved]}
    assert len({tag for tag, _ in before.values()}) == len(before), "the tag is per-view"
    for r in mirror.poll():
        assert not r.reseeded, "an empty poll reseeds nothing"
        assert mirror.cursor(r.view_id) == before[r.view_id], "with nothing pushed, a poll moves no cursor"

    # Only `t` is written, so only `f` has a round to carry.
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'late-{i:0>20}')" for i in range(9001, 9040)),
    )
    report = {r.view_id: r for r in _quiesce(client, mirror)}
    assert set(report) == set(before), "one poll reports every registration, moved or not"
    for vid, (tag, tick) in before.items():
        r = report[vid]
        assert r.error is None and not r.reseeded, f"{vid}: an ordinary advance neither fails nor reseeds"
        assert r.cursor == mirror.cursor(vid), "the report carries the round each copy now answers at"
        assert r.cursor[0] == tag, f"{vid}: a report carrying another view's tag is a mis-correlation"
        assert r.cursor[1] >= tick, f"{vid}: the round never goes backwards"
    assert len({r.cursor[1] for r in report.values()}) == 1, "the round is the master's global counter"

    for name, vid in [("f", moved), ("g0", idle["g0"])]:
        q = f"SELECT * FROM {name}"
        _samebag(q, _local(mirror, vid, q), rows(client, q))

    assert mirror.cursor(max(before) + 10_000) is None, "a view with no valid copy has no cursor"


# ── a mirror is not read-your-own-writes ────────────────────────────────────


def test_a_mirror_is_not_read_your_own_writes(client, mirror):
    """The contract a user is most likely to be surprised by: a write through the
    mirror's own connection is not in the copy until a poll carries its round."""
    _fed_view(client)
    churn(client, 1, 40)
    vid = mirror.mirror_view("f").view_id
    _quiesce(client, mirror)

    q = "SELECT id, v FROM f WHERE id = 7"
    before = bag(_local(mirror, vid, q))
    mirror.execute_sql("UPDATE t SET v = 999999 WHERE id = 7")
    assert bag(_local(mirror, vid, q)) == before, (
        "the copy answers at its last poll, not at what the write just made true"
    )

    _quiesce(client, mirror)
    local = _local(mirror, vid, q)
    assert bag(local) != before, "a drain must carry the write into the copy"
    _samebag(q, local, rows(client, q))


# ── the server goes away ────────────────────────────────────────────────────


def test_the_copy_outlives_its_server_and_reseeds_on_reconnect(own_server, mirror_on, mirror_dir):
    """The feature's actual promise: with the server stopped the copy still
    answers at the round it reached, and the poll says per view that it failed
    rather than raising. A restart stops the tags continuing, so after
    `reconnect` the next poll reports the reseed and the copy converges again."""
    own_server.start()
    q = "SELECT * FROM f"
    with gnitz.connect(own_server.target) as client:
        _fed_view(client)
        churn(client, 1, 60)
        m = mirror_on(mirror_dir, own_server.target)
        vid = m.mirror_view("f").view_id
        _quiesce(client, m)
        expected = bag(rows(client, q))
    assert expected

    own_server.stop()
    assert bag(_local(m, vid, q)) == expected
    assert all(isinstance(r.error, gnitz.GnitzConnectionError) for r in m.poll()), (
        "a dead connection fails every view's poll, and the exception says it was the connection"
    )
    assert bag(_local(m, vid, q)) == expected, "and the copy still answers at the round it reached"
    with pytest.raises(gnitz.GnitzConnectionError):
        m.execute_sql("SELECT * FROM t")

    own_server.restart()
    m.reconnect(own_server.target)
    with gnitz.connect(own_server.target) as client:
        churn(client, 61, 120)
        assert any(r.view_id == vid and r.reseeded for r in _quiesce(client, m)), (
            "a tag that stopped continuing is a reseed"
        )
        _samebag(q, _local(m, vid, q), rows(client, q))


def test_a_killed_host_reopens_at_its_last_checkpoint(client, server, mirror_on, mirror_dir):
    """`SIGKILL` means no destructor and so no exit checkpoint.

    The reopen must land on the last checkpoint and converge from there without
    doubling a weight — which is what a cursor file written out of step with the
    copies produces, and it leaves the row set identical.

    The signal goes to the mirror child, not the server, so the session server
    serves: nothing here kills, stops or restarts one.
    """
    _fed_view(client)
    churn(client, 1, 60)

    child = _childproc.spawn("_mirrorchild", "crash", mirror_dir, server, client.schema)
    try:
        _childproc.wait_for_ready(child)
        os.kill(child.pid, signal.SIGKILL)
    finally:
        child.wait()
        child.stdout.close()
        child.stderr.close()

    m = mirror_on(mirror_dir, server, client.schema)
    vid = m.mirror_view("f").view_id
    _quiesce(client, m)
    for q in ("SELECT * FROM f", "SELECT COUNT(*) AS n, SUM(v) AS total FROM f"):
        _samebag(q, _local(m, vid, q), rows(client, q))


# ── the GIL across a mirror call ────────────────────────────────────────────


class _Contender:
    """A thread that does nothing but count, so increments over a window say
    whether the GIL was available to another thread across it.

    Counts, never elapsed time: a clock would measure the call instead.
    """

    def __init__(self):
        self._stop = threading.Event()
        self._started = threading.Event()
        self.count = 0
        self._thread = threading.Thread(target=self._spin, daemon=True)

    def _spin(self):
        self._started.set()
        while not self._stop.is_set():
            self.count += 1

    def __enter__(self):
        self._thread.start()
        self._started.wait()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()

    def during(self, call, *args):
        """`call(*args)`'s result, and the increments this thread managed while
        it ran. No Python frame sits between the two reads of the count and the
        call, so an extension call that keeps the GIL leaves a structural zero."""
        before = self.count
        out = call(*args)
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
    """
    _fed_view(client)
    churn(client, 1, 200)
    vid = mirror.mirror_view("f").view_id
    churn(client, 201, 400)
    client.execute_sql("SELECT COUNT(*) AS n FROM f")

    with _Contender() as spin:
        polled, poll_dropped_gil = spin.wins(mirror.poll)
        before = mirror.requests_sent
        held, read_dropped_gil = spin.wins(lambda: rows(mirror, "SELECT * FROM f"))
        assert mirror.requests_sent == before, "the mirrored read must be answered off the copy"
        _, during_control = spin.during(mirror.cursor, vid)

    assert polled, "the poll must have covered the registered view"
    assert held, "the mirrored read must return rows, or it proves nothing"

    assert poll_dropped_gil, "a poll must drop the GIL across its delta read"
    assert read_dropped_gil, "a mirrored read must drop the GIL across the engine scan"
    assert during_control == 0, (
        f"a metadata getter answers out of memory, so it must never drop the GIL; "
        f"got {during_control} increments"
    )
