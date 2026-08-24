"""`gnitz.Mirror`: a local copy of a view, read in this interpreter.

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

import pytest
import gnitz
from _feedviews import (
    FEED, GROUPBY, JOIN, LINEAR, SETOP,
    _base_tables, _churn, _flood, _key, _mk_feed, _rows, _uid, _zset,
)
import _mirrorproc
from _serverproc import is_debug_build


# ── helpers ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _mirror_latch_free():
    """No test may leave a handle behind.

    A leaked one otherwise fails every *later* mirror test and hides which one
    leaked it; this fails the culprit. `any_live` reads the latch itself, so it
    costs nothing and names what it found rather than inferring it from a
    refused open.
    """
    yield
    assert not gnitz.Mirror.any_live(), "the test left a mirror handle open"


def _local(mirror, sn, vid, sql):
    """One read off the copy, with the proof that it was one."""
    assert mirror.mirrors(vid), f"{sql}: the view is not answered locally"
    before = mirror.requests_sent
    rows = _rows(mirror.execute_sql(sql, schema_name=sn))
    assert mirror.requests_sent == before, f"{sql}: the read was delegated upstream"
    return rows


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


def _t_rows_outside_u(client, sn, lo=9001, hi=9020):
    """Rows in `t` whose ids `u` never carries.

    `_churn` deletes a longer tail from `t` than from `u`, so `t.id` ends up a
    subset of `u.tid` and the `EXCEPT` body is empty — which the weight-exact
    comparison refuses as a pass that agrees about nothing. These rows are what
    give it something to compare; the other three bodies are unaffected by them
    except through their own predicates.
    """
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'solo-{i:0>20}')" for i in range(lo, hi + 1)),
        schema_name=sn,
    )


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
    _t_rows_outside_u(client, sn)
    _quiesce(client, mirror, sn)

    for q in _READS[kind]["plain"]:
        _same_zset(q, _local(mirror, sn, vid, q), _rows(client.execute_sql(q, schema_name=sn)))
    q = _READS[kind]["ordered"]
    _same_sequence(q, _local(mirror, sn, vid, q), _rows(client.execute_sql(q, schema_name=sn)))


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
    delegated = _rows(mirror.execute_sql("SELECT * FROM t", schema_name=sn))
    cost = mirror.requests_sent - before
    _same_zset("a delegated read", delegated, _rows(client.execute_sql("SELECT * FROM t", schema_name=sn)))
    assert cost == 2, f"a delegated scan must cost one resolve and one read, not {cost}"

    mirror.forget_view(vid)
    assert not mirror.mirrors(vid)
    before = mirror.requests_sent
    after_forget = _rows(mirror.execute_sql("SELECT * FROM f", schema_name=sn))
    assert mirror.requests_sent > before, "a forgotten view must be read upstream again"
    _same_zset("a forgotten view", after_forget, _rows(client.execute_sql("SELECT * FROM f", schema_name=sn)))


# ── P3 · the handle's life in an interpreter ─────────────────────────────────


def test_the_handles_life_in_one_interpreter(client, server, tmp_path):
    """Open, close, reopen — and the one-handle-per-process latch in between.

    Not the `mirror` fixture: this test owns the handles' lifetimes, which is
    what it is about.
    """
    sn = "s" + _uid()
    _fed_view(client, sn, LINEAR)
    _churn(client, sn, 1, 40)
    base = str(tmp_path / "m")

    # Under `with` even for the explicit-close half, so a failing assertion
    # still frees the latch and fails this test rather than every later one.
    with gnitz.Mirror(base, server) as first:
        r = first.mirror_view(sn, "f")
        assert r.reseeded is True, "a first registration bootstraps"

        with pytest.raises(gnitz.GnitzError):
            gnitz.Mirror(str(tmp_path / "other"), server)

        first.close()
        first.close()  # idempotent — a host must be able to release a handle twice
    for call in (lambda: first.poll(), lambda: first.mirrored_ids(), lambda: first.checkpoint()):
        with pytest.raises(gnitz.GnitzError):
            call()

    with gnitz.Mirror(base, server) as second:
        again = second.mirror_view(sn, "f")
        assert again.view_id == r.view_id, "the reopen lands on the same server id"
        assert again.reseeded is False, "a reopen resumes from its persisted cursor"
    with pytest.raises(gnitz.GnitzError):
        second.poll()


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


def test_a_storage_fault_poisons_the_handle_and_the_process_lives(client, server, mirror_dir):
    """The host process surviving a storage fault is the whole reason the crate
    is safe to link into someone's application.

    In a fresh interpreter because a fault seam is a per-process latch that would
    contaminate every sibling test. **The seam variable goes to the mirror child
    and nowhere else**: a server that inherited it would fail its own ingest and
    drop the connection, and the test would then report a protocol error instead
    of a poisoning.
    """
    if not is_debug_build():
        pytest.skip("the ingest fault seam requires a debug build")
    sn = "s" + _uid()
    _fed_view(client, sn, LINEAR)
    _churn(client, sn, 1, 40)
    client.execute_sql("SELECT COUNT(*) AS n FROM f", schema_name=sn)

    _mirrorproc.run(
        "poison", mirror_dir, server, sn, env={"GNITZ_INJECT_INGEST_APPLY_ERROR": "store"}
    )


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
    client.execute_sql("SELECT COUNT(*) AS n FROM f0", schema_name=sn)
    first = mirror.poll()
    assert {r.view_id for r in first} == set(ids.values()), "one poll advances every registration"
    assert not any(r.reseeded for r in first), "an ordinary advance is not a reseed"
    advanced = {vid: mirror.cursor(vid) for vid in ids.values()}

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
        client.execute_sql("SELECT COUNT(*) AS n FROM f", schema_name=sn)
        assert any(r.view_id == vid and r.reseeded for r in m.poll()), (
            "a tag that stopped continuing is a reseed"
        )
        _quiesce(client, m, sn)
        q = "SELECT * FROM f"
        _same_zset(q, _local(m, sn, vid, q), _rows(client.execute_sql(q, schema_name=sn)))


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
    _same_zset(q, _local(mirror, sn, vid, q), _rows(client.execute_sql(q, schema_name=sn)))


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
        _flood(client, sn, 1, 2_000)
        m = mirror_on(mirror_dir, sweeping_server.sock_path)
        vid = m.mirror_view(sn, "f").view_id
        _quiesce(client, m, sn)

        # Push the worker's retention floor past the mirror's cursor.
        _flood(client, sn, 2_001, 40_000)
        client.execute_sql("SELECT COUNT(*) AS n FROM f", schema_name=sn)
        assert any(r.view_id == vid and r.reseeded for r in m.poll()), (
            "an expired cursor is recovered by reseeding"
        )

        _quiesce(client, m, sn)
        q = "SELECT id, v FROM f WHERE id > 39000"
        _same_zset(q, _local(m, sn, vid, q), _rows(client.execute_sql(q, schema_name=sn)))


# ── P8 · a host crash ────────────────────────────────────────────────────────


def test_a_killed_host_reopens_at_its_last_checkpoint(own_server, mirror_on, mirror_dir):
    """`SIGKILL` means no destructor and so no exit checkpoint.

    The reopen must land on the last checkpoint and converge from there without
    doubling a weight — which is what a cursor file written out of step with the
    copies produces, and it leaves the row set identical.
    """
    own_server.start()
    sn = "s" + _uid()
    with gnitz.connect(own_server.sock_path) as client:
        _fed_view(client, sn, LINEAR)
        _churn(client, sn, 1, 60)

    child = _mirrorproc.spawn("crash", mirror_dir, own_server.sock_path, sn)
    try:
        _mirrorproc.wait_for_ready(child)
        os.kill(child.pid, signal.SIGKILL)
    finally:
        child.wait()
        child.stdout.close()
        child.stderr.close()

    with gnitz.connect(own_server.sock_path) as client:
        m = mirror_on(mirror_dir, own_server.sock_path)
        vid = m.mirror_view(sn, "f").view_id
        _quiesce(client, m, sn)
        for q in ("SELECT * FROM f", "SELECT COUNT(*) AS n, SUM(v) AS total FROM f"):
            _same_zset(q, _local(m, sn, vid, q), _rows(client.execute_sql(q, schema_name=sn)))


# ── P9 · the mirror does not disturb the process ─────────────────────────────


def test_the_mirrors_own_connection_serves_every_other_statement(client, mirror):
    """DDL, DML and a transaction through `mirror.execute_sql`, visible to a
    plain client — the handle owns a connection, and everything the copy cannot
    serve runs on it."""
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

    seen = _zset(_rows(client.execute_sql("SELECT * FROM k", schema_name=sn)))
    assert len(seen) == 3
    _same_zset(
        "written through the mirror's connection",
        _rows(mirror.execute_sql("SELECT * FROM k", schema_name=sn)),
        _rows(client.execute_sql("SELECT * FROM k", schema_name=sn)),
    )


def test_a_multi_relation_select_fails_identically_through_either_object(client, mirror):
    """No client plans an ad-hoc join, mirrored inputs included. The mirror must
    not turn that into a different error."""
    sn = "s" + _uid()
    _fed_view(client, sn, LINEAR)
    _churn(client, sn, 1, 20)
    mirror.mirror_view(sn, "f")
    _quiesce(client, mirror, sn)

    q = "SELECT t.id, u.w FROM t JOIN u ON t.id = u.tid"
    with pytest.raises(gnitz.GnitzError) as through_mirror:
        mirror.execute_sql(q, schema_name=sn)
    with pytest.raises(gnitz.GnitzError) as through_client:
        client.execute_sql(q, schema_name=sn)
    assert "derives a new one" in str(through_mirror.value)
    assert str(through_mirror.value) == str(through_client.value)


@pytest.mark.asyncio
async def test_an_open_mirror_leaves_the_rest_of_the_process_alone(client, server, mirror_dir):
    """A `gnitz.aio` transport and a plain client both work in an interpreter
    that has a mirror open, and after it is closed."""
    from gnitz import aio

    sn = "s" + _uid()
    _fed_view(client, sn, LINEAR)
    _churn(client, sn, 1, 20)

    with gnitz.Mirror(mirror_dir, server) as m:
        vid = m.mirror_view(sn, "f").view_id
        _quiesce(client, m, sn)
        expected = _zset(_rows(client.execute_sql("SELECT * FROM f", schema_name=sn)))
        assert expected, "the view must hold rows, or the reads below prove nothing"
        async with aio.connect(server) as aconn:
            assert _zset(await aconn.scan(vid)) == expected
        assert _zset(_rows(client.execute_sql("SELECT * FROM f", schema_name=sn))) == expected

    async with aio.connect(server) as aconn:
        assert _zset(await aconn.scan(vid)) == expected
    assert _zset(_rows(client.execute_sql("SELECT * FROM f", schema_name=sn))) == expected


# ── P10 · a mirrored read with the server stopped ────────────────────────────


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
        expected = _zset(_rows(client.execute_sql("SELECT * FROM f", schema_name=sn)))

    own_server.stop()

    assert _zset(_local(m, sn, vid, "SELECT * FROM f")) == expected
    assert _zset(_local(m, sn, vid, "SELECT id, v FROM f WHERE id = 7")) != {}
    with pytest.raises(gnitz.GnitzError):
        m.poll()
    with pytest.raises(gnitz.GnitzError):
        m.execute_sql("SELECT * FROM t", schema_name=sn)
