"""A mirroring `gnitz.GnitzClient`: a local copy of a view, read in this
interpreter.

What this suite adds over the mirror crate's own — which owns the storage
shapes, the view bodies, aliases, indexes, the worker counts and the poll's
recovery paths — is everything only the binding has: the pyclass and its
lifetime inside one long-lived interpreter, the error classes, the poll report
and dict-and-`Row` result shapes, and the GIL.

**Every correctness assertion is weight-exact, and every one of them asserts
where the answer came from** (`_feedviews.agree`).
"""
import threading

import pytest
import gnitz
from _feedviews import agree, churn, fed_view, mk_feed
from _read import bag, ordered, rows
import _childproc


# ── the copy answers, and it is the copy that answers ───────────────────────


def test_a_mirrored_read_equals_the_server_read(client, mirror):
    """The differential through the binding's own result objects: the dicts, the
    `Row`s, and the client-side finishing over a single-worker reply. An ordered
    read is compared as a sequence — a multiset comparison accepts the right rows
    in the wrong order."""
    fed_view(client)
    mirror.mirror_view("f")

    churn(client, 1, 200)
    mirror.sync()

    for q in [
        "SELECT * FROM f",
        "SELECT id, v FROM f WHERE v > 300",
        "SELECT v, COUNT(*) AS n, SUM(v) AS total FROM f GROUP BY v",
        "SELECT id, v, body FROM f WHERE id = 42",
    ]:
        agree(mirror, client, q)

    q = "SELECT id, v FROM f ORDER BY v DESC, id ASC LIMIT 17"
    before = mirror.requests_sent
    local = ordered(rows(mirror, q))
    assert mirror.requests_sent == before, "the ordered read was delegated upstream"
    assert local, "the ordered reply is empty, so it agrees about nothing"
    assert local == ordered(rows(client, q))


def test_a_mirrored_view_keeps_its_select_order(client, mirror):
    """A view whose key is not its first column is mirrored in its declared
    order: the copy answers `SELECT *` with the server's names, order and
    Z-set."""
    fed_view(client, "SELECT body, id, v FROM t WHERE v > 10")
    mirror.mirror_view("f")
    churn(client, 1, 120)
    mirror.sync()

    for q in ("SELECT * FROM f", "SELECT id, body FROM f WHERE id < 50"):
        agree(mirror, client, q)
    assert rows(mirror, "SELECT * FROM f")[0]._fields == ("body", "id", "v")


def test_a_subscription_is_mirrored_under_an_alias(client, mirror):
    """`mirror_subscription` reports as `mirror_view` does, and `_local.<alias>`
    is read off its copy as what the SELECT keeps of the view."""
    fed_view(client)
    churn(client, 1, 60)
    sql = "SELECT id, v FROM f WHERE v % 2 = 0"
    r = mirror.mirror_subscription("even", sql)
    assert r.reseeded and r.error is None and mirror.mirrored_ids() == [r.view_id]

    before = mirror.requests_sent
    got = bag(rows(mirror, "SELECT * FROM _local.even"))
    assert mirror.requests_sent == before, "the alias read went upstream"
    assert got and got == bag(rows(client, sql))


def test_what_the_copy_does_not_hold_is_read_upstream(client, mirror):
    """Locality's falsifiable other half: a relation the handle does not hold,
    and a view it has forgotten, issue requests and stay correct."""
    fed_view(client)
    vid = mirror.mirror_view("f").view_id
    churn(client, 1, 60)
    mirror.sync()

    agree(mirror, client, "SELECT * FROM t", local=False)
    mirror.forget_view(vid)
    assert mirror.cursor(vid) is None
    agree(mirror, client, "SELECT * FROM f", local=False)


def test_a_select_run_again_is_answered_where_its_view_is_held_now(client, mirror):
    """The same SELECT text run again and again on the mirroring client is
    answered off the copy while the view is mirrored, and upstream from the
    first run after it is forgotten, each run weight-exact. Its runs alternated
    with a client that mirrors nothing are answered the same way."""
    queries = ("SELECT * FROM f", "SELECT id, v FROM f WHERE v > 30 LIMIT 1000")
    fed_view(client)
    vid = mirror.mirror_view("f").view_id
    churn(client, 1, 60)
    mirror.sync()
    for q in queries:
        for _ in range(4):
            before = mirror.requests_sent
            got = bag(rows(mirror, q))
            assert mirror.requests_sent == before, f"{q}: a run went upstream"
        assert got and got == bag(rows(client, q))
        for _ in range(3):
            agree(mirror, client, q)
        for _ in range(3):
            rows(mirror, q)

    mirror.forget_view(vid)
    for q in queries:
        for _ in range(3):
            before = mirror.requests_sent
            got = bag(rows(mirror, q))
            assert mirror.requests_sent > before, f"{q}: answered off the forgotten copy"
        assert got and got == bag(rows(client, q))
        for _ in range(3):
            agree(mirror, client, q, local=False)


def test_a_point_read_is_answered_off_the_copy(client, mirror):
    """`seek` and `seek_by_index` over a mirrored view send nothing, carry no
    LSN, and agree with the same read through a client that mirrors nothing."""
    fed_view(client)
    vid = mirror.mirror_view("f").view_id
    churn(client, 1, 60)
    mirror.sync()
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
        assert bag(local) and bag(local) == bag(remote), what


# ── the handle's life in an interpreter ─────────────────────────────────────


def test_the_handles_life_in_one_interpreter(client, server, tmp_path):
    """Open, close, reopen.

    Not the `mirror` fixture: this test owns the clients' lifetimes, which is
    what it is about. Closing a mirroring client releases its copy directory,
    which is what lets the reopen below take it; and a closed client refuses
    every verb rather than reconnecting behind the caller's back.
    """
    fed_view(client)
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
    for call in (first.sync, first.mirrored_ids, first.checkpoint, lambda: first.scan(*client.resolve_table("t"))):
        with pytest.raises(gnitz.GnitzConnectionError, match="closed"):
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
        assert third.sync().mirrored == [], "a client that mirrors nothing has no copy to advance"
        assert rows(third, "SELECT * FROM t"), "the connection is untouched"
        third.mirror_at(base)
        assert third.mirror_view("f").view_id == r.view_id


# ── errors arrive as the right Python class ─────────────────────────────────


def test_a_refusal_arrives_as_its_error_class(client, mirror):
    """What cannot be mirrored is refused, and a name that resolves to nothing
    is not found. Which refusal says what is the mirror crate's to test. A sync
    told to wait a negative time is the binding's own `ValueError`."""
    fed_view(client)
    with pytest.raises(ValueError, match="non-negative"):
        mirror.sync(wait=-1)
    with pytest.raises(gnitz.GnitzRefusedError, match="is a table"):
        mirror.mirror_view("t")
    with pytest.raises(gnitz.GnitzNotFoundError, match="nosuchview"):
        mirror.mirror_view("nosuchview")
    mirror.schema = "nosuchschema"
    with pytest.raises(gnitz.GnitzNotFoundError):
        mirror.mirror_view("f")


def test_a_mirror_reads_a_view_of_another_schema_by_its_qualified_name(client, mirror):
    """The copy is keyed by the view's own schema, so a mirror whose schema is
    another registers it and reads it locally under its qualified name — and
    the unqualified name, which now means a different relation, is not it."""
    fed_view(client)
    mirror.schema = "public"
    qualified = f"{client.schema}.f"
    vid = mirror.mirror_view(qualified).view_id
    assert vid == client.resolve_table("f")[0]

    churn(client, 1, 50)
    mirror.sync()
    before = mirror.requests_sent
    local = bag(rows(mirror, f"SELECT * FROM {qualified}"))
    assert mirror.requests_sent == before, "the qualified read was delegated upstream"
    assert local and local == bag(rows(client, "SELECT * FROM f"))
    with pytest.raises(gnitz.GnitzNotFoundError, match="relation 'f' not found"):
        rows(mirror, "SELECT * FROM f")


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

    Gated on the *extension's* build, not the server's: the seams fire in the
    mirror's process, which runs the extension's code.
    """
    if not gnitz.fault_seams:
        pytest.skip("fault seams require a debug extension")
    fed_view(client)
    churn(client, 1, 40)

    _childproc.run("_mirrorchild", case, mirror_dir, server, client.schema, extra_env=env)


# ── the poll report ─────────────────────────────────────────────────────────


def test_a_poll_reports_every_view(client, mirror):
    """The report a host reads back: one `PollResult` per registration, moved or
    not, carrying the round its copy now answers at."""
    fed_view(client)
    mk_feed(client, "g", "SELECT id, w FROM u WHERE w > 0")
    moved, idle = mirror.mirror_view("f").view_id, mirror.mirror_view("g").view_id
    before = {vid: mirror.cursor(vid) for vid in (moved, idle)}

    client.execute_sql("INSERT INTO t VALUES (1, 100, 'one')")
    report = {r.view_id: r for r in mirror.sync().mirrored}
    assert set(report) == {moved, idle}, "one poll reports every registration, moved or not"
    for vid, r in report.items():
        assert r.error is None and not r.reseeded, f"{vid}: an ordinary advance neither fails nor reseeds"
        assert r.cursor == mirror.cursor(vid), "the report carries the round each copy now answers at"
    assert report[moved].cursor[1] > before[moved][1], "the view the round reached moved"
    assert report[idle].cursor == before[idle], "a round of another view moves no cursor"
    agree(mirror, client, "SELECT * FROM f")
    assert mirror.cursor(max(before) + 10_000) is None, "a view with no valid copy has no cursor"


# ── the server goes away ────────────────────────────────────────────────────


def test_the_copy_outlives_its_server_and_reseeds_on_reconnect(own_server, mirror_on, mirror_dir):
    """The feature's actual promise: with the server stopped the copy still
    answers at the round it reached, and the poll says per view that it failed
    rather than raising. A restart stops the tags continuing, so after
    `reconnect` the next poll reports the reseed and the copy converges again."""
    own_server.start()
    q = "SELECT * FROM f"
    with gnitz.connect(own_server.target) as client:
        fed_view(client)
        churn(client, 1, 60)
        m = mirror_on(mirror_dir, own_server.target)
        vid = m.mirror_view("f").view_id
        expected = bag(rows(client, q))
    assert expected

    own_server.stop()
    assert bag(rows(m, q)) == expected
    assert all(isinstance(r.error, gnitz.GnitzConnectionError) for r in m.sync().mirrored), (
        "a dead connection fails every view's poll, and the exception says it was the connection"
    )
    assert bag(rows(m, q)) == expected, "and the copy still answers at the round it reached"
    with pytest.raises(gnitz.GnitzConnectionError):
        m.execute_sql("SELECT * FROM t")

    own_server.restart()
    m.reconnect(own_server.target)
    with gnitz.connect(own_server.target) as client:
        churn(client, 61, 120)
        assert any(r.view_id == vid and r.reseeded for r in m.sync().mirrored), (
            "a tag that stopped continuing is a reseed"
        )
        agree(m, client, q)


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
    fed_view(client)
    churn(client, 1, 200)
    vid = mirror.mirror_view("f").view_id
    churn(client, 201, 400)

    with _Contender() as spin:
        synced, poll_dropped_gil = spin.wins(mirror.sync)
        before = mirror.requests_sent
        held, read_dropped_gil = spin.wins(lambda: rows(mirror, "SELECT * FROM f"))
        assert mirror.requests_sent == before, "the mirrored read must be answered off the copy"
        _, during_control = spin.during(mirror.cursor, vid)

    assert synced.mirrored, "the poll must have covered the registered view"
    assert held, "the mirrored read must return rows, or it proves nothing"

    assert poll_dropped_gil, "a poll must drop the GIL across its delta read"
    assert read_dropped_gil, "a mirrored read must drop the GIL across the engine scan"
    assert during_control == 0, (
        f"a metadata getter answers out of memory, so it must never drop the GIL; "
        f"got {during_control} increments"
    )
