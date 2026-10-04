"""`gnitz.aio` — the async transport's own contracts.

What is async here is the transport: submit-on-call, pipelining, reply
correlation across mixed verb kinds, error propagation out of a gather, and the
loop's own costs. The decode is shared with the sync client (`scan_result`),
so a value- or row-level claim belongs to the coordinate that owns it —
`mutation_pattern`, `value_domain`, `read_verb` — not here.
"""

import asyncio
import os
import shutil
import subprocess
import time

import pytest
import pytest_asyncio
import gnitz
from gnitz import aio
import _childproc
from _read import bag
from _schemas import KV


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def aconn(server):
    async with aio.connect(server) as conn:
        yield conn


@pytest.fixture
def table(client):
    """A disposable `(pk, val)` table; `client` drops it with its schema."""
    return client.create_table("t", KV)


def _batch(rows):
    return gnitz.ZSetBatch(KV).extend(rows)


# The schema of the system table the lifecycle tests read.
_SCHEMAS = gnitz.sys_schema(gnitz.SCHEMA_TAB)


# ---------------------------------------------------------------------------
# Pipelining
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pipeline_mixes_operation_kinds(client, aconn):
    """Push, empty push, scan, seek and scan_many gathered as one in-flight
    group. The server handles one request per connection at a time and replies
    in request order, so each future must resolve to *its own* operation's
    result — a swap between two kinds would hand a scan's rows to a push's
    future, or one relation's rows to another's. The empty push is a round trip
    answered with LSN 0, and must not shift the replies behind it.
    Run at W>1: replies leave the workers out of order and only the master's
    serialisation puts them back."""
    a = client.create_table("ta", KV)
    b = client.create_table("tb", KV)
    # Distinct per-relation payloads, so a mis-correlated reply is visible.
    await aconn.push(a, _batch([{"pk": i, "val": 100 + i} for i in range(1, 6)]))
    await aconn.push(b, _batch([{"pk": i, "val": 900 + i} for i in range(1, 4)]))

    push_lsn, empty_lsn, scan_a, scan_b, seek_a, seek_b, many = await asyncio.gather(
        aconn.push(a, _batch([{"pk": 42, "val": 4242}])),
        aconn.push(b, gnitz.ZSetBatch(KV)),
        aconn.scan(a, KV),
        aconn.scan(b, KV),
        aconn.seek(a, KV, 3),
        aconn.seek(b, KV, 2),
        aconn.scan_many([(b, KV), (a, KV)]),
    )

    rows_a = {(i, 100 + i): 1 for i in range(1, 6)} | {(42, 4242): 1}
    rows_b = {(i, 900 + i): 1 for i in range(1, 4)}
    assert push_lsn > 0 and empty_lsn == 0
    # The push was submitted first, so every read behind it in the batch sees its
    # row: request order is honoured, not just reply order.
    assert bag(scan_a) == rows_a
    assert bag(scan_b) == rows_b
    assert bag(seek_a) == {(3, 103): 1}
    assert bag(seek_b) == {(2, 902): 1}
    # scan_many keeps request order, which is the reverse of the two scans above.
    assert [bag(res) for res in many] == [rows_b, rows_a]


@pytest.mark.asyncio
async def test_a_gathered_burst_resolves_every_future(aconn, table):
    """Every verb submits when called and hands back a loop future, so the whole
    burst is queued before the first await.

    LSNs are non-decreasing but not distinct: pushes the committer batches
    together share one zone LSN, and which run batches depends on scheduling, so
    only monotonicity is asserted. Every row lands exactly once, and concurrent
    scans of the settled table all agree.
    """
    n = 500
    futures = [aconn.push(table, _batch([{"pk": i, "val": i}])) for i in range(n)]
    assert all(isinstance(f, asyncio.Future) for f in futures)
    lsns = await asyncio.gather(*futures)
    assert min(lsns) > 0 and lsns == sorted(lsns)

    expected = {(i, i): 1 for i in range(n)}
    for result in await asyncio.gather(*[aconn.scan(table, KV) for _ in range(10)]):
        assert bag(result) == expected


@pytest.mark.asyncio
async def test_an_abandoned_operation_is_not_a_cancellation(aconn, table):
    """`aio`'s stated limitation: abandoning a future does not stop the frame or
    the commit, only the waiting. The reply still arrives for a slot nobody is
    waiting on, and `settle` must drop it rather than raise `InvalidStateError`
    into the loop or leave the slot map misaligned for the next reply."""
    fut = aconn.push(table, _batch([{"pk": 1, "val": 10}]))
    fut.cancel()
    await asyncio.sleep(0)

    await aconn.push(table, _batch([{"pk": 2, "val": 20}]))
    assert bag(await aconn.scan(table, KV)) == {(1, 10): 1, (2, 20): 1}


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_an_error_surfaces_from_a_gather_and_the_connection_survives(aconn, table):
    """A refused request leaves the connection usable, whether it is a lone
    request the server refuses whole (a `scan_many` past the item cap, whose one
    error frame answers a request with every position unfilled) or one refused
    inside a gathered group — where the good pushes must not swallow the bad one."""
    with pytest.raises(gnitz.GnitzRefusedError, match="too many items"):
        await aconn.scan_many([(table, KV)] * 1000)

    bad = gnitz.ZSetBatch(gnitz.Schema([KV.columns[0]], [0])).extend([{"pk": 1}])
    with pytest.raises(gnitz.GnitzNotFoundError):
        await asyncio.gather(
            aconn.push(table, _batch([{"pk": 1, "val": 1}])),
            aconn.push(0xDEAD_BEEF, bad),                      # no such relation
            aconn.push(table, _batch([{"pk": 3, "val": 3}])),
        )

    await aconn.push(table, _batch([{"pk": 7, "val": 70}]))
    assert bag(await aconn.scan(table, KV)).get((7, 70)) == 1
    assert bag((await aconn.scan_many([(table, KV)]))[0]).get((7, 70)) == 1


@pytest.mark.asyncio
async def test_connection_loss_resolves_every_queued_request(own_server):
    """Losing the connection must fail every submitted request.

    All 3000 are submitted before the loop gets a turn. The first flushes at
    submit and meets the dead peer, which ends the session; the rest are
    refused at submit. Every one must still be a future that resolves: a future
    nobody resolves is a hang, not an error, and the `wait_for` is what turns
    that into a failure.
    """
    conn = await aio.connect(own_server.start().target)
    own_server.stop()

    futs = [conn.scan(gnitz.SCHEMA_TAB, _SCHEMAS) for _ in range(3000)]
    results = await asyncio.wait_for(
        asyncio.gather(*futs, return_exceptions=True), timeout=30)
    resolved_ok = [r for r in results if not isinstance(r, BaseException)]
    assert not resolved_ok, f"{len(resolved_ok)}/{len(results)} succeeded against a dead server"
    # Including the ones refused at submit, once the loss was known.
    lost = [r for r in results if not isinstance(r, gnitz.GnitzConnectionError)]
    assert not lost, f"{len(lost)}/{len(results)} failed otherwise, e.g. {lost[0]!r}"

    # A request submitted after the failure was observed must resolve too.
    with pytest.raises(gnitz.GnitzConnectionError):
        await asyncio.wait_for(conn.scan(gnitz.SCHEMA_TAB, _SCHEMAS), timeout=30)

    await conn.aclose()


# ---------------------------------------------------------------------------
# Connection lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect_shapes_close_and_refuse(server):
    """`connect()` returns the connection itself, so `await` and `async with`
    both yield it. Two are independent connections. Close is idempotent, and
    every later verb raises rather than hanging on a future nobody will
    resolve."""
    conn = await aio.connect(server)
    async with aio.connect(server) as other:
        mine = await conn.scan(gnitz.SCHEMA_TAB, _SCHEMAS)
        assert len(mine) == len(await other.scan(gnitz.SCHEMA_TAB, _SCHEMAS)) > 0

    await conn.aclose()
    await conn.aclose()          # idempotent
    fut = conn.scan(gnitz.SCHEMA_TAB, _SCHEMAS)   # refused through its future
    with pytest.raises(gnitz.GnitzConnectionError, match="connection closed"):
        await fut


def _open_sockets():
    n = 0
    for fd in os.listdir("/proc/self/fd"):
        try:
            n += os.readlink(f"/proc/self/fd/{fd}").startswith("socket:")
        except OSError:
            pass            # the listing's own fd, closed by now
    return n


@pytest.mark.asyncio
async def test_a_dropped_connection_frees_its_socket(server):
    """A connection dropped without `aclose` frees its socket on a live loop:
    at once when idle, and once its last operation resolves otherwise."""
    before = _open_sockets()      # the loop's self-pipe already exists

    conn = await aio.connect(server)
    assert len(await conn.scan(gnitz.SCHEMA_TAB, _SCHEMAS)) > 0
    del conn
    await asyncio.sleep(0)
    assert _open_sockets() == before

    fut = (await aio.connect(server)).scan(gnitz.SCHEMA_TAB, _SCHEMAS)
    assert len(await fut) > 0
    await asyncio.sleep(0)
    assert _open_sockets() == before


# ---------------------------------------------------------------------------
# A rename between encode and ingest
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pushes_encoded_before_a_rename_land(aconn, client):
    """A column rename changes names only, so a push encoded under the old names
    still lays out the table's columns: every such push lands, and the rows read
    back under the new name."""
    tid = client.create_table("t", KV)
    await aconn.push(tid, _batch([{"pk": 1, "val": 1}]))

    client.execute_sql("ALTER TABLE t RENAME COLUMN val TO amount")

    await asyncio.gather(*[aconn.push(tid, _batch([{"pk": 10 + i, "val": 100 + i}])) for i in range(3)])
    renamed = client.resolve_table("t")[1]
    assert bag(await aconn.scan(tid, renamed), "pk", "amount") == \
        {(1, 1): 1, (10, 100): 1, (11, 101): 1, (12, 102): 1}


# ---------------------------------------------------------------------------
# The loop's own costs: no spin, no thread, and a bounded syscall bill
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_idle_connection_consumes_no_cpu(aconn, table):
    """The writer-disarm guard: a writer callback left armed on an
    always-writable fd spins the loop at 100% CPU.

    `process_time` rather than `os.times`, whose `SC_CLK_TCK` quantum is 10 ms —
    coarse enough that an idle connection reads as a flat zero and the ceiling
    means "a few clock ticks" instead of a CPU budget. An idle connection spends
    ~0.15 ms here whatever the window, because that is one-off scheduling and not
    a rate; a spinning one spends the window.
    """
    await aconn.push(table, _batch([{"pk": 1, "val": 1}]))

    before = time.process_time()
    await asyncio.sleep(0.2)
    spent = time.process_time() - before
    assert spent < 0.02, f"an idle connection spent {spent:.4f}s of CPU over a quiet 0.2s"


_COUNTED = ("writev", "write", "sendto", "sendmsg", "recvfrom", "read", "recvmsg",
            "epoll_wait", "epoll_pwait", "epoll_ctl", "futex", "poll", "ppoll")


def _strace_counts(target, tid, n, mode):
    out = subprocess.run(
        ["strace", "-f", "-c", "-o", "/dev/stdout",
         *_childproc.argv("_aiochild", "syscalls", target, tid, n, mode)],
        capture_output=True, text=True, timeout=180, env=_childproc.env())
    assert out.returncode == 0, f"child exited {out.returncode}\n{out.stdout}\n{out.stderr}"
    counts = {}
    for line in out.stdout.splitlines():
        f = line.split()
        if len(f) >= 5 and f[-1] in _COUNTED:
            counts[f[-1]] = counts.get(f[-1], 0) + int(f[3])
    return counts


def test_syscalls_per_operation(server, client):
    """The acceptance measure: `strace -f -c` over a subprocess, with the n=0
    run subtracted so startup and connect are out. One baseline for both modes:
    at n=0 the child's two branches do byte-identical work.

    `await` in a loop costs 4 syscalls per operation — writev, recvfrom, one
    blocking epoll_wait, and the zero-timeout epoll_wait asyncio spends on the
    wake `Future.set_result` posts — and no epoll_ctl: a lone operation flushes
    at submit, so no writer is armed. A gathered burst's first frame leaves at
    submit and the rest in one writev on the next loop turn.

    `gather`'s bill is not a per-operation constant: it is set by how many ACKs
    the server has queued when a read runs. What is the client's, and is pinned
    here, is one writev for the whole burst and at most one recvfrom per reply
    frame — never the two a header-then-payload read costs. Futex and sendto
    reach zero on both: no thread left to wake."""
    if shutil.which("strace") is None:
        pytest.skip("strace not installed")
    tid = client.create_table("t", KV)

    n = 200
    base = _strace_counts(server, tid, 0, "loop")
    measured = {}
    for mode in ("loop", "gather"):
        full = _strace_counts(server, tid, n, mode)
        measured[mode] = {k: full.get(k, 0) - base.get(k, 0) for k in _COUNTED}
        assert measured[mode]["futex"] <= 0, f"{mode} still wakes a thread: {measured}"
        assert measured[mode]["sendto"] <= 0, f"{mode} still writes a self-pipe: {measured}"

    loop_total = sum(max(v, 0) for v in measured["loop"].values())
    assert loop_total <= 4 * n + 8, (
        f"await-in-a-loop: {loop_total / n:.2f}/op over a 4/op budget\n{measured}")

    g = measured["gather"]
    assert g["writev"] <= 8, f"a gathered burst must leave in one writev, not {g['writev']}"
    # One read per reply frame at worst, plus the slack the n=0 subtraction does
    # not perfectly cancel; two per frame is what this rules out.
    assert g["recvfrom"] <= n + 8, f"at most one read per reply frame, got {g['recvfrom']} for {n}"
    gather_total = sum(max(v, 0) for v in g.values())
    assert gather_total * 4 <= loop_total * 3, (
        f"gather must cost materially less than awaiting one at a time: "
        f"{gather_total} vs {loop_total}\n{measured}")


def test_an_exception_escaping_asyncio_run_exits_cleanly(server):
    """An exception escaping `asyncio.run` with work in flight must exit with
    the exception's status, never an abort."""
    out = subprocess.run(_childproc.argv("_aiochild", "shutdown", server),
                         capture_output=True, text=True, timeout=120, env=_childproc.env())
    assert out.returncode == 1, f"rc={out.returncode}\n{out.stdout}\n{out.stderr}"
    assert "RuntimeError: boom" in out.stderr, out.stderr
