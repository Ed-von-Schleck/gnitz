"""`gnitz.aio` — the async transport's own contracts.

What is async here is the transport: submit-on-call, pipelining, reply
correlation across mixed verb kinds, error propagation out of a gather, and the
loop's own costs. The decode is shared with the sync client (`scan_result`),
so a value- or row-level claim belongs to the coordinate that owns it —
`mutation_pattern`, `value_domain`, `read_verb` — not here.
"""

import asyncio
import shutil
import subprocess
import sys
import time

import pytest
import pytest_asyncio
import gnitz
from gnitz import aio
from _read import bag


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

PK_VAL_COLS = [
    gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("val", gnitz.TypeCode.I64),
]
SCHEMA = gnitz.Schema(PK_VAL_COLS)


@pytest_asyncio.fixture
async def aconn(server):
    async with aio.connect(server) as conn:
        yield conn


@pytest.fixture
def table(client, schema_name):
    """A disposable `(pk, val)` table; `schema_name` drops it with its schema."""
    return client.create_table(schema_name, "t", PK_VAL_COLS)


def _batch(rows):
    return gnitz.ZSetBatch(SCHEMA).extend(rows)


# ---------------------------------------------------------------------------
# Pipelining
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pipeline_mixes_operation_kinds(client, schema_name, aconn):
    """Push, empty push, scan, seek and scan_many gathered as one in-flight
    group. The server handles one request per connection at a time and replies
    in request order, so each future must resolve to *its own* operation's
    result — a swap between two kinds would hand a scan's rows to a push's
    future, or one relation's rows to another's. The empty push is answered
    without a server round trip, and still must not shift the replies behind it.
    Run at W>1: replies leave the workers out of order and only the master's
    serialisation puts them back."""
    a = client.create_table(schema_name, "ta", PK_VAL_COLS)
    b = client.create_table(schema_name, "tb", PK_VAL_COLS)
    # Distinct per-relation payloads, so a mis-correlated reply is visible.
    await aconn.push(a, _batch([{"pk": i, "val": 100 + i} for i in range(1, 6)]))
    await aconn.push(b, _batch([{"pk": i, "val": 900 + i} for i in range(1, 4)]))

    push_lsn, empty_lsn, scan_a, scan_b, seek_a, seek_b, many = await asyncio.gather(
        aconn.push(a, _batch([{"pk": 42, "val": 4242}])),
        aconn.push(b, gnitz.ZSetBatch(SCHEMA)),
        aconn.scan(a),
        aconn.scan(b),
        aconn.seek(a, 3),
        aconn.seek(b, 2),
        aconn.scan_many([b, a]),
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
    for result in await asyncio.gather(*[aconn.scan(table) for _ in range(10)]):
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
    assert bag(await aconn.scan(table)) == {(1, 10): 1, (2, 20): 1}


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_an_error_surfaces_from_a_gather_and_the_connection_survives(aconn, table):
    """A refused request leaves the connection usable, whether it is refused
    client-side before any frame is sent (an empty `scan_many`, whose count=0
    frame would draw a server error frame nothing reads) or by the server inside
    a gathered group — where the good pushes must not swallow the bad one."""
    with pytest.raises(gnitz.GnitzError):
        await aconn.scan_many([])

    bad = gnitz.ZSetBatch(gnitz.Schema([PK_VAL_COLS[0]])).extend([{"pk": 1}])
    with pytest.raises(gnitz.GnitzError):
        await asyncio.gather(
            aconn.push(table, _batch([{"pk": 1, "val": 1}])),
            aconn.push(0xDEAD_BEEF, bad),                      # no such relation
            aconn.push(table, _batch([{"pk": 3, "val": 3}])),
        )

    await aconn.push(table, _batch([{"pk": 7, "val": 70}]))
    assert bag(await aconn.scan(table)).get((7, 70)) == 1
    assert bag((await aconn.scan_many([table]))[0]).get((7, 70)) == 1


@pytest.mark.asyncio
async def test_connection_loss_resolves_every_queued_request(disposable_server):
    """Losing the connection must fail every submitted request.

    All 3000 are submitted before the loop gets a turn, so each holds a future
    and a queued frame. The step that meets the dead peer must resolve all of
    them: a future nobody resolves is a hang, not an error, and the `wait_for`
    is what turns that into a failure.
    """
    target, proc = disposable_server
    conn = await aio.connect(target)
    proc.kill()
    proc.wait()

    futs = [conn.scan(gnitz.SCHEMA_TAB) for _ in range(3000)]
    results = await asyncio.wait_for(
        asyncio.gather(*futs, return_exceptions=True), timeout=30)
    resolved_ok = [r for r in results if not isinstance(r, BaseException)]
    assert not resolved_ok, f"{len(resolved_ok)}/{len(results)} succeeded against a dead server"

    # A request submitted after the failure was observed must resolve too.
    with pytest.raises(gnitz.GnitzError):
        await asyncio.wait_for(conn.scan(gnitz.SCHEMA_TAB), timeout=30)

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
        assert len(await conn.scan(gnitz.SCHEMA_TAB)) == len(await other.scan(gnitz.SCHEMA_TAB)) > 0

    await conn.aclose()
    await conn.aclose()          # idempotent
    with pytest.raises(gnitz.GnitzError, match="connection closed"):
        await conn.scan(gnitz.SCHEMA_TAB)


# ---------------------------------------------------------------------------
# The mismatch a stale stamp draws
#
# That an async push packs warm at all is what makes the mismatch below
# reachable: a cold frame carries its own schema block and never draws one.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stale_stamp_pushes_fail_and_the_connection_recovers(aconn, client, schema_name):
    """A column rename bumps the relation's schema version, so every push
    already encoded at the stale stamp bounces. Each future fails with the
    mismatch, none of their rows is committed, and the connection stays usable:
    the eviction makes the next push cold, and it lands."""
    tid = client.create_table(schema_name, "t", PK_VAL_COLS)
    # Warm the async connection's cache under the current version.
    await aconn.push(tid, _batch([{"pk": 1, "val": 1}]))

    client.execute_sql("ALTER TABLE t RENAME COLUMN val TO amount", schema_name=schema_name)

    results = await asyncio.gather(
        *[aconn.push(tid, _batch([{"pk": 10 + i, "val": 100 + i}])) for i in range(3)],
        return_exceptions=True)
    assert all(isinstance(r, gnitz.GnitzError) for r in results), results
    assert all("schema version mismatch" in str(r) for r in results), results

    # Nothing they carried was committed — a mismatched push returns before the
    # commit path. Same connection, no reconnect: the next push is cold and lands.
    await aconn.push(tid, _batch([{"pk": 20, "val": 200}]))
    assert bag(await aconn.scan(tid), "pk", "amount") == {(1, 1): 1, (20, 200): 1}


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


_SYSCALL_CHILD = '''
import asyncio, sys
from gnitz import aio
import gnitz

target, tid, n, mode = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
schema = gnitz.Schema(cols)

def batch(pk):
    b = gnitz.ZSetBatch(schema)
    b.append(pk=pk, val=pk)
    return b

async def main():
    async with aio.connect(target) as conn:
        # One warm-up push, so the schema cache and the measured region are the
        # same shape for every mode and for the n=0 baseline.
        await conn.push(tid, batch(0))
        if mode == "loop":
            for i in range(n):
                await conn.push(tid, batch(i + 1))
        elif n:
            await asyncio.gather(*[conn.push(tid, batch(i + 1)) for i in range(n)])

asyncio.run(main())
'''

_COUNTED = ("writev", "write", "sendto", "sendmsg", "recvfrom", "read", "recvmsg",
            "epoll_wait", "epoll_pwait", "futex", "poll", "ppoll")


def _strace_counts(script, target, tid, n, mode):
    out = subprocess.run(
        ["strace", "-f", "-c", "-o", "/dev/stdout",
         sys.executable, script, target, str(tid), str(n), mode],
        capture_output=True, text=True, timeout=180)
    assert out.returncode == 0, f"child exited {out.returncode}\n{out.stdout}\n{out.stderr}"
    counts = {}
    for line in out.stdout.splitlines():
        f = line.split()
        if len(f) >= 5 and f[-1] in _COUNTED:
            counts[f[-1]] = counts.get(f[-1], 0) + int(f[3])
    return counts


def test_syscalls_per_operation(server, client, schema_name, tmp_path):
    """The acceptance measure: `strace -f -c` over a subprocess, with the n=0
    run subtracted so startup and connect are out. One baseline for both modes:
    at n=0 the child's two branches do byte-identical work.

    `await` in a loop costs 5 syscalls per operation — writev, recvfrom, one
    blocking epoll_wait, and the two zero-timeout epoll_waits asyncio spends on
    the two handles each operation schedules (the deferred flush, and the wake
    `Future.set_result` posts).

    `gather`'s bill is not a per-operation constant: it is set by how many ACKs
    the server has queued when a read runs. What is the client's, and is pinned
    here, is one writev for the whole burst and at most one recvfrom per reply
    frame — never the two a header-then-payload read costs. Futex and sendto
    reach zero on both: no thread left to wake."""
    if shutil.which("strace") is None:
        pytest.skip("strace not installed")
    tid = client.create_table(schema_name, "t", PK_VAL_COLS)
    script = tmp_path / "syscall_child.py"
    script.write_text(_SYSCALL_CHILD)

    n = 200
    base = _strace_counts(str(script), server, tid, 0, "loop")
    measured = {}
    for mode in ("loop", "gather"):
        full = _strace_counts(str(script), server, tid, n, mode)
        measured[mode] = {k: full.get(k, 0) - base.get(k, 0) for k in _COUNTED}
        assert measured[mode]["futex"] <= 0, f"{mode} still wakes a thread: {measured}"
        assert measured[mode]["sendto"] <= 0, f"{mode} still writes a self-pipe: {measured}"

    loop_total = sum(max(v, 0) for v in measured["loop"].values())
    assert loop_total <= 5 * n + 8, (
        f"await-in-a-loop: {loop_total / n:.2f}/op over a 5/op budget\n{measured}")

    g = measured["gather"]
    assert g["writev"] <= 8, f"a gathered burst must leave in one writev, not {g['writev']}"
    # One read per reply frame at worst, plus the slack the n=0 subtraction does
    # not perfectly cancel; two per frame is what this rules out.
    assert g["recvfrom"] <= n + 8, f"at most one read per reply frame, got {g['recvfrom']} for {n}"
    gather_total = sum(max(v, 0) for v in g.values())
    assert gather_total * 4 <= loop_total * 3, (
        f"gather must cost materially less than awaiting one at a time: "
        f"{gather_total} vs {loop_total}\n{measured}")


_SHUTDOWN_CHILD = '''
import asyncio, sys
from gnitz import aio

async def main():
    conn = await aio.connect(sys.argv[1])
    # Work in flight when the exception escapes.
    conn.scan(1)
    conn.scan(1)
    raise RuntimeError("boom")

asyncio.run(main())
'''


def test_an_exception_escaping_asyncio_run_exits_cleanly(server, tmp_path):
    """An exception escaping `asyncio.run` with work in flight must exit with
    the exception's status, never an abort."""
    script = tmp_path / "shutdown_child.py"
    script.write_text(_SHUTDOWN_CHILD)
    out = subprocess.run([sys.executable, str(script), server],
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 1, f"rc={out.returncode}\n{out.stdout}\n{out.stderr}"
    assert "RuntimeError: boom" in out.stderr, out.stderr
