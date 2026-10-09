"""One scenario on one fresh server: phases, and the record they leave.

A phase is the unit every number is taken over. It runs a fixed amount of work,
ends with every view drained — a push is acknowledged before its tick, so work
that is not drained is work some later phase would be charged — and records what
each process group spent on it.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import re
import shutil
import tempfile
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path

import gnitz
from _serverproc import ServerProc

from . import counters, disk, profile

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

WIDTHS = {"U8": 1, "I8": 1, "U16": 2, "I16": 2, "U32": 4, "I32": 4, "F32": 4, "U64": 8, "I64": 8,
          "F64": 8, "U128": 16, "UUID": 16, "I128": 16, "DATE": 4, "TIMESTAMP": 8, "DECIMAL": 8}

# What each storage regime sets on the server. `l0` leaves the RAM tier, which a
# store of a benchmark's size never fills; `compacted` shrinks it so the same
# rows spill, fold and compact; `checkpointed` shrinks the SAL's checkpoint
# threshold instead, so the run is cut by checkpoints.
REGIMES = {
    "l0": {},
    "compacted": {"GNITZ_RAM_TIER_BYTES": str(1 << 20)},
    "checkpointed": {"GNITZ_CHECKPOINT_BYTES": str(256 << 10)},
}

SAL_BYTES = 256 << 20

_VIEW = re.compile(r"CREATE VIEW (\w+)\s*(?:WITH \([^)]*\)\s*)?AS\s+(.*)", re.S | re.I)
_TABLE = re.compile(r"CREATE TABLE (\w+)", re.I)


def percentile(sorted_ms, q):
    return sorted_ms[min(int(len(sorted_ms) * q), len(sorted_ms) - 1)]


@dataclass
class Params:
    rows: int
    workers: int
    regime: str
    env: dict = field(default_factory=dict)
    keep: bool = False
    layers: tuple = ()


@dataclass
class Table:
    name: str
    tid: int
    schema: object
    pk: list
    widths: dict       # column -> bytes, None for a string
    stream: bool
    pushed: int = 0    # value bytes of every row `Run.ops` prepared for it


@dataclass
class Op:
    """One prepared push: built before the phase that sends it, so the phase is
    charged the push and not the row generator."""
    table: Table
    batch: object
    rows: int


def _client(sock, barrier, queue, fn, args):
    try:
        with gnitz.connect(sock) as conn:
            barrier.wait()
            queue.put(fn(conn, *args))
    except BaseException:
        barrier.abort()
        queue.put({"error": traceback.format_exc()})


def time_calls(fn, n):
    """`fn(i)` for `i < n`; each call's milliseconds."""
    out = []
    for i in range(n):
        t = time.perf_counter()
        fn(i)
        out.append((time.perf_counter() - t) * 1e3)
    return out


async def time_pushes(conn, ops):
    """Each of `ops` pushed on the `gnitz.aio` connection `conn` and awaited;
    each push's milliseconds."""
    out = []
    for op in ops:
        t = time.perf_counter()
        await conn.push(op.table.tid, op.batch)
        out.append((time.perf_counter() - t) * 1e3)
    return out


class Phase:
    """`boots` is a phase a boot runs in: no process of the server exists when
    it opens, so its groups are the server's, with their totals since they
    started."""

    def __init__(self, run, name, index, drain=True, boots=False):
        self.run, self.name, self.index, self._drain, self._boots = run, name, index, drain, boots
        self.rows = 0
        self.extra = {}
        self._calls = {}

    def __enter__(self):
        if not self._boots:
            self._counters = counters.Counters(self.run.groups())
            self._counters.start()
        profile.mark(self.index)
        self.t0 = time.clock_gettime(time.CLOCK_MONOTONIC)
        return self

    def add(self, kind, latencies_ms, rows=0):
        """Calls of one kind, each by its milliseconds."""
        ms, n = self._calls.setdefault(kind, ([], [0]))
        ms.extend(latencies_ms)
        n[0] += rows
        self.rows += rows

    def timed(self, kind, fn, *args, rows=0):
        t = time.perf_counter()
        out = fn(*args)
        self.add(kind, [(time.perf_counter() - t) * 1e3], rows)
        return out

    def push(self, op):
        self.timed("push", self.run.conn.push, op.table.tid, op.batch, rows=op.rows)

    def replay(self, ops, read=None):
        """Send `ops` in order; with `read`, read that view after each."""
        for op in ops:
            self.push(op)
            if read:
                self.timed("read", self.run.conn.execute_sql, f"SELECT * FROM {read} LIMIT 1")

    def sql(self, kind, stmt, rows=0):
        return self.timed(kind, self.run.conn.execute_sql, stmt, rows=rows)

    def count(self, fn, rows):
        """Run `fn` for the instructions this thread retires in it alone: a
        client-side cost with no request in it, which the phase's own count
        would bury under the harness."""
        self.extra["client_instr_exact"] = self.extra.get("client_instr_exact", 0) + gnitz._native.instructions_retired(fn)
        self.rows += rows

    def clients(self, specs):
        """Fork one process per `(fn, *args)`; each connects for itself, waits
        for the others, and runs `fn(conn, *args)`, which returns
        `{"calls": {kind: ([ms, ...], rows)}, ...}`. The calls join this
        phase's; every other key is summed over the clients and returned."""
        self.extra["unpaced"] = True        # the clients race each other
        ctx = multiprocessing.get_context("fork")
        barrier, queue = ctx.Barrier(len(specs)), ctx.Queue()
        procs = [ctx.Process(target=_client, args=(self.run.server.sock_path, barrier, queue, fn, args))
                 for fn, *args in specs]
        for p in procs:
            p.start()
            self.run.client_pids.add(p.pid)
        # Drained before joined: a child blocks in `put` once the pipe is full.
        parts = [queue.get() for _ in procs]
        for p in procs:
            p.join()
        totals = {}
        for part in parts:
            if "error" in part:
                raise RuntimeError(f"a client of phase {self.name} failed:\n{part['error']}")
            for kind, (ms, rows) in part.pop("calls", {}).items():
                self.add(kind, ms, rows)
            for k, v in part.items():
                totals[k] = totals.get(k, 0) + v
        return totals

    def drain(self):
        """A read of every view: the tick a pending push still owes runs here."""
        for v in self.run.views:
            self.timed("drain", self.run.conn.execute_sql, f"SELECT * FROM {v} LIMIT 1")

    def __exit__(self, exc_type, exc, tb):
        run = self.run
        if exc_type is None and self._drain:
            self.drain()
        t1 = time.clock_gettime(time.CLOCK_MONOTONIC)
        profile.mark(profile.BETWEEN)
        if not self._boots:
            groups = self._counters.stop()
        elif exc_type is None:
            groups = counters.since_start({g: pid for g, pid in run.groups().items() if g != "client"})
        if exc_type is None:
            calls = {}
            for kind, (ms, n) in self._calls.items():
                s = sorted(ms)
                calls[kind] = {"n": len(s), "rows": n[0], "sum_ms": sum(s), "p50_ms": percentile(s, 0.5),
                               "p90_ms": percentile(s, 0.9), "p99_ms": percentile(s, 0.99), "max_ms": s[-1]}
            run.phases.append({"name": self.name, "t0": self.t0, "t1": t1, "wall_s": t1 - self.t0,
                               "rows": self.rows, "calls": calls, "groups": groups, **self.extra})
        return False


class Run:
    def __init__(self, scenario, params: Params, out_dir: Path):
        self.scenario, self.params = scenario, params
        self.rows = scenario.fixed_rows or params.rows
        self.phases = []
        self.tables = {}
        self.views = {}            # name -> body, in the order created
        self.snapshots = {}
        self.client_pids = set()
        self.conn = None
        (REPO_ROOT / "tmp").mkdir(exist_ok=True)
        self.tmp = Path(tempfile.mkdtemp(dir=REPO_ROOT / "tmp", prefix=f"bench_{scenario.name}_"))
        self.data_dir = self.tmp / "data"
        self.server = ServerProc(str(self.data_dir), str(self.tmp / "gnitz.sock"))
        env = self.server.extra_env
        env["GNITZ_SAL_BYTES"] = str(SAL_BYTES)
        env["GNITZ_CPU_AFFINITY"] = "1"
        env.update(REGIMES[params.regime])
        env.update(params.env)
        self.layers = profile.Layers(params.layers, out_dir)

    # -- server ---------------------------------------------------------------

    def boot(self):
        self.server.start(workers=self.params.workers, timeout=60.0)
        self.conn = gnitz.connect(self.server.sock_path)
        self._workers = self.server.worker_pids()
        self.layers.server_started(self.groups())

    def groups(self):
        """`{name: pid}`: the driver, the master, and each worker."""
        return {"client": os.getpid(), "master": self.server.proc.pid,
                **{f"worker{i}": pid for i, pid in sorted(self._workers.items())}}

    # -- schema and data -------------------------------------------------------

    def ddl(self, *stmts):
        """Tables and views, views before any row: a view maintains only the
        deltas pushed after it exists."""
        for stmt in stmts:
            self.conn.execute_sql(stmt)
            self._created(stmt)

    def ddl_in(self, phase, stmt, rows=0):
        """One statement of DDL as a timed call of `phase`."""
        phase.sql("ddl", stmt, rows=rows)
        self._created(stmt)

    def _created(self, stmt):
        if m := _VIEW.match(stmt.strip()):
            self.views[m[1]] = m[2]
        elif m := _TABLE.match(stmt.strip()):
            tid, schema = self.conn.resolve_table(m[1])
            cols = schema.columns
            self.tables[m[1]] = Table(
                m[1], tid, schema, [cols[i].name for i in schema.pk_indices],
                {c.name: WIDTHS.get(gnitz.TypeCode(c.type_code).name) for c in cols},
                bool(re.search(r"stream\s*=\s*true", stmt, re.I)))

    @staticmethod
    def _price(t, row):
        """What `row` (column -> value) holds: each number at its column's
        width, each string at its length, a NULL at nothing."""
        return sum(0 if (v := row.get(k)) is None else w or len(v.encode()) for k, w in t.widths.items())

    def ops(self, table, rows, batch_rows):
        """`rows` (dicts) as prepared pushes of `batch_rows` each."""
        t = self.tables[table]
        out, batch, held = [], gnitz.ZSetBatch(t.schema), 0
        for row in rows:
            t.pushed += self._price(t, row)
            batch.append(**row)
            held += 1
            if held == batch_rows:
                out.append(Op(t, batch, held))
                batch, held = gnitz.ZSetBatch(t.schema), 0
        if held:
            out.append(Op(t, batch, held))
        return out

    def base_rows(self):
        """The rows the base tables hold."""
        return sum(len(self.conn.scan(t.tid, t.schema)) for t in self.tables.values() if not t.stream)

    # -- phases ----------------------------------------------------------------

    def phase(self, name, **kw):
        assert all(p["name"] != name for p in self.phases), f"a second phase named {name}"
        return Phase(self, name, len(self.phases), **kw)

    def _held(self):
        """What a disk reading is held against: `{relation id: (name, rows a
        scan returns)}`, and the value bytes of the live rows. A stream holds
        no row to scan; its value bytes are what it was pushed."""
        relations, value_bytes = {}, 0
        for t in self.tables.values():
            n = 0
            if t.stream:
                value_bytes += t.pushed
            else:
                for r in self.conn.scan(t.tid, t.schema):
                    n += 1
                    value_bytes += self._price(t, r._asdict())
            relations[t.tid] = (t.name, n)
        for v in self.views:
            tid, schema = self.conn.resolve_table(v)
            relations[tid] = (v, len(self.conn.scan(tid, schema)))
        return relations, value_bytes

    def _read_disk(self, reading, relations, value_bytes):
        """The disk reading `reading` of a server a graceful stop has just
        checkpointed, so that its shards are its stores' whole state."""
        self.layers.server_stopped(reading)
        self.snapshots[reading] = disk.snapshot(
            self.data_dir, relations, {t.tid for t in self.tables.values()}, value_bytes,
            sum(t.pushed for t in self.tables.values()))

    def _boot_phase(self, name, rows):
        with self.phase(name, boots=True) as ph:
            self.boot()
            ph.rows = rows
            ph.extra.update(ready_s=time.clock_gettime(time.CLOCK_MONOTONIC) - ph.t0,
                            rebuilt_views=self.server.rebuilt_view_count())

    def restart(self, reading):
        """A graceful stop and the boot on what it left, as the phases
        `checkpoint` and `boot`, with the disk reading `reading` between them."""
        relations, value_bytes = self._held()
        rows = sum(relations[t.tid][1] for t in self.tables.values())
        self.conn.close()
        with self.phase("checkpoint", drain=False) as ph:
            ph.rows = rows
            # A stop finds the stores wherever their background work had got to.
            ph.extra["unpaced"] = True
            self.server.stop_graceful(timeout=600)
        self._read_disk(reading, relations, value_bytes)
        self._boot_phase("boot", rows)

    def crash(self):
        """A kill, and the boot that replays the log as the phase `recover`."""
        rows = self.base_rows()
        self.conn.close()
        self.server.stop()
        self.layers.server_stopped()
        self._boot_phase("recover", rows)

    # -- backfill and verification ---------------------------------------------

    def bag(self, name):
        """`{row: summed weight}` of a scan of `name`, ghosts dropped."""
        tid, schema = self.conn.resolve_table(name)
        acc = {}
        for r in self.conn.scan(tid, schema):
            k = tuple(r)
            acc[k] = acc.get(k, 0) + r._weight
        return {k: w for k, w in acc.items() if w}

    def verify(self):
        """Create every view a second time over the data it already holds — the
        phase `backfill`, which is what a backfill costs — and hold each
        maintained view against that twin, weights included. The twins are the
        benchmark's own, and are dropped again."""
        if not self.views:
            return None
        rows = self.base_rows()
        with self.phase("backfill") as ph:
            for v, body in self.views.items():
                ph.sql("create_view", f"CREATE VIEW {v}__twin AS {body}", rows=rows)
        skipped = self.scenario.unverified
        checked, wrong = 0, []
        for v in self.views:
            if v in skipped:
                continue
            a, b = self.bag(v), self.bag(f"{v}__twin")
            checked += 1
            if a != b:
                wrong.append({"view": v, "maintained_rows": len(a), "backfilled_rows": len(b),
                              "differing_rows": sum(a.get(k) != b.get(k) for k in a.keys() | b.keys())})
        if wrong:
            raise AssertionError(f"{self.scenario.name}: a maintained view differs from its backfill: {wrong}")
        for v in self.views:
            self.conn.execute_sql(f"DROP VIEW {v}__twin")
        return {"checked": checked, "skipped": skipped}

    # -- lifecycle ---------------------------------------------------------------

    def execute(self):
        p = self.params
        try:
            self.layers.start()
            self.boot()
            self.scenario.run(self)
            verified = self.verify()
            relations, value_bytes = self._held()
            self.conn.close()
            self.server.stop_graceful(timeout=600)
            self._read_disk("final", relations, value_bytes)
        finally:
            profile.mark(profile.END)
            if self.server.proc is not None:
                self.server.stop()
            layers = self.layers.finish(self.phases, self.client_pids)
            if p.keep:
                # The shards are what a kept directory is read for; the SAL is its fallocated size.
                (self.data_dir / "wal.sal").unlink(missing_ok=True)
                if "final" in self.snapshots:
                    (self.tmp / "result.json").write_text(json.dumps(
                        {"scenario": self.scenario.name, "regime": p.regime, **self.snapshots["final"]}, indent=1))
            else:
                shutil.rmtree(self.tmp, ignore_errors=True)
        return {
            "scenario": self.scenario.name, "family": self.scenario.family, "doc": self.scenario.doc,
            "rows": self.rows, "workers": p.workers, "regime": p.regime, "capabilities": counters.capabilities(),
            "phases": self.phases, "disk": self.snapshots, "verified": verified, "layers": layers,
            **({"kept": str(self.tmp)} if p.keep else {}),
        }
