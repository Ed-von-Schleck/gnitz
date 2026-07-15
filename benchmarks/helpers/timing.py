"""Core measurement infrastructure for GnitzDB benchmarks."""

from __future__ import annotations

import multiprocessing
import time
from dataclasses import dataclass, field


@dataclass
class BenchResult:
    name: str
    category: str
    elapsed_s: float
    rows: int
    rows_per_sec: float
    iterations: int
    latencies_ms: list[float]
    p50_ms: float
    p90_ms: float
    p99_ms: float
    num_clients: int = 1
    # Non-latency scalars (txns_per_sec, conflicts, retries, torn_reads, ...)
    # surfaced by the transaction/HTAP/serving tiers. Copied verbatim into
    # summary.json / timings.csv by conftest.write_results.
    extra: dict = field(default_factory=dict)


def percentiles(latencies: list[float]) -> tuple[float, float, float]:
    if not latencies:
        return 0.0, 0.0, 0.0
    s = sorted(latencies)
    n = len(s)
    return s[int(n * 0.50)], s[int(n * 0.90)], s[min(int(n * 0.99), n - 1)]


class BenchTimer:
    def __init__(self, name: str, category: str, warmup: int = 2):
        self._name = name
        self._category = category
        self._warmup = warmup
        self._latencies: list[float] = []
        self._rows = 0
        self._iterations = 0
        # Public, writable by the test: non-latency scalars and client count.
        self.extra: dict = {}
        self.num_clients = 1

    def measure(self, fn, *args, rows_per_call: int = 0, **kwargs) -> float:
        """Time a single fn() call. Returns elapsed seconds."""
        start = time.perf_counter()
        fn(*args, **kwargs)
        dt = time.perf_counter() - start
        self._iterations += 1
        if self._iterations > self._warmup:
            self._latencies.append(dt * 1000.0)
            self._rows += rows_per_call
        return dt

    def add_latencies(self, latencies_ms, rows: int = 0) -> None:
        """Ingest externally-collected per-op latencies (ms) and row count.

        Used by benches that run in forked child processes (transactions,
        HTAP, contention) where `measure` cannot time across the fork.
        """
        self._latencies.extend(latencies_ms)
        self._rows += rows

    def result(self) -> BenchResult:
        p50, p90, p99 = percentiles(self._latencies)
        elapsed = sum(self._latencies) / 1000.0 if self._latencies else 0.0
        rps = self._rows / elapsed if elapsed > 0 else 0.0
        return BenchResult(
            name=self._name,
            category=self._category,
            elapsed_s=round(elapsed, 4),
            rows=self._rows,
            rows_per_sec=round(rps, 1),
            iterations=len(self._latencies),
            latencies_ms=[round(x, 3) for x in self._latencies],
            p50_ms=round(p50, 3),
            p90_ms=round(p90, 3),
            p99_ms=round(p99, 3),
            num_clients=self.num_clients,
            extra=dict(self.extra),
        )


def _rmw_worker(socket_path, sql, schema_name, ops, retry, barrier, queue):
    """Child: run `ops` autocommit RMW statements, counting commits/conflicts."""
    import gnitz

    commits = retries = conflicts = 0
    latencies: list[float] = []
    with gnitz.connect(socket_path) as c:
        barrier.wait()
        t0 = time.perf_counter()
        for _ in range(ops):
            start = time.perf_counter()
            while True:
                try:
                    c.execute_sql(sql, schema_name=schema_name)
                    commits += 1
                    break
                except gnitz.GnitzConflictError:
                    # Autocommit already retried <=4x internally, so a surfaced
                    # conflict is genuine sustained contention.
                    conflicts += 1
                    if retry:
                        retries += 1
                        continue
                    break
            latencies.append((time.perf_counter() - start) * 1000.0)
        elapsed = time.perf_counter() - t0
    queue.put({
        "commits": commits, "retries": retries, "conflicts": conflicts,
        "latencies_ms": latencies, "elapsed_s": elapsed,
    })


def run_contended_rmw(
    socket_path: str,
    sql: str,
    schema_name: str,
    n_clients: int,
    ops_per_client: int,
    *,
    retry: bool = True,
) -> dict:
    """Fork `n_clients` processes each running `ops_per_client` autocommit RMW
    executions of `sql`. With `retry=True` a surfaced GnitzConflictError is
    re-issued (counting a retry) until it commits. Aggregates across clients.

    Returns {commits, retries, conflicts, latencies_ms, elapsed_s}.
    """
    barrier = multiprocessing.Barrier(n_clients)
    queue: multiprocessing.Queue = multiprocessing.Queue()
    procs = [
        multiprocessing.Process(
            target=_rmw_worker,
            args=(socket_path, sql, schema_name, ops_per_client, retry, barrier, queue),
        )
        for _ in range(n_clients)
    ]
    for p in procs:
        p.start()
    # Drain before join: a child blocks on put() if the pipe fills, so joining
    # first could deadlock on large latency lists.
    parts = [queue.get() for _ in range(n_clients)]
    for p in procs:
        p.join(timeout=300)
    return {
        "commits": sum(r["commits"] for r in parts),
        "retries": sum(r["retries"] for r in parts),
        "conflicts": sum(r["conflicts"] for r in parts),
        "latencies_ms": [x for r in parts for x in r["latencies_ms"]],
        "elapsed_s": max((r["elapsed_s"] for r in parts), default=0.0),
    }


def _htap_worker(socket_path, fn, duration_s, barrier, queue):
    """Child: connect, sync, run `fn(conn, deadline)` for a wall-clock window."""
    import gnitz

    with gnitz.connect(socket_path) as c:
        barrier.wait()
        deadline = time.perf_counter() + duration_s
        stats = fn(c, deadline)
    queue.put(stats)


def run_htap(
    socket_path: str,
    writer_fn,
    reader_fn,
    n_writers: int,
    n_readers: int,
    duration_s: float,
) -> dict:
    """Fork `n_writers` writers and `n_readers` readers behind one barrier; each
    runs its fn for `duration_s`. `writer_fn(conn, deadline)` returns
    {commits, conflicts, latencies}; `reader_fn(conn, deadline)` returns
    {ops, latencies, torn_reads}.

    Returns aggregated {writer_commits, writer_conflicts, writer_latencies,
    reader_ops, reader_latencies, torn_reads}.
    """
    total = n_writers + n_readers
    barrier = multiprocessing.Barrier(total)
    wq: multiprocessing.Queue = multiprocessing.Queue()
    rq: multiprocessing.Queue = multiprocessing.Queue()
    procs = []
    for _ in range(n_writers):
        procs.append(multiprocessing.Process(
            target=_htap_worker, args=(socket_path, writer_fn, duration_s, barrier, wq)))
    for _ in range(n_readers):
        procs.append(multiprocessing.Process(
            target=_htap_worker, args=(socket_path, reader_fn, duration_s, barrier, rq)))
    for p in procs:
        p.start()
    w_parts = [wq.get() for _ in range(n_writers)]
    r_parts = [rq.get() for _ in range(n_readers)]
    for p in procs:
        p.join(timeout=max(300.0, duration_s * 4))
    return {
        "writer_commits": sum(s.get("commits", 0) for s in w_parts),
        "writer_conflicts": sum(s.get("conflicts", 0) for s in w_parts),
        "writer_latencies": [x for s in w_parts for x in s.get("latencies", [])],
        "reader_ops": sum(s.get("ops", 0) for s in r_parts),
        "reader_latencies": [x for s in r_parts for x in s.get("latencies", [])],
        "torn_reads": sum(s.get("torn_reads", 0) for s in r_parts),
    }


# Session-global results list
_results: list[BenchResult] = []


def record_result(r: BenchResult) -> None:
    _results.append(r)


def get_all_results() -> list[BenchResult]:
    return list(_results)
