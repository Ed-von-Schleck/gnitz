"""Core measurement infrastructure for GnitzDB benchmarks."""

from __future__ import annotations

import multiprocessing
import statistics
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
    perf_stat: dict = field(default_factory=dict)


def percentiles(latencies: list[float]) -> tuple[float, float, float]:
    if not latencies:
        return 0.0, 0.0, 0.0
    s = sorted(latencies)
    n = len(s)
    return (
        s[int(n * 0.50)] if n > 0 else 0.0,
        s[int(n * 0.90)] if n > 0 else 0.0,
        s[min(int(n * 0.99), n - 1)] if n > 0 else 0.0,
    )


class BenchTimer:
    def __init__(self, name: str, category: str, warmup: int = 2):
        self._name = name
        self._category = category
        self._warmup = warmup
        self._latencies: list[float] = []
        self._rows = 0
        self._iterations = 0
        self._t0: float | None = None
        self._elapsed: float = 0.0

    def measure(self, fn, *args, rows_per_call: int = 0, **kwargs) -> float:
        """Time a single fn() call. Returns elapsed seconds."""
        if self._t0 is None:
            self._t0 = time.perf_counter()
        start = time.perf_counter()
        fn(*args, **kwargs)
        dt = time.perf_counter() - start
        self._iterations += 1
        if self._iterations > self._warmup:
            self._latencies.append(dt * 1000.0)
            self._rows += rows_per_call
        return dt

    def result(self) -> BenchResult:
        if self._t0 is not None:
            self._elapsed = time.perf_counter() - self._t0
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
        )


def _worker_fn(socket_path, target_fn, args, barrier, queue):
    """Child process: connect, sync, run, report."""
    import gnitz

    with gnitz.connect(socket_path) as conn:
        barrier.wait()
        t0 = time.perf_counter()
        target_fn(conn, *args)
        dt = time.perf_counter() - t0
        queue.put(dt * 1000.0)


def run_parallel(
    socket_path: str,
    target_fn,
    args: tuple,
    num_clients: int,
) -> list[float]:
    """Fork N processes, each with its own gnitz.connect().

    target_fn(conn, *args) is called in each child after a barrier sync.
    Returns list of per-client latencies in ms.
    """
    barrier = multiprocessing.Barrier(num_clients)
    queue: multiprocessing.Queue = multiprocessing.Queue()
    procs = []
    for _ in range(num_clients):
        p = multiprocessing.Process(
            target=_worker_fn,
            args=(socket_path, target_fn, args, barrier, queue),
        )
        p.start()
        procs.append(p)
    for p in procs:
        p.join(timeout=120)
    latencies = []
    while not queue.empty():
        latencies.append(queue.get_nowait())
    return latencies


# Session-global results list
_results: list[BenchResult] = []


def record_result(r: BenchResult) -> None:
    _results.append(r)


def get_all_results() -> list[BenchResult]:
    return list(_results)
