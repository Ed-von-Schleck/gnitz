"""Session-scoped server lifecycle, pytest options, and result writing."""

from __future__ import annotations

import json
import csv
import datetime
import multiprocessing
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

import gnitz
from helpers.timing import get_all_results

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def client(socket_path):
    with gnitz.connect(socket_path) as conn:
        yield conn


def pytest_addoption(parser):
    parser.addoption("--full", action="store_true", default=False,
                     help="Run full-scale benchmarks (150k rows)")
    parser.addoption("--workers", type=int, default=1,
                     help="Number of server workers")
    parser.addoption("--clients", type=int, default=1,
                     help="Number of concurrent clients for parallel benchmarks")
    parser.addoption("--perf", action="store_true", default=False,
                     help="Enable perf record during benchmarks")
    parser.addoption("--perf-dwarf", action="store_true", default=False,
                     help="Enable perf record with --call-graph=dwarf for full userspace stacks")
    parser.addoption("--perf-stat", action="store_true", default=False,
                     help="Enable perf stat during benchmarks")
    parser.addoption("--results-dir", type=str, default=None,
                     help="Override results output directory")


@pytest.fixture(scope="session")
def scale_mode(request) -> str:
    return "full" if request.config.getoption("--full") else "quick"


@pytest.fixture(scope="session")
def num_clients(request) -> int:
    return request.config.getoption("--clients")


@pytest.fixture(scope="session")
def num_workers(request) -> int:
    return request.config.getoption("--workers")


@pytest.fixture(scope="session")
def server(request, results_dir):
    """Start gnitz-server-release, yield (socket_path, server_pid)."""
    workers = request.config.getoption("--workers")

    binary = os.environ.get("GNITZ_SERVER_BIN")
    if not binary:
        release_bin = REPO_ROOT / "gnitz-server-release"
        debug_bin = REPO_ROOT / "gnitz-server"
        if release_bin.is_file():
            binary = str(release_bin)
        elif debug_bin.is_file():
            binary = str(debug_bin)
    if not binary or not os.path.isfile(binary):
        pytest.skip(f"Server binary not found (looked for gnitz-server-release)")

    tmpdir = Path(
        __import__("tempfile").mkdtemp(
            dir=REPO_ROOT / "tmp", prefix="bench_"
        )
    )
    data_dir = tmpdir / "data"
    sock_path = tmpdir / "gnitz.sock"
    log_path = tmpdir / "server.log"

    cmd = [binary, str(data_dir), str(sock_path), f"--workers={workers}"]
    log_f = open(log_path, "w")
    proc = subprocess.Popen(cmd, stdout=log_f, stderr=log_f)

    for _ in range(100):
        if sock_path.exists():
            break
        time.sleep(0.1)
    else:
        proc.kill()
        proc.wait()
        log_f.close()
        shutil.rmtree(tmpdir, ignore_errors=True)
        pytest.fail("Benchmark server did not start within 10s")

    perf_recorder = None
    if request.config.getoption("--perf") or request.config.getoption("--perf-dwarf"):
        from helpers.perf import PerfRecorder
        dwarf = request.config.getoption("--perf-dwarf")
        perf_recorder = PerfRecorder(proc.pid, results_dir, dwarf=dwarf)
        perf_recorder.start()

    perf_stat = None
    if request.config.getoption("--perf-stat"):
        from helpers.perf import PerfStat
        perf_stat = PerfStat(proc.pid)
        perf_stat.start()

    yield str(sock_path), proc.pid, proc

    if perf_recorder:
        perf_recorder.stop()
        perf_recorder.flamegraph()
    if perf_stat:
        hw_counters = perf_stat.stop()
        # Store for result writing
        request.config._perf_stat = hw_counters

    proc.kill()
    proc.wait()
    log_f.close()

    # Preserve forensic evidence on failure. The SAL file is huge (~1 GB
    # per worker) so drop it — but server.log and every worker_*.log go
    # into results_dir next to the failing-test summary, so diagnosis
    # doesn't require rerunning with GNITZ_KEEP_BENCH_TMPDIR.
    if request.session.testsfailed == 0:
        shutil.rmtree(tmpdir, ignore_errors=True)
    else:
        try:
            shutil.copy(log_path, results_dir / f"server_{tmpdir.name}.log")
            for wl in data_dir.glob("worker_*.log"):
                shutil.copy(wl, results_dir / f"{tmpdir.name}_{wl.name}")
        except OSError as e:
            print(f"warning: failed to copy forensic logs: {e}")
        # Drop the giant SAL mmap file(s) — logs are what we actually need.
        for sal in data_dir.rglob("wal.sal"):
            try:
                sal.unlink()
            except OSError:
                pass
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture(scope="session")
def socket_path(server) -> str:
    return server[0]


@pytest.fixture(scope="session")
def server_pid(server) -> int:
    return server[1]


@pytest.fixture(scope="session")
def _server_proc(server):
    return server[2]


@pytest.fixture(autouse=True)
def _check_server_alive(_server_proc):
    """Skip remaining tests if the server has crashed."""
    rc = _server_proc.poll()
    if rc is not None:
        pytest.skip(
            f"server exited (code={rc}) — a prior test likely crashed it"
        )


@pytest.fixture(scope="session")
def results_dir(request) -> Path:
    override = request.config.getoption("--results-dir")
    if override:
        d = Path(override)
    else:
        workers = request.config.getoption("--workers")
        clients = request.config.getoption("--clients")
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        d = REPO_ROOT / "benchmarks" / "results" / f"{ts}_w{workers}_c{clients}"
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.fixture(scope="session", autouse=True)
def write_results(results_dir, request):
    """Session finalizer: write summary.json + timings.csv from all BenchResults."""
    yield

    results = get_all_results()
    if not results:
        return

    workers = request.config.getoption("--workers")
    clients = request.config.getoption("--clients")
    scale = "full" if request.config.getoption("--full") else "quick"

    # Get commit info
    commit = "unknown"
    dirty = False
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT, text=True,
        ).strip()
        dirty = bool(subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=REPO_ROOT, text=True,
        ).strip())
    except Exception:
        pass

    summary = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "commit": commit,
        "dirty": dirty,
        "workers": workers,
        "clients": clients,
        "scale": scale,
        "benchmarks": [
            {
                "name": r.name,
                "category": r.category,
                "elapsed_s": r.elapsed_s,
                "rows": r.rows,
                "rows_per_sec": r.rows_per_sec,
                "iterations": r.iterations,
                "p50_ms": r.p50_ms,
                "p90_ms": r.p90_ms,
                "p99_ms": r.p99_ms,
                "num_clients": r.num_clients,
            }
            for r in results
        ],
    }

    with open(results_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    with open(results_dir / "timings.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "name", "category", "elapsed_s", "rows", "rows_per_sec",
            "iterations", "p50_ms", "p90_ms", "p99_ms", "num_clients",
        ])
        for r in results:
            writer.writerow([
                r.name, r.category, r.elapsed_s, r.rows, r.rows_per_sec,
                r.iterations, r.p50_ms, r.p90_ms, r.p99_ms, r.num_clients,
            ])


def pytest_configure(config):
    """Set multiprocessing start method for Linux fork-based parallelism."""
    try:
        multiprocessing.set_start_method("fork")
    except RuntimeError:
        pass  # already set
