#!/usr/bin/env python3
"""
compare_jit.py — Compare JIT vs non-JIT release server performance.

QUICK START
===========
From the repo root:

    python scripts/compare_jit.py                    # run comparison (both binaries must exist)
    python scripts/compare_jit.py --rebuild          # rebuild both binaries first (~8 min total)
    python scripts/compare_jit.py --rebuild-jit      # rebuild only JIT binary
    python scripts/compare_jit.py --rebuild-nojit    # rebuild only non-JIT binary
    python scripts/compare_jit.py --ticks 20         # longer run → more stable numbers
    python scripts/compare_jit.py --runs 3           # run each server 3 times, use median

WHAT THIS DOES
==============
Runs the same benchmark workload (N ticks × M rows, 10 views) sequentially against:

  JIT binary    : gnitz-server-release-c   (--opt=jit --gc=incminimark --lto)
  Non-JIT binary: gnitz-server-nojit-c     (--opt=2   --gc=incminimark --lto)

Both use the same worker count, tick count, and row count.
Results are printed as a side-by-side table and saved to tmp/compare_runs/<timestamp>/.

INTERPRETING RESULTS
====================
The JIT build includes PyPy's tracing JIT compiler. For a server workload that
repeats the same hot loops (reduce, join, ingest) many times per tick, the JIT
warms up quickly and produces machine code tuned to the actual trace.

The non-JIT build (--opt=2) applies all RPython optimisations (inlining, escape
analysis, etc.) but generates static C — no adaptive recompilation at runtime.

Expected outcome:
  - First few ticks: JIT may be slower (compilation overhead)
  - Steady state (tick 3+): JIT typically 2–5× faster on tight loops
  - If JIT speedup is <1.5× the hot loop is spending most time in C libraries
    (glibc sort, memmove) which are already optimal; JIT has nothing to compile

PREREQUISITES
=============
  - Both binaries present (or pass --rebuild / --rebuild-jit / --rebuild-nojit)
  - uv with the gnitz-py project:
        cd rust_client/gnitz-py && uv sync
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from workload import (
    REPO_ROOT, CLIENT_DIR, start_server, stop_server, run_workload,
    run_realistic_workload, save_to_history, VIEW_NOTES, WorkloadError,
)

# ---------------------------------------------------------------------------
# Repository layout
# ---------------------------------------------------------------------------
BINARY_JIT     = REPO_ROOT / "gnitz-server-release-c"
BINARY_NOJIT   = REPO_ROOT / "gnitz-server-nojit-c"
RUNS_DIR       = REPO_ROOT / "tmp" / "compare_runs"

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
_COLOUR = sys.stdout.isatty()

def _c(code: str, s: str) -> str:
    return f"\033[{code}m{s}\033[0m" if _COLOUR else s

def bold(s: str)    -> str: return _c("1",  s)
def dim(s: str)     -> str: return _c("2",  s)
def green(s: str)   -> str: return _c("32", s)
def yellow(s: str)  -> str: return _c("33", s)
def red(s: str)     -> str: return _c("31", s)
def cyan(s: str)    -> str: return _c("36", s)

# (workload, server lifecycle, and run_workload imported from workload.py)


def _read_file(path: str) -> str:
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return ""


def _save_and_print_logs(out_dir: Path, slug: str, run_idx: int,
                          stderr: str, server_log: str,
                          worker_logs: dict) -> None:
    print(red("  FAILED"))
    print(red(f"\n  Workload error:\n{stderr.rstrip()}"), file=sys.stderr)
    if server_log.strip():
        saved = out_dir / f"{slug}_run{run_idx}_server.log"
        try:
            saved.write_text(server_log)
        except OSError:
            pass
        print(red(f"\n  Server log ({saved}):"), file=sys.stderr)
        for line in server_log.strip().splitlines()[-10:]:
            print(f"    {line}", file=sys.stderr)
    for n, wtext in sorted(worker_logs.items()):
        saved = out_dir / f"{slug}_run{run_idx}_worker{n}.log"
        try:
            saved.write_text(wtext)
        except OSError:
            pass
        print(red(f"\n  Worker {n} log ({saved}):"), file=sys.stderr)
        for line in wtext.strip().splitlines()[-20:]:
            print(f"    {line}", file=sys.stderr)


def run_server_trial(binary: Path, workers: int, ticks: int, rows: int,
                     label: str, run_idx: int, total_runs: int,
                     out_dir: Path, *,
                     realistic: bool = False, clients: int = 4) -> dict:
    """Start server, run workload, stop server.

    Returns timing dict on success.
    On failure: saves server log to out_dir, prints diagnostics, raises WorkloadError.
    """
    print(f"  [{label}] run {run_idx}/{total_runs} …", end="", flush=True)
    slug = label.lower().replace(" ", "_").replace("-", "_")
    proc, sock_path, tmpdir, log_f = start_server(
        binary, workers, tmpdir_prefix="compare_")
    try:
        if realistic:
            result = run_realistic_workload(sock_path, ticks, rows, clients)
        else:
            result = run_workload(sock_path, ticks, rows)
    except WorkloadError as e:
        # Read all logs before stop_server deletes tmpdir
        log_f.flush()
        server_log = _read_file(os.path.join(tmpdir, "server.log"))
        worker_logs = {}
        for n in range(workers):
            wtext = _read_file(os.path.join(tmpdir, "data", f"worker_{n}.log"))
            if wtext.strip():
                worker_logs[n] = wtext
        stop_server(proc, tmpdir, log_f)
        _save_and_print_logs(out_dir, slug, run_idx, str(e), server_log, worker_logs)
        raise
    else:
        stop_server(proc, tmpdir, log_f)

    total_push = sum(result["tick_push"])
    print(f"  total push: {total_push:.3f}s")
    return result


# ---------------------------------------------------------------------------
# Statistics helpers
# ---------------------------------------------------------------------------
def _median_result(results: list[dict]) -> dict:
    """Return the run whose total_push is closest to the median."""
    totals = [sum(r["tick_push"]) for r in results]
    med    = statistics.median(totals)
    best   = min(range(len(totals)), key=lambda i: abs(totals[i] - med))
    return results[best]


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------
_SEP = "─" * 76

def _section(title: str) -> None:
    print()
    print(bold(cyan(_SEP)))
    print(bold(cyan(f"  {title}")))
    print(bold(cyan(_SEP)))


def _speedup_colour(x: float) -> str:
    if x >= 2.0:
        return green(f"{x:.2f}×")
    if x >= 1.2:
        return yellow(f"{x:.2f}×")
    if x < 0.9:
        return red(f"{x:.2f}×")
    return f"{x:.2f}×"


def print_comparison(jit: dict, nojit: dict, ticks: int, rows: int) -> None:
    _section("PER-TICK PUSH LATENCY  (ms)")

    jit_push   = jit["tick_push"]
    nojit_push = nojit["tick_push"]

    print(f"  {'Tick':>4}  {'JIT (ms)':>10}  {'No-JIT (ms)':>12}  {'Speedup':>8}  Note")
    print(f"  {'----':>4}  {'--------':>10}  {'-----------':>12}  {'-------':>8}  ----")

    for i, (j, n) in enumerate(zip(jit_push, nojit_push)):
        speedup = n / j if j > 0 else float("inf")
        note    = dim("(JIT warmup)") if i < 2 else ""
        print(f"  {i+1:>4}  {j*1000:>9.0f}ms  {n*1000:>11.0f}ms  "
              f"{_speedup_colour(speedup):>8}  {note}")

    _section("SUMMARY")

    total_jit   = sum(jit_push)
    total_nojit = sum(nojit_push)
    speedup_push = total_nojit / total_jit if total_jit > 0 else float("inf")

    total_scan_jit   = sum(v["time_s"] for v in jit["scan"].values())
    total_scan_nojit = sum(v["time_s"] for v in nojit["scan"].values())
    speedup_scan = total_scan_nojit / total_scan_jit if total_scan_jit > 0 else float("inf")

    total_rows = ticks * rows
    tput_jit   = total_rows / total_jit   if total_jit   > 0 else 0
    tput_nojit = total_rows / total_nojit if total_nojit > 0 else 0

    rows_w = 14
    print(f"  {'Metric':<26}  {'JIT':>{rows_w}}  {'No-JIT':>{rows_w}}  {'Speedup':>8}")
    print(f"  {'------':<26}  {'-'*rows_w:>{rows_w}}  {'-'*rows_w:>{rows_w}}  {'-------':>8}")

    def _row(label, jv, nv, fmt, speedup=None):
        if speedup is None:
            sp = nv / jv if jv > 0 else float("inf")
        else:
            sp = speedup
        print(f"  {label:<26}  {fmt.format(jv):>{rows_w}}  {fmt.format(nv):>{rows_w}}  "
              f"{_speedup_colour(sp):>8}")

    _row("Setup",          jit["setup_s"] * 1000,   nojit["setup_s"] * 1000,   "{:.0f}ms")
    _row("Total push",     total_jit,                total_nojit,               "{:.3f}s",
         speedup=speedup_push)
    _row("Total scan",     total_scan_jit,           total_scan_nojit,          "{:.3f}s",
         speedup=speedup_scan)
    _row("Seek/index",     jit["seek_s"] * 1000,     nojit["seek_s"] * 1000,    "{:.0f}ms")
    _row("Throughput",     tput_jit,                 tput_nojit,                "{:,.0f} rows/s",
         speedup=tput_jit / tput_nojit if tput_nojit > 0 else float("inf"))

    _section("PER-VIEW SCAN TIME")
    print(f"  {'View':<20}  {'JIT':>8}  {'No-JIT':>8}  {'Speedup':>8}  Note")
    print(f"  {'----':<20}  {'---':>8}  {'------':>8}  {'-------':>8}  ----")
    for vname in jit["scan"]:
        j_t = jit["scan"][vname]["time_s"]
        n_t = nojit["scan"][vname]["time_s"]
        sp  = n_t / j_t if j_t > 0 else float("inf")
        note = VIEW_NOTES.get(vname, "")
        print(f"  {vname:<20}  {j_t*1000:>7.0f}ms  {n_t*1000:>7.0f}ms  "
              f"{_speedup_colour(sp):>8}  {dim(note)}")

    print()
    print(f"  Overall push speedup  (JIT vs no-JIT): {bold(_speedup_colour(speedup_push))}")
    if speedup_push >= 2.0:
        print(f"  {green('JIT is delivering significant speedup on tight loops.')}")
    elif speedup_push >= 1.2:
        print(f"  {yellow('Moderate JIT speedup — some hot code is already in C libraries.')}")
    else:
        print(f"  {dim('Minimal JIT speedup — hot path is dominated by library calls (memmove etc.).')}")

    # Realistic workload stats
    if "realistic" in jit and "realistic" in nojit:
        _section("REALISTIC WORKLOAD LATENCY")
        jp = jit["realistic"]["push_latency_ms"]
        np_ = nojit["realistic"]["push_latency_ms"]
        print(f"  {'Percentile':<14}  {'JIT (ms)':>10}  {'No-JIT (ms)':>12}  {'Speedup':>8}")
        print(f"  {'-'*14:<14}  {'-'*10:>10}  {'-'*12:>12}  {'-'*8:>8}")
        for key in ["p50", "p90", "p95", "p99", "mean"]:
            jv, nv = jp[key], np_[key]
            sp = nv / jv if jv > 0 else float("inf")
            print(f"  {key:<14}  {jv:>9.1f}ms  {nv:>11.1f}ms  {_speedup_colour(sp):>8}")
        js = jit["realistic"]["concurrent_scan_latency_ms"]
        ns = nojit["realistic"]["concurrent_scan_latency_ms"]
        print()
        print(f"  Concurrent scans: JIT={js['count']}  No-JIT={ns['count']}")
        if js["p50"] > 0 and ns["p50"] > 0:
            sp = ns["p50"] / js["p50"]
            print(f"  Scan p50: JIT={js['p50']:.1f}ms  No-JIT={ns['p50']:.1f}ms  ({_speedup_colour(sp)})")


# ---------------------------------------------------------------------------
# Prerequisite checks
# ---------------------------------------------------------------------------
def check_prerequisites(rebuild_jit: bool, rebuild_nojit: bool) -> None:
    errs = []
    if not rebuild_jit and not BINARY_JIT.exists():
        errs.append(
            f"JIT binary not found: {BINARY_JIT}\n"
            "  Fix: python scripts/compare_jit.py --rebuild-jit"
        )
    if not rebuild_nojit and not BINARY_NOJIT.exists():
        errs.append(
            f"Non-JIT binary not found: {BINARY_NOJIT}\n"
            "  Fix: python scripts/compare_jit.py --rebuild-nojit"
        )
    if not CLIENT_DIR.exists():
        errs.append(f"Client directory not found: {CLIENT_DIR}")
    if errs:
        print(red("Prerequisite check failed:"), file=sys.stderr)
        for e in errs:
            print(f"  • {e}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Compare JIT vs non-JIT GnitzDB server performance.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--rebuild",       action="store_true",
                    help="Rebuild both binaries before comparing.")
    ap.add_argument("--rebuild-jit",   action="store_true",
                    help="Rebuild only the JIT binary (make release-server).")
    ap.add_argument("--rebuild-nojit", action="store_true",
                    help="Rebuild only the non-JIT binary (make release-server-nojit).")
    ap.add_argument("--workers",  type=int, default=4,
                    help="Worker processes (default: 4).")
    ap.add_argument("--ticks",    type=int, default=2,
                    help="Push ticks per run (default: 2).")
    ap.add_argument("--rows",     type=int, default=10_000,
                    help="Rows per tick (default: 10,000).")
    ap.add_argument("--runs",     type=int, default=1,
                    help="Runs per binary; median is used (default: 1).  Use 3+ for stability.")
    ap.add_argument("--realistic", action="store_true",
                    help="Use realistic multi-client workload instead of throughput benchmark.")
    ap.add_argument("--clients", type=int, default=4,
                    help="Number of concurrent client processes (--realistic only, default: 4).")
    args = ap.parse_args()

    rebuild_jit   = args.rebuild or args.rebuild_jit
    rebuild_nojit = args.rebuild or args.rebuild_nojit

    check_prerequisites(rebuild_jit, rebuild_nojit)

    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = RUNS_DIR / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    total_rows = args.ticks * args.rows

    print()
    print(bold(cyan("═" * 76)))
    print(bold(cyan(f"  GnitzDB JIT vs Non-JIT Comparison — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")))
    print(bold(cyan("═" * 76)))
    jit_mtime   = datetime.fromtimestamp(BINARY_JIT.stat().st_mtime).strftime("%Y-%m-%d %H:%M") if BINARY_JIT.exists() else "?"
    nojit_mtime = datetime.fromtimestamp(BINARY_NOJIT.stat().st_mtime).strftime("%Y-%m-%d %H:%M") if BINARY_NOJIT.exists() else "?"
    print(f"  JIT     : {BINARY_JIT.name}   (--opt=jit --gc=incminimark --lto)  built {jit_mtime}")
    print(f"  No-JIT  : {BINARY_NOJIT.name}  (--opt=2   --gc=incminimark --lto)  built {nojit_mtime}")
    mode_s = f"realistic ({args.clients} clients)" if args.realistic else "throughput"
    print(f"  Workers : {args.workers}")
    print(f"  Mode    : {mode_s}")
    print(f"  Workload: {args.ticks} ticks × {args.rows:,} rows = {total_rows:,} rows total")
    print(f"  Runs    : {args.runs} per binary{'  (median reported)' if args.runs > 1 else ''}")
    print(f"  Output  : {out_dir}")
    print()

    # Rebuild
    if rebuild_jit:
        print(bold("Building JIT binary (make release-server) …"))
        r = subprocess.run(["make", "release-server"], cwd=REPO_ROOT)
        if r.returncode != 0:
            sys.exit("make release-server failed.")
        print(green("  Done."))
        print()

    if rebuild_nojit:
        print(bold("Building non-JIT binary (make release-server-nojit) …"))
        r = subprocess.run(["make", "release-server-nojit"], cwd=REPO_ROOT)
        if r.returncode != 0:
            sys.exit("make release-server-nojit failed.")
        print(green("  Done."))
        print()

    # Run JIT trials
    _section(f"RUNNING JIT SERVER  ({args.runs} run{'s' if args.runs > 1 else ''})")
    jit_results = []
    jit_failed  = False
    for i in range(args.runs):
        try:
            jit_results.append(
                run_server_trial(BINARY_JIT, args.workers, args.ticks, args.rows,
                                 "JIT", i + 1, args.runs, out_dir,
                                 realistic=args.realistic, clients=args.clients)
            )
        except WorkloadError:
            jit_failed = True
            break  # no point running more trials if this binary crashes

    # Run non-JIT trials
    _section(f"RUNNING NON-JIT SERVER  ({args.runs} run{'s' if args.runs > 1 else ''})")
    nojit_results = []
    nojit_failed  = False
    for i in range(args.runs):
        try:
            nojit_results.append(
                run_server_trial(BINARY_NOJIT, args.workers, args.ticks, args.rows,
                                 "No-JIT", i + 1, args.runs, out_dir,
                                 realistic=args.realistic, clients=args.clients)
            )
        except WorkloadError:
            nojit_failed = True
            break

    if jit_failed and nojit_failed:
        sys.exit(red("\nBoth binaries failed. Check server logs in: " + str(out_dir)))
    if jit_failed:
        sys.exit(red("\nJIT binary failed. Check server logs in: " + str(out_dir)))
    if nojit_failed:
        sys.exit(red("\nNon-JIT binary failed. Check server logs in: " + str(out_dir)))

    jit_best   = _median_result(jit_results)
    nojit_best = _median_result(nojit_results)

    if args.runs > 1:
        jit_totals   = [sum(r["tick_push"]) for r in jit_results]
        nojit_totals = [sum(r["tick_push"]) for r in nojit_results]
        print()
        print(f"  JIT   runs (total push): {[f'{t:.3f}s' for t in jit_totals]}")
        print(f"  No-JIT runs (total push): {[f'{t:.3f}s' for t in nojit_totals]}")
        print(f"  Reporting median run for each.")

    print_comparison(jit_best, nojit_best, args.ticks, args.rows)

    # Save JSON
    report = {
        "timestamp": ts,
        "workers": args.workers,
        "ticks": args.ticks,
        "rows_per_tick": args.rows,
        "runs": args.runs,
        "jit_binary": str(BINARY_JIT),
        "nojit_binary": str(BINARY_NOJIT),
        "jit": jit_best,
        "nojit": nojit_best,
        "jit_all_runs": jit_results,
        "nojit_all_runs": nojit_results,
    }
    (out_dir / "report.json").write_text(json.dumps(report, indent=2))

    # Save to benchmark history (one record per binary)
    mode = "realistic" if args.realistic else "throughput"
    save_to_history(jit_best, mode=mode, ticks=args.ticks, rows=args.rows,
                    workers=args.workers, tool="compare", binary="jit")
    save_to_history(nojit_best, mode=mode, ticks=args.ticks, rows=args.rows,
                    workers=args.workers, tool="compare", binary="nojit")

    print()
    print(f"  {dim('Results saved to:')} {out_dir}")
    print()


if __name__ == "__main__":
    main()
