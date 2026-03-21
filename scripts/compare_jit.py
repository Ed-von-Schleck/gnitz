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
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Repository layout
# ---------------------------------------------------------------------------
REPO_ROOT      = Path(__file__).resolve().parent.parent
CLIENT_DIR     = REPO_ROOT / "rust_client" / "gnitz-py"
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

# ---------------------------------------------------------------------------
# Workload (identical to perf_profile.py _WORKLOAD)
# ---------------------------------------------------------------------------
_WORKLOAD = r"""
import sys, time, json
import gnitz

sock_path     = sys.argv[1]
ticks         = int(sys.argv[2])
rows_per_tick = int(sys.argv[3])

NUM_CUSTOMERS = 500
NUM_PRODUCTS  = 100
NUM_REGIONS   = 10
NUM_BLOCKED   = 50

AGG_COUNT = 1
AGG_SUM   = 2

act_cols = [
    gnitz.ColumnDef("pk",         gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("product_id", gnitz.TypeCode.I64),
    gnitz.ColumnDef("region_id",  gnitz.TypeCode.I64),
    gnitz.ColumnDef("quantity",   gnitz.TypeCode.I64),
    gnitz.ColumnDef("revenue",    gnitz.TypeCode.I64),
]
cust_cols = [
    gnitz.ColumnDef("pk",        gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("region_id", gnitz.TypeCode.I64),
    gnitz.ColumnDef("tier",      gnitz.TypeCode.I64),
]
attrs_cols = [
    gnitz.ColumnDef("pk",     gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("budget", gnitz.TypeCode.I64),
]
blocked_cols = [
    gnitz.ColumnDef("pk",     gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("reason", gnitz.TypeCode.I64),
]

act_schema     = gnitz.Schema(act_cols)
cust_schema    = gnitz.Schema(cust_cols)
attrs_schema   = gnitz.Schema(attrs_cols)
blocked_schema = gnitz.Schema(blocked_cols)

enriched_cols = [
    gnitz.ColumnDef("pk",          gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("product_id",  gnitz.TypeCode.I64),
    gnitz.ColumnDef("region_id",   gnitz.TypeCode.I64),
    gnitz.ColumnDef("quantity",    gnitz.TypeCode.I64),
    gnitz.ColumnDef("revenue",     gnitz.TypeCode.I64),
    gnitz.ColumnDef("cust_region", gnitz.TypeCode.I64),
    gnitz.ColumnDef("tier",        gnitz.TypeCode.I64),
]
enriched2_cols = enriched_cols + [gnitz.ColumnDef("budget", gnitz.TypeCode.I64)]

def reduce2_cols(grp_name):
    return [
        gnitz.ColumnDef("pk",      gnitz.TypeCode.U128, primary_key=True),
        gnitz.ColumnDef(grp_name,  gnitz.TypeCode.I64),
        gnitz.ColumnDef("total",   gnitz.TypeCode.I64),
    ]

result = {"tick_build": [], "tick_push": [], "scan": {}}

with gnitz.connect(sock_path) as conn:
    t0 = time.perf_counter()
    sn = "bench"
    conn.create_schema(sn)

    act_tid     = conn.create_table(sn, "activities", act_cols,     unique_pk=False)
    cust_tid    = conn.create_table(sn, "customers",  cust_cols)
    attrs_tid   = conn.create_table(sn, "cust_attrs", attrs_cols)
    blocked_tid = conn.create_table(sn, "blocked",    blocked_cols)

    conn.execute_sql("CREATE INDEX ON activities(product_id)", schema_name=sn)
    conn.execute_sql("CREATE INDEX ON activities(region_id)",  schema_name=sn)

    cb_batch = gnitz.ZSetBatch(cust_schema)
    ab_batch = gnitz.ZSetBatch(attrs_schema)
    for cid in range(NUM_CUSTOMERS):
        cb_batch.append(pk=cid, region_id=cid % NUM_REGIONS, tier=cid % 3)
        ab_batch.append(pk=cid, budget=1000 + (cid % 5) * 200)
    conn.push(cust_tid,  cb_batch)
    conn.push(attrs_tid, ab_batch)

    bl_batch = gnitz.ZSetBatch(blocked_schema)
    for cid in range(NUM_BLOCKED):
        bl_batch.append(pk=cid, reason=1)
    conn.push(blocked_tid, bl_batch)

    eb = gnitz.ExprBuilder()
    r1 = eb.load_col_int(4); r2 = eb.load_const(800)
    pred_high_rev = eb.build(result_reg=eb.cmp_gt(r1, r2))

    eb = gnitz.ExprBuilder()
    r1 = eb.load_col_int(3); r2 = eb.load_const(7)
    pred_high_qty = eb.build(result_reg=eb.cmp_gt(r1, r2))

    eb = gnitz.ExprBuilder()
    r1 = eb.load_col_int(6); r2 = eb.load_const(2)
    pred_enterprise = eb.build(result_reg=eb.cmp_eq(r1, r2))

    eb = gnitz.ExprBuilder()
    r1 = eb.load_col_int(2); r2 = eb.load_const(5_000_000)
    pred_hot = eb.build(result_reg=eb.cmp_gt(r1, r2))

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.join(cb.input_delta(), cust_tid))
    vid_enriched = conn.create_view_with_circuit(sn, "v_enriched", cb.build(), enriched_cols)

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.join(cb.join(cb.input_delta(), cust_tid), attrs_tid))
    vid_enriched2 = conn.create_view_with_circuit(sn, "v_enriched2", cb.build(), enriched2_cols)

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.semi_join(cb.input_delta(), cust_tid))
    vid_active = conn.create_view_with_circuit(sn, "v_active", cb.build(), act_cols)

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.anti_join(cb.input_delta(), blocked_tid))
    vid_unblocked = conn.create_view_with_circuit(sn, "v_unblocked", cb.build(), act_cols)

    cb = conn.circuit_builder(source_table_id=vid_enriched)
    cb.sink(cb.reduce(cb.input_delta(), group_by_cols=[2], agg_func_id=AGG_SUM, agg_col_idx=4))
    vid_rev_region = conn.create_view_with_circuit(sn, "v_rev_region", cb.build(), reduce2_cols("region_id"))

    cb = conn.circuit_builder(source_table_id=vid_enriched)
    cb.sink(cb.reduce(cb.input_delta(), group_by_cols=[6], agg_func_id=AGG_SUM, agg_col_idx=4))
    vid_rev_tier = conn.create_view_with_circuit(sn, "v_rev_tier", cb.build(), reduce2_cols("tier"))

    cb = conn.circuit_builder(source_table_id=vid_enriched)
    inp = cb.input_delta()
    cb.sink(cb.reduce(cb.filter(inp, pred_enterprise), group_by_cols=[2], agg_func_id=AGG_COUNT, agg_col_idx=4))
    vid_premium_cnt = conn.create_view_with_circuit(sn, "v_premium_cnt", cb.build(), reduce2_cols("region_id"))

    cb = conn.circuit_builder(source_table_id=vid_rev_region)
    cb.sink(cb.filter(cb.input_delta(), pred_hot))
    vid_hot = conn.create_view_with_circuit(sn, "v_hot_regions", cb.build(), reduce2_cols("region_id"))

    cb  = conn.circuit_builder(source_table_id=act_tid)
    inp = cb.input_delta()
    cb.sink(cb.distinct(cb.union(cb.filter(inp, pred_high_rev), cb.filter(inp, pred_high_qty))))
    vid_flagged = conn.create_view_with_circuit(sn, "v_flagged", cb.build(), act_cols)

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.reduce(cb.input_delta(), group_by_cols=[2], agg_func_id=AGG_COUNT, agg_col_idx=4))
    vid_region_cnt = conn.create_view_with_circuit(sn, "v_region_cnt", cb.build(), reduce2_cols("region_id"))

    result["setup_s"] = time.perf_counter() - t0

    for tick in range(ticks):
        t_b0  = time.perf_counter()
        batch = gnitz.ZSetBatch(act_schema)

        if tick >= 5 and tick % 5 == 0:
            base_r    = (tick - 5) * rows_per_tick
            n_retract = max(1, rows_per_tick // 100)
            for i in range(n_retract):
                cid = (base_r + i) % NUM_CUSTOMERS
                pid = (base_r + i) % NUM_PRODUCTS
                rid = cid % NUM_REGIONS
                batch.append(pk=cid, product_id=pid, region_id=rid,
                             quantity=1 + (i % 10), revenue=100 + (i % 1000), weight=-1)

        base = tick * rows_per_tick
        for i in range(rows_per_tick):
            cid = (base + i) % NUM_CUSTOMERS
            pid = (base + i) % NUM_PRODUCTS
            rid = cid % NUM_REGIONS
            batch.append(pk=cid, product_id=pid, region_id=rid,
                         quantity=1 + (i % 10), revenue=100 + (i % 1000))

        result["tick_build"].append(round(time.perf_counter() - t_b0, 4))
        t_p0 = time.perf_counter()
        conn.push(act_tid, batch)
        result["tick_push"].append(round(time.perf_counter() - t_p0, 4))

    view_ids = {
        "v_enriched":    vid_enriched,
        "v_enriched2":   vid_enriched2,
        "v_active":      vid_active,
        "v_unblocked":   vid_unblocked,
        "v_rev_region":  vid_rev_region,
        "v_rev_tier":    vid_rev_tier,
        "v_premium_cnt": vid_premium_cnt,
        "v_hot_regions": vid_hot,
        "v_flagged":     vid_flagged,
        "v_region_cnt":  vid_region_cnt,
    }
    for vname, vid in view_ids.items():
        t_s0     = time.perf_counter()
        rows_out = list(conn.scan(vid))
        t_scan   = time.perf_counter() - t_s0
        live     = sum(1 for r in rows_out if r.weight > 0)
        result["scan"][vname] = {"time_s": round(t_scan, 4), "groups": live}

    t_s0 = time.perf_counter()
    for cid in range(0, NUM_CUSTOMERS, 50):
        conn.seek(cust_tid, pk=cid)
    for pid in range(0, NUM_PRODUCTS, 10):
        conn.seek_by_index(act_tid, col_idx=1, key=pid)
    for rid in range(NUM_REGIONS):
        conn.seek_by_index(act_tid, col_idx=2, key=rid)
    result["seek_s"] = round(time.perf_counter() - t_s0, 4)

print(json.dumps(result))
"""


# ---------------------------------------------------------------------------
# Server lifecycle (same as perf_profile.py)
# ---------------------------------------------------------------------------
def start_server(binary: Path, workers: int):
    tmpdir    = tempfile.mkdtemp(dir=REPO_ROOT / "tmp", prefix="compare_")
    data_dir  = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    log_path  = os.path.join(tmpdir, "server.log")

    cmd   = [str(binary), data_dir, sock_path, f"--workers={workers}"]
    log_f = open(log_path, "w")
    proc  = subprocess.Popen(cmd, stdout=log_f, stderr=log_f)

    for _ in range(100):
        if os.path.exists(sock_path):
            break
        time.sleep(0.1)
    else:
        proc.kill(); proc.wait(); log_f.close()
        shutil.rmtree(tmpdir, ignore_errors=True)
        sys.exit(f"Server {binary.name} did not start within 10s.  Log: {log_path}")

    return proc, sock_path, tmpdir, log_f


class WorkloadError(Exception):
    def __init__(self, stderr: str, server_log: str):
        self.stderr     = stderr
        self.server_log = server_log
    def __str__(self) -> str:
        return self.stderr


def stop_server(proc, tmpdir: str, log_f) -> None:
    proc.kill(); proc.wait(); log_f.close()
    shutil.rmtree(tmpdir, ignore_errors=True)


def run_workload(sock_path: str, ticks: int, rows: int) -> dict:
    proc = subprocess.run(
        ["uv", "run", "python", "-c", _WORKLOAD, sock_path, str(ticks), str(rows)],
        cwd=CLIENT_DIR,
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise WorkloadError(proc.stderr, "")   # server_log filled in by caller
    return json.loads(proc.stdout.strip())


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
                     out_dir: Path) -> dict:
    """Start server, run workload, stop server.

    Returns timing dict on success.
    On failure: saves server log to out_dir, prints diagnostics, raises WorkloadError.
    """
    print(f"  [{label}] run {run_idx}/{total_runs} …", end="", flush=True)
    slug = label.lower().replace(" ", "_").replace("-", "_")
    proc, sock_path, tmpdir, log_f = start_server(binary, workers)
    try:
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
        # Save to out_dir
        _save_and_print_logs(out_dir, slug, run_idx, e.stderr, server_log, worker_logs)
        raise WorkloadError(e.stderr, server_log)
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
    notes = {
        "v_enriched":    "join",
        "v_enriched2":   "chained join (3-table)",
        "v_active":      "semi_join",
        "v_unblocked":   "anti_join",
        "v_rev_region":  "view-on-view reduce",
        "v_rev_tier":    "reduce (fan-out)",
        "v_premium_cnt": "filter+reduce",
        "v_hot_regions": "3rd-hop filter",
        "v_flagged":     "union+distinct",
        "v_region_cnt":  "direct reduce",
    }
    for vname in jit["scan"]:
        j_t = jit["scan"][vname]["time_s"]
        n_t = nojit["scan"][vname]["time_s"]
        sp  = n_t / j_t if j_t > 0 else float("inf")
        note = notes.get(vname, "")
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
    ap.add_argument("--ticks",    type=int, default=10,
                    help="Push ticks per run (default: 10).")
    ap.add_argument("--rows",     type=int, default=50_000,
                    help="Rows per tick (default: 50,000).")
    ap.add_argument("--runs",     type=int, default=1,
                    help="Runs per binary; median is used (default: 1).  Use 3+ for stability.")
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
    print(f"  Workers : {args.workers}")
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
                                 "JIT", i + 1, args.runs, out_dir)
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
                                 "No-JIT", i + 1, args.runs, out_dir)
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
    print()
    print(f"  {dim('Results saved to:')} {out_dir}")
    print()


if __name__ == "__main__":
    main()
