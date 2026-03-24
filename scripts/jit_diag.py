#!/usr/bin/env python3
"""
jit_diag.py — JIT tracing diagnostics for the GnitzDB release server.

QUICK START
===========
From the repo root:

    python scripts/jit_diag.py                    # summary: loops, bridges, aborts
    python scripts/jit_diag.py --traces            # + dump optimized JIT traces
    python scripts/jit_diag.py --threshold 200     # override GNITZ_JIT threshold
    python scripts/jit_diag.py --ticks 10          # longer run for more JIT warmup
    python scripts/jit_diag.py --rebuild           # rebuild JIT binary first

WHAT THIS DOES
==============
1.  Starts the JIT release server (gnitz-server-release-c) with PYPYLOG set
    to capture jit-summary, jit-log-opt, and jit-abort-log.  Each process
    (master + workers) writes its own log via PYPYLOG's %%d PID expansion.

2.  Runs the standard benchmark workload (10 views, joins, reduces, filters).

3.  Sends SIGTERM to the server and waits for exit (JIT summary is flushed
    on clean shutdown via atexit).

4.  Parses the per-worker JIT logs and prints:
      - Per-worker: compiled loops, bridges, aborts
      - Compiled traces by opcode (mapped to DBSP operator names)
      - Abort details (which opcodes fail, and why)
      - Optionally: full optimized IR for each compiled trace (--traces)

5.  Saves raw logs + parsed report to tmp/jit_diag/<timestamp>/.

INTERPRETING RESULTS
====================
  Compiled loops   — traces the JIT successfully compiled to native code.
  Entry bridges    — straight-line traces (no backward jump); typical when
                     the interpreter loop is short (2-4 opcodes per circuit).
  ABORT_TOO_LONG   — trace exceeded trace_limit (default 6000 ops).  This
                     means the JIT tried to inline a large function and gave
                     up.  Fix: mark callee @jit.dont_look_inside, or remove
                     @jit.unroll_safe from outer function if it has a
                     row-count loop.
  Residual calls   — call_may_force in traces: functions the JIT could not
                     inline.  These are the boundary of what JIT optimizes.
"""

from __future__ import annotations

import argparse
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from workload import (
    REPO_ROOT, CLIENT_DIR, start_server, stop_server,
    run_workload, run_realistic_workload,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BINARY_JIT = REPO_ROOT / "gnitz-server-release-c"

OPCODE_NAMES = {
    0: "HALT",
    1: "FILTER",
    2: "NEGATE",
    3: "MAP",
    4: "UNION",
    5: "JOIN_DELTA_TRACE",
    6: "JOIN_DELTA_DELTA",
    7: "INTEGRATE",
    8: "DELAY",
    9: "REDUCE",
    10: "DISTINCT",
    16: "ANTI_JOIN_DT",
    17: "ANTI_JOIN_DD",
    18: "SEMI_JOIN_DT",
    19: "SEMI_JOIN_DD",
    22: "JOIN_DT_OUTER",
    24: "GATHER_REDUCE",
}

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
_COLOUR = sys.stdout.isatty()

def _c(code: str, s: str) -> str:
    return f"\033[{code}m{s}\033[0m" if _COLOUR else s

def bold(s):   return _c("1", s)
def dim(s):    return _c("2", s)
def green(s):  return _c("32", s)
def yellow(s): return _c("33", s)
def red(s):    return _c("31", s)
def cyan(s):   return _c("36", s)


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------

def parse_jit_log(path: str) -> dict:
    """Parse a single PYPYLOG file.  Returns a summary dict."""
    try:
        with open(path) as f:
            content = f.read()
    except OSError:
        return {"error": f"cannot read {path}"}

    if not content.strip():
        return {"empty": True, "size": 0}

    result = {
        "size": len(content),
        "compiled_loops": [],
        "aborts": [],
        "tracing_attempts": 0,
        "residual_calls": {},
        "opt_traces": [],
    }

    # PYPYLOG format: "[hex_timestamp] {section-name" ... "[hex_timestamp] section-name}"
    # Section markers may appear on lines with other text.

    # Count tracing attempts
    result["tracing_attempts"] = content.count("{jit-tracing")

    # Extract sections by scanning for open/close markers.
    def _extract_sections(tag):
        """Yield content between {tag and tag} markers."""
        open_tag = "{" + tag
        close_tag = tag + "}"
        start = 0
        while True:
            idx = content.find(open_tag, start)
            if idx == -1:
                break
            # Content starts after the line containing the open tag
            line_end = content.find("\n", idx)
            if line_end == -1:
                break
            body_start = line_end + 1
            end = content.find(close_tag, body_start)
            if end == -1:
                break
            # Body ends at the start of the line containing the close tag
            body_end = content.rfind("\n", body_start, end)
            if body_end == -1:
                body_end = end
            yield content[body_start:body_end]
            start = end + len(close_tag)

    # Compiled loops from jit-log-opt-loop sections
    for section in _extract_sections("jit-log-opt-loop"):
        lines = section.strip().splitlines()
        header = lines[0] if lines else ""
        # Extract opcode from header: "# Loop N (PC: X, Opcode: Y) : ..."
        op_m = re.search(r"Opcode: (\d+)", header)
        opcode = int(op_m.group(1)) if op_m else -1
        ops_m = re.search(r"with (\d+) ops", header)
        num_ops = int(ops_m.group(1)) if ops_m else 0
        loop_m = re.search(r"# Loop (\d+)", header)
        loop_id = int(loop_m.group(1)) if loop_m else -1
        trace_type = "bridge" if "bridge" in header else "loop"

        # Count residual calls in this trace
        calls = []
        for line in lines:
            cm = re.search(r"ConstClass\((\w+)\)", line)
            if cm and ("call_may_force" in line or "call_release_gil" in line):
                calls.append(cm.group(1))

        guards = sum(1 for l in lines if "guard_" in l)

        result["compiled_loops"].append({
            "loop_id": loop_id,
            "opcode": opcode,
            "opcode_name": OPCODE_NAMES.get(opcode, f"UNKNOWN({opcode})"),
            "num_ops": num_ops,
            "type": trace_type,
            "residual_calls": calls,
            "guards": guards,
        })
        result["opt_traces"].append({"header": header, "body": section})

        for c in calls:
            result["residual_calls"][c] = result["residual_calls"].get(c, 0) + 1

    # Aborts
    for section in _extract_sections("jit-tracing"):
        if "ABORT" not in section:
            continue
        abort_m = re.search(r"ABORTING TRACING (\S+)", section)
        reason = abort_m.group(1) if abort_m else "UNKNOWN"
        op_m = re.search(r"Opcode: (\d+)", section)
        opcode = int(op_m.group(1)) if op_m else -1
        result["aborts"].append({
            "reason": reason,
            "opcode": opcode,
            "opcode_name": OPCODE_NAMES.get(opcode, f"UNKNOWN({opcode})"),
        })

    # jit-summary (written at clean exit)
    for section in _extract_sections("jit-summary"):
        result["jit_summary_raw"] = section.strip()
        break

    return result


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

_SEP = "─" * 76

def _section(title: str) -> None:
    print()
    print(bold(cyan(_SEP)))
    print(bold(cyan(f"  {title}")))
    print(bold(cyan(_SEP)))


def print_report(worker_results: dict[int, dict], *, show_traces: bool = False) -> str:
    """Print parsed JIT diagnostics.  Returns report text."""
    lines = []

    def pr(s=""):
        print(s)
        lines.append(s)

    # ── Per-worker summary ────────────────────────────────────────────
    _section("PER-WORKER JIT SUMMARY")
    pr(f"  {'PID':>8}  {'Log Size':>10}  {'Traces':>7}  {'Compiled':>9}  {'Aborts':>7}")
    pr(f"  {'---':>8}  {'--------':>10}  {'------':>7}  {'--------':>9}  {'------':>7}")

    all_loops = []
    all_aborts = []
    all_residuals = {}

    for pid in sorted(worker_results.keys()):
        r = worker_results[pid]
        if r.get("empty") or r.get("error"):
            pr(f"  {pid:>8}  {'0 B':>10}  {'-':>7}  {'-':>9}  {'-':>7}  "
               + dim("(no JIT activity — master process?)"))
            continue
        n_compiled = len(r["compiled_loops"])
        n_aborts = len(r["aborts"])
        n_traces = r["tracing_attempts"]
        sz = r["size"]
        sz_s = f"{sz:,} B" if sz < 10000 else f"{sz // 1024:,} KB"
        colour = green if n_aborts == 0 else (yellow if n_aborts < 5 else red)
        pr(f"  {pid:>8}  {sz_s:>10}  {n_traces:>7}  {n_compiled:>9}  "
           f"{colour(str(n_aborts)):>7}")
        all_loops.extend(r["compiled_loops"])
        all_aborts.extend(r["aborts"])
        for name, count in r.get("residual_calls", {}).items():
            all_residuals[name] = all_residuals.get(name, 0) + count

    if not all_loops and not all_aborts:
        pr()
        pr(red("  No JIT activity detected.  The JIT threshold was likely never reached."))
        pr(dim("  Try: --threshold 50  or  --ticks 20"))
        return "\n".join(lines)

    # ── Compiled traces by opcode ─────────────────────────────────────
    _section("COMPILED TRACES BY OPCODE")
    opcode_groups: dict[int, list] = {}
    for loop in all_loops:
        opcode_groups.setdefault(loop["opcode"], []).append(loop)

    pr(f"  {'Opcode':<25}  {'Count':>6}  {'Avg Ops':>8}  {'Avg Guards':>11}  Residual Calls")
    pr(f"  {'------':<25}  {'-----':>6}  {'-------':>8}  {'----------':>11}  --------------")
    for opcode in sorted(opcode_groups.keys()):
        group = opcode_groups[opcode]
        name = OPCODE_NAMES.get(opcode, f"UNKNOWN({opcode})")
        avg_ops = sum(l["num_ops"] for l in group) / len(group)
        avg_guards = sum(l["guards"] for l in group) / len(group)
        calls = set()
        for l in group:
            calls.update(l["residual_calls"])
        calls_s = ", ".join(sorted(calls)) if calls else dim("(none)")
        pr(f"  {name:<25}  {len(group):>6}  {avg_ops:>7.0f}  {avg_guards:>10.0f}  {calls_s}")

    # ── Aborts ────────────────────────────────────────────────────────
    if all_aborts:
        _section("TRACE ABORTS")
        abort_groups: dict[tuple, int] = {}
        for a in all_aborts:
            key = (a["opcode_name"], a["reason"])
            abort_groups[key] = abort_groups.get(key, 0) + 1

        pr(f"  {'Opcode':<25}  {'Reason':<25}  {'Count':>6}")
        pr(f"  {'------':<25}  {'------':<25}  {'-----':>6}")
        for (opname, reason), count in sorted(abort_groups.items(), key=lambda x: -x[1]):
            pr(f"  {red(opname):<25}  {reason:<25}  {count:>6}")
    else:
        _section("TRACE ABORTS")
        pr(green("  No aborts — all tracing attempts succeeded."))

    # ── Residual calls ────────────────────────────────────────────────
    if all_residuals:
        _section("RESIDUAL CALLS  (functions the JIT could not inline)")
        pr(f"  {'Function':<45}  {'Count':>6}")
        pr(f"  {'--------':<45}  {'-----':>6}")
        for name, count in sorted(all_residuals.items(), key=lambda x: -x[1]):
            pr(f"  {name:<45}  {count:>6}")

    # ── jit-summary from any worker that has it ───────────────────────
    for pid in sorted(worker_results.keys()):
        r = worker_results[pid]
        raw = r.get("jit_summary_raw")
        if raw:
            _section(f"JIT SUMMARY (worker PID {pid})")
            for line in raw.splitlines():
                pr(f"  {line}")
            break  # show just one representative worker

    # ── Full traces ───────────────────────────────────────────────────
    if show_traces:
        # Use first non-empty worker
        for pid in sorted(worker_results.keys()):
            r = worker_results[pid]
            if r.get("opt_traces"):
                _section(f"OPTIMIZED TRACES (worker PID {pid})")
                for t in r["opt_traces"]:
                    pr()
                    pr(bold(f"  {t['header']}"))
                    for line in t["body"].splitlines()[1:]:  # skip header
                        pr(f"    {line}")
                break

    pr()
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="JIT tracing diagnostics for GnitzDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--rebuild", action="store_true",
                    help="Rebuild the JIT binary (make server) before running.")
    ap.add_argument("--workers", type=int, default=4,
                    help="Worker processes (default: 4).")
    ap.add_argument("--ticks", type=int, default=5,
                    help="Push ticks (default: 5).")
    ap.add_argument("--rows", type=int, default=10_000,
                    help="Rows per tick (default: 10,000).")
    ap.add_argument("--threshold", type=int, default=None,
                    help="Override GNITZ_JIT threshold (default: server default, 1039).")
    ap.add_argument("--trace-limit", type=int, default=None,
                    help="Override GNITZ_JIT trace_limit (default: 6000).")
    ap.add_argument("--realistic", action="store_true",
                    help="Use realistic multi-client workload instead of throughput.")
    ap.add_argument("--clients", type=int, default=4,
                    help="Concurrent client processes (--realistic only, default: 4).")
    ap.add_argument("--traces", action="store_true",
                    help="Dump full optimized IR for each compiled trace.")
    args = ap.parse_args()

    if not BINARY_JIT.exists() and not args.rebuild:
        print(red(f"JIT binary not found: {BINARY_JIT}"), file=sys.stderr)
        print("  Fix: python scripts/jit_diag.py --rebuild", file=sys.stderr)
        sys.exit(1)

    if args.rebuild:
        print(bold("Building JIT binary (make server) …"))
        r = subprocess.run(["make", "server"], cwd=REPO_ROOT)
        if r.returncode != 0:
            sys.exit("make server failed.")
        print(green("  Done."))
        print()

    # Output directory
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = REPO_ROOT / "tmp" / "jit_diag" / ts
    out_dir.mkdir(parents=True, exist_ok=True)
    log_pattern = str(out_dir / "jit_%d.log")

    # Environment: PYPYLOG + optional GNITZ_JIT
    env = dict(os.environ)
    env["PYPYLOG"] = f"jit:{log_pattern}"

    jit_parts = []
    if args.threshold is not None:
        jit_parts.append(f"threshold={args.threshold}")
        jit_parts.append(f"function_threshold={args.threshold}")
    if args.trace_limit is not None:
        jit_parts.append(f"trace_limit={args.trace_limit}")
    if jit_parts:
        env["GNITZ_JIT"] = ",".join(jit_parts)

    # Print header
    print()
    print(bold(cyan("═" * 76)))
    print(bold(cyan(f"  GnitzDB JIT Diagnostics — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")))
    print(bold(cyan("═" * 76)))
    print(f"  Binary    : {BINARY_JIT.name}")
    print(f"  Workers   : {args.workers}")
    mode_s = f"realistic ({args.clients} clients)" if args.realistic else "throughput"
    print(f"  Workload  : {args.ticks} ticks × {args.rows:,} rows ({mode_s})")
    if jit_parts:
        print(f"  GNITZ_JIT : {env['GNITZ_JIT']}")
    else:
        print(f"  GNITZ_JIT : (default)")
    print(f"  Output    : {out_dir}")
    print()

    # Start server with PYPYLOG
    tmpdir = tempfile.mkdtemp(dir=REPO_ROOT / "tmp", prefix="jitdiag_")
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    log_path = os.path.join(tmpdir, "server.log")

    cmd = [str(BINARY_JIT), data_dir, sock_path, f"--workers={args.workers}"]
    log_f = open(log_path, "w")
    proc = subprocess.Popen(cmd, stdout=log_f, stderr=log_f, env=env)

    for _ in range(100):
        if os.path.exists(sock_path):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        proc.wait()
        log_f.close()
        print(red(f"Server did not start within 10 s.  Log: {log_path}"),
              file=sys.stderr)
        sys.exit(1)

    print(f"  Server ready (PID {proc.pid})")

    # Run workload
    print(f"  Running workload ({args.ticks} ticks × {args.rows:,} rows, {mode_s}) …",
          end="", flush=True)
    try:
        if args.realistic:
            result = run_realistic_workload(sock_path, ticks=args.ticks,
                                            rows=args.rows, num_clients=args.clients)
        else:
            result = run_workload(sock_path, ticks=args.ticks, rows=args.rows)
    except Exception as e:
        print(red(f"  FAILED: {e}"))
        proc.kill()
        proc.wait()
        log_f.close()
        # Save server log
        import shutil
        shutil.copy2(log_path, str(out_dir / "server.log"))
        sys.exit(1)

    total_push = sum(result["tick_push"])
    print(f"  done ({total_push:.3f}s total push)")

    # Graceful shutdown — give the JIT time to flush summary
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    log_f.close()

    # Clean up server tmpdir (but keep jit logs in out_dir)
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)

    # Parse JIT logs
    print(f"  Parsing JIT logs …")
    worker_results: dict[int, dict] = {}
    for entry in sorted(out_dir.iterdir()):
        if entry.name.startswith("jit_") and entry.name.endswith(".log"):
            pid_m = re.search(r"jit_(\d+)\.log", entry.name)
            if pid_m:
                pid = int(pid_m.group(1))
                worker_results[pid] = parse_jit_log(str(entry))

    if not worker_results:
        print(red("  No JIT log files found.  PYPYLOG may not be supported by this binary."))
        sys.exit(1)

    # Print report
    report = print_report(worker_results, show_traces=args.traces)

    # Save report
    (out_dir / "report.txt").write_text(report)

    # Save timing
    import json
    (out_dir / "timing.json").write_text(json.dumps(result, indent=2))

    print()
    print(f"  {dim('Logs + report saved to:')} {out_dir}")
    print()


if __name__ == "__main__":
    main()
