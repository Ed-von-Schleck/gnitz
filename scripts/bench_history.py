#!/usr/bin/env python3
"""
bench_history.py — View and compare historical GnitzDB benchmark runs.

Records are saved automatically by perf_profile.py and compare_jit.py into
bench_history/history.jsonl (gitignored).  Each record is keyed by commit
hash, timestamp, mode (throughput/realistic), and tool (perf/compare).

USAGE
=====
    python scripts/bench_history.py                     # list all runs
    python scripts/bench_history.py --last 10            # last 10 runs
    python scripts/bench_history.py --commit abc1234     # filter by commit
    python scripts/bench_history.py --mode realistic     # filter by mode
    python scripts/bench_history.py --compare abc1234 def5678
                                                         # side-by-side comparison
    python scripts/bench_history.py --trend              # throughput trend over time
    python scripts/bench_history.py --json               # raw JSON output
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from workload import load_history

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
_COLOUR = sys.stdout.isatty()

def _c(code: str, s: str) -> str:
    return f"\033[{code}m{s}\033[0m" if _COLOUR else s

def bold(s: str)   -> str: return _c("1",  s)
def dim(s: str)    -> str: return _c("2",  s)
def green(s: str)  -> str: return _c("32", s)
def yellow(s: str) -> str: return _c("33", s)
def red(s: str)    -> str: return _c("31", s)
def cyan(s: str)   -> str: return _c("36", s)


def _speedup(old: float, new: float) -> str:
    if old <= 0 or new <= 0:
        return "n/a"
    ratio = old / new
    if ratio >= 1.2:
        return green(f"{ratio:.2f}x faster")
    elif ratio <= 0.8:
        return red(f"{1/ratio:.2f}x slower")
    else:
        return f"{ratio:.2f}x"


# ---------------------------------------------------------------------------
# List runs
# ---------------------------------------------------------------------------
def print_list(records: list[dict]) -> None:
    if not records:
        print("No benchmark history found.")
        print(dim("Run perf_profile.py or compare_jit.py to generate records."))
        return

    hdr = (f"  {'#':>3}  {'Timestamp':<20}  {'Commit':<9}  {'Mode':<10}  "
           f"{'Tool':<8}  {'Ticks':>5}  {'Rows':>7}  {'Push (s)':>9}  {'Tput (r/s)':>11}")
    sep = (f"  {'---':>3}  {'----':<20}  {'------':<9}  {'----':<10}  "
           f"{'----':<8}  {'-----':>5}  {'-----':>7}  {'---------':>9}  {'-----------':>11}")

    print(bold(cyan(f"  Benchmark History — {len(records)} records")))
    print()
    print(hdr)
    print(sep)

    for i, r in enumerate(records, 1):
        ts = r.get("timestamp", "?")[:19].replace("T", " ")
        commit = r.get("commit", "?")
        dirty = "*" if r.get("dirty", False) else ""
        mode = r.get("mode", "?")
        tool = r.get("tool", "?")
        ticks = r.get("ticks", 0)
        rows = r.get("rows_per_tick", 0)
        push = r.get("total_push_s", 0)
        tput = r.get("throughput_rows_per_sec", 0)

        commit_s = f"{commit}{dirty}"
        print(f"  {i:>3}  {ts:<20}  {commit_s:<9}  {mode:<10}  "
              f"{tool:<8}  {ticks:>5}  {rows:>7}  {push:>8.3f}s  {tput:>11,}")


# ---------------------------------------------------------------------------
# Compare two commits
# ---------------------------------------------------------------------------
def print_compare(records: list[dict], commit_a: str, commit_b: str) -> None:
    recs_a = [r for r in records if r.get("commit", "").startswith(commit_a)]
    recs_b = [r for r in records if r.get("commit", "").startswith(commit_b)]

    if not recs_a:
        print(red(f"No records found for commit {commit_a}"))
        return
    if not recs_b:
        print(red(f"No records found for commit {commit_b}"))
        return

    # Use most recent record for each commit
    a = recs_a[-1]
    b = recs_b[-1]

    print(bold(cyan(f"  Comparison: {a['commit']} vs {b['commit']}")))
    print()

    rows_w = 14
    print(f"  {'Metric':<28}  {a['commit']:>{rows_w}}  {b['commit']:>{rows_w}}  {'Change':>16}")
    print(f"  {'------':<28}  {'-'*rows_w:>{rows_w}}  {'-'*rows_w:>{rows_w}}  {'------':>16}")

    def _row(label, va, vb, fmt):
        sa = fmt.format(va)
        sb = fmt.format(vb)
        change = _speedup(va, vb) if isinstance(va, (int, float)) and va > 0 else ""
        print(f"  {label:<28}  {sa:>{rows_w}}  {sb:>{rows_w}}  {change:>16}")

    _row("Mode", a.get("mode", "?"), b.get("mode", "?"), "{}")
    _row("Ticks", a.get("ticks", 0), b.get("ticks", 0), "{}")
    _row("Rows/tick", a.get("rows_per_tick", 0), b.get("rows_per_tick", 0), "{:,}")
    _row("Workers", a.get("workers", 0), b.get("workers", 0), "{}")
    print()
    _row("Setup (s)", a.get("setup_s", 0), b.get("setup_s", 0), "{:.4f}")
    _row("Total push (s)", a.get("total_push_s", 0), b.get("total_push_s", 0), "{:.4f}")
    _row("Total scan (s)", a.get("total_scan_s", 0), b.get("total_scan_s", 0), "{:.4f}")
    _row("Seek (s)", a.get("seek_s", 0), b.get("seek_s", 0), "{:.4f}")
    _row("Throughput (rows/s)",
         a.get("throughput_rows_per_sec", 0),
         b.get("throughput_rows_per_sec", 0), "{:,}")

    # Realistic latency comparison
    ap = a.get("push_latency_ms", {})
    bp = b.get("push_latency_ms", {})
    if ap and bp:
        print()
        print(bold("  Push latency (ms)"))
        for key in ["p50", "p90", "p95", "p99", "mean"]:
            va = ap.get(key, 0)
            vb = bp.get(key, 0)
            # For latency, lower is better — invert speedup
            change = _speedup(vb, va) if va > 0 and vb > 0 else ""
            print(f"  {key:<28}  {va:>{rows_w}.3f}  {vb:>{rows_w}.3f}  {change:>16}")


# ---------------------------------------------------------------------------
# Throughput trend
# ---------------------------------------------------------------------------
def print_trend(records: list[dict]) -> None:
    if not records:
        print("No benchmark history found.")
        return

    print(bold(cyan("  Throughput Trend")))
    print()

    # Find max throughput for bar scaling
    tputs = [r.get("throughput_rows_per_sec", 0) for r in records]
    max_tput = max(tputs) if tputs else 1
    bar_width = 40

    print(f"  {'Commit':<9}  {'Mode':<10}  {'Tput (r/s)':>11}  Bar")
    print(f"  {'------':<9}  {'----':<10}  {'-----------':>11}  ---")

    for r in records:
        commit = r.get("commit", "?")
        dirty = "*" if r.get("dirty", False) else ""
        mode = r.get("mode", "?")
        tput = r.get("throughput_rows_per_sec", 0)
        filled = int(round(tput / max_tput * bar_width)) if max_tput > 0 else 0
        bar = green("█" * filled) + dim("░" * (bar_width - filled))
        print(f"  {commit}{dirty:<9}  {mode:<10}  {tput:>11,}  {bar}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(
        description="View and compare historical GnitzDB benchmark runs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--last", type=int, default=0, metavar="N",
                    help="Show only the last N runs.")
    ap.add_argument("--commit", default=None,
                    help="Filter records by commit hash prefix.")
    ap.add_argument("--mode", default=None, choices=["throughput", "realistic"],
                    help="Filter records by workload mode.")
    ap.add_argument("--tool", default=None, choices=["perf", "compare"],
                    help="Filter records by benchmark tool.")
    ap.add_argument("--compare", nargs=2, metavar="COMMIT",
                    help="Compare two commits side-by-side.")
    ap.add_argument("--trend", action="store_true",
                    help="Show throughput trend across all recorded runs.")
    ap.add_argument("--json", action="store_true",
                    help="Output raw JSON instead of formatted table.")
    args = ap.parse_args()

    records = load_history()

    # Apply filters
    if args.commit:
        records = [r for r in records if r.get("commit", "").startswith(args.commit)]
    if args.mode:
        records = [r for r in records if r.get("mode") == args.mode]
    if args.tool:
        records = [r for r in records if r.get("tool") == args.tool]

    if args.compare:
        all_records = load_history()  # unfiltered for compare
        print_compare(all_records, args.compare[0], args.compare[1])
        return

    if args.last > 0:
        records = records[-args.last:]

    if args.json:
        print(json.dumps(records, indent=2))
        return

    if args.trend:
        print_trend(records)
        return

    print_list(records)
    print()


if __name__ == "__main__":
    main()
