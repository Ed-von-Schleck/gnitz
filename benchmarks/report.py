#!/usr/bin/env python3
"""
report.py — write a formatted benchmark report from the latest (or specified) run.

Usage:
    uv run python benchmarks/report.py                # latest run → report.txt
    uv run python benchmarks/report.py <results-dir>  # specific run dir → report.txt
    uv run python benchmarks/report.py --all          # all worker/client combos → report.txt

The script reads summary.json (throughput) and, if present, perf.data (hot
functions) from the results directory produced by benchmarks/run.py.
"""

import argparse
import json
import re
import subprocess
import sys
from io import StringIO
from pathlib import Path

RESULTS_ROOT = Path(__file__).parent / "results"

# ---------------------------------------------------------------------------
# Benchmark grouping — by category tier (micro / features / txn / combined)
# ---------------------------------------------------------------------------

TIERS = [
    ("micro", "micro — SQL client-path latency"),
    ("features", "features — per-feature incremental-maintenance cost"),
    ("txn", "txn — transactions"),
    ("combined", "combined — realistic end-to-end"),
]


def _tier(b):
    return (b.get("category") or "").split("/")[0]


# ---------------------------------------------------------------------------
# Perf parsing
# ---------------------------------------------------------------------------

_PERF_LINE = re.compile(
    r"^\s+([\d.]+)%\s+\d+\s+\S+\s+(\S+)\s+\[.\]\s+(.+)$"
)


def parse_perf(perf_data: Path, top_n: int = 20, pid: str | None = None) -> list[dict]:
    """Run perf report and return the top N self-overhead entries.

    If pid is given (comma-separated string), filters samples to those PIDs.
    """
    cmd = ["perf", "report", "--input", str(perf_data), "--stdio", "--no-children", "-n"]
    if pid:
        cmd += ["--pid", pid]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []

    entries = []
    for line in result.stdout.splitlines():
        m = _PERF_LINE.match(line)
        if not m:
            continue
        pct, dso, sym = m.group(1), m.group(2), m.group(3).strip()
        kind = "kernel" if "[kernel" in dso or dso == "[kernel.kallsyms]" else "userspace"
        entries.append({"pct": float(pct), "dso": dso, "symbol": sym, "kind": kind})
        if len(entries) >= top_n * 3:  # collect extra, we'll trim per-kind
            break

    return entries


def _emit_perf_tables(perf_data: Path, top_n: int, label: str,
                      pid: str | None = None, file=None) -> None:
    entries = parse_perf(perf_data, top_n=top_n * 2, pid=pid)
    if not entries:
        return
    prefix = f"{label} " if label else ""
    for kind_label, kind_key in (("Userspace", "userspace"), ("Kernel", "kernel")):
        subset = [e for e in entries if e["kind"] == kind_key][:top_n]
        if not subset:
            continue
        rows = [(f"{e['pct']:.2f}%", e["symbol"], e["dso"]) for e in subset]
        print_table(
            f"Hot functions — {prefix}{kind_label}",
            ["%", "symbol", "dso"],
            rows,
            right_cols={0},
            file=file,
        )


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _hr(widths):
    return "+" + "+".join("-" * (w + 2) for w in widths) + "+"


def _row(cells, widths):
    parts = []
    for cell, w in zip(cells, widths):
        parts.append(f" {cell:<{w}} ")
    return "|" + "|".join(parts) + "|"


def _row_right(cells, widths):
    """Right-align all cells except the first."""
    parts = []
    for i, (cell, w) in enumerate(zip(cells, widths)):
        if i == 0:
            parts.append(f" {cell:<{w}} ")
        else:
            parts.append(f" {cell:>{w}} ")
    return "|" + "|".join(parts) + "|"


def print_table(title, headers, rows, right_cols=None, file=None):
    """Print a simple ASCII table."""
    if not rows:
        return
    right_cols = right_cols or set(range(1, len(headers)))
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    print(f"\n### {title}\n", file=file)
    print(_hr(col_widths), file=file)
    print(_row(headers, col_widths), file=file)
    print(_hr(col_widths), file=file)
    for row in rows:
        print(_row_right([str(c) for c in row], col_widths), file=file)
    print(_hr(col_widths), file=file)


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------

def report_metadata(meta: dict, file=None):
    commit = meta.get("commit", "unknown")
    dirty = " (dirty)" if meta.get("dirty") else ""
    workers = meta.get("workers", "?")
    clients = meta.get("clients", "?")
    scale = meta.get("scale", "?")
    ts = meta.get("timestamp", "")[:19].replace("T", " ")
    print(f"Benchmark run: {ts}  commit={commit}{dirty}  workers={workers}  clients={clients}  scale={scale}", file=file)


def report_throughput(benchmarks: list[dict], file=None):
    """One throughput/latency table per category tier."""
    for tier, label in TIERS:
        matched = [b for b in benchmarks if _tier(b) == tier]
        if not matched:
            continue
        matched.sort(key=lambda b: (-b["rows_per_sec"], b["p50_ms"]))
        rows = []
        for b in matched:
            rps = f"{b['rows_per_sec']:,.0f}" if b["rows_per_sec"] else "-"
            rows.append((
                b["name"], rps,
                f"{b['p50_ms']:.2f}", f"{b['p90_ms']:.2f}", f"{b['p99_ms']:.2f}",
                str(b["iterations"]),
            ))
        print_table(
            label,
            ["benchmark", "rows/s", "p50ms", "p90ms", "p99ms", "iters"],
            rows,
            file=file,
        )


def report_transactions(benchmarks: list[dict], file=None):
    """Transactions table: txns/s, conflicts, retries next to commit p50/p99."""
    matched = [b for b in benchmarks if "txns_per_sec" in (b.get("extra") or {})]
    if not matched:
        return
    matched.sort(key=lambda b: -(b["extra"].get("txns_per_sec") or 0))
    rows = []
    for b in matched:
        e = b["extra"]
        rows.append((
            b["name"], f"{e.get('txns_per_sec', 0):,.0f}",
            str(e.get("conflicts", "-")), str(e.get("retries", "-")),
            f"{b['p50_ms']:.2f}", f"{b['p99_ms']:.2f}", str(b.get("num_clients", 1)),
        ))
    print_table(
        "Transactions",
        ["benchmark", "txns/s", "conflicts", "retries", "p50ms", "p99ms", "clients"],
        rows,
        file=file,
    )


def report_htap_serving(benchmarks: list[dict], file=None):
    """HTAP / serving table: torn_reads / staleness + reader p99."""
    def _has(b):
        e = b.get("extra") or {}
        return "torn_reads" in e or "staleness" in e

    matched = [b for b in benchmarks if _has(b)]
    if not matched:
        return
    rows = []
    for b in matched:
        e = b["extra"]
        integrity = e.get("torn_reads", e.get("staleness", "-"))
        reader_p99 = e.get("reader_p99_ms", e.get("seek_p99_ms", b["p99_ms"]))
        writer_tps = e.get("writer_txns_per_sec", e.get("writer_pushes_per_sec", "-"))
        rows.append((
            b["name"], str(integrity), f"{reader_p99:.2f}" if isinstance(reader_p99, (int, float)) else str(reader_p99),
            f"{writer_tps:,.0f}" if isinstance(writer_tps, (int, float)) else str(writer_tps),
            str(e.get("reader_ops", "-")),
        ))
    print_table(
        "HTAP / Serving",
        ["benchmark", "torn/stale", "reader p99ms", "writer/s", "reader ops"],
        rows,
        file=file,
    )


def report_perf(perf_data: Path, top_n: int = 15, file=None):
    if not perf_data.exists():
        print("\n(no perf.data found or perf not available)", file=file)
        return

    pids_file = perf_data.parent / "pids.json"
    if pids_file.exists():
        with open(pids_file) as f:
            pids = json.load(f)
        master_pid = pids.get("master")
        worker_pids = pids.get("workers", [])

        if master_pid:
            _emit_perf_tables(perf_data, top_n, "Master", pid=str(master_pid), file=file)
        if worker_pids:
            pid_str = ",".join(str(p) for p in worker_pids)
            _emit_perf_tables(perf_data, top_n, "Workers", pid=pid_str, file=file)
    else:
        # Old perf.data without pids.json: report combined, no split.
        _emit_perf_tables(perf_data, top_n, "", file=file)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def find_latest_run() -> Path:
    runs = sorted(RESULTS_ROOT.iterdir(), key=lambda p: p.name)
    for run in reversed(runs):
        if run.is_dir() and any(run.iterdir()):
            return run
    sys.exit(f"No benchmark results found in {RESULTS_ROOT}")


def find_subdirs(run_dir: Path) -> list[Path]:
    return sorted(d for d in run_dir.iterdir() if d.is_dir())


def report_one(subdir: Path, file=None):
    summary_file = subdir / "summary.json"
    if not summary_file.exists():
        print(f"(no summary.json in {subdir})", file=file)
        return

    with open(summary_file) as f:
        data = json.load(f)

    report_metadata(data, file=file)
    benchmarks = data.get("benchmarks", [])
    report_throughput(benchmarks, file=file)
    report_transactions(benchmarks, file=file)
    report_htap_serving(benchmarks, file=file)

    perf_data = subdir / "perf.data"
    if perf_data.exists():
        print(f"\n--- perf profile: {perf_data} ---", file=file)
        report_perf(perf_data, file=file)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run_dir", nargs="?", help="Results directory (default: latest)")
    parser.add_argument("--all", action="store_true", help="Report all worker/client combos")
    parser.add_argument("--top", type=int, default=15, help="Top N perf entries per category (default: 15)")
    args = parser.parse_args()

    run_dir = Path(args.run_dir) if args.run_dir else find_latest_run()
    if not run_dir.is_dir():
        sys.exit(f"Not a directory: {run_dir}")

    subdirs = find_subdirs(run_dir)
    if not subdirs:
        sys.exit(f"No worker/client subdirectories found in {run_dir}")

    to_report = subdirs if args.all else [subdirs[-1]]

    # Collect output in StringIO
    output = StringIO()

    for subdir in to_report:
        print(f"\n{'='*72}", file=output)
        print(f"  {subdir}", file=output)
        print(f"{'='*72}", file=output)
        report_one(subdir, file=output)

    # Write to file
    report_file = run_dir / "report.txt"
    with open(report_file, "w") as f:
        f.write(output.getvalue())

    # Print path to stdout
    print(f"Report written to: {report_file}")


if __name__ == "__main__":
    main()
