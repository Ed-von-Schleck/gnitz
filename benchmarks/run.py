#!/usr/bin/env python3
"""Outer CLI runner for GnitzDB benchmarks.

Orchestrates pytest invocations across worker/client combinations,
handles result rotation and summary printing. Does NOT import pytest or gnitz.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"


def parse_args():
    p = argparse.ArgumentParser(description="GnitzDB SQL Benchmark Runner")
    p.add_argument("--full", action="store_true",
                   help="Full-scale mode (150k rows)")
    p.add_argument("--workers", type=str, default="1",
                   help="Comma-separated worker counts (e.g. 1,2,4)")
    p.add_argument("--clients", type=str, default="1",
                   help="Comma-separated client counts (e.g. 1,2,4)")
    p.add_argument("--perf", action="store_true",
                   help="Enable perf record")
    p.add_argument("--perf-dwarf", action="store_true",
                   help="Enable perf record with --call-graph=dwarf for full userspace stacks")
    p.add_argument("--perf-stat", action="store_true",
                   help="Enable perf stat")
    p.add_argument("-k", type=str, default=None,
                   help="pytest -k expression")
    return p.parse_args()


def parse_int_list(s: str) -> list[int]:
    return [int(x.strip()) for x in s.split(",")]


def prune_old_results(keep: int = 3) -> None:
    """Keep only the N most recent result directories."""
    if not RESULTS_DIR.exists():
        return
    dirs = sorted(
        [d for d in RESULTS_DIR.iterdir() if d.is_dir()],
        key=lambda d: d.name,
        reverse=True,
    )
    for old in dirs[keep:]:
        shutil.rmtree(old, ignore_errors=True)


def merge_summaries(output_dir: Path) -> None:
    """Merge all summary.json files in subdirectories into a combined summary."""
    all_benchmarks = []
    for sub in sorted(output_dir.iterdir()):
        summary_path = sub / "summary.json"
        if summary_path.exists():
            with open(summary_path) as f:
                data = json.load(f)
            for b in data.get("benchmarks", []):
                b["workers"] = data.get("workers")
                b["clients"] = data.get("clients")
                all_benchmarks.append(b)

    if all_benchmarks:
        combined = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "benchmarks": all_benchmarks,
        }
        with open(output_dir / "combined_summary.json", "w") as f:
            json.dump(combined, f, indent=2)


def print_summary_table(output_dir: Path) -> None:
    """Print a formatted summary to stdout."""
    combined_path = output_dir / "combined_summary.json"
    if not combined_path.exists():
        # Single run, look for summary.json directly
        for sub in output_dir.iterdir():
            if (sub / "summary.json").exists():
                combined_path = sub / "summary.json"
                break
    if not combined_path.exists():
        print("No results found.")
        return

    with open(combined_path) as f:
        data = json.load(f)

    benchmarks = data.get("benchmarks", [])
    if not benchmarks:
        print("No benchmark results.")
        return

    # Header
    print(f"\n{'Name':<45} {'rows/s':>10} {'p50ms':>8} {'p90ms':>8} "
          f"{'p99ms':>8} {'iters':>6}")
    print("-" * 95)
    for b in benchmarks:
        w = b.get("workers", "")
        c = b.get("clients", "")
        prefix = f"[w{w}c{c}] " if w else ""
        name = prefix + b["name"]
        if len(name) > 44:
            name = name[:41] + "..."
        print(f"{name:<45} {b['rows_per_sec']:>10.0f} {b['p50_ms']:>8.2f} "
              f"{b['p90_ms']:>8.2f} {b['p99_ms']:>8.2f} {b['iterations']:>6}")

    print(f"\nResults in: {output_dir}")


def main():
    args = parse_args()
    workers_list = parse_int_list(args.workers)
    clients_list = parse_int_list(args.clients)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = RESULTS_DIR / ts
    output_dir.mkdir(parents=True, exist_ok=True)

    prune_old_results(keep=3)

    for workers in workers_list:
        for clients in clients_list:
            subdir = output_dir / f"w{workers}_c{clients}"
            subdir.mkdir(parents=True, exist_ok=True)

            cmd = [
                "uv", "run", "pytest", "../../benchmarks/",
                f"--workers={workers}",
                f"--clients={clients}",
                f"--results-dir={subdir}",
                *(["--full"] if args.full else []),
                *(["--perf"] if args.perf else []),
                *(["--perf-dwarf"] if args.perf_dwarf else []),
                *(["--perf-stat"] if args.perf_stat else []),
                *(["-k", args.k] if args.k else []),
                "-q", "--tb=short",
            ]
            print(f"\n=== Running: workers={workers} clients={clients} ===")
            print(f"    cmd: {' '.join(cmd)}")
            result = subprocess.run(cmd, cwd=REPO_ROOT / "crates" / "gnitz-py")
            if result.returncode != 0:
                print(f"    WARNING: pytest exited with code {result.returncode}")

    # Merge and print
    if len(workers_list) > 1 or len(clients_list) > 1:
        merge_summaries(output_dir)
    print_summary_table(output_dir)


if __name__ == "__main__":
    main()
