#!/usr/bin/env python3
"""The whole-program benchmark: scenarios against a real server, measured in
counts — instructions, syscalls, syncs, bytes — per process and per phase.

    bench.py run [--scenario a,b] [--family f] [--workers 4] [--regime l0]
    bench.py report [results-dir] [--scenario a] [--disk]
    bench.py compare [before-dir after-dir]
    bench.py list

Run through `make bench`, which builds the server and the extension first.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS = REPO_ROOT / "benchmarks/results"


def git(*args):
    try:
        return subprocess.check_output(["git", *args], cwd=REPO_ROOT, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def latest_runs(n):
    """The `n` newest results directories, oldest first."""
    runs = sorted(d for d in RESULTS.iterdir() if (d / "meta.json").exists()) if RESULTS.exists() else []
    if len(runs) < n:
        sys.exit(f"fewer than {n} results under {RESULTS}")
    return runs[-n:]


def cmd_list(args):
    from scenarios import ALL
    for s in ALL:
        print(f"{s.family:<11} {s.name:<18} {','.join(s.regimes):<28} {s.doc}")


def cmd_run(args):
    from harness import counters, profile, report
    from harness.run import REGIMES, Params, Run
    from scenarios import ALL, BY_NAME

    wanted = [n for n in args.scenario.split(",") if n]
    unknown = set(wanted) - set(BY_NAME)
    if unknown:
        sys.exit(f"unknown scenario: {', '.join(sorted(unknown))}")
    chosen = [s for s in ALL if (not wanted or s.name in wanted) and (not args.family or s.family in args.family.split(","))]
    if not chosen:
        sys.exit("no scenario selected")
    unknown = set(args.regime.split(",")) - {"every", *REGIMES}
    if unknown:
        sys.exit(f"unknown regime: {', '.join(sorted(unknown))}")
    layers = profile.LAYERS if args.profile == "all" else tuple(n for n in args.profile.split(",") if n)
    profile.require(layers)

    os.environ["GNITZ_SERVER_BIN"] = os.path.abspath(args.server)
    out = RESULTS / (time.strftime("%Y%m%d-%H%M%S") + (f"-{args.label}" if args.label else ""))
    out.mkdir(parents=True)
    caps = counters.capabilities()
    meta = {
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "commit": git("rev-parse", "--short", "HEAD"),
        "dirty": bool(git("status", "--porcelain", "--untracked-files=no")), "label": args.label,
        "server": os.path.abspath(args.server), "rows": args.rows, "layers": layers,
        "kernel": platform.release(), "machine": platform.machine(), "capabilities": caps,
        "argv": sys.argv[1:],
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=1))
    for what, have in caps.items():
        if not have:
            print(f"note: no {what.replace('_', ' ')} on this machine; those columns stay empty")

    for scenario in chosen:
        regimes = scenario.regimes if args.regime == "every" else args.regime.split(",")
        for workers in [int(w) for w in args.workers.split(",")]:
            for regime in regimes:
                name = f"{scenario.name}.w{workers}.{regime}"
                def execute(only=()):
                    return Run(scenario, Params(
                        rows=args.rows, workers=workers, regime=regime, env=dict(kv.split("=", 1) for kv in args.env),
                        keep=args.keep and not only, layers=only), out / name).execute()
                t = time.time()
                print(f"=== {name} ...", end="", flush=True)
                record = execute()
                # A layer distorts what the others would see, so each has a run of its own.
                for layer in layers:
                    print(f" {layer} ...", end="", flush=True)
                    profiled = execute((layer,))
                    record["layers"].update(profiled["layers"])
                record["elapsed_s"] = time.time() - t
                (out / f"{name}.json").write_text(json.dumps(record, indent=1))
                print(f" {record['elapsed_s']:.1f}s")
                if len(chosen) == 1:
                    report.print_record(record)
    print()
    report.print_summary(report.load(out))
    print(f"\nresults: {out}")


def cmd_report(args):
    from harness import report
    run = Path(args.dir) if args.dir else latest_runs(1)[0]
    records = report.load(run)
    wanted = [n for n in args.scenario.split(",") if n]
    if wanted:
        records = [r for r in records if r["scenario"] in wanted]
        for r in records:
            report.print_record(r)
    if args.disk:
        report.print_disk(records)
    elif not wanted:
        report.print_summary(records)
    print(f"\nresults: {run}")


def cmd_compare(args):
    from harness import report
    if not args.dirs:
        args.dirs = latest_runs(2)
    elif len(args.dirs) != 2:
        sys.exit("compare takes a before and an after directory, or neither for the two latest runs")
    before, after = map(Path, args.dirs)
    print(f"before: {before}\nafter:  {after}")
    report.print_compare(report.load(before), report.load(after), args.all)


def main():
    from harness import profile
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="run scenarios and write a results directory")
    run.add_argument("--scenario", default="", help="comma-separated scenario names (default: all)")
    run.add_argument("--family", default="", help="comma-separated scenario families")
    run.add_argument("--workers", default="4", help="comma-separated worker counts")
    run.add_argument("--regime", default="l0",
                     help="comma-separated storage regimes (l0, compacted, checkpointed), or `every` for each "
                          "scenario's own list")
    run.add_argument("--rows", type=int, default=200_000, help="rows a scenario loads")
    run.add_argument("--server", default=str(REPO_ROOT / "gnitz-server-release"))
    run.add_argument("--profile", default="", help=f"comma-separated layers ({', '.join(profile.LAYERS)}), or `all`")
    run.add_argument("--env", action="append", default=[], metavar="NAME=VALUE", help="a server environment variable")
    run.add_argument("--label", default="", help="a label in the results directory's name")
    run.add_argument("--keep", action="store_true", help="keep each data directory")
    run.set_defaults(fn=cmd_run)

    rep = sub.add_parser("report", help="print a results directory")
    rep.add_argument("dir", nargs="?", help="results directory (default: the latest)")
    rep.add_argument("--scenario", default="", help="print these scenarios phase by phase")
    rep.add_argument("--disk", action="store_true", help="print every disk reading by store and region")
    rep.set_defaults(fn=cmd_report)

    cmp_ = sub.add_parser("compare", help="what changed between two results directories")
    cmp_.add_argument("dirs", nargs="*", metavar="dir", help="the before and the after directory (default: the two latest)")
    cmp_.add_argument("--all", action="store_true", help="print the rows that did not move, too")
    cmp_.set_defaults(fn=cmd_compare)

    sub.add_parser("list", help="list the scenarios").set_defaults(fn=cmd_list)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
