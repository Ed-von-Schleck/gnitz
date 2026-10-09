"""Reading a results directory: one scenario phase by phase, a run as one table,
and two runs against each other."""

from __future__ import annotations

import json
import math
from pathlib import Path

from . import disk
from .counters import GAUGES


def load(run_dir: Path):
    # In the order they ran.
    paths = sorted((p for p in run_dir.glob("*.json") if p.name != "meta.json"), key=lambda p: p.stat().st_mtime)
    records = [json.loads(p.read_text()) for p in paths]
    if not records:
        raise SystemExit(f"no scenario records in {run_dir}")
    return records


def key(record):
    return f"{record['scenario']}.w{record['workers']}.{record['regime']}"


def fold(groups, names):
    """The groups in `names` as one."""
    out = {}
    for g in names:
        for k, v in groups.get(g, {}).items():
            out[k] = max(out.get(k, 0), v) if k in GAUGES else out.get(k, 0) + v
    return out


def workers_of(phase):
    return sorted(g for g in phase["groups"] if g.startswith("worker"))


def server(phase):
    return fold(phase["groups"], ["master", *workers_of(phase)])


def metrics(phase):
    """The numbers of record for one phase; a metric the machine could not
    count is absent."""
    rows = max(phase["rows"], 1)
    srv, cli = server(phase), phase["groups"].get("client", {})
    out = {"wall_s": phase["wall_s"]}

    def put(name, value, per_row=False):
        if value is not None:
            out[name] = value / rows if per_row else value

    put("server_instr_per_row", srv.get("instr_user"), True)
    put("server_kernel_instr_per_row", srv.get("instr_kernel"), True)
    put("client_instr_per_row", phase.get("client_instr_exact", cli.get("instr_user")), True)
    put("server_cpu_s", srv.get("run_ns", 0) / 1e9)
    put("server_syscalls", srv.get("syscalls"))
    put("server_syncs", srv.get("syncs"))
    put("server_switches", srv.get("voluntary_switches"))
    put("written_bytes_per_row", srv.get("write_bytes"), True)
    put("server_rss_peak_mb", srv.get("rss_peak_bytes", 0) / 2**20 or None)
    for kind, c in phase["calls"].items():
        out[f"{kind}_p50_ms"] = c["p50_ms"]
        out[f"{kind}_p99_ms"] = c["p99_ms"]
    if "ready_s" in phase:
        out["ready_s"] = phase["ready_s"]
    return out


def _n(v, width, digits=0):
    return f"{'':>{width}}" if v is None else f"{v:>{width},.{digits}f}"


def print_phase(phase):
    rows, wall = max(phase["rows"], 1), phase["wall_s"]
    print(f"\n  {phase['name']}: {phase['rows']:,} rows in {wall:.3f} s"
          + (f" ({phase['rows'] / wall:,.0f} rows/s)" if phase["rows"] and wall else "")
          + (f"; ready after {phase['ready_s'] * 1e3:.0f} ms, {phase['rebuilt_views']} views rebuilt"
             if "ready_s" in phase else ""))
    w = workers_of(phase)
    lines = [("client", ["client"]), ("master", ["master"]), ("workers", w)]
    print(f"    {'':<9} {'instr/row':>10} {'kernel/row':>10} {'cpu ms':>8} {'runq ms':>8} {'busy':>5} "
          f"{'syscalls':>9} {'ring':>7} {'syncs':>6} {'switches':>8} {'faults':>7} {'written B':>11} {'rss MB':>7}")
    for label, names in lines:
        g = fold(phase["groups"], names)
        if not g:
            continue
        cpu = g.get("run_ns", 0) / 1e6
        busy = cpu / (wall * 1e3 * max(len(names), 1)) if wall else 0
        print(f"    {label:<9} {_n(g['instr_user'] / rows if 'instr_user' in g else None, 10)} "
              f"{_n(g['instr_kernel'] / rows if 'instr_kernel' in g else None, 10)} {cpu:>8.1f} "
              f"{g.get('runq_ns', 0) / 1e6:>8.1f} {busy:>5.0%} {_n(g.get('syscalls'), 9)} "
              f"{_n(g.get('io_uring_enters'), 7)} {_n(g.get('syncs'), 6)} {_n(g.get('voluntary_switches'), 8)} "
              f"{_n(g.get('minor_faults'), 7)} {_n(g.get('write_bytes'), 11)} "
              f"{_n(g.get('rss_peak_bytes', 0) / 2**20 or None, 7, 1)}")
    per_worker = [phase["groups"][g].get("instr_user", 0) for g in w]
    if len(per_worker) > 1 and sum(per_worker):
        print(f"    busiest worker ran {max(per_worker) / sum(per_worker):.0%} of the workers' instructions, "
              f"the idlest {min(per_worker) / sum(per_worker):.0%}")
    for kind, c in phase["calls"].items():
        print(f"    {kind:<12} {c['n']:>6} calls  p50 {c['p50_ms']:8.3f}  p90 {c['p90_ms']:8.3f}  "
              f"p99 {c['p99_ms']:8.3f}  max {c['max_ms']:8.3f} ms  ({c['sum_ms'] / 1e3:.3f} s in all)")


def print_record(record):
    v = record.get("verified")
    print(f"\n=== {key(record)}: {record['doc']}")
    print(f"    {record['rows']:,} rows; "
          + ("no view verified" if not v else f"{v.get('checked', 0)} views equal their backfill"
             + (f", {len(v['skipped'])} not held to it" if v.get("skipped") else "")))
    for phase in record["phases"]:
        print_phase(phase)
        for layer in record.get("layers", {}).values():
            for line in layer.get("by_phase", {}).get(phase["name"], []):
                print(f"    {line}")
    for name, layer in record.get("layers", {}).items():
        for line in layer.get("files", []):
            print(f"  {name}: {line}")
    for name, snap in record["disk"].items():
        t = disk.totals(snap)
        print(f"\n  disk, {name}: {t['shard_bytes']:,} shard bytes in {t['files']} files"
              + (f", {t['per_value_byte']:.2f} per value byte" if t["per_value_byte"] else "")
              + f" (base {t['base_bytes']:,}, views {t['view_bytes']:,}, "
              f"traces and indexes {t['trace_index_bytes']:,}); {t['dead_rows']:.0%} dead rows")


def print_summary(records):
    """Every phase of every scenario as one line: where a run's cost is."""
    print(f"{'scenario':<30} {'phase':<22} {'rows':>9} {'srv instr/row':>13} {'kernel/row':>10} "
          f"{'cli instr/row':>13} {'srv cpu s':>9} {'wall s':>7} {'syncs':>6} {'syscalls':>9} "
          f"{'written B/row':>13} {'call p50 ms':>11} {'p99 ms':>8}")
    for r in records:
        for i, phase in enumerate(r["phases"]):
            m = metrics(phase)
            main = max(phase["calls"].items(), key=lambda kv: kv[1]["sum_ms"], default=(None, None))[1]
            print(f"{key(r) if i == 0 else '':<30} {phase['name']:<22} {phase['rows']:>9,} "
                  f"{_n(m.get('server_instr_per_row'), 13)} {_n(m.get('server_kernel_instr_per_row'), 10)} "
                  f"{_n(m.get('client_instr_per_row'), 13)} {m.get('server_cpu_s', 0):>9.3f} {m['wall_s']:>7.3f} "
                  f"{_n(m.get('server_syncs'), 6)} {_n(m.get('server_syscalls'), 9)} "
                  f"{_n(m.get('written_bytes_per_row'), 13, 1)} "
                  f"{_n(main and main['p50_ms'], 11, 3)} {_n(main and main['p99_ms'], 8, 3)}")
        for name, snap in r["disk"].items():
            t = disk.totals(snap)
            print(f"{'':<30} {'disk ' + name:<22} {t['shard_bytes']:>9,} shard bytes, "
                  + (f"{t['per_value_byte']:.2f} per value byte, " if t["per_value_byte"] else "")
                  + f"{t['files']} files, {t['dead_rows']:.0%} dead rows")


def print_disk(records):
    """Every disk reading by store and region, then all of them as one table."""
    def readings(r):
        writes = r["layers"].get("writes", {}).get("readings", {})
        return [(name, snap, writes.get(name)) for name, snap in r["disk"].items()]
    for r in records:
        for name, snap, writes in readings(r):
            disk.print_snapshot(f"{key(r)} {name}", snap, writes)
    # `in 4K blocks` is the shard bytes with every file rounded up to a filesystem block.
    print(f"\n{'scenario':<32} {'reading':<8} {'value bytes':>12} {'shard bytes':>12} {'per value':>9} {'files':>6} "
          f"{'in 4K blocks':>12} {'base':>12} {'views':>12} {'traces+idx':>12} {'zstd-3':>6} {'dead rows':>9} "
          f"{'pushed':>12} {'written':>12} {'published':>12}")
    for r in records:
        for name, snap, writes in readings(r):
            t = disk.totals(snap)
            wrote, published = disk.written(writes)
            print(f"{key(r):<32} {name:<8} {_n(snap['value_bytes'], 12)} {t['shard_bytes']:>12,} "
                  f"{_n(t['per_value_byte'], 9, 2)} {t['files']:>6} {t['block_bytes']:>12,} {t['base_bytes']:>12,} "
                  f"{t['view_bytes']:>12,} {t['trace_index_bytes']:>12,} {t['zstd3_ratio']:>6.2f} {t['dead_rows']:>9.0%} "
                  f"{snap['pushed_bytes']:>12,} {_n(wrote, 12)} {_n(published, 12)}")


# How far a metric moves between two runs of the same code, measured over this
# suite: at least nine readings in ten stay inside it. A count repeats to a
# fraction of a percent, a duration does not, and the bytes a process is charged
# for follow the page cache's own timing.
NOISE = {
    "server_instr_per_row": 0.01, "client_instr_per_row": 0.10, "server_kernel_instr_per_row": 0.30,
    "server_syscalls": 0.15, "server_syncs": 0.0, "server_switches": 0.25, "written_bytes_per_row": 0.35,
    "server_rss_peak_mb": 0.10, "server_cpu_s": 0.40, "wall_s": 0.50, "ready_s": 0.50,
}
# Below these a reading is its own granularity: a handful of calls, a scheduler tick.
FLOOR = {"server_syscalls": 100, "server_switches": 100, "server_cpu_s": 0.25, "wall_s": 0.25, "ready_s": 0.25}
# Where clients race each other, or a stop finds the stores wherever their
# background work had got to, the work itself differs from run to run.
UNPACED_NOISE = 0.40


def flat(records):
    """`{(run key, phase, metric): (value, noise, paced)}` over a results directory."""
    out = {}
    for r in records:
        for phase in r["phases"]:
            paced = not phase.get("unpaced")
            for metric, v in metrics(phase).items():
                if metric.endswith("_ms"):
                    noise = None                # a latency: shown, never judged
                elif metric == "client_instr_per_row" and "client_instr_exact" in phase:
                    noise = 0.01
                else:
                    noise = NOISE[metric] if paced else max(NOISE[metric], UNPACED_NOISE)
                out[(key(r), phase["name"], metric)] = (v, noise, paced)
        for name, snap in r["disk"].items():
            t = disk.totals(snap)
            # A byte on disk is exact.
            for metric in ("per_value_byte", "shard_bytes", "files", "dead_rows"):
                if t[metric] is not None:
                    out[(key(r), f"disk {name}", f"disk_{metric}")] = (t[metric], 0.0, True)
    return out


def print_compare(before, after, everything=False):
    """What moved between two runs. A count is judged against its noise; a
    duration is judged too, loosely; a latency is printed only on request, and
    never judged — a percentile of a few hundred calls on a shared machine
    decides nothing."""
    a, b = flat(before), flat(after)
    only_a, only_b = sorted({k[0] for k in a} - {k[0] for k in b}), sorted({k[0] for k in b} - {k[0] for k in a})
    if only_a:
        print(f"only before: {', '.join(only_a)}")
    if only_b:
        print(f"only after: {', '.join(only_b)}")
    print(f"{'scenario':<30} {'phase':<20} {'metric':<28} {'before':>14} {'after':>14} {'change':>8}")
    ratios, last, moved, judged = [], None, 0, 0
    for k in sorted(a.keys() & b.keys()):
        (x, noise, paced), (y, _, _) = a[k], b[k]
        # Under one unit a row there is nothing to compare: the phase asked nothing of this process.
        if max(abs(x), abs(y)) < FLOOR.get(k[2], 1e-9) or (k[2].endswith("_per_row") and max(x, y) < 10):
            continue
        change = (y - x) / x if x else math.inf
        if k[2] == "server_instr_per_row" and x and y and paced:
            ratios.append(y / x)
        past = noise is not None and abs(change) > noise
        judged += noise is not None
        moved += past
        if past or everything:
            mark = ("  worse" if change > 0 else "  better") if past else ""
            print(f"{k[0] if k[0] != last else '':<30} {k[1]:<20} {k[2]:<28} {x:>14,.3f} {y:>14,.3f} {change:>+8.1%}{mark}")
            last = k[0]
    if ratios:
        geo = math.exp(sum(map(math.log, ratios)) / len(ratios))
        print(f"\nserver instructions per row, geometric mean over {len(ratios)} paced phases: {geo - 1:+.2%}")
    print(f"{moved} of {judged} readings moved past their noise")
