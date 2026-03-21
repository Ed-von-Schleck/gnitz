"""
Shared workload and server-lifecycle helpers for GnitzDB benchmark scripts.

Used by perf_profile.py, compare_jit.py, and any future benchmark tooling.
All three previously contained near-identical copies of this code.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT  = Path(__file__).resolve().parent.parent
CLIENT_DIR = REPO_ROOT / "rust_client" / "gnitz-py"


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

def start_server(binary: Path, workers: int,
                 tmpdir_prefix: str = "bench_") -> tuple:
    """Start gnitz-server and wait for the socket to appear.

    Returns (proc, sock_path, tmpdir, log_file).
    """
    tmpdir    = tempfile.mkdtemp(dir=REPO_ROOT / "tmp", prefix=tmpdir_prefix)
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
        sys.exit(f"Server {binary.name} did not start within 10 s.  Log: {log_path}")

    return proc, sock_path, tmpdir, log_f


def stop_server(proc, tmpdir: str, log_f) -> None:
    """Kill server and clean up its temp directory."""
    proc.kill(); proc.wait(); log_f.close()
    shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Workload script (runs in a subprocess via `uv run python -c ...`)
# ---------------------------------------------------------------------------
# This is passed to a child process so that the gnitz client (installed via
# uv in rust_client/gnitz-py) is importable.  The child receives
# (sock_path, ticks, rows_per_tick) as sys.argv[1:4].

WORKLOAD_SCRIPT = r"""
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

# ── schemas ──────────────────────────────────────────────────────────────────
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

    # ── tables ───────────────────────────────────────────────────────────
    act_tid     = conn.create_table(sn, "activities", act_cols,     unique_pk=False)
    cust_tid    = conn.create_table(sn, "customers",  cust_cols)
    attrs_tid   = conn.create_table(sn, "cust_attrs", attrs_cols)
    blocked_tid = conn.create_table(sn, "blocked",    blocked_cols)

    # ── secondary indices ────────────────────────────────────────────────
    conn.execute_sql("CREATE INDEX ON activities(product_id)", schema_name=sn)
    conn.execute_sql("CREATE INDEX ON activities(region_id)",  schema_name=sn)

    # ── pre-populate dimension tables ────────────────────────────────────
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

    # ── predicates ───────────────────────────────────────────────────────
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

    # ── views ────────────────────────────────────────────────────────────
    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.join(cb.input_delta(), cust_tid))
    vid_enriched = conn.create_view_with_circuit(
        sn, "v_enriched", cb.build(), enriched_cols)

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.join(cb.join(cb.input_delta(), cust_tid), attrs_tid))
    vid_enriched2 = conn.create_view_with_circuit(
        sn, "v_enriched2", cb.build(), enriched2_cols)

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.semi_join(cb.input_delta(), cust_tid))
    vid_active = conn.create_view_with_circuit(
        sn, "v_active", cb.build(), act_cols)

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.anti_join(cb.input_delta(), blocked_tid))
    vid_unblocked = conn.create_view_with_circuit(
        sn, "v_unblocked", cb.build(), act_cols)

    cb = conn.circuit_builder(source_table_id=vid_enriched)
    cb.sink(cb.reduce(cb.input_delta(), group_by_cols=[2],
                      agg_func_id=AGG_SUM, agg_col_idx=4))
    vid_rev_region = conn.create_view_with_circuit(
        sn, "v_rev_region", cb.build(), reduce2_cols("region_id"))

    cb = conn.circuit_builder(source_table_id=vid_enriched)
    cb.sink(cb.reduce(cb.input_delta(), group_by_cols=[6],
                      agg_func_id=AGG_SUM, agg_col_idx=4))
    vid_rev_tier = conn.create_view_with_circuit(
        sn, "v_rev_tier", cb.build(), reduce2_cols("tier"))

    cb = conn.circuit_builder(source_table_id=vid_enriched)
    inp = cb.input_delta()
    cb.sink(cb.reduce(cb.filter(inp, pred_enterprise),
                      group_by_cols=[2], agg_func_id=AGG_COUNT, agg_col_idx=4))
    vid_premium_cnt = conn.create_view_with_circuit(
        sn, "v_premium_cnt", cb.build(), reduce2_cols("region_id"))

    cb = conn.circuit_builder(source_table_id=vid_rev_region)
    cb.sink(cb.filter(cb.input_delta(), pred_hot))
    vid_hot = conn.create_view_with_circuit(
        sn, "v_hot_regions", cb.build(), reduce2_cols("region_id"))

    cb  = conn.circuit_builder(source_table_id=act_tid)
    inp = cb.input_delta()
    cb.sink(cb.distinct(cb.union(cb.filter(inp, pred_high_rev),
                                 cb.filter(inp, pred_high_qty))))
    vid_flagged = conn.create_view_with_circuit(
        sn, "v_flagged", cb.build(), act_cols)

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.reduce(cb.input_delta(), group_by_cols=[2],
                      agg_func_id=AGG_COUNT, agg_col_idx=4))
    vid_region_cnt = conn.create_view_with_circuit(
        sn, "v_region_cnt", cb.build(), reduce2_cols("region_id"))

    result["setup_s"] = time.perf_counter() - t0

    # ── tick loop ────────────────────────────────────────────────────────
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
                             quantity=1 + (i % 10), revenue=100 + (i % 1000),
                             weight=-1)

        base = tick * rows_per_tick
        for i in range(rows_per_tick):
            cid = (base + i) % NUM_CUSTOMERS
            pid = (base + i) % NUM_PRODUCTS
            rid = cid % NUM_REGIONS
            batch.append(pk=cid, product_id=pid, region_id=rid,
                         quantity=1 + (i % 10), revenue=100 + (i % 1000))

        t_build = time.perf_counter() - t_b0

        t_p0  = time.perf_counter()
        conn.push(act_tid, batch)
        t_push = time.perf_counter() - t_p0

        result["tick_build"].append(round(t_build, 4))
        result["tick_push"].append(round(t_push, 4))

    # ── scan all views ───────────────────────────────────────────────────
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

    # ── seek / seek_by_index ─────────────────────────────────────────────
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
# View notes (for pretty-printing)
# ---------------------------------------------------------------------------

VIEW_NOTES: dict[str, str] = {
    "v_enriched":    "join(act, customers)",
    "v_enriched2":   "join×2 chained 3-table",
    "v_active":      "semi_join(act, customers)",
    "v_unblocked":   "anti_join(act, blocked)",
    "v_rev_region":  "view-on-view reduce",
    "v_rev_tier":    "view-on-view reduce (fan-out)",
    "v_premium_cnt": "view-on-view filter+reduce",
    "v_hot_regions": "3rd-hop filter",
    "v_flagged":     "union+distinct",
    "v_region_cnt":  "direct reduce",
}


# ---------------------------------------------------------------------------
# Run workload via subprocess
# ---------------------------------------------------------------------------

def run_workload(sock_path: str, ticks: int, rows: int) -> dict:
    """Run the benchmark workload via `uv run` and return parsed timing dict."""
    proc = subprocess.run(
        ["uv", "run", "python", "-c", WORKLOAD_SCRIPT,
         sock_path, str(ticks), str(rows)],
        cwd=CLIENT_DIR,
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise WorkloadError(proc.stderr)
    return json.loads(proc.stdout.strip())


class WorkloadError(Exception):
    """Raised when the benchmark subprocess fails."""
    pass


# ---------------------------------------------------------------------------
# Benchmark history — append-only JSONL log in tmp/bench_history/
# ---------------------------------------------------------------------------
HISTORY_DIR  = REPO_ROOT / "bench_history"
HISTORY_FILE = HISTORY_DIR / "history.jsonl"


def _git_commit_hash() -> str:
    """Return the current HEAD short hash, or 'unknown'."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT, stderr=subprocess.DEVNULL, text=True,
        ).strip()
    except Exception:
        return "unknown"


def _git_dirty() -> bool:
    """Return True if the working tree has uncommitted changes."""
    try:
        r = subprocess.run(
            ["git", "diff", "--quiet", "HEAD"],
            cwd=REPO_ROOT, stderr=subprocess.DEVNULL,
        )
        return r.returncode != 0
    except Exception:
        return False


def save_to_history(timing: dict, *, mode: str, ticks: int, rows: int,
                    workers: int, tool: str, **extra) -> Path:
    """Append a benchmark record to the history JSONL file.

    Returns the path to the history file.
    """
    from datetime import datetime, timezone

    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    commit = _git_commit_hash()
    dirty = _git_dirty()

    total_push  = sum(timing.get("tick_push", []))
    total_scan  = sum(v["time_s"] for v in timing.get("scan", {}).values())
    total_rows  = ticks * rows
    throughput  = round(total_rows / total_push) if total_push > 0 else 0

    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "commit": commit,
        "dirty": dirty,
        "mode": mode,
        "tool": tool,
        "ticks": ticks,
        "rows_per_tick": rows,
        "workers": workers,
        "setup_s": timing.get("setup_s", 0),
        "total_push_s": round(total_push, 4),
        "total_scan_s": round(total_scan, 4),
        "seek_s": timing.get("seek_s", 0),
        "throughput_rows_per_sec": throughput,
    }

    # Include realistic stats if present
    if "realistic" in timing:
        r = timing["realistic"]
        record["num_clients"] = r.get("num_clients", 0)
        record["total_pushes"] = r.get("total_pushes", 0)
        record["push_latency_ms"] = r.get("push_latency_ms", {})
        record["concurrent_scan_latency_ms"] = r.get("concurrent_scan_latency_ms", {})

    record.update(extra)

    with open(HISTORY_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")

    return HISTORY_FILE


def load_history() -> list[dict]:
    """Load all records from the history JSONL file."""
    if not HISTORY_FILE.exists():
        return []
    records = []
    with open(HISTORY_FILE) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


# ---------------------------------------------------------------------------
# Realistic multi-client workload
# ---------------------------------------------------------------------------
# Exercises realistic access patterns: multiple concurrent writer processes
# pushing varied batch sizes (70% single-row, 20% small, 10% medium) with a
# 50/30/20 insert/update/delete operation mix, plus a concurrent reader
# scanning views during the push phase.

WORKLOAD_REALISTIC_SCRIPT = r"""
import sys, os, time, json, random, multiprocessing

sock_path     = sys.argv[1]
ticks         = int(sys.argv[2])
rows_per_tick = int(sys.argv[3])
num_clients   = int(sys.argv[4])

NUM_CUSTOMERS = 500
NUM_PRODUCTS  = 100
NUM_REGIONS   = 10
NUM_BLOCKED   = 50

AGG_COUNT = 1
AGG_SUM   = 2


def generate_batch_sizes(total_rows, rng):
    # Draw batch sizes from realistic distribution until they sum to total_rows.
    sizes = []
    remaining = total_rows
    while remaining > 0:
        r = rng.random()
        if r < 0.70:
            sz = 1
        elif r < 0.90:
            sz = rng.randint(2, 50)
        else:
            sz = rng.randint(50, 500)
        sz = min(sz, remaining)
        sizes.append(sz)
        remaining -= sz
    rng.shuffle(sizes)
    return sizes


def writer_worker(sock_path, act_tid, worker_id, num_workers,
                  rows_per_tick, ticks, barrier, queue):
    # Each writer pushes its share of rows per tick with realistic op mix.
    import gnitz as _gnitz

    act_cols = [
        _gnitz.ColumnDef("pk",         _gnitz.TypeCode.U64, primary_key=True),
        _gnitz.ColumnDef("product_id", _gnitz.TypeCode.I64),
        _gnitz.ColumnDef("region_id",  _gnitz.TypeCode.I64),
        _gnitz.ColumnDef("quantity",   _gnitz.TypeCode.I64),
        _gnitz.ColumnDef("revenue",    _gnitz.TypeCode.I64),
    ]
    act_schema = _gnitz.Schema(act_cols)

    my_rows = rows_per_tick // num_workers
    if worker_id < rows_per_tick % num_workers:
        my_rows += 1

    rng = random.Random(42 + worker_id)
    seen_pks = []
    pk_counter = worker_id  # strided by num_workers

    with _gnitz.connect(sock_path) as conn:
        for tick in range(ticks):
            barrier.wait()  # tick start

            batch_sizes = generate_batch_sizes(my_rows, rng)
            push_latencies = []
            t_build_total = 0.0
            t_push_total = 0.0

            row_idx = 0
            for bsz in batch_sizes:
                t_b0 = time.perf_counter()
                batch = _gnitz.ZSetBatch(act_schema)

                for _ in range(bsz):
                    r = rng.random()
                    if len(seen_pks) == 0 or r < 0.50:
                        # Insert
                        pk = pk_counter
                        pk_counter += num_workers
                        weight = 1
                        seen_pks.append(pk)
                    elif r < 0.80:
                        # Update (new values, weight=+1)
                        pk = rng.choice(seen_pks)
                        weight = 1
                    else:
                        # Delete (weight=-1)
                        pk = rng.choice(seen_pks)
                        weight = -1

                    cid = pk % NUM_CUSTOMERS
                    pid = pk % NUM_PRODUCTS
                    rid = cid % NUM_REGIONS
                    batch.append(pk=pk, product_id=pid, region_id=rid,
                                 quantity=1 + (pk % 10),
                                 revenue=100 + (pk % 1000),
                                 weight=weight)
                    row_idx += 1

                t_build = time.perf_counter() - t_b0
                t_build_total += t_build

                t_p0 = time.perf_counter()
                conn.push(act_tid, batch)
                t_push = time.perf_counter() - t_p0

                t_push_total += t_push
                push_latencies.append((bsz, t_push))

            barrier.wait()  # tick end
            queue.put((tick, worker_id, t_build_total, t_push_total, push_latencies))


def reader_worker(sock_path, view_ids_tuple, stop_event, queue):
    # Scan random views concurrently during the push phase.
    import gnitz as _gnitz

    rng = random.Random(999)
    view_names = [v[0] for v in view_ids_tuple]
    view_ids   = [v[1] for v in view_ids_tuple]

    with _gnitz.connect(sock_path) as conn:
        while not stop_event.is_set():
            idx = rng.randint(0, len(view_ids) - 1)
            vid = view_ids[idx]
            vname = view_names[idx]
            t0 = time.perf_counter()
            rows = list(conn.scan(vid))
            t_scan = time.perf_counter() - t0
            row_count = sum(1 for r in rows if r.weight > 0)
            queue.put(("scan", vname, t_scan, row_count))
            time.sleep(rng.uniform(0.005, 0.010))


# ── Main body ─────────────────────────────────────────────────────────────
import gnitz

result = {"tick_build": [], "tick_push": [], "scan": {}}

with gnitz.connect(sock_path) as conn:
    t0 = time.perf_counter()
    sn = "bench"
    conn.create_schema(sn)

    # ── tables ────────────────────────────────────────────────────────────
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

    act_tid     = conn.create_table(sn, "activities", act_cols,     unique_pk=False)
    cust_tid    = conn.create_table(sn, "customers",  cust_cols)
    attrs_tid   = conn.create_table(sn, "cust_attrs", attrs_cols)
    blocked_tid = conn.create_table(sn, "blocked",    blocked_cols)

    # ── secondary indices ─────────────────────────────────────────────────
    conn.execute_sql("CREATE INDEX ON activities(product_id)", schema_name=sn)
    conn.execute_sql("CREATE INDEX ON activities(region_id)",  schema_name=sn)

    # ── pre-populate dimension tables ─────────────────────────────────────
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

    # ── predicates ────────────────────────────────────────────────────────
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

    # ── views ─────────────────────────────────────────────────────────────
    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.join(cb.input_delta(), cust_tid))
    vid_enriched = conn.create_view_with_circuit(
        sn, "v_enriched", cb.build(), enriched_cols)

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.join(cb.join(cb.input_delta(), cust_tid), attrs_tid))
    vid_enriched2 = conn.create_view_with_circuit(
        sn, "v_enriched2", cb.build(), enriched2_cols)

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.semi_join(cb.input_delta(), cust_tid))
    vid_active = conn.create_view_with_circuit(
        sn, "v_active", cb.build(), act_cols)

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.anti_join(cb.input_delta(), blocked_tid))
    vid_unblocked = conn.create_view_with_circuit(
        sn, "v_unblocked", cb.build(), act_cols)

    cb = conn.circuit_builder(source_table_id=vid_enriched)
    cb.sink(cb.reduce(cb.input_delta(), group_by_cols=[2],
                      agg_func_id=AGG_SUM, agg_col_idx=4))
    vid_rev_region = conn.create_view_with_circuit(
        sn, "v_rev_region", cb.build(), reduce2_cols("region_id"))

    cb = conn.circuit_builder(source_table_id=vid_enriched)
    cb.sink(cb.reduce(cb.input_delta(), group_by_cols=[6],
                      agg_func_id=AGG_SUM, agg_col_idx=4))
    vid_rev_tier = conn.create_view_with_circuit(
        sn, "v_rev_tier", cb.build(), reduce2_cols("tier"))

    cb = conn.circuit_builder(source_table_id=vid_enriched)
    inp = cb.input_delta()
    cb.sink(cb.reduce(cb.filter(inp, pred_enterprise),
                      group_by_cols=[2], agg_func_id=AGG_COUNT, agg_col_idx=4))
    vid_premium_cnt = conn.create_view_with_circuit(
        sn, "v_premium_cnt", cb.build(), reduce2_cols("region_id"))

    cb = conn.circuit_builder(source_table_id=vid_rev_region)
    cb.sink(cb.filter(cb.input_delta(), pred_hot))
    vid_hot = conn.create_view_with_circuit(
        sn, "v_hot_regions", cb.build(), reduce2_cols("region_id"))

    cb  = conn.circuit_builder(source_table_id=act_tid)
    inp = cb.input_delta()
    cb.sink(cb.distinct(cb.union(cb.filter(inp, pred_high_rev),
                                 cb.filter(inp, pred_high_qty))))
    vid_flagged = conn.create_view_with_circuit(
        sn, "v_flagged", cb.build(), act_cols)

    cb = conn.circuit_builder(source_table_id=act_tid)
    cb.sink(cb.reduce(cb.input_delta(), group_by_cols=[2],
                      agg_func_id=AGG_COUNT, agg_col_idx=4))
    vid_region_cnt = conn.create_view_with_circuit(
        sn, "v_region_cnt", cb.build(), reduce2_cols("region_id"))

    result["setup_s"] = time.perf_counter() - t0

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

# ── Spawn workers ─────────────────────────────────────────────────────────
barrier     = multiprocessing.Barrier(num_clients)
timing_q    = multiprocessing.Queue()
scan_q      = multiprocessing.Queue()
stop_event  = multiprocessing.Event()

view_ids_tuple = tuple(view_ids.items())

writers = []
for wid in range(num_clients):
    p = multiprocessing.Process(
        target=writer_worker,
        args=(sock_path, act_tid, wid, num_clients,
              rows_per_tick, ticks, barrier, timing_q),
    )
    p.start()
    writers.append(p)

reader = multiprocessing.Process(
    target=reader_worker,
    args=(sock_path, view_ids_tuple, stop_event, scan_q),
)
reader.start()

for p in writers:
    p.join()

stop_event.set()
reader.join(timeout=5)
if reader.is_alive():
    reader.kill()
    reader.join()

# ── Drain queues and aggregate ────────────────────────────────────────────
tick_data = {}  # tick_idx -> {build: [per_worker], push: [per_worker], latencies: [...]}
while not timing_q.empty():
    tick_idx, worker_id, t_build, t_push, push_latencies = timing_q.get()
    if tick_idx not in tick_data:
        tick_data[tick_idx] = {"build": [], "push": [], "latencies": []}
    tick_data[tick_idx]["build"].append(t_build)
    tick_data[tick_idx]["push"].append(t_push)
    tick_data[tick_idx]["latencies"].extend(push_latencies)

scan_records = []
while not scan_q.empty():
    rec = scan_q.get()
    if rec[0] == "scan":
        scan_records.append({"view": rec[1], "time_s": rec[2], "rows": rec[3]})

# Per-tick: wall-clock = max across workers
for tick_idx in sorted(tick_data.keys()):
    td = tick_data[tick_idx]
    result["tick_build"].append(round(max(td["build"]), 4))
    result["tick_push"].append(round(max(td["push"]), 4))

# Aggregate all push latencies
all_latencies_ms = []
batch_size_dist = {"1": 0, "2_50": 0, "50_500": 0}
op_counts = {"insert": 0, "update": 0, "delete": 0}
total_pushes = 0

for td in tick_data.values():
    for bsz, t_push in td["latencies"]:
        all_latencies_ms.append(t_push * 1000.0)
        total_pushes += 1
        if bsz == 1:
            batch_size_dist["1"] += 1
        elif bsz <= 50:
            batch_size_dist["2_50"] += 1
        else:
            batch_size_dist["50_500"] += 1

# Estimate op counts from distribution parameters
total_rows_pushed = sum(rows_per_tick for _ in range(ticks))
est_insert = int(total_rows_pushed * 0.50)
est_update = int(total_rows_pushed * 0.30)
est_delete = total_rows_pushed - est_insert - est_update
op_counts = {"insert": est_insert, "update": est_update, "delete": est_delete}

# Percentiles
if all_latencies_ms:
    all_latencies_ms.sort()
    n = len(all_latencies_ms)
    def pctl(p):
        idx = min(int(p / 100.0 * n), n - 1)
        return round(all_latencies_ms[idx], 3)
    push_pctls = {
        "p50": pctl(50), "p90": pctl(90), "p95": pctl(95), "p99": pctl(99),
        "mean": round(sum(all_latencies_ms) / n, 3),
    }
else:
    push_pctls = {"p50": 0, "p90": 0, "p95": 0, "p99": 0, "mean": 0}

# Concurrent scan percentiles
scan_latencies_ms = [s["time_s"] * 1000.0 for s in scan_records]
if scan_latencies_ms:
    scan_latencies_ms.sort()
    sn_count = len(scan_latencies_ms)
    def spctl(p):
        idx = min(int(p / 100.0 * sn_count), sn_count - 1)
        return round(scan_latencies_ms[idx], 3)
    scan_pctls = {"p50": spctl(50), "p90": spctl(90), "count": sn_count}
else:
    scan_pctls = {"p50": 0, "p90": 0, "count": 0}

# ── Final scans + seeks (same as throughput workload) ─────────────────────
with gnitz.connect(sock_path) as conn:
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

result["realistic"] = {
    "num_clients": num_clients,
    "total_pushes": total_pushes,
    "batch_size_distribution": batch_size_dist,
    "operation_mix": op_counts,
    "push_latency_ms": push_pctls,
    "concurrent_scan_latency_ms": scan_pctls,
}

print(json.dumps(result))
"""


def run_realistic_workload(sock_path: str, ticks: int, rows: int,
                           num_clients: int = 4) -> dict:
    """Run the realistic multi-client workload and return parsed timing dict."""
    proc = subprocess.run(
        ["uv", "run", "python", "-c", WORKLOAD_REALISTIC_SCRIPT,
         sock_path, str(ticks), str(rows), str(num_clients)],
        cwd=CLIENT_DIR,
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise WorkloadError(proc.stderr)
    return json.loads(proc.stdout.strip())
