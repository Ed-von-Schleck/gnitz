#!/usr/bin/env python3
"""
perf_profile.py — Release-binary perf profiling for GnitzDB optimization work.

QUICK START
===========
From the repo root:

    python scripts/perf_profile.py                              # CPU cycles profile
    python scripts/perf_profile.py --rebuild                    # rebuild release binary first (~4 min)
    python scripts/perf_profile.py --ticks 20                   # longer run → more stable samples
    python scripts/perf_profile.py --dwarf                      # DWARF call-graph (full stacks, +20% overhead)
    python scripts/perf_profile.py --events cache-misses        # cache miss profile instead of cycles
    python scripts/perf_profile.py --annotate pypy_g_ArenaZSetBatch__direct_append_row
                                                                # instruction-level drill-down after a run

Results are always saved to tmp/perf_runs/<timestamp>/ and printed to stdout.


WHAT THIS DOES
==============
1.  Optionally rebuilds gnitz-server-release-c via `make release-server`
    (flags: --opt=jit --gc=incminimark --lto).

2.  Starts the release server with N workers (default 4, matching GNITZ_WORKERS=4
    required for multi-core tests).

3.  Attaches `perf record -F 99` to the server master PID and all worker PIDs.
    Sampling at 99 Hz avoids lock-step resonance with 100 Hz kernel timers.

4.  Runs the heavy-aggregation benchmark:
      - N ticks × 50 K rows inserted into a single "events" table
      - 7 concurrent reduce views (by region/product/store × count/sum, plus a
        filtered view), covering the full ingest → operator → storage path.

5.  Stops perf, copies the recording to tmp/perf_runs/<timestamp>/perf.data,
    then runs `perf report` and parses the flat symbol profile.

6.  Prints and saves:
      - Flat profile table (top symbols at ≥0.2% coverage)
      - Category breakdown (shows where CPU time really goes)
      - Ranked optimization targets with specific, actionable guidance
      - Commands for further investigation (annotate, diff, flamegraph)


HOW TO INTERPRET THE PROFILE
=============================
The release binary (--opt=jit --gc=incminimark --lto) has no debug-allocator
overhead.  Symbols you will see fall into a few kinds:

  pypy_g_<ClassName>_<method>   — RPython-generated C for a gnitz Python class
  pypy_g_<function_name>        — RPython-generated C for a module-level function
  __memmove_avx512_*            — glibc SIMD bulk memory copy (the fast path;
                                  high % here means bulk-copy rewrites are working)
  _int_malloc / malloc          — glibc heap allocator; high % = allocation churn
  pypy_g_IncrementalMiniMarkGC* — RPython GC; follows from allocation rate
  [k] 0xffffffff...             — kernel addresses (scheduling / syscall noise)

The profile is FLAT (--no-children): each symbol's % is the fraction of samples
where that function was at the top of the call stack (i.e., actually executing,
not waiting for a callee).  This is the right view for finding CPU hotspots.


FINDING OPTIMIZATION TARGETS
==============================
The script groups symbols into categories and highlights those above threshold:

  Bulk copy     — per-row fallback, indexed copy, consolidate (target: ↓)
  Allocation    — glibc + RPython arena allocators (target: ↓)
  GC            — GC tracing; follows allocation rate (target: ↓)
  Column access — virtual dispatch on RowAccessor / ColumnarBatchAccessor (target: ↓)
  Disk I/O      — shard writes, manifest flushes, fsync (target: ↓ or defer)
  Sort / hash   — mergesort, argsort, XXH3 (target: ↓ for large N)
  DBSP ops      — operator dispatch overhead (target: ↓)
  Bulk memory   — __memmove_avx512_* (desirable; ↑ means bulk rewrites working)

A category consuming ≥5 % of samples is worth a focused investigation.
Within a category, start with the symbol highest on the flat profile.

Symbols generally NOT worth optimizing from Python/C:
  __memmove_avx512_*    this IS the optimized path; drive more work through it
  fsync / __syscall_*   kernel I/O, not controllable from userspace
  [k] 0xffffffff...     scheduling / interrupt noise


CACHE MISS / HARDWARE COUNTER PROFILING
========================================
To find which functions cause the most cache misses (L1, LLC, TLB, branch
mispredictions, etc.) use the --events flag:

    # L1 data-cache load misses:
    python scripts/perf_profile.py --events L1-dcache-load-misses

    # Last-level cache misses (RAM accesses):
    python scripts/perf_profile.py --events LLC-load-misses

    # Branch mispredictions:
    python scripts/perf_profile.py --events branch-misses

    # Multiple events in one run (comma-separated):
    python scripts/perf_profile.py --events cycles,cache-misses

List available hardware events on this machine:
    perf list hw cache

The flat profile and category breakdown adapt automatically — the "%" column
now shows miss share instead of CPU-time share.

INSTRUCTION-LEVEL DRILL-DOWN (--annotate)
==========================================
Once the flat profile identifies a hot function, --annotate opens
`perf annotate` on that symbol and prints the assembly annotated with
hit/miss counts per instruction.  This tells you *exactly which instruction*
inside the function is the bottleneck.

    python scripts/perf_profile.py --annotate pypy_g_ArenaZSetBatch__direct_append_row

The annotation runs against the most recent recording in tmp/perf_runs/.
To annotate a specific earlier run:
    perf annotate -i tmp/perf_runs/<timestamp>/perf.data --stdio \\
        pypy_g_ArenaZSetBatch__direct_append_row | head -80

WHAT perf annotate SHOWS AND WHY
---------------------------------
GnitzDB has a three-layer compilation stack:

  Python source  →  RPython translator  →  generated C  →  gcc/clang  →  binary

perf samples the final binary.  What you see in --annotate depends on
what debug information is in that binary:

  Release binary (default, --opt=jit --lto):
    No -g; no DWARF.  perf annotate shows raw x86-64 assembly only.
    This is still useful: you can see which instruction inside the hot
    loop is stalling (a load from a cold cache line, a branch that keeps
    missing, a division, etc.).

  Dev binary (--opt=1 --lldebug, built by `make server`):
    RPython passes --lldebug which adds -g to the gcc invocation.
    perf annotate --source maps samples to lines in the *generated C*
    (usually in /tmp/usession-*/testing_1/annots/ or similar).
    The generated C is verbose but readable: variable names follow the
    Python source, and the structure mirrors the original class methods.
    Cross-referencing a hot generated-C line back to gnitz/*.py is
    usually a 30-second grep on the surrounding variable/function names.

  To get source-level annotation from the dev binary:
    1. make server                              # builds gnitz-server-c with -g
    2. python scripts/perf_profile.py          # runs without --release flag...
       (edit BINARY at top of script to point to gnitz-server-c temporarily)
    3. perf annotate -i tmp/perf_runs/<ts>/perf.data --stdio --source \\
           pypy_g_ArenaZSetBatch__direct_append_row | head -100

  The dev binary has pypy_debug_alloc_stop dominating the profile (~62% of
  samples in our baseline run).  Ignore that symbol — it is a debug-allocator
  accounting hook absent from release builds.  All other symbols are real.

COMPARING TWO RUNS
==================
Each run saves a report.json (machine-readable) and perf.data (raw):

    # Side-by-side diff of raw perf data:
    perf diff tmp/perf_runs/<old>/perf.data tmp/perf_runs/<new>/perf.data

    # Compare JSON summaries with jq:
    jq '.categories' tmp/perf_runs/<old>/report.json
    jq '.categories' tmp/perf_runs/<new>/report.json


PREREQUISITES
=============
  - perf on PATH:
        perf stat -- true

  - kernel.perf_event_paranoid ≤ 1 (required to sample without root):
        cat /proc/sys/kernel/perf_event_paranoid
        sudo sysctl -w kernel.perf_event_paranoid=1

  - gnitz-server-release-c present (or pass --rebuild):
        ls -lh gnitz-server-release-c

  - uv with the gnitz-py project (used to import the gnitz client):
        cd crates/gnitz-py && uv sync
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import signal
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
BINARY      = REPO_ROOT / "gnitz-server-release-c"
RUNS_DIR    = REPO_ROOT / "tmp" / "perf_runs"
PERF_TMP    = Path("/tmp/gnitz_perf.data")

# ---------------------------------------------------------------------------
# Colour helpers (auto-disabled when stdout is not a tty)
# ---------------------------------------------------------------------------
_COLOUR = sys.stdout.isatty()

def _c(code: str, s: str) -> str:
    return f"\033[{code}m{s}\033[0m" if _COLOUR else s

def bold(s: str)   -> str: return _c("1",    s)
def dim(s: str)    -> str: return _c("2",    s)
def green(s: str)  -> str: return _c("32",   s)
def yellow(s: str) -> str: return _c("33",   s)
def red(s: str)    -> str: return _c("31",   s)
def cyan(s: str)   -> str: return _c("36",   s)
def magenta(s: str)-> str: return _c("35",   s)

# ---------------------------------------------------------------------------
# Symbol category table
# ---------------------------------------------------------------------------
# Format: (key, display_label, is_fast_path, [(regex, short_description), ...])
# is_fast_path=True means high % here is *desirable* (e.g. memmove).
# First matching (regex, category) wins for each symbol.

CATEGORIES: list[tuple[str, str, bool, list[tuple[str, str]]]] = [
    ("memmove", "Bulk memory (fast path)", True, [
        (r"memmove|memcpy|memset", "glibc SIMD bulk copy — the fast path; ↑ is good"),
    ]),
    ("bulk_copy", "Batch bulk copy", False, [
        (r"_direct_append_row",              "per-row fallback (string cols bypass bulk path)"),
        (r"_copy_rows_indexed_src_weights",  "indexed copy with weight remap (col_ptr per row)"),
        (r"_copy_rows_indexed\b",            "indexed copy, no weight remap (col_ptr per row)"),
        (r"to_consolidated",                 "merge + consolidate in reduce hot path"),
        (r"append_batch\b",                  "bulk batch append"),
    ]),
    ("alloc", "Allocation churn", False, [
        (r"_int_malloc|^malloc$|calloc",              "glibc heap allocator"),
        (r"_int_free|cfree@|free@|malloc_consolidate"
         r"|unlink_chunk|_int_free_(create|merge)_chunk", "glibc heap free"),
        (r"ArenaCollection_malloc",                   "RPython arena allocator"),
        (r"ArenaZSetBatch___init__",                  "batch arena allocation"),
        (r"MemTable___init__",                        "memtable allocation on flush"),
    ]),
    ("gc", "GC overhead", False, [
        (r"IncrementalMiniMarkGC",                    "RPython incremental GC"),
        (r"trace_drag_out|gc_callback|\.visit\b",     "GC tracing / object marking"),
        (r"ArenaZSetBatch_free",                      "batch deallocation"),
    ]),
    ("io", "Disk I/O", False, [
        (r"_write_shard_file",                        "shard WAL write"),
        (r"_build_shard_image",                       "shard image serialization before write"),
        (r"_copy_and_record",                         "record copy during shard write"),
        (r"publish_new_version",                      "manifest flush to disk"),
        (r"ShardHandle___init__",                     "shard file open"),
        (r"TableShardView___init__",                  "shard view construction"),
        (r"open__str",                                "file open syscall wrapper"),
        (r"fsync|__syscall_cancel",                   "blocking kernel I/O syscall"),
        (r"EphemeralTable__scan_shards",              "shard scan during pk lookup"),
        (r"FLSMIndex_run_compact",                    "FLSM compaction run"),
    ]),
    ("sort", "Sort / argsort", False, [
        (r"_mergesort_i64",                           "merge sort of I64 column"),
        (r"_argsort_delta",                           "argsort on delta batch"),
        (r"_insertion_sort_indices",                  "insertion sort (small-N fast path)"),
        (r"_build_sorted_indices",                    "initial sort index array construction"),
        (r"compare_rows|compare_indices|_compare_by_cols", "row comparator"),
    ]),
    ("hash", "Hashing", False, [
        (r"XXH3|xxhash",                              "XXH3 content hash for group keys"),
        (r"_get_h0_h1_h2",                            "XOR-8 filter hash expansion"),
        (r"build_xor8",                               "XOR-8 filter construction on shard flush"),
        (r"BloomFilter",                              "bloom filter probe"),
    ]),
    ("accessor", "Column accessors", False, [
        (r"_read_col_int\b",                          "direct column int read"),
        (r"ColumnarBatchAccessor",                    "columnar accessor (virtual dispatch)"),
        (r"SoAAccessor",                              "SoA accessor (virtual dispatch)"),
        (r"ll_call_lookup_function",                  "RPython virtual dispatch trampoline"),
    ]),
    ("operators", "DBSP operators", False, [
        (r"op_reduce\b",                              "reduce operator"),
        (r"op_filter\b",                              "filter operator"),
        (r"op_join\b",                                "join operator"),
        (r"op_anti_join",                             "anti-join operator"),
        (r"op_semi_join",                             "semi-join operator"),
        (r"op_union\b",                               "union operator"),
        (r"op_distinct\b",                            "distinct operator"),
        (r"ingest_to_family|fan_out_push|_relay_exchange", "ingest / fan-out pipeline"),
    ]),
    ("ingest", "Ingest / scatter", False, [
        (r"PartitionedTable_ingest_batch",            "partition scatter loop"),
        (r"TableShard.*(?:add|upsert)|upsert_batch",  "shard-level ingest"),
        (r"MemTable.*(?:upsert|flush)",               "memtable upsert / flush"),
        (r"MemTable__total_bytes",                    "memtable size check (flush decision, per-row)"),
    ]),
    ("scan", "Scan / merge", False, [
        (r"TournamentTree__compare_nodes",            "tournament tree node comparison"),
        (r"TournamentTree__sift_down",                "tournament tree heapify"),
        (r"TournamentTree_advance_cursor",            "tournament tree cursor advance"),
        (r"SortedBatchCursor|ShardCursor",             "sorted shard / shard cursor"),
        (r"EphemeralTable__build_cursor",             "ephemeral table cursor construction"),
    ]),
]

def categorise(sym: str) -> tuple[str, str, bool, str]:
    """Return (key, label, is_fast_path, description) for a symbol."""
    for key, label, fast, patterns in CATEGORIES:
        for pat, desc in patterns:
            if re.search(pat, sym, re.IGNORECASE):
                return key, label, fast, desc
    return "other", "Other / kernel", False, sym


# ---------------------------------------------------------------------------
# Optimization guidance: fired when a category's share exceeds a threshold
# ---------------------------------------------------------------------------
# Maps category key → {threshold_pct, targets: {regex → advice_text}}

GUIDANCE: dict[str, dict] = {
    "bulk_copy": {
        "threshold": 5.0,
        "targets": {
            r"_direct_append_row": (
                "String columns force per-row fallback in append_batch.\n"
                "    Fix:  Two-pass string copy — bulk-memmove the fixed-width fields first,\n"
                "          then relocate string offsets + blob data in a second pass.\n"
                "    File: gnitz/core/batch.py  (_direct_append_row, append_batch)"
            ),
            r"_copy_rows_indexed": (
                "col_ptr lookup (col_strides[ci], col_bufs[ci].base_ptr) happens per-row\n"
                "    per-column inside the indexed copy loop.\n"
                "    Fix:  Hoist col_strides[] and base_ptr[] into a fixed-size stack array\n"
                "          (or local variables for small column counts) before the row loop.\n"
                "    File: gnitz/core/batch.py  (_copy_rows_indexed, _copy_rows_indexed_src_weights)"
            ),
            r"to_consolidated": (
                "Merge + consolidate is called on every reduce tick regardless of delta size.\n"
                "    Fix:  When the incoming delta is already sorted and the trace is empty,\n"
                "          skip the merge and write directly to the output batch.\n"
                "    File: gnitz/core/batch.py  (to_consolidated)\n"
                "          gnitz/dbsp/ops/reduce.py  (op_reduce)"
            ),
        },
    },
    "alloc": {
        "threshold": 5.0,
        "targets": {
            r"malloc|ArenaCollection_malloc": (
                "High allocation rate drives GC pressure and cache misses.\n"
                "    Fix 1: Pre-size per-partition index lists with newlist_hint(expected_n)\n"
                "            instead of newlist_hint(0) in PartitionedTable.ingest_batch.\n"
                "    Fix 2: Reuse batch arenas across ticks — keep one ArenaZSetBatch per\n"
                "            partition and reset its arena rather than alloc+free each push.\n"
                "    Fix 3: Avoid transient list/tuple construction in hot loops.\n"
                "    File: gnitz/storage/partitioned_table.py  (ingest_batch)\n"
                "          gnitz/core/batch.py  (batch arena lifecycle)"
            ),
        },
    },
    "gc": {
        "threshold": 5.0,
        "targets": {
            r".*": (
                "GC time is a symptom of allocation rate.  Target the 'alloc' hotspots first.\n"
                "    Fix:  Reduce short-lived object creation in hot paths.  Where the GC\n"
                "          can't be avoided, use RPython fixed-size arena objects that the\n"
                "          collector can trace cheaply (no interior pointers)."
            ),
        },
    },
    "accessor": {
        "threshold": 3.0,
        "targets": {
            r"ColumnarBatchAccessor|SoAAccessor|ll_call_lookup_function": (
                "Virtual dispatch through ll_call_lookup_function adds one indirect call per\n"
                "    column read.  RPython cannot devirtualise unless the concrete type is known.\n"
                "    Fix:  Pass a concrete ColumnarBatchAccessor into the inner loop (not the\n"
                "          abstract RowAccessor) so RPython's specialiser can inline the read.\n"
                "    File: gnitz/core/batch.py, gnitz/dbsp/ops/*.py"
            ),
        },
    },
    "io": {
        "threshold": 8.0,
        "targets": {
            r"_write_shard_file|publish_new_version": (
                "Shard WAL write + manifest flush happen synchronously on every push tick.\n"
                "    Fix 1: Batch multiple shards before calling fsync (write N, fsync once).\n"
                "    Fix 2: Defer manifest writes behind a write-behind queue; fsync less often.\n"
                "    File: gnitz/storage/writer_table.py\n"
                "          gnitz/storage/manifest.py  (ManifestManager.publish_new_version)"
            ),
        },
    },
    "sort": {
        "threshold": 5.0,
        "targets": {
            r"_mergesort_i64|_argsort_delta": (
                "Sort is on the reduce critical path for every delta batch.\n"
                "    If _argsort_delta ≥1%: the small-N insertion-sort threshold may be too low\n"
                "    for typical per-partition deltas.  Try raising the threshold (currently ≤32)\n"
                "    so more deltas skip the merge-sort setup cost.\n"
                "    File: gnitz/dbsp/ops/reduce.py  (_argsort_delta, _mergesort_i64)"
            ),
        },
    },
    "hash": {
        "threshold": 3.0,
        "targets": {
            r"XXH3|_get_h0_h1_h2": (
                "Group-key hashing runs once per row in op_reduce.\n"
                "    If XXH3 ≥2%: verify that the hash is only computed once per delta row\n"
                "    (not re-hashed during lookup).  Consider caching the hash alongside pk.\n"
                "    File: gnitz/dbsp/ops/reduce.py, gnitz/storage/bloom.py, gnitz/storage/xor8.py"
            ),
        },
    },
    "scan": {
        "threshold": 3.0,
        "targets": {
            r"TournamentTree": (
                "TournamentTree drives every multi-shard view scan.  High % here means\n"
                "    the scan is bottlenecked on comparison/heapify, not I/O.\n"
                "    Fix 1: If compare_nodes ≥1%: pass a concrete batch type into the\n"
                "            comparator so RPython can inline the column read (avoids\n"
                "            virtual dispatch inside the tight comparison loop).\n"
                "    Fix 2: If sift_down ≥1%: ensure the tournament-tree arity matches\n"
                "            the typical shard fan-in; too large an arity amplifies sift_down cost.\n"
                "    File: gnitz/dbsp/tournament.py  (TournamentTree)\n"
                "          gnitz/storage/flsm.py     (scan path that builds the tree)"
            ),
            r"SortedBatchCursor|EphemeralTable__build_cursor": (
                "Cursor construction or column-read overhead on the shard scan path.\n"
                "    Fix: cache cursor objects across ticks instead of rebuilding per scan.\n"
                "    File: gnitz/storage/flsm.py  (SortedBatchCursor)\n"
                "          gnitz/storage/ephemeral_table.py  (EphemeralTable)"
            ),
        },
    },
}

# ---------------------------------------------------------------------------
# Perf report parsing
# ---------------------------------------------------------------------------
_ROW_RE   = re.compile(r"^\s+([\d.]+)%\s+(\d+)\s+\[.\]\s+(.+)$")
_SAMP_RE  = re.compile(r"#\s+Samples:\s+([\d.]+)([KMG]?)")
_CYCLE_RE = re.compile(r"#\s+Event count.*?:\s+(\d+)")

def _expand_si(n: str, suffix: str) -> int:
    v = float(n)
    return int(v * {"K": 1_000, "M": 1_000_000, "G": 1_000_000_000}.get(suffix, 1))

def parse_perf_report(perf_data: Path, percent_limit: float = 0.2) -> tuple[list[dict], dict]:
    """
    Run `perf report` on *perf_data* and return:
      (rows, meta)
    where rows is a list of dicts and meta has 'total_samples' and 'total_cycles'.
    """
    try:
        raw = subprocess.check_output(
            ["perf", "report", "-i", str(perf_data), "--stdio",
             "--no-children", "-n", "--sort", "symbol",
             f"--percent-limit={percent_limit}"],
            stderr=subprocess.DEVNULL, text=True, timeout=60,
        )
    except FileNotFoundError:
        sys.exit("'perf' not found on PATH.  Install linux-perf / perf-tools.")
    except subprocess.CalledProcessError as e:
        sys.exit(f"perf report failed (exit {e.returncode})")

    rows: list[dict] = []
    meta: dict = {"total_samples": 0, "total_cycles": 0}

    sm = _SAMP_RE.search(raw)
    if sm:
        meta["total_samples"] = _expand_si(sm.group(1), sm.group(2))
    cm = _CYCLE_RE.search(raw)
    if cm:
        meta["total_cycles"] = int(cm.group(1))

    for line in raw.splitlines():
        m = _ROW_RE.match(line)
        if m:
            pct, samps = float(m.group(1)), int(m.group(2))
            # perf appends IPC columns ("  -      -  ") after many spaces; strip them.
            sym = re.split(r"\s{4,}", m.group(3).strip())[0].strip()
            key, label, fast, desc = categorise(sym)
            rows.append(dict(
                pct=pct, samples=samps, symbol=sym,
                category=key, cat_label=label, is_fast_path=fast, desc=desc,
            ))
    return rows, meta


def aggregate_categories(rows: list[dict]) -> list[dict]:
    """Sum pct by category; return sorted descending by pct."""
    agg: dict[str, dict] = {}
    for r in rows:
        k = r["category"]
        if k not in agg:
            agg[k] = dict(key=k, label=r["cat_label"], is_fast_path=r["is_fast_path"],
                          pct=0.0, symbols=[])
        agg[k]["pct"] += r["pct"]
        agg[k]["symbols"].append(r)
    return sorted(agg.values(), key=lambda x: -x["pct"])


# ---------------------------------------------------------------------------
# perf attachment
# ---------------------------------------------------------------------------
def attach_perf(server_pid: int, perf_data: Path, dwarf: bool) -> subprocess.Popen:
    try:
        children = subprocess.check_output(
            ["pgrep", "-P", str(server_pid)], text=True,
        ).split()
    except subprocess.CalledProcessError:
        children = []

    all_pids = [str(server_pid)] + children
    pid_arg  = ",".join(all_pids)
    print(f"  attaching to PIDs: {pid_arg}")

    cg  = ["--call-graph", "dwarf"] if dwarf else []
    cmd = ["perf", "record", "-F", "99", "-p", pid_arg, "-o", str(perf_data)] + cg
    return subprocess.Popen(cmd, stderr=subprocess.DEVNULL)


def attach_perf_events(server_pid: int, perf_data: Path,
                       events: str, dwarf: bool) -> subprocess.Popen:
    """Like attach_perf but records arbitrary PMU events (e.g. cache-misses)."""
    try:
        children = subprocess.check_output(
            ["pgrep", "-P", str(server_pid)], text=True,
        ).split()
    except subprocess.CalledProcessError:
        children = []

    all_pids = [str(server_pid)] + children
    pid_arg  = ",".join(all_pids)
    print(f"  attaching to PIDs: {pid_arg}  events: {events}")

    cg  = ["--call-graph", "dwarf"] if dwarf else []
    cmd = ["perf", "record", "-e", events, "-F", "99", "-p", pid_arg, "-o", str(perf_data)] + cg
    return subprocess.Popen(cmd, stderr=subprocess.DEVNULL)


def stop_perf(perf_proc: subprocess.Popen) -> None:
    perf_proc.send_signal(signal.SIGINT)
    perf_proc.wait()






# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------
_SEP = "─" * 70

def _section(title: str) -> None:
    print()
    print(bold(cyan(_SEP)))
    print(bold(cyan(f"  {title}")))
    print(bold(cyan(_SEP)))

def _bar(pct: float, width: int = 30) -> str:
    filled = int(round(pct / 100.0 * width))
    return "█" * filled + dim("░" * (width - filled))

def _ljust_v(s: str, width: int) -> str:
    """Left-justify s to visual width, ignoring ANSI escape codes."""
    visible = len(re.sub(r"\033\[[^m]*m", "", s))
    return s + " " * max(0, width - visible)

def _pct_colour(pct: float, fast_path: bool = False) -> str:
    if fast_path:
        return green(f"{pct:5.1f}%")
    if pct >= 10:
        return red(f"{pct:5.1f}%")
    if pct >= 5:
        return yellow(f"{pct:5.1f}%")
    return f"{pct:5.1f}%"


def print_benchmark_timing(result: dict, ticks: int, rows: int) -> None:
    total_rows = ticks * rows
    total_build = sum(result["tick_build"])
    total_push  = sum(result["tick_push"])
    total_scan  = sum(v["time_s"] for v in result["scan"].values())
    seek_s      = result.get("seek_s", 0.0)
    throughput  = total_rows / total_push if total_push > 0 else 0

    print(f"  {'Tick':>4}  {'Build':>8}  {'Push':>8}  {'Push cumul':>10}")
    print(f"  {'----':>4}  {'--------':>8}  {'--------':>8}  {'----------':>10}")
    cum = 0.0
    for i, (b, p) in enumerate(zip(result["tick_build"], result["tick_push"])):
        cum += p
        print(f"  {i+1:>4}  {b*1000:>7.0f}ms  {p*1000:>7.0f}ms  {cum:>10.3f}s")

    print()
    print(f"  Setup        : {result['setup_s']*1000:.0f} ms")
    print(f"  Total build  : {total_build:.3f}s")
    print(f"  Total push   : {bold(f'{total_push:.3f}s')}")
    print(f"  Total scan   : {total_scan:.3f}s")
    print(f"  Seek/index   : {seek_s*1000:.0f} ms  (10 seek + 20 seek_by_index)")
    print(f"  Throughput   : {bold(f'{throughput:,.0f}')} rows/sec (push path)")
    print()
    print(f"  {'View':<20}  {'Scan':>8}  {'Groups':>7}  Note")
    print(f"  {'----':<20}  {'--------':>8}  {'-------':>7}  ----")
    notes = VIEW_NOTES
    for vname, v in result["scan"].items():
        note = notes.get(vname, "")
        print(f"  {vname:<20}  {v['time_s']*1000:>7.0f}ms  {v['groups']:>7}  {dim(note)}")


def print_realistic_timing(result: dict) -> None:
    """Print extended timing stats for the realistic multi-client workload."""
    r = result["realistic"]
    print()
    print(bold("  Realistic workload stats"))
    print(f"  Clients      : {r['num_clients']}")
    print(f"  Total pushes : {r['total_pushes']:,}")
    dist = r["batch_size_distribution"]
    print(f"  Batch sizes  : 1-row={dist['1']}  2-50={dist['2_50']}  50-500={dist['50_500']}")
    mix = r["operation_mix"]
    print(f"  Op mix       : insert={mix['insert']}  update={mix['update']}  delete={mix['delete']}")
    p = r["push_latency_ms"]
    print(f"  Push latency : p50={p['p50']:.1f}ms  p90={p['p90']:.1f}ms  "
          f"p95={p['p95']:.1f}ms  p99={p['p99']:.1f}ms  mean={p['mean']:.1f}ms")
    s = r["concurrent_scan_latency_ms"]
    print(f"  Scan latency : p50={s['p50']:.1f}ms  p90={s['p90']:.1f}ms  ({s['count']} scans)")


def print_flat_profile(rows: list[dict], meta: dict, events: str = "cycles:u") -> None:
    samples = meta["total_samples"]
    cycles  = meta["total_cycles"]
    cycle_s = f"  |  {cycles/1e9:.1f}B cycles" if cycles else ""
    event_s = f"  |  event: {events}" if events != "cycles:u" else ""
    _section(f"FLAT PROFILE  ({samples:,} samples{cycle_s}{event_s})")

    CAT_W = 24  # visible width for category column
    SYM_W = 58  # truncate C-mangled names beyond this
    print(f"  {'Rank':>4}  {'%':>6}  {'Samp':>5}  {'Category':<{CAT_W}}  Symbol")
    print(f"  {'----':>4}  {'------':>6}  {'-----':>5}  {'--------':<{CAT_W}}  ------")
    for i, r in enumerate(rows, 1):
        pct_s = _pct_colour(r["pct"], r["is_fast_path"])
        cat_s = _ljust_v(dim(f"[{r['cat_label']}]"), CAT_W)
        sym   = r["symbol"]
        if len(sym) > SYM_W:
            sym = sym[:SYM_W - 1] + "…"
        print(f"  {i:>4}  {pct_s}  {r['samples']:>5}  {cat_s}  {sym}")


def print_category_breakdown(cats: list[dict]) -> None:
    _section("CATEGORY BREAKDOWN")

    total_accounted = sum(c["pct"] for c in cats)
    print(f"  {'Category':<26}  {'  %':>6}  {'Bar':30}  Note")
    print(f"  {'--------':<26}  {'---':>6}  {'---':30}  ----")
    for c in cats:
        pct_s = _pct_colour(c["pct"], c["is_fast_path"])
        bar   = _bar(c["pct"])
        note  = green("fast path ↑ good") if c["is_fast_path"] else ""
        print(f"  {c['label']:<26}  {pct_s}  {bar}  {note}")
    print()
    print(f"  {dim('(symbols below 0.2% threshold not shown)')}")
    print(f"  {dim(f'Accounted: {total_accounted:.1f}% of samples')}")


def print_optimization_targets(cats: list[dict], rows: list[dict]) -> None:
    _section("OPTIMIZATION TARGETS")

    actionable = [
        c for c in cats
        if not c["is_fast_path"] and c["key"] in GUIDANCE
        and c["pct"] >= GUIDANCE[c["key"]]["threshold"]
    ]

    if not actionable:
        print(green("  No category exceeds its threshold.  Profile looks healthy."))
        return

    for rank, cat in enumerate(actionable, 1):
        g = GUIDANCE[cat["key"]]
        print()
        print(f"  {bold(yellow(f'#{rank}'))}  {bold(cat['label'])}  "
              f"({_pct_colour(cat['pct'])}  threshold {g['threshold']:.0f}%)")

        # List contributing symbols above 0.4%
        for sym_row in [r for r in cat["symbols"] if r["pct"] >= 0.4]:
            print(f"       {yellow('▶')} {sym_row['pct']:5.1f}%  {sym_row['symbol']}")
            print(f"              {dim(sym_row['desc'])}")

        # Find and print matching guidance
        printed = set()
        for pat, advice in g["targets"].items():
            matched = any(
                re.search(pat, r["symbol"], re.IGNORECASE)
                for r in cat["symbols"]
            )
            if matched and advice not in printed:
                print()
                print(f"       {cyan('Fix:')}")
                for line in advice.splitlines():
                    print(f"         {line}")
                printed.add(advice)


def print_annotation(perf_data: Path, symbol: str) -> None:
    """Run perf annotate on *symbol* and print instruction-level breakdown."""
    _section(f"INSTRUCTION ANNOTATION  {symbol}")
    print(f"  {dim('(assembly annotated with hit counts per instruction)')}")
    print(f"  {dim('Tip: look for instructions with high counts clustered in one block.')}")
    print()
    try:
        out = subprocess.check_output(
            ["perf", "annotate", "-i", str(perf_data), "--stdio", symbol],
            stderr=subprocess.DEVNULL, text=True, timeout=30,
        )
        # Skip the perf header lines; print from the first assembly line
        in_asm = False
        lines_printed = 0
        for line in out.splitlines():
            if re.match(r"^\s+[0-9a-f]+:", line) or re.match(r"^\s+\d+\.\d+\s+:", line):
                in_asm = True
            if in_asm:
                # Highlight lines with ≥1% hit count
                m = re.match(r"^\s+([\d.]+)\s*:", line)
                if m and float(m.group(1)) >= 1.0:
                    print(yellow(line))
                else:
                    print(line)
                lines_printed += 1
                if lines_printed >= 120:
                    print(dim(f"  … (truncated; run `perf annotate -i {perf_data} --stdio {symbol}` for full output)"))
                    break
        if not in_asm:
            # Fall back to raw output
            for line in out.splitlines()[:80]:
                print(line)
    except subprocess.CalledProcessError as e:
        print(red(f"  perf annotate failed (exit {e.returncode})."))
        print(f"  Try: perf annotate -i {perf_data} --stdio {symbol}")
    except subprocess.TimeoutExpired:
        print(red("  perf annotate timed out."))


def print_next_steps(out_dir: Path, rows: list[dict], events: str) -> None:
    _section("NEXT STEPS")

    # Top non-fast-path symbol is the most useful annotation target
    top_sym = next(
        (r["symbol"] for r in rows if not r["is_fast_path"]),
        rows[0]["symbol"] if rows else "SYMBOL"
    )
    perf_data = out_dir / "perf.data"
    is_cycles = "cache" not in events and "miss" not in events

    print(f"""
  Run data saved to:
    {bold(str(out_dir))}

  Instruction-level drill-down on the top hotspot:
    {cyan(f'python scripts/perf_profile.py --annotate {top_sym}')}
  Or directly:
    {cyan(f'perf annotate -i {perf_data} --stdio {top_sym} | head -80')}
""")
    if is_cycles:
        print(f"""  Profile cache misses instead of CPU cycles:
    {cyan('python scripts/perf_profile.py --events L1-dcache-load-misses')}
    {cyan('python scripts/perf_profile.py --events LLC-load-misses')}
    {cyan('python scripts/perf_profile.py --events cache-misses,cycles')}
  List available hardware events:
    {cyan('perf list hw cache')}
""")
    print(f"""  Compare with a previous run:
    {cyan(f'perf diff tmp/perf_runs/<old>/perf.data {perf_data}')}
    {cyan('jq .categories tmp/perf_runs/<old>/report.json')}

  Full call-graph profile (DWARF unwinding, +20% overhead):
    {cyan('python scripts/perf_profile.py --dwarf')}

  Flamegraph (requires FlameGraph on PATH):
    {cyan(f'perf script -i {perf_data} | stackcollapse-perf.pl | flamegraph.pl > /tmp/gnitz.svg')}
""")


# ---------------------------------------------------------------------------
# Prerequisite checks
# ---------------------------------------------------------------------------
def check_prerequisites(rebuild: bool, dwarf: bool) -> None:
    errors = []

    if shutil.which("perf") is None:
        errors.append("'perf' not found on PATH.  Install linux-perf / perf-tools.")

    paranoid_path = Path("/proc/sys/kernel/perf_event_paranoid")
    if paranoid_path.exists():
        val = int(paranoid_path.read_text().strip())
        if val > 1:
            errors.append(
                f"kernel.perf_event_paranoid={val} (must be ≤1).\n"
                "  Fix: sudo sysctl -w kernel.perf_event_paranoid=1"
            )

    if not rebuild and not BINARY.exists():
        errors.append(
            f"Release binary not found: {BINARY}\n"
            "  Fix: python scripts/perf_profile.py --rebuild"
        )

    if not CLIENT_DIR.exists():
        errors.append(f"Client directory not found: {CLIENT_DIR}")

    if errors:
        print(red("Prerequisite check failed:"), file=sys.stderr)
        for e in errors:
            print(f"  • {e}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def _latest_run_dir() -> Path | None:
    """Return the most recently created run directory, or None."""
    if not RUNS_DIR.exists():
        return None
    dirs = sorted(RUNS_DIR.iterdir(), key=lambda p: p.name, reverse=True)
    return dirs[0] if dirs else None


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Perf profiling tool for GnitzDB optimization work.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--rebuild",  action="store_true",
                    help="Run 'make release-server' before profiling (~4 min).")
    ap.add_argument("--dwarf",    action="store_true",
                    help="Use DWARF call-graph unwinding (gives full call stacks, "
                         "adds ~20%% overhead).")
    ap.add_argument("--workers",  type=int, default=4,
                    help="Number of server worker processes (default: 4).")
    ap.add_argument("--ticks",    type=int, default=2,
                    help="Number of push ticks (default: 2).  Use 10+ for more stable samples.")
    ap.add_argument("--rows",     type=int, default=10_000,
                    help="Rows per tick (default: 10,000).")
    ap.add_argument("--percent-limit", type=float, default=0.2, dest="percent_limit",
                    help="Minimum %% to include in flat profile (default: 0.2).")
    ap.add_argument("--events",   default="cycles",
                    help="PMU event(s) to record (default: cycles).  "
                         "Examples: cycles:u (user only), cache-misses, "
                         "L1-dcache-load-misses, LLC-load-misses, branch-misses.  "
                         "Comma-separate multiple events.  "
                         "Run `perf list hw cache` to see what is available.")
    ap.add_argument("--annotate", metavar="SYMBOL", default=None,
                    help="After recording, print instruction-level annotation for SYMBOL.  "
                         "Uses the most recent run's perf.data if no new recording is made "
                         "(e.g. when combined with a run that already completed).")
    ap.add_argument("--realistic", action="store_true",
                    help="Use realistic multi-client workload instead of throughput benchmark.")
    ap.add_argument("--clients", type=int, default=4,
                    help="Number of concurrent client processes (--realistic only, default: 4).")
    args = ap.parse_args()

    # --annotate-only mode: annotate from most recent run, skip new recording
    if args.annotate and not any([args.rebuild, args.dwarf, args.events != "cycles:u"]):
        # Check if the user just wants to re-annotate from the last run
        latest = _latest_run_dir()
        if latest and (latest / "perf.data").exists():
            print(f"\n{dim('Using most recent run:')} {latest}")
            print_annotation(latest / "perf.data", args.annotate)
            return

    check_prerequisites(args.rebuild, args.dwarf)

    # Prepare output directory
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = RUNS_DIR / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    total_rows = args.ticks * args.rows

    # Header
    print()
    print(bold(cyan("═" * 70)))
    print(bold(cyan(f"  GnitzDB Perf Profile — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")))
    print(bold(cyan("═" * 70)))
    mode_s = f"realistic ({args.clients} clients)" if args.realistic else "throughput"
    print(f"  Binary  : {BINARY.name}  (--opt=jit --gc=incminimark --lto)")
    print(f"  Workers : {args.workers}")
    print(f"  Mode    : {mode_s}")
    print(f"  Workload: {args.ticks} ticks × {args.rows:,} rows = {total_rows:,} rows total")
    print(f"  Views   : 10 views — join/chained-join/semi-join/anti-join/union/distinct/reduce/view-on-view×3")
    print(f"  Events  : {args.events}")
    print(f"  Output  : {out_dir}")
    print()

    # Step 1: Rebuild
    if args.rebuild:
        print(bold("[1/4] Rebuilding release binary (make release-server) …"))
        result = subprocess.run(["make", "release-server"], cwd=REPO_ROOT)
        if result.returncode != 0:
            sys.exit("make release-server failed.")
        print(green("      Done."))
        print()
    else:
        mtime = datetime.fromtimestamp(BINARY.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        print(f"{dim('[1/4]')} Binary last built: {mtime}  {dim('(use --rebuild to rebuild)')}")
        print()

    server_proc = None
    tmpdir      = None
    log_f       = None
    perf_proc   = None

    try:
        # Step 2: Start server
        print(bold("[2/4] Starting server …"))
        server_proc, sock_path, tmpdir, log_f = start_server(
            BINARY, args.workers, tmpdir_prefix="perf_profile_")
        print(f"  PID: {server_proc.pid}")

        # Step 3: Attach perf
        print()
        print(bold("[3/4] Attaching perf (F=99) …"))
        time.sleep(0.5)   # let workers fully fork and register
        perf_proc = attach_perf_events(server_proc.pid, PERF_TMP, args.events, args.dwarf)
        time.sleep(0.2)   # give perf time to start sampling

        # Step 4: Run benchmark
        print()
        print(bold("[4/4] Running benchmark …"))
        print()
        if args.realistic:
            timing = run_realistic_workload(sock_path, args.ticks, args.rows, args.clients)
        else:
            timing = run_workload(sock_path, args.ticks, args.rows)
        print_benchmark_timing(timing, args.ticks, args.rows)
        if "realistic" in timing:
            print_realistic_timing(timing)

    finally:
        if perf_proc is not None:
            stop_perf(perf_proc)
        if server_proc is not None:
            stop_server(server_proc, tmpdir, log_f)

    # Copy perf data to output directory
    perf_data = out_dir / "perf.data"
    if PERF_TMP.exists():
        shutil.copy2(PERF_TMP, perf_data)
    else:
        sys.exit("perf.data not found — perf may have failed to attach.")

    # Analysis
    print()
    print(bold("Analyzing perf data …"))
    rows, meta = parse_perf_report(perf_data, args.percent_limit)

    if not rows:
        sys.exit(
            "No symbols found in perf report.  The recording may be empty.\n"
            "Check perf_event_paranoid and try again."
        )

    cats = aggregate_categories(rows)

    # Save full perf report text
    flat_txt = out_dir / "flat_profile.txt"
    try:
        raw_report = subprocess.check_output(
            ["perf", "report", "-i", str(perf_data), "--stdio",
             "--no-children", "-n", "--sort", "symbol",
             f"--percent-limit={args.percent_limit}"],
            stderr=subprocess.DEVNULL, text=True, timeout=60,
        )
        flat_txt.write_text(raw_report)
    except Exception:
        pass

    # Save machine-readable JSON
    total_push  = sum(timing["tick_push"])
    total_build = sum(timing["tick_build"])
    total_scan  = sum(v["time_s"] for v in timing["scan"].values())
    report_json = {
        "timestamp": ts,
        "binary": str(BINARY),
        "binary_mtime": datetime.fromtimestamp(BINARY.stat().st_mtime).isoformat(),
        "workers": args.workers,
        "ticks": args.ticks,
        "rows_per_tick": args.rows,
        "total_rows": total_rows,
        "events": args.events,
        "total_samples": meta["total_samples"],
        "total_cycles": meta["total_cycles"],
        "timing": {
            "setup_s":       timing.get("setup_s", 0),
            "total_build_s": round(total_build, 4),
            "total_push_s":  round(total_push,  4),
            "total_scan_s":  round(total_scan,  4),
            "throughput_rows_per_sec": round(total_rows / total_push) if total_push else 0,
        },
        "symbols": rows,
        "categories": [
            {"key": c["key"], "label": c["label"], "pct": round(c["pct"], 2),
             "is_fast_path": c["is_fast_path"]}
            for c in cats
        ],
    }
    (out_dir / "report.json").write_text(json.dumps(report_json, indent=2))

    # Print analysis sections
    print_flat_profile(rows, meta, args.events)
    print_category_breakdown(cats)
    print_optimization_targets(cats, rows)
    if args.annotate:
        print_annotation(perf_data, args.annotate)
    print_next_steps(out_dir, rows, args.events)

    # Save to benchmark history
    mode = "realistic" if args.realistic else "throughput"
    save_to_history(timing, mode=mode, ticks=args.ticks, rows=args.rows,
                    workers=args.workers, tool="perf", events=args.events)


if __name__ == "__main__":
    main()
