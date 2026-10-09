"""The layers that explain a run's numbers. A profiled run is never the run of
record: each layer perturbs what it observes.

    cpu      one `perf record` of the whole machine: where each process's
             cycles go (user and kernel frames), and where it waits, from the
             stack it left the CPU with
    kernel   syscalls, io_uring operations and syncs, by count and by time
    malloc   allocations by count, bytes and call site
    writes   every shard byte each store wrote, and the part a manifest published

`cpu` reads its stacks off frame pointers, so it wants the server and the
extension built with them (`make bench-profile`). `kernel` and `malloc` run
`bpftrace` as root; `malloc` probes libc's allocator in every process on the
machine for as long as it runs.

A process is told from the others by its pid, a phase by the driver's markers:
see `mark`.
"""

from __future__ import annotations

import bisect
import os
import re
import shutil
import signal
import subprocess
import time
from collections import defaultdict
from pathlib import Path

import gnitz

from .shards import store_of

LAYERS = ("cpu", "kernel", "malloc", "writes")
LIBC = "/lib64/libc.so.6"
TOP = 12

# A phase boundary a tracer attached to the whole machine can see: a
# `getpriority` of a pid no process has, whose number carries the phase's index.
MARK_BASE = 0x3A000000
BETWEEN = 0xFFFE        # no phase is open
END = 0xFFFF            # the run is over

IORING_OPS = {0: "NOP", 1: "READV", 2: "WRITEV", 3: "FSYNC", 4: "READ_FIXED", 5: "WRITE_FIXED", 6: "POLL_ADD",
              9: "SENDMSG", 10: "RECVMSG", 11: "TIMEOUT", 13: "ACCEPT", 14: "ASYNC_CANCEL", 16: "CONNECT",
              17: "FALLOCATE", 18: "OPENAT", 19: "CLOSE", 22: "READ", 23: "WRITE", 26: "SEND", 27: "RECV",
              51: "FUTEX_WAIT", 52: "FUTEX_WAKE", 53: "FUTEX_WAITV"}


def mark(n):
    try:
        os.getpriority(os.PRIO_PROCESS, MARK_BASE + n)
    except OSError:
        pass


def require(layers):
    """Refuse a layer this machine cannot deliver, before anything runs."""
    unknown = set(layers) - set(LAYERS)
    if unknown:
        raise SystemExit(f"unknown profile layer: {', '.join(sorted(unknown))} (of {', '.join(LAYERS)})")
    problems = []
    if "cpu" in layers:
        if not shutil.which("perf") or not shutil.which("inferno-flamegraph"):
            problems.append("cpu: needs `perf` and `inferno-flamegraph` (cargo install inferno)")
        if int(Path("/proc/sys/kernel/perf_event_paranoid").read_text()) > 0:
            problems.append("cpu: records the whole machine, which needs kernel.perf_event_paranoid <= 0")
        if not os.access("/sys/kernel/tracing/events/sched/sched_switch/id", os.R_OK):
            problems.append("cpu: needs /sys/kernel/tracing readable for the context-switch stacks")
    if {"kernel", "malloc"} & set(layers):
        if subprocess.run(["sudo", "-n", "bpftrace", "--version"], capture_output=True).returncode:
            problems.append("kernel, malloc: need `sudo -n bpftrace` to run without a password")
    if "writes" in layers and not shutil.which("strace"):
        problems.append("writes: needs `strace`")
    if problems:
        raise SystemExit("cannot profile:\n  " + "\n  ".join(problems))


# ---------------------------------------------------------------------------
# bpftrace
# ---------------------------------------------------------------------------

# Every probe keeps to the driver's process tree, which the server is part of,
# and files what it sees under the phase the driver last marked.
_PRELUDE = f"""
config = {{ max_map_keys = 1000000 }}
BEGIN {{ @track[(uint64)$1] = 1; @phase = {BETWEEN}; printf("ready\\n"); }}
tracepoint:sched:sched_process_fork /@track[pid]/ {{ @track[(uint64)args.child_pid] = 1; }}
tracepoint:syscalls:sys_enter_getpriority /pid == $1 && args.who >= {MARK_BASE}/ {{
    @phase = args.who - {MARK_BASE};
    if (args.who == {MARK_BASE + END}) {{ exit(); }}
}}
"""

_KERNEL = _PRELUDE + """
tracepoint:raw_syscalls:sys_enter /@track[pid]/ { @enter[tid] = nsecs; }
tracepoint:raw_syscalls:sys_exit /@enter[tid]/ {
    @sys_ns[@phase, pid, args.id] = sum(nsecs - @enter[tid]);
    @sys_n[@phase, pid, args.id] = count();
    delete(@enter[tid]);
}
tracepoint:io_uring:io_uring_submit_req /@track[pid]/ {
    @req_t[args.req] = nsecs; @req_op[args.req] = args.opcode; @req_pid[args.req] = pid;
}
tracepoint:io_uring:io_uring_complete /@req_t[args.req]/ {
    @ring_ns[@phase, @req_pid[args.req], @req_op[args.req]] = sum(nsecs - @req_t[args.req]);
    @ring_n[@phase, @req_pid[args.req], @req_op[args.req]] = count();
    delete(@req_t[args.req]); delete(@req_op[args.req]); delete(@req_pid[args.req]);
}
kprobe:vfs_fsync_range /@track[pid]/ { @sync_t[tid] = nsecs; }
kretprobe:vfs_fsync_range /@sync_t[tid]/ {
    @sync_ns[@phase, pid] = sum(nsecs - @sync_t[tid]);
    @sync_n[@phase, pid] = count();
    @sync_max[@phase, pid] = max(nsecs - @sync_t[tid]);
    delete(@sync_t[tid]);
}
END { clear(@track); clear(@enter); clear(@req_t); clear(@req_op); clear(@req_pid); clear(@sync_t); }
"""

_MALLOC = _PRELUDE + f"""
uprobe:{LIBC}:malloc /@track[pid] && @phase != {BETWEEN}/ {{
    @alloc_n[@phase, pid] = count(); @alloc_bytes[@phase, pid] = sum(arg0);
    @alloc_site[@phase, pid, ustack(raw, 10)] = count();
}}
uprobe:{LIBC}:calloc /@track[pid] && @phase != {BETWEEN}/ {{
    @alloc_n[@phase, pid] = count(); @alloc_bytes[@phase, pid] = sum(arg0 * arg1);
    @alloc_site[@phase, pid, ustack(raw, 10)] = count();
}}
uprobe:{LIBC}:realloc /@track[pid] && @phase != {BETWEEN}/ {{
    @alloc_n[@phase, pid] = count(); @alloc_bytes[@phase, pid] = sum(arg1);
    @alloc_site[@phase, pid, ustack(raw, 10)] = count();
}}
uprobe:{LIBC}:free /@track[pid] && @phase != {BETWEEN} && arg0 != 0/ {{ @free_n[@phase, pid] = count(); }}
END {{ clear(@track); }}
"""

_ENTRY = re.compile(r"^@(\w+)\[(.*?)\]: (-?\d+)$", re.S)


class _Bpftrace:
    def __init__(self, script, out: Path):
        self.out = out
        script_path = out.with_suffix(".bt")
        script_path.write_text(script)
        self._log = open(out, "w")
        self._proc = subprocess.Popen(["sudo", "-n", "bpftrace", str(script_path), str(os.getpid())],
                                      stdout=self._log, stderr=subprocess.STDOUT)
        deadline = time.time() + 60
        while "ready" not in out.read_text():
            if self._proc.poll() is not None or time.time() > deadline:
                raise RuntimeError(f"bpftrace did not start:\n{out.read_text()}")
            time.sleep(0.05)

    def maps(self):
        """`{map: {key tuple: value}}` once the driver has marked the end. A
        stack in a key is a tuple of addresses."""
        self._proc.wait(timeout=120)
        self._log.close()
        out = defaultdict(dict)
        # An entry ends at the `]: n` that closes its key, which a stack spreads over lines.
        for block in re.split(r"\n(?=@)", self.out.read_text()):
            if m := _ENTRY.match(block.strip()):
                parts = [p.strip() for p in m[2].split(",")]
                key = tuple(tuple(int(a, 16) for a in p.split()) if p.startswith("0x") or "\n" in p or not p
                            else int(p) for p in parts)
                out[m[1]][key] = int(m[3])
        return out


# ---------------------------------------------------------------------------
# Symbols of an address in a process that may be gone
# ---------------------------------------------------------------------------

def read_maps(pid):
    """The executable mappings of `pid`: `[(start, end, file offset, path)]`."""
    out = []
    try:
        for line in Path(f"/proc/{pid}/maps").read_text().splitlines():
            f = line.split(maxsplit=5)
            if len(f) == 6 and "x" in f[1] and f[5].startswith("/"):
                start, end = (int(x, 16) for x in f[0].split("-"))
                out.append((start, end, int(f[2], 16), f[5]))
    except OSError:
        pass
    return out


class Symbols:
    """Function names for addresses, from each file's own symbol table."""

    def __init__(self):
        self._files = {}

    def _file(self, path):
        if path not in self._files:
            loads = []
            for line in subprocess.run(["readelf", "-lW", path], capture_output=True, text=True).stdout.splitlines():
                f = line.split()
                if f and f[0] == "LOAD":
                    loads.append((int(f[1], 16), int(f[2], 16), int(f[4], 16)))      # offset, vaddr, file size
            table = []
            for flags in (["-C", "--defined-only"], ["-DC", "--defined-only"]):
                for line in subprocess.run(["nm", *flags, path], capture_output=True, text=True).stdout.splitlines():
                    f = line.split(maxsplit=2)
                    if len(f) == 3 and f[1] in "tTwW":
                        table.append((int(f[0], 16), f[2]))
            table.sort()
            self._files[path] = (loads, [a for a, _ in table], [n for _, n in table])
        return self._files[path]

    def vaddr(self, maps, addr):
        for start, end, off, path in maps:
            if start <= addr < end:
                file_off = addr - start + off
                for seg_off, seg_vaddr, size in self._file(path)[0]:
                    if seg_off <= file_off < seg_off + size:
                        return path, file_off - seg_off + seg_vaddr
        return None, addr

    def name(self, maps, addr):
        path, vaddr = self.vaddr(maps, addr)
        if path is None:
            return "[unknown]"
        _, addrs, names = self._file(path)
        i = bisect.bisect_right(addrs, vaddr) - 1
        return names[i] if i >= 0 else f"[{Path(path).name}]"


# ---------------------------------------------------------------------------
# Naming what a stack is doing
# ---------------------------------------------------------------------------

_GNITZ = re.compile(r"\bgnitz_(\w+)::(\w+)(?:::(\w+))?")
_SCHED = ("perf_trace_sched_switch", "__schedule", "schedule", "io_schedule", "schedule_timeout",
          "schedule_hrtimeout_range", "schedule_hrtimeout_range_clock", "schedule_preempt_disabled",
          "__cond_resched", "preempt_schedule_common", "do_nanosleep")


def engine_layer(symbol):
    """The rung of the engine a function belongs to: `zset::repr`,
    `server::runtime::reactor`, ..., or None for a function of no gnitz crate."""
    m = _GNITZ.search(symbol)
    if not m:
        return None
    crate, top, second = m.groups()
    if crate == "server" and top == "runtime" and second:
        return f"server::runtime::{second}"
    if crate in ("zset", "store", "server"):
        return f"{crate}::{top}"
    return crate


def kernel_entry(frames):
    """Why a stack is in the kernel, from its kernel frames, outermost first."""
    if any(name.startswith(("io_wq_", "io_worker")) for name in frames):
        return "ring worker"
    for name in frames:
        if m := re.match(r"__(?:x64|do)_sys_(\w+)", name):
            return f"syscall {m[1]}"
        if "page_fault" in name:
            return "page fault"
        if name.startswith(("asm_sysvec", "asm_common_interrupt", "irq_exit", "__irq_exit")):
            return "interrupt"
    return "kernel, other"


def classify(stack):
    """`(layer, function)` of a stack given root first as `(symbol, is_kernel)`.
    The function of a user stack is its innermost engine function: what that
    inlined, and the libc it called, are its own cost."""
    kernel = [s for s, k in stack if k]
    user = [s for s, k in stack if not k]
    if stack and stack[-1][1]:
        return "kernel: " + kernel_entry(kernel), stack[-1][0]
    for s in reversed(user):
        if layer := engine_layer(s):
            return layer, s
    leaf = user[-1] if user else "[unknown]"
    for s in reversed(user):
        if "python" in s.lower() or s.startswith(("_Py", "Py")):
            return "python", leaf
    return "other", leaf


def short(symbol, width=64):
    """A symbol without its generic arguments and leading crates, to `width`."""
    plain = re.sub(r"<[^<>]*>", "<>", re.sub(r"<[^<>]*>", "<>", symbol))
    return plain if len(plain) <= width else "…" + plain[-width:]


def wait_site(stack):
    """What a stack that left the CPU is waiting in: the kernel function that
    put it to sleep, and the engine function that asked."""
    kernel = [s for s, k in stack if k and s not in _SCHED]
    asked = next((s for s, k in reversed(stack) if not k and engine_layer(s)), None)
    where = kernel[-1] if kernel else "[user]"
    return f"{where} <- {asked}" if asked else where


# ---------------------------------------------------------------------------
# strace
# ---------------------------------------------------------------------------

class WriteTrace:
    """Every shard byte the traced processes wrote, by store: all of it, and the
    part a manifest went on to publish. A store syncs a shard only to publish
    it, so one written and unlinked between two publishes — a spill a fold
    consumed, a fold's output a split rewrote — is page cache that never has to
    reach the device."""

    _CALL = re.compile(r'^(\d+\.\d+) (pwrite64|rename|unlink)\((?:\d+<([^>]*)>|"([^"]*)")(?:, "([^"]*)")?.*\) = (\d+)$')

    def __init__(self, pids, prefix):
        self.prefix = Path(prefix)
        args = ["strace", "-ff", "-y", "-ttt", "-e", "trace=pwrite64,rename,unlink", "-o", str(prefix)]
        self.proc = subprocess.Popen(args + [f"--attach={pid}" for pid in pids], stderr=subprocess.PIPE, text=True)
        # A row pushed before the last attach would be written unseen.
        for _ in pids:
            line = self.proc.stderr.readline()
            if "attached" not in line:
                self.proc.kill()
                raise RuntimeError(f"strace could not attach: {line}")

    def by_store(self):
        """`{(relation, store): bytes}` twice over — written and published —
        once every traced process has exited."""
        self.proc.wait()
        events = []
        for log in self.prefix.parent.glob(self.prefix.name + ".*"):
            events += [m.groups() for line in log.read_text(errors="replace").splitlines()
                       if (m := self._CALL.match(line))]
            log.unlink()
        written, published = defaultdict(int), defaultdict(int)
        unpublished = {}                                                # shard path -> bytes written
        for _, call, fd_path, path, target, result in sorted(events, key=lambda e: float(e[0])):
            if call == "pwrite64" and (store := store_of(fd_path)):
                written[store] += int(result)
                unpublished[fd_path] = unpublished.get(fd_path, 0) + int(result)
            elif call == "unlink":
                unpublished.pop(path, None)
            elif call == "rename" and Path(target).name == "manifest.bin":
                for shard in [s for s in unpublished if Path(s).parent == Path(target).parent]:
                    published[store_of(shard)] += unpublished.pop(shard)
        return written, published


# ---------------------------------------------------------------------------
# The layers of one run
# ---------------------------------------------------------------------------

_SAMPLE = re.compile(r"^(.+?)\s+(\d+)/(\d+)\s+(\d+\.\d+):\s+(\d+)\s+(\S+):(?: (.*))?$")
_FRAME = re.compile(r"^\s+([0-9a-f]+)\s+(.+) \(([^()]*)\)$")
_SWITCH = re.compile(r":(\d+) \[\d+\] (\S+) ==> .*:(\d+) \[\d+\]$")


class Layers:
    """The layers `names` over one run; with no name, every method does nothing."""

    def __init__(self, names, out_dir: Path):
        self.names, self.out = names, out_dir
        self.groups = {os.getpid(): "client"}       # pid -> group, over every boot
        self.maps = {}
        self._written, self._published = defaultdict(int), defaultdict(int)
        self._readings = {}
        self._trace = None
        self._perf = None
        self._bpf = {}

    def start(self):
        names, out_dir = self.names, self.out
        if names:
            out_dir.mkdir(parents=True, exist_ok=True)
        if "kernel" in names:
            self._bpf["kernel"] = _Bpftrace(_KERNEL, out_dir / "kernel.txt")
        if "malloc" in names:
            self._bpf["malloc"] = _Bpftrace(_MALLOC, out_dir / "malloc.txt")
        if "cpu" in names:
            comms = ("gnitz-server*", "python*", "iou-*")
            switch = " || ".join(f'{side}_comm ~ "{c}"' for c in comms for side in ("prev", "next"))
            self._perf = subprocess.Popen(
                ["perf", "record", "-a", "-g", "-F", "4999", "-k", "CLOCK_MONOTONIC", "-o", str(out_dir / "perf.data"),
                 "-e", "cycles", "-e", "sched:sched_switch", "--filter", switch],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            time.sleep(1.0)
            if self._perf.poll() is not None:
                raise RuntimeError(f"perf record did not start:\n{self._perf.stderr.read()}")

    def server_started(self, groups):
        """A boot's processes, as `{group: pid}`."""
        for group, pid in groups.items():
            self.groups[pid] = "workers" if group.startswith("worker") else group
            self.maps[pid] = read_maps(pid)
        if "writes" in self.names:
            self._trace = WriteTrace([pid for g, pid in groups.items() if g.startswith("worker")],
                                     self.out / f"writes{len(self.maps)}")

    def server_stopped(self, reading=None):
        """Every server process has exited; `reading` names the disk reading
        taken of what they left."""
        if self._trace:
            written, published = self._trace.by_store()
            for k, v in written.items():
                self._written[k] += v
            for k, v in published.items():
                self._published[k] += v
            self._trace = None
        if reading and "writes" in self.names:
            self._readings[reading] = {
                f"{rel} {store}": [n, self._published[(rel, store)]]
                for (rel, store), n in sorted(self._written.items()) if rel >= gnitz.FIRST_USER_TABLE_ID}

    # -- reading the layers back ------------------------------------------------

    def finish(self, phases, client_pids):
        """Stop every layer and say, per phase, what it saw. The driver has
        marked the end, so the tracers are already on their way out."""
        self.groups.update(dict.fromkeys(client_pids, "client"))
        self.maps[os.getpid()] = read_maps(os.getpid())
        names = {i: p["name"] for i, p in enumerate(phases)}
        out = {}
        if self._perf:
            self._perf.send_signal(signal.SIGINT)
            self._perf.wait(timeout=120)
            out["cpu"] = self._cpu(phases)
        if "kernel" in self._bpf:
            out["kernel"] = self._kernel(self._bpf["kernel"].maps(), names)
        if "malloc" in self._bpf:
            out["malloc"] = self._malloc(self._bpf["malloc"].maps(), names, phases)
        if "writes" in self.names:
            # `{reading: {"<relation> <store>": [written, published]}}`, each up to its reading.
            out["writes"] = {"readings": self._readings}
        return out

    def _kernel(self, maps, names):
        numbers = {}
        for line in subprocess.run(["ausyscall", "--dump"], capture_output=True, text=True).stdout.splitlines()[1:]:
            n, name = line.split()
            numbers[int(n)] = name
        by_phase = defaultdict(list)
        data = defaultdict(dict)

        def table(count_map, time_map, label, name_of):
            agg = defaultdict(lambda: [0, 0])
            for (phase, pid, what), n in maps.get(count_map, {}).items():
                if (g := self.groups.get(pid)) and phase in names:
                    agg[(phase, g, what)][0] += n
                    agg[(phase, g, what)][1] += maps[time_map].get((phase, pid, what), 0)
            for phase in sorted({k[0] for k in agg}):
                for g in ("client", "master", "workers"):
                    rows = sorted(((n, ns, w) for (p, gg, w), (n, ns) in agg.items() if p == phase and gg == g),
                                  key=lambda r: -r[1])
                    if rows:
                        by_phase[names[phase]].append(
                            f"{label} {g}: " + ", ".join(f"{name_of(w)} {n:,}× {ns / 1e6:,.1f} ms" for n, ns, w in rows[:8]))
                        data[names[phase]][f"{label} {g}"] = {name_of(w): [n, ns] for n, ns, w in rows}

        table("sys_n", "sys_ns", "syscalls", lambda i: numbers.get(i, str(i)))
        table("ring_n", "ring_ns", "ring ops", lambda i: IORING_OPS.get(i, f"op{i}"))
        sync = defaultdict(lambda: [0, 0, 0])
        for (phase, pid), n in maps.get("sync_n", {}).items():
            if (g := self.groups.get(pid)) and phase in names:
                s = sync[(phase, g)]
                s[0] += n
                s[1] += maps["sync_ns"].get((phase, pid), 0)
                s[2] = max(s[2], maps["sync_max"].get((phase, pid), 0))
        for (phase, g), (n, ns, worst) in sorted(sync.items()):
            by_phase[names[phase]].append(
                f"syncs {g}: {n:,} taking {ns / 1e6:,.1f} ms, the longest {worst / 1e6:.2f} ms")
            data[names[phase]][f"syncs {g}"] = {"n": n, "ns": ns, "max_ns": worst}
        return {"by_phase": by_phase, "data": data, "files": [str(self.out / "kernel.txt")]}

    def _malloc(self, maps, names, phases):
        symbols = Symbols()
        by_phase = defaultdict(list)
        data = defaultdict(dict)
        rows = {p["name"]: max(p["rows"], 1) for p in phases}
        totals = defaultdict(lambda: [0, 0, 0])
        for (phase, pid), n in maps.get("alloc_n", {}).items():
            if (g := self.groups.get(pid)) and phase in names:
                t = totals[(phase, g)]
                t[0] += n
                t[1] += maps["alloc_bytes"].get((phase, pid), 0)
                t[2] += maps.get("free_n", {}).get((phase, pid), 0)
        sites = defaultdict(lambda: defaultdict(int))
        for (phase, pid, stack), n in maps.get("alloc_site", {}).items():
            if (g := self.groups.get(pid)) and phase in names:
                frames = [symbols.name(self.maps.get(pid, []), a) for a in stack]
                # The innermost engine function below the allocator.
                site = next((f for f in frames if engine_layer(f)), frames[1] if len(frames) > 1 else frames[0])
                sites[(phase, g)][site] += n
        for (phase, g), (n, nbytes, freed) in sorted(totals.items()):
            name = names[phase]
            by_phase[name].append(f"allocations {g}: {n:,} ({n / rows[name]:.2f} a row) of {nbytes:,} bytes, "
                                  f"{freed:,} frees")
            top = sorted(sites[(phase, g)].items(), key=lambda kv: -kv[1])[:TOP]
            for site, count in top:
                by_phase[name].append(f"    {count:>10,}  {count / n:5.1%}  {site}")
            data[name][f"allocations {g}"] = {"n": n, "bytes": nbytes, "frees": freed, "sites": dict(top)}
        return {"by_phase": by_phase, "data": data, "files": [str(self.out / "malloc.txt")]}

    def _cpu(self, phases):
        """One pass over `perf script`: every sample goes to the phase its time
        falls in and the group its pid belongs to."""
        spans = [(p["t0"], p["t1"], p["name"]) for p in phases]
        starts = [s[0] for s in spans]

        def phase_at(t):
            i = bisect.bisect_right(starts, t) - 1
            return spans[i][2] if i >= 0 and t <= spans[i][1] else None

        on = defaultdict(lambda: defaultdict(int))          # (phase, group) -> folded stack -> cycles
        off = defaultdict(lambda: defaultdict(int))         # (phase, group) -> folded stack -> ns
        layers = defaultdict(lambda: defaultdict(int))
        leaves = defaultdict(lambda: defaultdict(int))
        waits = defaultdict(lambda: defaultdict(int))
        left = {}                                           # tid -> (time, group, stack, state)

        def close(head, stack):
            comm, pid, tid, t, period, event, rest = head
            stack = stack[::-1]                             # root first
            if event == "cycles":
                g, phase = self.groups.get(pid), phase_at(t)
                if g and phase:
                    key = (phase, g)
                    on[key][";".join(s for s, _ in stack) or "[unknown]"] += period
                    layer, leaf = classify(stack)
                    layers[key][layer] += period
                    leaves[key][leaf] += period
            elif event == "sched:sched_switch" and (m := _SWITCH.search(rest)):
                prev, state, nxt = int(m[1]), m[2], int(m[3])
                if nxt in left:
                    t_out, g, s, st = left.pop(nxt)
                    for t0, t1, name in spans:
                        ns = int((min(t, t1) - max(t_out, t0)) * 1e9)
                        if ns > 0:
                            kind = "preempted" if st.startswith("R") else "blocked"
                            off[(name, g)][kind + ";" + (";".join(x for x, _ in s) or "[unknown]")] += ns
                            if kind == "blocked":
                                waits[(name, g)][wait_site(s)] += ns
                if (g := self.groups.get(pid)) and prev == tid:
                    left[tid] = (t, g, stack, state)

        env = dict(os.environ, DEBUGINFOD_URLS="")
        script = subprocess.Popen(
            ["perf", "script", "-i", str(self.out / "perf.data"), "--inline",
             "-F", "comm,pid,tid,time,period,event,ip,sym,dso,trace"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=env, errors="replace")
        head, stack = None, []
        for line in script.stdout:
            if not line.strip():
                if head:
                    close(head, stack)
                head, stack = None, []
            elif line[0] not in " \t":
                if head:
                    close(head, stack)
                m = _SAMPLE.match(line.rstrip("\n"))
                head = m and (m[1], int(m[2]), int(m[3]), float(m[4]), int(m[5]), m[6], m[7])
                stack = []
            elif head:
                if m := _FRAME.match(line.rstrip("\n")):
                    stack.append((m[2], m[3] == "[kernel.kallsyms]"))
        if head:
            close(head, stack)
        script.wait()

        by_phase, files, data = defaultdict(list), [], defaultdict(dict)
        for kind, folded, unit in (("oncpu", on, "cycles"), ("offcpu", off, "ns")):
            for (phase, g), stacks in sorted(folded.items()):
                folded_path = self.out / f"{kind}.{phase}.{g}.folded"
                folded_path.write_text("".join(f"{s} {n}\n" for s, n in stacks.items()))
                svg_path = folded_path.with_suffix(".svg")
                with open(svg_path, "w") as svg:
                    made = subprocess.run(
                        ["inferno-flamegraph", "--title", f"{kind} {phase} {g}", "--countname", unit, str(folded_path)],
                        stdout=svg, stderr=subprocess.DEVNULL)
                if made.returncode == 0:
                    files.append(str(svg_path))
        for key in sorted(layers):
            phase, g = key
            total = sum(layers[key].values())
            by_phase[phase].append(f"cycles {g}, by layer: " + ", ".join(
                f"{name} {n / total:.0%}" for name, n in sorted(layers[key].items(), key=lambda kv: -kv[1])[:TOP]))
            by_phase[phase].append(f"cycles {g}, by function: " + ", ".join(
                f"{short(name)} {n / total:.0%}" for name, n in sorted(leaves[key].items(), key=lambda kv: -kv[1])[:TOP]))
            data[phase][f"cycles {g}"] = {"layers": {k: v / total for k, v in layers[key].items()},
                                          "functions": {k: v / total for k, v in sorted(
                                              leaves[key].items(), key=lambda kv: -kv[1])[:40]}}
        for key in sorted(waits):
            phase, g = key
            total = sum(waits[key].values())
            by_phase[phase].append(f"blocked {g}, {total / 1e6:,.1f} ms: " + ", ".join(
                f"{short(name, 90)} {n / 1e6:,.1f} ms" for name, n in sorted(waits[key].items(), key=lambda kv: -kv[1])[:6]))
            data[phase][f"blocked {g}"] = dict(sorted(waits[key].items(), key=lambda kv: -kv[1])[:20])
        return {"by_phase": by_phase, "data": data, "files": files}
