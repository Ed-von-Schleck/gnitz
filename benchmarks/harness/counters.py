"""What a set of processes spent between two instants: `perf stat` counts and
`/proc` readings, per process group.

A group is one process under a name — the master, one worker, the driver. Every number
is a count or a kernel-kept nanosecond total, so two runs of the same work are
comparable to the precision the counter has and not to the machine's load.
"""

from __future__ import annotations

import functools
import os
import signal
import subprocess
from pathlib import Path

IORING_OP_FSYNC = 3
TRACEFS = Path("/sys/kernel/tracing/events")

# name -> perf event. Hardware events first: they need no tracefs.
HARDWARE = {
    "instr_user": "instructions:u",
    "instr_kernel": "instructions:k",
    "cycles_user": "cycles:u",
    "cycles_kernel": "cycles:k",
}
TRACEPOINTS = {
    "syscalls": "raw_syscalls:sys_enter",
    "io_uring_enters": "syscalls:sys_enter_io_uring_enter",
    "sync_calls": "syscalls:sys_enter_fdatasync",
    "fsync_calls": "syscalls:sys_enter_fsync",
}


def kernel_counts_allowed() -> bool:
    try:
        return int(Path("/proc/sys/kernel/perf_event_paranoid").read_text()) <= 1
    except (OSError, ValueError):
        return False


def tracepoints_readable() -> bool:
    return os.access(TRACEFS / "raw_syscalls/sys_enter/id", os.R_OK)


@functools.cache
def capabilities() -> dict[str, bool]:
    return {"kernel_counts": kernel_counts_allowed(), "tracepoints": tracepoints_readable()}


class _PerfStat:
    """One `perf stat` of `pid`, counting from `enable()` to `interrupt()`."""

    def __init__(self, pid):
        caps = capabilities()
        events = {k: v for k, v in HARDWARE.items() if caps["kernel_counts"] or not v.endswith(":k")}
        args = ["perf", "stat", "-x", ";", "-e", ",".join(events.values())]
        self._names = {v: k for k, v in events.items()}
        if caps["tracepoints"]:
            args += ["-e", ",".join(TRACEPOINTS.values())]
            self._names.update({v: k for k, v in TRACEPOINTS.items()})
            # A sync the SAL submits to its ring is no fdatasync call.
            args += ["-e", "io_uring:io_uring_submit_req", "--filter", f"opcode=={IORING_OP_FSYNC}"]
            self._names["io_uring:io_uring_submit_req"] = "sync_ring"
        ctl_r, self._ctl_w = os.pipe()
        self._ack_r, ack_w = os.pipe()
        self._proc = subprocess.Popen(
            args + ["-p", str(pid), "--delay", "-1", "--control", f"fd:{ctl_r},{ack_w}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, pass_fds=(ctl_r, ack_w))
        os.close(ctl_r)
        os.close(ack_w)

    def enable(self):
        os.write(self._ctl_w, b"enable\n")

    def await_enabled(self):
        if not os.read(self._ack_r, 16):
            raise RuntimeError(f"perf stat exited before it counted:\n{self._proc.communicate()[1]}")

    def interrupt(self):
        self._proc.send_signal(signal.SIGINT)

    def counts(self) -> dict[str, int]:
        _, err = self._proc.communicate(timeout=30)
        os.close(self._ctl_w)
        os.close(self._ack_r)
        out = {}
        for line in err.splitlines():
            f = line.split(";")
            if len(f) > 2 and f[2] in self._names:
                # A process that never ran in the window is not counted, which is a count of none.
                if not f[0].isdigit() and f[0] != "<not counted>":
                    raise RuntimeError(f"perf stat did not count {f[2]}: {line}")
                out[self._names[f[2]]] = int(f[0]) if f[0].isdigit() else 0
        missing = set(self._names.values()) - set(out)
        if missing:
            raise RuntimeError(f"perf stat reported no {sorted(missing)}:\n{err}")
        out["syncs"] = out.pop("sync_calls", 0) + out.pop("fsync_calls", 0) + out.pop("sync_ring", 0)
        if "syscalls" not in out:
            del out["syncs"]
        return out


def _tasks(pid):
    try:
        return os.listdir(f"/proc/{pid}/task")
    except FileNotFoundError:
        return []


def read_proc(pid) -> dict[str, int]:
    """The kernel's running totals for `pid`, every thread of it together; empty
    once the process is gone."""
    out = {"run_ns": 0, "runq_ns": 0, "voluntary_switches": 0, "involuntary_switches": 0}
    try:
        for tid in _tasks(pid):
            run, wait, _ = Path(f"/proc/{pid}/task/{tid}/schedstat").read_text().split()
            out["run_ns"] += int(run)
            out["runq_ns"] += int(wait)
            for line in Path(f"/proc/{pid}/task/{tid}/status").read_text().splitlines():
                if line.startswith("voluntary_ctxt_switches"):
                    out["voluntary_switches"] += int(line.split()[1])
                elif line.startswith("nonvoluntary_ctxt_switches"):
                    out["involuntary_switches"] += int(line.split()[1])
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        out["minor_faults"], out["major_faults"] = int(stat[7]), int(stat[9])
        for line in Path(f"/proc/{pid}/io").read_text().splitlines():
            key, value = line.split(": ")
            if key in ("read_bytes", "write_bytes"):
                out[key] = int(value)
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith(("VmHWM", "VmRSS")):
                out["rss_peak_bytes" if line.startswith("VmHWM") else "rss_bytes"] = int(line.split()[1]) * 1024
    except (FileNotFoundError, ProcessLookupError):
        return {}
    return out


GAUGES = ("rss_peak_bytes", "rss_bytes")


def reset_rss_peak(pid):
    try:
        Path(f"/proc/{pid}/clear_refs").write_text("5")
    except OSError:
        pass


class Counters:
    """Counts over `groups` (`{name: pid}`) from `start()` to `stop()`."""

    def __init__(self, groups):
        self._groups = groups

    def start(self):
        self._perf = {g: _PerfStat(pid) for g, pid in self._groups.items()}
        for pid in self._groups.values():
            reset_rss_peak(pid)
        self._before = {g: read_proc(pid) for g, pid in self._groups.items()}
        for p in self._perf.values():
            p.enable()
        for p in self._perf.values():
            p.await_enabled()

    def stop(self) -> dict[str, dict[str, int]]:
        # Every count ends before the driver reads any of them back.
        for p in self._perf.values():
            p.interrupt()
        out = {}
        for g, perf in self._perf.items():
            rec, before = perf.counts(), self._before[g]
            for k, v in read_proc(self._groups[g]).items():
                rec[k] = v if k in GAUGES else v - before.get(k, 0)
            out[g] = rec
        return out


def since_start(groups) -> dict[str, dict[str, int]]:
    """Each group's totals since its process started: what a boot cost."""
    return {g: read_proc(pid) for g, pid in groups.items()}
