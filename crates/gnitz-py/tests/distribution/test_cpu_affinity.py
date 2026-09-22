"""CPU pinning as the kernel reports it, for every thread of every process."""

import os
import time

import pytest


def _read_status(path):
    """`/proc/.../status` as a field dict, or None if the task has exited or is unreadable."""
    try:
        with open(path) as f:
            text = f.read()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    return dict(line.split(":\t", 1) for line in text.splitlines() if ":\t" in line)


def _children(pid):
    kids = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        status = _read_status(f"/proc/{name}/status")
        if status is not None and int(status["PPid"]) == pid:
            kids.append(int(name))
    return kids


def _threads(pid):
    """`{tid: status}` for every live thread of `pid`."""
    out = {}
    for tid in os.listdir(f"/proc/{pid}/task"):
        status = _read_status(f"/proc/{pid}/task/{tid}/status")
        if status is not None:
            out[int(tid)] = status
    return out


def _cpus(cpu_list):
    cpus = set()
    for part in cpu_list.strip().split(","):
        lo, _, hi = part.partition("-")
        cpus.update(range(int(lo), int(hi or lo) + 1))
    return frozenset(cpus)


def test_every_process_and_thread_holds_its_share(own_server):
    own_server.start(workers=2, extra_env={"GNITZ_CPU_AFFINITY": "1"})
    if "affinity: not applied" in own_server.log_text():
        pytest.skip("host has too few cores to seat 2 workers and the master")
    master = own_server.proc.pid
    workers = _children(master)
    assert len(workers) == 2, workers

    # An iou-wrk thread started before the master pinned would keep the unpinned mask.
    deadline = time.monotonic() + 10
    while not any(s["Name"].startswith("iou-wrk") for s in _threads(master).values()):
        assert time.monotonic() < deadline, "master never started an iou-wrk thread"
        time.sleep(0.05)

    masks = []
    for pid in [master, *workers]:
        per_thread = {tid: _cpus(s["Cpus_allowed_list"]) for tid, s in _threads(pid).items()}
        assert len(set(per_thread.values())) == 1, (pid, per_thread)
        masks.append(next(iter(per_thread.values())))

    for i, a in enumerate(masks):
        assert a, masks
        for b in masks[i + 1:]:
            assert not a & b, masks
    assert frozenset().union(*masks) == frozenset(os.sched_getaffinity(0))
