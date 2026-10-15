"""CPU pinning as the kernel reports it, for every thread of every process."""

import os
import time

import pytest


def _threads(pid):
    """`{tid: (name, cpus)}` for every live thread of `pid`."""
    out = {}
    for tid in map(int, os.listdir(f"/proc/{pid}/task")):
        try:
            with open(f"/proc/{pid}/task/{tid}/comm") as f:
                out[tid] = (f.read().strip(), frozenset(os.sched_getaffinity(tid)))
        except (FileNotFoundError, ProcessLookupError):
            pass
    return out


def test_every_process_and_thread_holds_its_share(own_server):
    own_server.start(workers=2, extra_env={"GNITZ_CPU_AFFINITY": "1"})
    if "affinity: not applied" in own_server.log_text():
        pytest.skip("host has too few cores to seat 2 workers and the master")
    master = own_server.proc.pid
    workers = own_server.worker_pids()
    assert sorted(workers) == [0, 1], workers

    # An iou-wrk thread started before the master pinned would keep the unpinned mask.
    deadline = time.monotonic() + 10
    while not any(name.startswith("iou-wrk") for name, _ in _threads(master).values()):
        assert time.monotonic() < deadline, "master never started an iou-wrk thread"
        time.sleep(0.05)

    masks = []
    for pid in [master, *workers.values()]:
        per_thread = {tid: cpus for tid, (_, cpus) in _threads(pid).items()}
        assert len(set(per_thread.values())) == 1, (pid, per_thread)
        masks.append(next(iter(per_thread.values())))

    for i, a in enumerate(masks):
        assert a, masks
        for b in masks[i + 1:]:
            assert not a & b, masks
    assert frozenset().union(*masks) == frozenset(os.sched_getaffinity(0))
