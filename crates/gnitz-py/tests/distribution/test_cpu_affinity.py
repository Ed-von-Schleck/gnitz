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
    note = next((l for l in own_server.log_text().splitlines() if "affinity: not applied" in l), None)
    if note:
        pytest.skip(note)
    master = own_server.proc.pid
    workers = own_server.worker_pids()
    assert sorted(workers) == [0, 1], workers
    pids = [master, *workers.values()]

    # An iou-wrk thread started before the master pinned would keep the unpinned
    # mask, so the check waits for one; the master starts one when io_uring
    # punts a blocking op. Every mask is read from that one snapshot.
    deadline = time.monotonic() + 10
    while True:
        snap = {pid: _threads(pid) for pid in pids}
        if any(name.startswith("iou-wrk") for name, _ in snap[master].values()):
            break
        assert time.monotonic() < deadline, "master never started an iou-wrk thread"
        time.sleep(0.05)

    masks = []
    for pid, threads in snap.items():
        per_thread = {tid: cpus for tid, (_, cpus) in threads.items()}
        assert len(set(per_thread.values())) == 1, (pid, per_thread)
        masks.append(next(iter(per_thread.values())))

    union = frozenset().union(*masks)
    assert all(masks) and sum(map(len, masks)) == len(union), masks   # non-empty, pairwise disjoint
    assert union == os.sched_getaffinity(0)
