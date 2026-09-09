"""E2E tests for server CLI argument validation.

The server enforces 1 <= --workers <= MAX_WORKERS (64) at startup: a value
outside that range cannot work (the partition→worker routing divides by
256/num_workers, which is 0 for num_workers > 256, and the SAL write path
rejects groups wider than MAX_WORKERS), so it is rejected at the boundary
rather than crashing later.

Run:
    cd crates/gnitz-py && uv run pytest tests/admissibility/test_server_args.py -v --tb=short
"""
import os
import subprocess

import pytest

from _serverproc import server_binary

MAX_WORKERS = 64


@pytest.mark.parametrize("bad", [MAX_WORKERS + 1, 257, 100000, 0, "abc"])
def test_invalid_workers_rejected(server_dirs, bad):
    """An out-of-range or non-numeric --workers must exit non-zero at argv
    parse time, before anything is created on disk."""
    data_dir, sock_path = server_dirs
    proc = subprocess.run(
        [server_binary(), data_dir, sock_path, f"--workers={bad}"],
        capture_output=True, timeout=10.0,
    )
    stderr = proc.stderr.decode(errors="replace")
    assert proc.returncode == 1, f"--workers={bad} should be rejected (exit 1), got rc={proc.returncode}"
    assert not os.path.exists(sock_path), "no socket should be created on rejection"
    if isinstance(bad, int) and bad > MAX_WORKERS:
        assert str(MAX_WORKERS) in stderr, f"error should name the limit; got: {stderr!r}"
