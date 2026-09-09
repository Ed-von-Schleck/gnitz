"""The server refuses a `--workers` outside `1..=MAX_WORKERS` at argv parse time.

`parse_workers` itself is unit-tested over the whole valid range and every
rejected spelling (`gnitz-server/src/tests/main.rs`). What only a spawn can show
is that its `Err` is wired to an exit before anything reaches the disk — so one
out-of-range case and one non-numeric case is the whole claim here.
"""
import os
import subprocess

import pytest

from _serverproc import server_binary

# gnitz_wire::MAX_WORKERS, which the server prints in the rejection.
MAX_WORKERS = 64


@pytest.mark.parametrize("bad", [MAX_WORKERS + 1, "abc"])
def test_invalid_workers_exits_before_creating_anything(server_dirs, bad):
    data_dir, sock_path = server_dirs
    proc = subprocess.run(
        [server_binary(), data_dir, sock_path, f"--workers={bad}"],
        capture_output=True, timeout=10.0,
    )
    stderr = proc.stderr.decode(errors="replace")
    assert proc.returncode == 1, f"--workers={bad} should be rejected (exit 1), got rc={proc.returncode}"
    assert not os.path.exists(sock_path), "no socket should be created on rejection"
    if isinstance(bad, int):
        assert str(MAX_WORKERS) in stderr, f"error should name the limit; got: {stderr!r}"
