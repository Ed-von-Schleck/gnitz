"""E2E tests for server CLI argument validation.

The server enforces 1 <= --workers <= MAX_WORKERS (64) at startup: a value
outside that range cannot work (the partition→worker routing divides by
256/num_workers, which is 0 for num_workers > 256, and the SAL write path
rejects groups wider than MAX_WORKERS), so it is rejected at the boundary
rather than crashing later.

Run:
    cd crates/gnitz-py && uv run pytest tests/test_server_args.py -v --tb=short
"""
import os
import shutil
import subprocess
import tempfile

import pytest

MAX_WORKERS = 64


def _server_bin():
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__),
                                     "../../../gnitz-server")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    return binary


def _run_server(workers_arg):
    """Spawn the server with a literal --workers=<arg> and return (rc, stderr).
    Used for invalid values, which must exit fast without creating a socket."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_args_",
    )
    try:
        data_dir = os.path.join(tmpdir, "data")
        sock_path = os.path.join(tmpdir, "gnitz.sock")
        cmd = [_server_bin(), data_dir, sock_path, f"--workers={workers_arg}"]
        proc = subprocess.run(cmd, capture_output=True, timeout=10.0)
        return proc.returncode, proc.stderr.decode(errors="replace"), sock_path
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.mark.parametrize("bad", [MAX_WORKERS + 1, 257, 100000])
def test_workers_above_max_rejected(bad):
    rc, stderr, sock_path = _run_server(bad)
    assert rc == 1, f"--workers={bad} should be rejected (exit 1), got rc={rc}"
    assert not os.path.exists(sock_path), "no socket should be created on rejection"
    assert str(MAX_WORKERS) in stderr, f"error should name the limit; got: {stderr!r}"


def test_workers_zero_rejected():
    rc, stderr, sock_path = _run_server(0)
    assert rc == 1, f"--workers=0 should be rejected (exit 1), got rc={rc}"
    assert not os.path.exists(sock_path)


def test_workers_nonnumeric_rejected():
    rc, stderr, sock_path = _run_server("abc")
    assert rc == 1, f"--workers=abc should be rejected (exit 1), got rc={rc}"
    assert not os.path.exists(sock_path)
