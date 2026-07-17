"""Interrupt-safe spawning for test-owned gnitz-server processes.

A gnitz-server master forks worker processes that carry
PR_SET_PDEATHSIG(SIGKILL) tied to the master, so killing the master cascades to
the workers. But nothing ties the *master* to the pytest process. If a run is
interrupted before fixture teardown runs — Ctrl-C, SIGTERM/SIGKILL of pytest, or
a pytest crash — the master is orphaned and keeps its workers alive, each
pinning the ~1 GB SAL mmap of a now-unlinked temp dir (unreclaimable). Repeated
interrupted runs accumulate GB of RAM and push the host into swap.

`server_preexec` runs in the forked child just before exec and asks the kernel
to send this process SIGKILL when its parent (pytest) dies, for ANY reason.
PR_SET_PDEATHSIG survives execve (gnitz-server is not setuid), so it persists
into the server binary. Killing the master then cascades to the workers via
their own PDEATHSIG. Pass it as `preexec_fn=server_preexec` to subprocess.Popen.

preexec_fn runs after fork in a single-fork-safe context; we only invoke
already-resolved libc + raw syscalls (no allocation-heavy work, no dlopen — libc
is loaded at import time, before the fork).
"""
import contextlib
import ctypes
import os
import shutil
import signal
import subprocess
import tempfile
import time

import pytest

_PR_SET_PDEATHSIG = 1
_libc = ctypes.CDLL("libc.so.6", use_errno=True)


def server_preexec():
    parent = os.getppid()
    _libc.prctl(_PR_SET_PDEATHSIG, signal.SIGKILL)
    # Close the race where pytest exited between fork() and the prctl above:
    # we were already reparented, so the death signal would never arrive.
    if os.getppid() != parent:
        os._exit(1)


def is_debug_build():
    """Whether the server under test still carries the `#[cfg(debug_assertions)]`
    injection seams. Cargo's default `cargo build` is debug and keeps them;
    release builds drop them. There is no reliable signal in the stripped
    binary, so we trust the GNITZ_RELEASE env the bench harness sets."""
    return os.environ.get("GNITZ_RELEASE", "0") == "0"


@contextlib.contextmanager
def tiny_checkpoint_server(prefix, checkpoint_bytes=32 * 1024):
    """A function-scoped gnitz-server with a tiny GNITZ_CHECKPOINT_BYTES, so
    SAL checkpoints fire repeatedly during a test's own writes. Yields
    `(sock_path, data_dir)`; kills the server and removes its tmpdir on exit.
    32 KB default: a single push of ~500 rows encodes to roughly 30-60 KB, so
    this fires multiple checkpoints per bulk insert."""
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../gnitz-server")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    workers = int(os.environ.get("GNITZ_WORKERS", "4"))
    tmpdir = tempfile.mkdtemp(dir=os.path.expanduser("~/git/gnitz/tmp"), prefix=prefix)
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    env = os.environ.copy()
    env["GNITZ_CHECKPOINT_BYTES"] = str(checkpoint_bytes)
    proc = subprocess.Popen(
        [binary, data_dir, sock_path, f"--workers={workers}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        preexec_fn=server_preexec,
    )
    try:
        for _ in range(100):
            if os.path.exists(sock_path):
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("checkpoint server did not start")
        yield sock_path, data_dir
    finally:
        proc.kill()
        proc.wait()
        shutil.rmtree(tmpdir, ignore_errors=True)


# Deadlock ceilings for concurrent-thread tests, NOT performance budgets. The
# guarded regression is a thread blocked forever (e.g. on `sal_writer_excl`); a
# deadlock never completes, so any finite ceiling catches it. Deliberately
# generous so a slow-but-completing run under saturated-CPU / parallel-suite
# load never flakes. Do NOT tighten these to "speed up the tests" — that
# reintroduces the flake.
HANG_TIMEOUT = 180   # per-thread join ceiling
START_TIMEOUT = 60   # thread waiting for the first concurrent write to land
