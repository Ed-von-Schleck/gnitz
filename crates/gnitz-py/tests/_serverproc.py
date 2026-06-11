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
import ctypes
import os
import signal

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
