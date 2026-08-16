"""Test-owned gnitz-server processes: spawning, readiness, restart, teardown.

`ServerProc` is the one lifecycle every test that runs its own server drives.
Tests that only need the shared session server use the `client` fixture instead.

**Interrupt safety.** A gnitz-server master forks worker processes that carry
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
their own PDEATHSIG.

preexec_fn runs after fork in a single-fork-safe context; we only invoke
already-resolved libc + raw syscalls (no allocation-heavy work, no dlopen — libc
is loaded at import time, before the fork).
"""
import ctypes
import os
import re
import signal
import subprocess
import time

import pytest

from _paths import REPO_ROOT

_PR_SET_PDEATHSIG = 1
_libc = ctypes.CDLL("libc.so.6", use_errno=True)

# The ambient worker count. One definition, so a `GNITZ_WORKERS`-conditional
# skip and the `--workers` a server is actually spawned with can never disagree.
NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))

# Each server fallocates its whole SAL at startup, so the 1 GiB production
# default would have a suite that spawns ~100 servers write 100 GiB. E2E tests
# are functional (small writes), so cap it — unless the caller pinned a size,
# which the SAL-sizing tests do.
TEST_SAL_BYTES = str(128 * 1024 * 1024)


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


def server_binary():
    """The server under test, skipping the test if it has not been built."""
    binary = os.environ.get("GNITZ_SERVER_BIN", str(REPO_ROOT / "gnitz-server"))
    binary = os.path.abspath(binary)
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    return binary


class ServerProc:
    """One gnitz-server process over a fixed `(data_dir, sock_path)` pair.

    `restart` reboots on the SAME data dir, which is what the recovery tests
    assert against; only the session server in conftest discards its data dir
    between runs, and it does that by constructing a new `ServerProc`.

    Output goes to a file next to the data dir, never a pipe: the master logs
    unbuffered to fd 1/2, and at `GNITZ_LOG_LEVEL=debug` it would fill a 64 KiB
    pipe buffer and block forever mid-test. Boots append to the one file, and
    `log_text` returns only what the current boot wrote, so a marker from an
    earlier boot can never satisfy an assertion about this one.
    """

    def __init__(self, data_dir, sock_path, *, workers=None, extra_env=None):
        self.data_dir = data_dir
        self.sock_path = sock_path
        self.workers = NUM_WORKERS if workers is None else workers
        self.extra_env = dict(extra_env or {})
        self.log_path = data_dir.rstrip("/") + ".log"
        self.proc = None
        self._log_start = 0

    # ── spawning ─────────────────────────────────────────────────────────────

    def _popen(self, spawn_env):
        env = os.environ.copy()
        env.setdefault("GNITZ_SAL_BYTES", TEST_SAL_BYTES)
        env.update(self.extra_env)     # config of this server, every boot
        env.update(spawn_env or {})    # this boot only
        # A killed server leaves its socket behind and the next bind would fail.
        self.clear_socket()
        # Where this boot's output starts, so `log_text` can exclude the last.
        self._log_start = os.path.getsize(self.log_path) if os.path.exists(self.log_path) else 0
        log = open(self.log_path, "ab")
        try:
            return subprocess.Popen(
                [server_binary(), self.data_dir, self.sock_path, f"--workers={self.workers}"],
                stdout=log, stderr=log, env=env,
                start_new_session=True, preexec_fn=server_preexec,
            )
        finally:
            log.close()

    def start(self, *, workers=None, extra_env=None, timeout=10.0):
        """Spawn and wait until the socket exists. Returns self.

        `workers` sticks — a later `restart()` reboots at the same count.
        `extra_env` applies to *this boot only*, which is what a fault injected
        for one boot and then recovered from needs; put configuration that must
        survive a reboot in `self.extra_env` instead."""
        if workers is not None:
            self.workers = workers
        self.proc = self._popen(extra_env)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if os.path.exists(self.sock_path):
                return self
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"server exited rc={self.proc.returncode} before binding "
                    f"{self.sock_path}\n{self.log_tail()}"
                )
            time.sleep(0.05)
        self.stop()
        raise RuntimeError(f"server did not start (no socket)\n{self.log_tail()}")

    def start_expecting_exit(self, *, workers=None, extra_env=None, timeout=20.0):
        """Spawn a server expected to die during boot. Returns its non-zero exit
        code, having confirmed it never bound the socket."""
        if workers is not None:
            self.workers = workers
        self.proc = self._popen(extra_env)
        deadline = time.time() + timeout
        while time.time() < deadline:
            rc = self.proc.poll()
            if rc is not None:
                return rc
            if os.path.exists(self.sock_path):
                self.stop()
                raise RuntimeError("server became ready; expected a boot crash")
            time.sleep(0.05)
        self.stop()
        raise RuntimeError(f"server did not exit within {timeout}s\n{self.log_tail()}")

    # ── stopping ─────────────────────────────────────────────────────────────

    def stop(self):
        """SIGKILL the whole process group — master and workers together. The
        master's PDEATHSIG reaps workers only asynchronously, and a restart on
        the same data dir must not race a worker still mapping `wal.sal`."""
        if self.proc is None:
            return
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        self.proc.wait()
        self.proc = None

    def stop_graceful(self, timeout=30):
        """SIGTERM the *master only* (not the process group), so its shutdown
        watcher can drive a final checkpoint (which needs live workers to ACK
        the flush) before broadcasting FLAG_SHUTDOWN and exiting. Returns the
        master's exit code; raises on hang."""
        assert self.proc is not None, "no running server"
        try:
            os.kill(self.proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        self.proc.wait(timeout=timeout)
        rc = self.proc.returncode
        self.proc = None
        return rc

    def wait_for_exit(self, timeout=15.0):
        """Wait for the server to abort on its own. Returns its exit code;
        raises if it is still running at the deadline."""
        assert self.proc is not None, "no running server"
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.stop()
            raise RuntimeError("server did not exit; expected an injected abort")
        rc = self.proc.returncode
        self.proc = None
        return rc

    # ── restarting ───────────────────────────────────────────────────────────

    def restart(self, *, graceful=False, workers=None, extra_env=None, timeout=10.0):
        """Stop and reboot on the SAME data dir. `graceful=True` shuts down via
        SIGTERM (final checkpoint) and asserts a clean exit; the default SIGKILL
        is the crash-recovery path."""
        if graceful:
            rc = self.stop_graceful()
            assert rc == 0, f"graceful shutdown must exit rc 0, got {rc}"
        else:
            self.stop()
        self.clear_socket()
        return self.start(workers=workers, extra_env=extra_env, timeout=timeout)

    def clear_socket(self):
        """Remove the stale socket a killed server left behind."""
        if os.path.exists(self.sock_path):
            os.unlink(self.sock_path)

    # ── output ───────────────────────────────────────────────────────────────

    def log_text(self):
        """What the current boot has written to stdout+stderr so far."""
        try:
            with open(self.log_path, "rb") as f:
                f.seek(self._log_start)
                return f.read().decode(errors="replace")
        except FileNotFoundError:
            return ""

    def log_tail(self, max_bytes=4096):
        return self.log_text()[-max_bytes:]

    def rebuilt_view_count(self):
        """N from the most recent `recovery: rebuilding N invalid view(s)`
        marker the master printed at boot, or None if it never printed one.
        0 means every view resumed from its checkpoint."""
        matches = re.findall(r"recovery: rebuilding (\d+) invalid view", self.log_text())
        return int(matches[-1]) if matches else None

    def rebuilt_index_counts(self):
        """The sibling index marker, per launched worker. Worker-side because the
        boot index rebuild runs after the child redirected fd 1 to its own log,
        so the master log never sees it. Worker logs are truncated per boot, so
        the last marker in each is this boot's."""
        counts = []
        for w in range(self.workers):
            with open(os.path.join(self.data_dir, f"worker_{w}.log")) as f:
                markers = re.findall(r"recovery: rebuilding (\d+) index\(es\)", f.read())
            assert markers, f"worker {w} printed no index-rebuild marker"
            counts.append(int(markers[-1]))
        return counts


# Deadlock ceilings for concurrent-thread tests, NOT performance budgets. The
# guarded regression is a thread blocked forever (e.g. on `sal_writer_excl`); a
# deadlock never completes, so any finite ceiling catches it. Deliberately
# generous so a slow-but-completing run under saturated-CPU / parallel-suite
# load never flakes. Do NOT tighten these to "speed up the tests" — that
# reintroduces the flake.
HANG_TIMEOUT = 180   # per-thread join ceiling
START_TIMEOUT = 60   # thread waiting for the first concurrent write to land
