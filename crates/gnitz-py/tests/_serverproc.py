"""Test-owned gnitz-server processes: spawning, readiness, restart, teardown.

`ServerProc` is the one lifecycle every test that runs its own server drives.
Tests that only need the shared session server use the `client` fixture instead.

**Interrupt safety.** A gnitz-server master forks worker processes that carry
PR_SET_PDEATHSIG(SIGKILL) tied to the master, so killing the master cascades to
the workers. But nothing ties the *master* to the pytest process. If a run is
interrupted before fixture teardown runs — Ctrl-C, SIGTERM/SIGKILL of pytest, or
a pytest crash — the master is orphaned and keeps its workers alive, each
pinning the SAL mmap of a now-unlinked temp dir (unreclaimable). Repeated
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

# Boot-to-socket is ~36 ms, so a 50 ms poll spends most of a spawn asleep past
# the event it is waiting for. Every loop using this is bounded by its own
# deadline, so the interval buys nothing but granularity.
_READY_POLL_S = 0.002

# What the server prints once its listeners are up — the last line of boot.
# See `ServerProc.start` for why the socket file is not the readiness signal.
_READY_MARKER = "GnitzDB ready"

_PR_SET_PDEATHSIG = 1
_libc = ctypes.CDLL("libc.so.6", use_errno=True)

# The ambient worker count. One definition, so a `GNITZ_WORKERS`-conditional
# skip and the `--workers` a server is actually spawned with can never disagree.
NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))

# A test whose claim is about placement — one copy rather than one per worker,
# weight 1 rather than weight W, a partial merged across workers — is not merely
# unproven at one worker, it passes vacuously. Marking it is what keeps a
# `WORKERS=1` run honest about what it did not check.
NEEDS_MULTI = pytest.mark.skipif(NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2")

def test_server_env():
    """The environment every test-spawned server boots in: this process's, plus
    the defaults below wherever the caller has not set one itself.

    One definition, because a suite runs several servers at once — the shared
    session server outlives every `own_server` — and a default that only some
    spawn sites remember is a default that does not hold.

    `GNITZ_SAL_BYTES`: each server fallocates its whole SAL at startup, so the
    1 GiB production default would have a suite that spawns ~100 servers write
    100 GiB. E2E tests are functional, so cap it — unless the caller pinned a
    size, which the SAL-sizing tests do.

    `GNITZ_CPU_AFFINITY`: the placement is derived from `sched_getaffinity` and
    knows nothing of other tenants, so concurrent servers pin onto each other's
    cores.
    """
    env = os.environ.copy()
    for k, v in (("GNITZ_SAL_BYTES", str(128 * 1024 * 1024)), ("GNITZ_CPU_AFFINITY", "0")):
        env.setdefault(k, v)
    return env


def server_preexec():
    parent = os.getppid()
    _libc.prctl(_PR_SET_PDEATHSIG, signal.SIGKILL)
    # Close the race where pytest exited between fork() and the prctl above:
    # we were already reparented, so the death signal would never arrive.
    if os.getppid() != parent:
        os._exit(1)


def kill_group(proc):
    """SIGKILL `proc`'s whole process group — master and workers together. The
    master's PDEATHSIG reaps workers only asynchronously, so anything that
    touches the data dir afterwards (a restart, an `rmtree`) would otherwise race
    a worker still mapping `wal.sal`. Requires the spawn to have used
    `start_new_session`, or this reaches the caller too."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    proc.wait()


def await_ready(proc, read_log, timeout):
    """Block until `proc` prints the readiness marker, raising if it dies first.

    Readiness is the server's own marker, not the socket file: `UnixListener::bind`
    publishes that file several boot steps earlier, so a server that binds and then
    dies leaves one behind, and waiting on it hands the caller a bare
    `ECONNREFUSED` with the crash only in the log. Liveness is therefore checked
    before readiness on every pass.

    `read_log` returns this boot's output so far; the caller owns where that comes
    from and what to do with a `proc` that never reported ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"server exited rc={proc.returncode} before becoming ready\n"
                f"{read_log()[-4096:]}")
        if _READY_MARKER in read_log():
            return
        time.sleep(_READY_POLL_S)
    raise TimeoutError(f"server did not start (never reported ready)\n{read_log()[-4096:]}")


def is_debug_build():
    """Whether the server under test still carries the `#[cfg(debug_assertions)]`
    injection seams. Cargo's default `cargo build` keeps them, and so does the
    `checked-server` build (release codegen, debug assertions on); only
    `release-server` drops them.

    There is no reliable signal in the binary, so this reads `GNITZ_RELEASE`,
    which the `e2e-release` Makefile target sets beside `GNITZ_SERVER_BIN` —
    the two name the same build and are set on one line for that reason.
    Nothing else sets it, so every other entry point answers "debug"."""
    return os.environ.get("GNITZ_RELEASE", "0") == "0"


_NEWEST_SOURCE = None


def _newest_source_mtime():
    """mtime of the newest `.rs` or manifest under `crates/`. Memoised: the walk
    covers every crate source and runs once per server a suite spawns."""
    global _NEWEST_SOURCE
    if _NEWEST_SOURCE is None:
        newest = 0.0
        for root, dirs, files in os.walk(REPO_ROOT / "crates"):
            dirs[:] = [d for d in dirs if d not in ("target", ".venv", "__pycache__")]
            for name in files:
                if name.endswith(".rs") or name in ("Cargo.toml", "Cargo.lock"):
                    newest = max(newest, os.stat(os.path.join(root, name)).st_mtime)
        _NEWEST_SOURCE = newest
    return _NEWEST_SOURCE


def server_binary():
    """The server under test, skipping the test if it has not been built.

    An artifact older than the newest Rust source is a **hard failure, not a
    skip**: every `make` target that runs this suite rebuilds first, so getting
    here stale means the run went around `make` — and a stale binary's failure
    mode is a *passing* run of code that is not the code under edit. The
    extension is checked alongside the binary because it carries the SQL planner
    and is just as invisible when stale. `GNITZ_ALLOW_STALE_BIN=1` opts out, for
    when an older binary is the point (bisecting, or a pinned `GNITZ_SERVER_BIN`).
    """
    binary = os.environ.get("GNITZ_SERVER_BIN", str(REPO_ROOT / "gnitz-server"))
    binary = os.path.abspath(binary)
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    if os.environ.get("GNITZ_ALLOW_STALE_BIN", "0") == "0":
        ext = (REPO_ROOT / "crates/gnitz-py/python/gnitz").glob("_native*.so")
        newest = _newest_source_mtime()
        stale = [str(f) for f in (binary, *ext) if os.stat(f).st_mtime < newest]
        if stale:
            raise RuntimeError(
                "Build artifacts are older than the Rust sources:\n  "
                + "\n  ".join(os.path.relpath(f, REPO_ROOT) for f in stale)
                + "\nRun `make e2e` (or `make e2e-debug`), which rebuilds first; "
                "GNITZ_ALLOW_STALE_BIN=1 to override."
            )
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
        env = test_server_env()
        env.update(self.extra_env)     # config of this server, every boot
        env.update(spawn_env or {})    # this boot only
        # A `GNITZ_INJECT_*` var names a `#[cfg(debug_assertions)]` seam a release
        # build folds away, leaving the test asserting against an ordinary healthy
        # run. Read off the prefix here rather than at the call site, so no boot
        # that arms a seam can forget it — the same rule `dedicated_server` applies.
        seams = [k for k in env if k.startswith("GNITZ_INJECT_")]
        if seams and not is_debug_build():
            pytest.skip(f"injection seam requires a debug build: {', '.join(seams)}")
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
        """Spawn and wait until the server declares itself ready. Returns self.

        `workers` sticks — a later `restart()` reboots at the same count.
        `extra_env` applies to *this boot only*, which is what a fault injected
        for one boot and then recovered from needs; put configuration that must
        survive a reboot in `self.extra_env` instead."""
        if workers is not None:
            self.workers = workers
        self.proc = self._popen(extra_env)
        try:
            await_ready(self.proc, self.log_text, timeout)
        except TimeoutError as e:
            self.stop()
            raise RuntimeError(str(e)) from None
        return self

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
            time.sleep(_READY_POLL_S)
        self.stop()
        raise RuntimeError(f"server did not exit within {timeout}s\n{self.log_tail()}")

    # ── stopping ─────────────────────────────────────────────────────────────

    def stop(self):
        if self.proc is None:
            return
        kill_group(self.proc)
        self.proc = None

    def stop_graceful(self, timeout=30):
        """SIGTERM the *master only* (not the process group), so its shutdown
        watcher can drive a final checkpoint (which needs live workers to ACK
        the flush) before broadcasting Shutdown and exiting. A non-zero exit
        means that checkpoint failed and the shards a caller is about to read
        are still in the SAL, so it fails here rather than at each call site."""
        assert self.proc is not None, "no running server"
        try:
            os.kill(self.proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        self.proc.wait(timeout=timeout)
        rc = self.proc.returncode
        self.proc = None
        assert rc == 0, f"graceful shutdown must exit rc 0, got {rc}\n{self.log_tail()}"

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
            self.stop_graceful()
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

    def worker_log_texts(self):
        """Each launched worker's log. Worker-side markers exist because a child
        redirects fd 1 to its own log before it prints them, so the master log
        never sees them. Worker logs are truncated per boot, so the last marker
        in each is this boot's."""
        texts = []
        for w in range(self.workers):
            with open(os.path.join(self.data_dir, f"worker_{w}.log")) as f:
                texts.append(f.read())
        return texts

    def rebuilt_view_count(self):
        """N from the most recent `recovery: rebuilding N invalid view(s)` marker
        the master printed at boot. 0 means every view resumed from its
        checkpoint. The marker is unconditional, so its absence is a harness
        fault rather than a count of zero."""
        matches = re.findall(r"recovery: rebuilding (\d+) invalid view", self.log_text())
        assert matches, f"master printed no view-rebuild marker\n{self.log_tail()}"
        return int(matches[-1])

    def rebuilt_index_counts(self):
        """The sibling index marker, per launched worker."""
        counts = []
        for w, text in enumerate(self.worker_log_texts()):
            markers = re.findall(r"recovery: rebuilding (\d+) index\(es\)", text)
            assert markers, f"worker {w} printed no index-rebuild marker"
            counts.append(int(markers[-1]))
        return counts

    def resliced(self):
        """Whether any launched rank took the changed-worker-count replay path,
        from the per-worker marker that path prints. A same-count boot reads
        every slot straight through and prints nothing, so this is what
        separates a re-cut tail from one that merely happened to land right."""
        return any("re-sliced from" in t for t in self.worker_log_texts())

    def sal_checkpoints(self):
        """How many SAL checkpoint resets this boot has logged. Only visible at
        `GNITZ_LOG_LEVEL=normal`, so a test reading it must set that."""
        return self.log_text().count("SAL checkpoint epoch=")


# Deadlock ceilings, NOT performance budgets: a deadlock never completes, so any
# finite ceiling catches it, and these are generous so a slow-but-completing run
# never flakes. Tightening them to "speed up the tests" only reintroduces that.
HANG_TIMEOUT = 180   # join ceiling for a group of threads
START_TIMEOUT = 60   # thread waiting for the first concurrent write to land


def join_or_fail(why, *threads):
    """Join `threads` against the ceiling above, failing with `why` on the first
    still running. One deadline for the group, not one each.

    Joining and checking are one call because the ceiling catches a wedge only if
    someone looks afterwards: a bare `join(timeout=...)` returns silently and
    leaves the test asserting against a thread that never finished."""
    deadline = time.monotonic() + HANG_TIMEOUT
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
        assert not t.is_alive(), why
