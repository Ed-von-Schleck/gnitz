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
import resource
import signal
import subprocess
import threading
import time

import gnitz
import pytest

from _paths import REPO_ROOT

# Every loop polling at this has its own deadline; the interval is granularity.
_READY_POLL_S = 0.002

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

# What a test on its own server boots when its claim needs more than one worker.
MULTI = max(2, NUM_WORKERS)

# Environments for `own_server.start(extra_env=...)` that more than one module boots.

# Chunked scans drain in 3-row chunks, so a small table already spans many chunk
# boundaries: the knob sizes index and view backfill, the bounded-view hydration
# merge, and the ad-hoc `ReadSpec` scan alike. At the 65 536-row default a test
# table is one chunk and pins nothing about chunk boundaries.
TINY_SCAN_CHUNKS = {"GNITZ_SCAN_CHUNK_ROWS": "3"}

# Workers chunk reply trains past a 16 KiB frame budget, so modest tables already
# produce multi-frame seek / range / gather / scan reply trains per worker. Any
# reply size is safe: the master parks a full train per ring while draining
# another worker, but `InFlightState` grows to track it, so the ring
# back-pressures by bytes, not a frame count.
TINY_REPLY_FRAMES = {"GNITZ_REPLY_FRAME_BUDGET": str(16 * 1024)}

# Stores reach the disk regime on a few thousand rows. A store spills once its
# RAM tier crosses this ceiling, which only happens at a memtable fold or a
# checkpoint's ephemeral round — so the checkpoint threshold is squeezed too.
SWEEP_ENV = {"GNITZ_RAM_TIER_BYTES": "1024", "GNITZ_CHECKPOINT_BYTES": str(16 * 1024)}

# The pending rows of one table that fire a tick without a read draining it.
TICK_COALESCE_ROWS = 10_000

# Every stage `GNITZ_INJECT_RECOVERY_PANIC` names: the boot's recovery steps
# from the generation bump to the tick of the replayed tail.
RECOVERY_STAGES = ["genbump", "reset", "backfill", "tick"]


def server_env():
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

    `GNITZ_TICK_COALESCE_ROWS` is set outright, not defaulted: a test sizing a
    push to fire a tick reads the threshold from `TICK_COALESCE_ROWS`, so the
    server must boot with that value; a test needing another passes it in
    `extra_env`.
    """
    env = os.environ.copy()
    for k, v in (("GNITZ_SAL_BYTES", str(128 * 1024 * 1024)), ("GNITZ_CPU_AFFINITY", "0")):
        env.setdefault(k, v)
    env["GNITZ_TICK_COALESCE_ROWS"] = str(TICK_COALESCE_ROWS)
    return env


def server_preexec():
    parent = os.getppid()
    _libc.prctl(_PR_SET_PDEATHSIG, signal.SIGKILL)
    # Close the race where pytest exited between fork() and the prctl above:
    # we were already reparented, so the death signal would never arrive.
    if os.getppid() != parent:
        os._exit(1)
    # The server aborts on every panic, an injected one included, and a core
    # handler reads every page of the image, its sparse shared mappings too:
    # one dump outlasts the boot many times over, holding the data dir all
    # along. A test reads a crash off the server's log instead.
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


# Deadlock ceiling, not a performance budget: a group of killed processes that
# never finishes exiting fails here rather than hanging the test.
_GROUP_EXIT_TIMEOUT_S = 60


def await_group_exit(proc):
    """Return once no process of `proc`'s group is left; `proc` itself must be
    reaped already. The master's exit is not the server's: each worker shares
    the data dir's lock and the master's shared mappings, dies of its PDEATHSIG
    only asynchronously, and is reparented rather than reaped here, so it can
    hold both for seconds after the master's exit code is in. Anything that
    touches the data dir next waits here: a reboot gives up on a held lock, and
    an `rmtree` races a worker still mapping `wal.sal`."""
    deadline = time.monotonic() + _GROUP_EXIT_TIMEOUT_S
    while True:
        try:
            # The group id is the master's pid, and stays the group's after the
            # master is reaped, for as long as any member is left.
            os.killpg(proc.pid, 0)
        except ProcessLookupError:
            return
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"server group {proc.pid} still running {_GROUP_EXIT_TIMEOUT_S}s after its master exited")
        time.sleep(_READY_POLL_S)


def kill_group(proc):
    """SIGKILL `proc`'s whole process group — master and workers together — and
    return once all of it is gone. Requires the spawn to have used
    `start_new_session`, or this reaches the caller too."""
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    proc.wait()
    await_group_exit(proc)


def await_log(proc, read_log, marker, timeout):
    """Block until `proc` logs `marker` and return the line holding it, raising
    with the log tail if it exits first. `read_log` returns the output so far."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"server exited rc={proc.returncode} before logging {marker!r}\n"
                f"{read_log()[-4096:]}")
        line = next((l for l in read_log().splitlines() if marker in l), None)
        if line is not None:
            return line
        time.sleep(_READY_POLL_S)
    raise TimeoutError(f"server never logged {marker!r}\n{read_log()[-4096:]}")


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


def disk_usage(data_dir):
    """`gnitz-server --disk-usage data_dir`, which reads the files alone and so
    runs beside a live server: the report, and each of its store lines — one per
    relation, store and LSM level — as a dict."""
    report = subprocess.run(
        [server_binary(), "--disk-usage", str(data_dir)], check=True, capture_output=True, text=True
    ).stdout
    stores = []
    for line in report.splitlines():
        f = line.split(maxsplit=8)
        if f and f[0].isdigit():
            stores.append({
                "relation": int(f[0]), "store": f[1], "level": f[2], "files": int(f[3]),
                "skeletons": int(f[4]), "rows": int(f[5]), "bytes": int(f[6]),
                "regions": f[8] if len(f) > 8 else "", "line": line,
            })
    return report, stores


class ServerProc:
    """One gnitz-server process over a fixed `(data_dir, sock_path)` pair.

    `restart` reboots on the SAME data dir, which is what the recovery tests
    assert against; only the session server in conftest discards its data dir
    between runs, and it does that by pointing `data_dir` at a fresh one.

    Every boot also listens for TLS on a port of the kernel's choosing, so
    `target` can answer with either transport.

    Output goes to a file next to the data dir, never a pipe: the master logs
    unbuffered to fd 1/2, and at `GNITZ_LOG_LEVEL=debug` it would fill a 64 KiB
    pipe buffer and block forever mid-test. Boots append to the one file, and
    `log_text` returns only what the current boot wrote, so a marker from an
    earlier boot can never satisfy an assertion about this one.
    """

    def __init__(self, data_dir, sock_path, *, log_path=None):
        self.data_dir = data_dir
        self.sock_path = sock_path
        self.workers = NUM_WORKERS
        # Configuration of this server on every boot; `start(extra_env=...)` is
        # for one boot only.
        self.extra_env = {}
        # Arguments of this server on every boot, behind the ones every server gets.
        self.extra_args = []
        self.log_path = log_path or data_dir.rstrip("/") + ".log"
        self.proc = None
        self.tls_target = None
        self._log_start = 0

    @property
    def target(self):
        """Connect string for clients: the socket path, or this boot's TLS
        address under GNITZ_TRANSPORT=tls (the full-suite transport sweep)."""
        return self.tls_target if os.environ.get("GNITZ_TRANSPORT") == "tls" else self.sock_path

    # ── spawning ─────────────────────────────────────────────────────────────

    def _popen(self, spawn_env, extra_args=()):
        env = server_env()
        env.update(self.extra_env)     # config of this server, every boot
        env.update(spawn_env or {})    # this boot only
        # A `GNITZ_INJECT_*` var names a `#[cfg(debug_assertions)]` seam a release
        # build folds away, leaving the test asserting against an ordinary healthy
        # run. Read off the prefix here rather than at the call site, so no boot
        # that arms a seam can forget it.
        seams = [k for k in env if k.startswith("GNITZ_INJECT_")]
        if seams and not is_debug_build():
            pytest.skip(f"injection seam requires a debug build: {', '.join(seams)}")
        # Where this boot's output starts, so `log_text` can exclude the last.
        self._log_start = os.path.getsize(self.log_path) if os.path.exists(self.log_path) else 0
        log = open(self.log_path, "ab")
        try:
            return subprocess.Popen(
                [server_binary(), self.data_dir, self.sock_path, f"--workers={self.workers}",
                 "--tls-listen=127.0.0.1:0", *self.extra_args, *extra_args],
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
            await_log(self.proc, self.log_text, gnitz._native.READY_LINE, timeout)
        except TimeoutError as e:
            self.stop()
            raise RuntimeError(str(e)) from None
        with open(os.path.join(self.data_dir, "tls_endpoint")) as f:
            port = f.read().strip().rsplit(":", 1)[1]
        self.tls_target = f"tls://127.0.0.1:{port}?ca={self.data_dir}/tls_cert.pem"
        return self

    def start_expecting_exit(self, marker, *, workers=None, extra_env=None, extra_args=(), timeout=20.0):
        """Spawn a server expected to die during boot. Returns its non-zero exit
        code, having confirmed it never became ready and logged `marker` — in the
        master's log or a worker's — so a boot that died of anything else fails.
        `extra_args` follow the arguments every boot gets, so a flag given there
        replaces its default."""
        if workers is not None:
            self.workers = workers
        self.proc = self._popen(extra_env, extra_args)
        deadline = time.time() + timeout
        while time.time() < deadline:
            rc = self.proc.poll()
            if rc is not None:
                self.stop()
                assert rc != 0, f"the boot exited cleanly\n{self.log_tail()}"
                assert marker in self.log_text() or any(marker in t for t in self.worker_log_texts()), \
                    f"the boot died without logging {marker!r}, rc={rc}\n{self.log_tail()}"
                return rc
            if gnitz._native.READY_LINE in self.log_text():
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
        the flush) before killing them and exiting. A non-zero exit
        means that checkpoint failed and the shards a caller is about to read
        are still in the SAL, so it fails here rather than at each call site."""
        assert self.proc is not None, "no running server"
        try:
            os.kill(self.proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        self.proc.wait(timeout=timeout)
        await_group_exit(self.proc)
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
        await_group_exit(self.proc)
        rc = self.proc.returncode
        self.proc = None
        return rc

    def exit_code_on(self, sql, seam):
        """Reboot with the fault `seam` armed for that boot, issue `sql`, and
        return the code the server died with. The request's own outcome is not
        the observable — whether it errors depends on whether the abort lands
        before or after its reply was queued. A statement that never reaches the
        seam leaves the server up, which `wait_for_exit` reports."""
        self.restart(extra_env=seam)
        try:
            with gnitz.connect(self.target) as conn:
                conn.execute_sql(sql)
        except gnitz.GnitzError:
            pass
        return self.wait_for_exit()

    # ── restarting ───────────────────────────────────────────────────────────

    def restart(self, *, graceful=False, workers=None, extra_env=None, timeout=10.0):
        """Stop and reboot on the SAME data dir. `graceful=True` shuts down via
        SIGTERM (final checkpoint) and asserts a clean exit; the default SIGKILL
        is the crash-recovery path."""
        if graceful:
            self.stop_graceful()
        else:
            self.stop()
        return self.start(workers=workers, extra_env=extra_env, timeout=timeout)

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

    def worker_pids(self):
        """`{worker index: pid}` of the running master's children: each worker's
        stdout is its own `worker_<N>.log`."""
        pids = {}
        for name in os.listdir("/proc"):
            if not name.isdigit():
                continue
            try:
                with open(f"/proc/{name}/stat") as f:
                    ppid = int(f.read().rsplit(")", 1)[1].split()[1])
                log = os.readlink(f"/proc/{name}/fd/1")
            except (FileNotFoundError, ProcessLookupError, PermissionError, IndexError):
                continue
            m = re.search(r"worker_(\d+)\.log$", log)
            if ppid == self.proc.pid and m:
                pids[int(m.group(1))] = int(name)
        return pids

    def worker_log_texts(self):
        """Each launched worker's log. Worker-side markers exist because a child
        redirects fd 1 to its own log before it prints them, so the master log
        never sees them. Worker logs are truncated per boot, so the last marker
        in each is this boot's."""
        texts = []
        for w in range(self.workers):
            try:
                with open(os.path.join(self.data_dir, f"worker_{w}.log")) as f:
                    texts.append(f.read())
            except FileNotFoundError:
                texts.append("")
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
        """How many checkpoints this boot has logged: one line per checkpoint, at
        its base round's rewind of the SAL. The boot's own checkpoint has no base
        round and logs none."""
        return self.log_text().count("SAL checkpoint epoch=")


# Deadlock ceilings, NOT performance budgets: a deadlock never completes, so any
# finite ceiling catches it, and these are generous so a slow-but-completing run
# never flakes. Tightening them to "speed up the tests" only reintroduces that.
HANG_TIMEOUT = 180   # join ceiling for a group of threads
START_TIMEOUT = 60   # thread waiting for the first concurrent write to land


class _Spawned(threading.Thread):
    error = None

    def run(self):
        try:
            super().run()
        except BaseException as e:  # noqa: BLE001 — re-raised by `join_or_fail`
            self.error = e


def spawn(target, *args):
    """Start `target(*args)` on a thread that keeps what it raised, for
    `join_or_fail` to re-raise — a plain thread's exception is printed and lost,
    and the test goes on to pass. A daemon, so a wedged one fails its test
    instead of hanging the interpreter's exit."""
    t = _Spawned(target=target, args=args, daemon=True)
    t.start()
    return t


def join_or_fail(why, *threads):
    """Join `spawn`ed `threads` against the ceiling above, failing with `why` on
    the first still running, then re-raise the first exception one of them
    raised. One deadline for the group, not one each.

    Joining and checking are one call because the ceiling catches a wedge only if
    someone looks afterwards: a bare `join(timeout=...)` returns silently and
    leaves the test asserting against a thread that never finished."""
    deadline = time.monotonic() + HANG_TIMEOUT
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
        assert not t.is_alive(), why
    for t in threads:
        if t.error is not None:
            raise t.error


def across(background, step, after=20):
    """Run `step(k)` for k = 0, 1, … — once before `background()` starts on a
    `spawn`ed thread, throughout it, and `after` more times once it returned —
    and return how many steps ran. Both sides of it are therefore always
    exercised."""
    step(0)
    th = spawn(background)
    k, past, deadline = 1, 0, time.monotonic() + HANG_TIMEOUT
    try:
        while past < after:
            assert time.monotonic() < deadline, "the background operation never returned"
            past += not th.is_alive()
            step(k)
            k += 1
    finally:
        join_or_fail("the background operation hung", th)
    return k
