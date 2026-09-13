import contextlib
import itertools
import os
import shutil
import subprocess
import time
import warnings

import pytest
import gnitz
from _paths import REPO_ROOT
from _uid import uid
from _serverproc import (
    NUM_WORKERS,
    ServerProc,
    is_debug_build,
    server_binary,
    server_preexec,
    test_server_env,
)

_TMP_DIR = str(REPO_ROOT / "tmp")
os.makedirs(_TMP_DIR, exist_ok=True)
_LOG_PATH = os.path.join(_TMP_DIR, "server_debug.log")

# Anchor pytest's basetemp inside the repo rather than the default /tmp. Two
# reasons, both about the SAL every server fallocates (128 MiB per test server,
# see `_serverproc.test_server_env`): /tmp is tmpfs here, where that fallocate
# is paid in kernel page-zeroing on every one of the dozens of servers a session
# spawns, and a run with failures retains each failed test's data dir (see the
# retention settings in pyproject.toml), which /tmp does not have the room for.
#
# PYTEST_DEBUG_TEMPROOT (not --basetemp) is the knob that keeps the numbered
# `pytest-of-<user>/pytest-<N>/` layout, and with it both pytest's lock-based
# cross-session GC of dead runs' directories and its wipe of the whole basetemp
# after a green run — `--basetemp` disables both.
os.environ.setdefault("PYTEST_DEBUG_TEMPROOT", _TMP_DIR)

# AF_UNIX caps sun_path at 108 bytes including the NUL, and the failure is an
# opaque errno rather than a message naming the path. Every socket the suite
# creates goes through `_sock_path`, which holds them under this budget; the
# margin is what keeps a deeper checkout or a longer username from turning it
# into a mysterious failure in some tests but not others.
_SUN_PATH_BUDGET = 100


# ── server lifecycle ──────────────────────────────────────────────────────────

class _Server:
    """
    Wraps a gnitz-server subprocess.

    The Unix socket path lives in a stable base directory and never changes
    across restarts.  Only the data directory is replaced on each restart,
    guaranteeing a clean catalog without invalidating any fixture that holds
    the socket path.

    Data directories come from `tmp_path_factory`, not the function-scoped
    `tmp_path`: this server outlives any single test. They are named `d` to keep
    the worker-log paths short.

    This is the only server in the suite with a TLS listener and with a data dir
    that is *replaced* on restart; a test that drives its own server uses
    `ServerProc`, which reboots on the same data dir and skips the per-boot cert
    minting and TCP bind.

    Lifecycle:
        _Server(sock_path, factory)  → __init__ pins the socket path and TLS port
        .start()                     → spawns first process in a fresh data_dir
        .restart()                   → kills process, discards data_dir, spawns again
        .teardown()                  → copies worker logs, kills process, removes dirs
    """

    def __init__(self, sock_path: str, tmp_path_factory):
        self._binary = server_binary()
        self._factory = tmp_path_factory
        self.sock_path = sock_path
        # One free TCP port, allocated once and passed on EVERY spawn
        # (including restarts): the TLS address must be as stable across
        # restarts as the socket path, so fixtures holding a target string
        # survive a server restart.
        self.tls_port = _probe_free_port()
        self.proc = None
        self._stderr_f = None
        self._data_dir: str | None = None
        # Read fresh on every spawn, not once here: the seam fixtures monkeypatch
        # GNITZ_WORKERS around the construction. Recorded so teardown copies the
        # logs of exactly the workers that ran.
        self._workers = NUM_WORKERS

    # ── public ────────────────────────────────────────────────────────────────

    def start(self) -> None:
        # Bounded retry with a fresh port: the pre-allocated port can be
        # stolen between the probe and the bind. A port stolen *between
        # restarts* still fails fast — the stable-target requirement forbids
        # re-porting mid-session.
        for attempt in range(3):
            try:
                self._spawn()
                return
            except RuntimeError:
                if attempt == 2:
                    raise
                self.tls_port = _probe_free_port()

    @property
    def tls_target(self) -> str:
        """TLS connect string for the always-on TLS listener."""
        return f"tls://127.0.0.1:{self.tls_port}?insecure"

    @property
    def target(self) -> str:
        """Connect string for clients: the socket path, or the TLS address
        when GNITZ_TRANSPORT=tls (the full-suite transport sweep)."""
        if os.environ.get("GNITZ_TRANSPORT") == "tls":
            return self.tls_target
        return self.sock_path

    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def restart(self) -> None:
        """Kill the current process, discard its data dir, spawn fresh."""
        if self.is_alive():
            self.proc.kill()
            self.proc.wait()
        if self._stderr_f:
            self._stderr_f.close()
            self._stderr_f = None
        with open(_LOG_PATH, "a") as f:
            f.write("\n\n--- server restarted by _server_guard (crash above this line) ---\n\n")
        self._spawn()

    def teardown(self) -> None:
        """Copy worker logs to _TMP_DIR, then kill and clean up."""
        if self._data_dir:
            data = os.path.join(self._data_dir, "data")
            for i in range(self._workers):
                src = os.path.join(data, f"worker_{i}.log")
                if os.path.exists(src):
                    shutil.copy2(src, os.path.join(_TMP_DIR, f"last_worker_{i}.log"))
        if self.is_alive():
            self.proc.kill()
            self.proc.wait()
        if self._stderr_f:
            self._stderr_f.close()
        self._discard_data_dir()

    # ── private ───────────────────────────────────────────────────────────────

    def _discard_data_dir(self) -> None:
        """Drop the current data dir now rather than leave it to pytest's
        end-of-session reclaim: each one holds a fallocated SAL, and a
        session spawns dozens of servers."""
        if self._data_dir:
            shutil.rmtree(self._data_dir, ignore_errors=True)
            self._data_dir = None

    def _spawn(self) -> None:
        """Start a new server process in a fresh data directory."""
        self._discard_data_dir()
        self._data_dir = str(self._factory.mktemp("d"))
        data_dir = os.path.join(self._data_dir, "data")
        if os.path.exists(self.sock_path):
            os.unlink(self.sock_path)
        self._workers = int(os.environ.get("GNITZ_WORKERS", NUM_WORKERS))
        cmd = [self._binary, data_dir, self.sock_path,
               f"--tls-listen=127.0.0.1:{self.tls_port}",
               f"--workers={self._workers}"]
        if ll := os.environ.get("GNITZ_LOG_LEVEL"):
            cmd += [f"--log-level={ll}"]
        # Append so a restart does not discard the log that contains the crash.
        self._stderr_f = open(_LOG_PATH, "a")
        env = test_server_env()
        # preexec_fn ties the master's life to pytest's (PR_SET_PDEATHSIG): an
        # interrupted run (Ctrl-C / SIGKILL / crash) can't orphan the server.
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=self._stderr_f,
                                     env=env, preexec_fn=server_preexec)
        # Readiness = socket file + TLS endpoint published (the endpoint file
        # is rename-published after the TCP bind, just before "GnitzDB ready").
        tls_endpoint = os.path.join(data_dir, "tls_endpoint")
        for _ in range(10_000):
            if os.path.exists(self.sock_path) and os.path.exists(tls_endpoint):
                break
            # Fail fast on a dead process (e.g. the pre-allocated port was
            # stolen) instead of blind-waiting the full 10 s and cascading a
            # generic error through the session.
            if self.proc.poll() is not None:
                self._stderr_f.close()
                self._stderr_f = None
                tail = _log_tail()
                raise RuntimeError(
                    f"Server exited during startup (rc={self.proc.returncode}).\n"
                    f"stderr tail:\n{tail}"
                )
            time.sleep(0.001)
        else:
            self.proc.kill()
            self.proc.communicate()
            self._stderr_f.close()
            self._stderr_f = None
            raise RuntimeError("Server did not start within 10 s")


def _probe_free_port() -> int:
    """Bind 127.0.0.1:0, read the assigned port back, close."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _log_tail(max_bytes: int = 8192) -> str:
    """Last few KB of the shared server log, for fail-fast diagnostics."""
    try:
        with open(_LOG_PATH, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - max_bytes))
            return f.read().decode(errors="replace")
    except OSError:
        return "<no server log>"


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def _sock_path(tmp_path_factory):
    """Mints socket paths for every server the suite starts.

    One short session directory holds them all. Putting a socket under a test's
    own `tmp_path` instead would name it after the test — 30 characters plus a
    number — which lands within a few bytes of the `sun_path` limit and pushes
    past it on a deeper checkout. A shared directory keeps every socket path the
    same modest length however many tests run, and costs one directory per
    session rather than one per test.

    Sockets are bytes-free, so leaving them to pytest's end-of-session reclaim
    (rather than the eager cleanup the data dirs get) costs nothing.
    """
    sock_dir = tmp_path_factory.mktemp("s")
    counter = itertools.count()

    def make() -> str:
        path = str(sock_dir / f"{next(counter)}.sock")
        assert len(os.fsencode(path)) < _SUN_PATH_BUDGET, (
            f"socket path is {len(os.fsencode(path))} bytes, over the "
            f"{_SUN_PATH_BUDGET}-byte sun_path budget: {path}"
        )
        return path

    return make


@pytest.fixture
def server_dirs(tmp_path, _sock_path):
    """`(data_dir, sock_path)` for a server the test starts itself.

    `data_dir` is where the gigabytes go, and `tmp_path` reclaims it as soon as a
    passing test finishes (`tmp_path_retention_policy = "failed"` in
    pyproject.toml); a failing test keeps it for post-mortem.
    """
    return str(tmp_path / "data"), _sock_path()


@pytest.fixture
def own_server(server_dirs):
    """A `ServerProc` the test starts and drives itself, stopped at teardown
    however the test left it. This is what a test uses to crash a server and
    reboot it on the same data dir; `server` / `client` are the shared session
    server, which no test may kill."""
    data_dir, sock_path = server_dirs
    proc = ServerProc(data_dir, sock_path)
    try:
        yield proc
    finally:
        proc.stop()


# A store spills once its RAM tier crosses this ceiling, which only happens at a
# memtable fold or a checkpoint's ephemeral round — so the checkpoint threshold
# is squeezed too. Together they give a few thousand rows the many spills a
# capacity sweep needs: it budgets itself to one push-down per spill.
_SWEEP_ENV = {"GNITZ_RAM_TIER_BYTES": "1024", "GNITZ_CHECKPOINT_BYTES": str(32 * 1024)}


@pytest.fixture
def sweeping_server(server_dirs):
    """A server whose stores sweep on modest data, for the capacity, retention
    and cursor-expiry cases. Shared by the bounded-view, delta-feed and mirror
    suites — the mirror's own expiry recovery is the same event seen from the
    other side."""
    data_dir, sock_path = server_dirs
    proc = ServerProc(data_dir, sock_path, extra_env=dict(_SWEEP_ENV))
    proc.start()
    try:
        yield proc
    finally:
        proc.stop()


@pytest.fixture
def sweeping_client(sweeping_server):
    """A connection to `sweeping_server`, for a test that only reads and writes
    over it."""
    with gnitz.connect(sweeping_server.sock_path) as conn:
        yield conn


@pytest.fixture(scope="session")
def _srv(tmp_path_factory, _sock_path):
    """Session-scoped mutable server handle shared by all per-class and per-test fixtures."""
    s = _Server(_sock_path(), tmp_path_factory)
    s.start()
    yield s
    s.teardown()


@pytest.fixture(scope="session")
def server(_srv):
    """
    Connect target of the session server (socket path, or the TLS address
    under GNITZ_TRANSPORT=tls).  Stable across restarts — the socket lives
    in a fixed base directory and the TLS port is pinned per session, so
    this value never changes.
    """
    return _srv.target


@pytest.fixture
def client(_srv):
    """Per-test connection.  Always resolves through _srv so it follows restarts."""
    with gnitz.connect(_srv.target) as conn:
        yield conn


@pytest.fixture
def schema_name(client):
    """A fresh schema for one test, dropped whole at teardown.

    `drop_schema` is `DROP SCHEMA ... CASCADE`: one atomic DDL bundle that
    retracts every view, then every table (each cascading its own indexes),
    then the schema row. So a test never names its own objects to tear them
    down, and dropping them by hand only adds round trips that must be
    redundant or fail.

    The drop is not wrapped: a schema that refuses to drop is a finding, and
    swallowing it leaks the objects into the shared session catalog for every
    later test.
    """
    sn = "s" + uid()
    client.create_schema(sn)
    yield sn
    client.drop_schema(sn)


# ── mirror fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def mirror_dir(tmp_path):
    """A private copy directory for one mirroring client, reclaimed with the
    test."""
    return str(tmp_path / "mirror")


@pytest.fixture
def mirror_on():
    """Factory for a client with a copy directory attached — `mirror_on(base_dir,
    target)` — closed at teardown however the test left it.

    Closing is not tidiness: an unclosed client skips the exit checkpoint and
    keeps its copy directory locked for the life of the interpreter.
    """
    with contextlib.ExitStack() as stack:
        def make(base_dir, target):
            client = stack.enter_context(gnitz.connect(target))
            client.mirror_at(base_dir)
            return client

        yield make


@pytest.fixture
def mirror(mirror_on, server, mirror_dir):
    """A mirroring client on the session server at `mirror_dir`."""
    return mirror_on(mirror_dir, server)


@pytest.fixture
def dedicated_server(monkeypatch, tmp_path_factory, _sock_path):
    """Factory for a server separate from the session one, torn down with the
    test. Call it as `dedicated_server(env)` → the started `_Server`, whose
    `.target` the test connects to and whose `.proc` it can assert is alive.

    Forces >= 2 workers — the paths under test are distributed. monkeypatch
    reverts the env vars at teardown; it is a dependency of this fixture, so it
    does so only after the server is dead.

    A `GNITZ_INJECT_*` var names a `#[cfg(debug_assertions)]` seam that a release
    build folds away, leaving the test asserting against an ordinary healthy
    run, so such a request skips on a release build. Read off the prefix, not
    passed in, so it cannot be forgotten at a call site.
    """
    started = []

    def make(env: dict[str, str]):
        if not is_debug_build() and any(k.startswith("GNITZ_INJECT_") for k in env):
            pytest.skip(f"injection seam requires a debug build: {', '.join(env)}")
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        if int(os.environ.get("GNITZ_WORKERS", NUM_WORKERS)) < 2:
            monkeypatch.setenv("GNITZ_WORKERS", "4")
        s = _Server(_sock_path(), tmp_path_factory)
        started.append(s)
        s.start()
        return s

    try:
        yield make
    finally:
        for s in started:
            s.teardown()


@pytest.fixture
def seamed_server(dedicated_server):
    """`dedicated_server`, returning a connected client instead of
    `(target, proc)`, for a test that only reads and writes."""
    with contextlib.ExitStack() as stack:
        def make(env: dict[str, str]):
            return stack.enter_context(gnitz.connect(dedicated_server(env).target))

        yield make


@pytest.fixture
def adhoc_group_cap_server(seamed_server):
    """Server with a tiny ad-hoc aggregate per-worker group cap, to exercise the
    GROUP BY resource-exhaustion abort (`GNITZ_ADHOC_GROUP_CAP`). Not a debug-only
    seam — the cap is read at bootstrap on every build."""
    return seamed_server({"GNITZ_ADHOC_GROUP_CAP": "4"})


@pytest.fixture
def tiny_ddl_chunk_server(seamed_server):
    """Server whose chunked scans drain in 3-row chunks, so a small table already
    spans many chunk boundaries: `GNITZ_SCAN_CHUNK_ROWS` sizes index and view
    backfill, the bounded-view hydration merge, and the ad-hoc `ReadSpec` scan
    alike. At the 65 536-row default a test table is one chunk and pins nothing
    about chunk boundaries."""
    return seamed_server({"GNITZ_SCAN_CHUNK_ROWS": "3"})


@pytest.fixture
def unique_preflight_frame_server(seamed_server):
    """Server whose CREATE UNIQUE INDEX pre-flight streams tiny (7-key) frames
    so a small table already produces multi-frame continuation trains per
    worker. Any per-worker frame count is safe: `InFlightState` grows with the
    parked depth, so the W2M ring back-pressures by bytes, not a frame count."""
    return seamed_server({"GNITZ_UNIQUE_PREFLIGHT_KEYS_PER_FRAME": "7"})


@pytest.fixture
def reply_frame_budget_server(seamed_server):
    """Server whose workers chunk reply trains past a tiny 16 KiB frame budget, so
    modest tables already produce multi-frame seek / range / gather / scan reply
    trains per worker. Any reply size is safe: the
    master parks a full train per ring while draining another worker, but
    `InFlightState` grows to track it, so the ring back-pressures by bytes, not
    a frame count."""
    return seamed_server({"GNITZ_REPLY_FRAME_BUDGET": str(16 * 1024)})


@pytest.fixture
def unique_preflight_fault_server(seamed_server):
    """Server whose workers fail every CREATE UNIQUE INDEX pre-flight scan,
    for asserting the master surfaces the fault, creates no index, seeds no
    filter, and leaves the cluster healthy."""
    return seamed_server({"GNITZ_INJECT_UNIQUE_PREFLIGHT_ERROR": "1"})


@pytest.fixture
def unique_preflight_spill_server(seamed_server):
    """Server whose CREATE UNIQUE INDEX pre-flight spills its key sort to disk at
    a tiny 256-byte budget, so a few hundred rows per worker force many
    external-sort spill runs and a k-way merge — exercising the bounded-memory
    path end-to-end. Unlike the debug seams above, `GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES`
    is a real config knob honoured in every build, so this also bites a release
    server."""
    return seamed_server({"GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES": "256"})


@pytest.fixture
def relay_lowspace_server(dedicated_server):
    """Server with the one-shot low-relay-space seam (GNITZ_INJECT_RELAY_SPACE_LOW)
    armed, for the barrier-only-checkpoint reclaim test. Unlike the other seam
    fixtures it yields `(target, proc)` so the test can assert the master is
    still alive after the low-space relay.

    No GNITZ_CHECKPOINT_BYTES override — the only checkpoint in the green run is
    the injected one, keeping the view results reliable."""
    srv = dedicated_server({"GNITZ_INJECT_RELAY_SPACE_LOW": "1"})
    return srv.target, srv.proc


@pytest.fixture
def tick_emit_fault_server(seamed_server):
    """Server whose first *replied* master-side tick emission fails, reproducing
    what a full SAL does to a tick. The CREATE's own view-seeding drain writes a
    silent tick group and so cannot spend it, and it is one-shot, so the
    follow-up read observes the re-queued tid ticking and the view converging."""
    return seamed_server({"GNITZ_INJECT_TICK_EMIT_ERROR": "1"})


@pytest.fixture
def tiny_sal_server(dedicated_server):
    """SAL pinned to its 16 MiB floor with the checkpoint threshold above the
    watchdog's 1/8-free line, so the watchdog is the ONLY thing that can reclaim
    — a committer checkpoint at the default 3/4 threshold would otherwise reset
    the cursor first and the test would pass with the watchdog deleted. Both are
    real config knobs honoured in every build."""
    srv = dedicated_server({
        "GNITZ_SAL_BYTES": str(16 * 1024 * 1024),
        "GNITZ_CHECKPOINT_BYTES": str(15 * 1024 * 1024),
    })
    return srv.target, srv.proc


@pytest.fixture
def disposable_server(dedicated_server):
    """A server the test alone owns, as `(target, proc)` so the test may kill it
    mid-flight. The session server cannot be used for that — every other test
    shares it. Teardown is kill-safe: `_Server.teardown` tolerates a process the
    test already reaped."""
    srv = dedicated_server({})
    return srv.target, srv.proc


@pytest.fixture
def restartable_server(dedicated_server):
    """The `_Server` handle itself, for a test that restarts it and reconnects
    on the same pinned TLS port."""
    return dedicated_server({})


@pytest.fixture
def checkpoint_server(dedicated_server):
    """Connect target of a server with a tiny SAL checkpoint threshold, so
    checkpoints fire repeatedly during a test's own writes. 32 KB: a single push
    of ~500 rows encodes to roughly 30-60 KB, so this fires several checkpoints
    per bulk insert. A target rather than a client — these tests drive a pusher
    and a reader concurrently over separate connections."""
    return dedicated_server({"GNITZ_CHECKPOINT_BYTES": str(32 * 1024)}).target


@pytest.fixture(autouse=True, scope="class")
def _server_guard(_srv, request):
    """
    Pre-class health gate.

    Checks whether the server process is still alive before each test class
    runs.  If it has died — which means a prior test triggered a panic that
    escaped guard_panic, or an OOM/signal killed the process — the server is
    restarted with a fresh catalog and a RuntimeWarning is emitted.

    Without this guard, one server death cascades into hundreds of ERROR
    entries for unrelated tests.  With it, only the class that caused the
    death produces genuine failures; subsequent classes see a fresh server
    and either pass or fail on their own merits.

    Triage procedure when a restart warning appears:
      1. Find the first FAILED or ERROR *before* the restart warning in
         pytest's chronological output (not alphabetical file order).
      2. Open <repo>/tmp/server_debug.log — the crash backtrace is
         before the "restarting" separator line.
      3. Run that one test in isolation with a fresh server to reproduce.
    """
    if not _srv.is_alive():
        class_id = (
            getattr(request.cls, "__name__", None)
            or getattr(request.node, "nodeid", repr(request.node))
        )
        warnings.warn(
            f"[gnitz] server died before '{class_id}' — restarting with a fresh catalog. "
            "Failures in THIS class may be secondary cascades. "
            "The true root cause is in the class that ran immediately before. "
            "See <repo>/tmp/server_debug.log for the crash.",
            RuntimeWarning,
            stacklevel=2,
        )
        _srv.restart()
    yield
