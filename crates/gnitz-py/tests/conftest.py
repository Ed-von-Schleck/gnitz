import os
import shutil
import subprocess
import tempfile
import time
import warnings

import pytest
import gnitz
from _serverproc import server_preexec, is_debug_build

_TMP_DIR = os.path.expanduser("~/git/gnitz/tmp")
os.makedirs(_TMP_DIR, exist_ok=True)
_LOG_PATH = os.path.join(_TMP_DIR, "server_debug.log")


# ── server lifecycle ──────────────────────────────────────────────────────────

class _Server:
    """
    Wraps a gnitz-server subprocess.

    The Unix socket path lives in a stable base directory and never changes
    across restarts.  Only the data directory is replaced on each restart,
    guaranteeing a clean catalog without invalidating any fixture that holds
    the socket path.

    Lifecycle:
        _Server(binary)  → __init__ creates base_dir / sock_path
        .start()         → spawns first process in a fresh data_dir
        .restart()       → kills process, discards data_dir, spawns again
        .teardown()      → copies worker logs, kills process, removes all dirs
    """

    def __init__(self, binary: str):
        self._binary = binary
        self._base_dir = tempfile.mkdtemp(dir=_TMP_DIR, prefix="gnitz_py_")
        self.sock_path = os.path.join(self._base_dir, "gnitz.sock")
        # One free TCP port, allocated once and passed on EVERY spawn
        # (including restarts): the TLS address must be as stable across
        # restarts as the socket path, so fixtures holding a target string
        # survive a server restart.
        self.tls_port = _probe_free_port()
        self.proc = None
        self._stderr_f = None
        self._data_dir: str | None = None

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
        if self._data_dir:
            shutil.rmtree(self._data_dir, ignore_errors=True)
            self._data_dir = None
        with open(_LOG_PATH, "a") as f:
            f.write("\n\n--- server restarted by _server_guard (crash above this line) ---\n\n")
        self._spawn()

    def teardown(self) -> None:
        """Copy worker logs to _TMP_DIR, then kill and clean up."""
        if self._data_dir:
            data = os.path.join(self._data_dir, "data")
            n = int(os.environ.get("GNITZ_WORKERS", "4"))
            for i in range(n):
                src = os.path.join(data, f"worker_{i}.log")
                if os.path.exists(src):
                    shutil.copy2(src, os.path.join(_TMP_DIR, f"last_worker_{i}.log"))
        if self.is_alive():
            self.proc.kill()
            self.proc.wait()
        if self._stderr_f:
            self._stderr_f.close()
        shutil.rmtree(self._base_dir, ignore_errors=True)
        if self._data_dir:
            shutil.rmtree(self._data_dir, ignore_errors=True)

    # ── private ───────────────────────────────────────────────────────────────

    def _spawn(self) -> None:
        """Start a new server process in a fresh data directory."""
        self._data_dir = tempfile.mkdtemp(dir=_TMP_DIR, prefix="gnitz_data_")
        data_dir = os.path.join(self._data_dir, "data")
        if os.path.exists(self.sock_path):
            os.unlink(self.sock_path)
        cmd = [self._binary, data_dir, self.sock_path,
               f"--tls-listen=127.0.0.1:{self.tls_port}"]
        if w := os.environ.get("GNITZ_WORKERS"):
            cmd += [f"--workers={w}"]
        if ll := os.environ.get("GNITZ_LOG_LEVEL"):
            cmd += [f"--log-level={ll}"]
        # Append so a restart does not discard the log that contains the crash.
        self._stderr_f = open(_LOG_PATH, "a")
        # preexec_fn ties the master's life to pytest's (PR_SET_PDEATHSIG): an
        # interrupted run (Ctrl-C / SIGKILL / crash) can't orphan the server.
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=self._stderr_f,
                                     preexec_fn=server_preexec)
        # Readiness = socket file + TLS endpoint published (the endpoint file
        # is rename-published after the TCP bind, just before "GnitzDB ready").
        tls_endpoint = os.path.join(data_dir, "tls_endpoint")
        for _ in range(100):
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
            time.sleep(0.1)
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

def _server_binary():
    """Resolve the server binary path, skipping the test if it is missing."""
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../gnitz-server")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    return binary


@pytest.fixture(scope="session")
def _srv():
    """Session-scoped mutable server handle shared by all per-class and per-test fixtures."""
    s = _Server(_server_binary())
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


def _seamed_server(monkeypatch, env: dict[str, str]):
    """Spawn a dedicated server (separate from the session server) with
    debug-only env seams set, yielding a connected client. Shared body of the
    seam fixtures below; no-op against a release server (the seams are
    `#[cfg(debug_assertions)]`). Forces >= 2 workers — the paths under test
    are distributed. monkeypatch reverts the env vars at fixture teardown."""
    binary = _server_binary()

    for k, v in env.items():
        monkeypatch.setenv(k, v)
    if int(os.environ.get("GNITZ_WORKERS", "1")) < 2:
        monkeypatch.setenv("GNITZ_WORKERS", "4")

    s = _Server(binary)
    try:
        s.start()
        with gnitz.connect(s.target) as conn:
            yield conn
    finally:
        s.teardown()


@pytest.fixture
def race_server(monkeypatch):
    """
    Server spawned with the `GNITZ_INJECT_TABLE_CREATE_DELAY_MS` seam, for the
    DROP-directory-removal race regression test.

    The seam makes every worker sleep between creating a table directory and
    its partition subdirectories, deterministically widening the window in
    which a master DROP `remove_dir_all` can race a lagging worker's CREATE.
    """
    yield from _seamed_server(
        monkeypatch, {"GNITZ_INJECT_TABLE_CREATE_DELAY_MS": "50"})


@pytest.fixture
def unique_preflight_frame_server(monkeypatch):
    """Server whose CREATE UNIQUE INDEX pre-flight streams tiny (7-key) frames
    so a small table already produces multi-frame continuation trains per
    worker. Any per-worker frame count is safe: `InFlightState` grows with the
    parked depth, so the W2M ring back-pressures by bytes, not a frame count."""
    yield from _seamed_server(
        monkeypatch, {"GNITZ_UNIQUE_PREFLIGHT_KEYS_PER_FRAME": "7"})


@pytest.fixture
def reply_frame_budget_server(monkeypatch):
    """Server whose workers chunk reply trains past a tiny 16 KiB frame budget
    (debug-only seam), so modest tables already produce multi-frame seek /
    range / gather / scan reply trains per worker. Any reply size is safe: the
    master parks a full train per ring while draining another worker, but
    `InFlightState` grows to track it, so the ring back-pressures by bytes, not
    a frame count."""
    yield from _seamed_server(
        monkeypatch, {"GNITZ_REPLY_FRAME_BUDGET": str(16 * 1024)})


@pytest.fixture
def unique_preflight_fault_server(monkeypatch):
    """Server whose workers fail every CREATE UNIQUE INDEX pre-flight scan,
    for asserting the master surfaces the fault, creates no index, seeds no
    filter, and leaves the cluster healthy."""
    yield from _seamed_server(
        monkeypatch, {"GNITZ_INJECT_UNIQUE_PREFLIGHT_ERROR": "1"})


@pytest.fixture
def unique_preflight_spill_server(monkeypatch):
    """Server whose CREATE UNIQUE INDEX pre-flight spills its key sort to disk at
    a tiny 256-byte budget, so a few hundred rows per worker force many
    external-sort spill runs and a k-way merge — exercising the bounded-memory
    path end-to-end. Unlike the debug seams above, `GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES`
    is a real config knob honoured in every build, so this also bites a release
    server."""
    yield from _seamed_server(
        monkeypatch, {"GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES": "256"})


@pytest.fixture
def relay_lowspace_server(monkeypatch):
    """Server with the one-shot low-relay-space seam (GNITZ_INJECT_RELAY_SPACE_LOW)
    armed, for the barrier-only-checkpoint reclaim test. Unlike the other seam
    fixtures it yields the `_Server` handle's (sock_path, proc) so the test can
    assert the master is still alive after the low-space relay.

    Skipped on a release build: without the seam no low-space relay occurs, so
    the test would pass without exercising the reclaim path. No
    GNITZ_CHECKPOINT_BYTES override — the only checkpoint in the green run is
    the seam-induced one, keeping the view results reliable."""
    if not is_debug_build():
        pytest.skip("relay-space injection seam requires a debug build")
    monkeypatch.setenv("GNITZ_INJECT_RELAY_SPACE_LOW", "1")
    if int(os.environ.get("GNITZ_WORKERS", "1")) < 2:
        monkeypatch.setenv("GNITZ_WORKERS", "4")
    s = _Server(_server_binary())
    try:
        s.start()
        yield s.target, s.proc
    finally:
        s.teardown()


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
      2. Open ~/git/gnitz/tmp/server_debug.log — the crash backtrace is
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
            "See ~/git/gnitz/tmp/server_debug.log for the crash.",
            RuntimeWarning,
            stacklevel=2,
        )
        _srv.restart()
    yield
