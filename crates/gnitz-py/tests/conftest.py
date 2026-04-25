import os
import shutil
import subprocess
import tempfile
import time
import warnings

import pytest
import gnitz

_TMP_DIR = os.path.expanduser("~/git/gnitz/tmp")
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
        self.proc = None
        self._stderr_f = None
        self._data_dir: str | None = None

    # ── public ────────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._spawn()

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
        cmd = [self._binary, data_dir, self.sock_path]
        if w := os.environ.get("GNITZ_WORKERS"):
            cmd += [f"--workers={w}"]
        if ll := os.environ.get("GNITZ_LOG_LEVEL"):
            cmd += [f"--log-level={ll}"]
        # Append so a restart does not discard the log that contains the crash.
        self._stderr_f = open(_LOG_PATH, "a")
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=self._stderr_f)
        for _ in range(100):
            if os.path.exists(self.sock_path):
                break
            time.sleep(0.1)
        else:
            self.proc.kill()
            self.proc.communicate()
            self._stderr_f.close()
            self._stderr_f = None
            raise RuntimeError("Server did not start within 10 s")


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def _srv():
    """Session-scoped mutable server handle shared by all per-class and per-test fixtures."""
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../gnitz-server")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    s = _Server(binary)
    s.start()
    yield s
    s.teardown()


@pytest.fixture(scope="session")
def server(_srv):
    """
    Socket path of the session server.  Stable across restarts — the socket
    lives in a fixed base directory so this value never changes.
    Preserved for backward compatibility with test files that accept `server`.
    """
    return _srv.sock_path


@pytest.fixture
def client(_srv):
    """Per-test connection.  Always resolves through _srv so it follows restarts."""
    with gnitz.connect(_srv.sock_path) as conn:
        yield conn


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
