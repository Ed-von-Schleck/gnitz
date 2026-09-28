import contextlib
import itertools
import os
import shutil
import warnings

import pytest
import gnitz
from _paths import REPO_ROOT
from _uid import uid
from _serverproc import NUM_WORKERS, ServerProc

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
    """A shared server run through a `ServerProc`, with a TLS listener. Its
    socket path, TLS port and CA path stay fixed across restarts; every boot gets
    a fresh data dir from `tmp_path_factory`, and so a clean catalog."""

    def __init__(self, sock_path: str, tmp_path_factory, *, workers=None, extra_env=None):
        self._factory = tmp_path_factory
        self.sock_path = sock_path
        # 0 until the first boot publishes the port it bound.
        self.tls_port = 0
        self.tls_ca_path = sock_path + ".ca.pem"
        self._workers = workers
        self._extra_env = extra_env
        self._proc: ServerProc | None = None
        self._data_dir: str | None = None

    # ── public ────────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._spawn()

    @property
    def proc(self):
        """The running server's `Popen`, or None before a boot succeeded."""
        return self._proc.proc if self._proc else None

    @property
    def tls_target(self) -> str:
        """TLS connect string for the always-on TLS listener."""
        return f"tls://127.0.0.1:{self.tls_port}?ca={self.tls_ca_path}"

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
        if self._proc:
            self._proc.stop()
        with open(_LOG_PATH, "a") as f:
            f.write("\n\n--- server restarted by _server_guard (crash above this line) ---\n\n")
        self._spawn()

    def teardown(self) -> None:
        """Copy worker logs to _TMP_DIR, then kill and clean up."""
        if self._proc:
            data = self._proc.data_dir
            for i in range(self._proc.workers):
                src = os.path.join(data, f"worker_{i}.log")
                if os.path.exists(src):
                    shutil.copy2(src, os.path.join(_TMP_DIR, f"last_worker_{i}.log"))
            self._proc.stop()
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
        self._proc = None
        self._data_dir = str(self._factory.mktemp("d"))
        data_dir = os.path.join(self._data_dir, "data")
        self._proc = ServerProc(
            data_dir, self.sock_path,
            workers=self._workers or int(os.environ.get("GNITZ_WORKERS", NUM_WORKERS)),
            extra_env=self._extra_env, log_path=_LOG_PATH,
            args=[f"--tls-listen=127.0.0.1:{self.tls_port}"]).start()
        with open(os.path.join(data_dir, "tls_endpoint")) as f:
            self.tls_port = int(f.read().strip().rsplit(":", 1)[1])
        tmp_ca = self.tls_ca_path + ".tmp"
        shutil.copyfile(os.path.join(data_dir, "tls_dev_cert.pem"), tmp_ca)
        os.replace(tmp_ca, self.tls_ca_path)


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
    and cursor-expiry cases."""
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


@pytest.fixture(scope="module")
def module_schema(server):
    """`(conn, schema)` shared by one module, dropped whole at its end — for
    fixtures whose data no test changes, built once instead of once per case.

    Its own connection off the session server, since `client` is per-test.
    """
    with gnitz.connect(server) as conn:
        sn = "m" + uid()
        conn.create_schema(sn)
        yield conn, sn
        conn.drop_schema(sn)


# ── mirror fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def mirror_dir(tmp_path):
    """A private copy directory for one mirroring client, reclaimed with the
    test."""
    return str(tmp_path / "mirror")


@pytest.fixture
def mirror_on(tmp_path):
    """Factory for a client with a copy directory attached — `mirror_on(base_dir,
    target)` — closed at teardown however the test left it.

    Closing is not tidiness: an unclosed client skips the exit checkpoint and
    keeps its copy directory locked for the life of the interpreter. The close
    raises if that checkpoint fails, so it must run while the directory still
    exists: depending on `tmp_path` tears this fixture down before the test's
    temporary directory is reclaimed.
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
def dedicated_server(tmp_path_factory, _sock_path):
    """`dedicated_server(env)` starts a `_Server` torn down with the test, with
    `env` set on the server process alone. It runs `GNITZ_WORKERS` workers, read
    from `env` or else this process, or 4 when that is below 2."""
    started = []

    def make(env: dict[str, str]):
        workers = int(env.get("GNITZ_WORKERS", os.environ.get("GNITZ_WORKERS", NUM_WORKERS)))
        s = _Server(_sock_path(), tmp_path_factory,
                    workers=workers if workers >= 2 else 4, extra_env=env)
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
    `(target, proc)`, for a test that only reads and writes. The server and its
    catalog die with the test, so the test works in the boot-time `public`
    schema and tears nothing down."""
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
def tick_emit_fault_server(seamed_server):
    """Server whose first *replied* master-side tick emission fails, reproducing
    what a full SAL does to a tick. The CREATE's own view-seeding drain writes a
    silent tick group and so cannot spend it, and it is one-shot, so the
    follow-up read observes the re-queued tid ticking and the view converging."""
    return seamed_server({"GNITZ_INJECT_TICK_EMIT_ERROR": "1"})


@pytest.fixture
def tiny_sal_server(dedicated_server):
    """SAL pinned to its 16 MiB floor with the checkpoint threshold at 15 MiB,
    high enough that the test's pushes stop short of it and only its reads cross
    it — so the watchdog, which fires at the threshold, is the ONLY thing that
    can reclaim. Both are real config knobs honoured in every build."""
    srv = dedicated_server({
        "GNITZ_SAL_BYTES": str(16 * 1024 * 1024),
        "GNITZ_CHECKPOINT_BYTES": str(15 * 1024 * 1024),
    })
    return srv.target, srv.proc


@pytest.fixture
def disposable_server(dedicated_server):
    """A server the test alone owns and may kill, as `(target, proc)`."""
    srv = dedicated_server({})
    return srv.target, srv.proc


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
