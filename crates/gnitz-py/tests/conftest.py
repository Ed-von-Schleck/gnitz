import contextlib
import itertools
import os
import shutil
import warnings

import pytest
import gnitz
from _paths import REPO_ROOT
from _uid import uid
from _serverproc import ServerProc

_TMP_DIR = str(REPO_ROOT / "tmp")
os.makedirs(_TMP_DIR, exist_ok=True)
_LOG_PATH = os.path.join(_TMP_DIR, "server_debug.log")

# Anchor pytest's basetemp inside the repo rather than the default /tmp. Two
# reasons, both about the SAL every server fallocates (128 MiB per test server,
# see `_serverproc.server_env`): /tmp is tmpfs here, where that fallocate
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
def own_server(tmp_path, _sock_path):
    """A `ServerProc` the test starts and drives itself, stopped at teardown
    however the test left it — what a test uses to configure a server through
    its environment, or to crash one and reboot it on the same data dir. Its
    catalog is the test's alone, so the test works in the boot-time `public`
    schema and tears nothing down.

    The data dir is where the gigabytes go, and `tmp_path` reclaims it as soon
    as a passing test finishes (`tmp_path_retention_policy = "failed"` in
    pyproject.toml); a failing test keeps it, and the server's log beside it,
    for post-mortem.
    """
    proc = ServerProc(str(tmp_path / "data"), _sock_path())
    try:
        yield proc
    finally:
        proc.stop()


def _boot_session_server(srv, tmp_path_factory):
    """(Re)start the session server on a fresh data dir, and so a clean catalog.
    The old one goes now rather than at pytest's end-of-session reclaim: each
    holds a fallocated SAL."""
    srv.stop()
    if srv.data_dir:
        shutil.rmtree(os.path.dirname(srv.data_dir), ignore_errors=True)
    srv.data_dir = str(tmp_path_factory.mktemp("d") / "data")
    srv.start()


@pytest.fixture(scope="session")
def _srv(tmp_path_factory, _sock_path):
    """The session server every `client` shares, which no test may kill. Its
    output goes to `tmp/server_debug.log` and its workers' logs are copied to
    `tmp/last_worker_N.log` at the end, so both outlive the run."""
    srv = ServerProc(None, _sock_path(), log_path=_LOG_PATH)
    _boot_session_server(srv, tmp_path_factory)
    yield srv
    for i in range(srv.workers):
        src = os.path.join(srv.data_dir, f"worker_{i}.log")
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(_TMP_DIR, f"last_worker_{i}.log"))
    srv.stop()
    shutil.rmtree(os.path.dirname(srv.data_dir), ignore_errors=True)


def _session_target(srv, tmp_path_factory, user):
    """Connect target of the session server, restarting it on a fresh catalog if
    it has died — a prior test triggered a panic that escaped guard_panic, or an
    OOM/signal killed the process.

    Without the restart, one server death cascades into hundreds of ERROR
    entries for unrelated tests. With it, only the test that caused the death
    fails for that reason.

    Triage when the restart warning appears:
      1. Find the first FAILED or ERROR *before* the warning in pytest's
         chronological output (not alphabetical file order).
      2. Open <repo>/tmp/server_debug.log — the crash backtrace is before the
         "restarted" separator line.
      3. Run that one test in isolation with a fresh server to reproduce.
    """
    if srv.proc.poll() is not None:
        warnings.warn(
            f"[gnitz] server died before '{user}' — restarting with a fresh catalog. "
            "The root cause is in a test that ran before this one. "
            "See <repo>/tmp/server_debug.log for the crash.",
            RuntimeWarning,
            stacklevel=2,
        )
        with open(_LOG_PATH, "a") as f:
            f.write("\n\n--- server restarted (crash above this line) ---\n\n")
        _boot_session_server(srv, tmp_path_factory)
    return srv.target


@pytest.fixture
def server(_srv, tmp_path_factory, request):
    """Connect target of the session server (socket path, or the TLS address
    under GNITZ_TRANSPORT=tls)."""
    return _session_target(_srv, tmp_path_factory, request.node.nodeid)


@contextlib.contextmanager
def _in_fresh_schema(target, prefix):
    """A connection bound to a schema of its own, dropped whole on exit.

    `drop_schema` is `DROP SCHEMA ... CASCADE`: one atomic DDL bundle that
    retracts every view, then every table (each cascading its own indexes),
    then the schema row. So a test never names its own objects to tear them
    down, and dropping them by hand only adds round trips that must be
    redundant or fail.

    The drop is not wrapped: a schema that refuses to drop is a finding, and
    swallowing it leaks the objects into the shared session catalog for every
    later test.
    """
    name = prefix + uid()
    with gnitz.connect(target, schema=name) as conn:
        conn.create_schema(name)
        yield conn
        conn.drop_schema(name)


@pytest.fixture
def client(server):
    """Per-test connection to the session server, in a fresh schema of its own
    (`client.schema`). A second connection a test opens to `server` reaches the
    same relations with `gnitz.connect(server, schema=client.schema)`."""
    with _in_fresh_schema(server, "s") as conn:
        yield conn


@pytest.fixture(scope="module")
def module_client(_srv, tmp_path_factory, request):
    """A connection and schema shared by one module — for fixtures whose data no
    test changes, built once instead of once per case."""
    with _in_fresh_schema(_session_target(_srv, tmp_path_factory, request.node.nodeid), "m") as conn:
        yield conn


@pytest.fixture
def priced(client):
    """`t(id, cat, price DECIMAL(10,2), qty NUMERIC(8,3))` — two scales, holding
    one row of each spelling a literal can take: a plain decimal, a string, a
    negative beside a NULL, an integer widened to the scale, and one whose
    fraction is longer than the scale, so it must round and its product needs
    five decimal places."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL, "
        "price DECIMAL(10, 2) NOT NULL, qty NUMERIC(8, 3)); "
        "INSERT INTO t VALUES (1, 1, 12.50, 3), (2, 1, 0.10, 3.5), "
        "(3, 2, '7.125', '0.001'), (4, 2, -0.5, NULL), (5, 3, 1.005, 2)")


# ── mirror fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def mirror_dir(tmp_path):
    """A private copy directory for one mirroring client, reclaimed with the
    test."""
    return str(tmp_path / "mirror")


@pytest.fixture
def mirror_on(tmp_path):
    """Factory for a client with a copy directory attached — `mirror_on(base_dir,
    target, schema)` — closed at teardown however the test left it.

    Closing is not tidiness: an unclosed client skips the exit checkpoint and
    keeps its copy directory locked for the life of the interpreter. The close
    raises if that checkpoint fails, so it must run while the directory still
    exists: depending on `tmp_path` tears this fixture down before the test's
    temporary directory is reclaimed.
    """
    with contextlib.ExitStack() as stack:
        def make(base_dir, target, schema="public"):
            client = stack.enter_context(gnitz.connect(target, schema=schema))
            client.mirror_at(base_dir)
            return client

        yield make


@pytest.fixture
def mirror(mirror_on, server, mirror_dir, client):
    """A mirroring client on the session server at `mirror_dir`, in `client`'s
    schema."""
    return mirror_on(mirror_dir, server, client.schema)
