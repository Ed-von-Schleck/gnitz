"""A server run from Python: `gnitz.local_server`, the `gnitz.testing`
fixtures over it, and a connection that finds its target in the environment."""

import importlib.metadata
import os
import shutil
import signal
import subprocess
import sys
import sysconfig
import textwrap
import time
from pathlib import Path

import gnitz
import pytest

from _read import bag, result_rows, rows
from _serverproc import server_binary

pytest_plugins = ["pytester"]


@pytest.fixture
def server_bin(monkeypatch):
    """The suite's server, as the one `gnitz.find_server` finds."""
    binary = server_binary()
    monkeypatch.setenv("GNITZ_SERVER_BIN", binary)
    return binary


def test_a_throwaway_server_serves_until_it_stops_and_takes_its_data_along(server_bin):
    with gnitz.local_server() as server:
        assert server.running
        scratch = Path(server.target).parent
        assert server.data_dir == scratch / "data"
        with gnitz.connect(server.target) as conn:
            conn.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
            conn.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)")
            assert bag(rows(conn, "SELECT * FROM t")) == {(1, 10): 1, (2, 20): 1}
    assert not server.running
    assert not scratch.exists()


def test_a_kept_server_leaves_its_rows_to_the_next(server_bin, tmp_path):
    data = tmp_path / "db"
    with gnitz.local_server(data) as server:
        assert server.data_dir == data
        with gnitz.connect(server.target) as conn:
            conn.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
            conn.execute_sql("INSERT INTO t VALUES (7, 70)")
    with gnitz.local_server(data) as server, gnitz.connect(server.target) as conn:
        assert bag(rows(conn, "SELECT * FROM t")) == {(7, 70): 1}


def test_a_server_that_does_not_serve_is_reported_with_its_log(server_bin, tmp_path):
    occupied = tmp_path / "not-a-directory"
    occupied.write_text("")
    with pytest.raises(RuntimeError, match="before it served"):
        gnitz.local_server(occupied)


def test_a_missing_server_names_how_to_install_one(monkeypatch, tmp_path):
    monkeypatch.setenv("GNITZ_SERVER_BIN", str(tmp_path / "no-such-server"))
    with pytest.raises(FileNotFoundError, match="GNITZ_SERVER_BIN names"):
        gnitz.find_server()
    monkeypatch.delenv("GNITZ_SERVER_BIN")
    monkeypatch.setenv("PATH", str(tmp_path))
    if (Path(sysconfig.get_path("scripts")) / "gnitz-server").exists():
        pytest.skip("this environment has gnitz-server installed")
    with pytest.raises(FileNotFoundError, match=r'pip install "gnitz\[server\]"'):
        gnitz.find_server()


def test_a_server_stops_when_the_process_that_ran_it_is_killed(server_bin, tmp_path):
    # The child runs a server on a kept data directory, reports its socket, and
    # is killed without stopping it: the server stops all the same.
    script = textwrap.dedent(
        """
        import sys, time, gnitz
        server = gnitz.local_server(sys.argv[1])
        print(server.target, flush=True)
        time.sleep(600)
        """
    )
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(tmp_path / "db")],
        stdout=subprocess.PIPE,
        text=True,
        env={**os.environ, "GNITZ_SERVER_BIN": server_bin},
    )
    target = child.stdout.readline().strip()
    # What the killed process could not remove.
    scratch = Path(target).parent
    with gnitz.connect(target) as conn:
        conn.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY)")
    child.send_signal(signal.SIGKILL)
    child.wait()
    deadline = time.monotonic() + 60
    while True:
        try:
            gnitz.connect(target).close()
        except gnitz.GnitzConnectionError:
            break
        assert time.monotonic() < deadline, "the server outlived the process that ran it"
        time.sleep(0.01)
    shutil.rmtree(scratch)
    # It stopped as a SIGTERM stops it, and what it acknowledged is kept.
    with gnitz.local_server(tmp_path / "db") as server, gnitz.connect(server.target) as conn:
        assert bag(rows(conn, "SELECT * FROM t")) == {}


def test_a_server_nothing_stopped_stops_as_the_interpreter_exits(server_bin):
    script = "import gnitz; print(gnitz.local_server().target)"
    out = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, check=True,
        env={**os.environ, "GNITZ_SERVER_BIN": server_bin},
    )
    target = out.stdout.strip()
    assert not Path(target).parent.exists(), "the exit left the server's scratch directory behind"
    with pytest.raises(gnitz.GnitzConnectionError):
        gnitz.connect(target)


def test_connect_takes_its_target_from_the_environment(server, monkeypatch):
    monkeypatch.setenv("GNITZ_TARGET", server)
    with gnitz.connect() as conn:
        assert bag(rows(conn, "SELECT 1 AS one")) == {(1,): 1}
    monkeypatch.delenv("GNITZ_TARGET")
    with pytest.raises(ValueError, match="GNITZ_TARGET"):
        gnitz.connect()


async def test_an_async_connect_takes_its_target_from_the_environment(server, monkeypatch):
    import gnitz.aio

    monkeypatch.setenv("GNITZ_TARGET", server)
    async with gnitz.aio.connect() as conn:
        assert bag(result_rows(await conn.execute_sql("SELECT 1 AS one"))) == {(1,): 1}


def test_the_server_extra_pins_this_release():
    version = importlib.metadata.version("gnitz")
    requires = importlib.metadata.requires("gnitz") or []
    pinned = f"gnitz-server=={version};extra=='server'"
    assert pinned in [r.replace(" ", "").replace('"', "'") for r in requires], requires


def test_the_pytest_plugin_gives_each_test_a_schema_of_its_own(server_bin, pytester):
    pytester.makeconftest('pytest_plugins = ["gnitz.testing"]')
    pytester.makepyfile(
        """
        def test_one(gnitz_client, gnitz_target):
            gnitz_client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY)")
            gnitz_client.execute_sql("INSERT INTO t VALUES (1)")
            assert gnitz_target.endswith("gnitz.sock")

        def test_two(gnitz_client):
            # The same name, in a schema of this test's own.
            gnitz_client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY)")
            [result] = gnitz_client.execute_sql("SELECT * FROM t")
            assert list(result["rows"]) == []
        """
    )
    result = pytester.runpytest_subprocess("-p", "no:cacheprovider")
    result.assert_outcomes(passed=2)
