"""Access tokens through the Python clients and the shipped binary's
``--new-token``, against a server booted with ``--auth-tokens``."""
import os
import stat
import subprocess

import pytest

import gnitz
from gnitz import aio
from _read import bag
from _serverproc import server_binary


def _mint(token_file, name):
    out = subprocess.run([server_binary(), "--new-token", str(token_file), name],
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


@pytest.fixture
def token_server(own_server, tmp_path):
    """A server requiring a token, its token file, and the token minted before the boot."""
    token_file = tmp_path / "tokens"
    token = _mint(token_file, "app")
    own_server.extra_args = [f"--auth-tokens={token_file}"]
    own_server.start()
    return own_server, token_file, token


def _token_target(srv, tmp_path, token, name="token"):
    path = tmp_path / name
    path.write_text(token + "\n")
    return f"{srv.tls_target}&token_file={path}"


def test_new_token_prints_a_token_once_and_keeps_its_digest(tmp_path):
    token_file = tmp_path / "tokens"
    token = _mint(token_file, "app")
    assert token.startswith("gnitz_") and len(token) == 70
    assert stat.S_IMODE(os.stat(token_file).st_mode) == 0o600
    text = token_file.read_text()
    assert text.startswith("app ") and token[6:] not in text
    again = subprocess.run([server_binary(), "--new-token", str(token_file), "app"],
                           capture_output=True, text=True)
    assert again.returncode == 1 and "already held" in again.stderr
    assert token_file.read_text() == text


def test_a_token_is_required_and_checked_at_connect(token_server, tmp_path):
    srv, _, token = token_server
    with pytest.raises(gnitz.GnitzError, match="requires an access token"):
        gnitz.connect(srv.tls_target)
    wrong = token[:-1] + ("1" if token.endswith("0") else "0")
    with pytest.raises(gnitz.GnitzError, match="does not recognise"):
        gnitz.connect(_token_target(srv, tmp_path, wrong, "wrong"))

    with gnitz.connect(_token_target(srv, tmp_path, token)) as conn:
        conn.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        conn.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)")
    with gnitz.connect(srv.sock_path) as conn:
        tid, schema = conn.resolve_table("t")
        assert bag(conn.scan(tid, schema)) == {(1, 10): 1, (2, 20): 1}, "the AF_UNIX socket asks for none"


def test_gnitz_token_reaches_the_client(token_server, monkeypatch):
    srv, _, token = token_server
    monkeypatch.setenv("GNITZ_TOKEN", token)
    with gnitz.connect(srv.tls_target) as conn:
        conn.execute_sql("CREATE TABLE e (id BIGINT PRIMARY KEY)")
    monkeypatch.setenv("GNITZ_TOKEN", "gnitz_" + "0" * 64)
    with pytest.raises(gnitz.GnitzError, match="does not recognise"):
        gnitz.connect(srv.tls_target)


@pytest.mark.asyncio
async def test_the_async_client_presents_its_token(token_server, tmp_path):
    srv, _, token = token_server
    async with aio.connect(_token_target(srv, tmp_path, token)) as conn:
        await conn.execute_sql("CREATE TABLE a (id BIGINT PRIMARY KEY)")
    with pytest.raises(gnitz.GnitzError, match="requires an access token"):
        await aio.connect(srv.tls_target)


def test_sighup_reloads_the_token_file_and_leaves_the_workers(token_server, tmp_path):
    srv, token_file, token = token_server
    second = _mint(token_file, "second")
    with pytest.raises(gnitz.GnitzError, match="does not recognise"):
        gnitz.connect(_token_target(srv, tmp_path, second, "second"))

    assert "SIGHUP: reloaded" in srv.sighup()
    with gnitz.connect(_token_target(srv, tmp_path, second, "second")) as conn:
        conn.execute_sql("CREATE TABLE r (id BIGINT PRIMARY KEY, v BIGINT)")
        conn.execute_sql("INSERT INTO r VALUES " + ", ".join(f"({i}, {i})" for i in range(64)))
        tid, schema = conn.resolve_table("r")
        assert bag(conn.scan(tid, schema)) == {(i, i): 1 for i in range(64)}, "every worker still answers"
    assert srv.proc.poll() is None
    with gnitz.connect(_token_target(srv, tmp_path, token)):
        pass
