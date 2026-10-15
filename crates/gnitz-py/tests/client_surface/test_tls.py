"""The Python clients over ``tls://…?ca=``, and the access tokens a TLS
connection presents.

The TLS transport and the token checks are gnitz-core's and gnitz-server's and
are tested there, and `make e2e-tls` runs this whole suite over TLS. What is
here is what only the binding has: the async transport's readiness over a
buffered TLS stream, a target or a token refused reaching Python as a
`GnitzError` from either client, and `GNITZ_TOKEN`.
"""
import asyncio
import subprocess

import pytest

import gnitz
from gnitz import aio
from _read import bag
from _schemas import KV, kv
from _serverproc import server_binary


async def test_pipelined_pushes_over_tls(server_proc, client):
    """A gathered burst over TLS: every push lands exactly once, and a sync TLS
    connection reads back what the async one wrote."""
    tid = client.create_table("t", KV)
    batches = [[(100 * i + j, j) for j in range(1, 100)] for i in range(50)]
    async with aio.connect(server_proc.tls_target) as aconn:
        await asyncio.gather(*[aconn.push(tid, kv(*b)) for b in batches])
    with gnitz.connect(server_proc.tls_target) as conn:
        assert bag(conn.scan(tid, KV)) == {row: 1 for b in batches for row in b}


def test_a_bad_ca_path_is_refused():
    """The CA file is read before any connect, so the address need not answer."""
    with pytest.raises(gnitz.GnitzError, match="ca.pem"):
        gnitz.connect("tls://127.0.0.1:1?ca=/nonexistent/ca.pem")


async def test_a_token_is_presented_by_either_client(own_server, tmp_path, monkeypatch):
    """A server booted with `--auth-tokens` refuses a TLS connection that holds
    no token, from either client, and admits one presenting it from a token file
    or from `GNITZ_TOKEN`."""
    token_file = tmp_path / "tokens"
    minted = subprocess.run([server_binary(), "--new-token", str(token_file), "app"],
                            capture_output=True, text=True, check=True)
    token = minted.stdout.strip()
    own_server.extra_args = [f"--auth-tokens={token_file}"]
    own_server.start()
    bare = own_server.tls_target
    held = tmp_path / "token"
    held.write_text(token + "\n")

    with pytest.raises(gnitz.GnitzError, match="requires an access token"):
        gnitz.connect(bare)
    with pytest.raises(gnitz.GnitzError, match="requires an access token"):
        await aio.connect(bare)

    with gnitz.connect(f"{bare}&token_file={held}") as conn:
        conn.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY)")
    async with aio.connect(f"{bare}&token_file={held}") as conn:
        await conn.resolve_table("t")

    monkeypatch.setenv("GNITZ_TOKEN", token)
    with gnitz.connect(bare) as conn:
        conn.resolve_table("t")
    monkeypatch.setenv("GNITZ_TOKEN", "gnitz_" + "0" * 64)
    with pytest.raises(gnitz.GnitzError, match="does not recognise"):
        gnitz.connect(bare)
