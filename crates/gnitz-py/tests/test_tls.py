"""E2E tests for the TLS transport: sync + async Python clients over
``tls://…?insecure`` against the session server's always-on TLS listener.

These run inside the normal (AF_UNIX) suite — the TLS target is derived
from the session server's pinned TLS port, independent of GNITZ_TRANSPORT.
"""

import os

import pytest
import pytest_asyncio

import gnitz
from gnitz import aio

_uid_counter = 0


def _uid():
    global _uid_counter
    _uid_counter += 1
    return f"tls{os.getpid()}_{_uid_counter}"


COLS = [
    gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("val", gnitz.TypeCode.I64),
]


def _batch(rows):
    schema = gnitz.Schema(COLS)
    b = gnitz.ZSetBatch(schema)
    for row in rows:
        b.append(**row)
    return b


@pytest.fixture
def tls_target(_srv):
    return _srv.tls_target


@pytest.fixture
def tls_client(tls_target):
    with gnitz.connect(tls_target) as conn:
        yield conn


@pytest_asyncio.fixture
async def tls_aconn(tls_target):
    async with aio.connect(tls_target) as conn:
        yield conn


@pytest.fixture
def tls_table(tls_client):
    sn = "s" + _uid()
    tls_client.create_schema(sn)
    tid = tls_client.create_table(sn, "t", COLS)
    yield tid
    try:
        tls_client.drop_table(sn, "t")
        tls_client.drop_schema(sn)
    except Exception:
        pass


# ── sync client ─────────────────────────────────────────────────────────────


class TestTlsSync:
    def test_sql_roundtrip(self, tls_client):
        sn = "s" + _uid()
        tls_client.create_schema(sn)
        tls_client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", sn)
        tls_client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", sn)
        results = tls_client.execute_sql("SELECT * FROM t", sn)
        assert results[0]["type"] == "Rows"
        rows = results[0]["rows"]
        assert sorted((r.pk, r.v) for r in rows) == [(1, 10), (2, 20), (3, 30)]

    def test_multiworker_scan_with_continuation_frames(self, tls_client, tls_table):
        # Enough rows that every worker (GNITZ_WORKERS=4) contributes scan
        # frames; the client stitches the continuation train back together.
        n = 20_000
        tls_client.push(tls_table, _batch(
            [{"pk": i, "val": i * 3} for i in range(1, n + 1)]))
        result = tls_client.scan(tls_table)
        rows = {r.pk: r.val for r in result if r.weight > 0}
        assert len(rows) == n
        assert rows[1] == 3
        assert rows[n] == n * 3

    def test_unix_and_tls_see_the_same_data(self, _srv, tls_client, tls_table):
        tls_client.push(tls_table, _batch([{"pk": 7, "val": 70}]))
        with gnitz.connect(_srv.sock_path) as unix_conn:
            result = unix_conn.scan(tls_table)
            assert {r.pk for r in result} == {7}


# ── async client ────────────────────────────────────────────────────────────


class TestTlsAsync:
    @pytest.mark.asyncio
    async def test_push_and_scan(self, tls_aconn, tls_table):
        await tls_aconn.push(tls_table, _batch([{"pk": 1, "val": 100}]))
        result = await tls_aconn.scan(tls_table)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_pipelined_pushes(self, tls_aconn, tls_table):
        async with tls_aconn.pipeline() as pipe:
            for i in range(50):
                pipe.push(tls_table, _batch(
                    [{"pk": 100 * i + j, "val": j} for j in range(1, 100)]))
        assert len(pipe.results) == 50
        assert all(isinstance(lsn, int) for lsn in pipe.results)
        result = await tls_aconn.scan(tls_table)
        assert len({r.pk for r in result if r.weight > 0}) == 50 * 99


# ── error surfaces ──────────────────────────────────────────────────────────


class TestTlsErrors:
    def test_bad_ca_path_is_a_clear_error(self, _srv):
        with pytest.raises(Exception) as ei:
            gnitz.connect(f"tls://127.0.0.1:{_srv.tls_port}?ca=/nonexistent/ca.pem")
        assert "ca" in str(ei.value).lower()

    def test_unknown_param_is_rejected(self, _srv):
        with pytest.raises(Exception):
            gnitz.connect(f"tls://127.0.0.1:{_srv.tls_port}?bogus")

    def test_unreachable_port_fails_fast(self):
        import socket
        # A port that was just free and closed again: connection refused.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        with pytest.raises(Exception):
            gnitz.connect(f"tls://127.0.0.1:{port}?insecure")

    def test_restart_surfaces_clear_error_then_reconnects(self):
        # Dedicated server (not the session one — a restart wipes the
        # catalog) with its own pinned TLS port.
        from conftest import _Server, _server_binary

        srv = _Server(_server_binary())
        try:
            srv.start()
            target = srv.tls_target
            conn = gnitz.connect(target)
            sn = "s" + _uid()
            conn.create_schema(sn)
            tid = conn.create_table(sn, "t", COLS)
            conn.push(tid, _batch([{"pk": 1, "val": 1}]))

            srv.restart()
            try:
                # The stale connection fails with a clear error, not a hang.
                with pytest.raises(Exception):
                    conn.scan(tid)
                # The same target (pinned port) accepts a fresh connection.
                with gnitz.connect(target) as fresh:
                    sn2 = "s" + _uid()
                    fresh.create_schema(sn2)
                    tid2 = fresh.create_table(sn2, "t", COLS)
                    fresh.push(tid2, _batch([{"pk": 2, "val": 2}]))
                    assert len(fresh.scan(tid2)) == 1
            finally:
                conn.close()
        finally:
            srv.teardown()
