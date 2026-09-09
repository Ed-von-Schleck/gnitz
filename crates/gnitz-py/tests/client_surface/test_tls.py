"""E2E tests for the TLS transport: sync + async Python clients over
``tls://…?insecure`` against the session server's always-on TLS listener.

These run inside the normal (AF_UNIX) suite — the TLS target is derived
from the session server's pinned TLS port, independent of GNITZ_TRANSPORT.
"""

import asyncio

import pytest
import pytest_asyncio

import gnitz
from gnitz import aio
from _uid import uid as _uid


COLS = [
    gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("val", gnitz.TypeCode.I64),
]
SCHEMA = gnitz.Schema(COLS)


def _batch(rows):
    return gnitz.ZSetBatch(SCHEMA).extend(rows)


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
def tls_schema(tls_client):
    """A schema created over the TLS connection, dropped whole at teardown.

    `conftest.schema_name` hangs off `client`, and the point here is that the
    DDL crosses TLS — but the drop is unwrapped for the same reason it is there:
    a schema that refuses to drop is a finding, not something to swallow.
    """
    sn = "s" + _uid()
    tls_client.create_schema(sn)
    yield sn
    tls_client.drop_schema(sn)


@pytest.fixture
def tls_table(tls_client, tls_schema):
    return tls_client.create_table(tls_schema, "t", COLS)


# ── sync client ─────────────────────────────────────────────────────────────


class TestTlsSync:
    def test_sql_roundtrip(self, tls_client, tls_schema):
        tls_client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", tls_schema)
        tls_client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", tls_schema)
        results = tls_client.execute_sql("SELECT * FROM t", tls_schema)
        assert results[0]["type"] == "Rows"
        assert sorted((r.pk, r.v) for r in results[0]["rows"]) == [(1, 10), (2, 20), (3, 30)]

    def test_a_multiworker_reply_is_stitched_back_together(self, tls_client, tls_table):
        """Every worker (GNITZ_WORKERS=4) contributes scan frames and the client
        reassembles them into one result.

        Compared row by row with weights, not as a `{pk: val}` dict: the failure
        a mis-stitched reply train actually has is a *repeated* frame, which a
        dict keyed by PK silently absorbs.
        """
        n = 2_000
        tls_client.push(tls_table, _batch([{"pk": i, "val": i * 3} for i in range(1, n + 1)]))
        result = tls_client.scan(tls_table)
        assert sorted((r.pk, r.val, r.weight) for r in result) == [
            (i, i * 3, 1) for i in range(1, n + 1)]

    def test_unix_and_tls_see_the_same_data(self, _srv, tls_client, tls_table):
        tls_client.push(tls_table, _batch([{"pk": 7, "val": 70}]))
        with gnitz.connect(_srv.sock_path) as unix_conn:
            assert [(r.pk, r.val, r.weight) for r in unix_conn.scan(tls_table)] == [(7, 70, 1)]


# ── async client ────────────────────────────────────────────────────────────


class TestTlsAsync:
    @pytest.mark.asyncio
    async def test_pipelined_pushes(self, tls_aconn, tls_table):
        """A gathered burst over TLS: every push lands, exactly once, at weight 1."""
        rows = [{"pk": 100 * i + j, "val": j} for i in range(50) for j in range(1, 100)]
        await asyncio.gather(*[
            tls_aconn.push(tls_table, _batch(rows[i * 99:(i + 1) * 99])) for i in range(50)])
        result = await tls_aconn.scan(tls_table)
        assert sorted((r.pk, r.val, r.weight) for r in result) == sorted(
            (r["pk"], r["val"], 1) for r in rows)


# ── error surfaces ──────────────────────────────────────────────────────────


class TestTlsErrors:
    def test_a_bad_target_is_refused_rather_than_hung(self, _srv):
        """Each rejection names its own cause, and none of them blocks: a
        connect that hung would fail this by timing out the test, not by
        returning something wrong."""
        port = _srv.tls_port
        for target, needle in [
            (f"tls://127.0.0.1:{port}?ca=/nonexistent/ca.pem", "ca.pem"),
            (f"tls://127.0.0.1:{port}?bogus", "bogus"),
        ]:
            with pytest.raises(Exception) as ei:
                gnitz.connect(target)
            assert needle in str(ei.value), f"{target} said: {ei.value}"

    def test_a_closed_port_is_refused(self):
        import socket
        # A port that was just free and closed again: connection refused.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        with pytest.raises(Exception):
            gnitz.connect(f"tls://127.0.0.1:{port}?insecure")

    def test_restart_surfaces_clear_error_then_reconnects(self, restartable_server):
        # Dedicated server (not the session one — a restart wipes the
        # catalog) with its own pinned TLS port.
        srv = restartable_server
        target = srv.tls_target
        with gnitz.connect(target) as conn:
            sn = "s" + _uid()
            conn.create_schema(sn)
            tid = conn.create_table(sn, "t", COLS)
            conn.push(tid, _batch([{"pk": 1, "val": 1}]))

            srv.restart()
            # The stale connection fails with a clear error, not a hang.
            with pytest.raises(Exception):
                conn.scan(tid)
            # The same target (pinned port) accepts a fresh connection.
            with gnitz.connect(target) as fresh:
                sn2 = "s" + _uid()
                fresh.create_schema(sn2)
                tid2 = fresh.create_table(sn2, "t", COLS)
                fresh.push(tid2, _batch([{"pk": 2, "val": 2}]))
                assert [(r.pk, r.val, r.weight) for r in fresh.scan(tid2)] == [(2, 2, 1)]
