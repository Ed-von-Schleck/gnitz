"""The Python clients over ``tls://…?insecure`` against the session server's
always-on TLS listener, whatever GNITZ_TRANSPORT the suite runs under.

The TLS transport itself is gnitz-core's and is tested there, and `make e2e-tls`
runs this whole suite over it. What is here is what only the binding has: the
async transport's readiness over a buffered TLS stream, and a target the parser
refuses reaching Python as a `GnitzError`.
"""
import asyncio

import pytest

import gnitz
from gnitz import aio
from _read import bag


COLS = [
    gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("val", gnitz.TypeCode.I64),
]
SCHEMA = gnitz.Schema(COLS)


@pytest.mark.asyncio
async def test_pipelined_pushes_over_tls(_srv, client, schema_name):
    """A gathered burst over TLS: every push lands exactly once, and a sync TLS
    connection reads back what the async one wrote."""
    tid = client.create_table(schema_name, "t", COLS)
    rows = [{"pk": 100 * i + j, "val": j} for i in range(50) for j in range(1, 100)]
    async with aio.connect(_srv.tls_target) as aconn:
        await asyncio.gather(*[
            aconn.push(tid, gnitz.ZSetBatch(SCHEMA).extend(rows[i * 99:(i + 1) * 99]))
            for i in range(50)])
    with gnitz.connect(_srv.tls_target) as conn:
        assert bag(conn.scan(tid)) == {(r["pk"], r["val"]): 1 for r in rows}


def test_a_bad_ca_path_is_refused(_srv):
    target = f"tls://127.0.0.1:{_srv.tls_port}?ca=/nonexistent/ca.pem"
    with pytest.raises(gnitz.GnitzError, match="ca.pem"):
        gnitz.connect(target)
