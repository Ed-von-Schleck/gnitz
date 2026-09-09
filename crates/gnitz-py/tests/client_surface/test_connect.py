"""Opening and closing a sync connection: the shapes `connect` supports, and
what a closed client refuses."""
import pytest

import gnitz
from _uid import uid as _uid


def test_connect_shapes_close_and_refuse(server):
    """`GnitzClient` works as a context manager and as a plain object, close is
    idempotent, and a closed client refuses every verb rather than reconnecting
    behind the caller's back. A fresh connection to the same target still
    works — closing one is not closing the transport."""
    sn = "s" + _uid()
    with gnitz.GnitzClient(server) as ctx:
        assert ctx.create_schema(sn) > 0
        ctx.drop_schema(sn)

    conn = gnitz.connect(server)
    assert len(conn.scan(gnitz.SCHEMA_TAB)) > 0
    conn.close()
    conn.close()                                  # idempotent
    with pytest.raises(gnitz.GnitzError, match="closed"):
        conn.scan(gnitz.SCHEMA_TAB)

    with gnitz.connect(server) as fresh:
        assert len(fresh.scan(gnitz.SCHEMA_TAB)) > 0
