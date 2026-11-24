"""A write too large for one frame: the client cuts it into several, and the
server commits them as the one write the statement or push is — every row, or
none of it.

`GNITZ_WRITE_FRAME_BYTES` makes the cut at 64 KiB, so each write here spans
several frames. Every read is a Z-set bag of the table and of a passthrough
view over it: the table clamps an upsert's weight to 1 and the view does not,
so a row emitted twice shows only in the view.
"""

import pytest
import gnitz
from _read import bag, rows

_N = 6_000


@pytest.fixture
def spanning(client, peer, monkeypatch):
    """A connection that cuts writes at 64 KiB, beside `client`, over table
    `t (id, v, w, x)` and the passthrough view `pv` of it."""
    client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT, w BIGINT, x BIGINT)")
    client.execute_sql("CREATE VIEW pv AS SELECT * FROM t")
    monkeypatch.setenv("GNITZ_WRITE_FRAME_BYTES", str(64 << 10))
    return peer()


def _state(client):
    """The bags of `t` and `pv`, which must agree."""
    t, v = bag(rows(client, "SELECT * FROM t")), bag(rows(client, "SELECT * FROM pv"))
    assert t == v
    return t


def _rows(lo, hi, v=0):
    return [{"id": i, "v": i + v, "w": i, "x": i} for i in range(lo, hi)]


def test_a_statement_over_several_frames_commits_whole(client, spanning):
    """A push, then an UPDATE and a DELETE of every row, each far past one
    frame: each commits every row exactly once."""
    tid, schema = spanning.resolve_table("t")
    spanning.push(tid, gnitz.ZSetBatch(schema).extend(_rows(0, _N)), "error")
    assert _state(client) == {(i, i, i, i): 1 for i in range(_N)}
    spanning.execute_sql("UPDATE t SET v = v + 1")
    assert _state(client) == {(i, i + 1, i, i): 1 for i in range(_N)}
    spanning.execute_sql("DELETE FROM t WHERE w >= 0")
    assert _state(client) == {}


def test_a_write_refused_by_its_last_frame_commits_nothing(client, spanning):
    """Error mode over the whole write: a key of the first frame repeated in
    the last, or one already committed, refuses it whole; so does an INSERT
    whose last row collides. Nothing of any of them commits."""
    tid, schema = spanning.resolve_table("t")
    spanning.push(tid, gnitz.ZSetBatch(schema).extend(_rows(0, 1)), "error")
    committed = _state(client)
    for repeat, why in [(_N, "multiple rows"), (0, "already exists")]:
        batch = gnitz.ZSetBatch(schema).extend(_rows(_N, 2 * _N) + _rows(repeat, repeat + 1, v=7))
        with pytest.raises(gnitz.GnitzIntegrityError, match=why):
            spanning.push(tid, batch, "error")
        assert _state(client) == committed
    with pytest.raises(gnitz.GnitzIntegrityError):
        with spanning.transaction() as txn:
            txn.push(tid, gnitz.ZSetBatch(schema).extend(_rows(_N, 2 * _N)), "error")
            txn.push(tid, gnitz.ZSetBatch(schema).extend(_rows(0, 1, v=7)), "error")
    assert _state(client) == committed


def test_a_stream_push_refused_by_its_last_frame_names_its_row(client, spanning):
    """A stream's append-only rule over the whole write: a retraction in the
    last frame refuses every frame, and is named by its row in the write."""
    client.execute_sql("CREATE TABLE s (id BIGINT PRIMARY KEY, v BIGINT, w BIGINT, x BIGINT) WITH (stream = true)")
    client.execute_sql("CREATE VIEW sv AS SELECT * FROM s")
    tid, schema = spanning.resolve_table("s")
    batch = gnitz.ZSetBatch(schema).extend(_rows(0, _N) + [{"id": 1, "v": 1, "w": 1, "x": 1, "_weight": -1}])
    with pytest.raises(gnitz.GnitzRefusedError, match=f"row {_N} of this push"):
        spanning.push(tid, batch)
    assert bag(rows(client, "SELECT * FROM sv")) == {}
    spanning.push(tid, gnitz.ZSetBatch(schema).extend(_rows(0, _N)))
    assert bag(rows(client, "SELECT * FROM sv")) == {(i, i, i, i): 1 for i in range(_N)}


def test_a_reference_refused_in_the_last_frame_commits_nothing(client, spanning):
    """Foreign keys over the whole write: one dangling reference in the last
    frame refuses the INSERT whole."""
    client.execute_sql("CREATE TABLE p (id BIGINT PRIMARY KEY)")
    client.execute_sql("CREATE TABLE c (cid BIGINT PRIMARY KEY, pid BIGINT NOT NULL REFERENCES p(id), w BIGINT, x BIGINT)")
    client.execute_sql("INSERT INTO p VALUES (0)")
    tid, schema = spanning.resolve_table("c")
    refs = [{"cid": i, "pid": 0, "w": i, "x": i} for i in range(_N)]
    with pytest.raises(gnitz.GnitzIntegrityError, match="(?i)foreign key"):
        spanning.push(tid, gnitz.ZSetBatch(schema).extend(refs + [{"cid": _N, "pid": 999, "w": 0, "x": 0}]), "error")
    assert bag(rows(client, "SELECT * FROM c")) == {}
    spanning.push(tid, gnitz.ZSetBatch(schema).extend(refs), "error")
    assert bag(rows(client, "SELECT * FROM c")) == {(i, 0, i, i): 1 for i in range(_N)}


def test_a_write_past_the_log_is_refused_as_no_retry_fits(own_server, monkeypatch):
    """A write over many frames whose rows a 16 MiB SAL cannot hold even empty
    is refused as a request no retry fits, not as a full log a retry may get
    room in; it commits nothing, and the connection serves the next write."""
    monkeypatch.setenv("GNITZ_WRITE_FRAME_BYTES", str(64 << 10))
    srv = own_server.start(extra_env={"GNITZ_SAL_BYTES": str(16 << 20)})
    with gnitz.connect(srv.target) as client:
        client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, pad TEXT)")
        tid, schema = client.resolve_table("t")
        pad = "x" * 8_000
        big = gnitz.ZSetBatch(schema).extend([{"id": i, "pad": pad} for i in range(2_600)])
        with pytest.raises(gnitz.GnitzRefusedError, match="no retry fits it") as refused:
            client.push(tid, big)
        assert type(refused.value) is gnitz.GnitzRefusedError, "not one a retry may fit"
        assert rows(client, "SELECT * FROM t") == []
        client.push(tid, gnitz.ZSetBatch(schema).extend([{"id": 1, "pad": "y"}]))
        assert bag(rows(client, "SELECT * FROM t")) == {(1, "y"): 1}
