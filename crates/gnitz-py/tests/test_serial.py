"""E2E tests for SERIAL / BIGSERIAL / SMALLSERIAL auto-increment primary keys.

The client draws monotone ids from a per-table durable sequence on the master
(range-cached per connection), stamps them into the PK before pushing, and
answers INSERT ... RETURNING locally. Users may not supply a value for a SERIAL
column.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_serial.py -v
"""

import os
import random
import signal
import subprocess
import tempfile
import time

import pytest
import gnitz
from _serverproc import server_preexec


def _uid():
    return str(random.randint(100000, 999999))


def _schema(client):
    sn = "srl" + _uid()
    client.create_schema(sn)
    return sn


def _cleanup(client, sn, *tables):
    for t in tables:
        try:
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _returned_rows(result):
    """The row list from an execute_sql Rows result (e.g. RETURNING / SELECT)."""
    r = next(x for x in result if x["type"] == "Rows")
    return list(r["rows"])


# ---------------------------------------------------------------------------
# Payload indexing around the SERIAL column
# ---------------------------------------------------------------------------


def test_serial_midcolumn_payload_indexing(client):
    """SERIAL need not be column 0. With payload columns on both sides of the PK,
    each VALUES element must land in the right column — the single-PK payload_idx
    closed form (`ci if ci < pk_index else ci - 1`). A `row[ci]`-style bug would
    misplace or OOB here, but is invisible in the (id SERIAL PK, one payload) shape."""
    sn = _schema(client)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT, id SERIAL PRIMARY KEY, b TEXT)", schema_name=sn
        )
        # Explicit non-SERIAL column list (a, b) — omits the middle SERIAL column.
        client.execute_sql("INSERT INTO t (a, b) VALUES (10, 'x')", schema_name=sn)
        # Bare positional INSERT supplies the non-SERIAL columns in schema order.
        client.execute_sql("INSERT INTO t VALUES (20, 'y')", schema_name=sn)
        tid, _ = client.resolve_table(sn, "t")
        rows = sorted((r.id, r.a, r.b) for r in client.scan(tid) if r.weight > 0)
        assert rows == [(1, 10, "x"), (2, 20, "y")]
    finally:
        _cleanup(client, sn, "t")


def test_serial_null_payload(client):
    """A NULL payload value must round-trip through the null bitmap while the
    SERIAL PK is still auto-assigned."""
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        client.execute_sql("INSERT INTO t (name) VALUES ('a'), (NULL), ('c')", schema_name=sn)
        tid, _ = client.resolve_table(sn, "t")
        rows = sorted(
            (r.id, r.name) for r in client.scan(tid) if r.weight > 0
        )
        assert rows == [(1, "a"), (2, None), (3, "c")]
    finally:
        _cleanup(client, sn, "t")


def test_serial_no_reuse_after_delete(client):
    """The sequence never rewinds: a deleted id is not reissued on the next INSERT."""
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        client.execute_sql("INSERT INTO t (name) VALUES ('a'), ('b')", schema_name=sn)
        client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
        client.execute_sql("INSERT INTO t (name) VALUES ('c')", schema_name=sn)
        tid, _ = client.resolve_table(sn, "t")
        ids = sorted(r.id for r in client.scan(tid) if r.weight > 0)
        assert ids == [1, 3]  # id 2 deleted and never reused
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Cross-connection recognition (the point of the COL_TAB is_serial marker)
# ---------------------------------------------------------------------------


def test_serial_recognized_cross_connection(server):
    """A connection that only fetched the schema — never saw the CREATE — must
    still recognize the SERIAL PK (via the is_serial marker round-tripped through
    COL_TAB) and auto-assign it, rather than treating it as a user-supplied PK."""
    sn = "srlx" + _uid()
    with gnitz.connect(server) as c1:
        c1.create_schema(sn)
        c1.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
    try:
        with gnitz.connect(server) as c2:
            # c2's schema for `t` comes purely from COL_TAB, not from the CREATE.
            c2.execute_sql("INSERT INTO t (name) VALUES ('a'), ('b')", schema_name=sn)
            tid, _ = c2.resolve_table(sn, "t")
            rows = sorted((r.id, r.name) for r in c2.scan(tid) if r.weight > 0)
            assert rows == [(1, "a"), (2, "b")]
            # And a bare positional value is still rejected on the fresh connection.
            with pytest.raises(gnitz.GnitzError):
                c2.execute_sql("INSERT INTO t VALUES (5, 'x')", schema_name=sn)
    finally:
        with gnitz.connect(server) as c:
            _cleanup(c, sn, "t")


def test_serial_two_connections_disjoint_ids(server):
    """Two connections drawing from the same table's sequence get disjoint id
    ranges — no collision, no reuse. If the durable per-range advance were not
    serialized, both would draw the same range and the second INSERT would hit a
    unique_pk violation (or, worse, silently duplicate)."""
    sn = "srld" + _uid()
    with gnitz.connect(server) as c0:
        c0.create_schema(sn)
        c0.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
    try:
        with gnitz.connect(server) as c1, gnitz.connect(server) as c2:
            # Interleave so c1 draws from its range both before and after c2 does.
            c1.execute_sql("INSERT INTO t (name) VALUES ('a1'), ('a2')", schema_name=sn)
            c2.execute_sql("INSERT INTO t (name) VALUES ('b1'), ('b2')", schema_name=sn)
            c1.execute_sql("INSERT INTO t (name) VALUES ('a3')", schema_name=sn)
            tid, _ = c1.resolve_table(sn, "t")
            ids = [r.id for r in c1.scan(tid) if r.weight > 0]
            assert len(ids) == 5
            assert len(set(ids)) == 5  # all distinct across both connections
    finally:
        with gnitz.connect(server) as c:
            _cleanup(c, sn, "t")


# ---------------------------------------------------------------------------
# Basic assignment + scan
# ---------------------------------------------------------------------------


def test_serial_assigns_contiguous_ids(client):
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        client.execute_sql("INSERT INTO t (name) VALUES ('a'), ('b'), ('c')", schema_name=sn)
        tid, _ = client.resolve_table(sn, "t")
        rows = sorted(((r.id, r.name) for r in client.scan(tid) if r.weight > 0))
        assert rows == [(1, "a"), (2, "b"), (3, "c")]
    finally:
        _cleanup(client, sn, "t")


def test_bigserial_and_smallserial_assign(client):
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE big (id BIGSERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        client.execute_sql("CREATE TABLE small (id SMALLSERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        client.execute_sql("INSERT INTO big (name) VALUES ('x'), ('y')", schema_name=sn)
        client.execute_sql("INSERT INTO small (name) VALUES ('p'), ('q')", schema_name=sn)
        btid, _ = client.resolve_table(sn, "big")
        stid, _ = client.resolve_table(sn, "small")
        assert sorted(r.id for r in client.scan(btid) if r.weight > 0) == [1, 2]
        assert sorted(r.id for r in client.scan(stid) if r.weight > 0) == [1, 2]
    finally:
        _cleanup(client, sn, "big", "small")


# ---------------------------------------------------------------------------
# RETURNING
# ---------------------------------------------------------------------------


def test_returning_id(client):
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        res = client.execute_sql(
            "INSERT INTO t (name) VALUES ('a'), ('b') RETURNING id", schema_name=sn
        )
        rows = _returned_rows(res)
        assert sorted(r.id for r in rows) == [1, 2]
    finally:
        _cleanup(client, sn, "t")


def test_returning_star(client):
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        res = client.execute_sql(
            "INSERT INTO t (name) VALUES ('a') RETURNING *", schema_name=sn
        )
        rows = _returned_rows(res)
        assert len(rows) == 1
        assert rows[0].id == 1
        assert rows[0].name == "a"
    finally:
        _cleanup(client, sn, "t")


def test_returning_rejections(client):
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        # Expressions are not allowed in RETURNING.
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("INSERT INTO t (name) VALUES ('a') RETURNING id + 1", schema_name=sn)
        # A non-PK-only projection is rejected (a Z-set batch needs an identity).
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("INSERT INTO t (name) VALUES ('a') RETURNING name", schema_name=sn)
        # RETURNING on UPDATE stays unsupported.
        client.execute_sql("INSERT INTO t (name) VALUES ('a')", schema_name=sn)
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("UPDATE t SET name = 'z' RETURNING id", schema_name=sn)
    finally:
        _cleanup(client, sn, "t")


def test_returning_multiple_columns(client):
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        res = client.execute_sql(
            "INSERT INTO t (name) VALUES ('a'), ('b') RETURNING id, name", schema_name=sn
        )
        rows = sorted((r.id, r.name) for r in _returned_rows(res))
        assert rows == [(1, "a"), (2, "b")]
    finally:
        _cleanup(client, sn, "t")


def test_returning_with_on_conflict_rejected(client):
    """RETURNING combined with ON CONFLICT is explicitly unsupported (the
    effective row under an upsert/skip is out of scope)."""
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "INSERT INTO t (name) VALUES ('a') ON CONFLICT (id) DO NOTHING RETURNING id",
                schema_name=sn,
            )
        assert "conflict" in str(exc.value).lower()
    finally:
        _cleanup(client, sn, "t")


def test_serial_aliases_end_to_end(client):
    """SERIAL2 / SERIAL4 / SERIAL8 aliases assign ids end-to-end (I16/I32/I64)."""
    sn = _schema(client)
    try:
        for tname, ty in [("t2", "SERIAL2"), ("t4", "SERIAL4"), ("t8", "SERIAL8")]:
            client.execute_sql(
                f"CREATE TABLE {tname} (id {ty} PRIMARY KEY, name TEXT)", schema_name=sn
            )
            client.execute_sql(f"INSERT INTO {tname} (name) VALUES ('a'), ('b')", schema_name=sn)
            tid, _ = client.resolve_table(sn, tname)
            assert sorted(r.id for r in client.scan(tid) if r.weight > 0) == [1, 2]
    finally:
        _cleanup(client, sn, "t2", "t4", "t8")


# ---------------------------------------------------------------------------
# User-supplied-value rejections
# ---------------------------------------------------------------------------


def test_cannot_supply_serial_value(client):
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        # Naming the SERIAL column in the column list is a targeted error.
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql("INSERT INTO t (id, name) VALUES (5, 'x')", schema_name=sn)
        assert "serial" in str(exc.value).lower()
        # A bare positional INSERT supplies a value for the auto-assigned PK too.
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("INSERT INTO t VALUES (5, 'x')", schema_name=sn)
        # UPDATE of the (SERIAL) primary key is rejected by the PK-write guard.
        client.execute_sql("INSERT INTO t (name) VALUES ('a')", schema_name=sn)
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("UPDATE t SET id = 9", schema_name=sn)
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# CREATE TABLE rejections
# ---------------------------------------------------------------------------


def test_serial_must_be_lone_pk(client):
    sn = _schema(client)
    try:
        # SERIAL column that is not the PK.
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("CREATE TABLE a (id SERIAL, name TEXT PRIMARY KEY)", schema_name=sn)
        # SERIAL inside a compound PK.
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(
                "CREATE TABLE b (id SERIAL, x BIGINT, PRIMARY KEY (id, x))", schema_name=sn
            )
        # Two SERIAL columns.
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(
                "CREATE TABLE c (id SERIAL PRIMARY KEY, id2 SERIAL)", schema_name=sn
            )
    finally:
        _cleanup(client, sn, "a", "b", "c")


# ---------------------------------------------------------------------------
# Overflow
# ---------------------------------------------------------------------------


def test_smallserial_overflow(client):
    """SMALLSERIAL exhausts at i16::MAX (32767); the next id raises."""
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE t (id SMALLSERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        # Fill exactly up to i16::MAX in one statement (ids 1..32767, all fit).
        values = ",".join("('n')" for _ in range(32767))
        client.execute_sql(f"INSERT INTO t (name) VALUES {values}", schema_name=sn)
        # The next id (32768) overflows i16::MAX.
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql("INSERT INTO t (name) VALUES ('overflow')", schema_name=sn)
        assert "exhausted" in str(exc.value).lower()
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Multi-worker + cache-refill correctness
# ---------------------------------------------------------------------------


def test_multiworker_scatter_all_ids_once(client):
    """Insert many rows (crossing hash partitions via `mix`) and confirm a full
    scan returns every id exactly once — the correctness property behind the
    no-hot-partition scatter."""
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        values = ",".join(f"('n{i}')" for i in range(200))
        client.execute_sql(f"INSERT INTO t (name) VALUES {values}", schema_name=sn)
        tid, _ = client.resolve_table(sn, "t")
        ids = sorted(r.id for r in client.scan(tid) if r.weight > 0)
        assert ids == list(range(1, 201))
    finally:
        _cleanup(client, sn, "t")


def test_returning_across_cache_refill(client):
    """RETURNING id over > SERIAL_RANGE_SIZE (64) rows in one connection stays
    contiguous across the range-cache refill boundary."""
    sn = _schema(client)
    try:
        client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=sn)
        values = ",".join(f"('n{i}')" for i in range(100))
        res = client.execute_sql(
            f"INSERT INTO t (name) VALUES {values} RETURNING id", schema_name=sn
        )
        ids = sorted(r.id for r in _returned_rows(res))
        assert ids == list(range(1, 101))
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Restart monotonicity (durable high-water survives a crash-restart)
# ---------------------------------------------------------------------------


def _server_binary():
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../gnitz-server")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    return binary


def _start(data_dir, sock_path, workers):
    cmd = [_server_binary(), data_dir, sock_path, f"--workers={workers}"]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True, preexec_fn=server_preexec,
    )
    deadline = time.time() + 10.0
    while time.time() < deadline:
        if os.path.exists(sock_path):
            return proc
        time.sleep(0.05)
    proc.kill()
    proc.wait()
    raise RuntimeError("server did not start")


def _stop(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def test_restart_id_monotonicity():
    """A committed id is never re-issued: after a crash-restart on the same data
    dir, every new id exceeds every id committed before the restart (gaps from a
    discarded range tail are permitted)."""
    workers = int(os.environ.get("GNITZ_WORKERS", "4"))
    tmp = tempfile.mkdtemp(dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_serial_")
    data_dir = os.path.join(tmp, "data")
    sock = os.path.join(tmp, "gnitz.sock")
    proc = _start(data_dir, sock, workers)
    try:
        with gnitz.connect(sock) as c:
            c.create_schema("s")
            c.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name="s")
            c.execute_sql("INSERT INTO t (name) VALUES ('a'), ('b'), ('c')", schema_name="s")
            tid, _ = c.resolve_table("s", "t")
            before = sorted(r.id for r in c.scan(tid) if r.weight > 0)
        assert before == [1, 2, 3]

        # Crash-restart on the SAME data dir (durable catalog + sequence survive).
        _stop(proc)
        if os.path.exists(sock):
            os.unlink(sock)
        proc = _start(data_dir, sock, workers)

        with gnitz.connect(sock) as c:
            # A fresh connection re-fetches the schema (is_serial round-trips
            # through COL_TAB) and continues drawing ids above the durable
            # high-water.
            c.execute_sql("INSERT INTO t (name) VALUES ('d'), ('e')", schema_name="s")
            tid, _ = c.resolve_table("s", "t")
            after = sorted(r.id for r in c.scan(tid) if r.weight > 0 and r.id not in before)
        assert after, "expected new rows after restart"
        assert min(after) > max(before), f"new ids {after} must exceed pre-restart max {max(before)}"
    finally:
        _stop(proc)
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)
