"""E2E: live CREATE VIEW over already-populated base tables.

A view created live (after server start) over base tables that already hold data
must come back identical to the same view created before the data — for every
shape (projection, GROUP BY, equi-join, set-op, SELECT DISTINCT, range/band
join) and every ordering of the source rows relative to the CREATE:

  * pending     — INSERT then CREATE VIEW (rows still un-ticked at CREATE)
  * committed   — INSERT, force a tick, then CREATE VIEW (rows committed)
  * view-first  — CREATE VIEW then INSERT (the steady-state control)

The historical bugs this guards: pending sources double-counted (projection 2x,
equi-join up to 4x) because the inline backfill and the deferred ticks both
drove the new view; committed sources under-counted (GROUP BY/DISTINCT/set-op
came back empty, multi-worker equi-join a per-key prefix) because the inline
single-process driver is the wrong one for exchange/join views. The assertions
check WEIGHTS, not just row presence — several broken states have the right row
set but doubled weights.

Run at GNITZ_WORKERS 1 and 4 (the exchange/fanout paths only engage at W>1):
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest \
        tests/test_view_backfill_existing_data.py -v --tb=short
"""
import os
import signal
import subprocess
import tempfile
import threading
import time
import random

import pytest
import gnitz
from _serverproc import server_preexec

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, tables=None, views=None):
    for name in (views or []):
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
    for name in (tables or []):
        try:
            client.execute_sql(f"DROP TABLE {name}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _values(rows):
    return ", ".join("(" + ", ".join(str(c) for c in r) + ")" for r in rows)


def _scan_dicts(client, tid):
    return [r._asdict() for r in client.scan(tid) if r.weight > 0]


def _positive_rows(client, vid):
    """(rows-as-dicts, weight-by-row-tuple) over a view scan, positive net only."""
    rows = []
    wmap = {}
    for r in client.scan(vid):
        if r.weight == 0:
            continue
        d = r._asdict()
        rows.append((d, r.weight))
        key = tuple(sorted(d.items()))
        wmap[key] = wmap.get(key, 0) + r.weight
    return rows, wmap


def _assert_unit(client, vid, expected_count, msg):
    """Every surviving row has net weight exactly 1, no duplicate-row ghosts,
    and the count matches. Catches over-count (weight 2/4), under-count
    (missing rows / empty), and the multi-worker join per-key prefix."""
    _, wmap = _positive_rows(client, vid)
    bad = {k: v for k, v in wmap.items() if v != 1}
    assert not bad, f"{msg}: non-unit weights (over/under-count): {bad}"
    assert len(wmap) == expected_count, \
        f"{msg}: got {len(wmap)} unit rows, want {expected_count}"


def _assert_keys(client, vid, expected_keys, key_fn, msg):
    """As `_assert_unit`, and the key set matches `expected_keys` exactly."""
    _, wmap = _positive_rows(client, vid)
    bad = {k: v for k, v in wmap.items() if v != 1}
    assert not bad, f"{msg}: non-unit weights (over/under-count): {bad}"
    got = {key_fn(dict(k)) for k in wmap}
    assert got == expected_keys, (
        f"{msg}: key mismatch — missing {expected_keys - got}, extra {got - expected_keys}"
    )


# ── shape builders ────────────────────────────────────────────────────────────
# Each returns: view DDL, the base tables, an insert() closure, the expected key
# set, a key extractor over the view row dict, and the tables to drop.

_A_DDL = "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)"
_A_ROWS = [(i, i % 5, i * 10) for i in range(30)]  # (id, k, av)


def _create_a(client, sn):
    """The standard 3-column `a` base table shared by most shapes."""
    client.execute_sql(_A_DDL, schema_name=sn)


def _insert_a(client, sn):
    client.execute_sql(f"INSERT INTO a VALUES {_values(_A_ROWS)}", schema_name=sn)


def _shape_proj(client, sn):
    _create_a(client, sn)
    return dict(
        view="CREATE VIEW v AS SELECT id, av + 1 AS p FROM a",
        bases=["a"],
        insert=lambda: _insert_a(client, sn),
        expected={(i, i * 10 + 1) for i in range(30)},
        key=lambda d: (d["id"], d["p"]),
        drop=["a"],
    )


def _shape_grp(client, sn):
    _create_a(client, sn)
    return dict(
        view="CREATE VIEW v AS SELECT k, COUNT(*) AS c FROM a GROUP BY k",
        bases=["a"],
        insert=lambda: _insert_a(client, sn),
        expected={(k, 6) for k in range(5)},
        key=lambda d: (d["k"], d["c"]),
        drop=["a"],
    )


def _shape_joi(client, sn):
    _create_a(client, sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)",
        schema_name=sn,
    )

    def insert():
        _insert_a(client, sn)
        client.execute_sql(f"INSERT INTO b VALUES {_values([(j, j * 100) for j in range(5)])}", schema_name=sn)

    return dict(
        view="CREATE VIEW v AS SELECT a.id AS aid, b.bv AS bv FROM a JOIN b ON a.k = b.id",
        bases=["a", "b"],
        insert=insert,
        expected={(i, (i % 5) * 100) for i in range(30)},
        key=lambda d: (d["aid"], d["bv"]),
        drop=["a", "b"],
    )


def _shape_setop(client, sn):
    client.execute_sql("CREATE TABLE s1 (id BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)
    client.execute_sql("CREATE TABLE s2 (id BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)

    def insert():
        client.execute_sql(f"INSERT INTO s1 VALUES {_values([(i,) for i in range(30)])}", schema_name=sn)
        client.execute_sql(f"INSERT INTO s2 VALUES {_values([(i,) for i in range(20, 40)])}", schema_name=sn)

    return dict(
        view="CREATE VIEW v AS SELECT id FROM s1 UNION SELECT id FROM s2",
        bases=["s1", "s2"],
        insert=insert,
        expected={(i,) for i in range(40)},
        key=lambda d: (d["id"],),
        drop=["s1", "s2"],
    )


def _shape_distinct(client, sn):
    _create_a(client, sn)
    return dict(
        view="CREATE VIEW v AS SELECT DISTINCT k FROM a",
        bases=["a"],
        insert=lambda: _insert_a(client, sn),
        expected={(k,) for k in range(5)},
        key=lambda d: (d["k"],),
        drop=["a"],
    )


def _shape_rangejoin(client, sn):
    client.execute_sql("CREATE TABLE ra (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE TABLE rb (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)

    def insert():
        client.execute_sql(f"INSERT INTO ra VALUES {_values([(i, i) for i in range(10)])}", schema_name=sn)
        client.execute_sql(f"INSERT INTO rb VALUES {_values([(j, j) for j in range(10)])}", schema_name=sn)

    return dict(
        view="CREATE VIEW v AS SELECT ra.x AS x, rb.y AS y FROM ra JOIN rb ON ra.x < rb.y",
        bases=["ra", "rb"],
        insert=insert,
        expected={(i, j) for i in range(10) for j in range(10) if i < j},
        key=lambda d: (d["x"], d["y"]),
        drop=["ra", "rb"],
    )


_SHAPES = {
    "proj": _shape_proj,
    "grp": _shape_grp,
    "joi": _shape_joi,
    "setop": _shape_setop,
    "distinct": _shape_distinct,
    "rangejoin": _shape_rangejoin,
}


@pytest.mark.parametrize("shape", list(_SHAPES))
@pytest.mark.parametrize("ordering", ["pending", "committed", "view_first"])
def test_vbf_matrix(client, shape, ordering):
    """All shapes x all orderings come back weight 1 with the exact key set."""
    sn = "vbf" + shape[:4] + ordering[:3] + _uid()
    client.create_schema(sn)
    spec = _SHAPES[shape](client, sn)
    try:
        if ordering == "view_first":
            client.execute_sql(spec["view"], schema_name=sn)
            spec["insert"]()
        else:
            spec["insert"]()
            if ordering == "committed":
                # Force the inserts to tick/commit before the view exists: a
                # scan drains pending deltas. Otherwise they would be picked up
                # as still-pending on the next tick and mask the committed path.
                for b in spec["bases"]:
                    bid = client.resolve_table(sn, b)[0]
                    list(client.scan(bid))
            client.execute_sql(spec["view"], schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        _assert_keys(client, vid, spec["expected"], spec["key"], f"{shape}/{ordering}")
    finally:
        _cleanup(client, sn, tables=spec["drop"], views=["v"])


# ── original regression tests (formerly xfail) ──────────────────────────────────


def test_vbf_control_view_first_join_sees_data(client):
    """Control: view created BEFORE data — steady-state ticks populate it."""
    sn = "vbfctl" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn)
        client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
        client.execute_sql("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.id", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (10, 100), (20, 200)", schema_name=sn)
        client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20)", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        rows = _scan_dicts(client, vid)
        assert len(rows) == 2, f"control (view-first) expected 2 rows, got {len(rows)}: {rows}"
    finally:
        _cleanup(client, sn, tables=["a", "b"], views=["v"])


def test_vbf_join_over_populated_tables_sees_existing_data(client):
    """A JOIN view created AFTER committed data must see it (no longer xfail)."""
    sn = "vbfjoin" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn)
        client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (10, 100), (20, 200)", schema_name=sn)
        client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20)", schema_name=sn)
        # Force the inserts to tick/commit before the view exists.
        aid = client.resolve_table(sn, "a")[0]
        bid = client.resolve_table(sn, "b")[0]
        assert len(_scan_dicts(client, aid)) == 2, "base table a should hold 2 rows pre-view"
        assert len(_scan_dicts(client, bid)) == 2, "base table b should hold 2 rows pre-view"
        client.execute_sql("CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.id", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        _assert_unit(client, vid, 2, "join over populated tables")
    finally:
        _cleanup(client, sn, tables=["a", "b"], views=["v"])


def test_vbf_group_by_over_populated_table_sees_existing_data(client):
    """A GROUP BY view created AFTER committed data must see it (no longer xfail)."""
    sn = "vbfgb" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, n BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 7, 100), (2, 7, 200), (3, 9, 300)", schema_name=sn)
        tid = client.resolve_table(sn, "t")[0]
        assert len(_scan_dicts(client, tid)) == 3, "base table t should hold 3 rows pre-view"
        client.execute_sql("CREATE VIEW v AS SELECT g, SUM(n) AS s FROM t GROUP BY g", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        _assert_keys(client, vid, {(7, 300), (9, 300)}, lambda d: (d["g"], d["s"]), "group-by over populated")
    finally:
        _cleanup(client, sn, tables=["t"], views=["v"])


# ── concurrent-insert variant ───────────────────────────────────────────────────


def test_vbf_concurrent_insert_join(client, server):
    """Insert into the sources from a second connection WHILE CREATE VIEW runs;
    the final view must equal brute force over the full base (snapshot rows +
    every post-create delta), each weight 1. The write lock serialises the
    interleave; the result must be correct whichever order they land."""
    sn = "vbfcc" + _uid()
    client.create_schema(sn)
    try:
        _create_a(client, sn)
        client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)", schema_name=sn)
        client.execute_sql(f"INSERT INTO b VALUES {_values([(j, j * 100) for j in range(5)])}", schema_name=sn)
        # Snapshot rows present before the CREATE.
        _insert_a(client, sn)

        errors = []

        def inserter():
            try:
                with gnitz.connect(server) as c2:
                    for i in range(30, 60):
                        c2.execute_sql(f"INSERT INTO a VALUES ({i}, {i % 5}, {i * 10})", schema_name=sn)
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        t = threading.Thread(target=inserter)
        t.start()
        client.execute_sql("CREATE VIEW v AS SELECT a.id AS aid, b.bv AS bv FROM a JOIN b ON a.k = b.id", schema_name=sn)
        t.join(timeout=60)
        assert not t.is_alive(), "inserter thread hung — possible wedge"
        assert not errors, f"concurrent inserter failed: {errors}"

        vid = client.resolve_table(sn, "v")[0]
        expected = {(i, (i % 5) * 100) for i in range(60)}
        _assert_keys(client, vid, expected, lambda d: (d["aid"], d["bv"]), "concurrent-insert join")
    finally:
        _cleanup(client, sn, tables=["a", "b"], views=["v"])


def test_vbf_concurrent_insert_projection(client, server):
    """Non-exchange shape under the same concurrent interleave."""
    sn = "vbfccp" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, av BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(f"INSERT INTO a VALUES {_values([(i, i * 10) for i in range(30)])}", schema_name=sn)
        errors = []

        def inserter():
            try:
                with gnitz.connect(server) as c2:
                    for i in range(30, 60):
                        c2.execute_sql(f"INSERT INTO a VALUES ({i}, {i * 10})", schema_name=sn)
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        t = threading.Thread(target=inserter)
        t.start()
        client.execute_sql("CREATE VIEW v AS SELECT id, av + 1 AS p FROM a", schema_name=sn)
        t.join(timeout=60)
        assert not t.is_alive(), "inserter thread hung — possible wedge"
        assert not errors, f"concurrent inserter failed: {errors}"

        vid = client.resolve_table(sn, "v")[0]
        expected = {(i, i * 10 + 1) for i in range(60)}
        _assert_keys(client, vid, expected, lambda d: (d["id"], d["p"]), "concurrent-insert projection")
    finally:
        _cleanup(client, sn, tables=["a"], views=["v"])


# ── in-flight steady-state exchange variant ─────────────────────────────────────


def test_vbf_inflight_exchange_no_wedge(client, server):
    """A large insert into a table feeding an EXISTING exchange view triggers
    auto-ticks (with exchange rounds) just as we CREATE another exchange view
    over the same table. The CREATE's quiesce must drain the in-flight exchange
    tick before parking the reactor — assert no wedge and both views correct.
    Crosses TICK_COALESCE_ROWS (10_000)."""
    sn = "vbfinf" + _uid()
    client.create_schema(sn)
    N = 12000
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW g1 AS SELECT g, COUNT(*) AS c FROM t GROUP BY g", schema_name=sn)

        errors = []

        def big_insert():
            try:
                with gnitz.connect(server) as c2:
                    chunk = 2000
                    for base in range(0, N, chunk):
                        rows = [(i, i % 8) for i in range(base, min(base + chunk, N))]
                        c2.execute_sql(f"INSERT INTO t VALUES {_values(rows)}", schema_name=sn)
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        t = threading.Thread(target=big_insert)
        t.start()
        # Give the insert a moment to start firing auto-ticks, then create a
        # second exchange view concurrently.
        time.sleep(0.05)
        client.execute_sql("CREATE VIEW g2 AS SELECT g, COUNT(*) AS c FROM t GROUP BY g", schema_name=sn)
        t.join(timeout=120)
        assert not t.is_alive(), "big inserter hung — wedge"
        assert not errors, f"big inserter failed: {errors}"

        # Drain any remaining pending ticks before the final read.
        tid = client.resolve_table(sn, "t")[0]
        list(client.scan(tid))
        exp = {}
        for i in range(N):
            exp[i % 8] = exp.get(i % 8, 0) + 1
        expected = set(exp.items())
        g1 = client.resolve_table(sn, "g1")[0]
        g2 = client.resolve_table(sn, "g2")[0]
        _assert_keys(client, g1, expected, lambda d: (d["g"], d["c"]), "in-flight g1")
        _assert_keys(client, g2, expected, lambda d: (d["g"], d["c"]), "in-flight g2")
    finally:
        _cleanup(client, sn, tables=["t"], views=["g1", "g2"])


# ── dedicated-server variants (checkpoint window, restart) ──────────────────────


def _spawn(data_dir, sock_path, extra_env=None):
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../gnitz-server")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    cmd = [binary, data_dir, sock_path]
    if _NUM_WORKERS:
        cmd += [f"--workers={_NUM_WORKERS}"]
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    proc = subprocess.Popen(
        cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        start_new_session=True, env=env, preexec_fn=server_preexec,
    )
    for _ in range(100):
        if os.path.exists(sock_path):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        proc.communicate()
        raise RuntimeError("Server did not start")
    return proc


def _kill(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def test_vbf_checkpoint_in_window_group_by():
    """Insert enough to cross a low SAL checkpoint threshold (draining
    pending_deltas), THEN CREATE an exchange GROUP BY view. The committed store
    is the only driver — a flushed-snapshot under-count or a checkpoint-strands-
    exchange regression both surface as a wrong/empty result. Assert weight 1."""
    tmp = tempfile.mkdtemp(dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_vbfckpt_")
    data_dir = os.path.join(tmp, "data")
    sock = os.path.join(tmp, "gnitz.sock")
    env = {"GNITZ_CHECKPOINT_BYTES": "65536"}
    proc = _spawn(data_dir, sock, extra_env=env)
    try:
        conn = gnitz.connect(sock)
        conn.create_schema("ck")
        conn.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, n BIGINT NOT NULL)",
            schema_name="ck",
        )
        N = 4000
        chunk = 1000
        for base in range(0, N, chunk):
            rows = [(i, i % 10, i) for i in range(base, min(base + chunk, N))]
            conn.execute_sql(f"INSERT INTO t VALUES {_values(rows)}", schema_name="ck")
        conn.execute_sql("CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM t GROUP BY g", schema_name="ck")
        vid, _ = conn.resolve_table("ck", "v")
        exp = {}
        for i in range(N):
            exp[i % 10] = exp.get(i % 10, 0) + 1
        _assert_keys(conn, vid, set(exp.items()), lambda d: (d["g"], d["c"]), "checkpoint-window group-by")
        conn.close()
    finally:
        _kill(proc)
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def test_vbf_nested_live_then_restart():
    """`g` over an already-created exchange view `v`, both created live after
    data. Assert live correctness, then crash-restart and assert `g` equals
    brute force — catching any boot nested double-count."""
    tmp = tempfile.mkdtemp(dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_vbfnest_")
    data_dir = os.path.join(tmp, "data")
    sock = os.path.join(tmp, "gnitz.sock")
    proc = _spawn(data_dir, sock)
    try:
        conn = gnitz.connect(sock)
        conn.create_schema("ne")
        conn.execute_sql(
            "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name="ne",
        )
        rows = [(i, i % 4, (i % 4) + 1) for i in range(40)]  # distinct per-group sums
        conn.execute_sql(f"INSERT INTO a VALUES {_values(rows)}", schema_name="ne")
        # Commit the rows before either view exists.
        aid, _ = conn.resolve_table("ne", "a")
        list(conn.scan(aid))
        # v (GROUP BY over a), then g (GROUP BY over v) — both live, over data.
        conn.execute_sql("CREATE VIEW v AS SELECT grp, SUM(val) AS s FROM a GROUP BY grp", schema_name="ne")
        conn.execute_sql("CREATE VIEW g AS SELECT s, COUNT(*) AS c FROM v GROUP BY s", schema_name="ne")

        v_sums = {}
        for (_, grp, val) in rows:
            v_sums[grp] = v_sums.get(grp, 0) + val
        exp_g = {}
        for s in v_sums.values():
            exp_g[s] = exp_g.get(s, 0) + 1

        gid, _ = conn.resolve_table("ne", "g")
        _assert_keys(conn, gid, set(exp_g.items()), lambda d: (d["s"], d["c"]), "nested live g")
        conn.close()

        # Crash-restart: boot backfill must rebuild g exactly, no double-count.
        _kill(proc)
        if os.path.exists(sock):
            os.unlink(sock)
        proc = _spawn(data_dir, sock)
        conn = gnitz.connect(sock)
        gid, _ = conn.resolve_table("ne", "g")
        _assert_keys(conn, gid, set(exp_g.items()), lambda d: (d["s"], d["c"]), "nested g after restart")
        conn.close()
    finally:
        _kill(proc)
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)
