"""Boot exchange-view backfill must re-derive each exchange view exactly once.

At startup the master rebuilds exchange views by driving each base table that
roots an exchange view exactly once (drive_dag(base) re-derives that base's whole
dependent closure). The previous loop drove once per (exchange view, immediate
source) pair, which re-derived shared closures N times (F1) and drove intermediate
views as sources (F2), over-counting a view's ephemeral state.

The backfill must also reach views the master classifies by node shape. An
equi-join carries no `ExchangeShard` node — it scatters its inputs through the
runtime join-shard path — so the `ExchangeShard`-only classifier used to exclude
it from the backfill base set. A two-base equi-join whose bases root no other
exchange view (`x = a JOIN b`) was then driven by neither backfill path and came
back a deterministic per-key prefix at W>1 (e.g. 9 of 30 rows). The fix seeds the
base walk with equi-join views too; these tests pin restart-equivalence for the
join shapes (bare, shared-base) and keep the single-pass-join and range-join
controls green.

These tests crash-restart a populated server and assert each derived view comes
back with its pre-restart value, not a multiple. Run at GNITZ_WORKERS=4 (the
exchange/fanout paths only engage with multiple workers).
"""

import os
import subprocess
import tempfile
import time
import shutil
import signal

import pytest
import gnitz
from _serverproc import server_preexec

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))


def _start_server(data_dir, sock_path, workers=None, extra_env=None):
    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__),
                                     "../../../gnitz-server")),
    )
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    cmd = [binary, data_dir, sock_path]
    if workers:
        cmd += [f"--workers={workers}"]
    env = None
    if extra_env:
        env = os.environ.copy()
        env.update(extra_env)
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
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


def _stop_server(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def _crash_and_restart(proc, sock_path, data_dir, workers=None, extra_env=None):
    """SIGKILL the whole process group, clean up the socket, restart."""
    _stop_server(proc)
    if os.path.exists(sock_path):
        os.unlink(sock_path)
    return _start_server(data_dir, sock_path, workers=workers, extra_env=extra_env)


def _make_env(prefix):
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"), prefix=prefix,
    )
    return tmpdir, os.path.join(tmpdir, "data"), os.path.join(tmpdir, "gnitz.sock")


def _values(rows):
    """Render a list of tuples as a SQL VALUES tail."""
    return ", ".join("(" + ", ".join(str(c) for c in r) + ")" for r in rows)


def _by(rows, key):
    """Positive-weight scan rows keyed by `key` (asserts one row per key)."""
    out = {}
    for r in rows:
        if r.weight <= 0:
            continue
        k = r[key]
        assert k not in out, f"duplicate key {k} in {[dict(r._asdict()) for r in rows]}"
        out[k] = r
    return out


def test_f1_shared_base_exchange_views_not_doubled():
    """F1: a base table feeding several exchange views must be driven once.

    `vx1` and `vx2` both group `t`; the old loop drove `t` once per view, so each
    view's aggregates came back exactly doubled. With the dedup they are driven
    once and the sums survive unchanged.
    """
    tmpdir, data_dir, sock_path = _make_env("gnitz_f1_")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("f1")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "g BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name="f1",
        )
        conn.execute_sql(
            "CREATE VIEW vx1 AS SELECT g, SUM(val) AS s FROM t GROUP BY g",
            schema_name="f1",
        )
        conn.execute_sql(
            "CREATE VIEW vx2 AS SELECT g, SUM(pk) AS s FROM t GROUP BY g",
            schema_name="f1",
        )
        rows = [(pk, pk % 6, pk + 1) for pk in range(60)]
        conn.execute_sql(f"INSERT INTO t VALUES {_values(rows)}", schema_name="f1")

        exp_vx1, exp_vx2 = {}, {}
        for pk, g, val in rows:
            exp_vx1[g] = exp_vx1.get(g, 0) + val
            exp_vx2[g] = exp_vx2.get(g, 0) + pk

        vx1, _ = conn.resolve_table("f1", "vx1")
        vx2, _ = conn.resolve_table("f1", "vx2")
        assert {g: r["s"] for g, r in _by(conn.scan(vx1), "g").items()} == exp_vx1
        conn.close()

        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        vx1, _ = conn.resolve_table("f1", "vx1")
        vx2, _ = conn.resolve_table("f1", "vx2")
        got1 = {g: r["s"] for g, r in _by(conn.scan(vx1), "g").items()}
        got2 = {g: r["s"] for g, r in _by(conn.scan(vx2), "g").items()}
        assert got1 == exp_vx1, f"vx1 doubled? got {got1}, want {exp_vx1}"
        assert got2 == exp_vx2, f"vx2 doubled? got {got2}, want {exp_vx2}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _build_f2(conn, schema, extra_views=False):
    """a (base) -> v (exchange GROUP BY) -> g (exchange GROUP BY over v)."""
    conn.create_schema(schema)
    conn.execute_sql(
        "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, "
        "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
        schema_name=schema,
    )
    # Optional decoy exchange views to perturb the boot loop's hashmap order;
    # the dedup-to-base set must make the result independent of that order.
    if extra_views:
        conn.execute_sql(
            "CREATE VIEW decoy1 AS SELECT grp, COUNT(*) AS c FROM a GROUP BY grp",
            schema_name=schema,
        )
    conn.execute_sql(
        "CREATE VIEW v AS SELECT grp, SUM(val) AS s FROM a GROUP BY grp",
        schema_name=schema,
    )
    if extra_views:
        conn.execute_sql(
            "CREATE VIEW decoy2 AS SELECT s, COUNT(*) AS c FROM v GROUP BY s",
            schema_name=schema,
        )
    # g groups v by its per-group sum and counts how many groups share each sum.
    conn.execute_sql(
        "CREATE VIEW g AS SELECT s, COUNT(*) AS c FROM v GROUP BY s",
        schema_name=schema,
    )


@pytest.mark.parametrize("extra_views", [False, True], ids=["plain", "perturbed"])
def test_f2_nested_exchange_over_exchange(extra_views):
    """F2: an exchange view nested over another exchange view must not be filled
    twice. Driving the base alone fills the whole chain transitively; the old
    loop additionally drove the intermediate view as a source, double-filling the
    top view in a hashmap-order-dependent way. Asserted with and without decoy
    views that perturb that order.
    """
    tmpdir, data_dir, sock_path = _make_env("gnitz_f2_")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        _build_f2(conn, "f2", extra_views=extra_views)
        # grp in {0,1}; pick vals so the two groups have DISTINCT sums (so g has
        # two singleton groups) — a doubling of v's sums would move them.
        rows = [(pk, pk % 2, (pk % 2) + 1) for pk in range(20)]
        conn.execute_sql(f"INSERT INTO a VALUES {_values(rows)}", schema_name="f2")

        v_sums = {}
        for pk, grp, val in rows:
            v_sums[grp] = v_sums.get(grp, 0) + val
        exp_g = {}
        for s in v_sums.values():
            exp_g[s] = exp_g.get(s, 0) + 1

        conn.close()
        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        vid, _ = conn.resolve_table("f2", "v")
        gid, _ = conn.resolve_table("f2", "g")
        got_v = {grp: r["s"] for grp, r in _by(conn.scan(vid), "grp").items()}
        got_g = {s: r["c"] for s, r in _by(conn.scan(gid), "s").items()}
        assert got_v == v_sums, f"v doubled? got {got_v}, want {v_sums}"
        assert got_g == exp_g, f"g double-filled? got {got_g}, want {exp_g}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_exchange_over_nonexchange_after_restart():
    """`x` (exchange) over `n` (non-exchange projection) over base `a`.

    The dedup drives the transitive base `a`, whose single drive fills both `n`
    and `x`. `n` is cascade-reachable, so the worker non-exchange pass must skip
    it — asserted by `n` carrying weight exactly 1 (not doubled) while `x` is
    correct. Base data is flushed so a regressed inline open-time backfill would
    have double-filled `n`.
    """
    tmpdir, data_dir, sock_path = _make_env("gnitz_xovern_")
    env = {"GNITZ_CHECKPOINT_BYTES": "1024"}
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS, extra_env=env)
        conn = gnitz.connect(sock_path)
        conn.create_schema("xn")
        conn.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="xn",
        )
        conn.execute_sql(
            "CREATE VIEW n AS SELECT pk, val + 1 AS p1 FROM a",
            schema_name="xn",
        )
        conn.execute_sql(
            "CREATE VIEW x AS SELECT p1, COUNT(*) AS c FROM n GROUP BY p1",
            schema_name="xn",
        )
        # val in {0..4}; p1 = val+1 in {1..5}; counts per p1 known.
        rows = [(pk, pk % 5) for pk in range(100)]
        conn.execute_sql(f"INSERT INTO a VALUES {_values(rows)}", schema_name="xn")

        exp_x = {}
        for pk, val in rows:
            exp_x[val + 1] = exp_x.get(val + 1, 0) + 1

        conn.close()
        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS, extra_env=env)
        conn = gnitz.connect(sock_path)

        nid, _ = conn.resolve_table("xn", "n")
        xid, _ = conn.resolve_table("xn", "x")

        # n: every base row projected once, weight exactly 1 (not double-filled).
        n_weight = {}
        for r in conn.scan(nid):
            n_weight[r["pk"]] = n_weight.get(r["pk"], 0) + r.weight
        assert len(n_weight) == len(rows), f"n missing rows: {len(n_weight)}/{len(rows)}"
        assert all(w == 1 for w in n_weight.values()), \
            f"n double-counted: weights {sorted(set(n_weight.values()))}"

        got_x = {p1: r["c"] for p1, r in _by(conn.scan(xid), "p1").items()}
        assert got_x == exp_x, f"x wrong after restart: got {got_x}, want {exp_x}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_join_sharing_a_base_after_restart():
    """Two joins sharing a base: `x = a JOIN b`, `y = a JOIN c`. The shared base
    `a` must be driven once; the old loop drove it once per listing view, duplicating
    every join output row. Assert both joins match a brute-force join with weight 1.

    These are two-base equi-joins: neither carries an `ExchangeShard` node, so the
    `ExchangeShard`-only classifier left them out of the backfill base set entirely
    and both came back a deterministic per-key prefix at W>1 (e.g. 9 of 30). Seeding
    `exchange_base_tables` with equi-join views drives `a`, `b`, `c`, filling both
    joins completely.
    """
    tmpdir, data_dir, sock_path = _make_env("gnitz_joinshare_")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("js")
        conn.execute_sql(
            "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
            schema_name="js",
        )
        conn.execute_sql(
            "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)",
            schema_name="js",
        )
        conn.execute_sql(
            "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, cv BIGINT NOT NULL)",
            schema_name="js",
        )
        conn.execute_sql(
            "CREATE VIEW x AS SELECT a.id AS aid, a.av AS av, b.bv AS bv "
            "FROM a JOIN b ON a.k = b.id",
            schema_name="js",
        )
        conn.execute_sql(
            "CREATE VIEW y AS SELECT a.id AS aid, a.av AS av, c.cv AS cv "
            "FROM a JOIN c ON a.k = c.id",
            schema_name="js",
        )
        a_rows = [(i, i % 5, i * 10) for i in range(30)]  # k in {0..4}
        bc_rows = [(k, k * 100) for k in range(5)]
        conn.execute_sql(f"INSERT INTO a VALUES {_values(a_rows)}", schema_name="js")
        conn.execute_sql(f"INSERT INTO b VALUES {_values(bc_rows)}", schema_name="js")
        conn.execute_sql(f"INSERT INTO c VALUES {_values(bc_rows)}", schema_name="js")

        bmap = {k: v for k, v in bc_rows}
        exp_x = {(aid, av, bmap[k]) for (aid, k, av) in a_rows}
        exp_y = exp_x  # b and c hold the same rows

        conn.close()
        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        xid, _ = conn.resolve_table("js", "x")
        yid, _ = conn.resolve_table("js", "y")

        # x: (aid, av, bv); y: (aid, av, cv) — same value tuples by construction.
        x_w = {}
        for r in conn.scan(xid):
            key = (r["aid"], r["av"], r["bv"])
            x_w[key] = x_w.get(key, 0) + r.weight
        assert all(w == 1 for w in x_w.values()), f"x duplicated: {x_w}"
        got_x = set(x_w.keys())

        y_w = {}
        for r in conn.scan(yid):
            key = (r["aid"], r["av"], r["cv"])
            y_w[key] = y_w.get(key, 0) + r.weight
        assert all(w == 1 for w in y_w.values()), f"y duplicated: {y_w}"
        got_y = set(y_w.keys())

        assert got_x == exp_x, f"x wrong: got {got_x} want {exp_x}"
        assert got_y == exp_y, f"y wrong: got {got_y} want {exp_y}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_diamond_single_counted_after_restart():
    """Diamond: `x` reaches base `a` by two paths — directly (`a JOIN v1`) and
    through `v1` (a GROUP BY over `a`). The dedup yields the single base `a`;
    within one drive_dag `x` is evaluated once per incoming edge — correct
    multi-input behaviour, not a re-run. The old loop drove `a` for both `v1` and
    `x` (and `v1` as `x`'s source), tripling `x`. Assert `x` matches brute force,
    weight 1.
    """
    tmpdir, data_dir, sock_path = _make_env("gnitz_diamond_")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("dia")
        conn.execute_sql(
            "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name="dia",
        )
        conn.execute_sql(
            "CREATE VIEW v1 AS SELECT g, SUM(val) AS s FROM a GROUP BY g",
            schema_name="dia",
        )
        conn.execute_sql(
            "CREATE VIEW x AS SELECT a.id AS id, a.g AS g, v1.s AS s "
            "FROM a JOIN v1 ON a.g = v1.g",
            schema_name="dia",
        )
        a_rows = [(i, i % 4, i + 1) for i in range(40)]  # g in {0..3}
        conn.execute_sql(f"INSERT INTO a VALUES {_values(a_rows)}", schema_name="dia")

        gsum = {}
        for (i, g, val) in a_rows:
            gsum[g] = gsum.get(g, 0) + val
        exp_x = {(i, g, gsum[g]) for (i, g, val) in a_rows}

        conn.close()
        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        xid, _ = conn.resolve_table("dia", "x")
        x_w = {}
        for r in conn.scan(xid):
            key = (r["id"], r["g"], r["s"])
            x_w[key] = x_w.get(key, 0) + r.weight
        assert all(w == 1 for w in x_w.values()), f"x duplicated: {sorted(x_w.items())}"
        assert set(x_w.keys()) == exp_x, f"x wrong: got {set(x_w.keys())} want {exp_x}"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_bare_two_base_join_after_restart():
    """Bare two-base equi-join `x = a JOIN b`, the minimal repro. Neither base
    roots any other exchange view, so the `ExchangeShard`-only classifier returned
    an empty base set and the boot backfill drove nothing for `x` — it came back a
    deterministic per-key prefix at W>1 (e.g. 9 of 30). With equi-join views seeding
    the base walk, the backfill drives both `a` and `b` and `x` recovers in full.

    Isolates the two-base equi-join under-fill from any shared-base / drive-once
    concern. Asserts restart-equivalence (set-equality vs. brute force, weight 1),
    never the W-dependent surviving subset.
    """
    tmpdir, data_dir, sock_path = _make_env("gnitz_barejoin_")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("bj")
        conn.execute_sql(
            "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
            schema_name="bj",
        )
        conn.execute_sql(
            "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)",
            schema_name="bj",
        )
        conn.execute_sql(
            "CREATE VIEW x AS SELECT a.id AS aid, a.av AS av, b.bv AS bv "
            "FROM a JOIN b ON a.k = b.id",
            schema_name="bj",
        )
        a_rows = [(i, i % 5, i * 10) for i in range(30)]  # k in {0..4}, every row matches
        b_rows = [(k, k * 100) for k in range(5)]
        conn.execute_sql(f"INSERT INTO a VALUES {_values(a_rows)}", schema_name="bj")
        conn.execute_sql(f"INSERT INTO b VALUES {_values(b_rows)}", schema_name="bj")

        bmap = {bid: bv for bid, bv in b_rows}
        exp_x = {(aid, av, bmap[k]) for (aid, k, av) in a_rows}

        conn.close()
        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        xid, _ = conn.resolve_table("bj", "x")
        x_w = {}
        for r in conn.scan(xid):
            key = (r["aid"], r["av"], r["bv"])
            x_w[key] = x_w.get(key, 0) + r.weight
        assert all(w == 1 for w in x_w.values()), f"x duplicated: {x_w}"
        assert set(x_w.keys()) == exp_x, \
            f"x under-filled: got {len(x_w)} of {len(exp_x)} rows"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_range_two_base_join_after_restart():
    """Range two-base join `r = ra JOIN rb ON ra.x < rb.y`. A range/band join
    carries an output `ExchangeShard` node, so `view_needs_exchange` already
    classifies it and the equi-join seed predicate (`DeltaTraceRange` is excluded)
    never touches it. Regression guard that the `ExchangeShard` classification path
    keeps recovering two-base joins correctly — this shape was never the bug.
    """
    tmpdir, data_dir, sock_path = _make_env("gnitz_rangejoin_")
    try:
        proc = _start_server(data_dir, sock_path, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)
        conn.create_schema("rj")
        conn.execute_sql(
            "CREATE TABLE ra (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
            schema_name="rj",
        )
        conn.execute_sql(
            "CREATE TABLE rb (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)",
            schema_name="rj",
        )
        conn.execute_sql(
            "CREATE VIEW r AS SELECT ra.x AS x, rb.y AS y FROM ra JOIN rb ON ra.x < rb.y",
            schema_name="rj",
        )
        ra_rows = [(i, i) for i in range(10)]   # (id, x)
        rb_rows = [(j, j) for j in range(10)]   # (id, y)
        conn.execute_sql(f"INSERT INTO ra VALUES {_values(ra_rows)}", schema_name="rj")
        conn.execute_sql(f"INSERT INTO rb VALUES {_values(rb_rows)}", schema_name="rj")

        # The view's pair-PK columns (r[0], r[1]) = (ra.id, rb.id).
        exp_pairs = {(ai, bi) for (ai, ax) in ra_rows for (bi, by) in rb_rows if ax < by}
        assert exp_pairs, "range join data must produce some matches"

        conn.close()
        proc = _crash_and_restart(proc, sock_path, data_dir, workers=_NUM_WORKERS)
        conn = gnitz.connect(sock_path)

        rid, _ = conn.resolve_table("rj", "r")
        p_w = {}
        for row in conn.scan(rid):
            key = (row[0], row[1])
            p_w[key] = p_w.get(key, 0) + row.weight
        assert all(w == 1 for w in p_w.values()), f"range join duplicated: {p_w}"
        assert set(p_w.keys()) == exp_pairs, \
            f"range join wrong after restart: got {len(p_w)} of {len(exp_pairs)} pairs"
        conn.close()
        _stop_server(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
