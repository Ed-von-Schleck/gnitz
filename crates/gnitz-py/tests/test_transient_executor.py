"""E2E tests for the on-demand (transient) ad-hoc query executor.

An ad-hoc SELECT that no thin client-side path can serve is compiled into the
same DBSP circuit a CREATE VIEW builds, registered as a RAM-only unmaintained
relation, driven ONCE over the committed base snapshot, streamed back, and
discarded.

These tests exist at W=4 specifically. Almost every way this feature can break is
*distributed*, and every one of those failures still returns "the right rows":

  * an un-skipped exchange over replicated sources    -> weights x W
  * an N-broadcast result scan instead of a unicast   -> rows x W
  * a mis-routed pure-range relay (scatter vs         -> rows silently MISSING
    broadcast)
  * a lost ViewMeta injection                         -> rows silently MISSING
  * a drive that races the reactor-parking DDL        -> cluster wedge

So the assertions here are on WEIGHTS and on equivalence to the CREATE VIEW path,
never on row presence alone. `GNITZ_WORKERS=1` skips the distribution-sensitive
ones -- they cannot fail there.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_transient_executor.py -v --tb=short
"""
import os
import random
import threading
import time

import pytest
import gnitz
from _serverproc import tiny_checkpoint_server

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="distribution bugs (exchange skip, N-broadcast) only appear at W >= 2"
)


def _uid():
    return str(random.randint(100000, 999999))


def _rows(client, sql, sn):
    """Every positive-weight row of an ad-hoc SELECT as (values..., weight).

    Weights are kept and rows are NOT deduped: a W-fold read inflation or a
    weight multiple is exactly what these tests must catch, and both are
    indistinguishable from a correct answer if you only compare row sets.
    """
    res = client.execute_sql(sql, schema_name=sn)
    assert res[0]["type"] == "Rows", f"expected Rows from {sql!r}, got {res[0]['type']}"
    out = []
    for r in res[0]["rows"]:
        if r.weight <= 0:
            continue
        d = r._asdict()
        out.append((tuple(d[k] for k in sorted(d)), r.weight))
    return sorted(out)


def _field_names(row):
    """The row's *visible* field names (hidden key slots excluded)."""
    return list(row._asdict().keys())


def _seed(client, sn, n=60):
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, v BIGINT NOT NULL)",
        schema_name=sn,
    )
    vals = ", ".join(f"({i}, {i % 7}, {i * 2})" for i in range(1, n + 1))
    client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)


def _assert_matches_view(client, sn, select_sql, view_name):
    """The governing property: an ad-hoc SELECT and the equivalent CREATE VIEW
    must agree on rows AND weights. The view is the trusted oracle -- the same
    circuit, driven by the maintained backfill instead of a transient drive."""
    client.execute_sql(f"CREATE VIEW {view_name} AS {select_sql}", schema_name=sn)
    expected = _rows(client, f"SELECT * FROM {view_name}", sn)
    got = _rows(client, select_sql, sn)
    assert got == expected, (
        f"ad-hoc executor disagrees with CREATE VIEW for {select_sql!r}\n"
        f"  view      : {expected}\n"
        f"  transient : {got}"
    )
    return got


# ---------------------------------------------------------------------------
# Every shuffle shape, weight-correct
#
# Each carries an ExchangeShard (or a Join, which repartitions at runtime), so
# each drives the full distributed path: prep broadcast -> per-source drive ->
# relay round(s) -> result scan.
# ---------------------------------------------------------------------------

@_NEEDS_MULTI
def test_group_by_split_count_is_weight_correct(client):
    """The canonical shape. A GROUP BY's rows split across workers by group key;
    if the drive's exchange or its termination is wrong the counts come back
    partial or multiplied -- never absent."""
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn)
    got = _assert_matches_view(client, sn, "SELECT g, COUNT(*) AS n FROM t GROUP BY g", "v_gb")
    # Independent oracle: 60 rows over g = i % 7.
    expected = {}
    for i in range(1, 61):
        expected[i % 7] = expected.get(i % 7, 0) + 1
    # values are (g, n) sorted by field name -> ('g','n')
    assert {v[0]: v[1] for v, _ in got} == expected
    assert all(w == 1 for _, w in got), f"each group row is emitted exactly once: {got}"


@_NEEDS_MULTI
def test_distinct_is_weight_correct(client):
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn)
    got = _assert_matches_view(client, sn, "SELECT DISTINCT g FROM t", "v_d")
    assert sorted(v[0] for v, _ in got) == list(range(7))
    assert all(w == 1 for _, w in got), f"DISTINCT clamps every weight to 1: {got}"


@_NEEDS_MULTI
def test_aggregate_is_weight_correct(client):
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn)
    got = _assert_matches_view(
        client, sn, "SELECT g, SUM(v) AS s, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY g", "v_agg"
    )
    # Independent oracle as well as the view: SUM/MIN/MAX are a linear + two
    # non-linear aggregates, and view-equivalence alone would not catch the two
    # paths sharing a wrong answer. Fields sort to (g, hi, lo, s).
    expected = {}
    for i in range(1, 61):
        g, v = i % 7, i * 2
        cur = expected.setdefault(g, [0, v, v])
        cur[0] += v
        cur[1] = min(cur[1], v)
        cur[2] = max(cur[2], v)
    assert {v[0]: [v[3], v[2], v[1]] for v, _ in got} == expected, f"SUM/MIN/MAX per group: {got}"


@_NEEDS_MULTI
def test_group_by_having_is_weight_correct(client):
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn)
    _assert_matches_view(
        client, sn, "SELECT g, COUNT(*) AS n FROM t GROUP BY g HAVING COUNT(*) > 8", "v_hav"
    )


@_NEEDS_MULTI
def test_set_ops_are_weight_correct(client):
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn)
    # UNION may draw both sides from `t`; INTERSECT/EXCEPT may not -- the planner
    # rejects an anti-join whose two inputs are the same relation (a single
    # source_id would feed both sides in one epoch, breaking the bilinear form),
    # so those two get a second table.
    _assert_matches_view(client, sn, "SELECT g FROM t WHERE g < 3 UNION SELECT g FROM t WHERE g > 4", "v_u")

    client.execute_sql(
        "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)", schema_name=sn
    )
    client.execute_sql(
        "INSERT INTO t2 VALUES " + ", ".join(f"({i}, {i % 4})" for i in range(1, 25)), schema_name=sn
    )
    _assert_matches_view(client, sn, "SELECT g FROM t EXCEPT SELECT g FROM t2", "v_e")
    _assert_matches_view(client, sn, "SELECT g FROM t INTERSECT SELECT g FROM t2", "v_i")
    _assert_matches_view(client, sn, "SELECT g FROM t UNION ALL SELECT g FROM t2", "v_ua")


@_NEEDS_MULTI
def test_two_way_join_is_weight_correct(client):
    """An equi-join repartitions both inputs at runtime via the join-shard
    scatter, so it drives two sources and two relay streams."""
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn)
    client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, tag BIGINT NOT NULL)",
        schema_name=sn,
    )
    client.execute_sql(
        "INSERT INTO u VALUES " + ", ".join(f"({i}, {i % 7}, {i * 10})" for i in range(1, 15)),
        schema_name=sn,
    )
    _assert_matches_view(client, sn, "SELECT t.id, u.tag FROM t JOIN u ON t.g = u.g", "v_j")


@_NEEDS_MULTI
def test_semi_join_is_weight_correct(client):
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn)
    client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)", schema_name=sn
    )
    client.execute_sql("INSERT INTO u VALUES (1, 1), (2, 3), (3, 5)", schema_name=sn)
    _assert_matches_view(
        client, sn, "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.g = t.g)", "v_ex"
    )
    _assert_matches_view(client, sn, "SELECT id FROM t WHERE g IN (SELECT g FROM u)", "v_in")


@_NEEDS_MULTI
def test_range_join_returns_all_rows(client):
    """A pure-range join (no equality conjunct) BROADCASTS its delta -- its
    matches spread over the whole key space. The relay decides scatter-vs-
    broadcast from `range_join_n_eq`, which for a transient can only come from
    the injected ViewMeta. Lose that and rows go silently MISSING."""
    sn = "tx" + _uid()
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn
    )
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn
    )
    client.execute_sql("INSERT INTO a VALUES " + ", ".join(f"({i}, {i})" for i in range(1, 21)), schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES " + ", ".join(f"({i}, {i * 3})" for i in range(1, 8)), schema_name=sn)
    got = _assert_matches_view(client, sn, "SELECT a.id, b.id AS bid FROM a JOIN b ON a.x < b.y", "v_range")
    # Independent oracle: a.x in 1..20, b.y in {3,6,...,21}; every (a,b) with a.x < b.y.
    expected = sum(min(y - 1, 20) for y in (3, 6, 9, 12, 15, 18, 21))
    assert len(got) == expected, (
        f"a pure-range join must return ALL {expected} matching pairs -- a scattered "
        f"(rather than broadcast) relay silently drops rows; got {len(got)}"
    )
    assert all(w == 1 for _, w in got), f"each pair exactly once: {got[:5]}"


# ---------------------------------------------------------------------------
# The replicated-output guard
# ---------------------------------------------------------------------------

@_NEEDS_MULTI
def test_all_replicated_group_by_returns_one_copy(client):
    """A transient over an all-replicated source computes the FULL result locally
    on every worker (its exchange is skipped). The output is therefore replicated
    and must be read from ONE worker.

    The sharpest test in the file. A transient is in no DepTab, so the
    replication verdict cannot be derived the way a view's is -- it rides the
    transient's stamped schema. Lose it and every row returns W times (broadcast
    result scan) and/or at W-fold weight (un-skipped exchange), with the row SET
    still perfectly correct.
    """
    sn = "tx" + _uid()
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL) WITH (replicated = true)",
        schema_name=sn,
    )
    client.execute_sql(
        "INSERT INTO dim VALUES " + ", ".join(f"({i}, {i % 3})" for i in range(1, 13)), schema_name=sn
    )
    got = _rows(client, "SELECT g, COUNT(*) AS n FROM dim GROUP BY g", sn)
    assert len(got) == 3, f"expected 3 groups exactly once, got {len(got)}: {got}"
    assert {v[0]: v[1] for v, _ in got} == {0: 4, 1: 4, 2: 4}, f"counts must not be x{_NUM_WORKERS}: {got}"
    assert all(w == 1 for _, w in got), f"weights must be 1, not x{_NUM_WORKERS}: {got}"

    got = _rows(client, "SELECT DISTINCT g FROM dim", sn)
    assert len(got) == 3, f"replicated DISTINCT must return one copy, got {got}"


# ---------------------------------------------------------------------------
# A transient over a VIEW source must see the view's ticked state
# ---------------------------------------------------------------------------

def test_transient_over_view_source_sees_ticked_state(client):
    """`SELECT ... FROM a_view` resolves to a ScanDelta of the view's maintained
    output store. Without draining that view's pending ticks first, the ad-hoc
    query reads a stale store and silently under-reports."""
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn, n=20)
    client.execute_sql("CREATE VIEW base AS SELECT id, g FROM t WHERE g > 2", schema_name=sn)
    # Written AFTER the view exists, so these rows reach `base` only via a tick.
    client.execute_sql("INSERT INTO t VALUES (500, 5, 999), (501, 6, 998)", schema_name=sn)

    got = _rows(client, "SELECT g, COUNT(*) AS n FROM base GROUP BY g", sn)
    counts = {v[0]: v[1] for v, _ in got}
    expected = {}
    for i in list(range(1, 21)) + [500, 501]:
        g = 5 if i == 500 else 6 if i == 501 else i % 7
        if g > 2:
            expected[g] = expected.get(g, 0) + 1
    assert counts == expected, f"the transient must see the view's ticked state: {counts} != {expected}"


# ---------------------------------------------------------------------------
# Concurrency: the drive_rwlock exclusion
# ---------------------------------------------------------------------------

@_NEEDS_MULTI
def test_transient_concurrent_with_pushes_and_create_view(client, server):
    """The drive_rwlock exclusion, end to end.

    A CREATE VIEW parks the single-threaded reactor (drain_tick_blocking +
    fan_out_backfill are synchronous futex loops) under the catalog write lock.
    Beginning that while a shuffle transient's exchange is in flight wedges the
    cluster: the transient's relays can never be emitted, so it never ACKs, so
    the DDL never returns. Both must complete and stay correct.
    """
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn, n=200)
    client.execute_sql(
        "CREATE TABLE other (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn
    )

    errors = []
    ddl_done = threading.Event()
    q_count = [0]

    def hammer_queries():
        # Runs until the DDL has returned (bounded), so the overlap does not
        # depend on guessing how long 12 queries take -- the test asserts below
        # that queries really were in flight across the DDL.
        try:
            with gnitz.connect(server) as c:
                while not ddl_done.is_set() and q_count[0] < 400:
                    res = c.execute_sql("SELECT g, COUNT(*) AS n FROM t GROUP BY g", schema_name=sn)
                    assert res[0]["type"] == "Rows"
                    n = sum(r.n for r in res[0]["rows"] if r.weight > 0)
                    assert n == 200, f"ad-hoc query must see all 200 rows, saw {n}"
                    q_count[0] += 1
        except Exception as e:  # noqa: BLE001
            errors.append(("query", repr(e)))

    def hammer_pushes():
        try:
            with gnitz.connect(server) as c:
                i = 0
                while not ddl_done.is_set() and i < 400:
                    c.execute_sql(f"INSERT INTO other VALUES ({i}, {i})", schema_name=sn)
                    i += 1
        except Exception as e:  # noqa: BLE001
            errors.append(("push", repr(e)))

    tq = threading.Thread(target=hammer_queries)
    tp = threading.Thread(target=hammer_pushes)
    tq.start()
    tp.start()

    # The reactor-parking DDL the exclusion exists for: it must complete, not wedge.
    time.sleep(0.05)
    q_before = q_count[0]
    t0 = time.time()
    client.execute_sql("CREATE VIEW mid AS SELECT g, COUNT(*) AS n FROM t GROUP BY g", schema_name=sn)
    ddl_secs = time.time() - t0
    q_across = q_count[0] - q_before
    ddl_done.set()

    tq.join(timeout=120)
    tp.join(timeout=120)
    assert not tq.is_alive() and not tp.is_alive(), "deadlock: a concurrent worker never completed"
    assert not errors, f"concurrent work failed: {errors}"
    assert ddl_secs < 60, f"CREATE VIEW took {ddl_secs:.1f}s -- wedged behind the drives"
    # The premise of this test, asserted rather than assumed: ad-hoc drives really
    # were running while the reactor-parking DDL ran. Without this the test can
    # silently degenerate into "CREATE VIEW works" and never catch a regression.
    assert q_across > 0, (
        "no ad-hoc query overlapped the CREATE VIEW -- the test proved nothing about "
        "the drive/DDL exclusion"
    )

    # The mid-flight DDL produced a correct view, and ad-hoc queries still agree.
    got = _rows(client, "SELECT g, COUNT(*) AS n FROM t GROUP BY g", sn)
    view = _rows(client, "SELECT * FROM mid", sn)
    assert got == view, f"post-DDL transient must agree with the view built mid-flight: {got} != {view}"


# ---------------------------------------------------------------------------
# Lifecycle: a transient leaves nothing behind
# ---------------------------------------------------------------------------

def test_repeated_transients_are_stable(client):
    """Ids are monotone and never recycled, and every drive tears down on every
    exit path. Repeating an ad-hoc query must give an identical answer -- a
    leaked store, a stale cached plan, or a reused id drifts the weights."""
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn)
    sql = "SELECT g, COUNT(*) AS n FROM t GROUP BY g"
    first = _rows(client, sql, sn)
    for i in range(15):
        assert _rows(client, sql, sn) == first, f"run {i} drifted from run 0"


def test_interleaved_distinct_transients_do_not_alias(client):
    """Back-to-back ad-hoc queries get distinct ids and must not see each other's
    circuit or store. If a tid were reused while a worker still held a stale
    plan, the second query would silently answer with the FIRST query's
    circuit."""
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn)
    q1 = "SELECT g, COUNT(*) AS n FROM t GROUP BY g"
    q2 = "SELECT DISTINCT g FROM t"
    q3 = "SELECT g, SUM(v) AS s FROM t GROUP BY g"
    a1, a2, a3 = _rows(client, q1, sn), _rows(client, q2, sn), _rows(client, q3, sn)
    assert len(a1[0][0]) == 2 and len(a2[0][0]) == 1, "the shapes must genuinely differ"
    for _ in range(5):
        assert _rows(client, q1, sn) == a1
        assert _rows(client, q2, sn) == a2
        assert _rows(client, q3, sn) == a3


def test_transient_reflects_writes_between_runs(client):
    """Each drive reads the committed base snapshot at drive time."""
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn, n=10)
    sql = "SELECT g, COUNT(*) AS n FROM t GROUP BY g HAVING g = 1"
    assert _rows(client, sql, sn)[0][0][1] == 2, "g=1 starts with ids 1 and 8"
    client.execute_sql("INSERT INTO t VALUES (777, 1, 1)", schema_name=sn)
    assert _rows(client, sql, sn)[0][0][1] == 3, "the next transient must see the new committed row"
    client.execute_sql("DELETE FROM t WHERE id = 777", schema_name=sn)
    assert _rows(client, sql, sn)[0][0][1] == 2, "and a retraction too"


def test_synthetic_key_slots_are_hidden(client):
    """A GROUP BY / DISTINCT / set-op adds a synthetic key column. It must be
    flagged hidden so user rows show only the projected columns -- exactly as a
    view scan does. A leaked synthetic key surfaces an internal hash to the
    user."""
    sn = "tx" + _uid()
    client.create_schema(sn)
    _seed(client, sn)
    for sql, projected in [
        ("SELECT g, COUNT(*) AS n FROM t GROUP BY g", ["g", "n"]),
        ("SELECT DISTINCT g FROM t", ["g"]),
        ("SELECT g FROM t WHERE g < 3 UNION SELECT g FROM t WHERE g > 4", ["g"]),
    ]:
        res = client.execute_sql(sql, schema_name=sn)
        rows = [r for r in res[0]["rows"] if r.weight > 0]
        assert rows, f"{sql!r} returned no rows to inspect"
        for r in rows:
            names = [n.lower() for n in _field_names(r)]
            assert names == projected, f"{sql!r} must present only {projected}, got {names}"
            assert not any(n.startswith("_") for n in names), f"synthetic key leaked: {names}"


# ---------------------------------------------------------------------------
# Checkpoints must not persist any transient state
# ---------------------------------------------------------------------------

@pytest.fixture
def checkpoint_server():
    """Function-scoped server with a tiny SAL checkpoint threshold, so
    checkpoints fire repeatedly during the test's own writes."""
    with tiny_checkpoint_server("gnitz_tx_ckpt_") as srv:
        yield srv


def test_transient_leaves_no_checkpoint_artifact(checkpoint_server):
    """A transient is RAM-only: never checkpointed, never recovered. Under a
    checkpoint storm interleaved with ad-hoc queries, none of its state may reach
    a manifest, and the answers must stay stable.

    The `_transient/` scratch tree is the transient's whole on-disk footprint. It
    must be gone once the drives are done -- teardown removes each tid's tree.
    """
    sock_path, data_dir = checkpoint_server
    sn = "tx" + _uid()
    with gnitz.connect(sock_path) as c:
        c.create_schema(sn)
        _seed(c, sn, n=400)  # enough SAL traffic to trip several checkpoints

        sql = "SELECT g, COUNT(*) AS n FROM t GROUP BY g"
        first = _rows(c, sql, sn)
        assert sum(v[1] for v, _ in first) == 400

        for i in range(6):
            # Interleave writes (driving checkpoints) with ad-hoc queries.
            c.execute_sql(f"INSERT INTO t VALUES ({10_000 + i}, 0, 1)", schema_name=sn)
            got = _rows(c, sql, sn)
            assert sum(v[1] for v, _ in got) == 400 + i + 1, (
                f"iteration {i}: a checkpoint must not perturb the ad-hoc answer: {got}"
            )

        # No transient scratch survives its drive. Teardown is fire-and-forget by
        # design -- the master broadcasts DropTransient after the drive's last ACK
        # and does NOT await one for the teardown itself (that would cost a
        # round-trip on every ad-hoc query). So the reap is asynchronous: poll,
        # rather than racing it. What must NOT happen is scratch surviving
        # indefinitely.
        def _leftovers():
            out = []
            for root, dirs, _files in os.walk(data_dir):
                if os.path.basename(root) == "_transient":
                    out += [os.path.join(root, d) for d in dirs]
            return out

        deadline = time.time() + 30
        while time.time() < deadline and _leftovers():
            time.sleep(0.1)
        assert not _leftovers(), f"transient scratch leaked past teardown: {_leftovers()}"
