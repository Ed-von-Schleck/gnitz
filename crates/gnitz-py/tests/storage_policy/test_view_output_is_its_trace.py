"""A view whose last operator is a GROUP BY or a top-N keeps that operator's
output once.

Such an operator needs the integral of its own output, which is what the view's
store holds: the view's directory carries no second copy of it. The store is
driven into the disk regime, so the operator reads its history off shards the
view's readers also read, and each step is checked at its weights: a group
whose aggregate moved must be there once, at the new value. A crash and a
graceful restart are both followed by more churn, which reads the history the
boot left.

An aggregate the view projects a column out of — a MIN or MAX carries one — is
not the view's output, and keeps its trace.
"""

import random

import gnitz
from _read import bag, scanned
from _serverproc import MULTI, disk_usage

_GROUPS = 6_000
_VIEWS = {
    "by_g": "SELECT g, COUNT(*) AS n, SUM(v) AS total FROM t GROUP BY g",
    "top": "SELECT id, g, v FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC) <= 2",
    "ext": "SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY g",
}


def _push(conn, rows, weight=1):
    tid, schema = conn.resolve_table("t")
    batch = gnitz.ZSetBatch(schema)
    for id_, (g, v) in rows.items():
        batch.append(_weight=weight, id=id_, g=g, v=v)
    conn.push(tid, batch)


def _assert_views(conn, live, ctx):
    groups = {}
    for id_, (g, v) in live.items():
        groups.setdefault(g, []).append((v, id_))
    by_g = {(g, len(m), sum(v for v, _ in m)): 1 for g, m in groups.items()}
    # No two rows share a `v`, so a group's two largest are determined.
    top = {(id_, g, v): 1 for g, m in groups.items() for v, id_ in sorted(m)[-2:]}
    ext = {(g, min(m)[0], max(m)[0]): 1 for g, m in groups.items()}
    assert bag(scanned(conn, "by_g"), "g", "n", "total") == by_g, ctx
    assert bag(scanned(conn, "top"), "id", "g", "v") == top, ctx
    assert bag(scanned(conn, "ext"), "g", "lo", "hi") == ext, ctx


def _churn(conn, live, rng, rounds, next_id):
    """`rounds` pushes, each inserting rows and moving some to another group and
    value; returns the next unused id."""
    for _ in range(rounds):
        rows = {}
        for _ in range(3_000):
            rows[next_id] = (rng.randrange(_GROUPS), next_id * 1000 + rng.randrange(1000))
            next_id += 1
        for id_ in rng.sample(sorted(live), min(len(live), 2_000)):
            rows[id_] = (rng.randrange(_GROUPS), id_ * 1000 + rng.randrange(1000))
        _push(conn, rows)
        live.update(rows)
    return next_id


def test_a_view_ending_in_an_aggregate_reads_its_own_store_as_history(own_server):
    own_server.extra_env = {"GNITZ_RAM_TIER_BYTES": "1024"}
    own_server.start(workers=MULTI)
    rng, live = random.Random(11), {}
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, v BIGINT NOT NULL)")
        for name, body in _VIEWS.items():
            conn.execute_sql(f"CREATE VIEW {name} AS {body}")
        next_id = _churn(conn, live, rng, 5, 1)
        _assert_views(conn, live, "live")

    own_server.restart()
    with gnitz.connect(own_server.target) as conn:
        _assert_views(conn, live, "after a crash")
        next_id = _churn(conn, live, rng, 2, next_id)
        _assert_views(conn, live, "churn after a crash")

    own_server.restart(graceful=True)
    with gnitz.connect(own_server.target) as conn:
        ids = {name: conn.resolve_table(name)[0] for name in _VIEWS}
    # The report reads the files alone, beside the server now running on them.
    report, stores = disk_usage(own_server.data_dir)

    def kinds(relation=None):
        """The stores holding shards, without the node each is numbered by."""
        return {s["store"].rstrip("0123456789").rstrip("_") for s in stores
                if s["relation"] == relation or (relation is None and s["relation"] >= gnitz.FIRST_USER_TABLE_ID)}

    assert kinds(ids["by_g"]) == {"rows"}, report
    assert kinds(ids["ext"]) == {"rows", "scratch_reduce", "scratch_avidx"}, report
    # The top-N's index of every input row is the one store it keeps beside
    # the view's rows.
    assert kinds(ids["top"]) == {"rows", "scratch_topnidx"}, report
    assert kinds() == {"rows", "scratch_reduce", "scratch_avidx", "scratch_topnidx"}, report
    with gnitz.connect(own_server.target) as conn:
        _assert_views(conn, live, "after a graceful restart")
        _churn(conn, live, rng, 2, next_id)
        _push(conn, {id_: live.pop(id_) for id_ in rng.sample(sorted(live), len(live) // 3)}, weight=-1)
        _assert_views(conn, live, "churn and deletes after a graceful restart")
