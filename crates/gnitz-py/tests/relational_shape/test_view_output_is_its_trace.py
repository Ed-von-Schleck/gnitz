"""A view whose last operator is a GROUP BY keeps that operator's output once,
and a top-N keeps no copy of its output at all.

A reduce needs the integral of its own output, which is what the view's store
holds: the view's directory carries no second copy of it. The store is
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
from _serverproc import disk_usage

_GROUPS = 6_000
_VIEWS = {
    "by_g": "SELECT g, COUNT(*) AS n, SUM(v) AS total FROM t GROUP BY g",
    "top": "SELECT id, g, v FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC) <= 2",
    "ext": "SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY g",
}


def _push(conn, rows, weight=1):
    tid, schema = conn.resolve_table("t")
    conn.push(tid, gnitz.ZSetBatch(schema).extend(
        {"id": id_, "g": g, "v": v, "_weight": weight} for id_, (g, v) in rows.items()))


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


def _churn(conn, live, rng, rounds):
    """`rounds` pushes, each inserting rows under fresh ids and moving some to
    another group and value."""
    next_id = max(live, default=0) + 1
    for _ in range(rounds):
        rows = {}
        for _ in range(3_000):
            rows[next_id] = (rng.randrange(_GROUPS), next_id * 1000 + rng.randrange(1000))
            next_id += 1
        for id_ in rng.sample(sorted(live), min(len(live), 2_000)):
            rows[id_] = (rng.randrange(_GROUPS), id_ * 1000 + rng.randrange(1000))
        _push(conn, rows)
        live.update(rows)


def test_a_view_ending_in_an_aggregate_reads_its_own_store_as_history(sweeping):
    rng, live = random.Random(11), {}
    with gnitz.connect(sweeping.target) as conn:
        conn.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, v BIGINT NOT NULL)")
        for name, body in _VIEWS.items():
            conn.execute_sql(f"CREATE VIEW {name} AS {body}")
        ids = {conn.resolve_table(name)[0]: name for name in ["t", *_VIEWS]}
        _churn(conn, live, rng, 5)
        _assert_views(conn, live, "live")

    sweeping.restart()
    with gnitz.connect(sweeping.target) as conn:
        _assert_views(conn, live, "after a crash")
        _churn(conn, live, rng, 2)
        _assert_views(conn, live, "churn after a crash")

    sweeping.restart(graceful=True)
    # The report reads the files alone, beside the server now running on them:
    # each relation's stores holding shards, without the node each is numbered by.
    report, stores = disk_usage(sweeping.data_dir)
    kinds = {name: set() for name in ids.values()}
    for s in stores:
        if s["relation"] in ids:
            kinds[ids[s["relation"]]].add(s["store"].rstrip("0123456789").rstrip("_"))
    assert kinds == {
        "t": {"rows"},
        "by_g": {"rows"},
        "ext": {"rows", "scratch_reduce", "scratch_avidx"},
        # The top-N's index of every input row is the one store it keeps
        # beside the view's rows.
        "top": {"rows", "scratch_topnidx"},
    }, report
    with gnitz.connect(sweeping.target) as conn:
        _assert_views(conn, live, "after a graceful restart")
        _churn(conn, live, rng, 2)
        _push(conn, {id_: live.pop(id_) for id_ in rng.sample(sorted(live), len(live) // 3)}, weight=-1)
        _assert_views(conn, live, "churn and deletes after a graceful restart")
