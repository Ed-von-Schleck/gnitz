"""OCC contention scaling curve (multiworker).

N concurrent clients each run `ops` autocommit `UPDATE ctr SET n=n+1 WHERE pk IN
(hot set of size H)` — table-grain first-committer-wins. With app-level retry
every increment eventually lands, so the final counter must equal N*ops (no lost
update — the cheap correctness guard). Reports committed txns/s, conflicts,
retries, p99 across the (N, H) sweep.
"""

from __future__ import annotations

import pytest

from helpers.datagen import OCC_CLIENTS, OCC_HOT_SET
from helpers.timing import run_contended_rmw

pytestmark = pytest.mark.multiworker

CONTENTION_OPS = {"quick": 100, "full": 1000}


def _positive(client, tid):
    return [(r.pk, r.n) for r in client.scan(tid) if r.weight > 0]


@pytest.mark.parametrize("n_clients", OCC_CLIENTS)
@pytest.mark.parametrize("hot", OCC_HOT_SET)
def test_occ_contention(client, socket_path, schema_name, bench_timer, scale_mode, n_clients, hot):
    sn = schema_name
    client.execute_sql("CREATE TABLE ctr (pk BIGINT NOT NULL PRIMARY KEY, n BIGINT NOT NULL)",
                       schema_name=sn)
    seed = ",".join(f"({k}, 0)" for k in range(1, hot + 1))
    client.execute_sql(f"INSERT INTO ctr VALUES {seed}", schema_name=sn)

    ops = CONTENTION_OPS[scale_mode]
    hot_list = ",".join(str(k) for k in range(1, hot + 1))
    sql = f"UPDATE ctr SET n = n + 1 WHERE pk IN ({hot_list})"

    res = run_contended_rmw(socket_path, sql, sn, n_clients, ops, retry=True)

    bench_timer.num_clients = n_clients
    bench_timer.add_latencies(res["latencies_ms"], rows=res["commits"])
    txps = res["commits"] / res["elapsed_s"] if res["elapsed_s"] else 0.0
    bench_timer.extra.update(
        txns_per_sec=round(txps, 1), conflicts=res["conflicts"],
        retries=res["retries"], hot_set=hot,
    )

    # No lost update: every hot row incremented exactly once per committed op.
    expected = n_clients * ops
    tid, _ = client.resolve_table(sn, "ctr")
    rows = _positive(client, tid)
    assert len(rows) == hot
    for pk, val in rows:
        assert val == expected, f"lost update: pk={pk} n={val} != {expected}"
