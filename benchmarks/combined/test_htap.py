"""HTAP: transactional writers + always-fresh views + consistent reads (multiworker).

Showcases the combination unique to gnitz: OLTP atomic transactions, incremental
dashboard views, and consistent multi-relation reads. Writers commit atomic
{order + K lineitems} transactions and RMW a hot inventory row under OCC; readers
hammer scan_many over base tables and dashboard views at one SAL cut. The hard
invariant: an atomic commit is never observed torn (every writer order present
has all K lineitems), and the group view never leads the base at the same cut —
so torn_reads must be 0.
"""

from __future__ import annotations

import os
import time

import pytest

import gnitz
from helpers.datagen import HTAP_SIZES, NATIONS, STATUS, push_rows, zipf_choice
from helpers.timing import percentiles, run_htap

pytestmark = pytest.mark.multiworker

WRITER_BASE = 10_000_000   # writer order keys live above every base order key
N_CUST = 200
SKEW_S = 1.1


def test_htap(client, socket_path, schema_name, bench_timer, scale_mode):
    import random

    sn = schema_name
    sz = HTAP_SIZES[scale_mode]
    K, products, base_orders = sz["K_lines"], sz["products"], sz["base_orders"]

    client.execute_sql("CREATE TABLE customer (c_key BIGINT NOT NULL PRIMARY KEY, "
                       "c_nation BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE TABLE orders (o_key BIGINT NOT NULL PRIMARY KEY, "
                       "o_cust BIGINT NOT NULL, o_status TEXT NOT NULL, o_price BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql("CREATE TABLE lineitem (l_order BIGINT NOT NULL, l_line BIGINT NOT NULL, "
                       "l_qty BIGINT NOT NULL, PRIMARY KEY (l_order, l_line))", schema_name=sn)
    client.execute_sql("CREATE TABLE inventory (pk BIGINT NOT NULL PRIMARY KEY, qty BIGINT NOT NULL)",
                       schema_name=sn)
    # Dashboard views (R1: join projects col-refs only, then group downstream).
    client.execute_sql("CREATE VIEW v_co AS SELECT customer.c_nation AS c_nation, "
                       "orders.o_price AS o_price FROM customer "
                       "JOIN orders ON orders.o_cust = customer.c_key", schema_name=sn)
    client.execute_sql("CREATE VIEW rev_by_nation AS SELECT c_nation, SUM(o_price) AS rev "
                       "FROM v_co GROUP BY c_nation", schema_name=sn)
    client.execute_sql("CREATE VIEW orders_by_status AS SELECT o_status, COUNT(*) AS cnt "
                       "FROM orders GROUP BY o_status", schema_name=sn)

    # Seed base.
    rng = random.Random(7)
    c_tid, c_sch = client.resolve_table(sn, "customer")
    push_rows(client, c_tid, c_sch, [{"c_key": k, "c_nation": k % len(NATIONS)}
                                 for k in range(1, N_CUST + 1)])
    o_tid, o_sch = client.resolve_table(sn, "orders")
    push_rows(client, o_tid, o_sch, [{"o_key": k, "o_cust": (k % N_CUST) + 1,
                                  "o_status": STATUS[k % len(STATUS)], "o_price": rng.randint(100, 50000)}
                                 for k in range(1, base_orders + 1)])
    l_tid, l_sch = client.resolve_table(sn, "lineitem")
    push_rows(client, l_tid, l_sch, [{"l_order": k, "l_line": 1, "l_qty": (k % 20) + 1}
                                 for k in range(1, base_orders + 1)])
    inv_tid, inv_sch = client.resolve_table(sn, "inventory")
    push_rows(client, inv_tid, inv_sch, [{"pk": p, "qty": 1_000_000} for p in range(1, products + 1)])

    def writer_fn(conn, deadline):
        wo_tid, wo_sch = conn.resolve_table(sn, "orders")
        wl_tid, wl_sch = conn.resolve_table(sn, "lineitem")
        wrng = random.Random(os.getpid())
        base = WRITER_BASE + os.getpid() * 10_000_000
        commits = conflicts = 0
        lat = []
        i = 0
        while time.perf_counter() < deadline:
            okey = base + i
            i += 1
            start = time.perf_counter()
            with conn.transaction() as txn:
                ob = gnitz.ZSetBatch(wo_sch)
                ob.append(o_key=okey, o_cust=(okey % N_CUST) + 1,
                          o_status=STATUS[okey % len(STATUS)], o_price=wrng.randint(100, 50000), weight=1)
                txn.push(wo_tid, ob)
                lb = gnitz.ZSetBatch(wl_sch)
                for ln in range(1, K + 1):
                    lb.append(l_order=okey, l_line=ln, l_qty=ln, weight=1)
                txn.push(wl_tid, lb)
            commits += 1
            prod = zipf_choice(wrng, products, SKEW_S)
            for _ in range(64):
                try:
                    conn.execute_sql(f"UPDATE inventory SET qty = qty - 1 WHERE pk = {prod}",
                                     schema_name=sn)
                    break
                except gnitz.GnitzConflictError:
                    conflicts += 1
            lat.append((time.perf_counter() - start) * 1000.0)
        return {"commits": commits, "conflicts": conflicts, "latencies": lat}

    def reader_fn(conn, deadline):
        ro_tid, _ = conn.resolve_table(sn, "orders")
        rl_tid, _ = conn.resolve_table(sn, "lineitem")
        rev_vid, _ = conn.resolve_table(sn, "rev_by_nation")
        obs_vid, _ = conn.resolve_table(sn, "orders_by_status")
        ops = torn = 0
        lat = []
        while time.perf_counter() < deadline:
            start = time.perf_counter()
            o_snap, l_snap, _rev, obs = conn.scan_many([ro_tid, rl_tid, rev_vid, obs_vid])
            lat.append((time.perf_counter() - start) * 1000.0)
            # Torn/consistency accounting runs AFTER the timed scan_many so it
            # never inflates the measured reader latency.
            line_counts: dict[int, int] = {}
            for r in l_snap:
                if r.weight > 0:
                    line_counts[r.l_order] = line_counts.get(r.l_order, 0) + 1
            n_orders = 0
            for r in o_snap:
                if r.weight > 0:
                    n_orders += 1
                    if r.o_key >= WRITER_BASE and line_counts.get(r.o_key, 0) != K:
                        torn += 1  # atomic {order + K lines} observed torn
                        break
            # The group view never LEADS the base at the same cut (it is derived
            # from orders, so it can only lag under concurrent writes, never
            # count an order the base snapshot lacks).
            total_status = sum(r.cnt for r in obs if r.weight > 0)
            if total_status > n_orders:
                torn += 1
            ops += 1
        return {"ops": ops, "latencies": lat, "torn_reads": torn}

    res = run_htap(socket_path, writer_fn, reader_fn, sz["writers"], sz["readers"], sz["duration_s"])

    bench_timer.num_clients = sz["writers"] + sz["readers"]
    bench_timer.add_latencies(res["reader_latencies"], rows=res["reader_ops"])
    _, _, wp99 = percentiles(res["writer_latencies"])
    _, _, rp99 = percentiles(res["reader_latencies"])
    wdur = res["writer_commits"] / sz["duration_s"] if sz["duration_s"] else 0.0
    bench_timer.extra.update(
        writer_txns_per_sec=round(wdur, 1), writer_conflicts=res["writer_conflicts"],
        writer_p99_ms=round(wp99, 3), reader_ops=res["reader_ops"],
        reader_p99_ms=round(rp99, 3), torn_reads=res["torn_reads"],
    )

    assert res["writer_commits"] > 0, "no writer commits"
    assert res["reader_ops"] > 0, "no reader ops"
    assert res["torn_reads"] == 0, f"observed {res['torn_reads']} torn/inconsistent snapshots"
