"""Transaction write-primitive latency / overhead (multiworker).

- atomic_multitable: the native atomic OLTP write primitive — a pool of
  connections each committing `with txn: push(order); push(K lineitems)`.
- ryow_overlay: the in-transaction RYOW overlay cost of a multi-statement SQL
  BEGIN; M compounding UPDATEs; COMMIT, vs an autocommit baseline (in extra).
- begin_commit_overhead: transaction-control round-trip cost isolated.

Marked multiworker: the atomic-commit lock union and OCC window are distributed.
"""

from __future__ import annotations

import time

import pytest

import gnitz
from helpers.datagen import TXN_SIZES
from helpers.timing import run_pool

pytestmark = pytest.mark.multiworker

POOL = 4  # concurrent atomic-writer connections


def _sz(scale_mode):
    return TXN_SIZES[scale_mode]


# ---------------------------------------------------------------------------
# atomic_multitable — native atomic multi-table commit throughput
# ---------------------------------------------------------------------------

def _atomic_worker(socket_path, sn, k_lines, ops, widx, barrier, queue):
    commits = conflicts = 0
    lat = []
    with gnitz.connect(socket_path) as c:
        o_tid, o_sch = c.resolve_table(sn, "orders")
        l_tid, l_sch = c.resolve_table(sn, "lineitem")
        base = widx * ops + 1  # disjoint order-key range per writer
        barrier.wait()
        t0 = time.perf_counter()
        for i in range(ops):
            okey = (base + i) * 1000  # room for lineitem sub-keys / no collisions
            start = time.perf_counter()
            try:
                with c.transaction() as txn:
                    ob = gnitz.ZSetBatch(o_sch)
                    ob.append(o_key=okey, o_cust=(okey % 97) + 1, o_amt=100, weight=1)
                    txn.push(o_tid, ob)
                    lb = gnitz.ZSetBatch(l_sch)
                    for ln in range(1, k_lines + 1):
                        lb.append(l_order=okey, l_line=ln, l_qty=ln, weight=1)
                    txn.push(l_tid, lb)
                commits += 1
            except gnitz.GnitzConflictError:
                conflicts += 1
            lat.append((time.perf_counter() - start) * 1000.0)
        elapsed = time.perf_counter() - t0
    queue.put({"commits": commits, "conflicts": conflicts, "latencies_ms": lat, "elapsed_s": elapsed})


def test_atomic_multitable(client, socket_path, schema_name, bench_timer, scale_mode):
    sn, sz = schema_name, _sz(scale_mode)
    client.execute_sql("CREATE TABLE orders (o_key BIGINT NOT NULL PRIMARY KEY, "
                       "o_cust BIGINT NOT NULL, o_amt BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE TABLE lineitem (l_order BIGINT NOT NULL, l_line BIGINT NOT NULL, "
                       "l_qty BIGINT NOT NULL, PRIMARY KEY (l_order, l_line))", schema_name=sn)

    ops, k = sz["ops"], sz["K_lines"]
    parts = run_pool([(_atomic_worker, (socket_path, sn, k, ops, w)) for w in range(POOL)])

    commits = sum(p["commits"] for p in parts)
    lat = [x for p in parts for x in p["latencies_ms"]]
    elapsed = max(p["elapsed_s"] for p in parts)
    bench_timer.num_clients = POOL
    bench_timer.add_latencies(lat, rows=commits * (k + 1))
    bench_timer.extra.update(txns_per_sec=round(commits / elapsed, 1) if elapsed else 0.0,
                             conflicts=sum(p["conflicts"] for p in parts), commits=commits)
    assert commits > 0
    vid_orders, _ = client.resolve_table(sn, "orders")
    assert len(client.scan(vid_orders)) > 0


# ---------------------------------------------------------------------------
# ryow_overlay — multi-statement SQL transaction with compounding UPDATEs
# ---------------------------------------------------------------------------

def test_ryow_overlay(client, schema_name, bench_timer, scale_mode):
    sn, sz = schema_name, _sz(scale_mode)
    client.execute_sql("CREATE TABLE acc (pk BIGINT NOT NULL PRIMARY KEY, bal BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql("INSERT INTO acc VALUES (1, 1000000000)", schema_name=sn)
    m = sz["K_lines"] * 2  # UPDATEs per transaction

    def txn_block():
        client.execute_sql("BEGIN", schema_name=sn)
        for _ in range(m):
            client.execute_sql("UPDATE acc SET bal = bal - 1 WHERE pk = 1", schema_name=sn)
        client.execute_sql("COMMIT", schema_name=sn)

    iters = min(sz["ops"], 300)
    for _ in range(iters):
        bench_timer.measure(txn_block, rows_per_call=m)

    # Autocommit baseline: the same M UPDATEs without the transaction overlay.
    base_iters = min(iters, 50)
    t0 = time.perf_counter()
    for _ in range(base_iters):
        for _ in range(m):
            client.execute_sql("UPDATE acc SET bal = bal - 1 WHERE pk = 1", schema_name=sn)
    autocommit_ms = (time.perf_counter() - t0) / base_iters * 1000.0
    bench_timer.extra.update(autocommit_ms=round(autocommit_ms, 3), updates_per_txn=m)


# ---------------------------------------------------------------------------
# begin_commit_overhead — transaction-control round-trip cost
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("variant", ["empty_begin_commit", "one_write_txn", "autocommit_push"])
def test_begin_commit_overhead(client, schema_name, bench_timer, scale_mode, variant):
    sn, sz = schema_name, _sz(scale_mode)
    client.execute_sql("CREATE TABLE kv (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                       schema_name=sn)
    tid, schema = client.resolve_table(sn, "kv")
    iters = min(sz["ops"], 500)

    def one_row_batch(pk):
        b = gnitz.ZSetBatch(schema)
        b.append(pk=pk, v=pk, weight=1)
        return b

    if variant == "empty_begin_commit":
        def op(_i):
            client.execute_sql("BEGIN", schema_name=sn)
            client.execute_sql("COMMIT", schema_name=sn)
    elif variant == "one_write_txn":
        def op(i):
            with client.transaction() as txn:
                txn.push(tid, one_row_batch(i + 1))
    else:  # autocommit_push
        def op(i):
            client.push(tid, one_row_batch(i + 1))

    for i in range(iters):
        bench_timer.measure(op, i, rows_per_call=1)
