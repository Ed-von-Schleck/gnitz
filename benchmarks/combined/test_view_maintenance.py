"""Sustained view-maintenance workload, for perf profiling (push-driven).

A fact/dim schema with a filter -> join -> grouped SUM/COUNT + grouped MIN/MAX +
DISTINCT view DAG. Bulk-loads a base, then streams deltas via the binary push()
path for a fixed wall-clock window, so the measured number is the DBSP circuit,
not the parser. Run under perf with `make bench-profile`. The INSERT-VALUES
ceiling for the same DAG lives in test_view_maintenance_sql.py.
"""

from __future__ import annotations

import random
import time
from collections import deque

import gnitz
from helpers.vm_dag import (AMOUNT_MAX, DELETES_PER_ITER, SIZES, UPDATES_PER_ITER,
                            seed, setup_views, status)


def test_view_maintenance(client, schema_name, bench_timer, scale_mode):
    """Stream inserts (measured) + upsert-updates + retract-deletes via push()
    over the view DAG for a wall-clock window."""
    sn = schema_name
    sz = SIZES[scale_mode]
    rng = random.Random(42)
    setup_views(client, sn)
    seed(client, sn, sz, rng)

    fact_tid, fact_schema = client.resolve_table(sn, "fact_orders")
    dim_rows, insert_rows = sz["DIM_ROWS"], sz["INSERT_ROWS"]
    fact_seed = sz["FACT_SEED"]
    next_pk = fact_seed + 1
    recent: deque[int] = deque()
    t_end = time.monotonic() + sz["DURATION_S"]
    while time.monotonic() < t_end:
        batch = gnitz.ZSetBatch(fact_schema)
        for _ in range(insert_rows):
            pk = next_pk
            next_pk += 1
            batch.append(o_id=pk, o_customer=rng.randint(1, dim_rows),
                         o_amount=rng.randint(0, AMOUNT_MAX), o_status=status(rng))
            recent.append(pk)
        bench_timer.measure(client.push, fact_tid, batch, rows_per_call=insert_rows)

        # Upsert-updates on seeded rows (present) — silent-upsert retracts the
        # old (pk, payload) and inserts the new. Unmeasured churn.
        ub = gnitz.ZSetBatch(fact_schema)
        for _ in range(UPDATES_PER_ITER):
            ub.append(o_id=rng.randint(1, fact_seed), o_customer=rng.randint(1, dim_rows),
                      o_amount=rng.randint(0, AMOUNT_MAX), o_status=status(rng))
        client.push(fact_tid, ub)

        # Retract-deletes of the oldest streamed rows (present, never updated).
        del_pks = [recent.popleft() for _ in range(min(DELETES_PER_ITER, len(recent)))]
        if del_pks:
            client.delete(fact_tid, fact_schema, del_pks)

    vid, _ = client.resolve_table(sn, "v_rev")
    assert len(client.scan(vid)) > 0, "v_rev empty"
