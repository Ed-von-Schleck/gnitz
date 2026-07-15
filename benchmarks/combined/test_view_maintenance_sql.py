"""SQL-path ceiling for the flagship view-maintenance DAG (INSERT VALUES stream).

The identical DAG as test_view_maintenance.py, but deltas stream through
`execute_sql("INSERT … VALUES …")` / SQL UPDATE / DELETE. This is parse-plan
bound, so it quantifies the parse-vs-binary gap — the cost a pure SQL client
pays — next to the push() maintenance number.
"""

from __future__ import annotations

import random
import time

from helpers.vm_dag import (AMOUNT_MAX, DELETES_PER_ITER, SIZES, UPDATES_PER_ITER,
                            seed, setup_views, status)


def test_view_maintenance_sql(client, schema_name, bench_timer, scale_mode):
    """Stream INSERT/UPDATE/DELETE via execute_sql over the view DAG."""
    sn = schema_name
    sz = SIZES[scale_mode]
    rng = random.Random(42)
    setup_views(client, sn)
    seed(client, sn, sz, rng)

    dim_rows, insert_rows = sz["DIM_ROWS"], sz["INSERT_ROWS"]
    next_pk = sz["FACT_SEED"] + 1
    t_end = time.monotonic() + sz["DURATION_S"]
    while time.monotonic() < t_end:
        rows = []
        for _ in range(insert_rows):
            pk = next_pk
            next_pk += 1
            rows.append(f"({pk},{rng.randint(1, dim_rows)},"
                        f"{rng.randint(0, AMOUNT_MAX)},{status(rng)})")
        ins = ("INSERT INTO fact_orders (o_id, o_customer, o_amount, o_status) "
               "VALUES " + ",".join(rows))
        bench_timer.measure(client.execute_sql, ins, sn, rows_per_call=insert_rows)

        hi = next_pk - 1
        for _ in range(UPDATES_PER_ITER):
            client.execute_sql(
                f"UPDATE fact_orders SET o_status={status(rng)}, "
                f"o_amount={rng.randint(0, AMOUNT_MAX)} WHERE o_id={rng.randint(1, hi)}",
                schema_name=sn)
        for _ in range(DELETES_PER_ITER):
            client.execute_sql(
                f"DELETE FROM fact_orders WHERE o_id={rng.randint(1, hi)}",
                schema_name=sn)

    vid, _ = client.resolve_table(sn, "v_rev")
    assert len(client.scan(vid)) > 0, "v_rev empty"
