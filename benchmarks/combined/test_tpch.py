"""Incremental OLAP materialized views: the TPC-H subset over a streaming feed.

Realistic decision-support workload — Q1/Q3/Q6/Q12/Q4-style views (Appendix A,
R1/R4-staged) over region/nation/customer/orders/lineitem with realistic types
and skew. The base is bulk-loaded, then lineitem deltas (fresh rows + retractions
of the previous batch — the CDC update path) stream via push(), so the measured
number is the DBSP circuits, not the parser. This is the flagship
"always-fresh incremental analytics" throughput number.
"""

from __future__ import annotations

from helpers import tpch
from helpers.datagen import TPCH_SIZES, build_batch, push_rows

_VIEW_DDLS = [
    # Q1 pricing summary (R4-staged: projection then aggregate).
    "CREATE VIEW q1_base AS SELECT l_rflag, l_lstatus, l_qty, l_price, "
    "l_price*(1-l_disc) AS disc_price, l_price*(1-l_disc)*(1+l_tax) AS charge, l_disc "
    "FROM lineitem WHERE l_ship <= 900",
    "CREATE VIEW q1 AS SELECT l_rflag, l_lstatus, SUM(l_qty) AS sum_qty, SUM(l_price) AS sum_base, "
    "SUM(disc_price) AS sum_disc, SUM(charge) AS sum_charge, AVG(l_qty) AS avg_qty, "
    "AVG(l_disc) AS avg_disc, COUNT(*) AS cnt FROM q1_base GROUP BY l_rflag, l_lstatus",
    # Q3 shipping priority (join-body CTE projects col-refs only, per R1).
    "CREATE VIEW q3_j AS WITH co AS (SELECT customer.c_key AS c_key, orders.o_key AS o_key, "
    "orders.o_date AS o_date FROM customer JOIN orders ON orders.o_cust = customer.c_key "
    "WHERE orders.o_status = 'O') SELECT co.o_key AS o_key, co.o_date AS o_date, "
    "lineitem.l_price AS l_price, lineitem.l_disc AS l_disc "
    "FROM co JOIN lineitem ON lineitem.l_order = co.o_key",
    "CREATE VIEW q3_rev AS SELECT o_key, o_date, l_price*(1-l_disc) AS rev FROM q3_j",
    "CREATE VIEW q3 AS SELECT o_key, o_date, SUM(rev) AS revenue FROM q3_rev GROUP BY o_key, o_date",
    # Q6 forecasting revenue (filter + projection then global SUM).
    "CREATE VIEW q6_base AS SELECT l_price*l_disc AS rev FROM lineitem "
    "WHERE l_ship >= 100 AND l_ship < 400 AND l_disc BETWEEN 0.05 AND 0.07 AND l_qty < 24",
    "CREATE VIEW q6 AS SELECT SUM(rev) AS revenue FROM q6_base",
    # Q12 shipping modes (join col-refs only; CASE in a downstream projection view, per R1).
    "CREATE VIEW q12_j AS SELECT lineitem.l_mode AS l_mode, orders.o_status AS o_status "
    "FROM orders JOIN lineitem ON lineitem.l_order = orders.o_key "
    "WHERE lineitem.l_mode IN ('MAIL','SHIP')",
    "CREATE VIEW q12_p AS SELECT l_mode, CASE WHEN o_status='O' THEN 1 ELSE 0 END AS hi, "
    "CASE WHEN o_status='O' THEN 0 ELSE 1 END AS lo FROM q12_j",
    "CREATE VIEW q12 AS SELECT l_mode, SUM(hi) AS high_line, SUM(lo) AS low_line "
    "FROM q12_p GROUP BY l_mode",
    # Q4-style (correlated EXISTS filter view, no outer GROUP BY/JOIN; group downstream).
    "CREATE VIEW q4_x AS SELECT orders.o_key AS o_key, orders.o_status AS o_status "
    "FROM orders WHERE EXISTS (SELECT 1 FROM lineitem WHERE lineitem.l_order = orders.o_key)",
    "CREATE VIEW q4 AS SELECT o_status, COUNT(*) AS order_count FROM q4_x GROUP BY o_status",
]

_READ_VIEWS = ["q1", "q3", "q6", "q12", "q4"]
_CHUNK = 250_000


def _bulk(client, sn, name, rows):
    tid, schema = client.resolve_table(sn, name)
    for i in range(0, len(rows), _CHUNK):
        push_rows(client, tid, schema, rows[i:i + _CHUNK])


def test_tpch(client, schema_name, bench_timer, scale_mode):
    sn = schema_name
    sz = TPCH_SIZES[scale_mode]
    tpch.create_tables(client, sn)
    for ddl in _VIEW_DDLS:
        client.execute_sql(ddl, schema_name=sn)

    data = tpch.generate_all(sz["sf"], skew=True)
    for name in ("region", "nation", "customer", "orders", "lineitem"):
        _bulk(client, sn, name, data[name])

    orders = data["orders"]
    li_tid, li_schema = client.resolve_table(sn, "lineitem")
    delta, iters = sz["delta"], sz["iters"]
    prev = None
    for it in range(iters):
        line_base = 2_000_000 + it * (delta + 1)
        rows = tpch.generate_incremental(orders, delta, seed=1000 + it,
                                         line_base=line_base, skew=True)
        # Build the batch outside measure() so only the push is timed.
        ins = build_batch(li_schema, rows)
        bench_timer.measure(client.push, li_tid, ins, rows_per_call=delta)
        if prev is not None:
            ret = build_batch(li_schema, prev, -1)
            bench_timer.measure(client.push, li_tid, ret, rows_per_call=len(prev))
        prev = rows

    for v in _READ_VIEWS:
        vid, _ = client.resolve_table(sn, v)
        assert len(client.scan(vid)) > 0, f"{v} empty after streaming"
