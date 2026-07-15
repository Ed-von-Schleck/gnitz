"""TPC-H-subset generator with realistic column types and skew.

The expressible subset (no LIKE/CAST/DECIMAL/DATE): money is integer cents
(BIGINT), rates (l_disc, l_tax) are DOUBLE, dates are integer day-ordinals
(BIGINT), flags/status/mode are short TEXT (<=12 chars, inline German strings).
Skew: o_cust (hot customers) and lineitem's l_order FK are drawn Zipfian.

Tables (rows scale with SF): region(5), nation(25), customer(~150*SF),
orders(~1500*SF), lineitem(~5000*SF, compound PK (l_order, l_line)).

Rows are dicts keyed by column name so a test can `batch.append(**row)`.
Independent of crates/gnitz-py/tests/tpch_gen.py (that one feeds circuit-builder
correctness tests and lacks these columns/skew).
"""

from __future__ import annotations

import random

from helpers.datagen import NAME, NATIONS, SHIPMODES, STATUS, zipf_choice

REGIONS = ["AMERICA", "ASIA", "EUROPE", "AFRICA", "MIDEAST"]

RFLAGS = ["A", "N", "R"]
LSTATUS = ["O", "F"]

# CREATE TABLE DDL for the five base tables (compound PK on lineitem needs SQL).
TABLE_DDL = {
    "region": "CREATE TABLE region (r_key BIGINT NOT NULL PRIMARY KEY, r_name TEXT NOT NULL)",
    "nation": ("CREATE TABLE nation (n_key BIGINT NOT NULL PRIMARY KEY, "
               "n_name TEXT NOT NULL, n_region BIGINT NOT NULL)"),
    "customer": ("CREATE TABLE customer (c_key BIGINT NOT NULL PRIMARY KEY, "
                 "c_name TEXT NOT NULL, c_nation BIGINT NOT NULL, c_acctbal BIGINT NOT NULL)"),
    "orders": ("CREATE TABLE orders (o_key BIGINT NOT NULL PRIMARY KEY, "
               "o_cust BIGINT NOT NULL, o_status TEXT NOT NULL, o_date BIGINT NOT NULL, "
               "o_price BIGINT NOT NULL)"),
    "lineitem": ("CREATE TABLE lineitem (l_order BIGINT NOT NULL, l_line BIGINT NOT NULL, "
                 "l_qty BIGINT NOT NULL, l_price BIGINT NOT NULL, l_disc DOUBLE NOT NULL, "
                 "l_tax DOUBLE NOT NULL, l_ship BIGINT NOT NULL, l_rflag TEXT NOT NULL, "
                 "l_lstatus TEXT NOT NULL, l_mode TEXT NOT NULL, PRIMARY KEY (l_order, l_line))"),
}


def create_tables(client, sn: str) -> None:
    """Create region/nation/customer/orders/lineitem in schema `sn`."""
    for name in ("region", "nation", "customer", "orders", "lineitem"):
        client.execute_sql(TABLE_DDL[name], schema_name=sn)


def generate_region():
    return [{"r_key": i, "r_name": REGIONS[i]} for i in range(len(REGIONS))]


def generate_nation():
    return [
        {"n_key": i, "n_name": NATIONS[i], "n_region": i % len(REGIONS)}
        for i in range(len(NATIONS))
    ]


def generate_customers(sf, seed=42):
    rng = random.Random(seed)
    n = max(1, round(150 * sf))
    return [
        {"c_key": i, "c_name": NAME("Cust", i), "c_nation": rng.randint(0, 24),
         "c_acctbal": rng.randint(-99999, 999999)}
        for i in range(1, n + 1)
    ]


def generate_orders(customers, sf, seed=43, skew_s=1.1, skew=True):
    rng = random.Random(seed)
    n = max(1, round(1500 * sf))
    n_cust = len(customers)
    rows = []
    for i in range(1, n + 1):
        cidx = zipf_choice(rng, n_cust, skew_s) if skew else rng.randint(1, n_cust)
        rows.append({
            "o_key": i,
            "o_cust": customers[cidx - 1]["c_key"],
            "o_status": rng.choice(STATUS),
            "o_date": rng.randint(1, 2000),
            "o_price": rng.randint(1000, 500000),
        })
    return rows


def _lineitem_row(rng, okey, lline):
    return {
        "l_order": okey,
        "l_line": lline,
        "l_qty": rng.randint(1, 50),
        "l_price": rng.randint(100, 100000),
        "l_disc": round(rng.uniform(0.0, 0.10), 2),
        "l_tax": round(rng.uniform(0.0, 0.08), 2),
        "l_ship": rng.randint(1, 1000),
        "l_rflag": rng.choice(RFLAGS),
        "l_lstatus": rng.choice(LSTATUS),
        "l_mode": rng.choice(SHIPMODES),
    }


def generate_lineitem(orders, sf, seed=44, skew_s=1.1, skew=True):
    """~5000*SF lineitems; l_order drawn Zipfian over orders (hot orders get more
    lines). (l_order, l_line) is unique via a per-order line counter."""
    rng = random.Random(seed)
    target = max(1, round(5000 * sf))
    n_ord = len(orders)
    line_ctr: dict[int, int] = {}
    rows = []
    while len(rows) < target:
        oidx = zipf_choice(rng, n_ord, skew_s) if skew else rng.randint(1, n_ord)
        okey = orders[oidx - 1]["o_key"]
        lline = line_ctr.get(okey, 0) + 1
        line_ctr[okey] = lline
        rows.append(_lineitem_row(rng, okey, lline))
    return rows


def generate_all(sf=1.0, seed=42, skew=True):
    """All five tables. Returns {table_name: [row_dict, ...]}."""
    region = generate_region()
    nation = generate_nation()
    customers = generate_customers(sf, seed)
    orders = generate_orders(customers, sf, seed + 1, skew=skew)
    lineitem = generate_lineitem(orders, sf, seed + 2, skew=skew)
    return {
        "region": region,
        "nation": nation,
        "customer": customers,
        "orders": orders,
        "lineitem": lineitem,
    }


def generate_incremental(orders, n, seed, line_base, skew=True, skew_s=1.1):
    """`n` fresh lineitem rows referencing EXISTING orders (Zipfian if skew), with
    l_line = line_base + i so the compound PK never collides with the base load
    or with an earlier streaming batch (pass a distinct line_base per iter)."""
    rng = random.Random(seed)
    n_ord = len(orders)
    rows = []
    for i in range(n):
        oidx = zipf_choice(rng, n_ord, skew_s) if skew else rng.randint(1, n_ord)
        okey = orders[oidx - 1]["o_key"]
        rows.append(_lineitem_row(rng, okey, line_base + i))
    return rows
