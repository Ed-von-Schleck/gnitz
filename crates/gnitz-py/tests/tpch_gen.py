"""Minimal TPC-H-like data generator for incrementality benchmarks.

Generates deterministic data for 4 tables:
  - nation:   25 rows (fixed)
  - customer: ~150 rows per SF
  - orders:   ~1500 rows per SF
  - lineitem: ~5000 rows per SF

All numeric types mapped to U64 (keys) or I64 (values).
Strings kept short (<12 chars to stay inline in German Strings).
"""

import random


# Fixed nation data (25 rows, matches TPC-H spec)
NATION_DATA = [
    (0, "ALGERIA", 0), (1, "ARGENTINA", 1), (2, "BRAZIL", 1),
    (3, "CANADA", 1), (4, "EGYPT", 4), (5, "ETHIOPIA", 0),
    (6, "FRANCE", 3), (7, "GERMANY", 3), (8, "INDIA", 2),
    (9, "INDONESIA", 2), (10, "IRAN", 4), (11, "IRAQ", 4),
    (12, "JAPAN", 2), (13, "JORDAN", 4), (14, "KENYA", 0),
    (15, "MOROCCO", 0), (16, "MOZAMBIQUE", 0), (17, "PERU", 1),
    (18, "CHINA", 2), (19, "ROMANIA", 3), (20, "S_ARABIA", 4),
    (21, "VIETNAM", 2), (22, "RUSSIA", 3), (23, "UK", 3),
    (24, "USA", 1),
]


def generate_nation():
    """Returns [(nationkey, name, regionkey), ...]."""
    return list(NATION_DATA)


def generate_customers(sf=1.0, seed=42):
    """Returns [(custkey, name, nationkey), ...]."""
    rng = random.Random(seed)
    n = int(150 * sf)
    rows = []
    for i in range(1, n + 1):
        name = f"Cust#{i}"
        nationkey = rng.randint(0, 24)
        rows.append((i, name, nationkey))
    return rows


def generate_orders(customers, sf=1.0, seed=43):
    """Returns [(orderkey, custkey, totalprice_cents, status), ...].

    Each customer gets ~10 orders. orderkey is sequential.
    """
    rng = random.Random(seed)
    n = int(1500 * sf)
    rows = []
    custkeys = [c[0] for c in customers]
    for i in range(1, n + 1):
        custkey = rng.choice(custkeys)
        totalprice = rng.randint(1000, 500000)  # cents
        status = rng.choice(["F", "O", "P"])
        rows.append((i, custkey, totalprice, status))
    return rows


def generate_lineitem(orders, sf=1.0, seed=44):
    """Returns [(pk, orderkey, quantity, price_cents), ...].

    pk = orderkey * 10 + linenumber (max 7 lines per order).
    """
    rng = random.Random(seed)
    n_target = int(5000 * sf)
    rows = []
    orderkeys = [o[0] for o in orders]
    count = 0
    while count < n_target:
        okey = rng.choice(orderkeys)
        n_lines = rng.randint(1, 7)
        for lnum in range(1, n_lines + 1):
            pk = okey * 10 + lnum
            qty = rng.randint(1, 50)
            price = rng.randint(100, 100000)  # cents
            rows.append((pk, okey, qty, price))
            count += 1
            if count >= n_target:
                break
    return rows


def generate_incremental_lineitem(orders, n_rows, seed=9999):
    """Generate fresh lineitem rows that don't collide with bulk data.

    Uses high orderkey range (> max existing) to avoid PK collisions.
    """
    rng = random.Random(seed)
    max_okey = max(o[0] for o in orders)
    rows = []
    for i in range(n_rows):
        okey = rng.choice([o[0] for o in orders])
        pk = (max_okey + 1000 + i) * 10 + 1  # guaranteed unique PK
        qty = rng.randint(1, 50)
        price = rng.randint(100, 100000)
        rows.append((pk, okey, qty, price))
    return rows


def generate_all(sf=1.0):
    """Generate all 4 tables. Returns dict of table_name -> rows."""
    nations = generate_nation()
    customers = generate_customers(sf)
    orders = generate_orders(customers, sf)
    lineitem = generate_lineitem(orders, sf)
    return {
        'nation': nations,
        'customer': customers,
        'orders': orders,
        'lineitem': lineitem,
    }
