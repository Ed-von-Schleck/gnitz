"""A join whose trace is its source table holds the same Z-set as one that
keeps a copy.

An equi join keeps, per side, the integral of that side's rows re-keyed on the
join key. Where the re-key is the table's own primary key — or leading columns
of it that also place its rows — the table's store already is that integral, and
the join reads it instead of integrating a second copy. What the join must then
get right is *which* state of the table it reads: the one the view last absorbed,
not the one a push has since moved it to.

Each layout below makes a different set of traces readable off their tables.
One model — two dicts and a nested loop — states the view after every step of a
random history that leaves pushes to both tables un-ticked together, restarts
the server cleanly, kills it with pushes still un-ticked, and creates a second
view over the populated tables, which is the backfill.

The comparison is a weight bag, and the premise — which traces exist on disk —
is read off the data directory.
"""

import random

import gnitz
import pytest
from _read import bag, scanned
from _serverproc import MULTI, disk_usage

_CUSTOMERS = "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL, address TEXT){}"
_ORDERS = "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NOT NULL, note TEXT NOT NULL)"
_ORDERS_CLUSTERED = (
    "CREATE TABLE orders (customer_id BIGINT NOT NULL, id BIGINT NOT NULL, note TEXT NOT NULL, "
    "PRIMARY KEY (customer_id, id)) CLUSTER BY customer_id"
)
_VIEW = (
    "CREATE VIEW {name} {opts} AS SELECT o.id AS oid, o.note AS note, c.id AS cid, c.name AS name, "
    "c.address AS address FROM orders o JOIN customers c ON o.customer_id = c.id"
)
_COLS = ("oid", "note", "cid", "name", "address")

# layout -> (customers suffix, orders DDL, the traces the view still keeps a copy of)
_LAYOUTS = {
    # `customers` is joined on its primary key; `orders` is re-keyed onto another worker.
    "by_id": ("", _ORDERS, 1),
    # `orders` leads with the join key and is placed by it: both tables are their traces.
    "clustered": ("", _ORDERS_CLUSTERED, 0),
    # Every worker holds `customers` whole; `orders` stays where it is, in id order.
    "replicated": (" WITH (replicated = true)", _ORDERS, 1),
}


class _Model:
    def __init__(self, seed):
        self.rng = random.Random(seed)
        self.customers = {}
        self.orders = {}
        self.next_order = 1

    def want(self):
        return {
            (oid, note, cid, *self.customers[cid]): 1
            for oid, (cid, note) in self.orders.items()
            if cid in self.customers
        }

    def push_customers(self, conn, n):
        tid, schema = conn.resolve_table("customers")
        batch = gnitz.ZSetBatch(schema)
        for cid in self.rng.sample(range(1, 61), n):
            name = f"customer {cid}, revision {self.rng.randrange(4)} of a name long enough for the heap"
            address = None if self.rng.random() < 0.2 else f"{self.rng.randrange(5)} Example Street"
            self.customers[cid] = (name, address)
            batch.append(id=cid, name=name, address=address)
        conn.push(tid, batch)

    def push_orders(self, conn, n):
        """New orders, and existing ones re-noted or moved to another customer.
        A move is a delete and an insert, since the key may carry the customer."""
        tid, schema = conn.resolve_table("orders")
        touched = {}
        for _ in range(n):
            reuse = self.orders and self.rng.random() < 0.4
            oid = self.rng.choice(sorted(self.orders)) if reuse else self.next_order
            self.next_order += not reuse
            cid = self.rng.randrange(1, 71)  # 61..70 never exist
            note = f"order {oid} note, revision {self.rng.randrange(3)}, padded past the inline width"
            if oid in self.orders and self.orders[oid][0] != cid:
                conn.execute_sql(f"DELETE FROM orders WHERE id = {oid}")
            self.orders[oid] = touched[oid] = (cid, note)
        batch = gnitz.ZSetBatch(schema)
        for oid, (cid, note) in touched.items():
            batch.append(id=oid, customer_id=cid, note=note)
        conn.push(tid, batch)

    def delete(self, conn, table, rows):
        for key in self.rng.sample(sorted(rows), min(3, len(rows))):
            del rows[key]
            conn.execute_sql(f"DELETE FROM {table} WHERE id = {key}")

    def step(self, conn):
        """A few writes to both tables with no read between them, so one tick
        carries both tables' deltas."""
        for _ in range(self.rng.randrange(1, 4)):
            match self.rng.randrange(6):
                case 0 | 1:
                    self.push_customers(conn, self.rng.randrange(1, 12))
                case 2 | 3:
                    self.push_orders(conn, self.rng.randrange(1, 25))
                case 4:
                    self.delete(conn, "customers", self.customers)
                case 5:
                    self.delete(conn, "orders", self.orders)


def _check(conn, model, what, views=("v",)):
    for view in views:
        assert bag(scanned(conn, view), *_COLS) == dict(sorted(model.want().items(), key=repr)), f"{view}: {what}"


@pytest.mark.parametrize("opts", ["", "WITH (capacity = '8 KB')"], ids=["stored", "bounded"])
@pytest.mark.parametrize("layout", list(_LAYOUTS))
def test_the_view_is_the_join_of_its_tables(layout, opts, own_server):
    customers_opts, orders_ddl, kept_traces = _LAYOUTS[layout]
    # A RAM tier of a few KiB: every store spills, so the tables are read off
    # shards and a bounded view is swept down to skeleton rows.
    own_server.extra_env = {"GNITZ_RAM_TIER_BYTES": "4096"}
    own_server.start(workers=MULTI)
    model = _Model(seed=sum(f"{layout}{opts}".encode()))
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(_CUSTOMERS.format(customers_opts))
        conn.execute_sql(orders_ddl)
        conn.execute_sql(_VIEW.format(name="v", opts=opts))
        for i in range(25):
            model.step(conn)
            if i % 3 == 0:
                _check(conn, model, f"step {i}")
        _check(conn, model, "before the restart")
        assert model.want(), "premise: the history leaves matched rows"

    own_server.restart(graceful=True)
    _, stores = disk_usage(own_server.data_dir)
    view_id = max(s["relation"] for s in stores)
    traces = {s["store"] for s in stores if s["relation"] == view_id and s["store"].startswith("scratch")}
    assert len(traces) == kept_traces, f"{layout}: the view keeps {traces}"
    with gnitz.connect(own_server.target) as conn:
        _check(conn, model, "resumed")
        for _ in range(8):
            model.step(conn)
        _check(conn, model, "after the resume")
        # Killed with writes to both tables un-ticked: the boot replays them into
        # the tables, and the view must still absorb each exactly once.
        for _ in range(4):
            model.step(conn)

    own_server.restart()
    with gnitz.connect(own_server.target) as conn:
        _check(conn, model, "after the crash")
        for _ in range(6):
            model.step(conn)
        _check(conn, model, "after the crash and more writes")
        # A view created over the populated tables is filled by a backfill of
        # each table in turn.
        conn.execute_sql(_VIEW.format(name="w", opts=""))
        _check(conn, model, "a second view over the populated tables", views=("v", "w"))
        for _ in range(4):
            model.step(conn)
        _check(conn, model, "both views after more writes", views=("v", "w"))
