"""A chain segment's rows are held only while its chain is built.

An aggregate over a join runs as two circuits: a hidden segment computes the
join, and the user's view aggregates the segment's output. The view's backfill
is the one reader of the segment's rows — the segment's own circuit keeps what it
needs of them in its traces, and a tick hands its delta to the view directly — so
they are dropped when the build ends, and a boot that keeps the chain keeps them
dropped.

The risk is a rebuild that reads rows no longer there, and it shows as a view
that comes back empty or short. Every test reads the view's Z-set against the
value recomputed from the rows pushed.
"""

import gnitz
from _read import bag, scanned
from _serverproc import disk_usage
from _sql import values

_REGIONS = ["north", "south", "east"]


def _orders(keys):
    return [(k, k % 7 + 1, k * 10) for k in keys]


def _build(conn, orders):
    """Rows before the view, so the view is built by a backfill through its
    segment. Answers the view's id."""
    customers = [(c, _REGIONS[c % 3]) for c in range(1, 8)]
    conn.execute_sql(
        "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, region TEXT NOT NULL); "
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NOT NULL, "
        "amount BIGINT NOT NULL); "
        f"INSERT INTO customers VALUES {values(customers)}; "
        f"INSERT INTO orders VALUES {values(orders)}; "
        "CREATE VIEW by_region AS SELECT c.region, COUNT(*) AS n, SUM(o.amount) AS total "
        "FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY c.region")
    return conn.resolve_table("by_region")[0]


def _want(orders, moved=()):
    """The view over `orders`, the customers of `moved` being in `north`."""
    out = {}
    for _, customer, amount in orders:
        region = "north" if customer in moved else _REGIONS[customer % 3]
        n, total = out.get(region, (0, 0))
        out[region] = (n + 1, total + amount)
    return {(region, n, total): 1 for region, (n, total) in out.items()}


def _assert_view(srv, orders, ctx, moved=()):
    with gnitz.connect(srv.target) as conn:
        got = bag(scanned(conn, "by_region"), "region", "n", "total")
    want = _want(orders, moved)
    assert got == want, f"{ctx}: the view is {got}, want {want}"


def _segment_stores(srv, view):
    """`{store: rows}` of `view`'s one segment, off the files of a stopped
    server. The planner allocates a chain's ids consecutively, its user view last."""
    out = {}
    for s in disk_usage(srv.data_dir)[1]:
        if s["relation"] == view - 1:
            out[s["store"]] = out.get(s["store"], 0) + s["rows"]
    return out


def test_a_built_chain_holds_no_segment_rows_and_keeps_ticking(own_server):
    """The checkpoint of a built chain holds the segment's traces and none of its
    rows; a boot that resumes the chain goes on ticking it from both sources,
    retractions included."""
    orders = _orders(range(1, 41))
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        view = _build(conn, orders)
    _assert_view(own_server, orders, "built by a backfill")
    own_server.stop_graceful()

    stores = _segment_stores(own_server, view)
    traces = {store: rows for store, rows in stores.items() if store != "rows"}
    assert any(traces.values()), f"the segment holds no trace at all: {stores}"
    assert stores.get("rows", 0) == 0, f"a built chain's segment still holds its rows: {stores}"

    own_server.start()
    assert own_server.rebuilt_view_count() == 0, "a built chain must resume"
    more = _orders(range(41, 61))
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(f"INSERT INTO orders VALUES {values(more)}")
        conn.execute_sql("DELETE FROM orders WHERE id <= 10")
        # A customer moving region retracts and re-adds every order it has.
        conn.execute_sql("UPDATE customers SET region = 'north' WHERE id = 2")
    kept = [o for o in orders + more if o[0] > 10]
    _assert_view(own_server, kept, "a resumed chain must tick from both sources", moved={2})


def test_a_rebuilt_chain_rebuilds_its_segment(own_server):
    """A crash before any checkpoint of the chain rebuilds it whole: the view's
    backfill reads segment rows the same boot put back."""
    orders = _orders(range(1, 41))
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        view = _build(conn, orders)
    own_server.restart()
    assert own_server.rebuilt_view_count() == 2, "the segment and its view rebuild together"
    _assert_view(own_server, orders, "rebuilt after a crash")

    more = _orders(range(41, 61))
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(f"INSERT INTO orders VALUES {values(more)}")
    _assert_view(own_server, orders + more, "a rebuilt chain must keep ticking")
    own_server.stop_graceful()
    assert _segment_stores(own_server, view).get("rows", 0) == 0, "a rebuilt chain's segment still holds its rows"


def test_a_changed_worker_count_rebuilds_a_built_chain(own_server):
    """A relayout rebuilds every view. The chain was checkpointed with its
    segment's rows already dropped, so the rebuild has to refill them."""
    orders = _orders(range(1, 41))
    own_server.start(workers=4)
    with gnitz.connect(own_server.target) as conn:
        _build(conn, orders)
    own_server.restart(graceful=True, workers=2)
    assert own_server.rebuilt_view_count() == 2
    _assert_view(own_server, orders, "rebuilt at another worker count")
