"""TPC-H-inspired incrementality benchmarks.

Verifies the core DBSP property: view update cost scales with delta size,
not total data size. Uses a small TPC-H-like dataset (SF=0.5 ~ 2500 lineitem
rows bulk-loaded) and measures push latency for incremental batches.

Each test:
1. Creates tables and bulk-loads data
2. Creates a view via CircuitBuilder
3. Pushes incremental batches of varying sizes
4. Asserts correctness AND timing proportionality

Tables use realistic TPC-H schemas including STRING columns to exercise
the full type system through join and reduce pipelines.
"""

import time
import random
import pytest

from gnitz_client import TypeCode, ColumnDef, Schema
from gnitz_client.batch import ZSetBatch
from gnitz_client.circuit import CircuitBuilder
from gnitz_client.expr import ExprBuilder
from tests.tpch_gen import generate_all, generate_incremental_lineitem


SF = 0.5  # ~2500 lineitem, ~750 orders, ~75 customers


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _uid():
    return str(random.randint(100000, 999999))


# Table schemas (mirroring simplified TPC-H with STRING columns)
LINEITEM_COLS = [
    ColumnDef("pk", TypeCode.U64),
    ColumnDef("orderkey", TypeCode.I64),
    ColumnDef("quantity", TypeCode.I64),
    ColumnDef("price", TypeCode.I64),
]
LINEITEM_SCHEMA = Schema(columns=LINEITEM_COLS, pk_index=0)

ORDERS_COLS = [
    ColumnDef("orderkey", TypeCode.U64),
    ColumnDef("custkey", TypeCode.I64),
    ColumnDef("totalprice", TypeCode.I64),
    ColumnDef("status", TypeCode.STRING),
]
ORDERS_SCHEMA = Schema(columns=ORDERS_COLS, pk_index=0)

# Aggregated lineitem by orderkey (used as delta source for join queries)
LI_AGG_COLS = [
    ColumnDef("orderkey", TypeCode.U64),
    ColumnDef("total_price", TypeCode.I64),
]
LI_AGG_SCHEMA = Schema(columns=LI_AGG_COLS, pk_index=0)


def _push_lineitem_rows(client, tid, rows, weight=1):
    """Push [(pk, orderkey, quantity, price), ...] with given weight."""
    batch = ZSetBatch(schema=LINEITEM_SCHEMA)
    for pk, okey, qty, price in rows:
        batch.pk_lo.append(pk)
        batch.pk_hi.append(0)
        batch.weights.append(weight)
        batch.nulls.append(0)
    batch.columns = [
        [],
        [r[1] for r in rows],
        [r[2] for r in rows],
        [r[3] for r in rows],
    ]
    client.push(tid, LINEITEM_SCHEMA, batch)


def _push_orders_rows(client, tid, rows):
    """Push [(orderkey, custkey, totalprice, status), ...] with weight=1."""
    batch = ZSetBatch(schema=ORDERS_SCHEMA)
    for okey, ckey, price, status in rows:
        batch.pk_lo.append(okey)
        batch.pk_hi.append(0)
        batch.weights.append(1)
        batch.nulls.append(0)
    batch.columns = [
        [],
        [r[1] for r in rows],
        [r[2] for r in rows],
        [r[3] for r in rows],
    ]
    client.push(tid, ORDERS_SCHEMA, batch)


def _push_li_agg_rows(client, tid, rows, weight=1):
    """Push [(orderkey, total_price), ...] with given weight."""
    batch = ZSetBatch(schema=LI_AGG_SCHEMA)
    for okey, total_price in rows:
        batch.pk_lo.append(okey)
        batch.pk_hi.append(0)
        batch.weights.append(weight)
        batch.nulls.append(0)
    batch.columns = [[], [r[1] for r in rows]]
    client.push(tid, LI_AGG_SCHEMA, batch)


def _scan_reduce_result(client, vid):
    """Scan a reduce view and return {group_col: agg_col} for positive-weight rows.

    Assumes reduce output schema: (pk, group_col, agg_col) — columns 1 and 2.
    """
    schema, batch = client.scan(vid)
    result = {}
    if batch:
        for i in range(len(batch.pk_lo)):
            if batch.weights[i] > 0:
                result[batch.columns[1][i]] = batch.columns[2][i]
    return result


def _build_and_create_view(client, sn, view_name, source_tid, builder_fn,
                            output_cols):
    vid = client.allocate_table_id()
    cb = CircuitBuilder(vid, source_tid)
    builder_fn(cb, vid)
    graph = cb.build()
    client.create_view_with_circuit(sn, view_name, graph, output_cols)
    return vid


def _timed_push_and_scan(client, tid, vid, rows):
    """Push lineitem rows and scan view, return elapsed seconds."""
    t0 = time.perf_counter()
    _push_lineitem_rows(client, tid, rows)
    client.scan(vid)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sn(client):
    name = "tpch_" + _uid()
    client.create_schema(name)
    return name


@pytest.fixture(scope="module")
def tpch_data():
    """Generate TPC-H data once per module (deterministic from SF)."""
    return generate_all(sf=SF)


def _aggregate_lineitem_by_orderkey(lineitem_data):
    """Pre-aggregate lineitem prices by orderkey. Returns [(okey, total), ...]."""
    agg = {}
    for pk, okey, qty, price in lineitem_data:
        agg[okey] = agg.get(okey, 0) + price
    return list(agg.items())


# ---------------------------------------------------------------------------
# Q1: Filter + Reduce (lineitem -> filter price>5000 -> SUM(qty) by orderkey)
# ---------------------------------------------------------------------------

class TestQ1FilterReduce:
    """Incrementality test: filter + reduce pipeline on lineitem."""

    def _setup(self, client, sn, tpch_data):
        """Create lineitem table, load bulk data, create view."""
        tid = client.create_table(sn, "lineitem_q1_" + _uid(), LINEITEM_COLS, pk_col_idx=0)

        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("orderkey", TypeCode.I64),
            ColumnDef("sum_qty", TypeCode.I64),
        ]

        eb = ExprBuilder()
        r0 = eb.load_col_int(3)  # price is payload col 3
        r1 = eb.load_const(5000)
        r2 = eb.cmp_gt(r0, r1)
        expr = eb.build(r2)

        def build(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp, expr=expr)
            r = cb.reduce(f, group_by_cols=[1], agg_func_id=2, agg_col_idx=2)
            cb.sink(r, vid)

        vid = _build_and_create_view(client, sn, "v_q1_" + _uid(), tid, build, out_cols)
        _push_lineitem_rows(client, tid, tpch_data['lineitem'])

        return tid, vid, tpch_data['orders']

    def test_correctness(self, client, sn, tpch_data):
        """Verify reduce output matches expected aggregation."""
        tid, vid, orders = self._setup(client, sn, tpch_data)

        expected = {}
        for pk, okey, qty, price in tpch_data['lineitem']:
            if price > 5000:
                expected[okey] = expected.get(okey, 0) + qty

        actual = _scan_reduce_result(client, vid)
        assert actual == expected

    def test_incremental_correctness(self, client, sn, tpch_data):
        """Push a small delta and verify the aggregate updates correctly."""
        tid, vid, orders = self._setup(client, sn, tpch_data)

        before = _scan_reduce_result(client, vid)

        new_rows = generate_incremental_lineitem(tpch_data['orders'], 10, seed=7777)
        new_rows = [(pk, okey, qty, 10000) for pk, okey, qty, _ in new_rows]
        _push_lineitem_rows(client, tid, new_rows)

        after = _scan_reduce_result(client, vid)

        for pk, okey, qty, price in new_rows:
            old_sum = before.get(okey, 0)
            assert after.get(okey, 0) == old_sum + qty, (
                f"orderkey={okey}: expected {old_sum + qty}, got {after.get(okey, 0)}"
            )

    def test_retraction(self, client, sn, tpch_data):
        """Delete rows via weight=-1 and verify aggregate decreases."""
        tid, vid, orders = self._setup(client, sn, tpch_data)

        before = _scan_reduce_result(client, vid)

        # Find rows that passed the filter (price > 5000) and pick a group
        # with multiple contributing rows so the group survives retraction.
        by_okey = {}
        for pk, okey, qty, price in tpch_data['lineitem']:
            if price > 5000:
                by_okey.setdefault(okey, []).append((pk, okey, qty, price))

        # Pick a group with >= 2 filtered rows
        target_okey = None
        for okey, rows in by_okey.items():
            if len(rows) >= 2:
                target_okey = okey
                break
        assert target_okey is not None, "Need a group with >= 2 filtered rows"

        retract_row = by_okey[target_okey][0]
        _push_lineitem_rows(client, tid, [retract_row], weight=-1)

        after = _scan_reduce_result(client, vid)
        expected_sum = before[target_okey] - retract_row[2]  # subtract qty
        assert after[target_okey] == expected_sum, (
            f"orderkey={target_okey}: expected {expected_sum}, got {after.get(target_okey)}"
        )

    def test_timing_proportionality(self, client, sn, tpch_data):
        """Push varying batch sizes and verify sub-linear scaling."""
        tid, vid, orders = self._setup(client, sn, tpch_data)

        warmup = generate_incremental_lineitem(orders, 5, seed=1000)
        warmup = [(pk, okey, qty, 10000) for pk, okey, qty, _ in warmup]
        _timed_push_and_scan(client, tid, vid, warmup)

        timings = {}
        for i, n in enumerate([1, 10, 50]):
            rows = generate_incremental_lineitem(orders, n, seed=2000 + i)
            rows = [(pk + i * 100000, okey, qty, 10000)
                    for pk, okey, qty, _ in rows]
            timings[n] = _timed_push_and_scan(client, tid, vid, rows)

        ratio = timings[50] / max(timings[1], 1e-9)
        assert ratio < 30, (
            f"Non-incremental! push(50)/push(1) = {ratio:.1f}x "
            f"(timings: {timings})"
        )


# ---------------------------------------------------------------------------
# Q2: Join + Reduce with STRING columns
# (li_agg JOIN orders(STRING status) -> SUM(price) by custkey)
# ---------------------------------------------------------------------------

class TestQ2JoinReduce:
    """Incrementality test: join + reduce pipeline.

    The orders trace table includes a STRING 'status' column, so the join
    output has STRING flowing into reduce. This exercises the full type
    system through the join->reduce pipeline.
    """

    def _setup(self, client, sn, tpch_data):
        """Create tables, load bulk, create view.

        Returns (li_agg_tid, li_agg_schema, vid, tpch_data).
        """
        # Delta source: aggregated lineitem (pk=orderkey, total_price)
        li_tid = client.create_table(sn, "li_q2_" + _uid(), LI_AGG_COLS, pk_col_idx=0)

        # Trace: orders with STRING status column
        ord_tid = client.create_table(sn, "ord_q2_" + _uid(), ORDERS_COLS, pk_col_idx=0)
        _push_orders_rows(client, ord_tid, tpch_data['orders'])

        # Join output: (orderkey:U64, total_price:I64,
        #               custkey:I64, totalprice:I64, status:STRING)
        # Reduce: group by col 2 (custkey), SUM col 1 (li total_price)
        # STRING at col 4 passes through the join but is NOT in reduce output.
        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("custkey", TypeCode.I64),
            ColumnDef("sum_price", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, ord_tid)
            r = cb.reduce(j, group_by_cols=[2], agg_func_id=2, agg_col_idx=1)
            cb.sink(r, vid)

        # Create view BEFORE pushing delta data
        vid = _build_and_create_view(
            client, sn, "v_q2_" + _uid(), li_tid, build, out_cols
        )

        # Bulk-load aggregated lineitem
        agg_rows = _aggregate_lineitem_by_orderkey(tpch_data['lineitem'])
        _push_li_agg_rows(client, li_tid, agg_rows)

        return li_tid, LI_AGG_SCHEMA, vid, tpch_data

    def test_correctness(self, client, sn, tpch_data):
        """Verify join+reduce produces correct per-customer sums."""
        li_tid, li_schema, vid, data = self._setup(client, sn, tpch_data)

        order_to_cust = {o[0]: o[1] for o in data['orders']}
        li_agg = {}
        for pk, okey, qty, price in data['lineitem']:
            li_agg[okey] = li_agg.get(okey, 0) + price

        expected_by_cust = {}
        for okey, total_price in li_agg.items():
            ckey = order_to_cust.get(okey)
            if ckey is not None:
                expected_by_cust[ckey] = expected_by_cust.get(ckey, 0) + total_price

        actual = _scan_reduce_result(client, vid)
        assert actual == expected_by_cust

    def test_incremental_push(self, client, sn, tpch_data):
        """Push new aggregated lineitem and verify customer sum updates."""
        li_tid, li_schema, vid, data = self._setup(client, sn, tpch_data)

        before = _scan_reduce_result(client, vid)

        # Push for an existing orderkey (adds weight, join sees delta)
        okey = data['orders'][0][0]
        ckey = data['orders'][0][1]
        new_price = 42000
        _push_li_agg_rows(client, li_tid, [(okey, new_price)])

        after = _scan_reduce_result(client, vid)
        assert after.get(ckey, 0) == before.get(ckey, 0) + new_price

    def test_retraction(self, client, sn, tpch_data):
        """Delete a li_agg row and verify customer sum decreases through join+reduce."""
        li_tid, li_schema, vid, data = self._setup(client, sn, tpch_data)

        before = _scan_reduce_result(client, vid)

        # Pick an orderkey whose customer has multiple orders contributing,
        # so the customer group survives the retraction.
        order_to_cust = {o[0]: o[1] for o in data['orders']}
        li_agg = _aggregate_lineitem_by_orderkey(data['lineitem'])

        # Count orders per customer to find one with >= 2
        cust_orders = {}
        for okey, price in li_agg:
            ckey = order_to_cust.get(okey)
            if ckey is not None:
                cust_orders.setdefault(ckey, []).append((okey, price))

        target_ckey = None
        for ckey, olist in cust_orders.items():
            if len(olist) >= 2:
                target_ckey = ckey
                break
        assert target_ckey is not None, "Need a customer with >= 2 orders"

        retract_okey, retract_price = cust_orders[target_ckey][0]
        _push_li_agg_rows(client, li_tid, [(retract_okey, retract_price)], weight=-1)

        after = _scan_reduce_result(client, vid)
        expected_sum = before[target_ckey] - retract_price
        assert after[target_ckey] == expected_sum, (
            f"custkey={target_ckey}: expected {expected_sum}, got {after.get(target_ckey)}"
        )

    def test_timing_proportionality(self, client, sn, tpch_data):
        """Verify incremental push scales with delta, not total data."""
        li_tid, li_schema, vid, data = self._setup(client, sn, tpch_data)

        def push_n(n, offset):
            rows = [(900000 + offset + i, 1000) for i in range(n)]
            t0 = time.perf_counter()
            _push_li_agg_rows(client, li_tid, rows)
            client.scan(vid)
            return time.perf_counter() - t0

        push_n(5, 0)  # warm up

        timings = {}
        for i, n in enumerate([1, 10, 50]):
            timings[n] = push_n(n, 10000 + i * 1000)

        ratio = timings[50] / max(timings[1], 1e-9)
        assert ratio < 30, (
            f"Non-incremental! push(50)/push(1) = {ratio:.1f}x "
            f"(timings: {timings})"
        )


# ---------------------------------------------------------------------------
# Q3: Join with STRING trace tables -> reduce
# (li_agg JOIN orders(STRING) -> reduce COUNT by custkey)
# ---------------------------------------------------------------------------

class TestQ3JoinChain:
    """Incrementality test: join with STRING-bearing trace, feeding reduce."""

    def _setup(self, client, sn, tpch_data):
        """Create tables with STRING columns, load bulk, build view."""
        # Delta source: aggregated lineitem
        a_tid = client.create_table(sn, "li_q3_" + _uid(), LI_AGG_COLS, pk_col_idx=0)

        # Trace: orders with STRING status
        b_tid = client.create_table(sn, "ord_q3_" + _uid(), ORDERS_COLS, pk_col_idx=0)
        _push_orders_rows(client, b_tid, tpch_data['orders'])

        # Join A(2 cols) x B(4 cols, incl STRING status):
        # output = (orderkey:U64, total_price:I64,
        #           custkey:I64, totalprice:I64, status:STRING)
        # Reduce: group by col 2 (custkey), COUNT
        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("custkey", TypeCode.I64),
            ColumnDef("order_count", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, b_tid)
            r = cb.reduce(j, group_by_cols=[2], agg_func_id=1, agg_col_idx=1)
            cb.sink(r, vid)

        vid = _build_and_create_view(
            client, sn, "v_q3_" + _uid(), a_tid, build, out_cols
        )

        # Bulk-load aggregated lineitem
        agg_rows = _aggregate_lineitem_by_orderkey(tpch_data['lineitem'])
        _push_li_agg_rows(client, a_tid, agg_rows)

        return a_tid, LI_AGG_SCHEMA, vid, tpch_data

    def test_correctness(self, client, sn, tpch_data):
        """Verify join+reduce gives correct per-customer order counts."""
        a_tid, a_schema, vid, data = self._setup(client, sn, tpch_data)

        order_to_cust = {o[0]: o[1] for o in data['orders']}
        li_orderkeys = set(row[1] for row in data['lineitem'])
        expected = {}
        for okey in li_orderkeys:
            ckey = order_to_cust.get(okey)
            if ckey is not None:
                expected[ckey] = expected.get(ckey, 0) + 1

        actual = _scan_reduce_result(client, vid)
        assert actual == expected

    def test_incremental_push(self, client, sn, tpch_data):
        """Push a new orderkey and verify customer count increases."""
        a_tid, a_schema, vid, data = self._setup(client, sn, tpch_data)

        before = _scan_reduce_result(client, vid)

        # Push a new orderkey that matches an existing order (joins successfully)
        okey = data['orders'][0][0]
        ckey = data['orders'][0][1]
        _push_li_agg_rows(client, a_tid, [(700000, 5000)])  # new PK, won't join
        _push_li_agg_rows(client, a_tid, [(okey, 5000)])    # existing okey, joins

        after = _scan_reduce_result(client, vid)
        assert after.get(ckey, 0) == before.get(ckey, 0) + 1

    def test_retraction(self, client, sn, tpch_data):
        """Delete a li_agg row and verify customer count decreases through join+reduce."""
        a_tid, a_schema, vid, data = self._setup(client, sn, tpch_data)

        before = _scan_reduce_result(client, vid)

        # Pick a customer with >= 2 contributing orderkeys so the group survives.
        order_to_cust = {o[0]: o[1] for o in data['orders']}
        li_orderkeys = set(row[1] for row in data['lineitem'])

        cust_okeys = {}
        for okey in li_orderkeys:
            ckey = order_to_cust.get(okey)
            if ckey is not None:
                cust_okeys.setdefault(ckey, []).append(okey)

        target_ckey = None
        for ckey, okeys in cust_okeys.items():
            if len(okeys) >= 2:
                target_ckey = ckey
                break
        assert target_ckey is not None, "Need a customer with >= 2 orderkeys"

        # Retract one of the li_agg rows for this customer
        retract_okey = cust_okeys[target_ckey][0]
        # Need to retract with the same price that was originally pushed
        li_agg = dict(_aggregate_lineitem_by_orderkey(data['lineitem']))
        _push_li_agg_rows(client, a_tid, [(retract_okey, li_agg[retract_okey])], weight=-1)

        after = _scan_reduce_result(client, vid)
        assert after[target_ckey] == before[target_ckey] - 1, (
            f"custkey={target_ckey}: expected {before[target_ckey] - 1}, "
            f"got {after.get(target_ckey)}"
        )

    def test_timing_proportionality(self, client, sn, tpch_data):
        """Incremental push through join(STRING trace) should scale with delta."""
        a_tid, a_schema, vid, data = self._setup(client, sn, tpch_data)

        def push_n(n, offset):
            rows = [(800000 + offset + i, 5000) for i in range(n)]
            t0 = time.perf_counter()
            _push_li_agg_rows(client, a_tid, rows)
            client.scan(vid)
            return time.perf_counter() - t0

        push_n(5, 0)  # warm up

        timings = {}
        for i, n in enumerate([1, 10, 50]):
            timings[n] = push_n(n, 50000 + i * 1000)

        ratio = timings[50] / max(timings[1], 1e-9)
        assert ratio < 30, (
            f"Non-incremental! push(50)/push(1) = {ratio:.1f}x "
            f"(timings: {timings})"
        )


# ---------------------------------------------------------------------------
# Q4: Join + Reduce MIN (non-linear aggregate — requires history replay)
# (li_agg JOIN orders(STRING) -> MIN(total_price) by custkey)
# ---------------------------------------------------------------------------

def _build_cust_prices(lineitem_data, orders_data):
    """Compute per-customer list of total_prices from raw data."""
    order_to_cust = {o[0]: o[1] for o in orders_data}
    li_agg = {}
    for pk, okey, qty, price in lineitem_data:
        li_agg[okey] = li_agg.get(okey, 0) + price

    cust_prices = {}
    for okey, total_price in li_agg.items():
        ckey = order_to_cust.get(okey)
        if ckey is not None:
            cust_prices.setdefault(ckey, []).append((okey, total_price))
    return cust_prices


def _find_cust_with_distinct_prices(cust_prices, order_to_cust, li_agg, key_fn):
    """Find a customer with >= 2 distinct prices and return
    (ckey, retract_okey, new_expected) where retract_okey holds the
    current extremum (min or max per key_fn) and new_expected is the
    extremum after retraction.
    """
    for ckey, okey_prices in cust_prices.items():
        prices = [p for _, p in okey_prices]
        if len(prices) >= 2 and len(set(prices)) >= 2:
            extremum = key_fn(prices)
            remaining = [p for p in prices if p != extremum]
            if remaining:
                # Find the orderkey holding the current extremum
                for okey, price in okey_prices:
                    if price == extremum:
                        return ckey, okey, key_fn(remaining)
    return None, None, None


class TestQ4JoinReduceMin:
    """Incrementality test: join + reduce MIN (non-linear aggregate).

    MIN cannot algebraically cancel retractions — reduce must replay
    the full consolidated history to recompute the aggregate. This exercises
    the non-linear path in reduce.py that the SUM/COUNT tests don't touch.
    """

    def _setup(self, client, sn, tpch_data):
        """Create tables, load bulk, create view with MIN aggregate."""
        li_tid = client.create_table(sn, "li_q4_" + _uid(), LI_AGG_COLS, pk_col_idx=0)

        ord_tid = client.create_table(sn, "ord_q4_" + _uid(), ORDERS_COLS, pk_col_idx=0)
        _push_orders_rows(client, ord_tid, tpch_data['orders'])

        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("custkey", TypeCode.I64),
            ColumnDef("min_price", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, ord_tid)
            r = cb.reduce(j, group_by_cols=[2], agg_func_id=3, agg_col_idx=1)
            cb.sink(r, vid)

        vid = _build_and_create_view(
            client, sn, "v_q4_" + _uid(), li_tid, build, out_cols
        )

        agg_rows = _aggregate_lineitem_by_orderkey(tpch_data['lineitem'])
        _push_li_agg_rows(client, li_tid, agg_rows)

        return li_tid, vid, tpch_data

    def test_correctness(self, client, sn, tpch_data):
        """Verify join+reduce MIN produces correct per-customer minimums."""
        li_tid, vid, data = self._setup(client, sn, tpch_data)

        cust_prices = _build_cust_prices(data['lineitem'], data['orders'])
        expected = {ckey: min(p for _, p in ops) for ckey, ops in cust_prices.items()}

        actual = _scan_reduce_result(client, vid)
        assert actual == expected

    def test_retraction_of_non_min(self, client, sn, tpch_data):
        """Retract a non-minimum row — MIN should be unchanged."""
        li_tid, vid, data = self._setup(client, sn, tpch_data)

        before = _scan_reduce_result(client, vid)

        cust_prices = _build_cust_prices(data['lineitem'], data['orders'])
        order_to_cust = {o[0]: o[1] for o in data['orders']}
        li_agg = dict(_aggregate_lineitem_by_orderkey(data['lineitem']))

        # Find a customer where we can retract a non-min order
        target_ckey = None
        retract_okey = None
        for ckey, okey_prices in cust_prices.items():
            if len(okey_prices) >= 2:
                min_price = min(p for _, p in okey_prices)
                for okey, price in okey_prices:
                    if price > min_price:
                        target_ckey = ckey
                        retract_okey = okey
                        break
            if target_ckey is not None:
                break
        assert target_ckey is not None, "Need a customer with a retractable non-min order"

        _push_li_agg_rows(client, li_tid, [(retract_okey, li_agg[retract_okey])], weight=-1)

        after = _scan_reduce_result(client, vid)
        assert after[target_ckey] == before[target_ckey], (
            f"custkey={target_ckey}: MIN should be unchanged after retracting non-min row"
        )

    def test_retraction_of_min(self, client, sn, tpch_data):
        """Retract the current minimum — forces full history replay to find new MIN."""
        li_tid, vid, data = self._setup(client, sn, tpch_data)

        cust_prices = _build_cust_prices(data['lineitem'], data['orders'])
        order_to_cust = {o[0]: o[1] for o in data['orders']}
        li_agg = dict(_aggregate_lineitem_by_orderkey(data['lineitem']))

        target_ckey, retract_okey, new_expected_min = _find_cust_with_distinct_prices(
            cust_prices, order_to_cust, li_agg, min
        )
        assert target_ckey is not None, "Need a customer with >= 2 distinct prices"

        _push_li_agg_rows(client, li_tid, [(retract_okey, li_agg[retract_okey])], weight=-1)

        after = _scan_reduce_result(client, vid)
        assert after[target_ckey] == new_expected_min, (
            f"custkey={target_ckey}: expected new MIN={new_expected_min}, "
            f"got {after.get(target_ckey)}"
        )

    def test_incremental_new_min(self, client, sn, tpch_data):
        """Push a value smaller than current MIN — should become the new MIN."""
        li_tid, vid, data = self._setup(client, sn, tpch_data)

        # Pick any customer and push a very small price via an existing orderkey
        okey = data['orders'][0][0]
        ckey = data['orders'][0][1]
        tiny_price = 1  # smaller than any generated price (min is 100)
        _push_li_agg_rows(client, li_tid, [(okey, tiny_price)])

        after = _scan_reduce_result(client, vid)
        assert after[ckey] == tiny_price, (
            f"custkey={ckey}: expected new MIN={tiny_price}, got {after.get(ckey)}"
        )


# ---------------------------------------------------------------------------
# Q5: Join + Reduce MAX (non-linear aggregate — complement of MIN)
# (li_agg JOIN orders(STRING) -> MAX(total_price) by custkey)
# ---------------------------------------------------------------------------

class TestQ5JoinReduceMax:
    """Incrementality test: join + reduce MAX (non-linear aggregate).

    Complements Q4 (MIN). Retraction of the current maximum forces history
    replay to find the new maximum.
    """

    def _setup(self, client, sn, tpch_data):
        """Create tables, load bulk, create view with MAX aggregate."""
        li_tid = client.create_table(sn, "li_q5_" + _uid(), LI_AGG_COLS, pk_col_idx=0)

        ord_tid = client.create_table(sn, "ord_q5_" + _uid(), ORDERS_COLS, pk_col_idx=0)
        _push_orders_rows(client, ord_tid, tpch_data['orders'])

        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("custkey", TypeCode.I64),
            ColumnDef("max_price", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, ord_tid)
            r = cb.reduce(j, group_by_cols=[2], agg_func_id=4, agg_col_idx=1)
            cb.sink(r, vid)

        vid = _build_and_create_view(
            client, sn, "v_q5_" + _uid(), li_tid, build, out_cols
        )

        agg_rows = _aggregate_lineitem_by_orderkey(tpch_data['lineitem'])
        _push_li_agg_rows(client, li_tid, agg_rows)

        return li_tid, vid, tpch_data

    def test_correctness(self, client, sn, tpch_data):
        """Verify join+reduce MAX produces correct per-customer maximums."""
        li_tid, vid, data = self._setup(client, sn, tpch_data)

        cust_prices = _build_cust_prices(data['lineitem'], data['orders'])
        expected = {ckey: max(p for _, p in ops) for ckey, ops in cust_prices.items()}

        actual = _scan_reduce_result(client, vid)
        assert actual == expected

    def test_retraction_of_max(self, client, sn, tpch_data):
        """Retract the current maximum — forces history replay to find new MAX."""
        li_tid, vid, data = self._setup(client, sn, tpch_data)

        cust_prices = _build_cust_prices(data['lineitem'], data['orders'])
        order_to_cust = {o[0]: o[1] for o in data['orders']}
        li_agg = dict(_aggregate_lineitem_by_orderkey(data['lineitem']))

        target_ckey, retract_okey, new_expected_max = _find_cust_with_distinct_prices(
            cust_prices, order_to_cust, li_agg, max
        )
        assert target_ckey is not None, "Need a customer with >= 2 distinct prices"

        _push_li_agg_rows(client, li_tid, [(retract_okey, li_agg[retract_okey])], weight=-1)

        after = _scan_reduce_result(client, vid)
        assert after[target_ckey] == new_expected_max, (
            f"custkey={target_ckey}: expected new MAX={new_expected_max}, "
            f"got {after.get(target_ckey)}"
        )

    def test_incremental_new_max(self, client, sn, tpch_data):
        """Push a value larger than current MAX — should become the new MAX."""
        li_tid, vid, data = self._setup(client, sn, tpch_data)

        okey = data['orders'][0][0]
        ckey = data['orders'][0][1]
        huge_price = 99999999  # larger than any generated price
        _push_li_agg_rows(client, li_tid, [(okey, huge_price)])

        after = _scan_reduce_result(client, vid)
        assert after[ckey] == huge_price, (
            f"custkey={ckey}: expected new MAX={huge_price}, got {after.get(ckey)}"
        )
