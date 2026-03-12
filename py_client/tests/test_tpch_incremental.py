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

CUSTOMER_COLS = [
    ColumnDef("custkey", TypeCode.U64),
    ColumnDef("name", TypeCode.STRING),
    ColumnDef("nationkey", TypeCode.I64),
]
CUSTOMER_SCHEMA = Schema(columns=CUSTOMER_COLS, pk_index=0)

# Aggregated lineitem by orderkey (used as delta source for join queries)
LI_AGG_COLS = [
    ColumnDef("orderkey", TypeCode.U64),
    ColumnDef("total_price", TypeCode.I64),
]
LI_AGG_SCHEMA = Schema(columns=LI_AGG_COLS, pk_index=0)


def _push_lineitem_rows(client, tid, rows):
    """Push [(pk, orderkey, quantity, price), ...] with weight=1."""
    batch = ZSetBatch(schema=LINEITEM_SCHEMA)
    for pk, okey, qty, price in rows:
        batch.pk_lo.append(pk)
        batch.pk_hi.append(0)
        batch.weights.append(1)
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


def _push_customer_rows(client, tid, rows):
    """Push [(custkey, name, nationkey), ...] with weight=1."""
    batch = ZSetBatch(schema=CUSTOMER_SCHEMA)
    for ckey, name, nkey in rows:
        batch.pk_lo.append(ckey)
        batch.pk_hi.append(0)
        batch.weights.append(1)
        batch.nulls.append(0)
    batch.columns = [
        [],
        [r[1] for r in rows],
        [r[2] for r in rows],
    ]
    client.push(tid, CUSTOMER_SCHEMA, batch)


def _push_li_agg_rows(client, tid, rows):
    """Push [(orderkey, total_price), ...] with weight=1."""
    batch = ZSetBatch(schema=LI_AGG_SCHEMA)
    for okey, total_price in rows:
        batch.pk_lo.append(okey)
        batch.pk_hi.append(0)
        batch.weights.append(1)
        batch.nulls.append(0)
    batch.columns = [[], [r[1] for r in rows]]
    client.push(tid, LI_AGG_SCHEMA, batch)


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


@pytest.fixture
def tpch_data():
    """Generate TPC-H data once per test."""
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

        schema, batch = client.scan(vid)
        assert batch is not None
        actual = {}
        for i in range(len(batch.pk_lo)):
            if batch.weights[i] > 0:
                actual[batch.columns[1][i]] = batch.columns[2][i]

        assert actual == expected

    def test_incremental_correctness(self, client, sn, tpch_data):
        """Push a small delta and verify the aggregate updates correctly."""
        tid, vid, orders = self._setup(client, sn, tpch_data)

        schema, batch_before = client.scan(vid)
        before = {}
        if batch_before:
            for i in range(len(batch_before.pk_lo)):
                if batch_before.weights[i] > 0:
                    before[batch_before.columns[1][i]] = batch_before.columns[2][i]

        new_rows = generate_incremental_lineitem(tpch_data['orders'], 10, seed=7777)
        new_rows = [(pk, okey, qty, 10000) for pk, okey, qty, _ in new_rows]
        _push_lineitem_rows(client, tid, new_rows)

        schema, batch_after = client.scan(vid)
        after = {}
        if batch_after:
            for i in range(len(batch_after.pk_lo)):
                if batch_after.weights[i] > 0:
                    after[batch_after.columns[1][i]] = batch_after.columns[2][i]

        for pk, okey, qty, price in new_rows:
            old_sum = before.get(okey, 0)
            assert after.get(okey, 0) == old_sum + qty, (
                f"orderkey={okey}: expected {old_sum + qty}, got {after.get(okey, 0)}"
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

        schema, batch = client.scan(vid)
        assert batch is not None
        actual = {}
        for i in range(len(batch.pk_lo)):
            if batch.weights[i] > 0:
                actual[batch.columns[1][i]] = batch.columns[2][i]

        assert actual == expected_by_cust

    def test_incremental_push(self, client, sn, tpch_data):
        """Push new aggregated lineitem and verify customer sum updates."""
        li_tid, li_schema, vid, data = self._setup(client, sn, tpch_data)

        schema, b0 = client.scan(vid)
        before = {}
        if b0:
            for i in range(len(b0.pk_lo)):
                if b0.weights[i] > 0:
                    before[b0.columns[1][i]] = b0.columns[2][i]

        # Push for an existing orderkey (adds weight, join sees delta)
        okey = data['orders'][0][0]
        ckey = data['orders'][0][1]
        new_price = 42000
        _push_li_agg_rows(client, li_tid, [(okey, new_price)])

        schema, b1 = client.scan(vid)
        after = {}
        if b1:
            for i in range(len(b1.pk_lo)):
                if b1.weights[i] > 0:
                    after[b1.columns[1][i]] = b1.columns[2][i]

        assert after.get(ckey, 0) == before.get(ckey, 0) + new_price

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
# Also loads customer(STRING name) to test multi-STRING-table setup
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

        # Also load customers (not used in the circuit, but validates that
        # STRING-heavy tables don't interfere with the server session)
        c_tid = client.create_table(sn, "cust_q3_" + _uid(), CUSTOMER_COLS, pk_col_idx=0)
        _push_customer_rows(client, c_tid, tpch_data['customer'])

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

        schema, batch = client.scan(vid)
        assert batch is not None
        actual = {}
        for i in range(len(batch.pk_lo)):
            if batch.weights[i] > 0:
                actual[batch.columns[1][i]] = batch.columns[2][i]

        assert actual == expected

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
