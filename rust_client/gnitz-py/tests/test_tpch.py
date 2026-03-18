"""TPC-H-inspired incrementality benchmarks.

Verifies the core DBSP property: view update cost scales with delta size,
not total data size. Uses a small TPC-H-like dataset and measures push
latency for incremental batches.
"""

import time
import random
import pytest
import gnitz

from tpch_gen import (
    generate_all,
    generate_incremental_lineitem,
)


SF = 0.5  # ~2500 lineitem, ~750 orders


def _uid():
    return str(random.randint(100000, 999999))


# ---------------------------------------------------------------------------
# Table schemas
# ---------------------------------------------------------------------------

LINEITEM_COLS = [
    gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("orderkey", gnitz.TypeCode.I64),
    gnitz.ColumnDef("quantity", gnitz.TypeCode.I64),
    gnitz.ColumnDef("price", gnitz.TypeCode.I64),
]
LINEITEM_SCHEMA = gnitz.Schema(LINEITEM_COLS)

# Aggregated lineitem by orderkey (delta source for join queries)
LI_AGG_COLS = [
    gnitz.ColumnDef("orderkey", gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("total_price", gnitz.TypeCode.I64),
]
LI_AGG_SCHEMA = gnitz.Schema(LI_AGG_COLS)

ORDERS_COLS = [
    gnitz.ColumnDef("orderkey", gnitz.TypeCode.U64, primary_key=True),
    gnitz.ColumnDef("custkey", gnitz.TypeCode.I64),
    gnitz.ColumnDef("status", gnitz.TypeCode.STRING, is_nullable=True),
]
ORDERS_SCHEMA = gnitz.Schema(ORDERS_COLS)

# Module-level TPC-H data (generated once)
_TPCH_DATA = generate_all(sf=SF)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _push_lineitem(client, tid, rows, weight=1):
    batch = gnitz.ZSetBatch(LINEITEM_SCHEMA)
    for pk, okey, qty, price in rows:
        batch.append(pk=pk, orderkey=okey, quantity=qty, price=price, weight=weight)
    client.push(tid, batch)


def _push_orders(client, tid, rows):
    batch = gnitz.ZSetBatch(ORDERS_SCHEMA)
    for row in rows:
        okey, ckey, _totalprice, status = row
        batch.append(orderkey=okey, custkey=ckey, status=status)
    client.push(tid, batch)


def _push_li_agg(client, tid, rows, weight=1):
    batch = gnitz.ZSetBatch(LI_AGG_SCHEMA)
    for okey, total_price in rows:
        batch.append(orderkey=okey, total_price=total_price, weight=weight)
    client.push(tid, batch)


def _scan_reduce(client, vid):
    """Scan a reduce view; return {group_val: agg_val} for positive-weight rows."""
    return {row[1]: row[2] for row in client.scan(vid) if row.weight > 0}


def _aggregate_lineitem_by_orderkey(lineitem_data):
    """Pre-aggregate lineitem prices by orderkey. Returns [(okey, total), ...]."""
    agg = {}
    for pk, okey, qty, price in lineitem_data:
        agg[okey] = agg.get(okey, 0) + price
    return list(agg.items())


def _build_cust_prices(lineitem_data, orders_data):
    """Compute per-customer list of (okey, total_price) from raw data."""
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


def _find_cust_with_distinct_prices(cust_prices, key_fn):
    for ckey, okey_prices in cust_prices.items():
        prices = [p for _, p in okey_prices]
        if len(prices) >= 2 and len(set(prices)) >= 2:
            extremum = key_fn(prices)
            remaining = [p for p in prices if p != extremum]
            if remaining:
                for okey, price in okey_prices:
                    if price == extremum:
                        return ckey, okey, key_fn(remaining)
    return None, None, None


def _cleanup(client, sn, views=(), tables=()):
    for v in views:
        try:
            client.execute_sql(f"DROP VIEW {v}", schema_name=sn)
        except Exception:
            pass
    for t in tables:
        try:
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _timed_push_and_scan(client, tid, vid, rows):
    """Push lineitem rows and scan view; return elapsed seconds."""
    t0 = time.perf_counter()
    _push_lineitem(client, tid, rows)
    client.scan(vid)
    return time.perf_counter() - t0


def _timed_push_agg_and_scan(client, tid, vid, rows, weight=1):
    """Push li_agg rows and scan view; return elapsed seconds."""
    t0 = time.perf_counter()
    _push_li_agg(client, tid, rows, weight=weight)
    client.scan(vid)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# Q1: Filter + Reduce
# lineitem -> filter(price > 5000) -> SUM(quantity) GROUP BY orderkey
# ---------------------------------------------------------------------------

class TestQ1FilterReduceScaling:

    def _setup(self, client, sn):
        tname = "li_q1_" + _uid()
        tid = client.create_table(sn, tname, LINEITEM_COLS, unique_pk=False)

        eb = gnitz.ExprBuilder()
        r0 = eb.load_col_int(3)   # price is col index 3
        r1 = eb.load_const(5000)
        cond = eb.cmp_gt(r0, r1)
        prog = eb.build(result_reg=cond)

        cb = client.circuit_builder(source_table_id=tid)
        inp = cb.input_delta()
        filt = cb.filter(inp, prog)
        red = cb.reduce(filt, group_by_cols=[1], agg_func_id=2, agg_col_idx=2)
        cb.sink(red)
        circuit = cb.build()

        vname = "v_q1_" + _uid()
        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U128, primary_key=True),
                    gnitz.ColumnDef("orderkey", gnitz.TypeCode.I64),
                    gnitz.ColumnDef("sum_qty", gnitz.TypeCode.I64)]
        vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)
        return tid, vid, tname, vname

    def test_correctness(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        tid, vid, tname, vname = self._setup(client, sn)
        try:
            _push_lineitem(client, tid, _TPCH_DATA['lineitem'])

            expected = {}
            for pk, okey, qty, price in _TPCH_DATA['lineitem']:
                if price > 5000:
                    expected[okey] = expected.get(okey, 0) + qty

            actual = _scan_reduce(client, vid)
            assert actual == expected
        finally:
            _cleanup(client, sn, views=[vname], tables=[tname])

    def test_incremental_correctness(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        tid, vid, tname, vname = self._setup(client, sn)
        try:
            _push_lineitem(client, tid, _TPCH_DATA['lineitem'])
            before = _scan_reduce(client, vid)

            new_rows = generate_incremental_lineitem(_TPCH_DATA['orders'], 10, seed=7777)
            new_rows = [(pk, okey, qty, 10000) for pk, okey, qty, _ in new_rows]
            _push_lineitem(client, tid, new_rows)

            after = _scan_reduce(client, vid)
            for pk, okey, qty, price in new_rows:
                assert after.get(okey, 0) == before.get(okey, 0) + qty
        finally:
            _cleanup(client, sn, views=[vname], tables=[tname])

    def test_retraction(self, client):
        """Delete rows via weight=-1; verify aggregate decreases."""
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        tid, vid, tname, vname = self._setup(client, sn)
        try:
            _push_lineitem(client, tid, _TPCH_DATA['lineitem'])
            before = _scan_reduce(client, vid)

            by_okey = {}
            for pk, okey, qty, price in _TPCH_DATA['lineitem']:
                if price > 5000:
                    by_okey.setdefault(okey, []).append((pk, okey, qty, price))

            target_okey = None
            for okey, rows in by_okey.items():
                if len(rows) >= 2:
                    target_okey = okey
                    break
            assert target_okey is not None, "Need a group with >= 2 filtered rows"

            retract_row = by_okey[target_okey][0]
            _push_lineitem(client, tid, [retract_row], weight=-1)

            after = _scan_reduce(client, vid)
            expected_sum = before[target_okey] - retract_row[2]
            assert after[target_okey] == expected_sum
        finally:
            _cleanup(client, sn, views=[vname], tables=[tname])

    def test_group_elimination(self, client):
        """Retract all rows for one group — group disappears."""
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        tid, vid, tname, vname = self._setup(client, sn)
        try:
            _push_lineitem(client, tid, _TPCH_DATA['lineitem'])

            by_okey = {}
            for pk, okey, qty, price in _TPCH_DATA['lineitem']:
                if price > 5000:
                    by_okey.setdefault(okey, []).append((pk, okey, qty, price))

            target_okey = target_rows = None
            for okey, rows in by_okey.items():
                if len(rows) == 1:
                    target_okey = okey
                    target_rows = rows
                    break
            assert target_okey is not None, "Need a group with exactly 1 filtered row"

            _push_lineitem(client, tid, target_rows, weight=-1)
            after = _scan_reduce(client, vid)
            assert after.get(target_okey, 0) == 0 or target_okey not in after
        finally:
            _cleanup(client, sn, views=[vname], tables=[tname])

    def test_timing_proportionality(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        tid, vid, tname, vname = self._setup(client, sn)
        try:
            _push_lineitem(client, tid, _TPCH_DATA['lineitem'])

            warmup = generate_incremental_lineitem(_TPCH_DATA['orders'], 5, seed=1000)
            warmup = [(pk, okey, qty, 10000) for pk, okey, qty, _ in warmup]
            _timed_push_and_scan(client, tid, vid, warmup)

            timings = {}
            for i, n in enumerate([1, 10, 50]):
                rows = generate_incremental_lineitem(_TPCH_DATA['orders'], n, seed=2000 + i)
                rows = [(pk + i * 100000, okey, qty, 10000) for pk, okey, qty, _ in rows]
                timings[n] = _timed_push_and_scan(client, tid, vid, rows)

            ratio = timings[50] / max(timings[1], 1e-9)
            assert ratio < 30, (
                f"Non-incremental! push(50)/push(1) = {ratio:.1f}x (timings: {timings})"
            )
        finally:
            _cleanup(client, sn, views=[vname], tables=[tname])


# ---------------------------------------------------------------------------
# Q2: Join + Reduce (li_agg JOIN orders(STRING) -> SUM(price) GROUP BY custkey)
# ---------------------------------------------------------------------------

class TestQ2JoinReduceScaling:

    def _setup(self, client, sn):
        li_tname = "li_q2_" + _uid()
        ord_tname = "ord_q2_" + _uid()
        li_tid = client.create_table(sn, li_tname, LI_AGG_COLS, unique_pk=False)
        ord_tid = client.create_table(sn, ord_tname, ORDERS_COLS, unique_pk=False)
        _push_orders(client, ord_tid, _TPCH_DATA['orders'])

        cb = client.circuit_builder(source_table_id=li_tid)
        inp = cb.input_delta()
        j = cb.join(inp, ord_tid)
        # join output: (orderkey:U64, total_price:I64, custkey:I64, status:STRING)
        # group_by=[2](custkey), SUM col 1 (total_price)
        red = cb.reduce(j, group_by_cols=[2], agg_func_id=2, agg_col_idx=1)
        cb.sink(red)
        circuit = cb.build()

        vname = "v_q2_" + _uid()
        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U128, primary_key=True),
                    gnitz.ColumnDef("custkey", gnitz.TypeCode.I64),
                    gnitz.ColumnDef("sum_price", gnitz.TypeCode.I64)]
        vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)

        agg_rows = _aggregate_lineitem_by_orderkey(_TPCH_DATA['lineitem'])
        _push_li_agg(client, li_tid, agg_rows)
        return li_tid, ord_tid, vid, li_tname, ord_tname, vname

    def test_correctness(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            order_to_cust = {o[0]: o[1] for o in _TPCH_DATA['orders']}
            li_agg = {}
            for pk, okey, qty, price in _TPCH_DATA['lineitem']:
                li_agg[okey] = li_agg.get(okey, 0) + price
            expected = {}
            for okey, total in li_agg.items():
                ckey = order_to_cust.get(okey)
                if ckey is not None:
                    expected[ckey] = expected.get(ckey, 0) + total

            actual = _scan_reduce(client, vid)
            assert actual == expected
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    def test_incremental_push(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            before = _scan_reduce(client, vid)
            okey = _TPCH_DATA['orders'][0][0]
            ckey = _TPCH_DATA['orders'][0][1]
            new_price = 42000
            _push_li_agg(client, li_tid, [(okey, new_price)])
            after = _scan_reduce(client, vid)
            assert after.get(ckey, 0) == before.get(ckey, 0) + new_price
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    def test_retraction(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            before = _scan_reduce(client, vid)
            order_to_cust = {o[0]: o[1] for o in _TPCH_DATA['orders']}
            li_agg = dict(_aggregate_lineitem_by_orderkey(_TPCH_DATA['lineitem']))
            cust_orders = {}
            for okey, price in li_agg.items():
                ckey = order_to_cust.get(okey)
                if ckey is not None:
                    cust_orders.setdefault(ckey, []).append((okey, price))
            target_ckey = None
            for ckey, olist in cust_orders.items():
                if len(olist) >= 2:
                    target_ckey = ckey
                    break
            assert target_ckey is not None

            retract_okey, retract_price = cust_orders[target_ckey][0]
            _push_li_agg(client, li_tid, [(retract_okey, retract_price)], weight=-1)
            after = _scan_reduce(client, vid)
            assert after[target_ckey] == before[target_ckey] - retract_price
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    def test_timing_proportionality(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            def push_n(n, offset):
                rows = [(900000 + offset + i, 1000) for i in range(n)]
                return _timed_push_agg_and_scan(client, li_tid, vid, rows)

            push_n(5, 0)
            timings = {n: push_n(n, 10000 + i * 1000) for i, n in enumerate([1, 10, 50])}
            ratio = timings[50] / max(timings[1], 1e-9)
            assert ratio < 30, f"Non-incremental! push(50)/push(1) = {ratio:.1f}x"
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])


# ---------------------------------------------------------------------------
# Q3: Join + Reduce COUNT (li_agg JOIN orders -> COUNT(*) GROUP BY custkey)
# ---------------------------------------------------------------------------

class TestQ3JoinChainScaling:

    def _setup(self, client, sn):
        a_tname = "li_q3_" + _uid()
        b_tname = "ord_q3_" + _uid()
        a_tid = client.create_table(sn, a_tname, LI_AGG_COLS, unique_pk=False)
        b_tid = client.create_table(sn, b_tname, ORDERS_COLS, unique_pk=False)
        _push_orders(client, b_tid, _TPCH_DATA['orders'])

        cb = client.circuit_builder(source_table_id=a_tid)
        inp = cb.input_delta()
        j = cb.join(inp, b_tid)
        red = cb.reduce(j, group_by_cols=[2], agg_func_id=1, agg_col_idx=1)
        cb.sink(red)
        circuit = cb.build()

        vname = "v_q3_" + _uid()
        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U128, primary_key=True),
                    gnitz.ColumnDef("custkey", gnitz.TypeCode.I64),
                    gnitz.ColumnDef("order_count", gnitz.TypeCode.I64)]
        vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)

        agg_rows = _aggregate_lineitem_by_orderkey(_TPCH_DATA['lineitem'])
        _push_li_agg(client, a_tid, agg_rows)
        return a_tid, b_tid, vid, a_tname, b_tname, vname

    def test_correctness(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        a_tid, b_tid, vid, a_tname, b_tname, vname = self._setup(client, sn)
        try:
            order_to_cust = {o[0]: o[1] for o in _TPCH_DATA['orders']}
            li_orderkeys = set(row[1] for row in _TPCH_DATA['lineitem'])
            expected = {}
            for okey in li_orderkeys:
                ckey = order_to_cust.get(okey)
                if ckey is not None:
                    expected[ckey] = expected.get(ckey, 0) + 1
            actual = _scan_reduce(client, vid)
            assert actual == expected
        finally:
            _cleanup(client, sn, views=[vname], tables=[a_tname, b_tname])

    def test_incremental_push(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        a_tid, b_tid, vid, a_tname, b_tname, vname = self._setup(client, sn)
        try:
            before = _scan_reduce(client, vid)
            okey = _TPCH_DATA['orders'][0][0]
            ckey = _TPCH_DATA['orders'][0][1]
            _push_li_agg(client, a_tid, [(okey, 5000)])
            after = _scan_reduce(client, vid)
            assert after.get(ckey, 0) == before.get(ckey, 0) + 1
        finally:
            _cleanup(client, sn, views=[vname], tables=[a_tname, b_tname])

    def test_retraction(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        a_tid, b_tid, vid, a_tname, b_tname, vname = self._setup(client, sn)
        try:
            before = _scan_reduce(client, vid)
            order_to_cust = {o[0]: o[1] for o in _TPCH_DATA['orders']}
            li_orderkeys = set(row[1] for row in _TPCH_DATA['lineitem'])
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
            assert target_ckey is not None
            li_agg = dict(_aggregate_lineitem_by_orderkey(_TPCH_DATA['lineitem']))
            retract_okey = cust_okeys[target_ckey][0]
            _push_li_agg(client, a_tid, [(retract_okey, li_agg[retract_okey])], weight=-1)
            after = _scan_reduce(client, vid)
            assert after[target_ckey] == before[target_ckey] - 1
        finally:
            _cleanup(client, sn, views=[vname], tables=[a_tname, b_tname])

    def test_timing_proportionality(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        a_tid, b_tid, vid, a_tname, b_tname, vname = self._setup(client, sn)
        try:
            def push_n(n, offset):
                rows = [(800000 + offset + i, 5000) for i in range(n)]
                return _timed_push_agg_and_scan(client, a_tid, vid, rows)

            push_n(5, 0)
            timings = {n: push_n(n, 50000 + i * 1000) for i, n in enumerate([1, 10, 50])}
            ratio = timings[50] / max(timings[1], 1e-9)
            assert ratio < 30, f"Non-incremental! push(50)/push(1) = {ratio:.1f}x"
        finally:
            _cleanup(client, sn, views=[vname], tables=[a_tname, b_tname])


# ---------------------------------------------------------------------------
# Q4: Join + ReduceMin (non-linear aggregate)
# ---------------------------------------------------------------------------

class TestQ4MinScaling:

    def _setup(self, client, sn):
        li_tname = "li_q4_" + _uid()
        ord_tname = "ord_q4_" + _uid()
        li_tid = client.create_table(sn, li_tname, LI_AGG_COLS, unique_pk=False)
        ord_tid = client.create_table(sn, ord_tname, ORDERS_COLS, unique_pk=False)
        _push_orders(client, ord_tid, _TPCH_DATA['orders'])

        cb = client.circuit_builder(source_table_id=li_tid)
        inp = cb.input_delta()
        j = cb.join(inp, ord_tid)
        red = cb.reduce(j, group_by_cols=[2], agg_func_id=3, agg_col_idx=1)  # MIN
        cb.sink(red)
        circuit = cb.build()

        vname = "v_q4_" + _uid()
        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U128, primary_key=True),
                    gnitz.ColumnDef("custkey", gnitz.TypeCode.I64),
                    gnitz.ColumnDef("min_price", gnitz.TypeCode.I64)]
        vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)

        agg_rows = _aggregate_lineitem_by_orderkey(_TPCH_DATA['lineitem'])
        _push_li_agg(client, li_tid, agg_rows)
        return li_tid, ord_tid, vid, li_tname, ord_tname, vname

    def test_correctness(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            cust_prices = _build_cust_prices(_TPCH_DATA['lineitem'], _TPCH_DATA['orders'])
            expected = {ckey: min(p for _, p in ops) for ckey, ops in cust_prices.items()}
            assert _scan_reduce(client, vid) == expected
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    @pytest.mark.xfail(strict=False, reason="join→reduce MIN retraction not fully implemented")
    def test_retraction_of_non_min(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            before = _scan_reduce(client, vid)
            cust_prices = _build_cust_prices(_TPCH_DATA['lineitem'], _TPCH_DATA['orders'])
            li_agg = dict(_aggregate_lineitem_by_orderkey(_TPCH_DATA['lineitem']))

            target_ckey = retract_okey = None
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
            assert target_ckey is not None

            _push_li_agg(client, li_tid, [(retract_okey, li_agg[retract_okey])], weight=-1)
            assert _scan_reduce(client, vid)[target_ckey] == before[target_ckey]
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    @pytest.mark.xfail(strict=False, reason="join→reduce MIN retraction not fully implemented")
    def test_retraction_of_min(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            cust_prices = _build_cust_prices(_TPCH_DATA['lineitem'], _TPCH_DATA['orders'])
            li_agg = dict(_aggregate_lineitem_by_orderkey(_TPCH_DATA['lineitem']))
            target_ckey, retract_okey, new_expected = _find_cust_with_distinct_prices(
                cust_prices, min
            )
            assert target_ckey is not None

            _push_li_agg(client, li_tid, [(retract_okey, li_agg[retract_okey])], weight=-1)
            assert _scan_reduce(client, vid)[target_ckey] == new_expected
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    def test_incremental_new_min(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            okey = _TPCH_DATA['orders'][0][0]
            ckey = _TPCH_DATA['orders'][0][1]
            tiny_price = 1
            _push_li_agg(client, li_tid, [(okey, tiny_price)])
            assert _scan_reduce(client, vid)[ckey] == tiny_price
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    def test_group_elimination(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            cust_prices = _build_cust_prices(_TPCH_DATA['lineitem'], _TPCH_DATA['orders'])
            li_agg = dict(_aggregate_lineitem_by_orderkey(_TPCH_DATA['lineitem']))

            target_ckey = target_okey = None
            for ckey, okey_prices in cust_prices.items():
                if len(okey_prices) == 1:
                    target_ckey = ckey
                    target_okey = okey_prices[0][0]
                    break
            assert target_ckey is not None

            _push_li_agg(client, li_tid, [(target_okey, li_agg[target_okey])], weight=-1)
            assert target_ckey not in _scan_reduce(client, vid)
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    @pytest.mark.xfail(strict=False, reason="join→reduce MIN retraction not fully implemented")
    def test_multi_tick_retraction(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            okey = _TPCH_DATA['orders'][0][0]
            ckey = _TPCH_DATA['orders'][0][1]
            cust_prices = _build_cust_prices(_TPCH_DATA['lineitem'], _TPCH_DATA['orders'])
            original_min = min(p for _, p in cust_prices.get(ckey, [(0, 1)]))

            _push_li_agg(client, li_tid, [(okey, 1)])
            assert _scan_reduce(client, vid).get(ckey) == 1

            _push_li_agg(client, li_tid, [(okey, 1)], weight=-1)
            assert _scan_reduce(client, vid).get(ckey) == original_min

            _push_li_agg(client, li_tid, [(okey, 99999998)])
            assert _scan_reduce(client, vid).get(ckey) == original_min
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    def test_timing_proportionality(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            def push_n(n, offset):
                rows = [(700000 + offset + i, 5000) for i in range(n)]
                return _timed_push_agg_and_scan(client, li_tid, vid, rows)

            push_n(5, 0)
            timings = {n: push_n(n, 10000 + i * 1000) for i, n in enumerate([1, 10, 50])}
            ratio = timings[50] / max(timings[1], 1e-9)
            assert ratio < 30, f"Non-incremental! push(50)/push(1) = {ratio:.1f}x"
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])


# ---------------------------------------------------------------------------
# Q5: Join + ReduceMax (complement of MIN)
# ---------------------------------------------------------------------------

class TestQ5MaxScaling:

    def _setup(self, client, sn):
        li_tname = "li_q5_" + _uid()
        ord_tname = "ord_q5_" + _uid()
        li_tid = client.create_table(sn, li_tname, LI_AGG_COLS, unique_pk=False)
        ord_tid = client.create_table(sn, ord_tname, ORDERS_COLS, unique_pk=False)
        _push_orders(client, ord_tid, _TPCH_DATA['orders'])

        cb = client.circuit_builder(source_table_id=li_tid)
        inp = cb.input_delta()
        j = cb.join(inp, ord_tid)
        red = cb.reduce(j, group_by_cols=[2], agg_func_id=4, agg_col_idx=1)  # MAX
        cb.sink(red)
        circuit = cb.build()

        vname = "v_q5_" + _uid()
        out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U128, primary_key=True),
                    gnitz.ColumnDef("custkey", gnitz.TypeCode.I64),
                    gnitz.ColumnDef("max_price", gnitz.TypeCode.I64)]
        vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)

        agg_rows = _aggregate_lineitem_by_orderkey(_TPCH_DATA['lineitem'])
        _push_li_agg(client, li_tid, agg_rows)
        return li_tid, ord_tid, vid, li_tname, ord_tname, vname

    def test_correctness(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            cust_prices = _build_cust_prices(_TPCH_DATA['lineitem'], _TPCH_DATA['orders'])
            expected = {ckey: max(p for _, p in ops) for ckey, ops in cust_prices.items()}
            assert _scan_reduce(client, vid) == expected
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    @pytest.mark.xfail(strict=False, reason="join→reduce MAX retraction not fully implemented")
    def test_retraction_of_non_max(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            before = _scan_reduce(client, vid)
            cust_prices = _build_cust_prices(_TPCH_DATA['lineitem'], _TPCH_DATA['orders'])
            li_agg = dict(_aggregate_lineitem_by_orderkey(_TPCH_DATA['lineitem']))

            target_ckey = retract_okey = None
            for ckey, okey_prices in cust_prices.items():
                if len(okey_prices) >= 2:
                    max_price = max(p for _, p in okey_prices)
                    for okey, price in okey_prices:
                        if price < max_price:
                            target_ckey = ckey
                            retract_okey = okey
                            break
                if target_ckey is not None:
                    break
            assert target_ckey is not None

            _push_li_agg(client, li_tid, [(retract_okey, li_agg[retract_okey])], weight=-1)
            assert _scan_reduce(client, vid)[target_ckey] == before[target_ckey]
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    @pytest.mark.xfail(strict=False, reason="join→reduce MAX retraction not fully implemented")
    def test_retraction_of_max(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            cust_prices = _build_cust_prices(_TPCH_DATA['lineitem'], _TPCH_DATA['orders'])
            li_agg = dict(_aggregate_lineitem_by_orderkey(_TPCH_DATA['lineitem']))
            target_ckey, retract_okey, new_expected = _find_cust_with_distinct_prices(
                cust_prices, max
            )
            assert target_ckey is not None

            _push_li_agg(client, li_tid, [(retract_okey, li_agg[retract_okey])], weight=-1)
            assert _scan_reduce(client, vid)[target_ckey] == new_expected
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    def test_incremental_new_max(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            okey = _TPCH_DATA['orders'][0][0]
            ckey = _TPCH_DATA['orders'][0][1]
            huge_price = 99999999
            _push_li_agg(client, li_tid, [(okey, huge_price)])
            assert _scan_reduce(client, vid)[ckey] == huge_price
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    def test_group_elimination(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            cust_prices = _build_cust_prices(_TPCH_DATA['lineitem'], _TPCH_DATA['orders'])
            li_agg = dict(_aggregate_lineitem_by_orderkey(_TPCH_DATA['lineitem']))

            target_ckey = target_okey = None
            for ckey, okey_prices in cust_prices.items():
                if len(okey_prices) == 1:
                    target_ckey = ckey
                    target_okey = okey_prices[0][0]
                    break
            assert target_ckey is not None

            _push_li_agg(client, li_tid, [(target_okey, li_agg[target_okey])], weight=-1)
            assert target_ckey not in _scan_reduce(client, vid)
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])

    def test_timing_proportionality(self, client):
        sn = "tpch_" + _uid()
        client.create_schema(sn)
        li_tid, ord_tid, vid, li_tname, ord_tname, vname = self._setup(client, sn)
        try:
            def push_n(n, offset):
                rows = [(600000 + offset + i, 5000) for i in range(n)]
                return _timed_push_agg_and_scan(client, li_tid, vid, rows)

            push_n(5, 0)
            timings = {n: push_n(n, 10000 + i * 1000) for i, n in enumerate([1, 10, 50])}
            ratio = timings[50] / max(timings[1], 1e-9)
            assert ratio < 30, f"Non-incremental! push(50)/push(1) = {ratio:.1f}x"
        finally:
            _cleanup(client, sn, views=[vname], tables=[li_tname, ord_tname])
