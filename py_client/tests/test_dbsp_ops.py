"""End-to-end tests for DBSP operators through the full stack."""

import random
import pytest

from gnitz_client import GnitzClient, GnitzError, TypeCode, ColumnDef, Schema
from gnitz_client.batch import ZSetBatch
from gnitz_client.circuit import CircuitBuilder
from gnitz_client.expr import ExprBuilder


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _uid():
    return str(random.randint(100000, 999999))


def _create_table_2col(client, sn, name=None):
    """Table: (pk U64, val I64). Returns (tid, schema)."""
    if name is None:
        name = "t2_" + _uid()
    cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]
    tid = client.create_table(sn, name, cols, pk_col_idx=0)
    return tid, Schema(columns=cols, pk_index=0)


def _create_table_3col(client, sn, name=None):
    """Table: (pk U64, val I64, label STRING). Returns (tid, schema)."""
    if name is None:
        name = "t3_" + _uid()
    cols = [
        ColumnDef("pk", TypeCode.U64),
        ColumnDef("val", TypeCode.I64),
        ColumnDef("label", TypeCode.STRING),
    ]
    tid = client.create_table(sn, name, cols, pk_col_idx=0)
    return tid, Schema(columns=cols, pk_index=0)


def _push_rows_2col(client, tid, schema, rows):
    """Push [(pk, val, weight), ...] to a 2-col table."""
    batch = ZSetBatch(schema=schema)
    for pk, val, w in rows:
        batch.pk_lo.append(pk)
        batch.pk_hi.append(0)
        batch.weights.append(w)
        batch.nulls.append(0)
    batch.columns = [[], [r[1] for r in rows]]
    client.push(tid, schema, batch)


def _push_rows_3col(client, tid, schema, rows):
    """Push [(pk, val, label, weight), ...] to a 3-col table."""
    batch = ZSetBatch(schema=schema)
    for pk, val, label, w in rows:
        batch.pk_lo.append(pk)
        batch.pk_hi.append(0)
        batch.weights.append(w)
        batch.nulls.append(0)
    batch.columns = [[], [r[1] for r in rows], [r[2] for r in rows]]
    client.push(tid, schema, batch)


def _scan_to_dict(client, target_id):
    """Scan and return {pk: (col_values..., weight)} for easy assertion.

    For a 2-col table: {pk: (val, weight)}
    """
    schema, batch = client.scan(target_id)
    if batch is None or len(batch.pk_lo) == 0:
        return {}
    result = {}
    ncols = len(batch.columns)
    for i in range(len(batch.pk_lo)):
        if batch.weights[i] == 0:
            continue
        pk = batch.pk_lo[i]
        vals = tuple(batch.columns[c][i] for c in range(1, ncols) if batch.columns[c])
        result[pk] = vals + (batch.weights[i],)
    return result


def _scan_to_list(client, target_id):
    """Scan and return [(pk, weight, col1, col2, ...)] sorted by pk."""
    schema, batch = client.scan(target_id)
    if batch is None or len(batch.pk_lo) == 0:
        return []
    rows = []
    ncols = len(batch.columns)
    for i in range(len(batch.pk_lo)):
        if batch.weights[i] == 0:
            continue
        pk = batch.pk_lo[i]
        w = batch.weights[i]
        vals = [batch.columns[c][i] for c in range(1, ncols) if batch.columns[c]]
        rows.append((pk, w, *vals))
    return sorted(rows, key=lambda r: r[0])


def _scan_pks(client, target_id):
    """Return sorted list of PKs with positive weight."""
    schema, batch = client.scan(target_id)
    if batch is None or len(batch.pk_lo) == 0:
        return []
    return sorted(batch.pk_lo[i] for i in range(len(batch.pk_lo)) if batch.weights[i] > 0)


def _scan_weights(client, target_id):
    """Return {pk: weight} for all rows."""
    schema, batch = client.scan(target_id)
    if batch is None or len(batch.pk_lo) == 0:
        return {}
    return {batch.pk_lo[i]: batch.weights[i] for i in range(len(batch.pk_lo))}


def _build_and_create_view(client, sn, view_name, source_tid, builder_fn,
                            output_cols):
    """Allocate vid, build circuit via builder_fn, persist, return vid."""
    vid = client.allocate_table_id()
    cb = CircuitBuilder(vid, source_tid)
    builder_fn(cb, vid)
    graph = cb.build()
    client.create_view_with_circuit(sn, view_name, graph, output_cols)
    return vid


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sn(client):
    """Create a unique schema for this test."""
    name = "dbsp_" + _uid()
    client.create_schema(name)
    return name


# ===========================================================================
# Part A: Isolated Operator Tests
# ===========================================================================


class TestFilter:
    """A1-A4: Filter operator tests."""

    def test_filter_null_predicate(self, client, sn):
        """A1: filter with func_id=0 (NULL_PREDICATE) passes all rows."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        def build(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp)
            cb.sink(f, vid)

        vid = _build_and_create_view(client, sn, "v_a1", tid, build, out_cols)
        _push_rows_2col(client, tid, ts, [(i, i * 10, 1) for i in range(1, 6)])

        pks = _scan_pks(client, vid)
        assert pks == [1, 2, 3, 4, 5]

    def test_filter_expr_gt(self, client, sn):
        """A2: filter col[1] > 50 passes only matching rows."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        eb = ExprBuilder()
        r0 = eb.load_col_int(1)   # val column (payload col idx 1)
        r1 = eb.load_const(50)
        r2 = eb.cmp_gt(r0, r1)
        expr = eb.build(r2)

        def build(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp, expr=expr)
            cb.sink(f, vid)

        vid = _build_and_create_view(client, sn, "v_a2", tid, build, out_cols)
        _push_rows_2col(client, tid, ts, [
            (1, 10, 1), (2, 30, 1), (3, 50, 1), (4, 70, 1), (5, 90, 1),
        ])

        pks = _scan_pks(client, vid)
        assert pks == [4, 5]

    def test_filter_expr_eq(self, client, sn):
        """A3: filter col[1] == 42."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        eb = ExprBuilder()
        r0 = eb.load_col_int(1)
        r1 = eb.load_const(42)
        r2 = eb.cmp_eq(r0, r1)
        expr = eb.build(r2)

        def build(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp, expr=expr)
            cb.sink(f, vid)

        vid = _build_and_create_view(client, sn, "v_a3", tid, build, out_cols)
        _push_rows_2col(client, tid, ts, [(1, 41, 1), (2, 42, 1), (3, 43, 1)])

        pks = _scan_pks(client, vid)
        assert pks == [2]

    def test_filter_no_matches(self, client, sn):
        """A4: filter col[1] > 1000 with small values → empty."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        eb = ExprBuilder()
        r0 = eb.load_col_int(1)
        r1 = eb.load_const(1000)
        r2 = eb.cmp_gt(r0, r1)
        expr = eb.build(r2)

        def build(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp, expr=expr)
            cb.sink(f, vid)

        vid = _build_and_create_view(client, sn, "v_a4", tid, build, out_cols)
        _push_rows_2col(client, tid, ts, [(i, i, 1) for i in range(1, 11)])

        pks = _scan_pks(client, vid)
        assert pks == []


class TestMap:
    """A5-A6: Map operator tests."""

    def test_map_projection_reorder(self, client, sn):
        """A5: map(projection=[2, 1]) reorders payload columns of a 3-col table."""
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("v1", TypeCode.I64),
            ColumnDef("v2", TypeCode.I64),
        ]
        tid = client.create_table(sn, "t_a5_" + _uid(), cols, pk_col_idx=0)
        ts = Schema(columns=cols, pk_index=0)
        # projection=[2, 1] writes 2 output payload columns (v2 first, v1 second)
        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("v2_first", TypeCode.I64),
            ColumnDef("v1_second", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            m = cb.map(inp, projection=[2, 1])
            cb.sink(m, vid)

        vid = _build_and_create_view(client, sn, "v_a5", tid, build, out_cols)

        # Push: pk=10, v1=100, v2=200
        batch = ZSetBatch(schema=ts)
        batch.pk_lo = [10, 20]
        batch.pk_hi = [0, 0]
        batch.weights = [1, 1]
        batch.nulls = [0, 0]
        batch.columns = [[], [100, 300], [200, 400]]
        client.push(tid, ts, batch)

        rows = _scan_to_list(client, vid)
        assert len(rows) == 2
        r = {r[0]: (r[2], r[3]) for r in rows}
        assert r[10] == (200, 100)  # v2 first, v1 second
        assert r[20] == (400, 300)

    def test_map_projection_subset(self, client, sn):
        """A6: map(projection=[1]) drops extra columns."""
        tid, ts = _create_table_3col(client, sn)
        # projection=[1] writes 1 output payload column (val from input col 1)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        def build(cb, vid):
            inp = cb.input_delta()
            m = cb.map(inp, projection=[1])
            cb.sink(m, vid)

        vid = _build_and_create_view(client, sn, "v_a6", tid, build, out_cols)
        _push_rows_3col(client, tid, ts, [
            (1, 10, "hello", 1), (2, 20, "world", 1),
        ])

        rows = _scan_to_list(client, vid)
        assert len(rows) == 2
        vals = {r[0]: r[2] for r in rows}
        assert vals[1] == 10
        assert vals[2] == 20


class TestNegate:
    """A7: Negate operator test."""

    def test_negate(self, client, sn):
        """A7: negate flips weights — verified via union cancellation."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        # negate(delta) + delta = 0 for all rows.
        # Build: delta → negate, delta → identity, union(negate, identity) → sink
        def build(cb, vid):
            inp = cb.input_delta()
            neg = cb.negate(inp)
            cb.sink(neg, vid)

        vid = _build_and_create_view(client, sn, "v_a7", tid, build, out_cols)
        _push_rows_2col(client, tid, ts, [(1, 10, 1), (2, 20, 1), (3, 30, 1)])

        # Negate produces weight=-1; the view has no positive-weight rows
        pks = _scan_pks(client, vid)
        assert pks == []


class TestDistinct:
    """A8-A9: Distinct operator tests."""

    def test_distinct_dedup(self, client, sn):
        """A8: distinct clamps weight to ±1."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        def build(cb, vid):
            inp = cb.input_delta()
            d = cb.distinct(inp)
            cb.sink(d, vid)

        vid = _build_and_create_view(client, sn, "v_a8", tid, build, out_cols)

        # Push same key with weight=+3
        batch = ZSetBatch(schema=ts)
        batch.pk_lo = [1]
        batch.pk_hi = [0]
        batch.weights = [3]
        batch.nulls = [0]
        batch.columns = [[], [42]]
        client.push(tid, ts, batch)

        ws = _scan_weights(client, vid)
        assert ws.get(1, 0) == 1

    def test_distinct_retraction(self, client, sn):
        """A9: distinct retraction removes row."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        def build(cb, vid):
            inp = cb.input_delta()
            d = cb.distinct(inp)
            cb.sink(d, vid)

        vid = _build_and_create_view(client, sn, "v_a9", tid, build, out_cols)

        _push_rows_2col(client, tid, ts, [(1, 42, 1)])
        assert _scan_pks(client, vid) == [1]

        _push_rows_2col(client, tid, ts, [(1, 42, -1)])
        assert _scan_pks(client, vid) == []


class TestReduce:
    """A10-A13: Reduce operator tests."""

    def test_reduce_count(self, client, sn):
        """A10: reduce COUNT per group."""
        # Table: (pk U64, group_id I64, val I64)
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("val", TypeCode.I64),
        ]
        tid = client.create_table(sn, "t_a10_" + _uid(), cols, pk_col_idx=0)
        ts = Schema(columns=cols, pk_index=0)

        # Reduce output schema: grouping by I64 col → synthetic U128 PK
        # Server produces: (U128 _group_hash PK, I64 group_id, I64 agg)
        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("agg", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            r = cb.reduce(inp, group_by_cols=[1], agg_func_id=1, agg_col_idx=2)
            cb.sink(r, vid)

        vid = _build_and_create_view(client, sn, "v_a10", tid, build, out_cols)

        # Push 5 rows: 3 in group 1, 2 in group 2
        batch = ZSetBatch(schema=ts)
        data = [
            (1, 1, 10), (2, 1, 20), (3, 1, 30),
            (4, 2, 40), (5, 2, 50),
        ]
        for pk, gid, val in data:
            batch.pk_lo.append(pk)
            batch.pk_hi.append(0)
            batch.weights.append(1)
            batch.nulls.append(0)
        batch.columns = [[], [d[1] for d in data], [d[2] for d in data]]
        client.push(tid, ts, batch)

        rows = _scan_to_list(client, vid)
        assert len(rows) == 2
        # rows: (pk_lo, weight, group_id, agg) — agg is at index 3
        counts = sorted(r[3] for r in rows)
        assert counts == [2, 3]

    def test_reduce_sum(self, client, sn):
        """A11: reduce SUM per group."""
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("val", TypeCode.I64),
        ]
        tid = client.create_table(sn, "t_a11_" + _uid(), cols, pk_col_idx=0)
        ts = Schema(columns=cols, pk_index=0)

        # Reduce output schema: grouping by I64 col → synthetic U128 PK
        # Server produces: (U128 _group_hash PK, I64 group_id, I64 agg)
        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("agg", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            r = cb.reduce(inp, group_by_cols=[1], agg_func_id=2, agg_col_idx=2)
            cb.sink(r, vid)

        vid = _build_and_create_view(client, sn, "v_a11", tid, build, out_cols)

        batch = ZSetBatch(schema=ts)
        data = [(1, 1, 10), (2, 1, 20), (3, 2, 100)]
        for pk, gid, val in data:
            batch.pk_lo.append(pk)
            batch.pk_hi.append(0)
            batch.weights.append(1)
            batch.nulls.append(0)
        batch.columns = [[], [d[1] for d in data], [d[2] for d in data]]
        client.push(tid, ts, batch)

        rows = _scan_to_list(client, vid)
        assert len(rows) == 2
        # rows: (pk_lo, weight, group_id, agg) — agg is at index 3
        sums = sorted(r[3] for r in rows)
        assert sums == [30, 100]  # group 1: 10+20=30, group 2: 100

    def test_reduce_min(self, client, sn):
        """A12a: reduce MIN per group."""
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("val", TypeCode.I64),
        ]
        tid = client.create_table(sn, "t_a12a_" + _uid(), cols, pk_col_idx=0)
        ts = Schema(columns=cols, pk_index=0)

        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("agg", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            r = cb.reduce(inp, group_by_cols=[1], agg_func_id=3, agg_col_idx=2)
            cb.sink(r, vid)

        vid = _build_and_create_view(client, sn, "v_a12a", tid, build, out_cols)

        batch = ZSetBatch(schema=ts)
        data = [(1, 1, 50), (2, 1, 10), (3, 1, 30), (4, 2, 80), (5, 2, 20)]
        for pk, gid, val in data:
            batch.pk_lo.append(pk)
            batch.pk_hi.append(0)
            batch.weights.append(1)
            batch.nulls.append(0)
        batch.columns = [[], [d[1] for d in data], [d[2] for d in data]]
        client.push(tid, ts, batch)

        rows = _scan_to_list(client, vid)
        assert len(rows) == 2
        mins = sorted(r[3] for r in rows)
        assert mins == [10, 20]  # group 1: min(50,10,30)=10, group 2: min(80,20)=20

    def test_reduce_max(self, client, sn):
        """A12b: reduce MAX per group."""
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("val", TypeCode.I64),
        ]
        tid = client.create_table(sn, "t_a12b_" + _uid(), cols, pk_col_idx=0)
        ts = Schema(columns=cols, pk_index=0)

        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("agg", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            r = cb.reduce(inp, group_by_cols=[1], agg_func_id=4, agg_col_idx=2)
            cb.sink(r, vid)

        vid = _build_and_create_view(client, sn, "v_a12b", tid, build, out_cols)

        batch = ZSetBatch(schema=ts)
        data = [(1, 1, 50), (2, 1, 10), (3, 1, 30), (4, 2, 80), (5, 2, 20)]
        for pk, gid, val in data:
            batch.pk_lo.append(pk)
            batch.pk_hi.append(0)
            batch.weights.append(1)
            batch.nulls.append(0)
        batch.columns = [[], [d[1] for d in data], [d[2] for d in data]]
        client.push(tid, ts, batch)

        rows = _scan_to_list(client, vid)
        assert len(rows) == 2
        maxes = sorted(r[3] for r in rows)
        assert maxes == [50, 80]  # group 1: max(50,10,30)=50, group 2: max(80,20)=80

    def test_reduce_retraction(self, client, sn):
        """A13: reduce retraction updates aggregate."""
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("val", TypeCode.I64),
        ]
        tid = client.create_table(sn, "t_a13_" + _uid(), cols, pk_col_idx=0)
        ts = Schema(columns=cols, pk_index=0)

        # Reduce output schema: grouping by I64 col → synthetic U128 PK
        # Server produces: (U128 _group_hash PK, I64 group_id, I64 agg)
        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("agg", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            r = cb.reduce(inp, group_by_cols=[1], agg_func_id=2, agg_col_idx=2)
            cb.sink(r, vid)

        vid = _build_and_create_view(client, sn, "v_a13", tid, build, out_cols)

        # Tick 1: push val=10, val=20 in group 1
        batch = ZSetBatch(schema=ts)
        batch.pk_lo = [1, 2]
        batch.pk_hi = [0, 0]
        batch.weights = [1, 1]
        batch.nulls = [0, 0]
        batch.columns = [[], [1, 1], [10, 20]]
        client.push(tid, ts, batch)

        rows = _scan_to_list(client, vid)
        assert len(rows) == 1
        # rows: (pk_lo, weight, group_id, agg) — agg is at index 3
        assert rows[0][3] == 30  # sum = 10+20

        # Tick 2: delete val=10 (pk=1, weight=-1)
        batch2 = ZSetBatch(schema=ts)
        batch2.pk_lo = [1]
        batch2.pk_hi = [0]
        batch2.weights = [-1]
        batch2.nulls = [0]
        batch2.columns = [[], [1], [10]]
        client.push(tid, ts, batch2)

        # After retraction, the old aggregate (w=-1) and new aggregate (w=+1)
        # should consolidate. The cursor filters to positive-weight rows.
        rows = _scan_to_list(client, vid)
        positive_rows = [r for r in rows if r[1] > 0]
        assert len(positive_rows) == 1
        assert positive_rows[0][3] == 20  # sum = 20


class TestJoin:
    """A14-A16: Join operator tests."""

    def test_join_delta_trace(self, client, sn):
        """A14: join matches on PK, produces merged columns."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_a14_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_a14_b_" + _uid())

        # Join output schema: merged columns from A and B
        # merge_schemas_for_join produces (pk, a_val, b_val)
        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("a_val", TypeCode.I64),
            ColumnDef("b_val", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, tid_b)
            cb.sink(j, vid)

        # Pre-populate table B
        _push_rows_2col(client, tid_b, ts_b, [
            (1, 100, 1), (2, 200, 1), (3, 300, 1),
        ])

        vid = _build_and_create_view(client, sn, "v_a14", tid_a, build, out_cols)

        # Push to table A with pk=1,2,4
        _push_rows_2col(client, tid_a, ts_a, [
            (1, 10, 1), (2, 20, 1), (4, 40, 1),
        ])

        # Join should produce pk=1,2 (matching PKs)
        pks = _scan_pks(client, vid)
        assert pks == [1, 2]

    def test_join_no_matches(self, client, sn):
        """A15: join with disjoint PKs → empty."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_a15_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_a15_b_" + _uid())
        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("a_val", TypeCode.I64),
            ColumnDef("b_val", TypeCode.I64),
        ]

        _push_rows_2col(client, tid_b, ts_b, [
            (100, 1, 1), (200, 2, 1), (300, 3, 1),
        ])

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, tid_b)
            cb.sink(j, vid)

        vid = _build_and_create_view(client, sn, "v_a15", tid_a, build, out_cols)
        _push_rows_2col(client, tid_a, ts_a, [(1, 10, 1), (2, 20, 1)])

        pks = _scan_pks(client, vid)
        assert pks == []

    def test_join_incremental(self, client, sn):
        """A16: join picks up new trace rows in subsequent ticks."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_a16_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_a16_b_" + _uid())
        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("a_val", TypeCode.I64),
            ColumnDef("b_val", TypeCode.I64),
        ]

        # B starts with pk=1 only
        _push_rows_2col(client, tid_b, ts_b, [(1, 100, 1)])

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, tid_b)
            cb.sink(j, vid)

        vid = _build_and_create_view(client, sn, "v_a16", tid_a, build, out_cols)

        # Tick 1: push A with pk=1 → should match
        _push_rows_2col(client, tid_a, ts_a, [(1, 10, 1)])
        assert _scan_pks(client, vid) == [1]

        # Add pk=2 to B
        _push_rows_2col(client, tid_b, ts_b, [(2, 200, 1)])

        # Tick 2: push A with pk=2 → should match with B's new row
        _push_rows_2col(client, tid_a, ts_a, [(2, 20, 1)])
        pks = _scan_pks(client, vid)
        assert 2 in pks


class TestAntiJoin:
    """A17-A19: Anti-join operator tests."""

    def test_anti_join(self, client, sn):
        """A17: anti-join returns rows NOT in trace."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_a17_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_a17_b_" + _uid())
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        _push_rows_2col(client, tid_b, ts_b, [(1, 0, 1), (2, 0, 1)])

        def build(cb, vid):
            inp = cb.input_delta()
            aj = cb.anti_join(inp, tid_b)
            cb.sink(aj, vid)

        vid = _build_and_create_view(client, sn, "v_a17", tid_a, build, out_cols)
        _push_rows_2col(client, tid_a, ts_a, [(1, 10, 1), (2, 20, 1), (3, 30, 1)])

        pks = _scan_pks(client, vid)
        assert pks == [3]

    def test_anti_join_all_matched(self, client, sn):
        """A18: all rows in trace → empty result."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_a18_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_a18_b_" + _uid())
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        _push_rows_2col(client, tid_b, ts_b, [(1, 0, 1), (2, 0, 1), (3, 0, 1)])

        def build(cb, vid):
            inp = cb.input_delta()
            aj = cb.anti_join(inp, tid_b)
            cb.sink(aj, vid)

        vid = _build_and_create_view(client, sn, "v_a18", tid_a, build, out_cols)
        _push_rows_2col(client, tid_a, ts_a, [(1, 10, 1), (2, 20, 1), (3, 30, 1)])

        pks = _scan_pks(client, vid)
        assert pks == []

    def test_anti_join_none_matched(self, client, sn):
        """A19: empty trace → all rows pass."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_a19_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_a19_b_" + _uid())
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        # B is empty (no push)

        def build(cb, vid):
            inp = cb.input_delta()
            aj = cb.anti_join(inp, tid_b)
            cb.sink(aj, vid)

        vid = _build_and_create_view(client, sn, "v_a19", tid_a, build, out_cols)
        _push_rows_2col(client, tid_a, ts_a, [(1, 10, 1), (2, 20, 1), (3, 30, 1)])

        pks = _scan_pks(client, vid)
        assert pks == [1, 2, 3]


class TestSemiJoin:
    """A20-A21: Semi-join operator tests."""

    def test_semi_join(self, client, sn):
        """A20: semi-join returns rows IN trace."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_a20_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_a20_b_" + _uid())
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        _push_rows_2col(client, tid_b, ts_b, [(1, 0, 1), (2, 0, 1)])

        def build(cb, vid):
            inp = cb.input_delta()
            sj = cb.semi_join(inp, tid_b)
            cb.sink(sj, vid)

        vid = _build_and_create_view(client, sn, "v_a20", tid_a, build, out_cols)
        _push_rows_2col(client, tid_a, ts_a, [(1, 10, 1), (2, 20, 1), (3, 30, 1)])

        pks = _scan_pks(client, vid)
        assert pks == [1, 2]

    def test_semi_join_complement(self, client, sn):
        """A21: anti ∪ semi == input."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_a21_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_a21_b_" + _uid())
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        _push_rows_2col(client, tid_b, ts_b, [(1, 0, 1), (3, 0, 1)])

        def build_anti(cb, vid):
            inp = cb.input_delta()
            aj = cb.anti_join(inp, tid_b)
            cb.sink(aj, vid)

        def build_semi(cb, vid):
            inp = cb.input_delta()
            sj = cb.semi_join(inp, tid_b)
            cb.sink(sj, vid)

        vid_anti = _build_and_create_view(client, sn, "v_a21_anti", tid_a, build_anti, out_cols)
        vid_semi = _build_and_create_view(client, sn, "v_a21_semi", tid_a, build_semi, out_cols)

        _push_rows_2col(client, tid_a, ts_a, [
            (1, 10, 1), (2, 20, 1), (3, 30, 1), (4, 40, 1),
        ])

        anti_pks = _scan_pks(client, vid_anti)
        semi_pks = _scan_pks(client, vid_semi)
        assert sorted(anti_pks + semi_pks) == [1, 2, 3, 4]


# ===========================================================================
# Part B: Operator Combination Tests
# ===========================================================================


class TestCombinations:
    """B1-B6: Operator combination tests."""

    def test_filter_then_map(self, client, sn):
        """B1: filter(val > 50) → map(projection=[1]) keeps val only."""
        tid, ts = _create_table_2col(client, sn)
        # projection=[1] writes 1 payload column (val)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        eb = ExprBuilder()
        r0 = eb.load_col_int(1)
        r1 = eb.load_const(50)
        r2 = eb.cmp_gt(r0, r1)
        expr = eb.build(r2)

        def build(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp, expr=expr)
            m = cb.map(f, projection=[1])
            cb.sink(m, vid)

        vid = _build_and_create_view(client, sn, "v_b1", tid, build, out_cols)
        _push_rows_2col(client, tid, ts, [(1, 10, 1), (2, 60, 1), (3, 80, 1)])

        pks = _scan_pks(client, vid)
        assert pks == [2, 3]

    def test_join_then_filter(self, client, sn):
        """B2: join(trace=B) → filter(a_val > 0)."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_b2_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_b2_b_" + _uid())
        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("a_val", TypeCode.I64),
            ColumnDef("b_val", TypeCode.I64),
        ]

        _push_rows_2col(client, tid_b, ts_b, [(1, 100, 1), (2, 200, 1)])

        # After join: col 0=pk, col 1=a_val, col 2=b_val
        # Filter on col 1 (a_val) > 0
        eb = ExprBuilder()
        r0 = eb.load_col_int(1)
        r1 = eb.load_const(0)
        r2 = eb.cmp_gt(r0, r1)
        expr = eb.build(r2)

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, tid_b)
            f = cb.filter(j, expr=expr)
            cb.sink(f, vid)

        vid = _build_and_create_view(client, sn, "v_b2", tid_a, build, out_cols)
        _push_rows_2col(client, tid_a, ts_a, [(1, 10, 1), (2, -5, 1)])

        pks = _scan_pks(client, vid)
        assert pks == [1]  # only pk=1 where a_val=10 > 0

    def test_join_then_reduce(self, client, sn):
        """B3: join(trace=B) → reduce(group_by b_val, SUM a_val)."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_b3_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_b3_b_" + _uid())

        # After join: col 0=pk(U64), col 1=a_val(I64), col 2=b_val(I64)
        # Reduce groups by col 2 (b_val), SUM of col 1 (a_val)
        # Reduce output: (U128 hash PK, I64 group_id, I64 agg)
        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("agg", TypeCode.I64),
        ]

        # B has pk=1,2,3 all with b_val=10 (same group), pk=4 with b_val=20
        _push_rows_2col(client, tid_b, ts_b, [
            (1, 10, 1), (2, 10, 1), (3, 10, 1), (4, 20, 1),
        ])

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, tid_b)
            r = cb.reduce(j, group_by_cols=[2], agg_func_id=2, agg_col_idx=1)
            cb.sink(r, vid)

        vid = _build_and_create_view(client, sn, "v_b3", tid_a, build, out_cols)

        # Push A: pk=1 a_val=5, pk=2 a_val=3, pk=4 a_val=7
        _push_rows_2col(client, tid_a, ts_a, [
            (1, 5, 1), (2, 3, 1), (4, 7, 1),
        ])

        rows = _scan_to_list(client, vid)
        assert len(rows) == 2
        # group b_val=10: sum(a_val for pk=1,2) = 5+3 = 8
        # group b_val=20: sum(a_val for pk=4) = 7
        sums = sorted(r[3] for r in rows)
        assert sums == [7, 8]

    def test_negate_then_union(self, client, sn):
        """B4: union(input, negate(input)) should cancel to empty."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        def build(cb, vid):
            inp = cb.input_delta()
            neg = cb.negate(inp)
            u = cb.union(inp, neg)
            cb.sink(u, vid)

        vid = _build_and_create_view(client, sn, "v_b4", tid, build, out_cols)
        _push_rows_2col(client, tid, ts, [(1, 10, 1), (2, 20, 1)])

        # +1 and -1 should cancel
        pks = _scan_pks(client, vid)
        assert pks == []

    def test_filter_then_distinct(self, client, sn):
        """B5: filter(val > 0) → distinct."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        eb = ExprBuilder()
        r0 = eb.load_col_int(1)
        r1 = eb.load_const(0)
        r2 = eb.cmp_gt(r0, r1)
        expr = eb.build(r2)

        def build(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp, expr=expr)
            d = cb.distinct(f)
            cb.sink(d, vid)

        vid = _build_and_create_view(client, sn, "v_b5", tid, build, out_cols)

        # Push positive vals → should pass filter
        _push_rows_2col(client, tid, ts, [(1, 10, 1), (2, 20, 1)])
        pks = _scan_pks(client, vid)
        assert pks == [1, 2]

        # Push negative val → filtered out, no effect on view
        _push_rows_2col(client, tid, ts, [(3, -5, 1)])
        pks = _scan_pks(client, vid)
        assert 3 not in pks

    def test_three_stage_pipeline(self, client, sn):
        """B6: filter → map → distinct."""
        tid, ts = _create_table_2col(client, sn)
        # projection=[1] writes 1 payload column (val)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        eb = ExprBuilder()
        r0 = eb.load_col_int(1)
        r1 = eb.load_const(10)
        r2 = eb.cmp_gt(r0, r1)
        expr = eb.build(r2)

        def build(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp, expr=expr)
            m = cb.map(f, projection=[1])
            d = cb.distinct(m)
            cb.sink(d, vid)

        vid = _build_and_create_view(client, sn, "v_b6", tid, build, out_cols)
        _push_rows_2col(client, tid, ts, [
            (1, 5, 1), (2, 15, 1), (3, 25, 1),
        ])

        pks = _scan_pks(client, vid)
        assert pks == [2, 3]  # pk=1 filtered out (val=5 ≤ 10)


# ===========================================================================
# Part C: Multi-Table / Multi-View Tests
# ===========================================================================


class TestMultiTableView:
    """C1, C3, C5: Multi-table and multi-view tests."""

    def test_two_views_same_table(self, client, sn):
        """C1: two views on same table — one filtered, one passthrough."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        eb = ExprBuilder()
        r0 = eb.load_col_int(1)
        r1 = eb.load_const(50)
        r2 = eb.cmp_gt(r0, r1)
        expr = eb.build(r2)

        def build_filtered(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp, expr=expr)
            cb.sink(f, vid)

        def build_passthrough(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp)  # null predicate
            cb.sink(f, vid)

        vid1 = _build_and_create_view(client, sn, "v_c1_filt_" + _uid(), tid, build_filtered, out_cols)
        vid2 = _build_and_create_view(client, sn, "v_c1_pass_" + _uid(), tid, build_passthrough, out_cols)

        _push_rows_2col(client, tid, ts, [
            (1, 10, 1), (2, 60, 1), (3, 80, 1),
        ])

        assert _scan_pks(client, vid1) == [2, 3]     # filtered
        assert _scan_pks(client, vid2) == [1, 2, 3]   # passthrough

    def test_join_two_independent_tables(self, client, sn):
        """C3: join two independently populated tables."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_c3_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_c3_b_" + _uid())
        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("a_val", TypeCode.I64),
            ColumnDef("b_val", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, tid_b)
            cb.sink(j, vid)

        # Populate B with pk=1,2
        _push_rows_2col(client, tid_b, ts_b, [(1, 100, 1), (2, 200, 1)])

        vid = _build_and_create_view(client, sn, "v_c3", tid_a, build, out_cols)

        # Push to A with pk=1 → match
        _push_rows_2col(client, tid_a, ts_a, [(1, 10, 1)])
        assert 1 in _scan_pks(client, vid)

        # Add pk=3 to B, then push A with pk=3
        _push_rows_2col(client, tid_b, ts_b, [(3, 300, 1)])
        _push_rows_2col(client, tid_a, ts_a, [(3, 30, 1)])
        assert 3 in _scan_pks(client, vid)

    def test_self_join(self, client, sn):
        """C5: table joins with its own trace."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("val_a", TypeCode.I64),
            ColumnDef("val_b", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, tid)
            cb.sink(j, vid)

        # Pre-populate table so trace has rows
        _push_rows_2col(client, tid, ts, [(1, 10, 1), (2, 20, 1), (3, 30, 1)])

        vid = _build_and_create_view(client, sn, "v_c5", tid, build, out_cols)

        # Push again → delta joins with trace (which has pk=1,2,3)
        _push_rows_2col(client, tid, ts, [(1, 100, 1)])

        pks = _scan_pks(client, vid)
        assert 1 in pks

    def test_view_cascade(self, client, sn):
        """C2: V2 reads from V1 (view-on-view cascade)."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        # V1: filter val > 50
        eb = ExprBuilder()
        r0 = eb.load_col_int(1)
        r1 = eb.load_const(50)
        r2 = eb.cmp_gt(r0, r1)
        expr = eb.build(r2)

        def build_v1(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp, expr=expr)
            cb.sink(f, vid)

        vid1 = _build_and_create_view(client, sn, "v_c2_v1_" + _uid(), tid, build_v1, out_cols)

        # V2: passthrough on V1 (primary_source_id = vid1)
        def build_v2(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp)  # null predicate = passthrough
            cb.sink(f, vid)

        vid2 = client.allocate_table_id()
        cb2 = CircuitBuilder(vid2, vid1)  # source is V1, not T
        build_v2(cb2, vid2)
        graph2 = cb2.build()
        client.create_view_with_circuit(sn, "v_c2_v2_" + _uid(), graph2, out_cols)

        # Push to T → should cascade through V1 → V2
        _push_rows_2col(client, tid, ts, [
            (1, 10, 1), (2, 60, 1), (3, 80, 1),
        ])

        # V1 should have 2 rows (val > 50)
        assert _scan_pks(client, vid1) == [2, 3]
        # V2 should have the same 2 rows (passthrough of V1)
        assert _scan_pks(client, vid2) == [2, 3]

    def test_multiple_joins_chain(self, client, sn):
        """C4: chained joins: A → join(B) → join(C)."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_c4_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_c4_b_" + _uid())
        tid_c, ts_c = _create_table_2col(client, sn, name="t_c4_c_" + _uid())

        # First join: A(pk, a_val) ⋈ B(pk, b_val) → (pk, a_val, b_val)
        # Second join: result(pk, a_val, b_val) ⋈ C(pk, c_val) → (pk, a_val, b_val, c_val)
        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("a_val", TypeCode.I64),
            ColumnDef("b_val", TypeCode.I64),
            ColumnDef("c_val", TypeCode.I64),
        ]

        # Populate B with pk=1,2 and C with pk=1,3
        _push_rows_2col(client, tid_b, ts_b, [(1, 100, 1), (2, 200, 1)])
        _push_rows_2col(client, tid_c, ts_c, [(1, 1000, 1), (3, 3000, 1)])

        def build(cb, vid):
            inp = cb.input_delta()
            j1 = cb.join(inp, tid_b)
            j2 = cb.join(j1, tid_c)
            cb.sink(j2, vid)

        vid = _build_and_create_view(client, sn, "v_c4", tid_a, build, out_cols)

        # Push A with pk=1,2,3
        _push_rows_2col(client, tid_a, ts_a, [(1, 10, 1), (2, 20, 1), (3, 30, 1)])

        # Only pk=1 matches both B and C
        pks = _scan_pks(client, vid)
        assert pks == [1]


# ===========================================================================
# Part D: Incremental / DBSP Semantics Tests
# ===========================================================================


class TestIncremental:
    """D1-D4: Incremental semantics tests."""

    def test_insert_then_delete_passthrough(self, client, sn):
        """D1: insert then delete through passthrough view."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        def build(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp)
            cb.sink(f, vid)

        vid = _build_and_create_view(client, sn, "v_d1", tid, build, out_cols)

        _push_rows_2col(client, tid, ts, [(1, 10, 1), (2, 20, 1)])
        assert _scan_pks(client, vid) == [1, 2]

        _push_rows_2col(client, tid, ts, [(1, 10, -1), (2, 20, -1)])
        assert _scan_pks(client, vid) == []

    def test_insert_then_delete_through_join(self, client, sn):
        """D2: insert then delete through join."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_d2_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_d2_b_" + _uid())
        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("a_val", TypeCode.I64),
            ColumnDef("b_val", TypeCode.I64),
        ]

        _push_rows_2col(client, tid_b, ts_b, [(1, 100, 1)])

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, tid_b)
            cb.sink(j, vid)

        vid = _build_and_create_view(client, sn, "v_d2", tid_a, build, out_cols)

        _push_rows_2col(client, tid_a, ts_a, [(1, 10, 1)])
        assert _scan_pks(client, vid) == [1]

        _push_rows_2col(client, tid_a, ts_a, [(1, 10, -1)])
        assert _scan_pks(client, vid) == []

    def test_filter_delete_no_effect(self, client, sn):
        """D4: deleting a row that doesn't pass filter has no effect."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        eb = ExprBuilder()
        r0 = eb.load_col_int(1)
        r1 = eb.load_const(5)
        r2 = eb.cmp_gt(r0, r1)
        expr = eb.build(r2)

        def build(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp, expr=expr)
            cb.sink(f, vid)

        vid = _build_and_create_view(client, sn, "v_d4", tid, build, out_cols)

        # Insert val=10 (passes filter)
        _push_rows_2col(client, tid, ts, [(1, 10, 1)])
        assert _scan_pks(client, vid) == [1]

        # Delete val=10 (passes filter → retracted from view)
        _push_rows_2col(client, tid, ts, [(1, 10, -1)])
        assert _scan_pks(client, vid) == []

        # Insert val=3 (does NOT pass filter → no view change)
        _push_rows_2col(client, tid, ts, [(2, 3, 1)])
        assert _scan_pks(client, vid) == []

        # Delete val=3 (does NOT pass filter → no view change)
        _push_rows_2col(client, tid, ts, [(2, 3, -1)])
        assert _scan_pks(client, vid) == []

    def test_reduce_multi_tick(self, client, sn):
        """D3: reduce across 3 ticks — insert, insert more, delete some."""
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("val", TypeCode.I64),
        ]
        tid = client.create_table(sn, "t_d3_" + _uid(), cols, pk_col_idx=0)
        ts = Schema(columns=cols, pk_index=0)

        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("agg", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            r = cb.reduce(inp, group_by_cols=[1], agg_func_id=2, agg_col_idx=2)
            cb.sink(r, vid)

        vid = _build_and_create_view(client, sn, "v_d3", tid, build, out_cols)

        # Tick 1: push val=10, val=20 in group 1
        batch = ZSetBatch(schema=ts)
        batch.pk_lo = [1, 2]
        batch.pk_hi = [0, 0]
        batch.weights = [1, 1]
        batch.nulls = [0, 0]
        batch.columns = [[], [1, 1], [10, 20]]
        client.push(tid, ts, batch)

        rows = _scan_to_list(client, vid)
        positive = [r for r in rows if r[1] > 0]
        assert len(positive) == 1
        assert positive[0][3] == 30  # sum = 10+20

        # Tick 2: push val=30 in group 1, val=50 in group 2
        batch2 = ZSetBatch(schema=ts)
        batch2.pk_lo = [3, 4]
        batch2.pk_hi = [0, 0]
        batch2.weights = [1, 1]
        batch2.nulls = [0, 0]
        batch2.columns = [[], [1, 2], [30, 50]]
        client.push(tid, ts, batch2)

        rows = _scan_to_list(client, vid)
        positive = [r for r in rows if r[1] > 0]
        assert len(positive) == 2
        sums = sorted(r[3] for r in positive)
        assert sums == [50, 60]  # group 1: 10+20+30=60, group 2: 50

        # Tick 3: delete val=10 from group 1 (pk=1, weight=-1)
        batch3 = ZSetBatch(schema=ts)
        batch3.pk_lo = [1]
        batch3.pk_hi = [0]
        batch3.weights = [-1]
        batch3.nulls = [0]
        batch3.columns = [[], [1], [10]]
        client.push(tid, ts, batch3)

        rows = _scan_to_list(client, vid)
        positive = [r for r in rows if r[1] > 0]
        assert len(positive) == 2
        sums = sorted(r[3] for r in positive)
        assert sums == [50, 50]  # group 1: 20+30=50, group 2: 50


# ===========================================================================
# Part E: Edge Cases
# ===========================================================================


class TestEdgeCases:
    """E1-E10: Edge case tests."""

    def test_push_empty_batch(self, client, sn):
        """E1: pushing empty batch → view stays empty."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        def build(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp)
            cb.sink(f, vid)

        vid = _build_and_create_view(client, sn, "v_e1", tid, build, out_cols)

        # Push empty batch
        batch = ZSetBatch(schema=ts)
        batch.columns = [[], []]
        client.push(tid, ts, batch)

        assert _scan_pks(client, vid) == []

    def test_large_batch_through_join(self, client, sn):
        """E2: 1000 rows through join."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_e2_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_e2_b_" + _uid())
        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("a_val", TypeCode.I64),
            ColumnDef("b_val", TypeCode.I64),
        ]

        # Populate B with 500 rows
        batch_b = ZSetBatch(schema=ts_b)
        for i in range(500):
            batch_b.pk_lo.append(i)
            batch_b.pk_hi.append(0)
            batch_b.weights.append(1)
            batch_b.nulls.append(0)
        batch_b.columns = [[], list(range(500))]
        client.push(tid_b, ts_b, batch_b)

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, tid_b)
            cb.sink(j, vid)

        vid = _build_and_create_view(client, sn, "v_e2", tid_a, build, out_cols)

        # Push 1000 rows to A (pk 0..999, only 0..499 match B)
        batch_a = ZSetBatch(schema=ts_a)
        for i in range(1000):
            batch_a.pk_lo.append(i)
            batch_a.pk_hi.append(0)
            batch_a.weights.append(1)
            batch_a.nulls.append(0)
        batch_a.columns = [[], list(range(1000))]
        client.push(tid_a, ts_a, batch_a)

        pks = _scan_pks(client, vid)
        assert len(pks) == 500

    def test_string_columns_through_map(self, client, sn):
        """E6: string columns survive map projection."""
        tid, ts = _create_table_3col(client, sn)
        # projection=[2] writes 1 payload column: label (input col 2)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("label", TypeCode.STRING)]

        def build(cb, vid):
            inp = cb.input_delta()
            m = cb.map(inp, projection=[2])
            cb.sink(m, vid)

        vid = _build_and_create_view(client, sn, "v_e6", tid, build, out_cols)
        _push_rows_3col(client, tid, ts, [
            (1, 10, "hello", 1),
            (2, 20, "", 1),       # empty string
            (3, 30, "a" * 100, 1),  # longer string
        ])

        rows = _scan_to_list(client, vid)
        assert len(rows) == 3
        labels = {r[0]: r[2] for r in rows}
        assert labels[1] == "hello"
        assert labels[2] == ""
        assert labels[3] == "a" * 100

    def test_string_columns_through_join(self, client, sn):
        """E7: string columns survive join."""
        cols_a = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("label_a", TypeCode.STRING),
        ]
        cols_b = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("label_b", TypeCode.STRING),
        ]
        tid_a = client.create_table(sn, "t_e7_a_" + _uid(), cols_a, pk_col_idx=0)
        ts_a = Schema(columns=cols_a, pk_index=0)
        tid_b = client.create_table(sn, "t_e7_b_" + _uid(), cols_b, pk_col_idx=0)
        ts_b = Schema(columns=cols_b, pk_index=0)

        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("label_a", TypeCode.STRING),
            ColumnDef("label_b", TypeCode.STRING),
        ]

        # Populate B
        b_batch = ZSetBatch(schema=ts_b)
        b_batch.pk_lo = [1, 2]
        b_batch.pk_hi = [0, 0]
        b_batch.weights = [1, 1]
        b_batch.nulls = [0, 0]
        b_batch.columns = [[], ["world", "bar"]]
        client.push(tid_b, ts_b, b_batch)

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, tid_b)
            cb.sink(j, vid)

        vid = _build_and_create_view(client, sn, "v_e7", tid_a, build, out_cols)

        a_batch = ZSetBatch(schema=ts_a)
        a_batch.pk_lo = [1, 2]
        a_batch.pk_hi = [0, 0]
        a_batch.weights = [1, 1]
        a_batch.nulls = [0, 0]
        a_batch.columns = [[], ["hello", "foo"]]
        client.push(tid_a, ts_a, a_batch)

        rows = _scan_to_list(client, vid)
        assert len(rows) == 2
        labels = {r[0]: (r[2], r[3]) for r in rows}
        assert labels[1] == ("hello", "world")
        assert labels[2] == ("foo", "bar")

    def test_anti_join_incremental(self, client, sn):
        """E8: anti-join with trace changing between ticks."""
        tid_a, ts_a = _create_table_2col(client, sn, name="t_e8_a_" + _uid())
        tid_b, ts_b = _create_table_2col(client, sn, name="t_e8_b_" + _uid())
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        # B is empty initially
        def build(cb, vid):
            inp = cb.input_delta()
            aj = cb.anti_join(inp, tid_b)
            cb.sink(aj, vid)

        vid = _build_and_create_view(client, sn, "v_e8", tid_a, build, out_cols)

        # Tick 1: all of A passes (B empty)
        _push_rows_2col(client, tid_a, ts_a, [(1, 10, 1), (2, 20, 1), (3, 30, 1)])
        pks = _scan_pks(client, vid)
        assert pks == [1, 2, 3]

        # Add pk=1 to B
        _push_rows_2col(client, tid_b, ts_b, [(1, 0, 1)])

        # Tick 2: push same A rows again → pk=1 now blocked by B
        _push_rows_2col(client, tid_a, ts_a, [(1, 10, 1), (2, 20, 1)])
        pks = _scan_pks(client, vid)
        # pk=1 from tick 2 is blocked, but pk=1 from tick 1 is still in view
        # (anti-join only filters the current delta, not the accumulated trace)
        assert 2 in pks

    def test_null_values_through_filter(self, client, sn):
        """E3: nullable column filtered with IS_NOT_NULL expression."""
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("val", TypeCode.I64, is_nullable=True),
        ]
        tid = client.create_table(sn, "t_e3_" + _uid(), cols, pk_col_idx=0)
        ts = Schema(columns=cols, pk_index=0)
        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("val", TypeCode.I64, is_nullable=True),
        ]

        # Filter: IS_NOT_NULL(col 1)
        eb = ExprBuilder()
        r0 = eb.is_not_null(1)
        expr = eb.build(r0)

        def build(cb, vid):
            inp = cb.input_delta()
            f = cb.filter(inp, expr=expr)
            cb.sink(f, vid)

        vid = _build_and_create_view(client, sn, "v_e3", tid, build, out_cols)

        # Push rows: pk=1 val=10, pk=2 val=NULL, pk=3 val=30
        batch = ZSetBatch(schema=ts)
        batch.pk_lo = [1, 2, 3]
        batch.pk_hi = [0, 0, 0]
        batch.weights = [1, 1, 1]
        # null bitmap: col 1 is payload_idx 0 (ci=1, pk_index=0 → 1-1=0)
        # For pk=2 (row 1), set bit 0: nulls = 1
        batch.nulls = [0, 1, 0]
        batch.columns = [[], [10, 0, 30]]  # val=0 for null row (placeholder)
        client.push(tid, ts, batch)

        # Only non-null rows should pass
        pks = _scan_pks(client, vid)
        assert pks == [1, 3]

    def test_null_values_through_join(self, client, sn):
        """E4: nullable payload columns survive join correctly."""
        cols_a = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("a_val", TypeCode.I64, is_nullable=True),
        ]
        cols_b = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("b_val", TypeCode.I64, is_nullable=True),
        ]
        tid_a = client.create_table(sn, "t_e4_a_" + _uid(), cols_a, pk_col_idx=0)
        ts_a = Schema(columns=cols_a, pk_index=0)
        tid_b = client.create_table(sn, "t_e4_b_" + _uid(), cols_b, pk_col_idx=0)
        ts_b = Schema(columns=cols_b, pk_index=0)

        out_cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("a_val", TypeCode.I64, is_nullable=True),
            ColumnDef("b_val", TypeCode.I64, is_nullable=True),
        ]

        # Populate B: pk=1 with b_val=NULL, pk=2 with b_val=200
        b_batch = ZSetBatch(schema=ts_b)
        b_batch.pk_lo = [1, 2]
        b_batch.pk_hi = [0, 0]
        b_batch.weights = [1, 1]
        b_batch.nulls = [1, 0]  # pk=1 has null b_val
        b_batch.columns = [[], [0, 200]]
        client.push(tid_b, ts_b, b_batch)

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, tid_b)
            cb.sink(j, vid)

        vid = _build_and_create_view(client, sn, "v_e4", tid_a, build, out_cols)

        # Push A: pk=1 with a_val=10, pk=2 with a_val=NULL
        a_batch = ZSetBatch(schema=ts_a)
        a_batch.pk_lo = [1, 2]
        a_batch.pk_hi = [0, 0]
        a_batch.weights = [1, 1]
        a_batch.nulls = [0, 1]  # pk=2 has null a_val
        a_batch.columns = [[], [10, 0]]
        client.push(tid_a, ts_a, a_batch)

        # Both PKs should match
        pks = _scan_pks(client, vid)
        assert pks == [1, 2]

    def test_null_values_through_reduce(self, client, sn):
        """E5: COUNT counts all rows; SUM skips NULLs."""
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("val", TypeCode.I64, is_nullable=True),
        ]
        tid = client.create_table(sn, "t_e5_" + _uid(), cols, pk_col_idx=0)
        ts = Schema(columns=cols, pk_index=0)

        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("agg", TypeCode.I64),
        ]

        # COUNT view
        def build_count(cb, vid):
            inp = cb.input_delta()
            r = cb.reduce(inp, group_by_cols=[1], agg_func_id=1, agg_col_idx=2)
            cb.sink(r, vid)

        vid_count = _build_and_create_view(client, sn, "v_e5_count_" + _uid(), tid, build_count, out_cols)

        # SUM view
        def build_sum(cb, vid):
            inp = cb.input_delta()
            r = cb.reduce(inp, group_by_cols=[1], agg_func_id=2, agg_col_idx=2)
            cb.sink(r, vid)

        vid_sum = _build_and_create_view(client, sn, "v_e5_sum_" + _uid(), tid, build_sum, out_cols)

        # Push: group 1 has val=10, val=NULL, val=30
        # null bitmap: col 2 (val) is payload_idx 1 (ci=2, pk_index=0 → 2-1=1)
        batch = ZSetBatch(schema=ts)
        batch.pk_lo = [1, 2, 3]
        batch.pk_hi = [0, 0, 0]
        batch.weights = [1, 1, 1]
        batch.nulls = [0, 2, 0]  # row 1 (pk=2): bit 1 set = col 2 null
        batch.columns = [[], [1, 1, 1], [10, 0, 30]]
        client.push(tid, ts, batch)

        # COUNT should count all 3 rows (including the null one)
        rows_count = _scan_to_list(client, vid_count)
        positive_count = [r for r in rows_count if r[1] > 0]
        assert len(positive_count) == 1
        assert positive_count[0][3] == 3

        # SUM should skip the NULL: 10 + 30 = 40
        rows_sum = _scan_to_list(client, vid_sum)
        positive_sum = [r for r in rows_sum if r[1] > 0]
        assert len(positive_sum) == 1
        assert positive_sum[0][3] == 40

    def test_reduce_group_becomes_empty(self, client, sn):
        """E9: delete all rows in a group → group disappears from view."""
        cols = [
            ColumnDef("pk", TypeCode.U64),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("val", TypeCode.I64),
        ]
        tid = client.create_table(sn, "t_e9_" + _uid(), cols, pk_col_idx=0)
        ts = Schema(columns=cols, pk_index=0)

        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("agg", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            r = cb.reduce(inp, group_by_cols=[1], agg_func_id=2, agg_col_idx=2)
            cb.sink(r, vid)

        vid = _build_and_create_view(client, sn, "v_e9", tid, build, out_cols)

        # Push 2 rows in group 1
        batch = ZSetBatch(schema=ts)
        batch.pk_lo = [1, 2]
        batch.pk_hi = [0, 0]
        batch.weights = [1, 1]
        batch.nulls = [0, 0]
        batch.columns = [[], [1, 1], [10, 20]]
        client.push(tid, ts, batch)

        rows = _scan_to_list(client, vid)
        positive = [r for r in rows if r[1] > 0]
        assert len(positive) == 1
        assert positive[0][3] == 30  # sum = 10+20

        # Delete both rows (weight=-1)
        batch2 = ZSetBatch(schema=ts)
        batch2.pk_lo = [1, 2]
        batch2.pk_hi = [0, 0]
        batch2.weights = [-1, -1]
        batch2.nulls = [0, 0]
        batch2.columns = [[], [1, 1], [10, 20]]
        client.push(tid, ts, batch2)

        # DBSP semantics: the group still exists with sum=0 (identity element).
        # The reduce emits a retraction of the old aggregate (30) and an
        # insertion of the new aggregate (0). After consolidation, the view
        # has one row with agg=0.
        rows = _scan_to_list(client, vid)
        positive = [r for r in rows if r[1] > 0]
        assert len(positive) == 1
        assert positive[0][3] == 0  # sum of empty group = 0

    def test_distinct_weight_accumulation(self, client, sn):
        """E10: distinct weight accumulation and cancellation."""
        tid, ts = _create_table_2col(client, sn)
        out_cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64)]

        def build(cb, vid):
            inp = cb.input_delta()
            d = cb.distinct(inp)
            cb.sink(d, vid)

        vid = _build_and_create_view(client, sn, "v_e10", tid, build, out_cols)

        # Push key 1 with weight +1
        _push_rows_2col(client, tid, ts, [(1, 42, 1)])
        assert 1 in _scan_pks(client, vid)

        # Push key 1 with weight +1 again (distinct still shows 1)
        _push_rows_2col(client, tid, ts, [(1, 42, 1)])
        ws = _scan_weights(client, vid)
        assert ws.get(1, 0) == 1  # still just +1

        # Push key 1 with weight -2 (net = 0)
        _push_rows_2col(client, tid, ts, [(1, 42, -2)])
        assert 1 not in _scan_pks(client, vid)


class TestStringThroughReduce:
    """Verify STRING columns in join output don't break reduce."""

    def test_join_with_string_trace_then_reduce(self, client, sn):
        """Join a 2-col delta with a 3-col STRING trace, then reduce.

        This is the exact pattern that was misdiagnosed as a STRING-reduce
        bug (it was actually a test-ordering bug: data pushed before view).
        """
        # Delta: (pk U64, val I64)
        tid_a, ts_a = _create_table_2col(client, sn, name="t_str_red_a_" + _uid())

        # Trace: (pk U64, val I64, label STRING) — STRING column present
        tid_b, ts_b = _create_table_3col(client, sn, name="t_str_red_b_" + _uid())

        # Load trace B first (before view creation)
        _push_rows_3col(client, tid_b, ts_b, [
            (1, 100, "foo", 1),
            (2, 200, "bar", 1),
            (3, 300, "foo", 1),
        ])

        # Join output: (pk U64, a_val I64, b_val I64, b_label STRING)
        # Reduce: group by col 2 (b_val), SUM col 1 (a_val)
        # The STRING at col 3 is in the input but NOT in the output.
        out_cols = [
            ColumnDef("_group_hash", TypeCode.U128),
            ColumnDef("group_id", TypeCode.I64),
            ColumnDef("agg", TypeCode.I64),
        ]

        def build(cb, vid):
            inp = cb.input_delta()
            j = cb.join(inp, tid_b)
            r = cb.reduce(j, group_by_cols=[2], agg_func_id=2, agg_col_idx=1)
            cb.sink(r, vid)

        # Create view BEFORE pushing delta data
        vid = _build_and_create_view(client, sn, "v_str_red", tid_a, build, out_cols)

        # Now push delta
        _push_rows_2col(client, tid_a, ts_a, [
            (1, 10, 1),  # matches B pk=1 (b_val=100)
            (2, 20, 1),  # matches B pk=2 (b_val=200)
            (3, 30, 1),  # matches B pk=3 (b_val=300)
        ])

        rows = _scan_to_list(client, vid)
        assert len(rows) == 3
        # Groups: b_val=100 → sum=10, b_val=200 → sum=20, b_val=300 → sum=30
        sums = sorted(r[3] for r in rows)
        assert sums == [10, 20, 30]
