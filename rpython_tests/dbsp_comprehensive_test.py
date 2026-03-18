# dbsp_comprehensive_test.py

import sys
import os

from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.longlong2float import float2longlong

from gnitz.core import types, batch, errors
from gnitz.core.batch import RowBuilder
from gnitz.dbsp.ops import linear, join, anti_join, reduce, distinct, source, group_index
from gnitz.dbsp import functions
from gnitz.storage.ephemeral_table import EphemeralTable


# ------------------------------------------------------------------------------
# RPython Debugging Helpers
# ------------------------------------------------------------------------------


def log(msg):
    """Writing to stdout is safe and visible during RPython test execution."""
    os.write(1, msg + "\n")


def fail(msg):
    os.write(2, "CRITICAL FAILURE: " + msg + "\n")
    raise Exception(msg)


def assert_true(condition, msg):
    if not condition:
        fail(msg)


def assert_equal_i(expected, actual, msg):
    if expected != actual:
        fail(msg + " (Expected " + str(expected) + ", got " + str(actual) + ")")


def assert_equal_i64(expected, actual, msg):
    if expected != actual:
        fail(msg + " (i64 mismatch)")


def assert_equal_u128(expected, actual, msg):
    if expected != actual:
        fail(msg + " (u128 mismatch)")


def cleanup_dir(path):
    if not os.path.exists(path):
        return
    for item in os.listdir(path):
        p = os.path.join(path, item)
        if os.path.isdir(p):
            cleanup_dir(p)
        else:
            os.unlink(p)
    os.rmdir(path)


# ------------------------------------------------------------------------------
# Test Suites
# ------------------------------------------------------------------------------


def test_linear_ops(base_dir):
    log("[DBSP] Testing Linear Ops...")

    cols = newlist_hint(3)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val_int", is_nullable=True))
    cols.append(types.ColumnDefinition(types.TYPE_F64, name="val_float"))
    schema = types.TableSchema(cols, 0)

    b_in = batch.ArenaZSetBatch(schema)
    b_filtered = batch.ArenaZSetBatch(schema)
    b_mapped = None
    b_neg = batch.ArenaZSetBatch(schema)
    b_union = batch.ArenaZSetBatch(schema)
    b_delay = None

    try:
        # 1. Null handling in Filter/Map
        rb = RowBuilder(schema, b_in)
        rb.begin(r_uint128(1), r_int64(1))
        rb.put_null()  # val_int is null
        rb.put_float(1.5)
        rb.commit()

        rb.begin(r_uint128(2), r_int64(2))
        rb.put_int(r_int64(20))
        rb.put_float(15.5)
        rb.commit()

        log("  - Filter + Nulls...")
        # Predicate: val_float > 10.0
        f_gt = functions.UniversalPredicate(
            2, functions.OP_GT, r_uint64(float2longlong(10.0)), is_float=True
        )
        linear.op_filter(b_in, b_filtered, f_gt)
        assert_equal_i(1, b_filtered.length(), "Filter failed to drop row")

        log("  - Map + Nulls...")
        map_cols = newlist_hint(2)
        map_cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
        map_cols.append(
            types.ColumnDefinition(types.TYPE_I64, name="val_int", is_nullable=True)
        )
        map_schema = types.TableSchema(map_cols, 0)
        b_mapped = batch.ArenaZSetBatch(map_schema)

        # Project column 1 (val_int) which has a null in row 1
        mapper = functions.UniversalProjection([1], [types.TYPE_I64.code])
        linear.op_map(b_in, batch.BatchWriter(b_mapped), mapper, map_schema)
        assert_true(b_mapped.get_accessor(0).is_null(1), "Map failed to propagate null")

        log("  - Negate & Union...")
        linear.op_negate(b_in, b_neg)
        linear.op_union(b_in, b_neg, b_union)

        # Functional consolidation: we must use the result or a Scope to avoid leaks
        with batch.ConsolidatedScope(b_union) as b_union_cons:
            assert_equal_i(0, b_union_cons.length(), "Union/Negate failed to annihilate")

        log("  - Union single-input sorted/consolidated propagation (Opt 1)...")
        b_cons = b_in.to_consolidated()
        b_empty = batch.ArenaZSetBatch(schema)
        b_union_single = batch.ArenaZSetBatch(schema)
        try:
            linear.op_union(b_cons, b_empty, b_union_single)
            assert_true(b_union_single._sorted,
                        "Union single-input: _sorted should propagate from consolidated input")
            assert_true(b_union_single._consolidated,
                        "Union single-input: _consolidated should propagate")
            assert_equal_i(b_in.length(), b_union_single.length(),
                           "Union single-input: row count mismatch")
        finally:
            if b_cons is not b_in:
                b_cons.free()
            b_empty.free()
            b_union_single.free()

        log("  - Delay & Integrate...")
        b_delay = batch.ArenaZSetBatch(schema)
        linear.op_delay(b_in, b_delay)
        assert_equal_i(2, b_delay.length(), "Delay failed")

        sink_path = os.path.join(base_dir, "sink")
        sink = EphemeralTable(sink_path, "sink", schema)
        linear.op_integrate(b_in, sink)
        assert_true(sink.has_pk(r_uint128(1)), "Integrate failed to sink data")
        sink.close()

    finally:
        b_in.free()
        b_filtered.free()
        if b_mapped:
            b_mapped.free()
        b_neg.free()
        b_union.free()
        if b_delay:
            b_delay.free()


def test_join_ops(base_dir):
    log("[DBSP] Testing Join Ops (M:N)...")

    cols_l = newlist_hint(2)
    cols_l.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols_l.append(types.ColumnDefinition(types.TYPE_I64, name="val_l"))
    schema_l = types.TableSchema(cols_l, 0)

    cols_r = newlist_hint(2)
    cols_r.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols_r.append(types.ColumnDefinition(types.TYPE_STRING, name="val_r"))
    schema_r = types.TableSchema(cols_r, 0)

    schema_out = types.merge_schemas_for_join(schema_l, schema_r)
    b_l = batch.ArenaZSetBatch(schema_l)
    b_r = batch.ArenaZSetBatch(schema_r)
    b_out = batch.ArenaZSetBatch(schema_out)

    try:
        # Left: 2 DISTINCT rows for PK 10
        rb_l = RowBuilder(schema_l, b_l)
        rb_l.begin(r_uint128(10), r_int64(1))
        rb_l.put_int(r_int64(1))
        rb_l.commit()

        rb_l.begin(r_uint128(10), r_int64(1))
        rb_l.put_int(r_int64(2))
        rb_l.commit()

        # Right: 3 DISTINCT rows for PK 10
        rb_r = RowBuilder(schema_r, b_r)
        rb_r.begin(r_uint128(10), r_int64(1))
        rb_r.put_string("match1")
        rb_r.commit()

        rb_r.begin(r_uint128(10), r_int64(1))
        rb_r.put_string("match2")
        rb_r.commit()

        rb_r.begin(r_uint128(10), r_int64(1))
        rb_r.put_string("match3")
        rb_r.commit()

        log("  - Delta-Delta Sort-Merge (M:N)...")
        join.op_join_delta_delta(b_l, b_r, batch.BatchWriter(b_out), schema_l, schema_r)
        assert_equal_i(6, b_out.length(), "M:N Join failed")

        log("  - Delta-Trace Index-Nested...")
        b_out.clear()
        trace_path = os.path.join(base_dir, "j_trace")
        trace_r = EphemeralTable(trace_path, "tr", schema_r)

        # Ingest the 3 rows. Because the strings differ, they won't be consolidated.
        trace_r.ingest_batch(b_r)

        cursor_r = trace_r.create_cursor()
        join.op_join_delta_trace(b_l, cursor_r, batch.BatchWriter(b_out), schema_l, schema_r)
        cursor_r.close()

        assert_equal_i(6, b_out.length(), "Delta-Trace Join failed")

        # Verify the weight multiplication still holds
        for i in range(b_out.length()):
            assert_equal_i64(r_int64(1), b_out.get_weight(i), "Join weight error")

        trace_r.close()

    finally:
        b_l.free()
        b_r.free()
        b_out.free()


def test_distinct_op(base_dir):
    log("[DBSP] Testing Distinct...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="v"))
    schema = types.TableSchema(cols, 0)

    trace_path = os.path.join(base_dir, "d_trace")
    trace = EphemeralTable(trace_path, "dist", schema)

    b_in = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        rb = RowBuilder(schema, b_in)

        # Tick 1: Weight 10 -> Should output Weight 1
        rb.begin(r_uint128(1), r_int64(10))
        rb.put_int(r_int64(1))
        rb.commit()

        cursor = trace.create_cursor()
        distinct.op_distinct(b_in, cursor, trace, batch.BatchWriter(b_out))
        cursor.close()
        assert_equal_i64(r_int64(1), b_out.get_weight(0), "Distinct failed to clamp")

        # Tick 2: Weight -5 -> Should output Weight 0 (Total 5 is still > 0)
        b_in.clear()
        b_out.clear()
        rb.begin(r_uint128(1), r_int64(-5))
        rb.put_int(r_int64(1))
        rb.commit()

        cursor = trace.create_cursor()
        distinct.op_distinct(b_in, cursor, trace, batch.BatchWriter(b_out))
        cursor.close()
        assert_equal_i(0, b_out.length(), "Distinct produced unnecessary update")

        # Tick 3: Weight -5 -> Should output Weight -1 (Total 0)
        b_in.clear()
        b_out.clear()
        rb.begin(r_uint128(1), r_int64(-5))
        rb.put_int(r_int64(1))
        rb.commit()

        cursor = trace.create_cursor()
        distinct.op_distinct(b_in, cursor, trace, batch.BatchWriter(b_out))
        cursor.close()
        assert_equal_i64(r_int64(-1), b_out.get_weight(0), "Distinct failed to retract")

    finally:
        b_in.free()
        b_out.free()
        trace.close()


def _make_reduce_schema():
    """Shared input schema: grp (U64, col 0), val (I64, col 1), pk (U64, col 2)."""
    cols = newlist_hint(3)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="grp"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    return types.TableSchema(cols, 2)  # PK is col 2


def _add_row(rb, pk, grp, val, weight):
    rb.begin(r_uint128(pk), r_int64(weight))
    rb.put_int(r_int64(grp))
    rb.put_int(r_int64(val))
    rb.commit()


def _find_insertion(b_out):
    """Returns the aggregate value (as signed i64) from the +1 weight record."""
    for i in range(b_out.length()):
        if b_out.get_weight(i) == r_int64(1):
            return b_out.get_accessor(i).get_int_signed(1)
    return r_int64(-9999999)


def test_reduce_op(base_dir):
    log("[DBSP] Testing Reduce (Linear & Non-Linear)...")
    in_schema = _make_reduce_schema()

    sum_agg = functions.UniversalAccumulator(1, functions.AGG_SUM, types.TYPE_I64)
    out_schema = types._build_reduce_output_schema(in_schema, [0], [sum_agg])

    t_in_path = os.path.join(base_dir, "red_in")
    t_out_path = os.path.join(base_dir, "red_out")
    trace_in = EphemeralTable(t_in_path, "in", in_schema)
    trace_out = EphemeralTable(t_out_path, "out", out_schema)

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)

        # --- Linear SUM: initial insertion ---
        log("  - Linear Sum...")
        _add_row(rb, pk=1, grp=10, val=5, weight=1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], [sum_agg], out_schema)
        assert_equal_i64(r_int64(1), b_out.get_weight(0), "Reduce insertion failed")
        assert_equal_i64(r_int64(5), _find_insertion(b_out), "Sum error")

        trace_in.ingest_batch(b_in)
        trace_out.ingest_batch(b_out)

        # --- Linear SUM: incremental update (retraction + new value) ---
        log("  - Retraction & Shortcut...")
        b_in.clear()
        b_out.clear()
        _add_row(rb, pk=2, grp=10, val=5, weight=1)

        c_in.close()
        c_out.close()
        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], [sum_agg], out_schema)

        with batch.ConsolidatedScope(b_out) as b_out_cons:
            assert_equal_i(2, b_out_cons.length(), "Incremental reduce failed to retract")

        c_in.close()
        c_out.close()
    finally:
        b_in.free()
        b_out.free()
        trace_in.close()
        trace_out.close()


def test_reduce_min_retraction(base_dir):
    """
    Regression test for MIN/MAX ignoring weight.
    History: grp=10 has val=5 (pk=1) and val=10 (pk=2).
    Delta: retract pk=1 (val=5, w=-1).
    Expected: MIN changes from 5 to 10.
    """
    log("[DBSP] Testing Reduce MIN retraction...")
    in_schema = _make_reduce_schema()
    min_agg = functions.UniversalAccumulator(1, functions.AGG_MIN, types.TYPE_I64)
    out_schema = types._build_reduce_output_schema(in_schema, [0], [min_agg])

    t_in_path = os.path.join(base_dir, "min_in")
    t_out_path = os.path.join(base_dir, "min_out")
    trace_in = EphemeralTable(t_in_path, "in", in_schema)
    trace_out = EphemeralTable(t_out_path, "out", out_schema)

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)

        # Tick 1: Insert two rows into grp=10
        log("  - Tick 1: insert val=5, val=10...")
        _add_row(rb, pk=1, grp=10, val=5, weight=1)
        _add_row(rb, pk=2, grp=10, val=10, weight=1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], [min_agg], out_schema)
        assert_equal_i64(r_int64(5), _find_insertion(b_out), "Initial MIN should be 5")

        trace_in.ingest_batch(b_in)
        trace_out.ingest_batch(b_out)
        c_in.close()
        c_out.close()

        # Tick 2: Retract val=5 (pk=1). MIN should become 10.
        log("  - Tick 2: retract val=5, expect MIN=10...")
        b_in.clear()
        b_out.clear()
        _add_row(rb, pk=1, grp=10, val=5, weight=-1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], [min_agg], out_schema)
        assert_equal_i64(r_int64(10), _find_insertion(b_out), "MIN after retraction should be 10")

        c_in.close()
        c_out.close()
    finally:
        b_in.free()
        b_out.free()
        trace_in.close()
        trace_out.close()


def test_reduce_max_retraction(base_dir):
    """
    MAX retraction: retract the maximum value, verify MAX drops.
    History: grp=10 has val=5 (pk=1) and val=100 (pk=2).
    Delta: retract pk=2 (val=100, w=-1).
    Expected: MAX changes from 100 to 5.
    """
    log("[DBSP] Testing Reduce MAX retraction...")
    in_schema = _make_reduce_schema()
    max_agg = functions.UniversalAccumulator(1, functions.AGG_MAX, types.TYPE_I64)
    out_schema = types._build_reduce_output_schema(in_schema, [0], [max_agg])

    t_in_path = os.path.join(base_dir, "max_in")
    t_out_path = os.path.join(base_dir, "max_out")
    trace_in = EphemeralTable(t_in_path, "in", in_schema)
    trace_out = EphemeralTable(t_out_path, "out", out_schema)

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)

        # Tick 1: Insert two rows
        log("  - Tick 1: insert val=5, val=100...")
        _add_row(rb, pk=1, grp=10, val=5, weight=1)
        _add_row(rb, pk=2, grp=10, val=100, weight=1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], [max_agg], out_schema)
        assert_equal_i64(r_int64(100), _find_insertion(b_out), "Initial MAX should be 100")

        trace_in.ingest_batch(b_in)
        trace_out.ingest_batch(b_out)
        c_in.close()
        c_out.close()

        # Tick 2: Retract val=100 (pk=2). MAX should become 5.
        log("  - Tick 2: retract val=100, expect MAX=5...")
        b_in.clear()
        b_out.clear()
        _add_row(rb, pk=2, grp=10, val=100, weight=-1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], [max_agg], out_schema)
        assert_equal_i64(r_int64(5), _find_insertion(b_out), "MAX after retraction should be 5")

        c_in.close()
        c_out.close()
    finally:
        b_in.free()
        b_out.free()
        trace_in.close()
        trace_out.close()


def test_reduce_group_becomes_empty(base_dir):
    """
    When all records in a group are retracted, only a retraction should
    be emitted (no insertion), effectively deleting the aggregate.
    """
    log("[DBSP] Testing Reduce group becomes empty...")
    in_schema = _make_reduce_schema()
    min_agg = functions.UniversalAccumulator(1, functions.AGG_MIN, types.TYPE_I64)
    out_schema = types._build_reduce_output_schema(in_schema, [0], [min_agg])

    t_in_path = os.path.join(base_dir, "empty_in")
    t_out_path = os.path.join(base_dir, "empty_out")
    trace_in = EphemeralTable(t_in_path, "in", in_schema)
    trace_out = EphemeralTable(t_out_path, "out", out_schema)

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)

        # Tick 1: Insert one row
        _add_row(rb, pk=1, grp=10, val=42, weight=1)
        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], [min_agg], out_schema)
        assert_equal_i64(r_int64(42), _find_insertion(b_out), "Initial MIN should be 42")

        trace_in.ingest_batch(b_in)
        trace_out.ingest_batch(b_out)
        c_in.close()
        c_out.close()

        # Tick 2: Retract the only row. Group should vanish.
        b_in.clear()
        b_out.clear()
        _add_row(rb, pk=1, grp=10, val=42, weight=-1)
        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], [min_agg], out_schema)

        # Output should have exactly one record: the retraction (w=-1)
        assert_equal_i(1, b_out.length(), "Empty group should emit only retraction")
        assert_equal_i64(r_int64(-1), b_out.get_weight(0), "Should be a retraction")

        c_in.close()
        c_out.close()
    finally:
        b_in.free()
        b_out.free()
        trace_in.close()
        trace_out.close()


def test_reduce_multiple_groups(base_dir):
    """
    Verify that multiple groups in the same delta are aggregated independently.
    """
    log("[DBSP] Testing Reduce multiple groups...")
    in_schema = _make_reduce_schema()
    sum_agg = functions.UniversalAccumulator(1, functions.AGG_SUM, types.TYPE_I64)
    out_schema = types._build_reduce_output_schema(in_schema, [0], [sum_agg])

    t_in_path = os.path.join(base_dir, "multi_in")
    t_out_path = os.path.join(base_dir, "multi_out")
    trace_in = EphemeralTable(t_in_path, "in", in_schema)
    trace_out = EphemeralTable(t_out_path, "out", out_schema)

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)

        # Two groups in one delta: grp=10 and grp=20
        _add_row(rb, pk=1, grp=10, val=3, weight=1)
        _add_row(rb, pk=2, grp=10, val=7, weight=1)
        _add_row(rb, pk=3, grp=20, val=100, weight=1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], [sum_agg], out_schema)

        assert_equal_i(2, b_out.length(), "Should have 2 group outputs")

        # Collect results by group key (PK = group key for natural PK)
        grp10_sum = r_int64(0)
        grp20_sum = r_int64(0)
        for i in range(b_out.length()):
            grp = r_uint64(b_out.get_pk(i))
            val = b_out.get_accessor(i).get_int_signed(1)
            if grp == r_uint64(10):
                grp10_sum = val
            elif grp == r_uint64(20):
                grp20_sum = val

        assert_equal_i64(r_int64(10), grp10_sum, "grp=10 SUM should be 3+7=10")
        assert_equal_i64(r_int64(100), grp20_sum, "grp=20 SUM should be 100")

        c_in.close()
        c_out.close()
    finally:
        b_in.free()
        b_out.free()
        trace_in.close()
        trace_out.close()


def test_reduce_count(base_dir):
    """
    COUNT aggregate: insert 3 rows, retract 1. Verify count goes from 3 to 2.
    """
    log("[DBSP] Testing Reduce COUNT...")
    in_schema = _make_reduce_schema()
    count_agg = functions.UniversalAccumulator(1, functions.AGG_COUNT, types.TYPE_I64)
    out_schema = types._build_reduce_output_schema(in_schema, [0], [count_agg])

    t_in_path = os.path.join(base_dir, "cnt_in")
    t_out_path = os.path.join(base_dir, "cnt_out")
    trace_in = EphemeralTable(t_in_path, "in", in_schema)
    trace_out = EphemeralTable(t_out_path, "out", out_schema)

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)

        # Tick 1: Insert 3 rows
        _add_row(rb, pk=1, grp=10, val=1, weight=1)
        _add_row(rb, pk=2, grp=10, val=2, weight=1)
        _add_row(rb, pk=3, grp=10, val=3, weight=1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], [count_agg], out_schema)
        assert_equal_i64(r_int64(3), _find_insertion(b_out), "Initial COUNT should be 3")

        trace_in.ingest_batch(b_in)
        trace_out.ingest_batch(b_out)
        c_in.close()
        c_out.close()

        # Tick 2: Retract one row. COUNT should become 2.
        b_in.clear()
        b_out.clear()
        _add_row(rb, pk=2, grp=10, val=2, weight=-1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], [count_agg], out_schema)
        assert_equal_i64(r_int64(2), _find_insertion(b_out), "COUNT after retraction should be 2")

        c_in.close()
        c_out.close()
    finally:
        b_in.free()
        b_out.free()
        trace_in.close()
        trace_out.close()


def test_anti_join_basic(base_dir):
    """
    Anti-join: A has keys 1,2,3. B has keys 2,3. Output should be key 1 only.
    """
    log("[DBSP] Testing Anti-Join basic...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b_a = batch.ArenaZSetBatch(schema)
    b_b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        rb_a = RowBuilder(schema, b_a)
        rb_a.begin(r_uint128(1), r_int64(1))
        rb_a.put_int(r_int64(10))
        rb_a.commit()
        rb_a.begin(r_uint128(2), r_int64(1))
        rb_a.put_int(r_int64(20))
        rb_a.commit()
        rb_a.begin(r_uint128(3), r_int64(1))
        rb_a.put_int(r_int64(30))
        rb_a.commit()

        rb_b = RowBuilder(schema, b_b)
        rb_b.begin(r_uint128(2), r_int64(1))
        rb_b.put_int(r_int64(200))
        rb_b.commit()
        rb_b.begin(r_uint128(3), r_int64(1))
        rb_b.put_int(r_int64(300))
        rb_b.commit()

        log("  - Delta-Delta Anti-Join...")
        anti_join.op_anti_join_delta_delta(b_a, b_b, b_out, schema)
        assert_equal_i(1, b_out.length(), "Anti-join should emit 1 row")
        assert_equal_u128(r_uint128(1), b_out.get_pk(0), "Anti-join should emit key 1")
        assert_equal_i64(r_int64(1), b_out.get_weight(0), "Anti-join weight should be 1")

        log("  - Delta-Trace Anti-Join...")
        b_out.clear()
        trace_path = os.path.join(base_dir, "aj_trace")
        trace_b = EphemeralTable(trace_path, "tr", schema)
        trace_b.ingest_batch(b_b)

        cursor_b = trace_b.create_cursor()
        anti_join.op_anti_join_delta_trace(b_a, cursor_b, batch.BatchWriter(b_out), schema)
        cursor_b.close()

        assert_equal_i(1, b_out.length(), "Anti-join DT should emit 1 row")
        assert_equal_u128(r_uint128(1), b_out.get_pk(0), "Anti-join DT should emit key 1")

        trace_b.close()

    finally:
        b_a.free()
        b_b.free()
        b_out.free()


def test_anti_join_empty_right(base_dir):
    """When B is empty, all of A should be emitted."""
    log("[DBSP] Testing Anti-Join empty right...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b_a = batch.ArenaZSetBatch(schema)
    b_b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        rb_a = RowBuilder(schema, b_a)
        rb_a.begin(r_uint128(1), r_int64(1))
        rb_a.put_int(r_int64(10))
        rb_a.commit()
        rb_a.begin(r_uint128(2), r_int64(1))
        rb_a.put_int(r_int64(20))
        rb_a.commit()

        anti_join.op_anti_join_delta_delta(b_a, b_b, b_out, schema)
        assert_equal_i(2, b_out.length(), "Empty-right anti-join should emit all A rows")

    finally:
        b_a.free()
        b_b.free()
        b_out.free()


def test_anti_join_full_overlap(base_dir):
    """When all A keys exist in B, output should be empty."""
    log("[DBSP] Testing Anti-Join full overlap...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b_a = batch.ArenaZSetBatch(schema)
    b_b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        rb_a = RowBuilder(schema, b_a)
        rb_a.begin(r_uint128(1), r_int64(1))
        rb_a.put_int(r_int64(10))
        rb_a.commit()

        rb_b = RowBuilder(schema, b_b)
        rb_b.begin(r_uint128(1), r_int64(1))
        rb_b.put_int(r_int64(99))
        rb_b.commit()

        anti_join.op_anti_join_delta_delta(b_a, b_b, b_out, schema)
        assert_equal_i(0, b_out.length(), "Full-overlap anti-join should be empty")

    finally:
        b_a.free()
        b_b.free()
        b_out.free()


def test_anti_join_weight_semantics(base_dir):
    """
    Only positive-weight B records count as 'present'.
    B has key=1 with w=-1 (retraction) -> should NOT suppress A's key=1.
    """
    log("[DBSP] Testing Anti-Join weight semantics...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b_a = batch.ArenaZSetBatch(schema)
    b_b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        rb_a = RowBuilder(schema, b_a)
        rb_a.begin(r_uint128(1), r_int64(1))
        rb_a.put_int(r_int64(10))
        rb_a.commit()

        # B has key=1 but with negative weight (retraction)
        rb_b = RowBuilder(schema, b_b)
        rb_b.begin(r_uint128(1), r_int64(-1))
        rb_b.put_int(r_int64(99))
        rb_b.commit()

        anti_join.op_anti_join_delta_delta(b_a, b_b, b_out, schema)
        assert_equal_i(1, b_out.length(), "Negative-weight B should not suppress A")
        assert_equal_u128(r_uint128(1), b_out.get_pk(0), "Should emit key 1")

    finally:
        b_a.free()
        b_b.free()
        b_out.free()


def test_semi_join_basic(base_dir):
    """
    Semi-join: A has keys 1,2,3. B has keys 2,3. Output should be keys 2,3.
    """
    log("[DBSP] Testing Semi-Join basic...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b_a = batch.ArenaZSetBatch(schema)
    b_b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        rb_a = RowBuilder(schema, b_a)
        rb_a.begin(r_uint128(1), r_int64(1))
        rb_a.put_int(r_int64(10))
        rb_a.commit()
        rb_a.begin(r_uint128(2), r_int64(1))
        rb_a.put_int(r_int64(20))
        rb_a.commit()
        rb_a.begin(r_uint128(3), r_int64(1))
        rb_a.put_int(r_int64(30))
        rb_a.commit()

        rb_b = RowBuilder(schema, b_b)
        rb_b.begin(r_uint128(2), r_int64(1))
        rb_b.put_int(r_int64(200))
        rb_b.commit()
        rb_b.begin(r_uint128(3), r_int64(1))
        rb_b.put_int(r_int64(300))
        rb_b.commit()

        log("  - Delta-Delta Semi-Join...")
        anti_join.op_semi_join_delta_delta(b_a, b_b, b_out, schema)
        assert_equal_i(2, b_out.length(), "Semi-join should emit 2 rows")

        # Verify the output has left-side values (not right-side)
        found_key2 = False
        found_key3 = False
        for i in range(b_out.length()):
            pk = b_out.get_pk(i)
            val = b_out.get_accessor(i).get_int_signed(1)
            if pk == r_uint128(2):
                found_key2 = True
                assert_equal_i64(r_int64(20), val, "Semi-join key 2 should have left val")
            elif pk == r_uint128(3):
                found_key3 = True
                assert_equal_i64(r_int64(30), val, "Semi-join key 3 should have left val")
        assert_true(found_key2, "Semi-join missing key 2")
        assert_true(found_key3, "Semi-join missing key 3")

        log("  - Delta-Trace Semi-Join...")
        b_out.clear()
        trace_path = os.path.join(base_dir, "sj_trace")
        trace_b = EphemeralTable(trace_path, "tr", schema)
        trace_b.ingest_batch(b_b)

        cursor_b = trace_b.create_cursor()
        anti_join.op_semi_join_delta_trace(b_a, cursor_b, batch.BatchWriter(b_out), schema)
        cursor_b.close()

        assert_equal_i(2, b_out.length(), "Semi-join DT should emit 2 rows")

        trace_b.close()

    finally:
        b_a.free()
        b_b.free()
        b_out.free()


def test_anti_semi_join_complement(base_dir):
    """
    Anti-join and semi-join should be complements:
    anti_join(A, B) + semi_join(A, B) = A (after consolidation).
    """
    log("[DBSP] Testing Anti/Semi-Join complement property...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b_a = batch.ArenaZSetBatch(schema)
    b_b = batch.ArenaZSetBatch(schema)
    b_anti = batch.ArenaZSetBatch(schema)
    b_semi = batch.ArenaZSetBatch(schema)

    try:
        rb_a = RowBuilder(schema, b_a)
        for pk in range(1, 6):  # keys 1..5
            rb_a.begin(r_uint128(pk), r_int64(1))
            rb_a.put_int(r_int64(pk * 10))
            rb_a.commit()

        rb_b = RowBuilder(schema, b_b)
        for pk in [2, 4]:  # B has keys 2 and 4
            rb_b.begin(r_uint128(pk), r_int64(1))
            rb_b.put_int(r_int64(pk * 100))
            rb_b.commit()

        anti_join.op_anti_join_delta_delta(b_a, b_b, b_anti, schema)
        anti_join.op_semi_join_delta_delta(b_a, b_b, b_semi, schema)

        total = b_anti.length() + b_semi.length()
        assert_equal_i(5, total, "Anti + Semi should cover all A rows")
        assert_equal_i(3, b_anti.length(), "Anti-join should have 3 rows (keys 1,3,5)")
        assert_equal_i(2, b_semi.length(), "Semi-join should have 2 rows (keys 2,4)")

    finally:
        b_a.free()
        b_b.free()
        b_anti.free()
        b_semi.free()


def test_join_adaptive_swap(base_dir):
    """
    Swap path: delta 10 rows, trace 2 rows → delta_len > trace_len triggers swap.
    Verify output matches expected join result (PKs 3 and 7 match).
    """
    log("[DBSP] Testing Join adaptive swap path...")

    cols_l = newlist_hint(2)
    cols_l.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols_l.append(types.ColumnDefinition(types.TYPE_I64, name="val_l"))
    schema_l = types.TableSchema(cols_l, 0)

    cols_r = newlist_hint(2)
    cols_r.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols_r.append(types.ColumnDefinition(types.TYPE_I64, name="val_r"))
    schema_r = types.TableSchema(cols_r, 0)

    schema_out = types.merge_schemas_for_join(schema_l, schema_r)
    b_l = batch.ArenaZSetBatch(schema_l)
    b_r = batch.ArenaZSetBatch(schema_r)
    b_out = batch.ArenaZSetBatch(schema_out)

    try:
        rb_l = RowBuilder(schema_l, b_l)
        for pk in range(1, 11):  # 10 distinct delta rows
            rb_l.begin(r_uint128(pk), r_int64(1))
            rb_l.put_int(r_int64(pk * 10))
            rb_l.commit()

        rb_r = RowBuilder(schema_r, b_r)
        rb_r.begin(r_uint128(3), r_int64(1))
        rb_r.put_int(r_int64(300))
        rb_r.commit()
        rb_r.begin(r_uint128(7), r_int64(1))
        rb_r.put_int(r_int64(700))
        rb_r.commit()

        trace_path = os.path.join(base_dir, "jas_trace")
        trace_r = EphemeralTable(trace_path, "tr", schema_r)
        trace_r.ingest_batch(b_r)

        cursor_r = trace_r.create_cursor()
        join.op_join_delta_trace(b_l, cursor_r, batch.BatchWriter(b_out), schema_l, schema_r)
        cursor_r.close()

        assert_equal_i(2, b_out.length(), "Swap join: expected 2 output rows")
        found_3 = False
        found_7 = False
        for i in range(b_out.length()):
            pk = b_out.get_pk(i)
            if pk == r_uint128(3):
                found_3 = True
            elif pk == r_uint128(7):
                found_7 = True
        assert_true(found_3, "Swap join: missing PK 3")
        assert_true(found_7, "Swap join: missing PK 7")

        trace_r.close()
    finally:
        b_l.free()
        b_r.free()
        b_out.free()
    log("  PASSED")


def test_join_merge_walk(base_dir):
    """
    Merge-walk path: consolidated delta (5 rows), larger trace (8 rows).
    delta_len <= trace_len and _consolidated -> merge-walk path.
    """
    log("[DBSP] Testing Join merge-walk path...")

    cols_l = newlist_hint(2)
    cols_l.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols_l.append(types.ColumnDefinition(types.TYPE_I64, name="val_l"))
    schema_l = types.TableSchema(cols_l, 0)

    cols_r = newlist_hint(2)
    cols_r.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols_r.append(types.ColumnDefinition(types.TYPE_I64, name="val_r"))
    schema_r = types.TableSchema(cols_r, 0)

    schema_out = types.merge_schemas_for_join(schema_l, schema_r)
    b_l = batch.ArenaZSetBatch(schema_l)
    b_r = batch.ArenaZSetBatch(schema_r)
    b_out = batch.ArenaZSetBatch(schema_out)

    try:
        rb_l = RowBuilder(schema_l, b_l)
        for pk in range(1, 6):  # 5 distinct consolidated delta rows
            rb_l.begin(r_uint128(pk), r_int64(1))
            rb_l.put_int(r_int64(pk * 10))
            rb_l.commit()
        b_l.mark_consolidated(True)

        rb_r = RowBuilder(schema_r, b_r)
        for pk in range(1, 9):  # 8 trace rows (superset of delta PKs)
            rb_r.begin(r_uint128(pk), r_int64(1))
            rb_r.put_int(r_int64(pk * 100))
            rb_r.commit()

        trace_path = os.path.join(base_dir, "jmw_trace")
        trace_r = EphemeralTable(trace_path, "tr", schema_r)
        trace_r.ingest_batch(b_r)

        cursor_r = trace_r.create_cursor()
        join.op_join_delta_trace(b_l, cursor_r, batch.BatchWriter(b_out), schema_l, schema_r)
        cursor_r.close()

        # All 5 delta PKs (1-5) match trace PKs (1-8) -> 5 output rows
        assert_equal_i(5, b_out.length(), "Merge-walk join: expected 5 output rows")
        assert_true(b_out._sorted, "Merge-walk join: output should be sorted")

        trace_r.close()
    finally:
        b_l.free()
        b_r.free()
        b_out.free()
    log("  PASSED")


def test_anti_join_merge_walk(base_dir):
    """
    Anti-join merge-walk: consolidated delta PKs 1-5.
    Trace: PK 2 (+1), PK 3 (+1), PK 4 (-1).
    Expected output: PKs 1, 4, 5 (PK 4 has negative weight so not suppressed).
    """
    log("[DBSP] Testing Anti-Join merge-walk path...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b_a = batch.ArenaZSetBatch(schema)
    b_t = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        rb_a = RowBuilder(schema, b_a)
        for pk in range(1, 6):
            rb_a.begin(r_uint128(pk), r_int64(1))
            rb_a.put_int(r_int64(pk * 10))
            rb_a.commit()
        b_a.mark_consolidated(True)

        rb_t = RowBuilder(schema, b_t)
        rb_t.begin(r_uint128(2), r_int64(1))   # positive: suppresses delta PK 2
        rb_t.put_int(r_int64(200))
        rb_t.commit()
        rb_t.begin(r_uint128(3), r_int64(1))   # positive: suppresses delta PK 3
        rb_t.put_int(r_int64(300))
        rb_t.commit()
        rb_t.begin(r_uint128(4), r_int64(-1))  # negative: does NOT suppress delta PK 4
        rb_t.put_int(r_int64(400))
        rb_t.commit()

        trace_path = os.path.join(base_dir, "ajmw_trace")
        trace_t = EphemeralTable(trace_path, "tr", schema)
        trace_t.ingest_batch(b_t)

        cursor_t = trace_t.create_cursor()
        anti_join.op_anti_join_delta_trace(b_a, cursor_t, batch.BatchWriter(b_out), schema)
        cursor_t.close()

        assert_equal_i(3, b_out.length(), "Anti-join merge-walk: expected 3 rows (PKs 1,4,5)")
        assert_true(b_out._consolidated, "Anti-join merge-walk: output should be consolidated")
        found_1 = False
        found_4 = False
        found_5 = False
        for i in range(b_out.length()):
            pk = b_out.get_pk(i)
            if pk == r_uint128(1):
                found_1 = True
            elif pk == r_uint128(4):
                found_4 = True
            elif pk == r_uint128(5):
                found_5 = True
        assert_true(found_1, "Anti-join merge-walk: missing PK 1")
        assert_true(found_4, "Anti-join merge-walk: missing PK 4 (negative trace weight)")
        assert_true(found_5, "Anti-join merge-walk: missing PK 5")

        trace_t.close()
    finally:
        b_a.free()
        b_t.free()
        b_out.free()
    log("  PASSED")


def test_semi_join_merge_walk(base_dir):
    """
    Semi-join merge-walk: consolidated delta (3 rows), trace larger (5 rows) to
    avoid triggering the swap path (3 <= 5).
    Trace: PK 2 (+1), PK 3 (+1), PK 4 (-1), PKs 5,6 (+1).
    Expected output: PKs 2, 3 only (PK 1 not in trace, PK 4 has negative weight).
    """
    log("[DBSP] Testing Semi-Join merge-walk path...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b_a = batch.ArenaZSetBatch(schema)
    b_t = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        # Delta: 3 rows (PKs 1, 2, 3) — consolidated, trace has 5 rows so no swap
        rb_a = RowBuilder(schema, b_a)
        for pk in range(1, 4):
            rb_a.begin(r_uint128(pk), r_int64(1))
            rb_a.put_int(r_int64(pk * 10))
            rb_a.commit()
        b_a.mark_consolidated(True)

        rb_t = RowBuilder(schema, b_t)
        rb_t.begin(r_uint128(2), r_int64(1))   # positive: matches delta PK 2
        rb_t.put_int(r_int64(200))
        rb_t.commit()
        rb_t.begin(r_uint128(3), r_int64(1))   # positive: matches delta PK 3
        rb_t.put_int(r_int64(300))
        rb_t.commit()
        rb_t.begin(r_uint128(4), r_int64(-1))  # negative weight (extra trace entry)
        rb_t.put_int(r_int64(400))
        rb_t.commit()
        rb_t.begin(r_uint128(5), r_int64(1))
        rb_t.put_int(r_int64(500))
        rb_t.commit()
        rb_t.begin(r_uint128(6), r_int64(1))
        rb_t.put_int(r_int64(600))
        rb_t.commit()

        trace_path = os.path.join(base_dir, "sjmw_trace")
        trace_t = EphemeralTable(trace_path, "tr", schema)
        trace_t.ingest_batch(b_t)

        cursor_t = trace_t.create_cursor()
        anti_join.op_semi_join_delta_trace(b_a, cursor_t, batch.BatchWriter(b_out), schema)
        cursor_t.close()

        # PK 1: not in trace → not emitted; PKs 2,3: positive weight → emitted
        assert_equal_i(2, b_out.length(), "Semi-join merge-walk: expected 2 rows (PKs 2,3)")
        assert_true(b_out._consolidated, "Semi-join merge-walk: output should be consolidated")
        found_2 = False
        found_3 = False
        for i in range(b_out.length()):
            pk = b_out.get_pk(i)
            if pk == r_uint128(2):
                found_2 = True
                assert_equal_i64(r_int64(20), b_out.get_accessor(i).get_int_signed(1),
                                 "Semi-join merge-walk: PK 2 should have left val")
            elif pk == r_uint128(3):
                found_3 = True
                assert_equal_i64(r_int64(30), b_out.get_accessor(i).get_int_signed(1),
                                 "Semi-join merge-walk: PK 3 should have left val")
        assert_true(found_2, "Semi-join merge-walk: missing PK 2")
        assert_true(found_3, "Semi-join merge-walk: missing PK 3")

        trace_t.close()
    finally:
        b_a.free()
        b_t.free()
        b_out.free()
    log("  PASSED")


def test_semi_join_dt_nonconsolidated(base_dir):
    """
    Semi-join DT: non-consolidated delta (2 rows), trace larger (3 rows) so swap
    does not fire (delta_len=2 <= trace_len=3). Exercises ConsolidatedScope else branch.
    Delta PKs: 1, 2 (not consolidated). Trace PKs: 2 (+1), 3 (+1), 4 (+1).
    Expected output: PK 2 only. Output must be consolidated.
    """
    log("[DBSP] Testing Semi-Join DT non-consolidated path...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b_a = batch.ArenaZSetBatch(schema)
    b_t = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        # Delta: 2 rows (PKs 1, 2), _consolidated explicitly False
        rb_a = RowBuilder(schema, b_a)
        rb_a.begin(r_uint128(1), r_int64(1))
        rb_a.put_int(r_int64(10))
        rb_a.commit()
        rb_a.begin(r_uint128(2), r_int64(1))
        rb_a.put_int(r_int64(20))
        rb_a.commit()
        # leave _consolidated = False (default)

        # Trace: 3 rows (PKs 2, 3, 4) — all positive weight so delta_len (2) <= trace_len (3)
        rb_t = RowBuilder(schema, b_t)
        rb_t.begin(r_uint128(2), r_int64(1))
        rb_t.put_int(r_int64(200))
        rb_t.commit()
        rb_t.begin(r_uint128(3), r_int64(1))
        rb_t.put_int(r_int64(300))
        rb_t.commit()
        rb_t.begin(r_uint128(4), r_int64(1))
        rb_t.put_int(r_int64(400))
        rb_t.commit()

        trace_path = os.path.join(base_dir, "sjdtnc_trace")
        trace_t = EphemeralTable(trace_path, "tr", schema)
        trace_t.ingest_batch(b_t)

        cursor_t = trace_t.create_cursor()
        anti_join.op_semi_join_delta_trace(b_a, cursor_t, batch.BatchWriter(b_out), schema)
        cursor_t.close()

        assert_equal_i(1, b_out.length(), "Semi-join DT nonconsolidated: expected 1 row (PK 2)")
        assert_true(b_out._consolidated, "Semi-join DT nonconsolidated: output should be consolidated")
        assert_true(b_out.get_pk(0) == r_uint128(2),
                    "Semi-join DT nonconsolidated: expected PK 2")
        assert_equal_i64(r_int64(20), b_out.get_accessor(0).get_int_signed(1),
                         "Semi-join DT nonconsolidated: val should be 20 (from delta)")

        trace_t.close()
    finally:
        b_a.free()
        b_t.free()
        b_out.free()
    log("  PASSED")


def test_source_ops(base_dir):
    log("[DBSP] Testing Source Ops...")

    cols = newlist_hint(1)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    schema = types.TableSchema(cols, 0)

    table_path = os.path.join(base_dir, "src_table")
    table = EphemeralTable(table_path, "src", schema)

    b_in = batch.ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b_in)
    for i in range(10):
        rb.begin(r_uint128(i), r_int64(1))
        rb.commit()
    table.ingest_batch(b_in)

    b_out = batch.ArenaZSetBatch(schema)
    cursor = table.create_cursor()

    # Per AbstractCursor protocol, cursors must be seeked before scanning.
    source.op_seek_trace(cursor, r_uint128(0))

    # Scan in chunks of 3
    n1 = source.op_scan_trace(cursor, b_out, 3)
    assert_equal_i(3, n1, "Chunk scan 1 failed")
    assert_equal_i(3, b_out.length(), "Batch fill failed")

    n2 = source.op_scan_trace(cursor, b_out, 100)
    assert_equal_i(7, n2, "Chunk scan 2 failed")
    assert_equal_i(10, b_out.length(), "Total scan mismatch")

    cursor.close()
    b_in.free()
    b_out.free()
    table.close()


# ------------------------------------------------------------------------------
# Group-Index helpers and test
# ------------------------------------------------------------------------------


def _update_group_idx(b_in, gi):
    """Populate or update the group secondary index from a delta batch."""
    n = b_in.length()
    if n == 0:
        return
    gi_acc = group_index.GroupIdxAccessor()
    acc = b_in.get_accessor(0)
    for i in range(n):
        b_in.bind_accessor(i, acc)
        if acc.is_null(gi.col_idx):
            continue
        gc_u64 = group_index.promote_group_col_to_u64(acc, gi.col_idx, gi.col_type)
        source_pk = b_in.get_pk(i)
        ck = ((r_uint128(gc_u64) << 64)
              | r_uint128(r_uint64(intmask(source_pk))))
        gi_acc.spk_hi = r_int64(intmask(source_pk >> 64))
        weight = b_in.get_weight(i)
        try:
            gi.table.memtable.upsert_single(ck, weight, gi_acc)
        except errors.MemTableFullError:
            gi.table.flush()
            gi.table.memtable.upsert_single(ck, weight, gi_acc)


def _find_min_for_dept(b_out, dept_id):
    """
    Find the MIN salary insertion in the reduce output for a given dept_id.
    Output schema: (U128 hash [pk=0], I64 dept_id [col 1], I64 agg [col 2]).
    Returns r_int64(-9999999) if not found.
    """
    for i in range(b_out.length()):
        if b_out.get_weight(i) == r_int64(1):
            acc = b_out.get_accessor(i)
            if acc.get_int_signed(1) == dept_id:
                return acc.get_int_signed(2)
    return r_int64(-9999999)


def test_reduce_group_idx(base_dir):
    """
    Group secondary index: non-PK I64 group-by triggers O(log N + k) indexed path.
    Schema: (pk: U128, dept_id: I64, salary: I64); GROUP BY dept_id; MIN(salary).
    10 departments, 10 rows each (salaries 0..9 within each dept).
    """
    log("[DBSP] Testing Reduce non-PK group secondary index...")

    cols = newlist_hint(3)
    cols.append(types.ColumnDefinition(types.TYPE_U128, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="dept_id"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="salary"))
    in_schema = types.TableSchema(cols, pk_index=0)

    min_agg = functions.UniversalAccumulator(2, functions.AGG_MIN, types.TYPE_I64)
    out_schema = types._build_reduce_output_schema(in_schema, [1], [min_agg])

    t_in_path = os.path.join(base_dir, "gi_trace_in")
    t_out_path = os.path.join(base_dir, "gi_trace_out")
    t_gi_path = os.path.join(base_dir, "gi_idx")
    trace_in = EphemeralTable(t_in_path, "in", in_schema)
    trace_out = EphemeralTable(t_out_path, "out", out_schema)
    gi_schema = group_index.make_group_idx_schema()
    gi_table = EphemeralTable(t_gi_path, "gidx", gi_schema, table_id=0)
    gi = group_index.ReduceGroupIndex(gi_table, 1, types.TYPE_I64)

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)

        # Tick 1: Insert 100 rows, 10 per dept.
        # dept d: pk = d*10+1 .. d*10+10, salary = d*10+0 .. d*10+9
        # MIN per dept d = d*10.
        log("  - Tick 1: insert 100 rows across 10 depts...")
        for dept in range(10):
            for row in range(10):
                actual_pk = dept * 10 + row + 1
                salary = dept * 10 + row
                rb.begin(r_uint128(actual_pk), r_int64(1))
                rb.put_int(r_int64(dept))
                rb.put_int(r_int64(salary))
                rb.commit()

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [1], [min_agg], out_schema, gi)
        c_in.close()
        c_out.close()

        # Verify initial MIN per department
        for dept in range(10):
            expected = r_int64(dept * 10)
            actual = _find_min_for_dept(b_out, r_int64(dept))
            assert_equal_i64(expected, actual,
                             "Tick 1 MIN for dept " + str(dept))

        # Populate traces and group index before Tick 2
        trace_in.ingest_batch(b_in)
        trace_out.ingest_batch(b_out)
        _update_group_idx(b_in, gi)
        b_in.clear()
        b_out.clear()

        # Tick 2: Retract the minimum-salary row in each dept.
        # After retraction, MIN per dept d should become d*10+1.
        log("  - Tick 2: retract min-salary row in each dept...")
        for dept in range(10):
            actual_pk = dept * 10 + 1  # row=0 of each dept
            salary = dept * 10         # = d*10 + 0
            rb.begin(r_uint128(actual_pk), r_int64(-1))
            rb.put_int(r_int64(dept))
            rb.put_int(r_int64(salary))
            rb.commit()

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [1], [min_agg], out_schema, gi)
        c_in.close()
        c_out.close()

        for dept in range(10):
            expected = r_int64(dept * 10 + 1)
            actual = _find_min_for_dept(b_out, r_int64(dept))
            assert_equal_i64(expected, actual,
                             "Tick 2 MIN for dept " + str(dept))

        log("[DBSP] Reduce non-PK group index test PASSED")
    finally:
        b_in.free()
        b_out.free()
        trace_in.close()
        trace_out.close()
        gi_table.close()


# ------------------------------------------------------------------------------
# AggValueIndex helpers and tests
# ------------------------------------------------------------------------------


def _update_avi(b_in, avi):
    """Populate or update the AggValueIndex from a delta batch."""
    n = b_in.length()
    if n == 0:
        return
    acc = b_in.get_accessor(0)
    for i in range(n):
        b_in.bind_accessor(i, acc)
        if acc.is_null(avi.agg_col_idx):
            continue
        gc_u64 = group_index._extract_gc_u64(acc, avi.input_schema, avi.group_by_cols)
        av_u64 = group_index.promote_agg_col_to_u64_ordered(
            acc, avi.agg_col_idx, avi.agg_col_type, avi.for_max,
        )
        ck = (r_uint128(gc_u64) << 64) | r_uint128(av_u64)
        weight = b_in.get_weight(i)
        try:
            avi.table.memtable.upsert_single(ck, weight, group_index._UNIT_GI_ACC)
        except errors.MemTableFullError:
            avi.table.flush()
            avi.table.memtable.upsert_single(ck, weight, group_index._UNIT_GI_ACC)


def test_reduce_min_agg_value_idx(base_dir):
    """
    AggValueIndex path: MIN(salary) GROUP BY dept_id (I64), no trace_in (Opt 2).
    Schema: (pk: U128, dept_id: I64, salary: I64).
    10 departments, 10 rows each (salaries d*10+0 .. d*10+9).
    """
    log("[DBSP] Testing AggValueIndex MIN path...")

    cols = newlist_hint(3)
    cols.append(types.ColumnDefinition(types.TYPE_U128, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="dept_id"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="salary"))
    in_schema = types.TableSchema(cols, pk_index=0)

    min_agg = functions.UniversalAccumulator(2, functions.AGG_MIN, types.TYPE_I64)
    out_schema = types._build_reduce_output_schema(in_schema, [1], [min_agg])

    t_out_path = os.path.join(base_dir, "avi_trace_out")
    t_avi_path = os.path.join(base_dir, "avi_idx")
    trace_out = EphemeralTable(t_out_path, "out", out_schema)
    avi_schema = group_index.make_agg_value_idx_schema()
    avi_table = EphemeralTable(t_avi_path, "avidx", avi_schema, table_id=0)
    avi = group_index.AggValueIndex(
        avi_table, [1], in_schema, 2, types.TYPE_I64, False,
    )

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)

        # Tick 1: Insert 100 rows, 10 per dept.
        log("  - AVI Tick 1: insert 100 rows across 10 depts...")
        for dept in range(10):
            for row in range(10):
                actual_pk = dept * 10 + row + 1
                salary = dept * 10 + row
                rb.begin(r_uint128(actual_pk), r_int64(1))
                rb.put_int(r_int64(dept))
                rb.put_int(r_int64(salary))
                rb.commit()

        _update_avi(b_in, avi)

        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, None, c_out, b_out, [1], [min_agg], out_schema,
                         agg_value_idx=avi)
        c_out.close()

        for dept in range(10):
            expected = r_int64(dept * 10)
            actual = _find_min_for_dept(b_out, r_int64(dept))
            assert_equal_i64(expected, actual, "AVI Tick 1 MIN for dept " + str(dept))

        trace_out.ingest_batch(b_out)
        b_in.clear()
        b_out.clear()
        min_agg.reset()

        # Tick 2: Retract min-salary row in each dept; new MIN = d*10+1.
        log("  - AVI Tick 2: retract min-salary row in each dept...")
        for dept in range(10):
            actual_pk = dept * 10 + 1
            salary = dept * 10
            rb.begin(r_uint128(actual_pk), r_int64(-1))
            rb.put_int(r_int64(dept))
            rb.put_int(r_int64(salary))
            rb.commit()

        _update_avi(b_in, avi)

        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, None, c_out, b_out, [1], [min_agg], out_schema,
                         agg_value_idx=avi)
        c_out.close()

        for dept in range(10):
            expected = r_int64(dept * 10 + 1)
            actual = _find_min_for_dept(b_out, r_int64(dept))
            assert_equal_i64(expected, actual, "AVI Tick 2 MIN for dept " + str(dept))

        trace_out.ingest_batch(b_out)
        b_in.clear()
        b_out.clear()
        min_agg.reset()

        # Tick 3: Retract ALL rows in dept 5; no output for dept 5.
        log("  - AVI Tick 3: retract all rows in dept 5...")
        for row in range(1, 10):   # row 0 already retracted in Tick 2
            actual_pk = 5 * 10 + row + 1
            salary = 5 * 10 + row
            rb.begin(r_uint128(actual_pk), r_int64(-1))
            rb.put_int(r_int64(5))
            rb.put_int(r_int64(salary))
            rb.commit()

        _update_avi(b_in, avi)

        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, None, c_out, b_out, [1], [min_agg], out_schema,
                         agg_value_idx=avi)
        c_out.close()

        # dept 5 should produce only a retraction (weight=-1), no insertion
        found_positive = False
        for i in range(b_out.length()):
            if b_out.get_weight(i) == r_int64(1):
                acc_i = b_out.get_accessor(i)
                if acc_i.get_int_signed(1) == r_int64(5):
                    found_positive = True
        assert_true(not found_positive, "AVI Tick 3: dept 5 should have no +1 output")

        log("[DBSP] AggValueIndex MIN path test PASSED")
    finally:
        b_in.free()
        b_out.free()
        trace_out.close()
        avi_table.close()


def _find_min_for_dept_year(b_out, dept_id, year_id):
    """Find MIN salary for a specific (dept_id, year) group in output batch."""
    for i in range(b_out.length()):
        if b_out.get_weight(i) == r_int64(1):
            acc = b_out.get_accessor(i)
            if (acc.get_int_signed(1) == dept_id
                    and acc.get_int_signed(2) == year_id):
                return acc.get_int_signed(3)
    return r_int64(-9999999)


def test_reduce_min_multi_col_group(base_dir):
    """
    AggValueIndex with multi-column GROUP BY: MIN(salary) GROUP BY (dept_id, year).
    Schema: (pk: U128, dept_id: I64, year: I64, salary: I64).
    3 depts x 3 years x 5 salaries = 45 rows. Uses hash-based gc_u64.
    """
    log("[DBSP] Testing AggValueIndex multi-col group MIN...")

    cols = newlist_hint(4)
    cols.append(types.ColumnDefinition(types.TYPE_U128, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="dept_id"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="year"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="salary"))
    in_schema = types.TableSchema(cols, pk_index=0)

    min_agg = functions.UniversalAccumulator(3, functions.AGG_MIN, types.TYPE_I64)
    out_schema = types._build_reduce_output_schema(in_schema, [1, 2], [min_agg])

    t_out_path = os.path.join(base_dir, "mc_trace_out")
    t_avi_path = os.path.join(base_dir, "mc_avi_idx")
    trace_out = EphemeralTable(t_out_path, "out", out_schema)
    avi_schema = group_index.make_agg_value_idx_schema()
    avi_table = EphemeralTable(t_avi_path, "avidx", avi_schema, table_id=0)
    avi = group_index.AggValueIndex(
        avi_table, [1, 2], in_schema, 3, types.TYPE_I64, False,
    )

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)

        # Tick 1: Insert 45 rows. salaries are dept*30 + year*5 + 0..4.
        # MIN per (dept, year) = dept*30 + year*5.
        log("  - MCG Tick 1: insert 45 rows...")
        pk = 1
        for dept in range(3):
            for year in range(3):
                for s in range(5):
                    salary = dept * 30 + year * 5 + s
                    rb.begin(r_uint128(pk), r_int64(1))
                    rb.put_int(r_int64(dept))
                    rb.put_int(r_int64(year))
                    rb.put_int(r_int64(salary))
                    rb.commit()
                    pk += 1

        _update_avi(b_in, avi)

        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, None, c_out, b_out, [1, 2], [min_agg], out_schema,
                         agg_value_idx=avi)
        c_out.close()

        for dept in range(3):
            for year in range(3):
                expected = r_int64(dept * 30 + year * 5)
                actual = _find_min_for_dept_year(b_out, r_int64(dept), r_int64(year))
                assert_equal_i64(expected, actual,
                                 "MCG T1 MIN dept=" + str(dept) + " year=" + str(year))

        trace_out.ingest_batch(b_out)
        b_in.clear()
        b_out.clear()
        min_agg.reset()

        # Tick 2: Retract minimum-salary row for (dept=1, year=2); new MIN = 41.
        # dept=1, year=2, s=0: salary = 1*30 + 2*5 + 0 = 40; pk = 1*15 + 2*5 + 0 + 1 = 26
        log("  - MCG Tick 2: retract min-salary row for (dept=1, year=2)...")
        rb.begin(r_uint128(26), r_int64(-1))
        rb.put_int(r_int64(1))
        rb.put_int(r_int64(2))
        rb.put_int(r_int64(40))
        rb.commit()

        _update_avi(b_in, avi)

        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, None, c_out, b_out, [1, 2], [min_agg], out_schema,
                         agg_value_idx=avi)
        c_out.close()

        expected_new_min = r_int64(41)   # dept*30 + year*5 + 1 = 30+10+1
        actual = _find_min_for_dept_year(b_out, r_int64(1), r_int64(2))
        assert_equal_i64(expected_new_min, actual, "MCG T2 new MIN for (dept=1, year=2)")

        trace_out.ingest_batch(b_out)
        b_in.clear()
        b_out.clear()
        min_agg.reset()

        # Tick 3: Retract all rows for (dept=0, year=0); no output for that group.
        log("  - MCG Tick 3: retract all rows for (dept=0, year=0)...")
        # (dept=0, year=0): pks 1..5, salaries 0..4
        for s in range(5):
            pk_r = s + 1
            rb.begin(r_uint128(pk_r), r_int64(-1))
            rb.put_int(r_int64(0))
            rb.put_int(r_int64(0))
            rb.put_int(r_int64(s))
            rb.commit()

        _update_avi(b_in, avi)

        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, None, c_out, b_out, [1, 2], [min_agg], out_schema,
                         agg_value_idx=avi)
        c_out.close()

        # (dept=0, year=0) should have only retraction, no new insertion
        found_positive = False
        for i in range(b_out.length()):
            if b_out.get_weight(i) == r_int64(1):
                acc_i = b_out.get_accessor(i)
                if (acc_i.get_int_signed(1) == r_int64(0)
                        and acc_i.get_int_signed(2) == r_int64(0)):
                    found_positive = True
        assert_true(not found_positive, "MCG T3: (dept=0,year=0) should have no +1 output")

        log("[DBSP] AggValueIndex multi-col group MIN test PASSED")
    finally:
        b_in.free()
        b_out.free()
        trace_out.close()
        avi_table.close()


def test_reduce_max_string_group(base_dir):
    """
    AggValueIndex with STRING group column: MAX(value) GROUP BY category.
    Schema: (pk: U128, category: STRING, value: I64).
    4 distinct categories, 5 rows each (values 1..5). Uses hash-based gc_u64.
    """
    log("[DBSP] Testing AggValueIndex MAX with STRING group...")

    cols = newlist_hint(3)
    cols.append(types.ColumnDefinition(types.TYPE_U128, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_STRING, name="category"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="value"))
    in_schema = types.TableSchema(cols, pk_index=0)

    max_agg = functions.UniversalAccumulator(2, functions.AGG_MAX, types.TYPE_I64)
    out_schema = types._build_reduce_output_schema(in_schema, [1], [max_agg])

    t_out_path = os.path.join(base_dir, "sg_trace_out")
    t_avi_path = os.path.join(base_dir, "sg_avi_idx")
    trace_out = EphemeralTable(t_out_path, "out", out_schema)
    avi_schema = group_index.make_agg_value_idx_schema()
    avi_table = EphemeralTable(t_avi_path, "avidx", avi_schema, table_id=0)
    avi = group_index.AggValueIndex(
        avi_table, [1], in_schema, 2, types.TYPE_I64, True,
    )

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)
        categories = ["alpha", "beta", "gamma", "delta"]

        # Tick 1: Insert 20 rows, 4 categories x 5 values (1..5).
        # MAX per category = 5.
        log("  - SG Tick 1: insert 20 rows (4 categories x values 1..5)...")
        pk = 1
        for ci in range(4):
            for v in range(1, 6):
                rb.begin(r_uint128(pk), r_int64(1))
                rb.put_string(categories[ci])
                rb.put_int(r_int64(v))
                rb.commit()
                pk += 1

        _update_avi(b_in, avi)

        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, None, c_out, b_out, [1], [max_agg], out_schema,
                         agg_value_idx=avi)
        c_out.close()

        # All 4 categories should have MAX = 5; count +1 rows
        pos_count = 0
        for i in range(b_out.length()):
            if b_out.get_weight(i) == r_int64(1):
                acc_i = b_out.get_accessor(i)
                assert_equal_i64(r_int64(5), acc_i.get_int_signed(2),
                                 "SG T1 MAX should be 5")
                pos_count += 1
        assert_equal_i(4, pos_count, "SG T1: should have 4 positive output rows")

        trace_out.ingest_batch(b_out)
        b_in.clear()
        b_out.clear()
        max_agg.reset()

        # Tick 2: Retract value=5 rows for all categories; new MAX = 4.
        log("  - SG Tick 2: retract value=5 rows for all categories...")
        # value=5 rows are at pks 5, 10, 15, 20
        for ci in range(4):
            pk_r = ci * 5 + 5
            rb.begin(r_uint128(pk_r), r_int64(-1))
            rb.put_string(categories[ci])
            rb.put_int(r_int64(5))
            rb.commit()

        _update_avi(b_in, avi)

        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, None, c_out, b_out, [1], [max_agg], out_schema,
                         agg_value_idx=avi)
        c_out.close()

        pos_count = 0
        for i in range(b_out.length()):
            if b_out.get_weight(i) == r_int64(1):
                acc_i = b_out.get_accessor(i)
                assert_equal_i64(r_int64(4), acc_i.get_int_signed(2),
                                 "SG T2 new MAX should be 4")
                pos_count += 1
        assert_equal_i(4, pos_count, "SG T2: should have 4 positive output rows")

        log("[DBSP] AggValueIndex MAX with STRING group test PASSED")
    finally:
        b_in.free()
        b_out.free()
        trace_out.close()
        avi_table.close()


def test_reduce_multi_agg(base_dir):
    """Multi-aggregate: COUNT(*) + SUM(val) on same group."""
    log("[DBSP] Testing Reduce multi-agg (COUNT + SUM)...")
    in_schema = _make_reduce_schema()

    count_agg = functions.UniversalAccumulator(1, functions.AGG_COUNT, types.TYPE_I64)
    sum_agg = functions.UniversalAccumulator(1, functions.AGG_SUM, types.TYPE_I64)
    agg_funcs = [count_agg, sum_agg]
    out_schema = types._build_reduce_output_schema(in_schema, [0], agg_funcs)

    t_in_path = os.path.join(base_dir, "multi_in")
    t_out_path = os.path.join(base_dir, "multi_out")
    trace_in = EphemeralTable(t_in_path, "in", in_schema)
    trace_out = EphemeralTable(t_out_path, "out", out_schema)

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)
        # grp=10: val=5, val=10, val=15 => COUNT=3, SUM=30
        _add_row(rb, pk=1, grp=10, val=5, weight=1)
        _add_row(rb, pk=2, grp=10, val=10, weight=1)
        _add_row(rb, pk=3, grp=10, val=15, weight=1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], agg_funcs, out_schema)

        # Find the +1 weight row
        found = False
        for i in range(b_out.length()):
            if b_out.get_weight(i) == r_int64(1):
                acc = b_out.get_accessor(i)
                count_val = acc.get_int_signed(1)
                sum_val = acc.get_int_signed(2)
                assert_equal_i64(r_int64(3), count_val, "multi-agg COUNT")
                assert_equal_i64(r_int64(30), sum_val, "multi-agg SUM")
                found = True
        assert_true(found, "multi-agg: no +1 weight row found")

        c_in.close()
        c_out.close()
    finally:
        b_in.free()
        b_out.free()
        trace_in.close()
        trace_out.close()
    log("  PASSED")


def test_reduce_multi_agg_linear_merge(base_dir):
    """Multi-agg linear merge: incremental insert with COUNT + SUM."""
    log("[DBSP] Testing Reduce multi-agg linear merge...")
    in_schema = _make_reduce_schema()

    count_agg = functions.UniversalAccumulator(1, functions.AGG_COUNT, types.TYPE_I64)
    sum_agg = functions.UniversalAccumulator(1, functions.AGG_SUM, types.TYPE_I64)
    agg_funcs = [count_agg, sum_agg]
    out_schema = types._build_reduce_output_schema(in_schema, [0], agg_funcs)

    t_in_path = os.path.join(base_dir, "mlm_in")
    t_out_path = os.path.join(base_dir, "mlm_out")
    trace_in = EphemeralTable(t_in_path, "in", in_schema)
    trace_out = EphemeralTable(t_out_path, "out", out_schema)

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)

        # Tick 1: grp=10, val=5
        _add_row(rb, pk=1, grp=10, val=5, weight=1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], agg_funcs, out_schema)

        found = False
        for i in range(b_out.length()):
            if b_out.get_weight(i) == r_int64(1):
                acc = b_out.get_accessor(i)
                assert_equal_i64(r_int64(1), acc.get_int_signed(1), "T1 COUNT")
                assert_equal_i64(r_int64(5), acc.get_int_signed(2), "T1 SUM")
                found = True
        assert_true(found, "T1: no +1 weight row")

        trace_in.ingest_batch(b_in)
        trace_out.ingest_batch(b_out)
        c_in.close()
        c_out.close()

        # Tick 2: add val=10 to same group => COUNT=2, SUM=15
        b_in.clear()
        b_out.clear()
        count_agg.reset()
        sum_agg.reset()
        _add_row(rb, pk=2, grp=10, val=10, weight=1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], agg_funcs, out_schema)

        # Should have retraction (-1 for old) and insertion (+1 for new)
        assert_equal_i(2, b_out.length(), "T2 should have 2 rows (retraction + insertion)")
        found_new = False
        for i in range(b_out.length()):
            if b_out.get_weight(i) == r_int64(1):
                acc = b_out.get_accessor(i)
                assert_equal_i64(r_int64(2), acc.get_int_signed(1), "T2 COUNT")
                assert_equal_i64(r_int64(15), acc.get_int_signed(2), "T2 SUM")
                found_new = True
        assert_true(found_new, "T2: no +1 weight row")

        c_in.close()
        c_out.close()
    finally:
        b_in.free()
        b_out.free()
        trace_in.close()
        trace_out.close()
    log("  PASSED")


def test_reduce_multi_agg_nonlinear(base_dir):
    """Multi-agg non-linear: COUNT(*) + MIN(val) triggers replay path."""
    log("[DBSP] Testing Reduce multi-agg non-linear (COUNT + MIN)...")
    in_schema = _make_reduce_schema()

    count_agg = functions.UniversalAccumulator(1, functions.AGG_COUNT, types.TYPE_I64)
    min_agg = functions.UniversalAccumulator(1, functions.AGG_MIN, types.TYPE_I64)
    agg_funcs = [count_agg, min_agg]
    out_schema = types._build_reduce_output_schema(in_schema, [0], agg_funcs)

    t_in_path = os.path.join(base_dir, "mnl_in")
    t_out_path = os.path.join(base_dir, "mnl_out")
    trace_in = EphemeralTable(t_in_path, "in", in_schema)
    trace_out = EphemeralTable(t_out_path, "out", out_schema)

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)

        # Tick 1: grp=10, val=5 and val=10 => COUNT=2, MIN=5
        _add_row(rb, pk=1, grp=10, val=5, weight=1)
        _add_row(rb, pk=2, grp=10, val=10, weight=1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], agg_funcs, out_schema)

        found = False
        for i in range(b_out.length()):
            if b_out.get_weight(i) == r_int64(1):
                acc = b_out.get_accessor(i)
                assert_equal_i64(r_int64(2), acc.get_int_signed(1), "T1 COUNT")
                assert_equal_i64(r_int64(5), acc.get_int_signed(2), "T1 MIN")
                found = True
        assert_true(found, "T1: no +1 weight row")

        trace_in.ingest_batch(b_in)
        trace_out.ingest_batch(b_out)
        c_in.close()
        c_out.close()

        # Tick 2: retract val=5 => COUNT=1, MIN=10 (replay path for MIN)
        b_in.clear()
        b_out.clear()
        count_agg.reset()
        min_agg.reset()
        _add_row(rb, pk=1, grp=10, val=5, weight=-1)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], agg_funcs, out_schema)

        # Find the new insertion
        found_new = False
        for i in range(b_out.length()):
            if b_out.get_weight(i) == r_int64(1):
                acc = b_out.get_accessor(i)
                assert_equal_i64(r_int64(1), acc.get_int_signed(1), "T2 COUNT")
                assert_equal_i64(r_int64(10), acc.get_int_signed(2), "T2 MIN")
                found_new = True
        assert_true(found_new, "T2: no +1 weight row after retraction")

        c_in.close()
        c_out.close()
    finally:
        b_in.free()
        b_out.free()
        trace_in.close()
        trace_out.close()
    log("  PASSED")


def test_count_non_null(base_dir):
    """AGG_COUNT_NON_NULL skips NULLs."""
    log("[DBSP] Testing COUNT_NON_NULL with nullable column...")

    cols = newlist_hint(3)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="grp"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val", is_nullable=True))
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    in_schema = types.TableSchema(cols, 2)

    count_nn = functions.UniversalAccumulator(1, functions.AGG_COUNT_NON_NULL, types.TYPE_I64)
    out_schema = types._build_reduce_output_schema(in_schema, [0], [count_nn])

    t_in_path = os.path.join(base_dir, "cnn_in")
    t_out_path = os.path.join(base_dir, "cnn_out")
    trace_in = EphemeralTable(t_in_path, "in", in_schema)
    trace_out = EphemeralTable(t_out_path, "out", out_schema)

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        rb = RowBuilder(in_schema, b_in)

        # grp=10: val=5 (non-null), val=NULL, val=15 (non-null) => COUNT_NON_NULL=2
        rb.begin(r_uint128(1), r_int64(1))
        rb.put_int(r_int64(10))  # grp
        rb.put_int(r_int64(5))   # val (non-null)
        rb.commit()

        rb.begin(r_uint128(2), r_int64(1))
        rb.put_int(r_int64(10))  # grp
        rb.put_null()             # val (NULL)
        rb.commit()

        rb.begin(r_uint128(3), r_int64(1))
        rb.put_int(r_int64(10))  # grp
        rb.put_int(r_int64(15))  # val (non-null)
        rb.commit()

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()
        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], [count_nn], out_schema)

        found = False
        for i in range(b_out.length()):
            if b_out.get_weight(i) == r_int64(1):
                acc = b_out.get_accessor(i)
                assert_equal_i64(r_int64(2), acc.get_int_signed(1), "COUNT_NON_NULL")
                found = True
        assert_true(found, "COUNT_NON_NULL: no +1 weight row")

        c_in.close()
        c_out.close()
    finally:
        b_in.free()
        b_out.free()
        trace_in.close()
        trace_out.close()
    log("  PASSED")


def test_compaction_through_ticks(base_dir):
    """EphemeralTable shards stay bounded across many ticks via auto-compaction."""
    log("[DBSP] Testing compaction through tick lifecycle...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="v"))
    schema = types.TableSchema(cols, 0)

    trace_path = os.path.join(base_dir, "ctt_trace")
    trace = EphemeralTable(trace_path, "ctt", schema)

    b_in = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        rb = RowBuilder(schema, b_in)
        tick = 1
        while tick <= 10:
            b_in.clear()
            b_out.clear()

            # Each tick introduces a fresh unique PK
            rb.begin(r_uint128(tick), r_int64(1))
            rb.put_int(r_int64(tick))
            rb.commit()

            cursor = trace.create_cursor()
            distinct.op_distinct(b_in, cursor, trace, batch.BatchWriter(b_out))
            cursor.close()

            # New PK always produces output weight +1
            if b_out.length() != 1:
                fail("Tick " + str(tick) + ": expected 1 output row")
            if b_out.get_weight(0) != r_int64(1):
                fail("Tick " + str(tick) + ": expected output weight 1")

            # Flush to disk to exercise compaction (ingest already done by op_distinct)
            trace.flush()

            tick += 1

        # Shard count bounded: <= 2 * compaction_threshold (default threshold=4)
        num_handles = len(trace.index.handles)
        if num_handles > 8:
            fail(
                "Shard count unbounded: "
                + str(num_handles)
                + " handles after 10 ticks"
            )

        # All 10 inserted PKs must be present in trace
        i = 1
        while i <= 10:
            if not trace.has_pk(r_uint128(i)):
                fail("PK " + str(i) + " missing from trace after 10 ticks")
            i += 1

    finally:
        b_in.free()
        b_out.free()
        trace.close()

    log("  PASSED")


# ------------------------------------------------------------------------------
# _consolidated flag tests
# ------------------------------------------------------------------------------


def test_consolidated_flag_basics():
    log("[DBSP] Testing consolidated flag basics...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b = batch.ArenaZSetBatch(schema)
    try:
        rb = RowBuilder(schema, b)
        rb.begin(r_uint128(1), r_int64(1))
        rb.put_int(r_int64(10))
        rb.commit()
        rb.begin(r_uint128(2), r_int64(2))
        rb.put_int(r_int64(20))
        rb.commit()
        rb.begin(r_uint128(3), r_int64(1))
        rb.put_int(r_int64(30))
        rb.commit()

        b.mark_consolidated(True)
        assert_true(b._consolidated, "mark_consolidated(True) should set _consolidated")
        assert_true(b._sorted, "mark_consolidated(True) should imply _sorted=True")

        # to_consolidated() should short-circuit and return self
        result = b.to_consolidated()
        assert_true(result is b, "to_consolidated() on consolidated batch should return self")
        assert_equal_i(3, result._count, "count unchanged after short-circuit")

    finally:
        b.free()
    log("  PASSED")


def test_consolidated_short_circuit_empty():
    log("[DBSP] Testing consolidated short-circuit on empty batch...")

    cols = newlist_hint(1)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    schema = types.TableSchema(cols, 0)

    b = batch.ArenaZSetBatch(schema)
    try:
        result = b.to_consolidated()
        assert_true(result is b, "Empty batch: to_consolidated() should return self")
        assert_equal_i(0, result._count, "Empty batch count should be 0")
    finally:
        b.free()
    log("  PASSED")


def test_consolidated_propagation_filter(base_dir):
    log("[DBSP] Testing consolidated propagation through op_filter...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b_in = batch.ArenaZSetBatch(schema)
    b_out1 = batch.ArenaZSetBatch(schema)
    b_out2 = batch.ArenaZSetBatch(schema)
    b_out3 = batch.ArenaZSetBatch(schema)

    try:
        rb = RowBuilder(schema, b_in)
        rb.begin(r_uint128(1), r_int64(1))
        rb.put_int(r_int64(5))
        rb.commit()
        rb.begin(r_uint128(2), r_int64(1))
        rb.put_int(r_int64(15))
        rb.commit()
        rb.begin(r_uint128(3), r_int64(1))
        rb.put_int(r_int64(25))
        rb.commit()
        b_in.mark_consolidated(True)

        # Case 1: pass-all predicate (NullPredicate) — output should be consolidated
        pass_func = functions.NullPredicate()
        linear.op_filter(b_in, b_out1, pass_func)
        assert_true(b_out1._consolidated, "Filter pass-all: output should be consolidated")

        # Case 2: predicate keeps 2 of 3 rows (val > 10) — output still consolidated
        filter_func = functions.UniversalPredicate(1, functions.OP_GT, r_uint64(10))
        linear.op_filter(b_in, b_out2, filter_func)
        assert_true(b_out2._consolidated, "Filter subset: output should be consolidated")
        assert_equal_i(2, b_out2._count, "Filter should keep 2 rows (val>10)")

        # Case 3: sorted-but-not-consolidated input — _consolidated should stay False
        b_sorted = batch.ArenaZSetBatch(schema)
        rb2 = RowBuilder(schema, b_sorted)
        rb2.begin(r_uint128(4), r_int64(1))
        rb2.put_int(r_int64(40))
        rb2.commit()
        b_sorted.mark_sorted(True)
        # _consolidated is False by default

        linear.op_filter(b_sorted, b_out3, pass_func)
        assert_true(not b_out3._consolidated,
                    "Filter on sorted-not-consolidated: _consolidated should be False")
        assert_true(b_out3._sorted, "Filter on sorted input: _sorted should be True")
        b_sorted.free()

    finally:
        b_in.free()
        b_out1.free()
        b_out2.free()
        b_out3.free()
    log("  PASSED")


def test_consolidated_propagation_negate():
    log("[DBSP] Testing consolidated propagation through op_negate...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b_in = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        rb = RowBuilder(schema, b_in)
        rb.begin(r_uint128(1), r_int64(3))
        rb.put_int(r_int64(10))
        rb.commit()
        rb.begin(r_uint128(2), r_int64(1))
        rb.put_int(r_int64(20))
        rb.commit()
        b_in.mark_consolidated(True)

        linear.op_negate(b_in, b_out)
        assert_true(b_out._consolidated, "Negate of consolidated: output should be consolidated")
        assert_equal_i(2, b_out._count, "Negate should preserve row count")
        assert_equal_i64(r_int64(-3), b_out.get_weight(0), "Negate: weight should be flipped")

    finally:
        b_in.free()
        b_out.free()
    log("  PASSED")


def test_scan_trace_marks_consolidated(base_dir):
    log("[DBSP] Testing scan_trace marks output consolidated...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    table_path = os.path.join(base_dir, "scan_trace_cons")
    table = EphemeralTable(table_path, "scan_cons", schema)

    b_in = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        rb = RowBuilder(schema, b_in)
        rb.begin(r_uint128(1), r_int64(1))
        rb.put_int(r_int64(10))
        rb.commit()
        rb.begin(r_uint128(2), r_int64(1))
        rb.put_int(r_int64(20))
        rb.commit()
        rb.begin(r_uint128(3), r_int64(1))
        rb.put_int(r_int64(30))
        rb.commit()
        table.ingest_batch(b_in)

        cursor = table.create_cursor()
        source.op_seek_trace(cursor, r_uint128(0))
        source.op_scan_trace(cursor, b_out, 0)
        cursor.close()

        assert_true(b_out._consolidated, "scan_trace should mark output consolidated")
        assert_equal_i(3, b_out._count, "scan_trace should emit 3 rows")

    finally:
        b_in.free()
        b_out.free()
        table.close()
    log("  PASSED")


def test_distinct_marks_consolidated(base_dir):
    log("[DBSP] Testing distinct marks output consolidated...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    hist_path = os.path.join(base_dir, "distinct_cons_hist")
    hist_table = EphemeralTable(hist_path, "hist_cons", schema)

    b_in = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        rb = RowBuilder(schema, b_in)
        rb.begin(r_uint128(10), r_int64(2))
        rb.put_int(r_int64(100))
        rb.commit()
        rb.begin(r_uint128(20), r_int64(1))
        rb.put_int(r_int64(200))
        rb.commit()

        cursor = hist_table.create_cursor()
        distinct.op_distinct(b_in, cursor, hist_table, batch.BatchWriter(b_out))
        cursor.close()

        assert_true(b_out._consolidated, "op_distinct should mark output consolidated")
        assert_equal_i(2, b_out._count, "distinct should emit 2 rows")

    finally:
        b_in.free()
        b_out.free()
        hist_table.close()
    log("  PASSED")


def test_clone_preserves_consolidated():
    log("[DBSP] Testing clone preserves _consolidated flag...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    b_cons = batch.ArenaZSetBatch(schema)
    b_sorted = batch.ArenaZSetBatch(schema)

    try:
        rb = RowBuilder(schema, b_cons)
        rb.begin(r_uint128(1), r_int64(1))
        rb.put_int(r_int64(10))
        rb.commit()
        b_cons.mark_consolidated(True)

        clone_cons = b_cons.clone()
        assert_true(clone_cons._consolidated, "Clone of consolidated: _consolidated should be True")
        assert_true(clone_cons._sorted, "Clone of consolidated: _sorted should be True")
        clone_cons.free()

        rb2 = RowBuilder(schema, b_sorted)
        rb2.begin(r_uint128(2), r_int64(1))
        rb2.put_int(r_int64(20))
        rb2.commit()
        b_sorted.mark_sorted(True)
        # _consolidated remains False

        clone_sorted = b_sorted.clone()
        assert_true(not clone_sorted._consolidated,
                    "Clone of sorted-not-consolidated: _consolidated should be False")
        assert_true(clone_sorted._sorted,
                    "Clone of sorted-not-consolidated: _sorted should be True")
        clone_sorted.free()

    finally:
        b_cons.free()
        b_sorted.free()
    log("  PASSED")


def test_union_sorted_merge():
    log("[DBSP] Testing op_union sorted merge (Opt 3)...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    batch_a_raw = batch.ArenaZSetBatch(schema)
    batch_b_raw = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)

    try:
        rb_a = RowBuilder(schema, batch_a_raw)
        rb_a.begin(r_uint128(1), r_int64(1))
        rb_a.put_int(r_int64(10))
        rb_a.commit()
        rb_a.begin(r_uint128(2), r_int64(1))
        rb_a.put_int(r_int64(20))
        rb_a.commit()
        rb_a.begin(r_uint128(3), r_int64(1))
        rb_a.put_int(r_int64(30))
        rb_a.commit()

        rb_b = RowBuilder(schema, batch_b_raw)
        rb_b.begin(r_uint128(2), r_int64(1))
        rb_b.put_int(r_int64(20))
        rb_b.commit()
        rb_b.begin(r_uint128(4), r_int64(1))
        rb_b.put_int(r_int64(40))
        rb_b.commit()
        rb_b.begin(r_uint128(5), r_int64(1))
        rb_b.put_int(r_int64(50))
        rb_b.commit()

        batch_a_sorted = batch_a_raw.to_sorted()
        batch_b_sorted = batch_b_raw.to_sorted()

        try:
            linear.op_union(batch_a_sorted, batch_b_sorted, b_out)
            assert_true(b_out._sorted, "Sorted merge: output _sorted should be True")
            assert_equal_i(6, b_out.length(),
                           "Sorted merge: pk=1,2,2,3,4,5 -> 6 rows")
            for i in range(b_out.length() - 1):
                pk_i = b_out.get_pk(i)
                pk_next = b_out.get_pk(i + 1)
                assert_true(pk_i <= pk_next, "Sorted merge: PKs not in ascending order")
        finally:
            if batch_a_sorted is not batch_a_raw:
                batch_a_sorted.free()
            if batch_b_sorted is not batch_b_raw:
                batch_b_sorted.free()

    finally:
        batch_a_raw.free()
        batch_b_raw.free()
        b_out.free()
    log("  PASSED")


def test_outer_join_delta_trace(base_dir):
    """
    Tests for op_join_delta_trace_outer covering:
    1. Match case: delta PK in trace -> inner join rows, no null-fill.
    2. No-match case: delta PK not in trace -> null-fill row.
    3. Mixed: some PKs match, some don't.
    4. Multiset delta: two delta rows with same PK, different payload, no match.
    5. Negative-weight trace: w_delta * w_trace != 0 -> matched, no null-fill.
    6. Schema: right-side columns are nullable in output schema.
    """
    log("[DBSP] Testing Left Outer Join (delta-trace)...")

    cols_l = newlist_hint(2)
    cols_l.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols_l.append(types.ColumnDefinition(types.TYPE_I64, name="val_l"))
    schema_l = types.TableSchema(cols_l, 0)

    cols_r = newlist_hint(2)
    cols_r.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols_r.append(types.ColumnDefinition(types.TYPE_I64, name="val_r"))
    schema_r = types.TableSchema(cols_r, 0)

    schema_out = types.merge_schemas_for_join_outer(schema_l, schema_r)

    log("  - Schema: right col is nullable...")
    assert_true(not schema_out.columns[1].is_nullable,
                "Outer join: left payload col should not be nullable")
    assert_true(schema_out.columns[2].is_nullable,
                "Outer join: right payload col must be nullable")

    log("  - Match case: inner join rows emitted, no null-fill...")
    b_delta = batch.ArenaZSetBatch(schema_l)
    b_out = batch.ArenaZSetBatch(schema_out)
    trace_path = os.path.join(base_dir, "oj_trace_match")
    trace_r = EphemeralTable(trace_path, "tr", schema_r)
    try:
        rb_l = RowBuilder(schema_l, b_delta)
        rb_l.begin(r_uint128(10), r_int64(1))
        rb_l.put_int(r_int64(100))
        rb_l.commit()

        rb_r_batch = batch.ArenaZSetBatch(schema_r)
        rb_r = RowBuilder(schema_r, rb_r_batch)
        rb_r.begin(r_uint128(10), r_int64(1))
        rb_r.put_int(r_int64(200))
        rb_r.commit()
        trace_r.ingest_batch(rb_r_batch)
        rb_r_batch.free()

        cursor_r = trace_r.create_cursor()
        join.op_join_delta_trace_outer(b_delta, cursor_r,
                                       batch.BatchWriter(b_out), schema_l, schema_r)
        cursor_r.close()
        assert_equal_i(1, b_out.length(), "Match: should emit 1 inner-join row")
        acc = b_out.get_accessor(0)
        assert_equal_u128(r_uint128(10), b_out.get_pk(0), "Match: PK should be 10")
        assert_true(not acc.is_null(2), "Match: right col must not be null")
        assert_equal_i64(r_int64(200), acc.get_int_signed(2), "Match: right val should be 200")
    finally:
        b_delta.free()
        b_out.free()
        trace_r.close()

    log("  - No-match case: null-fill row emitted...")
    b_delta2 = batch.ArenaZSetBatch(schema_l)
    b_out2 = batch.ArenaZSetBatch(schema_out)
    trace_path2 = os.path.join(base_dir, "oj_trace_nomatch")
    trace_r2 = EphemeralTable(trace_path2, "tr", schema_r)
    try:
        rb_l2 = RowBuilder(schema_l, b_delta2)
        rb_l2.begin(r_uint128(5), r_int64(1))
        rb_l2.put_int(r_int64(50))
        rb_l2.commit()

        cursor_r2 = trace_r2.create_cursor()
        join.op_join_delta_trace_outer(b_delta2, cursor_r2,
                                       batch.BatchWriter(b_out2), schema_l, schema_r)
        cursor_r2.close()
        assert_equal_i(1, b_out2.length(), "No-match: should emit 1 null-fill row")
        acc2 = b_out2.get_accessor(0)
        assert_equal_u128(r_uint128(5), b_out2.get_pk(0), "No-match: PK should be 5")
        assert_equal_i64(r_int64(50), acc2.get_int_signed(1), "No-match: left val should be 50")
        assert_true(acc2.is_null(2), "No-match: right col must be null")
    finally:
        b_delta2.free()
        b_out2.free()
        trace_r2.close()

    log("  - Mixed case: PK=1 matches, PK=2 does not...")
    b_delta3 = batch.ArenaZSetBatch(schema_l)
    b_out3 = batch.ArenaZSetBatch(schema_out)
    trace_path3 = os.path.join(base_dir, "oj_trace_mixed")
    trace_r3 = EphemeralTable(trace_path3, "tr", schema_r)
    try:
        rb_l3 = RowBuilder(schema_l, b_delta3)
        rb_l3.begin(r_uint128(1), r_int64(1))
        rb_l3.put_int(r_int64(10))
        rb_l3.commit()
        rb_l3.begin(r_uint128(2), r_int64(1))
        rb_l3.put_int(r_int64(20))
        rb_l3.commit()

        rb_r3_batch = batch.ArenaZSetBatch(schema_r)
        rb_r3 = RowBuilder(schema_r, rb_r3_batch)
        rb_r3.begin(r_uint128(1), r_int64(1))
        rb_r3.put_int(r_int64(100))
        rb_r3.commit()
        trace_r3.ingest_batch(rb_r3_batch)
        rb_r3_batch.free()

        cursor_r3 = trace_r3.create_cursor()
        join.op_join_delta_trace_outer(b_delta3, cursor_r3,
                                       batch.BatchWriter(b_out3), schema_l, schema_r)
        cursor_r3.close()
        assert_equal_i(2, b_out3.length(), "Mixed: should emit 2 rows")
        # Row 0: PK=1 matched
        acc3a = b_out3.get_accessor(0)
        assert_equal_u128(r_uint128(1), b_out3.get_pk(0), "Mixed: first row PK=1")
        assert_true(not acc3a.is_null(2), "Mixed: PK=1 right col not null")
        assert_equal_i64(r_int64(100), acc3a.get_int_signed(2), "Mixed: PK=1 right val=100")
        # Row 1: PK=2 not matched -> null-fill
        acc3b = b_out3.get_accessor(1)
        assert_equal_u128(r_uint128(2), b_out3.get_pk(1), "Mixed: second row PK=2")
        assert_true(acc3b.is_null(2), "Mixed: PK=2 right col is null")
    finally:
        b_delta3.free()
        b_out3.free()
        trace_r3.close()

    log("  - Multiset delta: same PK, different payload, no trace match...")
    b_delta4 = batch.ArenaZSetBatch(schema_l)
    b_out4 = batch.ArenaZSetBatch(schema_out)
    trace_path4 = os.path.join(base_dir, "oj_trace_multiset")
    trace_r4 = EphemeralTable(trace_path4, "tr", schema_r)
    try:
        rb_l4 = RowBuilder(schema_l, b_delta4)
        rb_l4.begin(r_uint128(7), r_int64(1))
        rb_l4.put_int(r_int64(71))
        rb_l4.commit()
        rb_l4.begin(r_uint128(7), r_int64(1))
        rb_l4.put_int(r_int64(72))
        rb_l4.commit()

        cursor_r4 = trace_r4.create_cursor()
        join.op_join_delta_trace_outer(b_delta4, cursor_r4,
                                       batch.BatchWriter(b_out4), schema_l, schema_r)
        cursor_r4.close()
        assert_equal_i(2, b_out4.length(), "Multiset: should emit 2 null-fill rows")
        assert_true(b_out4.get_accessor(0).is_null(2), "Multiset: row 0 right col null")
        assert_true(b_out4.get_accessor(1).is_null(2), "Multiset: row 1 right col null")
    finally:
        b_delta4.free()
        b_out4.free()
        trace_r4.close()

    log("  - Negative-weight trace: w_out != 0 -> matched, no null-fill...")
    b_delta5 = batch.ArenaZSetBatch(schema_l)
    b_out5 = batch.ArenaZSetBatch(schema_out)
    trace_path5 = os.path.join(base_dir, "oj_trace_negw")
    trace_r5 = EphemeralTable(trace_path5, "tr", schema_r)
    try:
        rb_l5 = RowBuilder(schema_l, b_delta5)
        rb_l5.begin(r_uint128(3), r_int64(1))
        rb_l5.put_int(r_int64(30))
        rb_l5.commit()

        rb_r5_batch = batch.ArenaZSetBatch(schema_r)
        rb_r5 = RowBuilder(schema_r, rb_r5_batch)
        rb_r5.begin(r_uint128(3), r_int64(-1))
        rb_r5.put_int(r_int64(300))
        rb_r5.commit()
        trace_r5.ingest_batch(rb_r5_batch)
        rb_r5_batch.free()

        cursor_r5 = trace_r5.create_cursor()
        join.op_join_delta_trace_outer(b_delta5, cursor_r5,
                                       batch.BatchWriter(b_out5), schema_l, schema_r)
        cursor_r5.close()
        assert_equal_i(1, b_out5.length(), "NegWeight: should emit 1 inner-join row (w=-1)")
        assert_equal_i64(r_int64(-1), b_out5.get_weight(0), "NegWeight: output weight=-1")
        assert_true(not b_out5.get_accessor(0).is_null(2), "NegWeight: right col not null")
    finally:
        b_delta5.free()
        b_out5.free()
        trace_r5.close()

    log("  PASSED")


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------


def entry_point(argv):
    base_dir = "dbsp_test_data"
    cleanup_dir(base_dir)
    os.mkdir(base_dir)

    try:
        test_linear_ops(base_dir)
        test_join_ops(base_dir)
        test_distinct_op(base_dir)
        test_reduce_op(base_dir)
        test_reduce_min_retraction(base_dir)
        test_reduce_max_retraction(base_dir)
        test_reduce_group_becomes_empty(base_dir)
        test_reduce_multiple_groups(base_dir)
        test_reduce_count(base_dir)
        test_anti_join_basic(base_dir)
        test_anti_join_empty_right(base_dir)
        test_anti_join_full_overlap(base_dir)
        test_anti_join_weight_semantics(base_dir)
        test_semi_join_basic(base_dir)
        test_anti_semi_join_complement(base_dir)
        test_join_adaptive_swap(base_dir)
        test_join_merge_walk(base_dir)
        test_anti_join_merge_walk(base_dir)
        test_semi_join_merge_walk(base_dir)
        test_semi_join_dt_nonconsolidated(base_dir)
        test_source_ops(base_dir)
        test_reduce_group_idx(base_dir)
        test_reduce_min_agg_value_idx(base_dir)
        test_reduce_min_multi_col_group(base_dir)
        test_reduce_max_string_group(base_dir)
        test_reduce_multi_agg(base_dir)
        test_reduce_multi_agg_linear_merge(base_dir)
        test_reduce_multi_agg_nonlinear(base_dir)
        test_count_non_null(base_dir)
        test_compaction_through_ticks(base_dir)
        test_consolidated_flag_basics()
        test_consolidated_short_circuit_empty()
        test_consolidated_propagation_filter(base_dir)
        test_consolidated_propagation_negate()
        test_scan_trace_marks_consolidated(base_dir)
        test_distinct_marks_consolidated(base_dir)
        test_clone_preserves_consolidated()
        test_union_sorted_merge()
        test_outer_join_delta_trace(base_dir)
        log("\nALL DBSP TESTS PASSED")
    except Exception as e:
        os.write(2, "FAILURE\n")
        return 1
    finally:
        cleanup_dir(base_dir)

    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
