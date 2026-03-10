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

from gnitz.core import types, batch
from gnitz.core.batch import RowBuilder
from gnitz.dbsp.ops import linear, join, anti_join, reduce, distinct, source
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
        linear.op_map(b_in, b_mapped, mapper, map_schema)
        assert_true(b_mapped.get_accessor(0).is_null(1), "Map failed to propagate null")

        log("  - Negate & Union...")
        linear.op_negate(b_in, b_neg)
        linear.op_union(b_in, b_neg, b_union)

        # Functional consolidation: we must use the result or a Scope to avoid leaks
        with batch.ConsolidatedScope(b_union) as b_union_cons:
            assert_equal_i(0, b_union_cons.length(), "Union/Negate failed to annihilate")

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
        join.op_join_delta_delta(b_l, b_r, b_out, schema_l, schema_r)
        assert_equal_i(6, b_out.length(), "M:N Join failed")

        log("  - Delta-Trace Index-Nested...")
        b_out.clear()
        trace_path = os.path.join(base_dir, "j_trace")
        trace_r = EphemeralTable(trace_path, "tr", schema_r)

        # Ingest the 3 rows. Because the strings differ, they won't be consolidated.
        trace_r.ingest_batch(b_r)

        cursor_r = trace_r.create_cursor()
        join.op_join_delta_trace(b_l, cursor_r, b_out, schema_l, schema_r)
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

        distinct.op_distinct(b_in, trace, b_out)
        assert_equal_i64(r_int64(1), b_out.get_weight(0), "Distinct failed to clamp")

        # Tick 2: Weight -5 -> Should output Weight 0 (Total 5 is still > 0)
        b_in.clear()
        b_out.clear()
        rb.begin(r_uint128(1), r_int64(-5))
        rb.put_int(r_int64(1))
        rb.commit()

        distinct.op_distinct(b_in, trace, b_out)
        assert_equal_i(0, b_out.length(), "Distinct produced unnecessary update")

        # Tick 3: Weight -5 -> Should output Weight -1 (Total 0)
        b_in.clear()
        b_out.clear()
        rb.begin(r_uint128(1), r_int64(-5))
        rb.put_int(r_int64(1))
        rb.commit()

        distinct.op_distinct(b_in, trace, b_out)
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
    out_schema = types._build_reduce_output_schema(in_schema, [0], sum_agg)

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

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], sum_agg, out_schema)
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

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], sum_agg, out_schema)

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
    out_schema = types._build_reduce_output_schema(in_schema, [0], min_agg)

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

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], min_agg, out_schema)
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

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], min_agg, out_schema)
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
    out_schema = types._build_reduce_output_schema(in_schema, [0], max_agg)

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

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], max_agg, out_schema)
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

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], max_agg, out_schema)
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
    out_schema = types._build_reduce_output_schema(in_schema, [0], min_agg)

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

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], min_agg, out_schema)
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

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], min_agg, out_schema)

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
    out_schema = types._build_reduce_output_schema(in_schema, [0], sum_agg)

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

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], sum_agg, out_schema)

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
    out_schema = types._build_reduce_output_schema(in_schema, [0], count_agg)

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

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], count_agg, out_schema)
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

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], count_agg, out_schema)
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
        anti_join.op_anti_join_delta_trace(b_a, cursor_b, b_out, schema)
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
        anti_join.op_semi_join_delta_trace(b_a, cursor_b, b_out, schema)
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
        test_source_ops(base_dir)
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
