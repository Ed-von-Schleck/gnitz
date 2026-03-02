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

from gnitz.core import types, values, batch, comparator
from gnitz.dbsp.ops import linear, join, reduce, distinct, source
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

    try:
        # 1. Null handling in Filter/Map
        r1 = values.make_payload_row(schema)
        r1.append_null(0)  # val_int is null
        r1.append_float(1.5)
        b_in.append(r_uint128(1), r_int64(1), r1)

        r2 = values.make_payload_row(schema)
        r2.append_int(r_int64(20))
        r2.append_float(15.5)
        b_in.append(r_uint128(2), r_int64(2), r2)

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
        b_union.consolidate()
        assert_equal_i(0, b_union.length(), "Union/Negate failed to annihilate")

        log("  - Delay & Integrate...")
        b_delay = batch.ArenaZSetBatch(schema)
        linear.op_delay(b_in, b_delay)
        assert_equal_i(2, b_delay.length(), "Delay failed")

        sink_path = os.path.join(base_dir, "sink")
        sink = EphemeralTable(sink_path, "sink", schema)
        linear.op_integrate(b_in, sink)
        assert_true(sink.has_pk(r_uint128(1)), "Integrate failed to sink data")
        sink.close()
        b_delay.free()

    finally:
        b_in.free()
        b_filtered.free()
        if b_mapped:
            b_mapped.free()
        b_neg.free()
        b_union.free()


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
        rl1 = values.make_payload_row(schema_l)
        rl1.append_int(r_int64(1))
        b_l.append(r_uint128(10), r_int64(1), rl1)

        rl2 = values.make_payload_row(schema_l)
        rl2.append_int(r_int64(2))
        b_l.append(r_uint128(10), r_int64(1), rl2)

        # Right: 3 DISTINCT rows for PK 10
        rr1 = values.make_payload_row(schema_r)
        rr1.append_string("match1")
        b_r.append(r_uint128(10), r_int64(1), rr1)

        rr2 = values.make_payload_row(schema_r)
        rr2.append_string("match2")
        b_r.append(r_uint128(10), r_int64(1), rr2)

        rr3 = values.make_payload_row(schema_r)
        rr3.append_string("match3")
        b_r.append(r_uint128(10), r_int64(1), rr3)

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
        r = values.make_payload_row(schema)
        r.append_int(r_int64(1))
        # Tick 1: Weight 10 -> Should output Weight 1
        b_in.append(r_uint128(1), r_int64(10), r)
        distinct.op_distinct(b_in, trace, b_out)
        assert_equal_i64(r_int64(1), b_out.get_weight(0), "Distinct failed to clamp")

        # Tick 2: Weight -5 -> Should output Weight 0 (Total 5 is still > 0)
        b_in.clear()
        b_out.clear()
        b_in.append(r_uint128(1), r_int64(-5), r)
        distinct.op_distinct(b_in, trace, b_out)
        assert_equal_i(0, b_out.length(), "Distinct produced unnecessary update")

        # Tick 3: Weight -5 -> Should output Weight -1 (Total 0)
        b_in.clear()
        b_out.clear()
        b_in.append(r_uint128(1), r_int64(-5), r)
        distinct.op_distinct(b_in, trace, b_out)
        assert_equal_i64(r_int64(-1), b_out.get_weight(0), "Distinct failed to retract")

    finally:
        b_in.free()
        b_out.free()
        trace.close()


def test_reduce_op(base_dir):
    log("[DBSP] Testing Reduce (Linear & Non-Linear)...")

    cols = newlist_hint(3)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="grp"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    in_schema = types.TableSchema(cols, 2)  # PK is col 2

    sum_agg = functions.UniversalAccumulator(1, functions.AGG_SUM, types.TYPE_I64)
    out_schema = types._build_reduce_output_schema(in_schema, [0], sum_agg)

    t_in_path = os.path.join(base_dir, "red_in")
    t_out_path = os.path.join(base_dir, "red_out")
    trace_in = EphemeralTable(t_in_path, "in", in_schema)
    trace_out = EphemeralTable(t_out_path, "out", out_schema)

    b_in = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)

    try:
        log("  - Linear Sum...")
        r = values.make_payload_row(in_schema)
        r.append_int(r_int64(10))  # grp 10
        r.append_int(r_int64(5))  # val 5
        b_in.append(r_uint128(1), r_int64(1), r)

        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], sum_agg, out_schema)
        assert_equal_i64(r_int64(1), b_out.get_weight(0), "Reduce insertion failed")
        assert_equal_i64(r_int64(5), b_out.get_row(0).get_int_signed(0), "Sum error")

        trace_in.ingest_batch(b_in)
        trace_out.ingest_batch(b_out)

        log("  - Retraction & Shortcut...")
        b_in.clear()
        b_out.clear()
        b_in.append(r_uint128(2), r_int64(1), r)  # Same group, new record

        c_in.close()
        c_out.close()
        c_in = trace_in.create_cursor()
        c_out = trace_out.create_cursor()

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], sum_agg, out_schema)
        b_out.consolidate()
        assert_equal_i(2, b_out.length(), "Incremental reduce failed to retract")

        log("  - Non-linear Max (History Replay)...")
        max_agg = functions.UniversalAccumulator(1, functions.AGG_MAX, types.TYPE_I64)
        b_in.clear()
        b_out.clear()
        r_large = values.make_payload_row(in_schema)
        r_large.append_int(r_int64(10))
        r_large.append_int(r_int64(100))
        b_in.append(r_uint128(3), r_int64(1), r_large)

        reduce.op_reduce(b_in, in_schema, c_in, c_out, b_out, [0], max_agg, out_schema)
        # Search for weight +1 (the insertion)
        found_max = False
        for i in range(b_out.length()):
            if b_out.get_weight(i) == r_int64(1):
                assert_equal_i64(r_int64(100), b_out.get_row(i).get_int_signed(0), "Max error")
                found_max = True
        assert_true(found_max, "Max insertion not found")

        c_in.close()
        c_out.close()
    finally:
        b_in.free()
        b_out.free()
        trace_in.close()
        trace_out.close()


def test_source_ops(base_dir):
    log("[DBSP] Testing Source Ops...")

    cols = newlist_hint(1)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    schema = types.TableSchema(cols, 0)

    table_path = os.path.join(base_dir, "src_table")
    table = EphemeralTable(table_path, "src", schema)

    b_in = batch.ArenaZSetBatch(schema)
    for i in range(10):
        b_in.append(r_uint128(i), r_int64(1), values.make_payload_row(schema))
    table.ingest_batch(b_in)

    b_out = batch.ArenaZSetBatch(schema)
    cursor = table.create_cursor()

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
