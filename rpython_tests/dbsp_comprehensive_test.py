# dbsp_comprehensive_test.py

import sys
import os

from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import types, values, batch, comparator
from gnitz.dbsp.ops import linear, join, reduce, distinct
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

def assert_equal_s(expected, actual, msg):
    if expected != actual:
        fail(msg + " (Expected '" + expected + "', got '" + actual + "')")

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
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))        # Idx 0
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val_int"))   # Idx 1
    cols.append(types.ColumnDefinition(types.TYPE_F64, name="val_float")) # Idx 2
    schema = types.TableSchema(cols, 0)
    
    b_in = batch.ArenaZSetBatch(schema)
    b_filtered = batch.ArenaZSetBatch(schema)
    b_mapped = None
    b_neg = batch.ArenaZSetBatch(schema)
    b_union = batch.ArenaZSetBatch(schema)

    try:
        log("  - Ingesting rows...")
        r1 = values.make_payload_row(schema)
        r1.append_int(r_int64(10))
        r1.append_float(1.5)
        b_in.append(r_uint128(1), r_int64(1), r1)

        r2 = values.make_payload_row(schema)
        r2.append_int(r_int64(20))
        r2.append_float(15.5)
        b_in.append(r_uint128(2), r_int64(2), r2)

        log("  - Testing Filter (FloatGt)...")
        f_gt = functions.FloatGt(2, 10.0) # Column 2 is the float
        linear.op_filter(b_in, b_filtered, f_gt)
        assert_equal_i(1, b_filtered.length(), "Filter failed to drop row")
        assert_equal_u128(r_uint128(2), b_filtered.get_pk(0), "Filter kept wrong row")

        log("  - Testing Map (Projection)...")
        map_cols = newlist_hint(2)
        map_cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
        map_cols.append(types.ColumnDefinition(types.TYPE_I64, name="val_int"))
        map_schema = types.TableSchema(map_cols, 0)
        b_mapped = batch.ArenaZSetBatch(map_schema)

        # src_indices references original Table indices.
        # We project val_int (Index 1). 
        # Index 0 is the PK and is handled automatically by op_map.
        src_indices = [1]
        src_types = [types.TYPE_I64.code]
        mapper = functions.ProjectionMapper(src_indices, src_types)
        
        linear.op_map(b_in, b_mapped, mapper, map_schema)
        assert_equal_i(2, b_mapped.length(), "Map project failed")
        assert_equal_i64(r_int64(10), b_mapped.get_row(0).get_int_signed(0), "Mapped value mismatch")

        log("  - Testing Negate and Union...")
        linear.op_negate(b_in, b_neg)
        linear.op_union(b_in, b_neg, b_union)
        b_union.consolidate()
        assert_equal_i(0, b_union.length(), "Union/Negate failed to annihilate")

    finally:
        b_in.free()
        b_filtered.free()
        if b_mapped: b_mapped.free()
        b_neg.free()
        b_union.free()

def test_join_ops(base_dir):
    log("[DBSP] Testing Join Ops...")
    
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
    trace_r = None

    try:
        log("  - Ingesting join data...")
        rl = values.make_payload_row(schema_l); rl.append_int(r_int64(777))
        b_l.append(r_uint128(10), r_int64(2), rl)

        rr = values.make_payload_row(schema_r); rr.append_string("match")
        b_r.append(r_uint128(10), r_int64(3), rr)

        log("  - Testing Delta-Delta Join...")
        join.op_join_delta_delta(b_l, b_r, b_out, schema_l, schema_r)
        assert_equal_i(1, b_out.length(), "Join failed to find match")
        assert_equal_i64(r_int64(6), b_out.get_weight(0), "Join weight multiplication error")

        log("  - Testing Delta-Trace Join...")
        b_out.clear()
        trace_path = os.path.join(base_dir, "j_trace")
        trace_r = EphemeralTable(trace_path, "tr", schema_r)
        trace_r.ingest_batch(b_r)
        
        cursor_r = trace_r.create_cursor()
        join.op_join_delta_trace(b_l, cursor_r, b_out, schema_l, schema_r)
        cursor_r.close()
        
        assert_equal_i64(r_int64(6), b_out.get_weight(0), "Delta-Trace Join failed")

    finally:
        b_l.free()
        b_r.free()
        b_out.free()
        if trace_r: trace_r.close()

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
        r = values.make_payload_row(schema); r.append_int(r_int64(1))
        # Tick 1: Weight 10 -> Should output Weight 1
        b_in.append(r_uint128(1), r_int64(10), r)
        distinct.op_distinct(b_in, trace, b_out)
        assert_equal_i64(r_int64(1), b_out.get_weight(0), "Distinct failed to clamp")

        # Tick 2: Weight -5 -> Should output Weight 0 (Total 5 is still > 0)
        b_in.clear(); b_out.clear()
        b_in.append(r_uint128(1), r_int64(-5), r)
        distinct.op_distinct(b_in, trace, b_out)
        assert_equal_i(0, b_out.length(), "Distinct produced unnecessary update")

        # Tick 3: Weight -5 -> Should output Weight -1 (Total 0)
        b_in.clear(); b_out.clear()
        b_in.append(r_uint128(1), r_int64(-5), r)
        distinct.op_distinct(b_in, trace, b_out)
        assert_equal_i64(r_int64(-1), b_out.get_weight(0), "Distinct failed to retract")

    finally:
        b_in.free()
        b_out.free()
        trace.close()

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
        log("\nALL DBSP TESTS PASSED")
    except Exception as e:
        # Standard Exception printing for RPython-translated binaries
        os.write(2, "FAILURE\n")
        return 1
    finally:
        cleanup_dir(base_dir)

    return 0

def target(driver, args):
    return entry_point, None

if __name__ == "__main__":
    entry_point(sys.argv)
