# vm_comprehensive_test.py

import sys
import os

from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import types, values, batch, comparator
from gnitz.dbsp import functions
from gnitz.vm import query, runtime, interpreter, instructions
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

def test_vm_linear_pipeline(base_dir):
    log("[VM] Testing Linear Pipeline (Filter -> Map -> Union -> Sink)...")
    
    # TableSchema handles resizable lists internally, so we can use a literal here.
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val")
    ]
    schema = types.TableSchema(cols, 0)

    src_table = EphemeralTable(os.path.join(base_dir, "src_lin"), "src", schema)
    sink_table = EphemeralTable(os.path.join(base_dir, "sink_lin"), "sink", schema)

    builder = query.QueryBuilder(src_table, schema)
    
    filter_fn = functions.IntEq(1, r_int64(42))
    map_fn = functions.IdentityMapper(schema)

    view = (builder
            .filter(filter_fn)
            .map(map_fn, schema)
            .union(None) 
            .sink(sink_table)
            .build())

    in_batch = batch.ArenaZSetBatch(schema)
    try:
        log("  - Ingesting Test Batch...")
        r1 = values.make_payload_row(schema)
        r1.append_int(r_int64(42))
        in_batch.append(r_uint128(1), r_int64(1), r1)
        
        r2 = values.make_payload_row(schema)
        r2.append_int(r_int64(100))
        in_batch.append(r_uint128(2), r_int64(1), r2)

        out_batch = view.process(in_batch)

        log("  - Verifying Interpreter Execution...")
        assert_equal_i(1, out_batch.length(), "Pipeline failed to filter/map correctly")
        assert_equal_u128(r_uint128(1), out_batch.get_pk(0), "Wrong PK survived filter")
        
        sink_cursor = sink_table.create_cursor()
        sink_cursor.seek(r_uint128(1))
        assert_true(sink_cursor.is_valid(), "IntegrateOp failed to persist to sink")
        assert_equal_i64(r_int64(1), sink_cursor.weight(), "Persisted weight mismatch")
        sink_cursor.close()

    finally:
        in_batch.free()
        view.close()
        src_table.close()
        sink_table.close()


def test_vm_reduce_and_distinct(base_dir):
    log("[VM] Testing Non-Linear Pipeline (Reduce & Distinct)...")
    
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="group_pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val")
    ]
    schema = types.TableSchema(cols, 0)

    src_table = EphemeralTable(os.path.join(base_dir, "src_red"), "src", schema)

    # Use a literal list [0] here. Instruction.REDUCE uses [*] hint.
    # Passing a resizable list (from newlist_hint) would trigger ListChangeUnallowed.
    agg_fn = functions.SumAggregateFunction(1, types.TYPE_I64)
    group_cols = [0]

    view = (query.QueryBuilder(src_table, schema)
            .reduce(group_cols, agg_fn) 
            .distinct()
            .build())

    in_batch = batch.ArenaZSetBatch(schema)
    try:
        log("  - Tick 1: Accumulation...")
        r1 = values.make_payload_row(schema)
        r1.append_int(r_int64(5))
        in_batch.append(r_uint128(10), r_int64(1), r1)

        r2 = values.make_payload_row(schema)
        r2.append_int(r_int64(15))
        in_batch.append(r_uint128(10), r_int64(1), r2)

        out_batch1 = view.process(in_batch)
        
        assert_equal_i(1, out_batch1.length(), "Reduce tick 1 length mismatch")
        assert_equal_u128(r_uint128(10), out_batch1.get_pk(0), "Reduce tick 1 PK mismatch")
        assert_equal_i64(r_int64(1), out_batch1.get_weight(0), "Distinct weight mismatch")
        
        row_out = out_batch1.get_row(0)
        assert_equal_i64(r_int64(20), row_out.get_int_signed(0), "Sum aggregation mismatch")

        log("  - Tick 2: Retraction & Stateful Tracing...")
        in_batch.clear()
        
        r3 = values.make_payload_row(schema)
        r3.append_int(r_int64(15))
        in_batch.append(r_uint128(10), r_int64(-1), r3) 

        out_batch2 = view.process(in_batch)
        
        assert_equal_i(2, out_batch2.length(), "Reduce tick 2 length mismatch")
        
        row_a = out_batch2.get_row(0)
        row_b = out_batch2.get_row(1)
        
        if row_a.get_int_signed(0) == r_int64(5):
            assert_equal_i64(r_int64(1), out_batch2.get_weight(0), "Addition weight mismatch")
            assert_equal_i64(r_int64(20), row_b.get_int_signed(0), "Retraction value mismatch")
            assert_equal_i64(r_int64(-1), out_batch2.get_weight(1), "Retraction weight mismatch")
        else:
            assert_equal_i64(r_int64(-1), out_batch2.get_weight(0), "Retraction weight mismatch")
            assert_equal_i64(r_int64(5), row_b.get_int_signed(0), "Addition value mismatch")
            assert_equal_i64(r_int64(1), out_batch2.get_weight(1), "Addition weight mismatch")

    finally:
        in_batch.free()
        view.close()
        src_table.close()


def test_vm_join_persistent(base_dir):
    log("[VM] Testing Incremental Join (Delta ⋈ Trace)...")
    
    cols_l = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val_l")
    ]
    schema_l = types.TableSchema(cols_l, 0)
    
    cols_r = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_STRING, name="val_r")
    ]
    schema_r = types.TableSchema(cols_r, 0)

    src_table = EphemeralTable(os.path.join(base_dir, "src_join"), "src", schema_l)
    trace_table = EphemeralTable(os.path.join(base_dir, "trace_join"), "trace", schema_r)

    log("  - Pre-populating persistent trace...")
    batch_r = batch.ArenaZSetBatch(schema_r)
    rr = values.make_payload_row(schema_r)
    rr.append_string("Matched!")
    batch_r.append(r_uint128(99), r_int64(2), rr)
    trace_table.ingest_batch(batch_r)
    batch_r.free()

    view = (query.QueryBuilder(src_table, schema_l)
            .join_persistent(trace_table)
            .build())

    in_batch = batch.ArenaZSetBatch(schema_l)
    try:
        log("  - Executing Join Delta...")
        rl = values.make_payload_row(schema_l)
        rl.append_int(r_int64(1000))
        in_batch.append(r_uint128(99), r_int64(3), rl) 

        rl2 = values.make_payload_row(schema_l)
        rl2.append_int(r_int64(2000))
        in_batch.append(r_uint128(100), r_int64(1), rl2)

        out_batch = view.process(in_batch)

        assert_equal_i(1, out_batch.length(), "Join output length mismatch")
        assert_equal_u128(r_uint128(99), out_batch.get_pk(0), "Join PK mismatch")
        assert_equal_i64(r_int64(6), out_batch.get_weight(0), "Join weight multiplication mismatch")

        out_row = out_batch.get_row(0)
        assert_equal_i64(r_int64(1000), out_row.get_int_signed(0), "Left payload mismatch")
        assert_equal_s("Matched!", out_row.get_str(1), "Right payload mismatch")
        
    finally:
        in_batch.free()
        view.close()
        src_table.close()
        trace_table.close()


def test_vm_edge_cases(base_dir):
    log("[VM] Testing Edge Cases and Builder Exceptions...")
    
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val")
    ]
    schema = types.TableSchema(cols, 0)
    src_table = EphemeralTable(os.path.join(base_dir, "src_edge"), "src", schema)

    log("  - Processing empty batch...")
    view = query.QueryBuilder(src_table, schema).build()
    in_batch = batch.ArenaZSetBatch(schema)
    try:
        out_batch = view.process(in_batch)
        assert_equal_i(0, out_batch.length(), "Empty batch should produce empty output")
    finally:
        in_batch.free()
        view.close()

    log("  - Validating internal Ghost Property annihilation...")
    view2 = query.QueryBuilder(src_table, schema).distinct().build()
    in_batch2 = batch.ArenaZSetBatch(schema)
    try:
        row = values.make_payload_row(schema)
        row.append_int(r_int64(123))
        
        in_batch2.append(r_uint128(1), r_int64(5), row)
        in_batch2.append(r_uint128(1), r_int64(-5), row)
        
        out_batch2 = view2.process(in_batch2)
        assert_equal_i(0, out_batch2.length(), "Ghost Property failed: VM emitted annihilated data")
    finally:
        in_batch2.free()
        view2.close()

    log("  - Validating QueryBuilder schema enforcement...")
    raised = False
    try:
        builder = query.QueryBuilder(src_table, schema)
        agg_fn = functions.MinAggregateFunction(1, types.TYPE_I64)
        # Use literal [0] to match REDUCE instruction expectations
        group_cols = [0]
        builder.reduce(group_cols, agg_fn, -1)
    except query.QueryError:
        raised = True
    assert_true(raised, "Builder failed to reject non-linear reduce without trace_in")

    src_table.close()

# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------

def entry_point(argv):
    os.write(1, "--- GnitzDB Comprehensive VM Package Test ---\n")
    base_dir = "vm_test_data"
    cleanup_dir(base_dir)
    os.mkdir(base_dir)

    try:
        test_vm_linear_pipeline(base_dir)
        test_vm_reduce_and_distinct(base_dir)
        test_vm_join_persistent(base_dir)
        test_vm_edge_cases(base_dir)
        os.write(1, "\nALL VM TEST PATHS PASSED\n")
    except Exception as e:
        os.write(2, "TEST FAILED: " + str(e) + "\n")
        return 1
    finally:
        cleanup_dir(base_dir)

    return 0

def target(driver, args):
    return entry_point, None

if __name__ == "__main__":
    entry_point(sys.argv)
