# gnitz/rpython_tests/vm_additional_test.py

import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, values, batch
from gnitz.vm import query, runtime
from gnitz.storage.ephemeral_table import EphemeralTable

# ------------------------------------------------------------------------------
# RPython Testing Helpers
# ------------------------------------------------------------------------------

def log(msg):
    os.write(1, msg + "\n")

def fail(msg):
    os.write(2, "FAILURE: " + msg + "\n")
    raise Exception(msg)

def assert_true(condition, msg):
    if not condition:
        fail(msg)

def assert_equal_i(expected, actual, msg):
    if expected != actual:
        fail(msg + " (Expected " + str(expected) + ", got " + str(actual) + ")")

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
# Test Cases
# ------------------------------------------------------------------------------

def test_chunked_scan_resume(base_dir):
    """
    Tests SCAN_TRACE + YIELD + JUMP loop.
    Simulates a host processing a large table in chunks of 10.
    """
    log("[VM] Testing Chunked Scan and Resume Loop...")
    
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="id"),
        types.ColumnDefinition(types.TYPE_I64, name="val")
    ]
    schema = types.TableSchema(cols, 0)
    table_path = os.path.join(base_dir, "chunk_table")
    table = EphemeralTable(table_path, "chunks", schema)

    # 1. Fill table with 25 records
    b = batch.ArenaZSetBatch(schema)
    for i in range(25):
        row = values.make_payload_row(schema)
        row.append_int(r_int64(i * 10))
        b.append(r_uint128(i), r_int64(1), row)
    table.ingest_batch(b)
    b.free()

    # 2. Build a looping circuit
    builder = query.QueryBuilder(table)
    builder.scan(table, chunk_limit=10) 
    builder.yield_(runtime.YIELD_REASON_ROW_LIMIT)
    builder.clear_deltas()
    builder.jump(0)
    
    view = builder.build()
    reg_file = view.context.reg_file

    try:
        # CHUNK 1 (Records 0-9)
        status = view.run()
        assert_equal_i(runtime.STATUS_YIELDED, status, "Should yield after 10 rows")
        assert_equal_i(10, reg_file.get_register(view.output_reg_id).batch.length(), "Chunk 1 size mismatch")
        assert_equal_i(2, view.context.pc, "PC should be at Index 2")

        # CHUNK 2 (Records 10-19)
        status = view.run() 
        assert_equal_i(runtime.STATUS_YIELDED, status, "Should yield after next 10 rows")
        assert_equal_i(10, reg_file.get_register(view.output_reg_id).batch.length(), "Chunk 2 size mismatch")
        assert_equal_i(2, view.context.pc, "PC should be back at Index 2")

        # CHUNK 3 (Records 20-24)
        status = view.run()
        assert_equal_i(runtime.STATUS_YIELDED, status, "Should yield after remaining 5 rows")
        assert_equal_i(5, reg_file.get_register(view.output_reg_id).batch.length(), "Chunk 3 size mismatch")

    finally:
        view.close()
        table.close()


def test_seek_trace_logic(base_dir):
    """
    Tests SEEK_TRACE by passing a key through a Delta register.
    """
    log("[VM] Testing SEEK_TRACE and Point Lookup...")
    
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="id"),
        types.ColumnDefinition(types.TYPE_I64, name="val")
    ]
    schema = types.TableSchema(cols, 0)
    table_path = os.path.join(base_dir, "seek_table")
    table = EphemeralTable(table_path, "seek", schema)

    b = batch.ArenaZSetBatch(schema)
    ids = [10, 20, 30, 40, 50]
    for i in ids:
        row = values.make_payload_row(schema)
        row.append_int(r_int64(i * 100))
        b.append(r_uint128(i), r_int64(1), row)
    table.ingest_batch(b)
    b.free()

    builder = query.QueryBuilder(table)
    
    # 1. Create a cursor and register it
    cursor = table.create_cursor()
    builder.cursors.append(cursor) # Ensure it gets closed by view.close()
    tr_idx, tr_reg = builder._add_register(schema, is_trace=True, cursor=cursor, table=table)
    
    # 2. Add SEEK and SCAN instructions using the same trace register
    # Register 0 is input. Seek cursor to input.PK, then scan 1 row from that position.
    builder.seek_trace(tr_idx, 0)
    builder.scan_trace(tr_idx, chunk_limit=1) 
    
    view = builder.build()
    
    seek_batch = batch.ArenaZSetBatch(schema)
    r = values.make_payload_row(schema)
    r.append_int(r_int64(0))
    seek_batch.append(r_uint128(30), r_int64(1), r)
    
    try:
        out_batch = view.process(seek_batch)
        assert_equal_i(1, out_batch.length(), "Should find exactly 1 row")
        # Extract PK from the result
        pk_res = intmask(r_uint64(out_batch.get_pk(0)))
        assert_equal_i(30, pk_res, "Seek found wrong record")
    finally:
        seek_batch.free()
        view.close()
        table.close()


def test_empty_scans(base_dir):
    log("[VM] Testing Empty Table Scan...")
    
    cols = [types.ColumnDefinition(types.TYPE_U64, name="id")]
    schema = types.TableSchema(cols, 0)
    table = EphemeralTable(os.path.join(base_dir, "empty_table"), "empty", schema)
    
    builder = query.QueryBuilder(table)
    builder.scan(table, chunk_limit=10)
    view = builder.build()
    
    try:
        view.context.reset()
        status = view.run()
        assert_equal_i(runtime.STATUS_HALTED, status, "Empty table should HALT immediately")
        assert_equal_i(0, view.context.reg_file.get_register(view.output_reg_id).batch.length(), "Batch should be empty")
    finally:
        view.close()
        table.close()

# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------

def entry_point(argv):
    log("--- GnitzDB Additional VM Logic Tests ---")
    base_dir = "vm_additional_data"
    cleanup_dir(base_dir)
    os.mkdir(base_dir)

    try:
        test_chunked_scan_resume(base_dir)
        test_seek_trace_logic(base_dir)
        test_empty_scans(base_dir)
        log("ALL ADDITIONAL VM TESTS PASSED")
    except Exception as e:
        log("TEST FAILED")
        return 1
    finally:
        cleanup_dir(base_dir)
    return 0

def target(driver, args):
    return entry_point, None
