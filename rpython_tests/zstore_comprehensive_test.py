# zstore_comprehensive_test.py

import os
from rpython.rlib import rposix, rsocket, jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, values, batch, errors
from gnitz.storage import buffer, mmap_posix, table
from gnitz.catalog import system_tables, engine, program_cache, system_records
from gnitz.vm import runtime, interpreter, instructions
from gnitz.server import ipc, ipc_ffi, executor

# ------------------------------------------------------------------------------
# RPython Testing Infrastructure
# ------------------------------------------------------------------------------

def assert_true(condition, msg):
    if not condition:
        os.write(2, "FAIL: " + msg + "\n")
        raise Exception("Assertion Failed")

def assert_equal_i(expected, actual, msg):
    if expected != actual:
        os.write(2, "FAIL: " + msg + " (Expected " + str(expected) + ", got " + str(actual) + ")\n")
        raise Exception("Value Mismatch")

def assert_equal_s(expected, actual, msg):
    if expected != actual:
        os.write(2, "FAIL: " + msg + " (Expected " + expected + ", got " + actual + ")\n")
        raise Exception("String Mismatch")

def cleanup_dir(path):
    """Recursively delete directory for test idempotency."""
    try:
        # Very basic RPython-compatible cleanup
        rposix.rmdir(path)
    except OSError:
        pass

# ------------------------------------------------------------------------------
# Test Case 1: VM Chunking & Yield (Phase 1)
# ------------------------------------------------------------------------------

def test_vm_chunking_and_yield():
    os.write(1, "[VM] Testing Chunked Execution and YIELD...\n")
    
    schema = system_tables.make_tables_schema()
    # Create a persistent table with 3 records
    base_dir = "/tmp/gnitz_test_vm"
    if not os.path.exists(base_dir): os.mkdir(base_dir)
    
    pt = table.PersistentTable(base_dir + "/chunk_test", "t1", schema)
    b = batch.ArenaZSetBatch(schema)
    for i in range(3):
        row = values.make_payload_row(schema)
        # Fill required columns for Tables schema
        row.append_int(r_int64(1)) # sid
        row.append_string("name_" + str(i))
        row.append_string("dir")
        row.append_int(r_int64(0)) # pk_idx
        row.append_int(r_int64(0)) # lsn
        b.append(r_uint128(i), r_int64(1), row)
    pt.ingest_batch(b)
    
    # Define Program: SCAN_TRACE with chunk_limit=2, then YIELD
    vm_schema = runtime.VMSchema(schema)
    reg_file = runtime.RegisterFile(16)
    reg_trace = runtime.TraceRegister(0, vm_schema, pt.create_cursor(), pt)
    reg_out = runtime.DeltaRegister(1, vm_schema)
    reg_file.registers[0] = reg_trace
    reg_file.registers[1] = reg_out
    
    program = [
        instructions.ScanTraceOp(reg_trace, reg_out, chunk_limit=2),
        instructions.YieldOp(reason=ipc.YIELD_REASON_BUFFER_FULL),
        instructions.ScanTraceOp(reg_trace, reg_out, chunk_limit=2),
        instructions.HaltOp()
    ]
    
    ctx = runtime.ExecutionContext(reg_file)
    interp = interpreter.DBSPInterpreter(program)
    
    # First Tick
    interp.resume(ctx)
    assert_equal_i(runtime.STATUS_YIELDED, ctx.status, "Should yield after 2 rows")
    assert_equal_i(2, reg_out.batch.length(), "Out batch should have 2 records")
    
    # Resume Tick
    reg_out.batch.clear()
    interp.resume(ctx)
    assert_equal_i(runtime.STATUS_HALTED, ctx.status, "Should halt after last row")
    assert_equal_i(1, reg_out.batch.length(), "Out batch should have 1 remaining record")
    
    pt.close()
    b.free()

# ------------------------------------------------------------------------------
# Test Case 2: Reactive DDL & Directory Management (Phase 3)
# ------------------------------------------------------------------------------

def test_reactive_ddl_and_registry():
    os.write(1, "[CATALOG] Testing Reactive DDL Triggers...\n")
    
    base_dir = "/tmp/gnitz_test_ddl"
    if os.path.exists(base_dir + "/public/test_table_100"):
        # Simulated cleanup
        pass

    db = engine.open_engine(base_dir)
    
    # 1. Create Table (Triggers reactive _on_table_delta -> mkdir)
    cols = [types.ColumnDefinition(types.TYPE_U64, name="id")]
    db.create_table("public.test_table", cols, 0)
    
    # Check physical side-effect
    # Table ID starts at 11 based on Phase 4 system_tables.py
    table_path = base_dir + "/public/test_table_11"
    assert_true(os.path.exists(table_path), "Reactive DDL failed to create table directory")
    
    # 2. Check Registry lookup
    family = db.get_table("public.test_table")
    assert_equal_i(11, family.table_id, "Registry ID mismatch")
    
    db.close()

# ------------------------------------------------------------------------------
# Test Case 3: Dataflow Cascade & Broadcast (Phases 2 & 4)
# ------------------------------------------------------------------------------

def test_dataflow_cascade_and_broadcast():
    os.write(1, "[SERVER] Testing DAG Cascade and O(1) Broadcast...\n")
    
    base_dir = "/tmp/gnitz_test_cascade"
    db = engine.open_engine(base_dir)
    exec_loop = executor.ServerExecutor(db)
    
    # Setup: 1 Table, 1 View depending on it
    cols = [types.ColumnDefinition(types.TYPE_U64, name="val")]
    tbl_family = db.create_table("public.source", cols, 0)
    
    # Manually register a view in _system._views and its program in _instructions
    view_id = 100
    # In a real system, SQL compiler does this. Here we write records directly.
    # Instruction: Map (Reg 0 -> Reg 1), basically an identity view.
    instr_batch = batch.ZSetBatch(db.sys.instructions.schema)
    # PK = (100 << 64) | 0
    pk = (r_uint128(view_id) << 64) | r_uint128(0)
    row = values.make_payload_row(db.sys.instructions.schema)
    row.append_int(r_int64(instructions.Instruction.MAP)) # opcode
    row.append_int(r_int64(0)) # reg_in
    row.append_int(r_int64(1)) # reg_out
    # ... fill other 18 cols with 0 ...
    for _ in range(11): row.append_int(r_int64(0))
    row.append_int(r_int64(0)) # target_id
    row.append_int(r_int64(0)) # func_id
    row.append_int(r_int64(0)) # agg_func_id
    row.append_string("")      # group_by
    row.append_int(r_int64(0)) # chunk
    row.append_int(r_int64(0)) # jump
    row.append_int(r_int64(0)) # yield
    instr_batch.append(pk, r_int64(1), row)
    db.sys.instructions.ingest_batch(instr_batch)
    db.sys.instructions.flush()
    
    # Add dependency: View 100 depends on Table 11
    dep_batch = batch.ZSetBatch(db.sys.view_deps.schema)
    row_dep = values.make_payload_row(db.sys.view_deps.schema)
    row_dep.append_int(r_int64(view_id))
    row_dep.append_int(r_int64(0)) # dep_view
    row_dep.append_int(r_int64(11)) # dep_table
    dep_batch.append(r_uint128(1), r_int64(1), row_dep)
    db.sys.view_deps.ingest_batch(dep_batch)
    db.sys.view_deps.flush()
    
    # 2. Setup Mock Subscription
    client_id = 555
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
    exec_loop.client_sockets[s2.fd] = s2
    exec_loop.client_to_fd[client_id] = s2.fd
    
    sub_batch = batch.ZSetBatch(db.sys.subscriptions.schema)
    row_sub = values.make_payload_row(db.sys.subscriptions.schema)
    row_sub.append_int(r_int64(view_id))
    row_sub.append_int(r_int64(client_id))
    sub_batch.append(r_uint128(1), r_int64(1), row_sub)
    db.sys.subscriptions.ingest_batch(sub_batch)
    db.sys.subscriptions.flush()
    
    # 3. Trigger Ingestion -> Cascade -> Broadcast
    test_delta = batch.ZSetBatch(tbl_family.schema)
    r = values.make_payload_row(tbl_family.schema)
    test_delta.append(r_uint128(999), r_int64(1), r)
    
    # This should trigger the DAG cascade
    exec_loop._evaluate_dag(tbl_family.table_id, test_delta)
    
    # 4. Receive Broadcast on s1
    payload = ipc.receive_payload(s1.fd, db.registry)
    assert_equal_i(view_id, payload.target_id, "Broadcast target ID mismatch")
    assert_equal_i(1, payload.batch.length(), "Broadcast batch should contain cascading delta")
    assert_equal_i(999, intmask(r_uint64(payload.batch.get_pk(0))), "Data corruption in cascade")
    
    payload.close()
    s1.close()
    s2.close()
    db.close()

# ------------------------------------------------------------------------------
# Test Case 4: Algebraic Disconnect Cleanup (Phase 4)
# ------------------------------------------------------------------------------

def test_client_disconnect_algebra():
    os.write(1, "[SERVER] Testing Algebraic Retraction on Disconnect...\n")
    
    base_dir = "/tmp/gnitz_test_disconnect"
    db = engine.open_engine(base_dir)
    exec_loop = executor.ServerExecutor(db)
    
    client_id = 777
    view_id = 200
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
    
    # Manually register the socket and a subscription
    exec_loop.active_fds.append(s2.fd)
    exec_loop.client_sockets[s2.fd] = s2
    exec_loop.fd_to_client[s2.fd] = client_id
    
    # Add subscription record
    sub_schema = db.sys.subscriptions.schema
    sub_batch = batch.ZSetBatch(sub_schema)
    row = values.make_payload_row(sub_schema)
    row.append_int(r_int64(view_id))
    row.append_int(r_int64(client_id))
    sub_batch.append(r_uint128(99), r_int64(1), row)
    db.sys.subscriptions.ingest_batch(sub_batch)
    db.sys.subscriptions.flush()
    
    # Verify subscription exists
    assert_true(db.sys.subscriptions.has_pk(r_uint128(99)), "Subscription not recorded")
    
    # Trigger Disconnect Cleanup
    exec_loop._cleanup_client(s2.fd)
    
    # Verify subscription was algebraically retracted (weight should be 0)
    cursor = db.sys.subscriptions.create_cursor()
    cursor.seek(r_uint128(99))
    # In DBSP/ZSetStore, a retracted record either disappears or has 0 weight
    exists = cursor.is_valid() and cursor.key() == r_uint128(99)
    if exists:
        assert_equal_i(0, intmask(cursor.weight()), "Subscription weight should be retracted to 0")
    
    s1.close()
    db.close()

# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------

def entry_point(argv):
    os.write(1, "=== GnitzDB Z-Store Comprehensive Test Suite ===\n")
    try:
        test_vm_chunking_and_yield()
        test_reactive_ddl_and_registry()
        test_dataflow_cascade_and_broadcast()
        test_client_disconnect_algebra()
        os.write(1, "\nALL SYSTEM INTEGRATION TESTS PASSED\n")
    except Exception as e:
        os.write(2, "\n!! TEST SUITE CRASHED !!\n")
        return 1
    return 0

def target(driver, args):
    return entry_point, None

if __name__ == "__main__":
    import sys
    entry_point(sys.argv)
