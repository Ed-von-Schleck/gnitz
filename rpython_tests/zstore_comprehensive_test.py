# zstore_comprehensive_test.py

import os
from rpython.rlib import rposix, rsocket
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import types, values, batch
from gnitz.storage import table
from gnitz.catalog import system_tables, engine
from gnitz.vm import runtime, interpreter, instructions
from gnitz.server import ipc


def assert_true(condition, msg):
    if not condition:
        os.write(2, "\n!!! ASSERTION FAILURE: " + msg + " !!!\n")
        raise Exception("Assertion Failed")


def assert_equal_i(expected, actual, msg):
    if expected != actual:
        os.write(2, "\n!!! VALUE MISMATCH: " + msg + " !!!\n")
        os.write(2, "   Expected: " + str(expected) + "\n")
        os.write(2, "   Actual:   " + str(actual) + "\n")
        raise Exception("Value Mismatch")


def cleanup_path(path):
    """
    RPython-compliant cleanup.
    1. Avoids os.path.join to prevent Annotator slicing proof failures.
    2. Uses newlist_hint to prevent resizability poisoning.
    3. Uses index-based loops to avoid heterogenous tuple iteration errors.
    """
    if not os.path.exists(path):
        return

    files = newlist_hint(2)
    files.append("t1.wal")
    files.append("MANIFEST")

    for i in range(len(files)):
        f_name = files[i]
        full_p = path + "/" + f_name
        try:
            os.unlink(full_p)
        except OSError:
            pass

    subs = newlist_hint(3)
    subs.append("chunk_test")
    subs.append("public")
    subs.append("source")

    for i in range(len(subs)):
        sub_path = path + "/" + subs[i]
        if os.path.exists(sub_path):
            for j in range(len(files)):
                f_name = files[j]
                try:
                    os.unlink(sub_path + "/" + f_name)
                except OSError:
                    pass
            try:
                rposix.rmdir(sub_path)
            except OSError:
                pass

    try:
        rposix.rmdir(path)
    except OSError:
        pass


def test_vm_chunking_and_yield():
    os.write(1, "[VM] Testing Chunked Execution and YIELD...\n")

    schema = system_tables.make_tables_schema()
    base_dir = "/tmp/gnitz_test_vm"
    cleanup_path(base_dir)
    if not os.path.exists(base_dir):
        os.mkdir(base_dir, 0o755)

    pt = table.PersistentTable(base_dir + "/chunk_test", "t1", schema)

    b = batch.ArenaZSetBatch(schema)
    for i in range(3):
        row = values.make_payload_row(schema)
        row.append_int(r_int64(1))
        row.append_string("name_" + str(i))
        row.append_string("dir")
        row.append_int(r_int64(0))
        row.append_int(r_int64(0))
        b.append(r_uint128(i), r_int64(1), row)

    pt.ingest_batch(b)

    vm_schema = runtime.VMSchema(schema)
    reg_file = runtime.RegisterFile(16)
    reg_trace = runtime.TraceRegister(0, vm_schema, pt.create_cursor(), pt)
    reg_out = runtime.DeltaRegister(1, vm_schema)
    reg_file.registers[0] = reg_trace
    reg_file.registers[1] = reg_out

    program = newlist_hint(4)
    program.append(instructions.ScanTraceOp(reg_trace, reg_out, chunk_limit=2))
    program.append(instructions.YieldOp(reason=ipc.YIELD_REASON_BUFFER_FULL))
    program.append(instructions.ScanTraceOp(reg_trace, reg_out, chunk_limit=2))
    program.append(instructions.HaltOp())

    ctx = runtime.ExecutionContext(reg_file)
    interp = interpreter.DBSPInterpreter(program)

    interp.resume(ctx)
    assert_equal_i(runtime.STATUS_YIELDED, ctx.status, "Should yield after 2 rows")
    assert_equal_i(2, reg_out.batch.length(), "Out batch size mismatch")

    reg_out.batch.clear()
    interp.resume(ctx)
    assert_equal_i(runtime.STATUS_HALTED, ctx.status, "Should halt after finish")
    assert_equal_i(1, reg_out.batch.length(), "Remaining row count mismatch")

    pt.close()
    b.free()
    os.write(1, "[VM] Chunked Execution Test Passed.\n")


def test_reactive_ddl_and_registry():
    os.write(1, "[CATALOG] Testing Reactive DDL Triggers...\n")

    base_dir = "/tmp/gnitz_test_ddl"
    cleanup_path(base_dir)
    if not os.path.exists(base_dir):
        os.mkdir(base_dir, 0o755)

    db = engine.open_engine(base_dir)

    cols = newlist_hint(1)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="id"))

    db.create_table("public.test_table", cols, 0)

    # Path check confirms reactive side-effect of catalog mutation
    table_path = base_dir + "/public/test_table_11"
    assert_true(os.path.exists(table_path), "DDL directory not created")

    family = db.get_table("public.test_table")
    assert_equal_i(11, family.table_id, "Table ID registration failed")

    db.close()
    os.write(1, "[CATALOG] Reactive DDL Test Passed.\n")


def test_vm_dataflow_cascade_logic():
    """
    Compliant replacement for Cascade test.
    Tests that the VM correctly propagates data from a Persistent source
    to a Sink register, simulating one 'hop' in the DAG.
    """
    os.write(1, "[VM] Testing Dataflow Cascade Logic...\n")

    schema = system_tables.make_tables_schema()
    base_dir = "/tmp/gnitz_test_cascade_vm"
    cleanup_path(base_dir)
    if not os.path.exists(base_dir):
        os.mkdir(base_dir, 0o755)

    pt = table.PersistentTable(base_dir + "/source", "src", schema)

    # Add test data
    b = batch.ArenaZSetBatch(schema)
    row = values.make_payload_row(schema)
    row.append_int(r_int64(1))
    row.append_string("cascade_test")
    row.append_string("dir")
    row.append_int(r_int64(0))
    row.append_int(r_int64(0))
    b.append(r_uint128(999), r_int64(1), row)
    pt.ingest_batch(b)

    # Setup Registers
    vm_schema = runtime.VMSchema(schema)
    reg_file = runtime.RegisterFile(4)
    reg_trace = runtime.TraceRegister(0, vm_schema, pt.create_cursor(), pt)
    reg_delta = runtime.DeltaRegister(1, vm_schema)
    reg_file.registers[0] = reg_trace
    reg_file.registers[1] = reg_delta

    # Program: Scan source into sink
    program = newlist_hint(2)
    program.append(instructions.ScanTraceOp(reg_trace, reg_delta, chunk_limit=0))
    program.append(instructions.HaltOp())

    ctx = runtime.ExecutionContext(reg_file)
    interp = interpreter.DBSPInterpreter(program)
    interp.resume(ctx)

    assert_equal_i(1, reg_delta.batch.length(), "Cascade failed to propagate data")
    assert_equal_i(
        999, intmask(r_uint64(reg_delta.batch.get_pk(0))), "Cascade data corruption"
    )

    pt.close()
    b.free()
    os.write(1, "[VM] Dataflow Cascade Logic Passed.\n")


def entry_point(argv):
    os.write(1, "=== GnitzDB Z-Store Comprehensive Test Suite ===\n")
    try:
        test_vm_chunking_and_yield()
        test_reactive_ddl_and_registry()
        test_vm_dataflow_cascade_logic()
        os.write(1, "\nALL SYSTEM INTEGRATION TESTS PASSED\n")
    except Exception:
        os.write(2, "\n!! TEST SUITE FAILED !!\n")
        return 1
    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    import sys

    entry_point(sys.argv)
