# rpython_tests/zstore_comprehensive_test.py

import os
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import types, values, batch
from gnitz.core.errors import LayoutError
from gnitz.catalog import system_tables as sys
from gnitz.catalog import engine, identifiers
from gnitz.catalog.metadata import ensure_dir
from gnitz.vm import runtime, interpreter, instructions

# -- RPython Safe Hex Formatting ----------------------------------------------


def _u64_to_hex_padded(val):
    """
    Manual hex conversion because RPython % formatting does not support
    zero-padding/width specifiers for hex.
    """
    chars = "0123456789abcdef"
    res = ["0"] * 16
    temp = val
    for i in range(15, -1, -1):
        res[i] = chars[intmask(temp & r_uint64(0xF))]
        temp >>= 4
    return "".join(res)


# -- RPython Test Utilities ---------------------------------------------------


def log(msg):
    os.write(1, "[TEST] " + msg + "\n")


def log_step(name):
    os.write(1, "\n[CHECKPOINT] " + name + "...\n")


def fail(msg):
    os.write(2, "\n!!! CRITICAL TEST FAILURE !!!\n")
    os.write(2, msg + "\n")
    raise Exception("Test Failure")


def assert_true(cond, msg):
    if not cond:
        fail("Assertion Failed: " + msg)


def assert_equal_i(expected, actual, msg):
    if expected != actual:
        fail(msg + " -> Expected: %d, Actual: %d" % (expected, actual))


def assert_equal_u128(expected, actual, msg):
    if expected != actual:
        hi_e = r_uint64(expected >> 64)
        lo_e = r_uint64(expected)
        hi_a = r_uint64(actual >> 64)
        lo_a = r_uint64(actual)

        os.write(
            2,
            "   Expected: " + _u64_to_hex_padded(hi_e) + _u64_to_hex_padded(lo_e) + "\n",
        )
        os.write(
            2, "   Actual:   " + _u64_to_hex_padded(hi_a) + _u64_to_hex_padded(lo_a) + "\n"
        )
        fail(msg + " -> U128 Mismatch")


def dump_filesystem(path, indent=""):
    """
    Diagnostic helper to prove what files actually exist on disk.
    """
    if not os.path.exists(path):
        return
    try:
        items = os.listdir(path)
        for item in items:
            full_path = path + "/" + item
            # RPython: os.path.isdir/isfile is fine
            if os.path.isdir(full_path):
                log(indent + " [D] " + item)
                dump_filesystem(full_path, indent + "    ")
            else:
                log(indent + " [F] " + item)
    except OSError:
        pass


def _recursive_delete(path):
    """
    Nasty but necessary RPython recursive cleanup.
    RPython does not provide shutil.rmtree.
    """
    if not os.path.exists(path):
        return
    if os.path.isdir(path):
        items = os.listdir(path)
        for item in items:
            _recursive_delete(path + "/" + item)
        try:
            rposix.rmdir(path)
        except OSError:
            pass
    else:
        try:
            os.unlink(path)
        except OSError:
            pass


def cleanup_base_dir(path):
    """
    Aggressive cleanup of the test environment.
    """
    if not os.path.exists(path):
        return
    log("Cleaning up: " + path)
    _recursive_delete(path)

    if os.path.exists(path):
        log("WARNING: Cleanup failed to remove base directory.")
        dump_filesystem(path)


# -- Simple Query Builder for View Intent -------------------------------------


class QueryBuilder(object):
    """
    Minimal builder to hold instructions for view creation.
    We use REAL runtime registers to satisfy RPython type unification.
    """

    _immutable_fields_ = ["instructions", "registers", "current_reg_idx"]

    def __init__(self, instructions_list, out_reg, registers):
        self.instructions = instructions_list
        self.registers = registers
        self.current_reg_idx = out_reg.reg_id


# -- The Main Test Suite ------------------------------------------------------


def test_programmable_zset_lifecycle():
    log("Starting Comprehensive Programmable Z-Set Lifecycle Test...")

    base_dir = "/tmp/gnitz_comp_test"
    cleanup_base_dir(base_dir)
    ensure_dir(base_dir)

    # 1. Bootstrapping
    log_step("Phase 1: Bootstrapping Engine")
    db = engine.open_engine(base_dir)
    assert_true(db.registry.has_schema("public"), "Public schema missing")

    # DIAGNOSTIC: Check registry state after bootstrap
    log("--- Registry Post-Bootstrap Diagnostic ---")
    app_exists = db.registry.has_schema("app")
    log("Schema 'app' exists: " + ("YES" if app_exists else "NO"))
    if app_exists:
        u_exists = db.registry.has("app", "users")
        o_exists = db.registry.has("app", "orders")
        log("Table 'app.users' exists: " + ("YES" if u_exists else "NO"))
        log("Table 'app.orders' exists: " + ("YES" if o_exists else "NO"))

    # 2. Schema and 128-bit Table Creation
    log_step("Phase 2: Creating 128-bit Relational Schema")
    db.create_schema("app")

    # app.users: PK is U128
    user_cols = newlist_hint(2)
    user_cols.append(types.ColumnDefinition(types.TYPE_U128, name="uid"))
    user_cols.append(types.ColumnDefinition(types.TYPE_STRING, name="username"))
    users_family = db.create_table("app.users", user_cols, 0)

    # app.orders: FK is U128 to users.uid
    order_cols = newlist_hint(3)
    order_cols.append(types.ColumnDefinition(types.TYPE_U64, name="oid"))
    order_cols.append(
        types.ColumnDefinition(
            types.TYPE_U128,
            name="uid",
            fk_table_id=users_family.table_id,
            fk_col_idx=0,
        )
    )
    order_cols.append(types.ColumnDefinition(types.TYPE_I64, name="amount"))
    orders_family = db.create_table("app.orders", order_cols, 0)

    log("Verifying Registry and FK Wiring...")
    assert_equal_i(1, len(orders_family.fk_constraints), "FK Constraint not wired")

    # 3. Referential Integrity Enforcement
    log_step("Phase 3: Testing Foreign Key Enforcement")
    # Synthetic 128-bit key
    u128_val = (r_uint128(0xDEADBEEF) << 64) | r_uint128(0xCAFEBABE)

    # PROBE: Verify user doesn't exist yet
    assert_true(
        not users_family.has_pk(u128_val), "User unexpectedly found before ingestion"
    )

    # Attempt to ingest order for non-existent user
    bad_batch = batch.ZSetBatch(orders_family.schema)
    r = values.make_payload_row(orders_family.schema)
    r.append_u128(r_uint64(0xCAFEBABE), r_uint64(0xDEADBEEF))
    r.append_int(r_int64(500))
    bad_batch.append(r_uint128(1), r_int64(1), r)

    fk_raised = False
    try:
        orders_family.ingest_batch(bad_batch)
    except LayoutError:
        fk_raised = True
        log("Caught expected FK violation (User not found)")
    assert_true(fk_raised, "FK violation should have raised LayoutError")
    bad_batch.free()

    # 4. Valid Data Ingestion
    log_step("Phase 4: Ingesting valid relational data")
    # Ingest User
    u_batch = batch.ZSetBatch(users_family.schema)
    ru = values.make_payload_row(users_family.schema)
    ru.append_string("alice")
    u_batch.append(u128_val, r_int64(1), ru)
    users_family.ingest_batch(u_batch)
    u_batch.free()

    assert_true(users_family.has_pk(u128_val), "User ingestion failed visibility check")

    # Ingest Order (now valid)
    o_batch = batch.ZSetBatch(orders_family.schema)
    ro = values.make_payload_row(orders_family.schema)
    ro.append_u128(r_uint64(0xCAFEBABE), r_uint64(0xDEADBEEF))
    ro.append_int(r_int64(1000))
    o_batch.append(r_uint128(101), r_int64(1), ro)
    orders_family.ingest_batch(o_batch)
    o_batch.free()

    assert_true(orders_family.has_pk(r_uint128(101)), "Order ingestion visibility failed")

    # 5. Reactive View (Programmable Z-Set)
    log_step("Phase 5: Creating Reactive View (Scan Users)")

    v_sch = runtime.VMSchema(users_family.schema)
    reg0 = runtime.TraceRegister(0, v_sch, users_family.create_cursor(), users_family)
    reg1 = runtime.DeltaRegister(1, v_sch)

    regs = newlist_hint(16)
    for _ in range(16):
        regs.append(None)
    regs[0] = reg0
    regs[1] = reg1

    instrs = newlist_hint(2)
    instrs.append(instructions.ScanTraceOp(reg0, reg1, chunk_limit=0))
    instrs.append(instructions.HaltOp())

    builder = QueryBuilder(instrs, reg1, regs)
    db.create_view("app.active_users", builder, "SELECT * FROM users")

    view_family = db.get_table("app.active_users")

    # 6. Persistence Audit (Close and Restart)
    log_step("Phase 6: Persistence Audit (Close and Restart)")
    db.create_table("app.tombstone_test", user_cols, 0)
    db.drop_table("app.tombstone_test")

    db.close()
    log("Engine closed. Re-opening for recovery...")

    db2 = engine.open_engine(base_dir)
    assert_true(db2.registry.has("app", "users"), "Registry lost users table")
    assert_true(
        not db2.registry.has("app", "tombstone_test"), "Dropped table resurrected!"
    )

    # 7. Recovery Audit (Program Cache)
    log_step("Phase 7: Auditing recovered VM Program")
    plan = db2.program_cache.get_program(view_family.table_id)
    assert_true(plan is not None, "Failed to recover view plan")

    # Verify recovered ScanTraceOp
    scan_op = plan.program[0]
    assert_equal_i(
        instructions.Instruction.SCAN_TRACE, scan_op.opcode, "Opcode mismatch"
    )

    # 8. VM Execution
    log_step("Phase 8: VM Execution on recovered Programmable Z-Set")
    ctx = runtime.ExecutionContext()
    ctx.reset()
    interpreter.run_vm(plan.program, plan.reg_file, ctx)

    out_reg = plan.reg_file.registers[1]
    # Check result (1 user: Alice)
    assert_equal_i(1, out_reg.batch.length(), "View produced wrong row count")
    assert_equal_u128(u128_val, out_reg.batch.get_pk(0), "View data corrupted")

    db2.close()
    log("Full Programmable Z-Set lifecycle audit PASSED.")


# -- Diagnostic Entry Point ---------------------------------------------------


def entry_point(argv):
    os.write(1, "====================================================\n")
    os.write(1, "      GnitzDB Diagnostic Integration Suite          \n")
    os.write(1, "====================================================\n")

    try:
        test_programmable_zset_lifecycle()
    except LayoutError as e:
        # RPython: avoid hasattr/getattr on exceptions.
        # str(e) is the safest way to extract the message passed to raise.
        os.write(2, "\n[FATAL] Caught LayoutError: " + str(e) + "\n")
        return 1
    except KeyError:
        os.write(2, "\n[FATAL] Caught KeyError: Registry/Cache lookup failure.\n")
        return 1
    except OSError as e:
        os.write(2, "\n[FATAL] Caught OSError: Errno " + str(e.errno) + "\n")
        return 1
    except Exception as e:
        os.write(2, "\n[FATAL] Caught Unhandled Exception: " + str(e) + "\n")
        return 1

    os.write(1, "\nSUCCESS: System integrity verified.\n")
    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    import sys as pysys

    entry_point(pysys.argv)
