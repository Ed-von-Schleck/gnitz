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
from rpython.rtyper.lltypesystem import rffi

from gnitz.core import types, batch
from gnitz.core.batch import RowBuilder
from gnitz.core.errors import LayoutError
from gnitz.catalog import system_tables as sys
from gnitz.catalog import engine, identifiers
from gnitz.catalog.registry import ingest_to_family
from gnitz.catalog.metadata import ensure_dir
from gnitz.client.circuit_builder import CircuitBuilder

# -- RPython Safe Hex Formatting ----------------------------------------------


def _u64_to_hex_padded(val):
    chars = "0123456789abcdef"
    res = ["0"] * 16
    temp = val
    for i in range(15, -1, -1):
        res[i] = chars[intmask(temp & r_uint64(0xF))]
        temp >>= 4
    return "".join(res)


# -- RPython Test Utilities ---------------------------------------------------


def log(msg):
    rposix.write(1, "[TEST] " + msg + "\n")


def log_step(name):
    rposix.write(1, "\n[CHECKPOINT] " + name + "...\n")


def fail(msg):
    rposix.write(2, "\n!!! CRITICAL TEST FAILURE !!!\n")
    rposix.write(2, msg + "\n")
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

        rposix.write(
            2,
            "   Expected: "
            + _u64_to_hex_padded(hi_e)
            + _u64_to_hex_padded(lo_e)
            + "\n",
        )
        rposix.write(
            2,
            "   Actual:   "
            + _u64_to_hex_padded(hi_a)
            + _u64_to_hex_padded(lo_a)
            + "\n",
        )
        fail(msg + " -> U128 Mismatch")


def _recursive_delete(path):
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
    if not os.path.exists(path):
        return
    log("Cleaning up: " + path)
    _recursive_delete(path)


# -- The Main Test Suite ------------------------------------------------------


def test_programmable_zset_lifecycle():
    log("Starting Comprehensive Programmable Z-Set Lifecycle Test...")

    base_dir = "zstore_test_data"
    cleanup_base_dir(base_dir)
    ensure_dir(base_dir)

    # 1. Bootstrapping
    log_step("Phase 1: Bootstrapping Engine")
    db = engine.open_engine(base_dir)
    assert_true(db.registry.has_schema("public"), "Public schema missing")

    # 2. Schema and 128-bit Table Creation
    log_step("Phase 2: Creating 128-bit Relational Schema")
    db.create_schema("app")

    user_cols = newlist_hint(2)
    user_cols.append(types.ColumnDefinition(types.TYPE_U128, name="uid"))
    user_cols.append(types.ColumnDefinition(types.TYPE_STRING, name="username"))
    users_family = db.create_table("app.users", user_cols, 0)

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

    # 3. Referential Integrity Enforcement
    log_step("Phase 3: Testing Foreign Key Enforcement")
    # Synthetic 128-bit key using shifts to avoid prebuilt long literals
    u128_val = (r_uint128(0xDEADBEEF) << 64) | r_uint128(0xCAFEBABE)

    bad_batch = batch.ZSetBatch(orders_family.schema)
    rb_bad = RowBuilder(orders_family.schema, bad_batch)
    rb_bad.begin(r_uint128(1), r_int64(1))
    rb_bad.put_u128(r_uint64(0xCAFEBABE), r_uint64(0xDEADBEEF))
    rb_bad.put_int(r_int64(500))
    rb_bad.commit()

    fk_raised = False
    try:
        ingest_to_family(orders_family, bad_batch)
    except LayoutError:
        fk_raised = True
        log("Caught expected FK violation (User not found)")
    assert_true(fk_raised, "FK violation should have raised LayoutError")
    bad_batch.free()

    # 4. Valid Data Ingestion
    log_step("Phase 4: Ingesting valid relational data")
    u_batch = batch.ZSetBatch(users_family.schema)
    rb_u = RowBuilder(users_family.schema, u_batch)
    rb_u.begin(u128_val, r_int64(1))
    rb_u.put_string("alice")
    rb_u.commit()
    ingest_to_family(users_family, u_batch)
    u_batch.free()

    assert_true(
        users_family.store.has_pk(u128_val), "User ingestion visibility failed"
    )

    o_batch = batch.ZSetBatch(orders_family.schema)
    rb_o = RowBuilder(orders_family.schema, o_batch)
    rb_o.begin(r_uint128(101), r_int64(1))
    rb_o.put_u128(r_uint64(0xCAFEBABE), r_uint64(0xDEADBEEF))
    rb_o.put_int(r_int64(1000))
    rb_o.commit()
    ingest_to_family(orders_family, o_batch)
    o_batch.free()

    # 5. Reactive View Construction (New Circuit API)
    log_step("Phase 5: Creating Reactive View (Scan Users)")

    # CircuitBuilder now requires the primary_source_id (users table)
    builder = CircuitBuilder(
        view_id=0, primary_source_id=users_family.table_id
    )
    # input_delta() represents the reactive delta stream for that source
    users_src = builder.input_delta()
    builder.sink(users_src, target_table_id=0)

    out_cols = newlist_hint(2)
    out_cols.append(("uid", types.TYPE_U128.code))
    out_cols.append(("username", types.TYPE_STRING.code))

    graph = builder.build(out_cols)

    db.create_view("app.active_users", graph, "SELECT * FROM users")
    view_family = db.get_table("app.active_users")

    # 5.1 Relational Dependency Enforcement
    log_step("Phase 5.1: Verifying Dependency Enforcement")
    drop_denied = False
    try:
        db.drop_table("app.users")
    except LayoutError:
        drop_denied = True
        log("Correctly blocked dropping table referenced by view")
    assert_true(drop_denied, "Dropping table used by view should be blocked")

    # 6. Persistence Audit
    log_step("Phase 6: Persistence Audit (Close and Restart)")
    db.close()

    db2 = engine.open_engine(base_dir)
    assert_true(
        db2.registry.has("app", "users"), "Registry lost users table"
    )
    assert_true(
        db2.registry.has("app", "active_users"), "Registry lost view"
    )

    # 7. Recovery Audit
    log_step("Phase 7: Auditing recovered VM Program")
    plan = db2.program_cache.get_program(view_family.table_id)
    assert_true(plan is not None, "Failed to recover view plan")

    # 8. View Execution (New View API)
    log_step("Phase 8: Execution of Recovered View handle")
    # Feed the actual alice record as a delta to the reactive view
    in_batch = batch.ZSetBatch(users_family.schema)
    rb_in = RowBuilder(users_family.schema, in_batch)
    rb_in.begin(u128_val, r_int64(1))
    rb_in.put_string("alice")
    rb_in.commit()

    out_batch = plan.execute_epoch(in_batch)
    in_batch.free()

    assert_true(out_batch is not None, "View should have produced Alice")
    assert_equal_i(1, out_batch.length(), "View produced wrong row count")
    assert_equal_u128(u128_val, out_batch.get_pk(0), "View data corrupted")
    out_batch.free()

    # 9. Edge Case: Graph Compilation Failures
    log_step("Phase 9: Testing Graph Compilation Failures")

    # A. Disconnected sink failure
    bad_builder = CircuitBuilder(0, users_family.table_id)
    bad_builder.input_delta()
    # No sink
    bad_graph = bad_builder.build(out_cols)

    compilation_failed = False
    try:
        db2.create_view("app.bad_view", bad_graph, "")
    except LayoutError:
        compilation_failed = True
        log("Correctly rejected view missing sink")
    assert_true(compilation_failed, "Should fail compilation: missing sink")

    # B. Cyclic Graph
    cycle_builder = CircuitBuilder(0, users_family.table_id)
    s1 = cycle_builder.input_delta()
    d1 = cycle_builder.delay(s1)
    # Manual connection to create a cycle (illegal for Kahn's)
    cycle_builder._connect(d1, s1, 1)  # PORT_IN
    cycle_graph = cycle_builder.build(out_cols)

    compilation_failed = False
    try:
        db2.create_view("app.cycle_view", cycle_graph, "")
    except LayoutError:
        compilation_failed = True
        log("Correctly rejected cyclic graph")
    assert_true(
        compilation_failed, "Should fail compilation: cycle detected"
    )

    # 10. Teardown
    log_step("Phase 10: Full Teardown and Cleanup")
    db2.drop_view("app.active_users")
    db2.drop_table("app.orders")
    db2.drop_table("app.users")
    db2.drop_schema("app")

    assert_true(not db2.registry.has_schema("app"), "Schema cleanup failed")
    db2.close()
    log("Full Programmable Z-Set lifecycle audit PASSED.")


def entry_point(argv):
    rposix.write(1, "====================================================\n")
    rposix.write(1, "      GnitzDB Diagnostic Integration Suite          \n")
    rposix.write(1, "====================================================\n")

    try:
        test_programmable_zset_lifecycle()
    except LayoutError as e:
        rposix.write(2, "\n[FATAL] Caught LayoutError: " + str(e) + "\n")
        return 1
    except KeyError as e:
        rposix.write(2, "\n[FATAL] Caught KeyError: " + str(e) + "\n")
        return 1
    except OSError as e:
        rposix.write(
            2, "\n[FATAL] Caught OSError: Errno " + str(e.errno) + "\n"
        )
        return 1
    except Exception as e:
        rposix.write(
            2, "\n[FATAL] Caught Unhandled Exception: " + str(e) + "\n"
        )
        return 1

    rposix.write(1, "\nSUCCESS: System integrity verified.\n")
    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    import pysys

    entry_point(pysys.argv)
