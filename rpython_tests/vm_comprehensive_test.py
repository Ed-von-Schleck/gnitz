# vm_comprehensive_test.py

import sys
import os

from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rtyper.lltypesystem import rffi

from gnitz.core import types, values, batch
from gnitz.dbsp import functions
from gnitz.vm import runtime, instructions, interpreter
from gnitz.storage.ephemeral_table import EphemeralTable

# ------------------------------------------------------------------------------
# RPython Debugging Helpers
# ------------------------------------------------------------------------------

def log(msg):
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
# Plan Builder Helper
# ------------------------------------------------------------------------------

def make_plan(program, reg_file, schema, in_reg=0, out_reg=1):
    return runtime.ExecutablePlan(program, reg_file, schema, in_reg, out_reg)

# ------------------------------------------------------------------------------
# Test 1: Filter -> Map -> Negate linear pipeline
# ------------------------------------------------------------------------------

def test_filter_map_negate():
    log("[VM] Test 1: Filter -> Map -> Negate linear pipeline...")

    # Schema: (pk:U64, val:I64, label:STRING)
    in_cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
        types.ColumnDefinition(types.TYPE_STRING, name="label"),
    ]
    in_schema = types.TableSchema(in_cols, 0)
    in_vm = runtime.VMSchema(in_schema)

    # Map output schema: (pk:U64, val:I64, label:STRING) — project [val, label]
    # The map projects col 1 (val) and col 2 (label) from the input
    # Output is same structure: pk + val + label
    out_map_cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
        types.ColumnDefinition(types.TYPE_STRING, name="label"),
    ]
    out_map_schema = types.TableSchema(out_map_cols, 0)
    out_map_vm = runtime.VMSchema(out_map_schema)

    # Registers: R0=input, R1=filter out, R2=map out, R3=negate out
    reg_file = runtime.RegisterFile(4)
    reg_file.registers[0] = runtime.DeltaRegister(0, in_vm)
    reg_file.registers[1] = runtime.DeltaRegister(1, in_vm)
    reg_file.registers[2] = runtime.DeltaRegister(2, out_map_vm)
    reg_file.registers[3] = runtime.DeltaRegister(3, out_map_vm)

    # FilterOp: val > 10 (col_idx=1, OP_GT, val_bits=10)
    filter_func = functions.UniversalPredicate(1, functions.OP_GT, r_uint64(10))
    # MapOp: project [val, label] (indices [1, 2])
    map_func = functions.UniversalProjection(
        [1, 2],
        [types.TYPE_I64.code, types.TYPE_STRING.code],
    )

    program = [
        instructions.FilterOp(reg_file.registers[0], reg_file.registers[1], filter_func),
        instructions.MapOp(reg_file.registers[1], reg_file.registers[2], map_func),
        instructions.NegateOp(reg_file.registers[2], reg_file.registers[3]),
        instructions.HaltOp(),
    ]

    plan = make_plan(program, reg_file, out_map_schema, in_reg=0, out_reg=3)

    # Input: 3 rows. pk=1 val=5 (below threshold), pk=2 val=20, pk=3 val=42
    in_batch = batch.ZSetBatch(in_schema)
    r1 = values.make_payload_row(in_schema)
    r1.append_int(r_int64(5))
    r1.append_string("low")
    in_batch.append(r_uint128(1), r_int64(1), r1)

    r2 = values.make_payload_row(in_schema)
    r2.append_int(r_int64(20))
    r2.append_string("mid")
    in_batch.append(r_uint128(2), r_int64(1), r2)

    r3 = values.make_payload_row(in_schema)
    r3.append_int(r_int64(42))
    r3.append_string("high")
    in_batch.append(r_uint128(3), r_int64(1), r3)

    result = plan.execute_epoch(in_batch)

    assert_true(result is not None, "Expected non-None result")
    assert_equal_i(2, result.length(), "Filter should drop 1 of 3 rows")

    # Both surviving rows should have negated weights (-1)
    for i in range(result.length()):
        assert_equal_i64(r_int64(-1), result.get_weight(i),
                         "Negate should flip weight to -1")

    # Check that pk=1 (val=5) was filtered out
    pks = []
    for i in range(result.length()):
        pks.append(result.get_pk(i))
    assert_true(r_uint128(1) not in pks, "pk=1 should be filtered (val=5 <= 10)")

    result.free()
    in_batch.free()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 2: Union — algebraic addition
# ------------------------------------------------------------------------------

def test_union():
    log("[VM] Test 2: Union — algebraic addition...")

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    schema = types.TableSchema(cols, 0)
    vm_schema = runtime.VMSchema(schema)

    # R0=input A, R1=input B, R2=output
    reg_file = runtime.RegisterFile(3)
    reg_file.registers[0] = runtime.DeltaRegister(0, vm_schema)
    reg_file.registers[1] = runtime.DeltaRegister(1, vm_schema)
    reg_file.registers[2] = runtime.DeltaRegister(2, vm_schema)

    program = [
        instructions.UnionOp(reg_file.registers[0], reg_file.registers[1], reg_file.registers[2]),
        instructions.HaltOp(),
    ]

    plan = make_plan(program, reg_file, schema, in_reg=0, out_reg=2)

    # Batch A: pk=1 val=10
    batch_a = batch.ZSetBatch(schema)
    ra = values.make_payload_row(schema)
    ra.append_int(r_int64(10))
    batch_a.append(r_uint128(1), r_int64(1), ra)

    # Pre-load R1 with batch B data before execute_epoch
    # We need to manually put data in R1 since execute_epoch only binds R0
    batch_b_data = batch.ZSetBatch(schema)
    rb = values.make_payload_row(schema)
    rb.append_int(r_int64(20))
    batch_b_data.append(r_uint128(2), r_int64(1), rb)

    # Manually load R1 with batch B before execution
    # We'll use bind to set R1's batch
    reg_file.registers[1].bind(batch_b_data)

    result = plan.execute_epoch(batch_a)

    assert_true(result is not None, "Union should produce output")
    assert_equal_i(2, result.length(), "Union should have 2 rows (A + B)")

    found_pk1 = False
    found_pk2 = False
    for i in range(result.length()):
        pk = result.get_pk(i)
        if pk == r_uint128(1):
            found_pk1 = True
        if pk == r_uint128(2):
            found_pk2 = True
    assert_true(found_pk1, "Union output missing pk=1 from batch A")
    assert_true(found_pk2, "Union output missing pk=2 from batch B")

    reg_file.registers[1].unbind()
    result.free()
    batch_a.free()
    batch_b_data.free()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 3: Join Delta-Trace
# ------------------------------------------------------------------------------

def test_join_delta_trace(base_dir):
    log("[VM] Test 3: Join Delta-Trace (index nested loop)...")

    # Left schema: (pk:U64, dept:I64)
    cols_l = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="dept"),
    ]
    schema_l = types.TableSchema(cols_l, 0)
    vm_l = runtime.VMSchema(schema_l)

    # Right schema: (pk:U64, budget:I64)
    cols_r = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="budget"),
    ]
    schema_r = types.TableSchema(cols_r, 0)
    vm_r = runtime.VMSchema(schema_r)

    # Output schema: merge_schemas_for_join => (pk:U64, dept:I64, budget:I64)
    out_schema = types.merge_schemas_for_join(schema_l, schema_r)
    out_vm = runtime.VMSchema(out_schema)

    # Pre-populate right-side EphemeralTable
    trace_dir = os.path.join(base_dir, "trace_join")
    trace_table = EphemeralTable(trace_dir, "trace_r", schema_r)

    batch_r = batch.ZSetBatch(schema_r)
    rr = values.make_payload_row(schema_r)
    rr.append_int(r_int64(50000))
    batch_r.append(r_uint128(99), r_int64(2), rr)

    rr2 = values.make_payload_row(schema_r)
    rr2.append_int(r_int64(30000))
    batch_r.append(r_uint128(100), r_int64(1), rr2)

    trace_table.ingest_batch(batch_r)
    batch_r.free()

    # R0=delta (left), R1=trace (right cursor), R2=output
    trace_cursor = trace_table.create_cursor()
    reg_file = runtime.RegisterFile(3)
    reg_file.registers[0] = runtime.DeltaRegister(0, vm_l)
    reg_file.registers[1] = runtime.TraceRegister(1, vm_r, trace_cursor, trace_table)
    reg_file.registers[2] = runtime.DeltaRegister(2, out_vm)

    program = [
        instructions.JoinDeltaTraceOp(
            reg_file.registers[0], reg_file.registers[1], reg_file.registers[2]
        ),
        instructions.HaltOp(),
    ]

    plan = make_plan(program, reg_file, out_schema, in_reg=0, out_reg=2)

    # Delta: pk=99 dept=7 w=3 (matches right pk=99), pk=101 dept=9 w=1 (no match)
    in_batch = batch.ZSetBatch(schema_l)
    rl = values.make_payload_row(schema_l)
    rl.append_int(r_int64(7))
    in_batch.append(r_uint128(99), r_int64(3), rl)

    rl2 = values.make_payload_row(schema_l)
    rl2.append_int(r_int64(9))
    in_batch.append(r_uint128(101), r_int64(1), rl2)

    result = plan.execute_epoch(in_batch)

    assert_true(result is not None, "Join should produce output")
    assert_equal_i(1, result.length(), "Only pk=99 matches the trace")
    assert_equal_u128(r_uint128(99), result.get_pk(0), "Join PK mismatch")
    # Weight = delta_w * trace_w = 3 * 2 = 6
    assert_equal_i64(r_int64(6), result.get_weight(0), "Join weight = 3*2 = 6")

    # Check merged payload: dept=7, budget=50000
    out_row = result.get_row(0)
    assert_equal_i64(r_int64(7), out_row.get_int_signed(0), "Left dept mismatch")
    assert_equal_i64(r_int64(50000), out_row.get_int_signed(1), "Right budget mismatch")

    result.free()
    in_batch.free()
    trace_table.close()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 4: Distinct multi-tick (stateful)
# ------------------------------------------------------------------------------

def test_distinct_multi_tick(base_dir):
    log("[VM] Test 4: Distinct across multiple ticks...")

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    schema = types.TableSchema(cols, 0)
    vm_schema = runtime.VMSchema(schema)

    # History table for distinct state
    hist_dir = os.path.join(base_dir, "distinct_hist")
    history_table = EphemeralTable(hist_dir, "hist", schema)

    # R0=input, R1=history (TraceRegister), R2=output
    # DistinctOp handles history internally via the table, but we need
    # a TraceRegister so the VM can access it
    hist_cursor = history_table.create_cursor()
    reg_file = runtime.RegisterFile(3)
    reg_file.registers[0] = runtime.DeltaRegister(0, vm_schema)
    reg_file.registers[1] = runtime.TraceRegister(1, vm_schema, hist_cursor, history_table)
    reg_file.registers[2] = runtime.DeltaRegister(2, vm_schema)

    # DistinctOp(reg_in=R0, reg_history=R1, reg_out=R2) uses history_table from R1
    # Then IntegrateOp merges the delta into history (already done inside op_distinct)
    program = [
        instructions.DistinctOp(
            reg_file.registers[0], reg_file.registers[1], reg_file.registers[2]
        ),
        instructions.HaltOp(),
    ]

    plan = make_plan(program, reg_file, schema, in_reg=0, out_reg=2)

    # Tick 1: insert pk=1 w=3 => distinct output should be w=+1 (appeared)
    in_batch1 = batch.ZSetBatch(schema)
    row1 = values.make_payload_row(schema)
    row1.append_int(r_int64(42))
    in_batch1.append(r_uint128(1), r_int64(3), row1)

    result1 = plan.execute_epoch(in_batch1)
    assert_true(result1 is not None, "Tick 1: distinct should emit output")
    assert_equal_i(1, result1.length(), "Tick 1: one row appeared")
    assert_equal_i64(r_int64(1), result1.get_weight(0), "Tick 1: weight should be +1 (appeared)")

    result1.free()
    in_batch1.free()

    # Tick 2: retract pk=1 w=-3 => net becomes 0 => distinct output w=-1 (disappeared)
    in_batch2 = batch.ZSetBatch(schema)
    row2 = values.make_payload_row(schema)
    row2.append_int(r_int64(42))
    in_batch2.append(r_uint128(1), r_int64(-3), row2)

    result2 = plan.execute_epoch(in_batch2)
    assert_true(result2 is not None, "Tick 2: distinct should emit output")
    assert_equal_i(1, result2.length(), "Tick 2: one row disappeared")
    assert_equal_i64(r_int64(-1), result2.get_weight(0), "Tick 2: weight should be -1 (disappeared)")

    result2.free()
    in_batch2.free()
    history_table.close()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 5: Reduce SUM — GROUP BY with linear aggregation
# ------------------------------------------------------------------------------

def test_reduce_sum(base_dir):
    log("[VM] Test 5: Reduce SUM (GROUP BY with linear aggregation)...")

    # Input schema: (pk:U64, group:U64, amount:I64)
    # Using U64 for group so it can serve as the output PK
    in_cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_U64, name="grp"),
        types.ColumnDefinition(types.TYPE_I64, name="amount"),
    ]
    in_schema = types.TableSchema(in_cols, 0)
    in_vm = runtime.VMSchema(in_schema)

    # Output schema: built by _build_reduce_output_schema
    # GROUP BY col 1 (grp:U64) => natural PK
    # Output: (grp:U64, agg:I64)
    agg_func = functions.UniversalAccumulator(2, functions.AGG_SUM, types.TYPE_I64)
    group_by_cols = [1]
    out_schema = types._build_reduce_output_schema(in_schema, group_by_cols, agg_func)
    out_vm = runtime.VMSchema(out_schema)

    # Trace tables for reduce state
    trace_in_dir = os.path.join(base_dir, "reduce_trace_in")
    trace_out_dir = os.path.join(base_dir, "reduce_trace_out")
    trace_in_table = EphemeralTable(trace_in_dir, "trace_in", in_schema)
    trace_out_table = EphemeralTable(trace_out_dir, "trace_out", out_schema)

    # R0=input, R1=trace_in, R2=trace_out, R3=output
    trace_in_cursor = trace_in_table.create_cursor()
    trace_out_cursor = trace_out_table.create_cursor()
    reg_file = runtime.RegisterFile(4)
    reg_file.registers[0] = runtime.DeltaRegister(0, in_vm)
    reg_file.registers[1] = runtime.TraceRegister(1, in_vm, trace_in_cursor, trace_in_table)
    reg_file.registers[2] = runtime.TraceRegister(2, out_vm, trace_out_cursor, trace_out_table)
    reg_file.registers[3] = runtime.DeltaRegister(3, out_vm)

    program = [
        instructions.ReduceOp(
            reg_file.registers[0],       # reg_in
            reg_file.registers[1],       # reg_trace_in
            reg_file.registers[2],       # reg_trace_out
            reg_file.registers[3],       # reg_out
            group_by_cols,
            agg_func,
            out_schema,
        ),
        # Integrate delta into trace_in (for history) and trace_out (for agg state)
        instructions.IntegrateOp(reg_file.registers[0], trace_in_table),
        instructions.IntegrateOp(reg_file.registers[3], trace_out_table),
        instructions.HaltOp(),
    ]

    plan = make_plan(program, reg_file, out_schema, in_reg=0, out_reg=3)

    # Tick 1: group=10 amount=100, group=10 amount=50, group=20 amount=200
    in_batch1 = batch.ZSetBatch(in_schema)

    row_a = values.make_payload_row(in_schema)
    row_a.append_int(rffi.cast(rffi.LONGLONG, r_uint64(10)))  # grp=10
    row_a.append_int(r_int64(100))
    in_batch1.append(r_uint128(1), r_int64(1), row_a)

    row_b = values.make_payload_row(in_schema)
    row_b.append_int(rffi.cast(rffi.LONGLONG, r_uint64(10)))  # grp=10
    row_b.append_int(r_int64(50))
    in_batch1.append(r_uint128(2), r_int64(1), row_b)

    row_c = values.make_payload_row(in_schema)
    row_c.append_int(rffi.cast(rffi.LONGLONG, r_uint64(20)))  # grp=20
    row_c.append_int(r_int64(200))
    in_batch1.append(r_uint128(3), r_int64(1), row_c)

    result1 = plan.execute_epoch(in_batch1)

    assert_true(result1 is not None, "Tick 1: reduce should produce output")
    assert_equal_i(2, result1.length(), "Tick 1: two groups")

    # Find group 10 (sum=150) and group 20 (sum=200)
    for i in range(result1.length()):
        pk = result1.get_pk(i)
        row = result1.get_row(i)
        w = result1.get_weight(i)
        if pk == r_uint128(10):
            assert_equal_i64(r_int64(150), row.get_int_signed(0), "Group 10 sum should be 150")
            assert_equal_i64(r_int64(1), w, "Group 10 weight should be +1")
        elif pk == r_uint128(20):
            assert_equal_i64(r_int64(200), row.get_int_signed(0), "Group 20 sum should be 200")
            assert_equal_i64(r_int64(1), w, "Group 20 weight should be +1")
        else:
            fail("Unexpected group PK")

    result1.free()
    in_batch1.free()

    # Tick 2: retract one row from group 10 (pk=2, amount=50, w=-1)
    in_batch2 = batch.ZSetBatch(in_schema)
    row_d = values.make_payload_row(in_schema)
    row_d.append_int(rffi.cast(rffi.LONGLONG, r_uint64(10)))
    row_d.append_int(r_int64(50))
    in_batch2.append(r_uint128(2), r_int64(-1), row_d)

    result2 = plan.execute_epoch(in_batch2)

    assert_true(result2 is not None, "Tick 2: reduce should produce output")
    # Should emit retraction of old sum (150, w=-1) and insertion of new sum (100, w=+1)
    assert_equal_i(2, result2.length(), "Tick 2: retraction + insertion")

    found_retract = False
    found_insert = False
    for i in range(result2.length()):
        pk = result2.get_pk(i)
        row = result2.get_row(i)
        w = result2.get_weight(i)
        if pk == r_uint128(10):
            if w == r_int64(-1):
                assert_equal_i64(r_int64(150), row.get_int_signed(0),
                                 "Retraction should have old sum 150")
                found_retract = True
            elif w == r_int64(1):
                assert_equal_i64(r_int64(100), row.get_int_signed(0),
                                 "Insertion should have new sum 100")
                found_insert = True

    assert_true(found_retract, "Missing retraction of old aggregate")
    assert_true(found_insert, "Missing insertion of new aggregate")

    result2.free()
    in_batch2.free()
    trace_in_table.close()
    trace_out_table.close()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 6: Ghost Property — zero-weight annihilation
# ------------------------------------------------------------------------------

def test_ghost_property():
    log("[VM] Test 6: Ghost Property (zero-weight annihilation)...")

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    schema = types.TableSchema(cols, 0)
    vm_schema = runtime.VMSchema(schema)

    # Simple passthrough: FilterOp with no predicate (pass all) then Halt
    # Use a null predicate that always returns True
    pass_func = functions.NullPredicate()

    reg_file = runtime.RegisterFile(2)
    reg_file.registers[0] = runtime.DeltaRegister(0, vm_schema)
    reg_file.registers[1] = runtime.DeltaRegister(1, vm_schema)

    program = [
        instructions.FilterOp(reg_file.registers[0], reg_file.registers[1], pass_func),
        instructions.HaltOp(),
    ]

    plan = make_plan(program, reg_file, schema, in_reg=0, out_reg=1)

    # Insert pk=1 w=+1 and pk=1 w=-1 in same batch
    in_batch = batch.ZSetBatch(schema)
    row1 = values.make_payload_row(schema)
    row1.append_int(r_int64(123))
    in_batch.append(r_uint128(1), r_int64(1), row1)

    row2 = values.make_payload_row(schema)
    row2.append_int(r_int64(123))
    in_batch.append(r_uint128(1), r_int64(-1), row2)

    # Consolidate the input batch (this is where ghost annihilation happens)
    consolidated = in_batch.to_consolidated()

    result = plan.execute_epoch(consolidated)

    # After consolidation, pk=1 has net weight 0, so it's pruned
    assert_true(result is None, "Ghost property: consolidated zero-weight should produce None")

    if consolidated is not in_batch:
        consolidated.free()
    in_batch.free()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 7: Empty input — edge case
# ------------------------------------------------------------------------------

def test_empty_input():
    log("[VM] Test 7: Empty input edge case...")

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    schema = types.TableSchema(cols, 0)
    vm_schema = runtime.VMSchema(schema)

    reg_file = runtime.RegisterFile(2)
    reg_file.registers[0] = runtime.DeltaRegister(0, vm_schema)
    reg_file.registers[1] = runtime.DeltaRegister(1, vm_schema)

    program = [
        instructions.FilterOp(
            reg_file.registers[0], reg_file.registers[1],
            functions.NullPredicate(),
        ),
        instructions.HaltOp(),
    ]

    plan = make_plan(program, reg_file, schema, in_reg=0, out_reg=1)

    empty_batch = batch.ZSetBatch(schema)
    result = plan.execute_epoch(empty_batch)

    assert_true(result is None, "Empty input should produce None result")

    empty_batch.free()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 8: Chunked scan with yield/resume loop
# ------------------------------------------------------------------------------

def test_chunked_scan_resume(base_dir):
    log("[VM] Test 8: Chunked scan with yield/resume (ScanTrace + Yield + Jump)...")

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    schema = types.TableSchema(cols, 0)
    vm_schema = runtime.VMSchema(schema)

    table_path = os.path.join(base_dir, "chunk_table")
    table = EphemeralTable(table_path, "chunks", schema)

    # Fill table with 25 rows
    b = batch.ZSetBatch(schema)
    for i in range(25):
        row = values.make_payload_row(schema)
        row.append_int(r_int64(i * 10))
        b.append(r_uint128(i), r_int64(1), row)
    table.ingest_batch(b)
    b.free()

    # R0=trace (cursor into table), R1=output
    trace_cursor = table.create_cursor()
    reg_file = runtime.RegisterFile(2)
    reg_file.registers[0] = runtime.TraceRegister(0, vm_schema, trace_cursor, table)
    reg_file.registers[1] = runtime.DeltaRegister(1, vm_schema)

    # Program: ScanTrace(R0, R1, chunk=10) -> Yield(ROW_LIMIT) -> ClearDeltas -> Jump(0)
    program = [
        instructions.ScanTraceOp(reg_file.registers[0], reg_file.registers[1], 10),
        instructions.YieldOp(runtime.YIELD_REASON_ROW_LIMIT),
        instructions.ClearDeltasOp(),
        instructions.JumpOp(0),
    ]

    context = runtime.ExecutionContext()

    # Chunk 1: expect 10 rows, then yield
    context.reset()
    interpreter.run_vm(program, reg_file, context)
    assert_equal_i(runtime.STATUS_YIELDED, context.status, "Chunk 1: should yield")
    assert_equal_i(10, reg_file.registers[1].batch.length(), "Chunk 1: should have 10 rows")
    assert_equal_i(2, context.pc, "Chunk 1: PC should be at ClearDeltas (idx 2)")

    # Chunk 2: resume from PC=2 (ClearDeltas), expect 10 more rows
    interpreter.run_vm(program, reg_file, context)
    assert_equal_i(runtime.STATUS_YIELDED, context.status, "Chunk 2: should yield")
    assert_equal_i(10, reg_file.registers[1].batch.length(), "Chunk 2: should have 10 rows")

    # Chunk 3: resume, expect remaining 5 rows
    interpreter.run_vm(program, reg_file, context)
    assert_equal_i(runtime.STATUS_YIELDED, context.status, "Chunk 3: should yield")
    assert_equal_i(5, reg_file.registers[1].batch.length(), "Chunk 3: should have 5 rows")

    table.close()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 9: Seek trace point lookup
# ------------------------------------------------------------------------------

def test_seek_trace_point_lookup(base_dir):
    log("[VM] Test 9: SeekTrace + ScanTrace point lookup...")

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    schema = types.TableSchema(cols, 0)
    vm_schema = runtime.VMSchema(schema)

    table_path = os.path.join(base_dir, "seek_table")
    table = EphemeralTable(table_path, "seek", schema)

    # Populate with PKs [10, 20, 30, 40, 50]
    b = batch.ZSetBatch(schema)
    ids = [10, 20, 30, 40, 50]
    for pk_val in ids:
        row = values.make_payload_row(schema)
        row.append_int(r_int64(pk_val * 100))
        b.append(r_uint128(pk_val), r_int64(1), row)
    table.ingest_batch(b)
    b.free()

    # R0=trace cursor, R1=key (delta), R2=output
    trace_cursor = table.create_cursor()
    reg_file = runtime.RegisterFile(3)
    reg_file.registers[0] = runtime.TraceRegister(0, vm_schema, trace_cursor, table)
    reg_file.registers[1] = runtime.DeltaRegister(1, vm_schema)
    reg_file.registers[2] = runtime.DeltaRegister(2, vm_schema)

    # Program: SeekTrace(R0, R1) -> ScanTrace(R0, R2, chunk=1) -> Halt
    program = [
        instructions.SeekTraceOp(reg_file.registers[0], reg_file.registers[1]),
        instructions.ScanTraceOp(reg_file.registers[0], reg_file.registers[2], 1),
        instructions.HaltOp(),
    ]

    plan = make_plan(program, reg_file, schema, in_reg=1, out_reg=2)

    # Bind R1 with a batch containing pk=30
    seek_batch = batch.ZSetBatch(schema)
    r = values.make_payload_row(schema)
    r.append_int(r_int64(0))
    seek_batch.append(r_uint128(30), r_int64(1), r)

    result = plan.execute_epoch(seek_batch)

    assert_true(result is not None, "Seek should find a result")
    assert_equal_i(1, result.length(), "Should find exactly 1 row")
    assert_equal_u128(r_uint128(30), result.get_pk(0), "Should find pk=30")
    assert_equal_i64(r_int64(3000), result.get_row(0).get_int_signed(0),
                     "Payload val should be 3000 for pk=30")

    result.free()
    seek_batch.free()
    table.close()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 10: Empty table scan
# ------------------------------------------------------------------------------

def test_empty_table_scan(base_dir):
    log("[VM] Test 10: Empty table scan...")

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    schema = types.TableSchema(cols, 0)
    vm_schema = runtime.VMSchema(schema)

    table_path = os.path.join(base_dir, "empty_table")
    table = EphemeralTable(table_path, "empty", schema)

    # R0=trace cursor (empty), R1=output
    trace_cursor = table.create_cursor()
    reg_file = runtime.RegisterFile(2)
    reg_file.registers[0] = runtime.TraceRegister(0, vm_schema, trace_cursor, table)
    reg_file.registers[1] = runtime.DeltaRegister(1, vm_schema)

    # Program: ScanTrace(R0, R1, chunk=10) -> Halt
    program = [
        instructions.ScanTraceOp(reg_file.registers[0], reg_file.registers[1], 10),
        instructions.HaltOp(),
    ]

    context = runtime.ExecutionContext()
    context.reset()
    interpreter.run_vm(program, reg_file, context)

    assert_equal_i(runtime.STATUS_HALTED, context.status, "Empty scan should halt")
    assert_equal_i(0, reg_file.registers[1].batch.length(), "Output should be empty")

    table.close()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 11: Join delta-delta (sort-merge)
# ------------------------------------------------------------------------------

def test_join_delta_delta():
    log("[VM] Test 11: JoinDeltaDelta (sort-merge join)...")

    # Schema A: (pk:U64, val_a:I64)
    cols_a = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val_a"),
    ]
    schema_a = types.TableSchema(cols_a, 0)
    vm_a = runtime.VMSchema(schema_a)

    # Schema B: (pk:U64, val_b:I64)
    cols_b = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val_b"),
    ]
    schema_b = types.TableSchema(cols_b, 0)
    vm_b = runtime.VMSchema(schema_b)

    out_schema = types.merge_schemas_for_join(schema_a, schema_b)
    out_vm = runtime.VMSchema(out_schema)

    # R0=delta A, R1=delta B, R2=output
    reg_file = runtime.RegisterFile(3)
    reg_file.registers[0] = runtime.DeltaRegister(0, vm_a)
    reg_file.registers[1] = runtime.DeltaRegister(1, vm_b)
    reg_file.registers[2] = runtime.DeltaRegister(2, out_vm)

    program = [
        instructions.JoinDeltaDeltaOp(
            reg_file.registers[0], reg_file.registers[1], reg_file.registers[2]
        ),
        instructions.HaltOp(),
    ]

    plan = make_plan(program, reg_file, out_schema, in_reg=0, out_reg=2)

    # Batch A: pk=5 w=1, pk=10 w=2
    batch_a = batch.ZSetBatch(schema_a)
    ra1 = values.make_payload_row(schema_a)
    ra1.append_int(r_int64(55))
    batch_a.append(r_uint128(5), r_int64(1), ra1)
    ra2 = values.make_payload_row(schema_a)
    ra2.append_int(r_int64(100))
    batch_a.append(r_uint128(10), r_int64(2), ra2)

    # Batch B: pk=10 w=3, pk=20 w=1
    batch_b = batch.ZSetBatch(schema_b)
    rb1 = values.make_payload_row(schema_b)
    rb1.append_int(r_int64(200))
    batch_b.append(r_uint128(10), r_int64(3), rb1)
    rb2 = values.make_payload_row(schema_b)
    rb2.append_int(r_int64(300))
    batch_b.append(r_uint128(20), r_int64(1), rb2)

    # Pre-load R1 with batch B
    reg_file.registers[1].bind(batch_b)

    result = plan.execute_epoch(batch_a)

    assert_true(result is not None, "Delta-delta join should produce output")
    assert_equal_i(1, result.length(), "Only pk=10 matches both sides")
    assert_equal_u128(r_uint128(10), result.get_pk(0), "Joined PK should be 10")
    assert_equal_i64(r_int64(6), result.get_weight(0), "Weight should be 2*3=6")

    # Check merged payload: val_a=100, val_b=200
    out_row = result.get_row(0)
    assert_equal_i64(r_int64(100), out_row.get_int_signed(0), "val_a mismatch")
    assert_equal_i64(r_int64(200), out_row.get_int_signed(1), "val_b mismatch")

    reg_file.registers[1].unbind()
    result.free()
    batch_a.free()
    batch_b.free()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 12: Delay op (z^{-1} forwarding)
# ------------------------------------------------------------------------------

def test_delay_op():
    log("[VM] Test 12: DelayOp (z^-1 forwarding)...")

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    schema = types.TableSchema(cols, 0)
    vm_schema = runtime.VMSchema(schema)

    # R0=input, R1=delay output
    reg_file = runtime.RegisterFile(2)
    reg_file.registers[0] = runtime.DeltaRegister(0, vm_schema)
    reg_file.registers[1] = runtime.DeltaRegister(1, vm_schema)

    program = [
        instructions.DelayOp(reg_file.registers[0], reg_file.registers[1]),
        instructions.HaltOp(),
    ]

    plan = make_plan(program, reg_file, schema, in_reg=0, out_reg=1)

    # Input: 2 rows
    in_batch = batch.ZSetBatch(schema)
    r1 = values.make_payload_row(schema)
    r1.append_int(r_int64(111))
    in_batch.append(r_uint128(1), r_int64(1), r1)

    r2 = values.make_payload_row(schema)
    r2.append_int(r_int64(222))
    in_batch.append(r_uint128(2), r_int64(3), r2)

    result = plan.execute_epoch(in_batch)

    assert_true(result is not None, "Delay should produce output")
    assert_equal_i(2, result.length(), "Delay should forward all rows")

    # Verify rows are forwarded with same PKs, weights, and payloads
    found_pk1 = False
    found_pk2 = False
    for i in range(result.length()):
        pk = result.get_pk(i)
        if pk == r_uint128(1):
            assert_equal_i64(r_int64(1), result.get_weight(i), "pk=1 weight mismatch")
            assert_equal_i64(r_int64(111), result.get_row(i).get_int_signed(0),
                             "pk=1 payload should be 111")
            found_pk1 = True
        elif pk == r_uint128(2):
            assert_equal_i64(r_int64(3), result.get_weight(i), "pk=2 weight mismatch")
            assert_equal_i64(r_int64(222), result.get_row(i).get_int_signed(0),
                             "pk=2 payload should be 222")
            found_pk2 = True
    assert_true(found_pk1, "Missing pk=1 in delay output")
    assert_true(found_pk2, "Missing pk=2 in delay output")

    result.free()
    in_batch.free()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 13: Reduce MIN — non-linear aggregate with history replay
# ------------------------------------------------------------------------------

def test_reduce_min_nonlinear(base_dir):
    log("[VM] Test 13: Reduce MIN (non-linear aggregate with history replay)...")

    # Input schema: (pk:U64, val:I64) — GROUP BY pk forces group_by_pk=True
    # so trace_in lookups match the PK index correctly.
    in_cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    in_schema = types.TableSchema(in_cols, 0)
    in_vm = runtime.VMSchema(in_schema)

    # AGG_MIN on col 1 (val) is non-linear, so is_linear() returns False
    agg_func = functions.UniversalAccumulator(1, functions.AGG_MIN, types.TYPE_I64)
    group_by_cols = [0]  # GROUP BY pk
    out_schema = types._build_reduce_output_schema(in_schema, group_by_cols, agg_func)
    out_vm = runtime.VMSchema(out_schema)

    trace_in_dir = os.path.join(base_dir, "min_trace_in")
    trace_out_dir = os.path.join(base_dir, "min_trace_out")
    trace_in_table = EphemeralTable(trace_in_dir, "trace_in", in_schema)
    trace_out_table = EphemeralTable(trace_out_dir, "trace_out", out_schema)

    trace_in_cursor = trace_in_table.create_cursor()
    trace_out_cursor = trace_out_table.create_cursor()
    reg_file = runtime.RegisterFile(4)
    reg_file.registers[0] = runtime.DeltaRegister(0, in_vm)
    reg_file.registers[1] = runtime.TraceRegister(1, in_vm, trace_in_cursor, trace_in_table)
    reg_file.registers[2] = runtime.TraceRegister(2, out_vm, trace_out_cursor, trace_out_table)
    reg_file.registers[3] = runtime.DeltaRegister(3, out_vm)

    program = [
        instructions.ReduceOp(
            reg_file.registers[0],
            reg_file.registers[1],
            reg_file.registers[2],
            reg_file.registers[3],
            group_by_cols,
            agg_func,
            out_schema,
        ),
        instructions.IntegrateOp(reg_file.registers[0], trace_in_table),
        instructions.IntegrateOp(reg_file.registers[3], trace_out_table),
        instructions.HaltOp(),
    ]

    plan = make_plan(program, reg_file, out_schema, in_reg=0, out_reg=3)

    # Tick 1: pk=1 val=30 w=1 => MIN(val)=30
    in_batch1 = batch.ZSetBatch(in_schema)
    row_a = values.make_payload_row(in_schema)
    row_a.append_int(r_int64(30))
    in_batch1.append(r_uint128(1), r_int64(1), row_a)

    result1 = plan.execute_epoch(in_batch1)

    assert_true(result1 is not None, "Tick 1: reduce MIN should produce output")
    assert_equal_i(1, result1.length(), "Tick 1: one group")
    assert_equal_i64(r_int64(30), result1.get_row(0).get_int_signed(0),
                     "Tick 1: MIN should be 30")
    assert_equal_i64(r_int64(1), result1.get_weight(0), "Tick 1: weight should be +1")

    result1.free()
    in_batch1.free()

    # Tick 2: pk=1 val=10 w=1 => replay history(val=30) + delta(val=10) => MIN=10
    # This forces the non-linear replay path since AGG_MIN.is_linear() is False.
    in_batch2 = batch.ZSetBatch(in_schema)
    row_b = values.make_payload_row(in_schema)
    row_b.append_int(r_int64(10))
    in_batch2.append(r_uint128(1), r_int64(1), row_b)

    result2 = plan.execute_epoch(in_batch2)

    assert_true(result2 is not None, "Tick 2: reduce MIN should produce output")
    # Should emit retraction of old MIN=30 and insertion of new MIN=10
    assert_equal_i(2, result2.length(), "Tick 2: retraction + insertion")

    found_retract = False
    found_insert = False
    for i in range(result2.length()):
        row = result2.get_row(i)
        w = result2.get_weight(i)
        if w == r_int64(-1):
            assert_equal_i64(r_int64(30), row.get_int_signed(0),
                             "Retraction should have old MIN=30")
            found_retract = True
        elif w == r_int64(1):
            assert_equal_i64(r_int64(10), row.get_int_signed(0),
                             "Insertion should have new MIN=10")
            found_insert = True

    assert_true(found_retract, "Missing retraction of old MIN")
    assert_true(found_insert, "Missing insertion of new MIN")

    result2.free()
    in_batch2.free()
    trace_in_table.close()
    trace_out_table.close()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Test 14: Join delta-trace multi-match + zero-weight skip
# ------------------------------------------------------------------------------

def test_join_delta_trace_multi_match(base_dir):
    log("[VM] Test 14: JoinDeltaTrace multi-match + zero-weight skip...")

    # Left (delta): (pk:U64, tag:I64)
    cols_l = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="tag"),
    ]
    schema_l = types.TableSchema(cols_l, 0)
    vm_l = runtime.VMSchema(schema_l)

    # Right (trace): (pk:U64, data:I64)
    cols_r = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="data"),
    ]
    schema_r = types.TableSchema(cols_r, 0)
    vm_r = runtime.VMSchema(schema_r)

    out_schema = types.merge_schemas_for_join(schema_l, schema_r)
    out_vm = runtime.VMSchema(out_schema)

    # Pre-populate trace with pk=42 appearing 3 times (different payloads, w=1)
    trace_dir = os.path.join(base_dir, "trace_multi")
    trace_table = EphemeralTable(trace_dir, "trace_multi", schema_r)

    # Ingest 3 separate batches so pk=42 has 3 entries
    for payload_val in [100, 200, 300]:
        tb = batch.ZSetBatch(schema_r)
        tr = values.make_payload_row(schema_r)
        tr.append_int(r_int64(payload_val))
        tb.append(r_uint128(42), r_int64(1), tr)
        trace_table.ingest_batch(tb)
        tb.free()

    trace_cursor = trace_table.create_cursor()
    reg_file = runtime.RegisterFile(3)
    reg_file.registers[0] = runtime.DeltaRegister(0, vm_l)
    reg_file.registers[1] = runtime.TraceRegister(1, vm_r, trace_cursor, trace_table)
    reg_file.registers[2] = runtime.DeltaRegister(2, out_vm)

    program = [
        instructions.JoinDeltaTraceOp(
            reg_file.registers[0], reg_file.registers[1], reg_file.registers[2]
        ),
        instructions.HaltOp(),
    ]

    plan = make_plan(program, reg_file, out_schema, in_reg=0, out_reg=2)

    # Delta: pk=42 w=2 (should match 3 trace rows), pk=99 w=0 (zero-weight, skip)
    in_batch = batch.ZSetBatch(schema_l)
    rl1 = values.make_payload_row(schema_l)
    rl1.append_int(r_int64(7))
    in_batch.append(r_uint128(42), r_int64(2), rl1)

    rl2 = values.make_payload_row(schema_l)
    rl2.append_int(r_int64(9))
    in_batch.append(r_uint128(99), r_int64(0), rl2)

    result = plan.execute_epoch(in_batch)

    assert_true(result is not None, "Multi-match join should produce output")
    assert_equal_i(3, result.length(), "Should have 3 output rows (one per trace match)")

    found_payloads = [False, False, False]
    trace_vals = [r_int64(100), r_int64(200), r_int64(300)]
    for i in range(result.length()):
        assert_equal_u128(r_uint128(42), result.get_pk(i), "All output PKs should be 42")
        assert_equal_i64(r_int64(2), result.get_weight(i), "Weight should be 2*1=2")
        out_row = result.get_row(i)
        # col 0 = left tag (should be 7), col 1 = right data (100/200/300)
        assert_equal_i64(r_int64(7), out_row.get_int_signed(0),
                         "Left tag payload should be 7")
        right_data = out_row.get_int_signed(1)
        for j in range(3):
            if right_data == trace_vals[j]:
                found_payloads[j] = True
    assert_true(found_payloads[0], "Missing trace payload 100 in join output")
    assert_true(found_payloads[1], "Missing trace payload 200 in join output")
    assert_true(found_payloads[2], "Missing trace payload 300 in join output")

    result.free()
    in_batch.free()
    trace_table.close()
    log("  PASSED")

# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------

def entry_point(argv):
    os.write(1, "--- GnitzDB Comprehensive VM Package Test ---\n")
    base_dir = "vm_test_data"
    cleanup_dir(base_dir)
    os.mkdir(base_dir)

    try:
        test_filter_map_negate()
        test_union()
        test_join_delta_trace(base_dir)
        test_distinct_multi_tick(base_dir)
        test_reduce_sum(base_dir)
        test_ghost_property()
        test_empty_input()
        test_chunked_scan_resume(base_dir)
        test_seek_trace_point_lookup(base_dir)
        test_empty_table_scan(base_dir)
        test_join_delta_delta()
        test_delay_op()
        test_reduce_min_nonlinear(base_dir)
        test_join_delta_trace_multi_match(base_dir)
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
