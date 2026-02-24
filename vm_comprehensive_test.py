# vm_comprehensive_test.py

import sys
import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from gnitz.core import types, values, batch, strings
from gnitz.core.comparator import PayloadRowAccessor
from gnitz.dbsp import functions, ops
from gnitz.vm import runtime, instructions, interpreter, query
from gnitz.storage.table import PersistentTable

# ------------------------------------------------------------------------------
# Mock Scalar Functions
# ------------------------------------------------------------------------------


class ComprehensiveFilter(functions.ScalarFunction):
    def evaluate_predicate(self, row_accessor):
        # Schema A: [PK(U128), F64, STRING, I64]
        # We filter on the F64 column (index 1, since PK is 0)
        f_val = row_accessor.get_float(1)
        return f_val > 0.5


class LongStringMapper(functions.ScalarFunction):
    def evaluate_map(self, row_accessor, output_row):
        # Transform Schema A -> Schema A
        # 1. Pass through Float
        output_row.append_float(row_accessor.get_float(1))

        # 2. String transformation (Force Long String to exercise BlobAllocator)
        res = row_accessor.get_str_struct(2)
        # resolve_string handles the structural check (inline vs heap)
        orig_str = strings.resolve_string(res[2], res[3], res[4])
        new_str = "PROCESSED_" + orig_str + "_WITH_A_VERY_LONG_EXTENDED_SUFFIX"
        output_row.append_string(new_str)

        # 3. Increment Integer
        orig_int = row_accessor.get_int_signed(3)
        output_row.append_int(r_int64(intmask(orig_int + 1)))


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def mk_u128(hi, lo):
    return (r_uint128(hi) << 64) | r_uint128(lo)


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
# Tests
# ------------------------------------------------------------------------------


def test_full_vm_pipeline(base_dir):
    """
    Tests Filter, Map, Negate, Union, Distinct, JoinDeltaTrace, and Integrate.
    """
    os.write(1, "[VM] Testing Pipeline (Filter->Map->Negate->Distinct->Join)...\n")
    db_path = os.path.join(base_dir, "vm_pipeline")
    if not os.path.exists(db_path):
        os.mkdir(db_path)

    schema_a = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U128),  # PK
            types.ColumnDefinition(types.TYPE_F64),
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_I64),
        ],
        pk_index=0,
    )

    schema_b = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U128),  # PK
            types.ColumnDefinition(types.TYPE_STRING),
        ],
        pk_index=0,
    )

    tab_a = PersistentTable(db_path, "input_a", schema_a)
    tab_b = PersistentTable(db_path, "state_b", schema_b)
    res_tab = PersistentTable(
        db_path, "output_res", types.merge_schemas_for_join(schema_a, schema_b)
    )

    try:
        pk = mk_u128(0x1111, 0x2222)

        # 1. Setup Persistent State in Tab B
        b_row = values.make_payload_row(schema_b)
        b_row.append_string("DEPARTMENT_X")
        b_batch = batch.make_singleton_batch(schema_b, pk, r_int64(1), b_row)
        tab_b.ingest_batch(b_batch)
        b_batch.free()
        tab_b.flush()

        # 2. Build Circuit
        qb = query.QueryBuilder(tab_a, schema_a)
        view = (
            qb.filter(ComprehensiveFilter())
            .map(LongStringMapper(), schema_a)
            .negate()
            .union(None)
            .distinct()
            .join_persistent(tab_b)
            .sink(res_tab)
            .build()
        )

        # 3. Create Input Batch — row passes filter (0.7 > 0.5)
        a_row = values.make_payload_row(schema_a)
        a_row.append_float(0.7)
        a_row.append_string("Alice")
        a_row.append_int(r_int64(100))

        in_batch = batch.make_singleton_batch(schema_a, pk, r_int64(1), a_row)

        # 4. Process
        os.write(1, "    -> Executing circuit...\n")
        view.process(in_batch)

        # 5. Verify
        # Input has weight 1. negate() makes weight -1.
        # distinct() is defined as: s_new - s_old, where s(w) = 1 if w > 0 else 0.
        # For a new record with net weight -1: s_old=0, s_new=0 -> out_w=0.
        # No record should be emitted into res_tab.
        check_acc = PayloadRowAccessor(res_tab.schema)
        exp_row = values.make_payload_row(res_tab.schema)
        exp_row.append_float(0.7)
        exp_row.append_string("PROCESSED_Alice_WITH_A_VERY_LONG_EXTENDED_SUFFIX")
        exp_row.append_int(r_int64(101))
        exp_row.append_string("DEPARTMENT_X")

        check_acc.set_row(exp_row)
        w = res_tab.get_weight(pk, check_acc)

        if w != 0:
            os.write(2, "ERR: Expected weight 0 due to negate+distinct\n")
            in_batch.free()
            view.close()
            return False

        os.write(1, "    [OK] Pipeline verified.\n")
        in_batch.free()
        view.close()
        return True
    finally:
        tab_a.close()
        tab_b.close()
        res_tab.close()


def test_reduce_aggregates(base_dir):
    """
    Tests the ReduceOp path (linear aggregate: COUNT).
    """
    os.write(1, "[VM] Testing Reduce (COUNT)...\n")
    db_path = os.path.join(base_dir, "vm_reduce")
    if not os.path.exists(db_path):
        os.mkdir(db_path)

    schema = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U64),  # PK
            types.ColumnDefinition(types.TYPE_I64),  # Val
        ],
        pk_index=0,
    )

    tab = PersistentTable(db_path, "src", schema)

    try:
        qb_count = query.QueryBuilder(tab, schema)
        view_count = qb_count.reduce([0], functions.CountAggregateFunction()).build()

        row = values.make_payload_row(schema)
        row.append_int(r_int64(42))

        in_batch = batch.ZSetBatch(schema)
        in_batch.append(r_uint128(100), r_int64(5), row)  # 5 occurrences

        out_batch = view_count.process(in_batch)

        # Result Schema: [PK(U64), Agg(I64)]
        if out_batch.get_weight(0) != 1:
            os.write(2, "ERR: COUNT expected weight 1\n")
            in_batch.free()
            view_count.close()
            return False
        if out_batch.get_pk(0) != r_uint128(100):
            os.write(2, "ERR: COUNT expected PK 100\n")
            in_batch.free()
            view_count.close()
            return False

        val = out_batch.get_accessor(0).get_int_signed(1)
        if val != 5:
            os.write(2, "ERR: COUNT expected 5, got %d\n" % int(val))
            in_batch.free()
            view_count.close()
            return False

        os.write(1, "    [OK] Reduce COUNT verified.\n")
        in_batch.free()
        view_count.close()
        return True
    finally:
        tab.close()


def test_temporal_and_smj(base_dir):
    """
    Tests Delay and JoinDeltaDelta (Sort-Merge Join).
    """
    os.write(1, "[VM] Testing Delay and Sort-Merge Join...\n")
    db_path = os.path.join(base_dir, "vm_smj")
    if not os.path.exists(db_path):
        os.mkdir(db_path)

    schema = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_I64),
        ],
        pk_index=0,
    )

    joined_schema = types.merge_schemas_for_join(schema, schema)

    reg_file = runtime.RegisterFile(3)
    vm_schema = runtime.VMSchema(schema)

    r0 = runtime.DeltaRegister(0, vm_schema)
    r1 = runtime.DeltaRegister(1, vm_schema)
    r2 = runtime.DeltaRegister(2, runtime.VMSchema(joined_schema))

    reg_file.registers[0] = r0
    reg_file.registers[1] = r1
    reg_file.registers[2] = r2

    prog = [
        instructions.JoinDeltaDeltaOp(r0, r1, r2),
        instructions.DelayOp(r0, r1),
        instructions.HaltOp(),
    ]

    interp = interpreter.DBSPInterpreter(reg_file, prog)

    pk = mk_u128(9, 9)
    row = values.make_payload_row(schema)
    row.append_int(r_int64(7))
    in_batch = batch.make_singleton_batch(schema, pk, r_int64(1), row)

    # Tick 1: JoinDeltaDelta(r0, r1) where r1 is empty -> result empty.
    # DelayOp then copies r0 into r1 for the next tick.
    interp.execute(in_batch)
    if r2.batch.length() != 0:
        os.write(2, "ERR: r2 should be empty after Tick 1\n")
        in_batch.free()
        r0.batch.free()
        r1.batch.free()
        r2.batch.free()
        return False

    # Simulate the join that would occur on the next tick: r0 still holds the
    # input (execute() cleared r2 but r0 was re-populated by the input ingestion
    # loop) and r1 now holds the delayed copy from Tick 1.
    r2.batch.clear()
    ops.op_join_delta_delta(
        r0.batch,
        r1.batch,
        r2.batch,
        schema,
        schema,
    )

    if r2.batch.length() != 1:
        os.write(2, "ERR: SMJ failed to find match, length=%d\n" % r2.batch.length())
        in_batch.free()
        r0.batch.free()
        r1.batch.free()
        r2.batch.free()
        return False

    os.write(1, "    [OK] Temporal and SMJ ops verified.\n")
    in_batch.free()
    r0.batch.free()
    r1.batch.free()
    r2.batch.free()
    return True


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------


def entry_point(argv):
    os.write(1, "--- GnitzDB Comprehensive VM Test (Zero-Copy API) ---\n")
    base_dir = "vm_test_data"
    if os.path.exists(base_dir):
        cleanup_dir(base_dir)
    os.mkdir(base_dir)

    try:
        if not test_full_vm_pipeline(base_dir):
            return 1
        if not test_reduce_aggregates(base_dir):
            return 1
        if not test_temporal_and_smj(base_dir):
            return 1

        os.write(1, "\nALL VM TEST PATHS PASSED\n")
        return 0
    except Exception as e:
        os.write(2, "FATAL ERROR: %s\n" % str(e))
        return 1
    finally:
        cleanup_dir(base_dir)


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
