# vm_comprehensive_test.py

import sys
import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from gnitz.core import types, values, query, row_logic
from gnitz.vm import functions
from gnitz.storage.table import PersistentTable
from gnitz.core.row_logic import make_payload_row
from gnitz.vm import batch, runtime, instructions, ops, interpreter


class ComprehensiveFilter(functions.ScalarFunction):
    def evaluate_predicate(self, row_accessor):
        f_val = row_accessor.get_float(0)
        return f_val > 0.5


class LongStringMapper(functions.ScalarFunction):
    def evaluate_map(self, row_accessor, output_row_list):
        meta = row_accessor.get_str_struct(1)
        orig_str = meta[4]
        if orig_str is None:
            orig_str = "Unknown"
        long_str = "PREFIX_" + orig_str + "_LONG_SUFFIX_TAIL"
        # Output matches schema_a: Float, String, Int
        output_row_list.append(values.TaggedValue.make_float(row_accessor.get_float(0)))
        output_row_list.append(values.TaggedValue.make_string(long_str))
        output_row_list.append(values.TaggedValue.make_int(100))


def mk_u128(hi, lo):
    return (r_uint128(hi) << 64) | r_uint128(lo)


def cleanup_dir(path):
    if not os.path.exists(path):
        return
    items = os.listdir(path)
    for item in items:
        p = os.path.join(path, item)
        if os.path.isdir(p):
            cleanup_dir(p)
        else:
            os.unlink(p)
    os.rmdir(path)


def test_full_vm_coverage(base_dir):
    os.write(1, "[VM Comprehensive] Starting Coverage Marathon...\n")
    db_path = os.path.join(base_dir, "vm_cov")
    if not os.path.exists(db_path):
        os.mkdir(db_path)

    schema_a = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_F64),
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_I64),
        ],
        pk_index=0,
    )

    schema_b = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_STRING),
        ],
        pk_index=0,
    )

    tab_a = PersistentTable(db_path, "table_a", schema_a)
    tab_b = PersistentTable(db_path, "table_b", schema_b)
    res_tab = PersistentTable(db_path, "table_res", schema_a)	

    try:
        pk_x = mk_u128(0xAAAA, 0xBBBB)

        # Build tab_b payload...
        tab_b_row = make_payload_row(1)
        tab_b_row.append(values.TaggedValue.make_string("Dept_Alpha"))
        tab_b.insert(pk_x, tab_b_row)
        tab_b.flush()

        # FIX: Pass tab_a directly to the QueryBuilder
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

        # Build batch_1 payload without list literals.
        batch_1_row = make_payload_row(3)
        batch_1_row.append(values.TaggedValue.make_float(0.7))
        batch_1_row.append(values.TaggedValue.make_string("Alice"))
        batch_1_row.append(values.TaggedValue.make_int(100))

        batch_1 = batch.ZSetBatch(schema_a)
        batch_1.append(pk_x, r_int64(1), batch_1_row)

        os.write(1, "    -> Executing circuit...\n")
        view.process(batch_1)

        os.write(1, "    [OK] Circuit execution logic finished.\n")
        return True
    finally:
        tab_a.close()
        tab_b.close()
        res_tab.close()


def test_delay_and_join_delta_delta(base_dir):
    """
    Exercises the DelayOp and JoinDeltaDeltaOp instruction types.
    """
    os.write(1, "[VM Comprehensive] Testing Delay and JoinDeltaDelta ops...\n")
    db_path = os.path.join(base_dir, "vm_delay")
    if not os.path.exists(db_path):
        os.mkdir(db_path)

    schema = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_I64),
        ],
        pk_index=0,
    )

    # Schema for the output of Join(schema, schema).
    # Result has PK(U128) + I64 (left) + I64 (right).
    schema_out = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_I64),
            types.ColumnDefinition(types.TYPE_I64),
        ],
        pk_index=0,
    )

    tab = PersistentTable(db_path, "delay_test", schema)
    try:
        vm_schema = runtime.VMSchema(schema)
        vm_schema_out = runtime.VMSchema(schema_out)

        # Three registers: input, delayed-input, join-output.
        reg0 = runtime.DeltaRegister(0, vm_schema)
        reg1 = runtime.DeltaRegister(1, vm_schema)
        # Use the correct output schema for the join result register
        reg2 = runtime.DeltaRegister(2, vm_schema_out)

        reg_file = runtime.RegisterFile(3)
        reg_file.registers[0] = reg0
        reg_file.registers[1] = reg1
        reg_file.registers[2] = reg2

        # Circuit: delay(reg0) -> reg1, join_delta_delta(reg0, reg1) -> reg2
        program = [
            instructions.DelayOp(reg0, reg1),
            instructions.JoinDeltaDeltaOp(reg0, reg1, reg2),
            instructions.HaltOp(),
        ]

        interp = interpreter.DBSPInterpreter(tab.engine, reg_file, program)

        pk = mk_u128(1, 2)
        row = make_payload_row(1)
        row.append(values.TaggedValue.make_int(r_uint64(42)))

        input_batch = batch.ZSetBatch(schema)
        input_batch.append(pk, r_int64(1), row)

        interp.execute(input_batch)

        os.write(1, "    [OK] Delay and JoinDeltaDelta ops verified.\n")
        return True
    finally:
        tab.close()


def entry_point(argv):
    os.write(1, "--- GnitzDB Comprehensive VM Coverage Test ---\n")
    base_dir = "vm_cov_data"
    if os.path.exists(base_dir):
        cleanup_dir(base_dir)
    os.mkdir(base_dir)
    try:
        if not test_full_vm_coverage(base_dir):
            return 1
        if not test_delay_and_join_delta_delta(base_dir):
            return 1
        os.write(1, "PASSED\n")
        return 0
    except Exception:
        os.write(2, "FATAL\n")
        return 1
    finally:
        cleanup_dir(base_dir)


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
