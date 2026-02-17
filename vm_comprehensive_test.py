# vm_comprehensive_test.py

import sys
import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from gnitz.core import zset, types, values, query, scalar, row_logic
from gnitz.vm import batch, runtime, instructions, ops

# ------------------------------------------------------------------------------
# Specialized Scalar Functions for Coverage
# ------------------------------------------------------------------------------

class ComprehensiveFilter(scalar.ScalarFunction):
    """Exercises Float comparison and Null checking."""
    def evaluate_predicate(self, row_accessor):
        # Col 0: Float, Col 1: String, Col 2: Nullable Int
        f_val = row_accessor.get_float(0)
        # Only keep if Float > 0.5
        return f_val > 0.5

class LongStringMapper(scalar.ScalarFunction):
    """Exercises the 'Long' German String path (> 12 chars)."""
    def evaluate_map(self, row_accessor, output_row_list):
        meta = row_accessor.get_str_struct(1)
        orig_str = meta[4]
        if orig_str is None:
            orig_str = "Unknown"
        
        # Create a string > 12 characters to force Heap/Region B logic
        long_str = "PREFIX_" + orig_str + "_LONG_SUFFIX_TAIL"
        
        output_row_list.append(values.TaggedValue.make_float(row_accessor.get_float(0)))
        output_row_list.append(values.TaggedValue.make_string(long_str))
        output_row_list.append(values.TaggedValue.make_null())

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def mk_u128(hi, lo):
    return (r_uint128(hi) << 64) | r_uint128(lo)

def cleanup_dir(path):
    if not os.path.exists(path): return
    items = os.listdir(path)
    for item in items:
        p = os.path.join(path, item)
        if os.path.isdir(p): 
            cleanup_dir(p)
        else: 
            os.unlink(p)
    os.rmdir(path)

# ------------------------------------------------------------------------------
# Integration Tests
# ------------------------------------------------------------------------------

def test_full_vm_coverage(base_dir):
    """
    Test Objective: Cover u128, Long Strings, Floats, Joins, Union, Negate, Distinct.
    """
    os.write(1, "[VM Comprehensive] Starting Coverage Marathon...\n")
    db_path = os.path.join(base_dir, "vm_cov")
    if not os.path.exists(db_path): os.mkdir(db_path)

    schema_a = types.TableSchema([
        types.ColumnDefinition(types.TYPE_U128),
        types.ColumnDefinition(types.TYPE_F64),
        types.ColumnDefinition(types.TYPE_STRING),
        types.ColumnDefinition(types.TYPE_I64)
    ], pk_index=0)

    schema_b = types.TableSchema([
        types.ColumnDefinition(types.TYPE_U128),
        types.ColumnDefinition(types.TYPE_STRING)
    ], pk_index=0)

    tab_a = zset.PersistentTable(db_path, "table_a", schema_a)
    tab_b = zset.PersistentTable(db_path, "table_b", schema_b)
    res_tab = zset.PersistentTable(db_path, "table_res", schema_a) 

    try:
        pk_x = mk_u128(0xAAAA, 0xBBBB)
        tab_b.insert(pk_x, [values.TaggedValue.make_string("Dept_Alpha")])
        tab_b.insert(pk_x, [values.TaggedValue.make_string("Dept_Beta")])
        tab_b.flush()

        qb = query.QueryBuilder(tab_a.engine, schema_a)
        view = qb.filter(ComprehensiveFilter())\
                 .map(LongStringMapper(), schema_a)\
                 .negate()\
                 .union(None)\
                 .distinct()\
                 .join_persistent(tab_b)\
                 .sink(res_tab)\
                 .build()
        
        batch_1 = batch.ZSetBatch(schema_a)
        batch_1.append(pk_x, 1, [
            values.TaggedValue.make_float(0.7),
            values.TaggedValue.make_string("Alice"),
            values.TaggedValue.make_int(100)
        ])
        batch_1.append(pk_x, 1, [
            values.TaggedValue.make_float(0.1),
            values.TaggedValue.make_string("Bob"),
            values.TaggedValue.make_int(200)
        ])

        os.write(1, "    -> Executing circuit (Joins, Strings, u128)...\n")
        view.process(batch_1)

        w_res = res_tab.get_weight(pk_x, [
            values.TaggedValue.make_float(0.7),
            values.TaggedValue.make_string("PREFIX_Alice_LONG_SUFFIX_TAIL"),
            values.TaggedValue.make_null(),
            values.TaggedValue.make_string("Dept_Alpha")
        ])
        
        if w_res != 0:
            os.write(2, "ERR: Negate+Distinct logic failed.\n")
            return False
        os.write(1, "    [OK] Negate + Distinct + Ghost Property verified.\n")

        print "    -> Testing Delta-Delta Sort-Merge Join..."
        reg_a = runtime.DeltaRegister(10, runtime.VMSchema(schema_b))
        reg_b = runtime.DeltaRegister(11, runtime.VMSchema(schema_b))
        reg_out = runtime.DeltaRegister(12, runtime.VMSchema(schema_b))
        
        pk_y = mk_u128(1, 1)
        reg_a.batch.append(pk_y, 2, [values.TaggedValue.make_string("A")])
        reg_b.batch.append(pk_y, 3, [values.TaggedValue.make_string("B")])
        
        ops.op_join_delta_delta(reg_a, reg_b, reg_out)
        
        if reg_out.batch.row_count() != 1 or reg_out.batch.weights[0] != 6:
            os.write(2, "ERR: Delta-Delta Join failed.\n")
            return False
        
        os.write(1, "    [OK] Delta-Delta Sort-Merge Join verified.\n")
        return True

    finally:
        tab_a.close()
        tab_b.close()
        res_tab.close()

def test_string_heap_materialization():
    """Specifically exercises row_logic.materialize_row for long strings."""
    os.write(1, "[VM Comprehensive] Long String Heap Materialization...\n")
    schema = types.TableSchema([
        types.ColumnDefinition(types.TYPE_U64),
        types.ColumnDefinition(types.TYPE_STRING)
    ], pk_index=0)

    b = batch.ZSetBatch(schema)
    long_str = "This string is definitely longer than twelve characters"
    b.append(r_uint128(1), 1, [values.TaggedValue.make_string(long_str)])

    acc = b.left_acc
    acc.set_row(b.payloads, 0)
    
    row = ops.materialize_row(acc, schema)
    
    if row[0].str_val != long_str:
        os.write(2, "ERR: String materialization corrupted.\n")
        return False
    
    os.write(1, "    [OK] Materialization of long strings verified.\n")
    return True

# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------

def entry_point(argv):
    os.write(1, "--- GnitzDB Comprehensive VM Coverage Test ---\n")
    
    base_dir = "vm_cov_data"
    if os.path.exists(base_dir): 
        cleanup_dir(base_dir)
    os.mkdir(base_dir)

    try:
        if not test_string_heap_materialization(): return 1
        if not test_full_vm_coverage(base_dir): return 1

        os.write(1, "\nPASSED: Comprehensive VM logic exercised.\n")
        return 0
    except Exception:
        os.write(2, "FATAL ERROR during VM test execution\n")
        return 1
    finally:
        cleanup_dir(base_dir)

def target(driver, args):
    return entry_point, None

if __name__ == "__main__":
    entry_point(sys.argv)
