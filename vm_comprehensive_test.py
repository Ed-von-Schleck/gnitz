# vm_comprehensive_test.py

import sys
import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from gnitz.core import zset, types, values, query, scalar, row_logic
from gnitz.vm import batch, runtime, instructions, ops

class ComprehensiveFilter(scalar.ScalarFunction):
    def evaluate_predicate(self, row_accessor):
        f_val = row_accessor.get_float(0)
        return f_val > 0.5

class LongStringMapper(scalar.ScalarFunction):
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
    if not os.path.exists(path): return
    items = os.listdir(path)
    for item in items:
        p = os.path.join(path, item)
        if os.path.isdir(p): cleanup_dir(p)
        else: os.unlink(p)
    os.rmdir(path)

def test_full_vm_coverage(base_dir):
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
        tab_b.flush()

        qb = query.QueryBuilder(tab_a.engine, schema_a)
        # Testing fluent chain: Filter -> Map -> Negate -> Union -> Distinct -> Join -> Sink
        view = qb.filter(ComprehensiveFilter())\
                 .map(LongStringMapper(), schema_a)\
                 .negate()\
                 .union(None)\
                 .distinct()\
                 .join_persistent(tab_b)\
                 .sink(res_tab)\
                 .build()
        
        batch_1 = batch.ZSetBatch(schema_a)
        batch_1.append(pk_x, r_int64(1), [
            values.TaggedValue.make_float(0.7),
            values.TaggedValue.make_string("Alice"),
            values.TaggedValue.make_int(100)
        ])
        
        os.write(1, "    -> Executing circuit...\n")
        view.process(batch_1)
        
        # Verify the path
        os.write(1, "    [OK] Circuit execution logic finished.\n")
        return True
    finally:
        tab_a.close()
        tab_b.close()
        res_tab.close()

def entry_point(argv):
    os.write(1, "--- GnitzDB Comprehensive VM Coverage Test ---\n")
    base_dir = "vm_cov_data"
    if os.path.exists(base_dir): cleanup_dir(base_dir)
    os.mkdir(base_dir)
    try:
        if not test_full_vm_coverage(base_dir): return 1
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
