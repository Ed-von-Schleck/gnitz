# gnitz/tests/vm/test_vm_ops.py

import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, r_int64
from gnitz.core import types, values, scalar
from gnitz.storage.table import PersistentTable
from gnitz.vm import batch, ops, runtime

# ------------------------------------------------------------------------------
# Concrete ScalarFunctions for Testing
# ------------------------------------------------------------------------------

class ScoreFilter(scalar.ScalarFunction):
    """Filters rows where Score (Col 2) > 50."""
    def evaluate_predicate(self, row_accessor):
        return row_accessor.get_int(2) > 50

class ScoreDoubler(scalar.ScalarFunction):
    """Maps Score (Col 2) to Score * 2."""
    def evaluate_map(self, row_accessor, output_row_list):
        # Col 1: Name (String)
        meta = row_accessor.get_str_struct(1)
        name = meta[4] # For ZSetBatch, meta[4] contains the python string
        output_row_list.append(values.TaggedValue.make_string(name))
        # Col 2: Score (Int)
        score = row_accessor.get_int(2)
        output_row_list.append(values.TaggedValue.make_int(r_int64(score * 2)))

# ------------------------------------------------------------------------------
# Test Suite
# ------------------------------------------------------------------------------

class TestVMOps(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_vm_ops_env"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.mkdir(self.test_dir)

        # Schema: PK(u128) | Name(String) | Score(Int64)
        self.schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_I64)
        ], pk_index=0)
        self.vm_schema = runtime.VMSchema(self.schema)

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def _mk_payload(self, name, score):
        return [
            values.TaggedValue.make_string(name),
            values.TaggedValue.make_int(score)
        ]

    def test_filter_op(self):
        """Verify OP_FILTER correctly selects rows based on predicate."""
        reg_in = runtime.DeltaRegister(0, self.vm_schema)
        reg_out = runtime.DeltaRegister(1, self.vm_schema)
        
        reg_in.batch.append(1, 1, self._mk_payload("Low", 10))
        reg_in.batch.append(2, 1, self._mk_payload("High", 100))
        
        ops.op_filter(reg_in, reg_out, ScoreFilter())
        
        self.assertEqual(reg_out.batch.row_count(), 1)
        self.assertEqual(reg_out.batch.get_key(0), r_uint128(2))
        self.assertEqual(reg_out.batch.get_payload(0)[1].i64, 100)

    def test_map_op(self):
        """Verify OP_MAP correctly transforms columns."""
        reg_in = runtime.DeltaRegister(0, self.vm_schema)
        reg_out = runtime.DeltaRegister(1, self.vm_schema)
        reg_in.batch.append(1, 1, self._mk_payload("Test", 25))
        
        ops.op_map(reg_in, reg_out, ScoreDoubler())
        
        self.assertEqual(reg_out.batch.row_count(), 1)
        self.assertEqual(reg_out.batch.get_payload(0)[1].i64, 50)

    def test_negate_op(self):
        """Verify OP_NEGATE flips the weights (Algebraic Retraction)."""
        reg_in = runtime.DeltaRegister(0, self.vm_schema)
        reg_out = runtime.DeltaRegister(1, self.vm_schema)
        reg_in.batch.append(1, 5, self._mk_payload("Data", 0))
        
        ops.op_negate(reg_in, reg_out)
        self.assertEqual(reg_out.batch.get_weight(0), r_int64(-5))

    def test_join_delta_delta(self):
        """Verify sort-merge join between two transient batches."""
        # Schema B: PK(u128) | Tag(String)
        schema_b = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_STRING)
        ], pk_index=0)
        vm_schema_b = runtime.VMSchema(schema_b)
        
        # Result Schema: PK | Name | Score | Tag
        res_schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_I64),
            types.ColumnDefinition(types.TYPE_STRING)
        ], pk_index=0)
        vm_schema_res = runtime.VMSchema(res_schema)
        
        reg_a = runtime.DeltaRegister(0, self.vm_schema)
        reg_b = runtime.DeltaRegister(1, vm_schema_b)
        reg_out = runtime.DeltaRegister(2, vm_schema_res)
        
        reg_a.batch.append(10, 2, self._mk_payload("UserA", 100))
        reg_b.batch.append(10, 3, [values.TaggedValue.make_string("Premium")])
        
        ops.op_join_delta_delta(reg_a, reg_b, reg_out)
        
        self.assertEqual(reg_out.batch.row_count(), 1)
        # Weight product: 2 * 3 = 6
        self.assertEqual(reg_out.batch.get_weight(0), r_int64(6))
        # Payload concatenation: ["UserA", 100] + ["Premium"]
        payload = reg_out.batch.get_payload(0)
        self.assertEqual(len(payload), 3)
        self.assertEqual(payload[0].str_val, "UserA")
        self.assertEqual(payload[2].str_val, "Premium")

    def test_join_delta_trace(self):
        """Verify INLJ join between a batch and a PersistentTable cursor."""
        db = PersistentTable(self.test_dir, "join_table", self.schema)
        try:
            # 1. Prepare Persistent State (Trace)
            db.insert(123, self._mk_payload("Persistent", 999))
            db.flush()
            
            # 2. Prepare Delta
            reg_delta = runtime.DeltaRegister(0, self.vm_schema)
            reg_delta.batch.append(123, 2, self._mk_payload("Incoming", 1))
            
            # 3. Setup Trace Register
            cursor = db.create_cursor()
            reg_trace = runtime.TraceRegister(1, self.vm_schema, cursor)
            
            # 4. Setup Output Register
            # Result: PK | Name_D | Score_D | Name_T | Score_T
            res_columns = [types.ColumnDefinition(types.TYPE_U128)]
            res_columns.extend(self.schema.columns[1:])
            res_columns.extend(self.schema.columns[1:])
            res_schema = types.TableSchema(res_columns, pk_index=0)
            reg_out = runtime.DeltaRegister(2, runtime.VMSchema(res_schema))
            
            ops.op_join_delta_trace(reg_delta, reg_trace, reg_out)
            
            self.assertEqual(reg_out.batch.row_count(), 1)
            # Weight: 2 (Delta) * 1 (Trace) = 2
            self.assertEqual(reg_out.batch.get_weight(0), r_int64(2))
            # Payload: ["Incoming", 1, "Persistent", 999]
            payload = reg_out.batch.get_payload(0)
            self.assertEqual(payload[0].str_val, "Incoming")
            self.assertEqual(payload[2].str_val, "Persistent")
            self.assertEqual(payload[3].i64, 999)
            
        finally:
            db.close()

    def test_distinct_op(self):
        """Verify Distinct normalizes positive weights to 1 and consolidates."""
        reg_in = runtime.DeltaRegister(0, self.vm_schema)
        reg_out = runtime.DeltaRegister(1, self.vm_schema)
        
        db = PersistentTable(self.test_dir, "dist_history", self.schema)
        try:
            cursor = db.create_cursor()
            # Pass db as the table reference for state lookups
            reg_hist = runtime.TraceRegister(2, self.vm_schema, cursor, db)
            
            pk = r_uint128(1)
            payload = self._mk_payload("Same", 10)
            
            # Record doesn't exist (T=0). Add +5.
            # sign(0+5) - sign(0) = 1.
            reg_in.batch.append(pk, 5, payload)
            
            ops.op_distinct(reg_in, reg_hist, reg_out)
            
            self.assertEqual(reg_out.batch.row_count(), 1)
            self.assertEqual(reg_out.batch.get_weight(0), r_int64(1))
            
            # Verify state was updated in history table
            self.assertEqual(db.get_weight(pk, payload), 5)
        finally:
            db.close()

    def test_integrate_op(self):
        """Verify OP_INTEGRATE correctly flushes data into the storage engine."""
        db = PersistentTable(self.test_dir, "sink_table", self.schema)
        try:
            reg_in = runtime.DeltaRegister(0, self.vm_schema)
            reg_in.batch.append(444, 1, self._mk_payload("Sinked", 42))
            
            # FIXED: Removed the '1' (table_id) argument. 
            # The operator now only needs the register and the engine instance.
            ops.op_integrate(reg_in, db.engine)
            
            # Verify weight via storage engine logic
            self.assertEqual(db.get_weight(444, self._mk_payload("Sinked", 42)), 1)
        finally:
            db.close()

if __name__ == '__main__':
    unittest.main()
