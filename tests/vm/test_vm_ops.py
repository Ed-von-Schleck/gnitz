# gnitz/tests/vm/test_vm_ops.py

import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, r_int64
from gnitz.core import types
from gnitz.vm import functions
from gnitz.storage.table import PersistentTable
from gnitz.vm import ops, runtime
from tests.row_helpers import create_test_row

# ------------------------------------------------------------------------------
# Concrete ScalarFunctions for Testing
# ------------------------------------------------------------------------------

class ScoreFilter(functions.ScalarFunction):
    """Filters rows where Score (Schema Col 2) > 50."""
    def evaluate_predicate(self, row_accessor):
        # RowAccessor accepts schema-based indices (0=PK, 1=Name, 2=Score)
        return row_accessor.get_int(2) > 50

class ScoreDoubler(functions.ScalarFunction):
    """Maps Score (Col 2) to Score * 2."""
    def evaluate_map(self, row_accessor, output_row):
        # Col 1: Name (String)
        res = row_accessor.get_str_struct(1)
        name = res[4]  # The python string is in the 5th element for PayloadRowAccessor
        output_row.append_string(name)
        
        # Col 2: Score (Int)
        score = row_accessor.get_int(2)
        output_row.append_int(r_int64(score * 2))

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
        # Helper to create a PayloadRow using the standardized test helper
        return create_test_row(self.schema, [name, score])

    def test_filter_op(self):
        """Verify OP_FILTER correctly selects rows based on predicate."""
        reg_in = runtime.DeltaRegister(0, self.vm_schema)
        reg_out = runtime.DeltaRegister(1, self.vm_schema)
        
        reg_in.batch.append(r_uint128(1), 1, self._mk_payload("Low", 10))
        reg_in.batch.append(r_uint128(2), 1, self._mk_payload("High", 100))
        
        ops.op_filter(reg_in, reg_out, ScoreFilter())
        
        self.assertEqual(reg_out.batch.length(), 1)
        self.assertEqual(reg_out.batch.get_pk(0), r_uint128(2))
        # PayloadRow indices: 0=Name, 1=Score
        self.assertEqual(reg_out.batch.get_row(0).get_int(1), 100)

    def test_map_op(self):
        """Verify OP_MAP correctly transforms columns."""
        reg_in = runtime.DeltaRegister(0, self.vm_schema)
        reg_out = runtime.DeltaRegister(1, self.vm_schema)
        reg_in.batch.append(r_uint128(1), 1, self._mk_payload("Test", 25))
        
        ops.op_map(reg_in, reg_out, ScoreDoubler())
        
        self.assertEqual(reg_out.batch.length(), 1)
        # PayloadRow index 1 is Score
        self.assertEqual(reg_out.batch.get_row(0).get_int(1), 50)

    def test_negate_op(self):
        """Verify OP_NEGATE flips the weights (Algebraic Retraction)."""
        reg_in = runtime.DeltaRegister(0, self.vm_schema)
        reg_out = runtime.DeltaRegister(1, self.vm_schema)
        reg_in.batch.append(r_uint128(1), 5, self._mk_payload("Data", 0))
        
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
        
        reg_a.batch.append(r_uint128(10), 2, self._mk_payload("UserA", 100))
        reg_b.batch.append(r_uint128(10), 3, create_test_row(schema_b, ["Premium"]))
        
        ops.op_join_delta_delta(reg_a, reg_b, reg_out)
        
        self.assertEqual(reg_out.batch.length(), 1)
        # Weight product: 2 * 3 = 6
        self.assertEqual(reg_out.batch.get_weight(0), r_int64(6))
        
        # Result PayloadRow indices: 0=Name, 1=Score, 2=Tag
        row = reg_out.batch.get_row(0)
        self.assertEqual(row.get_str(0), "UserA")
        self.assertEqual(row.get_int(1), 100)
        self.assertEqual(row.get_str(2), "Premium")

    def test_join_delta_trace(self):
        """Verify INLJ join between a batch and a PersistentTable cursor."""
        db = PersistentTable(self.test_dir, "join_table", self.schema)
        try:
            # 1. Prepare Persistent State (Trace)
            db.ingest(r_uint128(123), r_int64(1), self._mk_payload("Persistent", 999))
            db.flush()
            
            # 2. Prepare Delta
            reg_delta = runtime.DeltaRegister(0, self.vm_schema)
            reg_delta.batch.append(r_uint128(123), 2, self._mk_payload("Incoming", 1))
            
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
            
            self.assertEqual(reg_out.batch.length(), 1)
            # Weight: 2 (Delta) * 1 (Trace) = 2
            self.assertEqual(reg_out.batch.get_weight(0), r_int64(2))
            
            # Result PayloadRow indices: 0=Name_D, 1=Score_D, 2=Name_T, 3=Score_T
            row = reg_out.batch.get_row(0)
            self.assertEqual(row.get_str(0), "Incoming")
            self.assertEqual(row.get_str(2), "Persistent")
            self.assertEqual(row.get_int(3), 999)
            
        finally:
            db.close()

    def test_distinct_op(self):
        """Verify Distinct normalizes positive weights to 1 and consolidates."""
        reg_in = runtime.DeltaRegister(0, self.vm_schema)
        reg_out = runtime.DeltaRegister(1, self.vm_schema)
        
        db = PersistentTable(self.test_dir, "dist_history", self.schema)
        try:
            cursor = db.create_cursor()
            reg_hist = runtime.TraceRegister(2, self.vm_schema, cursor, db)
            
            pk = r_uint128(1)
            payload = self._mk_payload("Same", 10)
            
            # Record doesn't exist (T=0). Add +5.
            # sign(0+5) - sign(0) = 1.
            reg_in.batch.append(pk, 5, payload)
            
            ops.op_distinct(reg_in, reg_hist, reg_out)
            
            self.assertEqual(reg_out.batch.length(), 1)
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
            payload = self._mk_payload("Sinked", 42)
            reg_in.batch.append(r_uint128(444), 1, payload)
            
            ops.op_integrate(reg_in, db)
            
            # Verify weight via storage engine logic
            self.assertEqual(db.get_weight(r_uint128(444), payload), 1)
        finally:
            db.close()

if __name__ == '__main__':
    unittest.main()
