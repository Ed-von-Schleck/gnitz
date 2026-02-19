# test_vm_interpreter.py

import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, r_int64
from gnitz.core import types, values 
from gnitz.vm import functions
from gnitz.storage.table import PersistentTable
from gnitz.vm import batch, runtime, query

# ------------------------------------------------------------------------------
# Concrete ScalarFunctions for Testing
# ------------------------------------------------------------------------------

class AgeFilter(functions.ScalarFunction):
    """Filters rows where Age (Col 2) >= 30."""
    def evaluate_predicate(self, row_accessor):
        return row_accessor.get_int(2) >= 30

class AgeDoubler(functions.ScalarFunction):
    """Maps (Name, Age) -> (Name, Age * 2)."""
    def evaluate_map(self, row_accessor, output_row_list):
        # Col 1: Name
        meta = row_accessor.get_str_struct(1)
        name = meta[4] # For ZSetBatch, meta[4] contains the python string
        output_row_list.append(values.TaggedValue.make_string(name))
        # Col 2: Age -> Age * 2
        age = row_accessor.get_int(2)
        output_row_list.append(values.TaggedValue.make_int(r_int64(age * 2)))

# ------------------------------------------------------------------------------
# Test Suite
# ------------------------------------------------------------------------------

class TestVMInterpreter(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_vm_interp_env"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.mkdir(self.test_dir)

        # Schema: PK(u128) | Name(String) | Age(Int64)
        self.schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_I64)
        ], pk_index=0)
        
        # Helper DB for engine/context
        self.db = PersistentTable(self.test_dir, "ctx_db", self.schema)

    def tearDown(self):
        self.db.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def _mk_payload(self, name, age):
        return [
            values.TaggedValue.make_string(name),
            values.TaggedValue.make_int(age)
        ]

    def _batch_to_dict(self, zbatch):
        """Converts batch to (Key, PayloadTuple) -> Weight for easy comparison."""
        res = {}
        for i in range(zbatch.row_count()):
            key = zbatch.get_key(i)
            payload = tuple([v.to_string() for v in zbatch.get_payload(i)])
            weight = zbatch.get_weight(i)
            res[(key, payload)] = res.get((key, payload), 0) + int(weight)
        return res

    def test_basic_pipeline(self):
        """Tests Source -> Filter -> Map -> Result."""
        qb = query.QueryBuilder(self.db, self.schema)
        
        # Result Schema: PK | Name | Age*2
        res_schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_I64)
        ], pk_index=0)

        view = qb.filter(AgeFilter()).map(AgeDoubler(), res_schema).build()

        # Input data
        in_batch = batch.ZSetBatch(self.schema)
        in_batch.append(1, 1, self._mk_payload("Young", 20)) # Should be filtered
        in_batch.append(2, 1, self._mk_payload("Old", 40))   # Should pass and double

        out_batch = view.process(in_batch)
        
        results = self._batch_to_dict(out_batch)
        self.assertEqual(len(results), 1)
        # Key 2: ("Old", 80) -> weight 1
        self.assertEqual(results.get((r_uint128(2), ("Old", "80"))), 1)

    def test_incremental_linearity(self):
        """Verify Q(dA + dB) = Q(dA) + Q(dB)."""
        qb = query.QueryBuilder(self.db, self.schema)
        view = qb.filter(AgeFilter()).build()

        # Delta A
        batch_a = batch.ZSetBatch(self.schema)
        batch_a.append(10, 1, self._mk_payload("Alice", 35))
        
        # Delta B
        batch_b = batch.ZSetBatch(self.schema)
        batch_b.append(20, 1, self._mk_payload("Bob", 45))

        # Process individually
        res_a = view.process(batch_a)
        dict_a = self._batch_to_dict(res_a)

        res_b = view.process(batch_b)
        dict_b = self._batch_to_dict(res_b)

        # Process combined
        batch_total = batch.ZSetBatch(self.schema)
        batch_total.append(10, 1, self._mk_payload("Alice", 35))
        batch_total.append(20, 1, self._mk_payload("Bob", 45))
        
        res_total = view.process(batch_total)
        dict_total = self._batch_to_dict(res_total)

        # Verify dict_a + dict_b == dict_total
        combined_individual = dict_a.copy()
        for k, w in dict_b.items():
            combined_individual[k] = combined_individual.get(k, 0) + w

        self.assertEqual(combined_individual, dict_total)

    def test_integrate_feedback_loop(self):
        """Verify circuit can sink data back into a PersistentTable."""
        sink_db = PersistentTable(self.test_dir, "sink_db", self.schema)
        
        # Use the new fluent API
        qb = query.QueryBuilder(self.db, self.schema)
        view = qb.filter(AgeFilter()).sink(sink_db).build()

        in_batch = batch.ZSetBatch(self.schema)
        in_batch.append(55, 1, self._mk_payload("Survivor", 55))
        in_batch.append(11, 1, self._mk_payload("Ghost", 11))

        view.process(in_batch)

        # Verify sink_db state
        # The Filter AgeFilter() passes Age >= 30.
        # "Survivor" (55) passes. "Ghost" (11) fails.
        self.assertEqual(sink_db.get_weight(55, self._mk_payload("Survivor", 55)), 1)
        self.assertEqual(sink_db.get_weight(11, self._mk_payload("Ghost", 11)), 0)
        
        # Verify the context DB (db) remains empty (ensures sink isolation)
        self.assertEqual(self.db.get_weight(55, self._mk_payload("Survivor", 55)), 0)
        
        sink_db.close()

    def test_incremental_join_logic(self):
        """
        Verify join between a stream and a persistent table.
        Tests that as the persistent table (Trace) changes, the circuit behaves reactively.
        """
        # Persistent Table (B)
        table_b = PersistentTable(self.test_dir, "table_b", self.schema)
        table_b.insert(100, self._mk_payload("Metadata_100", 0))
        table_b.flush()

        # Circuit: Delta(A) JOIN Trace(B)
        qb = query.QueryBuilder(self.db, self.schema)
        view = qb.join_persistent(table_b).build()

        # 1. Delta A arrives for PK 100
        batch_a1 = batch.ZSetBatch(self.schema)
        batch_a1.append(100, 1, self._mk_payload("Event_A", 99))
        
        res1 = view.process(batch_a1)
        results1 = self._batch_to_dict(res1)
        
        # Result should be join of Event_A and Metadata_100
        self.assertEqual(len(results1), 1)
        # PK | NameA | AgeA | NameB | AgeB
        # (100, ("Event_A", "99", "Metadata_100", "0"))
        expected_payload = ("Event_A", "99", "Metadata_100", "0")
        self.assertEqual(results1.get((r_uint128(100), expected_payload)), 1)

        # 2. Change Persistent Table B (Add another payload for PK 100)
        table_b.insert(100, self._mk_payload("Metadata_New", 1))
        # Note: In a real system, the View's cursor would see the new MemTable state
        
        # Re-run same Delta A
        res2 = view.process(batch_a1)
        results2 = self._batch_to_dict(res2)
        
        # Now PK 100 has TWO payloads in B, so join produces 2 rows
        self.assertEqual(len(results2), 2)
        self.assertEqual(results2.get((r_uint128(100), ("Event_A", "99", "Metadata_100", "0"))), 1)
        self.assertEqual(results2.get((r_uint128(100), ("Event_A", "99", "Metadata_New", "1"))), 1)

        table_b.close()

    def test_retraction_logic(self):
        """Tests that negative weights (retractions) propagate through the VM correctly."""
        qb = query.QueryBuilder(self.db, self.schema)
        # Circuit: Source -> Negate
        view = qb.negate().build()

        in_batch = batch.ZSetBatch(self.schema)
        in_batch.append(1, 1, self._mk_payload("Alice", 30))
        in_batch.append(2, -1, self._mk_payload("Bob", 40))

        out_batch = view.process(in_batch)
        results = self._batch_to_dict(out_batch)

        # Alice: 1 -> -1
        # Bob: -1 -> 1
        self.assertEqual(results.get((r_uint128(1), ("Alice", "30"))), -1)
        self.assertEqual(results.get((r_uint128(2), ("Bob", "40"))), 1)

if __name__ == '__main__':
    unittest.main()
