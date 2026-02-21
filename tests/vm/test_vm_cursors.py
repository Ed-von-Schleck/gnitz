# test_vm_cursors.py

import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, r_int64
from gnitz.core import types, strings
from gnitz.storage.table import PersistentTable
from tests.row_helpers import create_test_row

class TestVMCursors(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_vm_cursors_env"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.mkdir(self.test_dir)
        
        # Schema: PK (U128) | String | Int64
        self.schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_I64)
        ], pk_index=0)
        
        self.db = PersistentTable(self.test_dir, "test_trace", self.schema)

    def tearDown(self):
        if not self.db.is_closed:
            self.db.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def _mk_payload(self, name, score):
        """Helper to create a PayloadRow (excluding PK)."""
        return create_test_row(self.schema, [name, score])

    def _extract_name(self, cursor):
        """
        Extracts the string column (Schema Index 1) using the RowAccessor API.
        This works for both MemTable (AoS) and Shard (SoA) backends.
        """
        accessor = cursor.get_accessor()
        # RowAccessor.get_str_struct returns (length, prefix, struct_ptr, heap_ptr, py_str)
        res = accessor.get_str_struct(1)
        return strings.resolve_string(res[2], res[3], res[4])

    def test_memtable_traversal(self):
        self.db.insert(r_uint128(10), self._mk_payload("Alice", 100))
        self.db.insert(r_uint128(20), self._mk_payload("Bob", 200))
        
        cursor = self.db.create_cursor()
        cursor.seek(r_uint128(0))
        
        self.assertTrue(cursor.is_valid())
        self.assertEqual(cursor.key(), r_uint128(10))
        self.assertEqual(cursor.weight(), r_int64(1))
        self.assertEqual(self._extract_name(cursor), "Alice")
        
        cursor.advance() 
        self.assertTrue(cursor.is_valid())
        self.assertEqual(cursor.key(), r_uint128(20))
        self.assertEqual(self._extract_name(cursor), "Bob")
        
        cursor.advance()
        self.assertFalse(cursor.is_valid())

    def test_multiset_cursor_logic(self):
        pk = r_uint128(99)
        # Multiset: Same PK, different payloads
        self.db.insert(pk, self._mk_payload("Version_A", 1))
        self.db.insert(pk, self._mk_payload("Version_B", 2))
        
        cursor = self.db.create_cursor()
        cursor.seek(pk)
        
        count = 0
        while cursor.is_valid() and cursor.key() == pk:
            count += 1
            cursor.advance()
        
        self.assertEqual(count, 2)

    def test_ghost_annihilation_cross_layer(self):
        pk = r_uint128(500)
        payload = self._mk_payload("Ghost", 0)
        
        # Insert then retract
        self.db.insert(pk, payload)
        self.db.flush() # Force to disk (Shard)
        self.db.remove(pk, payload) # Exists as retraction in MemTable
        
        cursor = self.db.create_cursor()
        cursor.seek(pk)
        
        # UnifiedCursor should algebraically sum the Shard (+1) and MemTable (-1)
        # to produce a weight of 0, then skip it (The Ghost Property).
        if cursor.is_valid() and cursor.key() == pk:
            self.fail("Cursor should have annihilated the zero-weight ghost")

    def test_unified_merge_summation(self):
        pk = r_uint128(777)
        payload = self._mk_payload("Accumulator", 1)
        
        # Spread weight across layers
        self.db.insert(pk, payload)
        self.db.flush() # +1 in Shard
        self.db.insert(pk, payload)
        self.db.insert(pk, payload) # +2 in MemTable
        
        cursor = self.db.create_cursor()
        cursor.seek(pk)
        
        self.assertTrue(cursor.is_valid())
        self.assertEqual(cursor.key(), pk)
        self.assertEqual(cursor.weight(), r_int64(3))

    def test_seek_boundary_logic(self):
        p = self._mk_payload("data", 0)
        self.db.insert(r_uint128(100), p)
        self.db.insert(r_uint128(200), p)
        self.db.insert(r_uint128(300), p)
        self.db.flush()
        
        cursor = self.db.create_cursor()
        
        # Exact match
        cursor.seek(r_uint128(200))
        self.assertEqual(cursor.key(), r_uint128(200))
        
        # Seek into gap
        cursor.seek(r_uint128(150))
        self.assertEqual(cursor.key(), r_uint128(200))
        
        # Seek before start
        cursor.seek(r_uint128(10))
        self.assertEqual(cursor.key(), r_uint128(100))
        
        # Seek past end
        cursor.seek(r_uint128(500))
        self.assertFalse(cursor.is_valid())

    def test_u128_sorting_integrity(self):
        # Large keys with high bits set
        k_low = (r_uint128(1) << 64) | r_uint128(0xFF)
        k_high = (r_uint128(2) << 64) | r_uint128(0x00)
        
        p = self._mk_payload("sort", 0)
        self.db.insert(k_high, p)
        self.db.insert(k_low, p)
        self.db.flush()
        
        cursor = self.db.create_cursor()
        cursor.seek(r_uint128(0))
        
        self.assertEqual(cursor.key(), k_low)
        cursor.advance()
        self.assertEqual(cursor.key(), k_high)

if __name__ == '__main__':
    unittest.main()
