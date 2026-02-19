# test_vm_cursors.py

import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, r_int64
from gnitz.core import types, values
from gnitz.storage.table import PersistentTable

class TestVMCursors(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_vm_cursors_env"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.mkdir(self.test_dir)
        
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
        return [
            values.TaggedValue.make_string(name),
            values.TaggedValue.make_int(score)
        ]

    def _extract_name(self, cursor):
        """
        Extracts the string column using the row_logic API.
        Note: UnifiedCursor yields an accessor for data extraction.
        """
        accessor = cursor.get_accessor()
        meta = accessor.get_str_struct(1)
        if meta[4] is not None:
            return meta[4]
        from gnitz.core import strings
        return strings.unpack_string(meta[2], meta[3])

    def test_memtable_traversal(self):
        self.db.insert(10, self._mk_payload("Alice", 100))
        self.db.insert(20, self._mk_payload("Bob", 200))
        
        cursor = self.db.create_cursor()
        cursor.seek(r_uint128(0))
        
        self.assertTrue(cursor.is_valid())
        self.assertEqual(cursor.key(), r_uint128(10))
        self.assertEqual(cursor.weight(), r_int64(1))
        self.assertEqual(self._extract_name(cursor), "Alice")
        
        cursor.advance() # Corrected from next()
        self.assertTrue(cursor.is_valid())
        self.assertEqual(cursor.key(), r_uint128(20))
        self.assertEqual(self._extract_name(cursor), "Bob")
        
        cursor.advance()
        self.assertFalse(cursor.is_valid())

    def test_multiset_cursor_logic(self):
        pk = r_uint128(99)
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
        
        self.db.insert(pk, payload)
        self.db.flush()
        self.db.remove(pk, payload)
        
        cursor = self.db.create_cursor()
        cursor.seek(pk)
        
        # Should be empty or pointing to next available key > 500
        if cursor.is_valid() and cursor.key() == pk:
            self.fail("Cursor should have annihilated the zero-weight ghost")

    def test_unified_merge_summation(self):
        pk = r_uint128(777)
        payload = self._mk_payload("Accumulator", 1)
        
        self.db.insert(pk, payload)
        self.db.flush()
        self.db.insert(pk, payload)
        self.db.insert(pk, payload)
        
        cursor = self.db.create_cursor()
        cursor.seek(pk)
        
        self.assertTrue(cursor.is_valid())
        self.assertEqual(cursor.key(), pk)
        self.assertEqual(cursor.weight(), r_int64(3))

    def test_seek_boundary_logic(self):
        p = self._mk_payload("data", 0)
        self.db.insert(100, p)
        self.db.insert(200, p)
        self.db.insert(300, p)
        self.db.flush()
        
        cursor = self.db.create_cursor()
        
        cursor.seek(r_uint128(200))
        self.assertEqual(cursor.key(), r_uint128(200))
        
        cursor.seek(r_uint128(150))
        self.assertEqual(cursor.key(), r_uint128(200))
        
        cursor.seek(r_uint128(10))
        self.assertEqual(cursor.key(), r_uint128(100))
        
        cursor.seek(r_uint128(500))
        self.assertFalse(cursor.is_valid())

    def test_u128_sorting_integrity(self):
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
