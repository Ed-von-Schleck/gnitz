import os
import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, values as db_values, strings, strings as string_logic
from gnitz.storage import arena, writer_table, shard_table, comparator

class TestDataComparators(unittest.TestCase):
    def setUp(self):
        self.schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_I64),    # PK
            types.ColumnDefinition(types.TYPE_STRING), # Payload 1
            types.ColumnDefinition(types.TYPE_F64)     # Payload 2
        ], 0)
        self.arena = arena.Arena(1024)
        self.struct_buf1 = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        self.struct_buf2 = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        self.heap_buf = lltype.malloc(rffi.CCHARP.TO, 1024, flavor='raw')

    def tearDown(self):
        self.arena.free()
        lltype.free(self.struct_buf1, flavor='raw')
        lltype.free(self.struct_buf2, flavor='raw')
        lltype.free(self.heap_buf, flavor='raw')

    def _pack_aos(self, s_val, f_val):
        ptr = lltype.malloc(rffi.CCHARP.TO, self.schema.stride, flavor='raw')
        strings.pack_string(rffi.ptradd(ptr, self.schema.get_column_offset(1)), s_val, 0)
        rffi.cast(rffi.DOUBLEP, rffi.ptradd(ptr, self.schema.get_column_offset(2)))[0] = f_val
        return ptr

    def test_german_string_split_boundary(self):
        """Verify inline vs heap packing at the 12-byte threshold."""
        # Exactly 12 bytes - should be inline
        s12 = "123456789012"
        strings.pack_string(self.struct_buf1, s12, 0)
        u32_ptr = rffi.cast(rffi.UINTP, self.struct_buf1)
        self.assertEqual(rffi.cast(lltype.Signed, u32_ptr[0]), 12)
        
        # 13 bytes - should be heap (8-byte offset written at byte 8)
        s13 = "1234567890123"
        strings.pack_string(self.struct_buf2, s13, 999) # 999 is fake offset
        off_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.struct_buf2, 8))
        self.assertEqual(rffi.cast(lltype.Signed, off_ptr[0]), 999)

    def test_string_equality_logic(self):
        """Verify O(1) equality failure via prefix and length, and full content check."""
        strings.pack_string(self.struct_buf1, "apple_sauce", 0)
        strings.pack_string(self.struct_buf2, "apple_juice", 0)
        
        # Prefixes match ("appl"), but total string doesn't
        self.assertFalse(strings.string_equals_dual(self.struct_buf1, self.heap_buf, 
                                                   self.struct_buf2, self.heap_buf))
        
        # Exact match
        strings.pack_string(self.struct_buf2, "apple_sauce", 0)
        self.assertTrue(strings.string_equals_dual(self.struct_buf1, self.heap_buf, 
                                                  self.struct_buf2, self.heap_buf))

    def test_payload_ordering_and_equality(self):
        """Test lexicographical ordering and equality of packed AoS payloads."""
        p1 = self._pack_aos("apple", 1.0)
        p2 = self._pack_aos("banana", 1.0)
        p3 = self._pack_aos("apple", 1.0)
        
        # p1 < p2
        self.assertEqual(comparator.compare_payloads(self.schema, p1, None, p2, None), -1)
        # p1 == p3
        self.assertEqual(comparator.compare_payloads(self.schema, p1, None, p3, None), 0)
        
        lltype.free(p1, flavor='raw'); lltype.free(p2, flavor='raw'); lltype.free(p3, flavor='raw')

    def test_soa_to_soa_comparison(self):
        """Verify that rows in discrete columnar shards can be compared semantically."""
        fn1, fn2 = "comp_soa1.db", "comp_soa2.db"
        for fn in [fn1, fn2]:
            w = writer_table.TableShardWriter(self.schema)
            w.add_row_from_values(10, 1, [db_values.wrap("test"), db_values.wrap(1.5)])
            w.finalize(fn)
        
        v1 = shard_table.TableShardView(fn1, self.schema)
        v2 = shard_table.TableShardView(fn2, self.schema)
        
        # Rows are identical across shards
        self.assertEqual(comparator.compare_soa_rows(self.schema, v1, 0, v2, 0), 0)
        
        v1.close(); v2.close()
        if os.path.exists(fn1): os.unlink(fn1)
        if os.path.exists(fn2): os.unlink(fn2)

    def test_search_comparator_dbvalue(self):
        """Verify dry-run comparison: DBValue (search key) vs Packed German String."""
        s = "search_target"
        strings.pack_string(self.struct_buf1, s, 0)
        # FIXED: Initialize the heap buffer. For long strings (>12 chars), the 
        # comparator must read content from the heap to verify equality.
        for i in range(len(s)):
            self.heap_buf[i] = s[i]

        val = db_values.StringValue(s)
        self.assertEqual(string_logic.compare_db_value_to_german(val, self.struct_buf1, self.heap_buf), 0)
        
        val_lower = db_values.StringValue("abc")
        # "search_target" > "abc", result is 1
        self.assertEqual(string_logic.compare_db_value_to_german(val_lower, self.struct_buf1, self.heap_buf), 1)

if __name__ == '__main__':
    unittest.main()
