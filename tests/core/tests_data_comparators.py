# tests_data_comparators.py

import os
import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, values as db_values, strings as string_logic
from gnitz.storage import buffer, writer_table, shard_table, comparator

class TestDataComparators(unittest.TestCase):
    def setUp(self):
        self.schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_I64),    # PK
            types.ColumnDefinition(types.TYPE_STRING), # Payload 1
            types.ColumnDefinition(types.TYPE_F64)     # Payload 2
        ], 0)
        self.struct_buf1 = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        self.struct_buf2 = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        self.heap_buf = lltype.malloc(rffi.CCHARP.TO, 1024, flavor='raw')

        # This shim is needed for tests that deal with raw payload pointers
        # instead of full SkipList nodes.
        class AbsolutePackedAccessor(comparator.PackedNodeAccessor):
            def set_row(self, payload_ptr, ignored=0):
                self.payload_ptr = payload_ptr
        self.packed_accessor_shim = AbsolutePackedAccessor

    def tearDown(self):
        lltype.free(self.struct_buf1, flavor='raw')
        lltype.free(self.struct_buf2, flavor='raw')
        lltype.free(self.heap_buf, flavor='raw')

    def _pack_aos_with_heap(self, s_val, f_val, heap_ptr):
        """Helper to create a packed payload, handling long string relocation."""
        ptr = lltype.malloc(rffi.CCHARP.TO, self.schema.stride, flavor='raw')
        
        heap_offset = 0
        if len(s_val) > string_logic.SHORT_STRING_THRESHOLD:
            for i in range(len(s_val)):
                heap_ptr[i] = s_val[i]
            # In a real scenario, this offset would come from a blob arena allocator
            heap_offset = 0 
        
        string_logic.pack_string(
            rffi.ptradd(ptr, self.schema.get_column_offset(1)), 
            s_val, 
            heap_offset
        )
        rffi.cast(rffi.DOUBLEP, rffi.ptradd(ptr, self.schema.get_column_offset(2)))[0] = f_val
        return ptr

    def test_german_string_split_boundary(self):
        """Verify inline vs heap packing at the 12-byte threshold."""
        s12 = "123456789012"
        string_logic.pack_string(self.struct_buf1, s12, 0)
        u32_ptr = rffi.cast(rffi.UINTP, self.struct_buf1)
        self.assertEqual(rffi.cast(lltype.Signed, u32_ptr[0]), 12)
        
        s13 = "1234567890123"
        string_logic.pack_string(self.struct_buf2, s13, 999) # 999 is fake offset
        off_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.struct_buf2, 8))
        self.assertEqual(rffi.cast(lltype.Signed, off_ptr[0]), 999)

    def test_string_equality_logic_unified(self):
        """Verify string comparison via the new unified compare_structures kernel."""
        s1 = "apple_sauce" # long string
        s2 = "apple_juice" # long string
        
        for i in range(len(s1)): self.heap_buf[i] = s1[i]
        string_logic.pack_string(self.struct_buf1, s1, 0)

        for i in range(len(s2)): self.heap_buf[i+20] = s2[i] # use different heap area
        string_logic.pack_string(self.struct_buf2, s2, 20)

        u32_p1 = rffi.cast(rffi.UINTP, self.struct_buf1)
        len1, pref1 = rffi.cast(lltype.Signed, u32_p1[0]), rffi.cast(lltype.Signed, u32_p1[1])
        u32_p2 = rffi.cast(rffi.UINTP, self.struct_buf2)
        len2, pref2 = rffi.cast(lltype.Signed, u32_p2[0]), rffi.cast(lltype.Signed, u32_p2[1])

        # "apple_sauce" > "apple_juice", result should be 1
        self.assertEqual(string_logic.compare_structures(
            len1, pref1, self.struct_buf1, self.heap_buf, None,
            len2, pref2, self.struct_buf2, self.heap_buf, None
        ), 1)
        
        # Test for exact match
        string_logic.pack_string(self.struct_buf2, s1, 0)
        u32_p2 = rffi.cast(rffi.UINTP, self.struct_buf2)
        len2, pref2 = rffi.cast(lltype.Signed, u32_p2[0]), rffi.cast(lltype.Signed, u32_p2[1])
        self.assertEqual(string_logic.compare_structures(
            len1, pref1, self.struct_buf1, self.heap_buf, None,
            len2, pref2, self.struct_buf2, self.heap_buf, None
        ), 0)

    def test_payload_ordering_and_equality_unified(self):
        """Test lexicographical ordering of packed AoS payloads via unified comparator."""
        p1 = self._pack_aos_with_heap("apple", 1.0, self.heap_buf)
        p2 = self._pack_aos_with_heap("banana", 1.0, self.heap_buf)
        p3 = self._pack_aos_with_heap("apple", 1.0, self.heap_buf)
        
        acc1 = self.packed_accessor_shim(self.schema, self.heap_buf)
        acc2 = self.packed_accessor_shim(self.schema, self.heap_buf)

        # p1 < p2 should be -1
        acc1.set_row(p1)
        acc2.set_row(p2)
        self.assertEqual(comparator.compare_rows(self.schema, acc1, acc2), -1)
        
        # p1 == p3 should be 0
        acc2.set_row(p3)
        self.assertEqual(comparator.compare_rows(self.schema, acc1, acc2), 0)
        
        lltype.free(p1, flavor='raw'); lltype.free(p2, flavor='raw'); lltype.free(p3, flavor='raw')

    def test_soa_to_soa_comparison_unified(self):
        """Verify row comparison in columnar shards via unified comparator."""
        fn1, fn2 = "comp_soa1.db", "comp_soa2.db"
        for fn in [fn1, fn2]:
            w = writer_table.TableShardWriter(self.schema)
            w.add_row_from_values(10, 1, [db_values.TaggedValue.make_string("test_long_string_in_soa"), db_values.TaggedValue.make_float(1.5)])
            w.finalize(fn)
        
        v1 = shard_table.TableShardView(fn1, self.schema)
        v2 = shard_table.TableShardView(fn2, self.schema)
        
        acc1 = comparator.SoAAccessor(self.schema)
        acc2 = comparator.SoAAccessor(self.schema)
        acc1.set_row(v1, 0)
        acc2.set_row(v2, 0)

        self.assertEqual(comparator.compare_rows(self.schema, acc1, acc2), 0)
        
        v1.close(); v2.close()
        if os.path.exists(fn1): os.unlink(fn1)
        if os.path.exists(fn2): os.unlink(fn2)

    def test_search_comparator_value_vs_packed(self):
        """Verify unified comparison: DBValue list vs Packed AoS Row."""
        s_target = "search_target_long"
        f_target = 42.123
        
        packed_row = self._pack_aos_with_heap(s_target, f_target, self.heap_buf)
        values_eq = [db_values.TaggedValue.make_string(s_target), db_values.TaggedValue.make_float(f_target)]
        values_lt = [db_values.TaggedValue.make_string("abc"), db_values.TaggedValue.make_float(f_target)]

        acc_packed = self.packed_accessor_shim(self.schema, self.heap_buf)
        acc_val = comparator.ValueAccessor(self.schema)
        
        # Test for equality
        acc_packed.set_row(packed_row)
        acc_val.set_row(values_eq)
        self.assertEqual(comparator.compare_rows(self.schema, acc_packed, acc_val), 0)
        
        # Test for inequality (packed > value)
        acc_val.set_row(values_lt)
        self.assertEqual(comparator.compare_rows(self.schema, acc_packed, acc_val), 1)
        
        # Test for inequality (value < packed)
        self.assertEqual(comparator.compare_rows(self.schema, acc_val, acc_packed), -1)

        lltype.free(packed_row, flavor='raw')

if __name__ == '__main__':
    unittest.main()
