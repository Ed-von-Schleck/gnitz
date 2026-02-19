# tests_data_comparators.py

import os
import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, r_uint64, r_int64
from gnitz.core import types, values as db_values, strings as string_logic, comparator as core_comparator
from gnitz.storage import buffer, writer_table, shard_table, comparator
from gnitz.storage.memtable_node import get_key_offset, node_get_payload_ptr

# Constants for mocking MemTable node structure
# Simplified structure for testing: [8:Weight][1:Height][3:Pad][4*MAX_HEIGHT:Pointers][16:PK_Aligned][Payload]
MOCK_NODE_HEIGHT = 1 # We only need level 0 for payload tests
MOCK_NODE_BASE_SIZE = get_key_offset(MOCK_NODE_HEIGHT) # Size up to key offset

class TestDataComparators(unittest.TestCase):
    def setUp(self):
        self.schema_i64 = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),    # Changed from I64 to U64
            types.ColumnDefinition(types.TYPE_STRING), 
            types.ColumnDefinition(types.TYPE_F64)     
        ], 0)
        
        self.schema_u128 = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128),   # PK
            types.ColumnDefinition(types.TYPE_STRING), # Payload 1
            types.ColumnDefinition(types.TYPE_U128)    # Payload 2
        ], 0)

        # Buffers for direct string_logic tests (not full nodes)
        self.struct_buf1 = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        self.struct_buf2 = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        self.heap_buf = lltype.malloc(rffi.CCHARP.TO, 2048, flavor='raw') # Larger heap buffer for long string tests
        self.heap_offset_counter = 0 # To simulate heap allocation
        
        # Initialize heap_buf to nulls
        for i in range(2048):
            self.heap_buf[i] = '\x00'

        # Tracking for cleanup
        self.temp_files = [] # For file paths (strings)
        self.temp_ptrs = []  # For raw pointers (to be freed)

    def tearDown(self):
        lltype.free(self.struct_buf1, flavor='raw')
        lltype.free(self.struct_buf2, flavor='raw')
        lltype.free(self.heap_buf, flavor='raw')
        
        # Free allocated temporary raw pointers
        for ptr in self.temp_ptrs:
            lltype.free(ptr, flavor='raw')

        # Remove temporary files
        for fn in self.temp_files:
            if os.path.exists(fn):
                os.unlink(fn)

    def _alloc_heap_blob(self, s_val):
        """Helper to simulate blob allocation and return its offset."""
        length = len(s_val)
        current_offset = self.heap_offset_counter
        if current_offset + length > 2048:
            raise MemoryError("Heap buffer exhausted for test")
        
        for i in range(length):
            self.heap_buf[current_offset + i] = s_val[i]
        
        self.heap_offset_counter += length
        return current_offset

    def _pack_aos_row_data(self, schema, s_val, f_val_or_u128_val, heap_ptr, is_u128_col=False):
        """Helper to create a packed payload within a mock MemTable node structure."""
        key_size = schema.get_pk_column().field_type.size
        payload_offset = MOCK_NODE_BASE_SIZE + key_size
        total_node_size = payload_offset + schema.memtable_stride
        
        ptr = lltype.malloc(rffi.CCHARP.TO, total_node_size, flavor='raw')
        # Initialize to zero
        for i in range(total_node_size): ptr[i] = '\x00'

        # Set mock node height (required for get_key_offset in PackedNodeAccessor)
        ptr[8] = chr(MOCK_NODE_HEIGHT)

        # Simulate heap allocation if string is long
        heap_offset = 0
        if len(s_val) > string_logic.SHORT_STRING_THRESHOLD:
            heap_offset = self._alloc_heap_blob(s_val)
        
        # Pack string
        string_logic.pack_string(
            rffi.ptradd(ptr, payload_offset + schema.get_column_offset(1)), 
            s_val, 
            heap_offset
        )
        
        # Pack float or u128
        target_col_ptr = rffi.ptradd(ptr, payload_offset + schema.get_column_offset(2))
        if is_u128_col:
            r_val = r_uint128(f_val_or_u128_val)
            rffi.cast(rffi.ULONGLONGP, target_col_ptr)[0] = r_uint64(r_val)
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target_col_ptr, 8))[0] = r_uint64(r_val >> 64)
        else:
            rffi.cast(rffi.DOUBLEP, target_col_ptr)[0] = rffi.cast(rffi.DOUBLE, f_val_or_u128_val)
            
        return ptr, payload_offset # Return node ptr and offset to payload

    def test_german_string_split_boundary(self):
        """Verify inline vs heap packing at the 12-byte threshold."""
        s12 = "123456789012"
        string_logic.pack_string(self.struct_buf1, s12, 0)
        u32_ptr = rffi.cast(rffi.UINTP, self.struct_buf1)
        self.assertEqual(rffi.cast(lltype.Signed, u32_ptr[0]), 12) # Length
        
        # Payload (bytes 8-15) should contain suffix starting from index 4.
        # "1234" is prefix (indices 0-3). Index 4 is '5'.
        self.assertEqual(self.struct_buf1[8], '5') 
        
        s13 = "1234567890123"
        string_logic.pack_string(self.struct_buf2, s13, 999) # 999 is fake offset
        off_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.struct_buf2, 8))
        self.assertEqual(rffi.cast(lltype.Signed, off_ptr[0]), 999) # Should be heap offset

    def test_unpack_string_roundtrip(self):
        """Verify packing to and unpacking from German String format, short and long."""
        short_s = "short_str"
        long_s = "this is a very long string that definitely exceeds the short string threshold of 12 bytes"

        # Test short string
        string_logic.pack_string(self.struct_buf1, short_s, 0)
        unpacked_short = string_logic.unpack_string(self.struct_buf1, self.heap_buf)
        self.assertEqual(unpacked_short, short_s)

        # Test long string
        heap_offset_long = self._alloc_heap_blob(long_s)
        string_logic.pack_string(self.struct_buf2, long_s, heap_offset_long)
        unpacked_long = string_logic.unpack_string(self.struct_buf2, self.heap_buf)
        self.assertEqual(unpacked_long, long_s)
        
        # Test empty string
        empty_s = ""
        string_logic.pack_string(self.struct_buf1, empty_s, 0)
        unpacked_empty = string_logic.unpack_string(self.struct_buf1, self.heap_buf)
        self.assertEqual(unpacked_empty, empty_s)

    def test_string_equality_logic_unified(self):
        """Verify string comparison via the new unified compare_structures kernel."""
        s1 = "apple_sauce"
        s2 = "apple_juice"
        s3 = "apple_sauce"
        s4 = "apple" # shorter, same prefix

        # Pack s1, s2, s3, s4 into the test heap for consistent offsets
        off1 = self._alloc_heap_blob(s1)
        string_logic.pack_string(self.struct_buf1, s1, off1)

        off2 = self._alloc_heap_blob(s2)
        string_logic.pack_string(self.struct_buf2, s2, off2)

        # Use struct_buf1 for s3 to get same struct pointer, but different physical heap offset (not tested here)
        # For this test, we care about the content being compared via `compare_structures`
        off3 = self._alloc_heap_blob(s3) 
        struct_buf3 = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        string_logic.pack_string(struct_buf3, s3, off3)
        self.temp_ptrs.append(struct_buf3) # Track for cleanup

        off4 = self._alloc_heap_blob(s4)
        struct_buf4 = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        string_logic.pack_string(struct_buf4, s4, off4)
        self.temp_ptrs.append(struct_buf4) # Track for cleanup

        u32_p1 = rffi.cast(rffi.UINTP, self.struct_buf1)
        len1, pref1 = rffi.cast(lltype.Signed, u32_p1[0]), rffi.cast(lltype.Signed, u32_p1[1])
        u32_p2 = rffi.cast(rffi.UINTP, self.struct_buf2)
        len2, pref2 = rffi.cast(lltype.Signed, u32_p2[0]), rffi.cast(lltype.Signed, u32_p2[1])
        u32_p3 = rffi.cast(rffi.UINTP, struct_buf3)
        len3, pref3 = rffi.cast(lltype.Signed, u32_p3[0]), rffi.cast(lltype.Signed, u32_p3[1])
        u32_p4 = rffi.cast(rffi.UINTP, struct_buf4)
        len4, pref4 = rffi.cast(lltype.Signed, u32_p4[0]), rffi.cast(lltype.Signed, u32_p4[1])

        # "apple_sauce" > "apple_juice", result should be 1
        self.assertEqual(string_logic.compare_structures(
            len1, pref1, self.struct_buf1, self.heap_buf, None,
            len2, pref2, self.struct_buf2, self.heap_buf, None
        ), 1)
        
        # Test for exact match
        self.assertEqual(string_logic.compare_structures(
            len1, pref1, self.struct_buf1, self.heap_buf, None,
            len3, pref3, struct_buf3, self.heap_buf, None
        ), 0)

        # Test shorter prefix match: "apple_sauce" > "apple"
        self.assertEqual(string_logic.compare_structures(
            len1, pref1, self.struct_buf1, self.heap_buf, None,
            len4, pref4, struct_buf4, self.heap_buf, None
        ), 1)
        # Test reverse: "apple" < "apple_sauce"
        self.assertEqual(string_logic.compare_structures(
            len4, pref4, struct_buf4, self.heap_buf, None,
            len1, pref1, self.struct_buf1, self.heap_buf, None
        ), -1)

    def test_string_prefix_collision_and_ordering(self):
        """Test cases for strings that share prefixes but differ later, or have different lengths."""
        s_base = "apple"
        s_long = "applesauce"
        s_diff_char = "applf" # differs at 5th char

        off_base = self._alloc_heap_blob(s_base)
        string_logic.pack_string(self.struct_buf1, s_base, off_base)
        len1, pref1 = rffi.cast(rffi.UINTP, self.struct_buf1)[0], rffi.cast(rffi.UINTP, self.struct_buf1)[1]

        off_long = self._alloc_heap_blob(s_long)
        struct_buf_long = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        string_logic.pack_string(struct_buf_long, s_long, off_long)
        len2, pref2 = rffi.cast(rffi.UINTP, struct_buf_long)[0], rffi.cast(rffi.UINTP, struct_buf_long)[1]
        self.temp_ptrs.append(struct_buf_long)

        off_diff_char = self._alloc_heap_blob(s_diff_char)
        struct_buf_diff_char = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        string_logic.pack_string(struct_buf_diff_char, s_diff_char, off_diff_char)
        len3, pref3 = rffi.cast(rffi.UINTP, struct_buf_diff_char)[0], rffi.cast(rffi.UINTP, struct_buf_diff_char)[1]
        self.temp_ptrs.append(struct_buf_diff_char)

        # "apple" < "applesauce"
        self.assertEqual(string_logic.compare_structures(len1, pref1, self.struct_buf1, self.heap_buf, None,
                                                         len2, pref2, struct_buf_long, self.heap_buf, None), -1)
        # "apple" < "applf" (e < f at char 5)
        self.assertEqual(string_logic.compare_structures(len1, pref1, self.struct_buf1, self.heap_buf, None,
                                                         len3, pref3, struct_buf_diff_char, self.heap_buf, None), -1)
        # "applesauce" < "applf" ('e' < 'f' at char 5)
        self.assertEqual(string_logic.compare_structures(len2, pref2, struct_buf_long, self.heap_buf, None,
                                                         len3, pref3, struct_buf_diff_char, self.heap_buf, None), -1)


    def test_payload_ordering_and_equality_unified(self):
        """Test lexicographical ordering of packed AoS payloads via unified comparator."""
        p1_node, p1_offset = self._pack_aos_row_data(self.schema_i64, "apple", 1.0, self.heap_buf)
        p2_node, p2_offset = self._pack_aos_row_data(self.schema_i64, "banana", 1.0, self.heap_buf)
        p3_node, p3_offset = self._pack_aos_row_data(self.schema_i64, "apple", 1.0, self.heap_buf)
        
        self.temp_ptrs.extend([p1_node, p2_node, p3_node]) # Track pointers

        acc1 = comparator.PackedNodeAccessor(self.schema_i64, self.heap_buf)
        acc2 = comparator.PackedNodeAccessor(self.schema_i64, self.heap_buf)

        # p1 < p2 should be -1
        acc1.set_row(p1_node, 0) # Node is at offset 0 relative to itself
        acc2.set_row(p2_node, 0)
        self.assertEqual(core_comparator.compare_rows(self.schema_i64, acc1, acc2), -1)
        
        # p1 == p3 should be 0
        acc2.set_row(p3_node, 0)
        self.assertEqual(core_comparator.compare_rows(self.schema_i64, acc1, acc2), 0)

    def test_soa_to_soa_comparison_unified(self):
        """Verify row comparison in columnar shards via unified comparator."""
        fn1, fn2 = "comp_soa1.db", "comp_soa2.db"
        self.temp_files.extend([fn1, fn2])

        w1 = writer_table.TableShardWriter(self.schema_i64)
        w1.add_row_from_values(10, 1, [db_values.TaggedValue.make_string("test_long_string_in_soa_A"), db_values.TaggedValue.make_float(1.5)])
        w1.finalize(fn1)
        
        w2 = writer_table.TableShardWriter(self.schema_i64)
        w2.add_row_from_values(10, 1, [db_values.TaggedValue.make_string("test_long_string_in_soa_B"), db_values.TaggedValue.make_float(1.5)])
        w2.finalize(fn2)
        
        v1 = shard_table.TableShardView(fn1, self.schema_i64)
        v2 = shard_table.TableShardView(fn2, self.schema_i64)
        
        acc1 = comparator.SoAAccessor(self.schema_i64)
        acc2 = comparator.SoAAccessor(self.schema_i64)
        acc1.set_row(v1, 0)
        acc2.set_row(v2, 0)

        # Should be -1 because "A" < "B"
        self.assertEqual(core_comparator.compare_rows(self.schema_i64, acc1, acc2), -1)
        
        v1.close(); v2.close()

    def test_search_comparator_value_vs_packed(self):
        """Verify unified comparison: DBValue list vs Packed AoS Row."""
        s_target = "search_target_long_string"
        f_target = 42.123
        
        # Create a mock packed node for PackedNodeAccessor
        packed_node, _ = self._pack_aos_row_data(self.schema_i64, s_target, f_target, self.heap_buf)
        self.temp_ptrs.append(packed_node)

        values_eq = [db_values.TaggedValue.make_string(s_target), db_values.TaggedValue.make_float(f_target)]
        values_lt = [db_values.TaggedValue.make_string("abc"), db_values.TaggedValue.make_float(f_target)]
        values_gt = [db_values.TaggedValue.make_string("xyz"), db_values.TaggedValue.make_float(f_target)]

        acc_packed = comparator.PackedNodeAccessor(self.schema_i64, self.heap_buf)
        acc_val = comparator.ValueAccessor(self.schema_i64)
        
        # Set node to 0 offset of itself
        acc_packed.set_row(packed_node, 0) 
        
        # Test for equality
        acc_val.set_row(values_eq)
        self.assertEqual(core_comparator.compare_rows(self.schema_i64, acc_packed, acc_val), 0)
        
        # Test for inequality (packed > value)
        acc_val.set_row(values_lt)
        self.assertEqual(core_comparator.compare_rows(self.schema_i64, acc_packed, acc_val), 1)
        
        # Test for inequality (value < packed)
        self.assertEqual(core_comparator.compare_rows(self.schema_i64, acc_val, acc_packed), -1)

        # Test for inequality (packed < value)
        acc_val.set_row(values_gt)
        self.assertEqual(core_comparator.compare_rows(self.schema_i64, acc_packed, acc_val), -1)
        
        # Test for inequality (value > packed)
        self.assertEqual(core_comparator.compare_rows(self.schema_i64, acc_val, acc_packed), 1)

    def test_u128_lexicographical_ordering(self):
        """Verify 128-bit integer comparison across accessors."""
        u128_1 = r_uint128(100)
        u128_2 = r_uint128(200)
        u128_3 = r_uint128(150)
        u128_max = r_uint128(-1) # All ones for 128 bit

        fn = "u128_test.db"
        self.temp_files.append(fn)

        # Create a shard with u128 PK and a u128 column
        w = writer_table.TableShardWriter(self.schema_u128)
        w.add_row_from_values(u128_1, 1, [
            db_values.TaggedValue.make_string("valA"), 
            db_values.TaggedValue.make_u128(r_uint64(u128_max), r_uint64(u128_max >> 64))
        ])
        w.add_row_from_values(u128_2, 1, [
            db_values.TaggedValue.make_string("valB"), 
            db_values.TaggedValue.make_u128(r_uint64(u128_2), r_uint64(u128_2 >> 64))
        ])
        w.finalize(fn)

        v = shard_table.TableShardView(fn, self.schema_u128)
        acc_soa = comparator.SoAAccessor(self.schema_u128)
        acc_val = comparator.ValueAccessor(self.schema_u128)

        # Compare SoA row 0 (PK: 100, Col2: MAX_U128) vs Value (PK: not used, Col2: 150)
        acc_soa.set_row(v, 0)
        acc_val.set_row([
            db_values.TaggedValue.make_string("valA"), 
            db_values.TaggedValue.make_u128(r_uint64(u128_3), r_uint64(u128_3 >> 64))
        ])
        # The string "valA" will match. Comparison moves to u128 column.
        # MAX_U128 > 150, so SoA > Value -> result 1
        self.assertEqual(core_comparator.compare_rows(self.schema_u128, acc_soa, acc_val), 1)

        # Compare SoA row 1 (PK: 200, Col2: 200) vs Value (PK: not used, Col2: 150)
        acc_soa.set_row(v, 1)
        # "valB" > "valA" already, so first column mismatch. SoA > Value -> result 1
        self.assertEqual(core_comparator.compare_rows(self.schema_u128, acc_soa, acc_val), 1)
        
        v.close()

    def test_cross_shard_content_equality_long_strings(self):
        """
        Verify that identical long strings at different heap offsets 
        in different shards compare as equal.
        """
        fn_a, fn_b = "long_str_shard_a.db", "long_str_shard_b.db"
        self.temp_files.extend([fn_a, fn_b])

        shared_long_string = "this is a very long string that will be deduplicated by the writer but still compare correctly across shards"

        # Writer A
        w_a = writer_table.TableShardWriter(self.schema_i64)
        w_a.add_row_from_values(1, 1, [db_values.TaggedValue.make_string(shared_long_string), db_values.TaggedValue.make_float(10.0)])
        w_a.finalize(fn_a)

        # Writer B (same string)
        w_b = writer_table.TableShardWriter(self.schema_i64)
        w_b.add_row_from_values(1, 1, [db_values.TaggedValue.make_string(shared_long_string), db_values.TaggedValue.make_float(10.0)])
        w_b.finalize(fn_b)

        # Load views
        v_a = shard_table.TableShardView(fn_a, self.schema_i64)
        v_b = shard_table.TableShardView(fn_b, self.schema_i64)

        # Check that blob buffer pointers are different (meaning different physical offsets)
        self.assertNotEqual(rffi.cast(lltype.Signed, v_a.blob_buf.ptr), rffi.cast(lltype.Signed, v_b.blob_buf.ptr))
        
        acc_a = comparator.SoAAccessor(self.schema_i64)
        acc_b = comparator.SoAAccessor(self.schema_i64)

        acc_a.set_row(v_a, 0)
        acc_b.set_row(v_b, 0)

        # The core test: core_comparator.compare_rows should return 0 despite different blob_buf.ptr
        self.assertEqual(core_comparator.compare_rows(self.schema_i64, acc_a, acc_b), 0)

        v_a.close(); v_b.close()

if __name__ == '__main__':
    unittest.main()
