import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import strings, values

class TestGermanStrings(unittest.TestCase):
    def setUp(self):
        self.struct_buf1 = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        self.struct_buf2 = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        self.heap_buf = lltype.malloc(rffi.CCHARP.TO, 1024, flavor='raw')
        self.heap_off = 0

    def tearDown(self):
        lltype.free(self.struct_buf1, flavor='raw')
        lltype.free(self.struct_buf2, flavor='raw')
        lltype.free(self.heap_buf, flavor='raw')

    def _pack(self, buf, s):
        if len(s) <= strings.SHORT_STRING_THRESHOLD:
            strings.pack_string(buf, s, 0)
        else:
            off = self.heap_off
            for i in range(len(s)):
                self.heap_buf[off + i] = s[i]
            strings.pack_string(buf, s, off)
            self.heap_off += len(s)

    def test_german_structure_logic(self):
        """Verify inline vs heap packing at the 12-byte threshold."""
        # Exactly 12 bytes - should be inline
        s12 = "123456789012"
        self._pack(self.struct_buf1, s12)
        u32_ptr = rffi.cast(rffi.UINTP, self.struct_buf1)
        self.assertEqual(rffi.cast(lltype.Signed, u32_ptr[0]), 12)
        
        # 13 bytes - should be heap
        s13 = "1234567890123"
        self._pack(self.struct_buf2, s13)
        off_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.struct_buf2, 8))
        self.assertEqual(rffi.cast(lltype.Signed, off_ptr[0]), 0) # First heap alloc

    def test_equality_and_prefix_shortcircuit(self):
        """Verify O(1) equality failure via prefix and length."""
        self._pack(self.struct_buf1, "apple_sauce")
        self._pack(self.struct_buf2, "apple_juice")
        
        # Prefixes match ("appl"), but total string doesn't
        self.assertFalse(strings.string_equals_dual(self.struct_buf1, self.heap_buf, 
                                                   self.struct_buf2, self.heap_buf))
        
        # Length mismatch - instant False
        self._pack(self.struct_buf2, "apple")
        self.assertFalse(strings.string_equals_dual(self.struct_buf1, self.heap_buf, 
                                                   self.struct_buf2, self.heap_buf))

    def test_compare_db_value_to_german(self):
        """Verify the dry-run comparison used for SkipList/Engine searches."""
        self._pack(self.struct_buf1, "identical_payload")
        val = values.StringValue("identical_payload")
        
        # Should be 0 (Equal)
        self.assertEqual(strings.compare_db_value_to_german(val, self.struct_buf1, self.heap_buf), 0)
        
        # Lexicographical check
        val_lower = values.StringValue("abc")
        self._pack(self.struct_buf1, "def")
        # Structure (def) is > Value (abc) -> 1
        self.assertEqual(strings.compare_db_value_to_german(val_lower, self.struct_buf1, self.heap_buf), 1)

    def test_ordering_semantics(self):
        """Verify strict lexicographical ordering across inline/heap boundaries."""
        self._pack(self.struct_buf1, "abc")
        self._pack(self.struct_buf2, "abcd")
        self.assertEqual(strings.string_compare(self.struct_buf1, self.heap_buf, 
                                               self.struct_buf2, self.heap_buf), -1)

if __name__ == '__main__':
    unittest.main()
