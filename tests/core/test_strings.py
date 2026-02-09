import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import strings

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

    def test_equality(self):
        self._pack(self.struct_buf1, "hello world")
        self._pack(self.struct_buf2, "hello world")
        self.assertTrue(strings.string_equals_dual(self.struct_buf1, self.heap_buf, self.struct_buf2, self.heap_buf))
        
        self._pack(self.struct_buf2, "hello world!")
        self.assertFalse(strings.string_equals_dual(self.struct_buf1, self.heap_buf, self.struct_buf2, self.heap_buf))

    def test_compare_ordering(self):
        # Short vs Short
        self._pack(self.struct_buf1, "apple")
        self._pack(self.struct_buf2, "apply")
        self.assertEqual(strings.string_compare(self.struct_buf1, self.heap_buf, self.struct_buf2, self.heap_buf), -1)
        self.assertEqual(strings.string_compare(self.struct_buf2, self.heap_buf, self.struct_buf1, self.heap_buf), 1)

        # Long vs Long
        self._pack(self.struct_buf1, "this is string a")
        self._pack(self.struct_buf2, "this is string b")
        self.assertEqual(strings.string_compare(self.struct_buf1, self.heap_buf, self.struct_buf2, self.heap_buf), -1)

        # Mixed lengths
        self._pack(self.struct_buf1, "abc")
        self._pack(self.struct_buf2, "abcd")
        self.assertEqual(strings.string_compare(self.struct_buf1, self.heap_buf, self.struct_buf2, self.heap_buf), -1)

        # Equal
        self._pack(self.struct_buf1, "identical")
        self._pack(self.struct_buf2, "identical")
        self.assertEqual(strings.string_compare(self.struct_buf1, self.heap_buf, self.struct_buf2, self.heap_buf), 0)

    def test_compare_prefix_boundary(self):
        # Difference at index 3 (within prefix)
        self._pack(self.struct_buf1, "abcA")
        self._pack(self.struct_buf2, "abcB")
        self.assertEqual(strings.string_compare(self.struct_buf1, self.heap_buf, self.struct_buf2, self.heap_buf), -1)

        # Difference at index 4 (just after prefix, inline)
        self._pack(self.struct_buf1, "abcdE")
        self._pack(self.struct_buf2, "abcdF")
        self.assertEqual(strings.string_compare(self.struct_buf1, self.heap_buf, self.struct_buf2, self.heap_buf), -1)

if __name__ == '__main__':
    unittest.main()
