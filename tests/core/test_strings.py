import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import strings

class TestGermanStrings(unittest.TestCase):
    def setUp(self):
        self.struct_buf = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        self.heap_buf = lltype.malloc(rffi.CCHARP.TO, 1024, flavor='raw')

    def tearDown(self):
        lltype.free(self.struct_buf, flavor='raw')
        lltype.free(self.heap_buf, flavor='raw')

    def _write_to_heap(self, s):
        for i in range(len(s)):
            self.heap_buf[i] = s[i]

    def _check(self, s_buf, h_buf, s):
        l = len(s)
        p = strings.compute_prefix(s)
        return strings.string_equals(s_buf, h_buf, s, l, p)

    def test_pack_short_string(self):
        s = "hello"
        strings.pack_string(self.struct_buf, s, 0)
        
        u32_ptr = rffi.cast(rffi.UINTP, self.struct_buf)
        self.assertEqual(u32_ptr[0], 5)
        
        unpacked = rffi.charpsize2str(rffi.ptradd(self.struct_buf, 4), 5)
        self.assertEqual(unpacked, "hello")

    def test_pack_long_string(self):
        s = "this is a long string"
        heap_offset = 128
        strings.pack_string(self.struct_buf, s, heap_offset)
        
        u32_ptr = rffi.cast(rffi.UINTP, self.struct_buf)
        u64_ptr = rffi.cast(rffi.ULONGLONGP, self.struct_buf)
        
        self.assertEqual(u32_ptr[0], len(s))
        prefix = rffi.charpsize2str(rffi.ptradd(self.struct_buf, 4), 4)
        self.assertEqual(prefix, "this")
        self.assertEqual(u64_ptr[1], heap_offset)

    def test_equality_short(self):
        s = "world"
        strings.pack_string(self.struct_buf, s, 0)
        self.assertTrue(self._check(self.struct_buf, self.heap_buf, "world"))
        self.assertFalse(self._check(self.struct_buf, self.heap_buf, "worlds"))
        self.assertFalse(self._check(self.struct_buf, self.heap_buf, "worl"))
        self.assertFalse(self._check(self.struct_buf, self.heap_buf, "planet"))

    def test_equality_long(self):
        s = "this is also very long"
        heap_offset = 0
        self._write_to_heap(s)
        strings.pack_string(self.struct_buf, s, heap_offset)
        
        self.assertTrue(self._check(self.struct_buf, self.heap_buf, s))
        self.assertFalse(self._check(self.struct_buf, self.heap_buf, "this is also very short"))
        self.assertFalse(self._check(self.struct_buf, self.heap_buf, "that is also very long"))

    def test_equality_prefix_optimization(self):
        s_long = "prefix_long_suffix"
        s_short = "prefix_short"
        
        self._write_to_heap(s_long)
        strings.pack_string(self.struct_buf, s_long, 0)
        self.assertFalse(self._check(self.struct_buf, self.heap_buf, "prefix_lorn_suffix"))

        strings.pack_string(self.struct_buf, s_short, 0)
        self.assertFalse(self._check(self.struct_buf, self.heap_buf, "prefix_shor"))

if __name__ == '__main__':
    unittest.main()
