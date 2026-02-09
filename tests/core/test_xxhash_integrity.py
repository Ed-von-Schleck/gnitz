import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import checksum

class TestXXHashIntegrity(unittest.TestCase):
    def test_determinism(self):
        res1 = checksum.compute_checksum_bytes("GnitzDB")
        res2 = checksum.compute_checksum_bytes("GnitzDB")
        self.assertEqual(res1, res2)

    def test_stability(self):
        # Empty input hash
        res = checksum.compute_checksum_bytes("")
        self.assertNotEqual(res, 0)

    def test_raw_buffer_alignment(self):
        size = 100
        buf = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
        try:
            for i in range(size):
                buf[i] = chr(i % 256)
            
            c1 = checksum.compute_checksum(buf, 10)
            c2 = checksum.compute_checksum(buf, 11)
            self.assertNotEqual(c1, c2)
            
            ptr_offset = rffi.ptradd(buf, 1)
            c3 = checksum.compute_checksum(ptr_offset, 9)
            self.assertNotEqual(c1, c3)
        finally:
            lltype.free(buf, flavor='raw')

if __name__ == '__main__':
    unittest.main()
