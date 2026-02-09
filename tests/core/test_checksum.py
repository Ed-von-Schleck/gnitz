import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import checksum

class TestChecksum(unittest.TestCase):
    def test_compute_checksum_deterministic(self):
        data = "test data for checksumming"
        c1 = checksum.compute_checksum_bytes(data)
        c2 = checksum.compute_checksum_bytes(data)
        self.assertEqual(c1, c2)

    def test_different_data_different_checksum(self):
        c1 = checksum.compute_checksum_bytes("data1")
        c2 = checksum.compute_checksum_bytes("data2")
        self.assertNotEqual(c1, c2)

    def test_empty_data(self):
        c = checksum.compute_checksum_bytes("")
        self.assertIsInstance(c, (int, long))

    def test_verify_checksum_pass(self):
        data = "verification test"
        buf = lltype.malloc(rffi.CCHARP.TO, len(data), flavor='raw')
        try:
            for i in range(len(data)):
                buf[i] = data[i]
            
            expected = checksum.compute_checksum(buf, len(data))
            self.assertTrue(checksum.verify_checksum(buf, len(data), expected))
        finally:
            lltype.free(buf, flavor='raw')

    def test_verify_checksum_fail(self):
        data = "verification test"
        buf = lltype.malloc(rffi.CCHARP.TO, len(data), flavor='raw')
        try:
            for i in range(len(data)):
                buf[i] = data[i]
            
            wrong_checksum = 0xDEADBEEF
            self.assertFalse(checksum.verify_checksum(buf, len(data), wrong_checksum))
        finally:
            lltype.free(buf, flavor='raw')

    def test_raw_pointer_checksum(self):
        data = "raw pointer test"
        buf = lltype.malloc(rffi.CCHARP.TO, len(data), flavor='raw')
        try:
            for i in range(len(data)):
                buf[i] = data[i]
            
            c = checksum.compute_checksum(buf, len(data))
            self.assertIsInstance(c, (int, long))
            self.assertNotEqual(c, 0)
        finally:
            lltype.free(buf, flavor='raw')

    def test_large_buffer(self):
        # Reduced from 1MB to 64KB to accommodate slow ll2ctypes overhead 
        # in untranslated tests while still verifying loop logic.
        size = 64 * 1024 
        buf = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
        try:
            for i in range(size):
                buf[i] = chr(i % 256)
            
            c = checksum.compute_checksum(buf, size)
            self.assertIsInstance(c, (int, long))
            self.assertNotEqual(c, 0)
        finally:
            lltype.free(buf, flavor='raw')

if __name__ == '__main__':
    unittest.main()
