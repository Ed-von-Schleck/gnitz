import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import checksum

class TestChecksum(unittest.TestCase):
    def test_determinism_and_stability(self):
        """Verify XXH3-64 scalar returns consistent results for strings."""
        data = "GnitzDB_Theoretical_Foundation"
        c1 = checksum.compute_checksum_bytes(data)
        c2 = checksum.compute_checksum_bytes(data)
        self.assertEqual(c1, c2)
        self.assertNotEqual(c1, 0)
        
        # Verify different data produces different hash
        self.assertNotEqual(c1, checksum.compute_checksum_bytes("GnitzDB_Practical_Foundation"))

    def test_empty_and_small_data(self):
        """Verify hash handles minimal lengths and zero-length inputs."""
        c_empty = checksum.compute_checksum_bytes("")
        self.assertNotEqual(c_empty, 0)
        
        # Test 1-byte boundary
        c1 = checksum.compute_checksum_bytes("a")
        c2 = checksum.compute_checksum_bytes("b")
        self.assertNotEqual(c1, c2)

    def test_raw_pointer_and_alignment(self):
        """Verify checksumming raw RPython memory buffers."""
        size = 128
        buf = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
        try:
            for i in range(size):
                buf[i] = chr(i % 256)
            
            # Checksum a sub-region
            c1 = checksum.compute_checksum(buf, 64)
            self.assertTrue(checksum.verify_checksum(buf, 64, c1))
            
            # Checksum with an offset (simulating reading a field from a shard)
            ptr_offset = rffi.ptradd(buf, 10)
            c2 = checksum.compute_checksum(ptr_offset, 64)
            self.assertNotEqual(c1, c2)
            self.assertTrue(checksum.verify_checksum(ptr_offset, 64, c2))
        finally:
            lltype.free(buf, flavor='raw')

    def test_large_buffer_loop_logic(self):
        """Ensure the internal loop logic handles buffers exceeding stripe length."""
        size = 1024 * 2 # Exceeds XXH_STRIPE_LEN
        buf = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
        try:
            for i in range(size):
                buf[i] = chr(i % 127)
            c = checksum.compute_checksum(buf, size)
            self.assertNotEqual(c, 0)
        finally:
            lltype.free(buf, flavor='raw')

if __name__ == '__main__':
    unittest.main()
