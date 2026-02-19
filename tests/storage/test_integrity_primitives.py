import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import xxh as checksum
from gnitz.core.errors import GnitzError, StorageError, BoundsError

class TestIntegrityPrimitives(unittest.TestCase):
    # --- Hashing Tests ---
    def test_xxhash_stability(self):
        data = "GnitzDB_Theoretical_Foundation"
        c1 = checksum.compute_checksum_bytes(data)
        c2 = checksum.compute_checksum_bytes(data)
        self.assertEqual(c1, c2)
        self.assertNotEqual(c1, 0)
        self.assertNotEqual(c1, checksum.compute_checksum_bytes("GnitzDB_Practical_Foundation"))

    def test_xxhash_raw_pointers(self):
        size = 128
        buf = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
        try:
            for i in range(size):
                buf[i] = chr(i % 256)
            
            c1 = checksum.compute_checksum(buf, 64)
            self.assertTrue(checksum.verify_checksum(buf, 64, c1))
            
            ptr_offset = rffi.ptradd(buf, 10)
            c2 = checksum.compute_checksum(ptr_offset, 64)
            self.assertNotEqual(c1, c2)
        finally:
            lltype.free(buf, flavor='raw')

    def test_xxhash_large_buffer_loop(self):
        # Exceeds XXH_STRIPE_LEN (64) to test internal loop logic
        size = 1024
        buf = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
        try:
            for i in range(size):
                buf[i] = chr(i % 127)
            c = checksum.compute_checksum(buf, size)
            self.assertNotEqual(c, 0)
        finally:
            lltype.free(buf, flavor='raw')

    # --- Error Hierarchy Tests ---
    def test_exception_hierarchy(self):
        self.assertTrue(issubclass(StorageError, GnitzError))
        self.assertTrue(issubclass(BoundsError, StorageError))

    def test_error_raise_catch(self):
        try:
            # BoundsError requires (offset, length, limit)
            raise BoundsError(10, 5, 10)
        except StorageError as e:
            self.assertEqual(e.offset, 10)
        except Exception:
            self.fail("BoundsError not caught as StorageError")

if __name__ == '__main__':
    unittest.main()
