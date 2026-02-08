import unittest
import mmap
from gnitz.storage import mmap_posix

class TestMMapPosix(unittest.TestCase):
    def test_constants(self):
        self.assertEqual(mmap_posix.PROT_READ, mmap.PROT_READ)
        self.assertEqual(mmap_posix.PROT_WRITE, mmap.PROT_WRITE)
        self.assertEqual(mmap_posix.MAP_SHARED, mmap.MAP_SHARED)

    def test_error_instantiation(self):
        # Ensure the custom error can be created
        err = mmap_posix.MMapError()
        self.assertIsInstance(err, Exception)

if __name__ == '__main__':
    unittest.main()
