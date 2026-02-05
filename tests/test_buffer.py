import unittest
from gnitz.storage.buffer import MappedBuffer
from gnitz.storage.errors import BoundsError

class TestBuffer(unittest.TestCase):
    def test_bounds_checking(self):
        # Create a buffer of 10 bytes
        buf = MappedBuffer("fake_ptr", 10)
        
        # Should pass
        buf._check_bounds(0, 10)
        buf._check_bounds(5, 5)
        
        # Should fail
        with self.assertRaises(BoundsError):
            buf._check_bounds(0, 11)
        with self.assertRaises(BoundsError):
            buf._check_bounds(-1, 5)
        with self.assertRaises(BoundsError):
            buf._check_bounds(9, 2)

if __name__ == '__main__':
    unittest.main()
