import unittest
from gnitz.storage.arena import Arena
from gnitz.storage.errors import StorageError

class TestArena(unittest.TestCase):
    def test_alignment(self):
        arena = Arena(1024)
        p1 = arena.alloc(5)
        p2 = arena.alloc(1)
        # In unittest, we use self.assertEqual instead of assert
        self.assertEqual(arena.offset, 9) 
        arena.free()

    def test_overflow(self):
        arena = Arena(10)
        arena.alloc(5)
        # Use self.assertRaises for exception testing
        with self.assertRaises(StorageError):
            arena.alloc(10)
        arena.free()

if __name__ == '__main__':
    unittest.main()
