import unittest
import mmap
from rpython.rtyper.lltypesystem import rffi, lltype
#from gnitz.storage.arena import Arena
from gnitz.storage.buffer import MappedBuffer, Buffer
from gnitz.storage import mmap_posix, errors

class TestStorageLowLevel(unittest.TestCase):
    # --- Arena Tests ---
    def _get_off(self, arena, ptr):
        """Helper to calculate relative offset of a raw pointer."""
        return rffi.cast(lltype.Signed, ptr) - rffi.cast(lltype.Signed, arena.base_ptr)

    def test_arena_alignment_logic(self):
        arena = Buffer(1024, growable=False)
        try:
            # Check first allocation (should be at offset 0)
            p1 = arena.alloc(5, alignment=1)
            self.assertEqual(self._get_off(arena, p1), 0)
            
            # Force 8-byte alignment (offset 5 -> 8)
            p2 = arena.alloc(1, alignment=8)
            self.assertEqual(self._get_off(arena, p2), 8)

            # Force 16-byte alignment (offset 9 -> 16)
            p3 = arena.alloc(1, alignment=16)
            self.assertEqual(self._get_off(arena, p3), 16)
        finally:
            arena.free()

    def test_arena_overflow(self):
        arena = Buffer(10, growable=False)
        try:
            arena.alloc(5)
            with self.assertRaises(errors.MemTableFullError):
                arena.alloc(10)
        finally:
            arena.free()

    def test_arena_lifecycle(self):
        arena = Buffer(1024, growable=False)
        ptr = arena.alloc(100)
        self.assertNotEqual(rffi.cast(lltype.Signed, ptr), 0)
        arena.free()
        self.assertEqual(rffi.cast(lltype.Signed, arena.base_ptr), 0)

    # --- MappedBuffer Tests ---
    def test_buffer_bounds_checking(self):
        # Create a buffer of 10 bytes (using a dummy pointer string for logic test)
        buf = MappedBuffer(rffi.cast(rffi.CCHARP, 0x12345678), 10)
        
        # Valid cases
        buf._check_bounds(0, 10)
        buf._check_bounds(5, 5)
        
        # Invalid cases
        with self.assertRaises(errors.BoundsError):
            buf._check_bounds(0, 11)
        with self.assertRaises(errors.BoundsError):
            buf._check_bounds(-1, 5)
        with self.assertRaises(errors.BoundsError):
            buf._check_bounds(9, 2)

    # --- MMap Posix Tests ---
    def test_mmap_constants(self):
        self.assertEqual(mmap_posix.PROT_READ, mmap.PROT_READ)
        self.assertEqual(mmap_posix.PROT_WRITE, mmap.PROT_WRITE)
        self.assertEqual(mmap_posix.MAP_SHARED, mmap.MAP_SHARED)

    def test_mmap_error_instantiation(self):
        err = mmap_posix.MMapError()
        self.assertIsInstance(err, Exception)

if __name__ == '__main__':
    unittest.main()
