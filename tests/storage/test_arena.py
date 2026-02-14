import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage.arena import Arena
from gnitz.storage import errors

class TestArena(unittest.TestCase):
    def _get_off(self, arena, ptr):
        """Helper to calculate relative offset of a raw pointer."""
        return rffi.cast(lltype.Signed, ptr) - rffi.cast(lltype.Signed, arena.base_ptr)

    def test_alignment_logic(self):
        """
        Verifies that the allocator correctly aligns pointers.
        Explicitly tests various alignments to avoid brittleness.
        """
        # Create a large enough arena
        arena = Arena(1024)
        try:
            # 1. Check first allocation (should be at offset 0)
            p1 = arena.alloc(5, alignment=1)
            self.assertEqual(self._get_off(arena, p1), 0)
            self.assertEqual(arena.offset, 5)

            # 2. Force 8-byte alignment
            # Offset is 5, next 8-byte boundary is 8
            p2 = arena.alloc(1, alignment=8)
            self.assertEqual(self._get_off(arena, p2), 8)
            self.assertEqual(arena.offset, 9)

            # 3. Force 16-byte alignment
            # Offset is 9, next 16-byte boundary is 16
            p3 = arena.alloc(1, alignment=16)
            self.assertEqual(self._get_off(arena, p3), 16)
            self.assertEqual(arena.offset, 17)

            # 4. Force 64-byte alignment (Cache line)
            # Offset is 17, next 64-byte boundary is 64
            p4 = arena.alloc(1, alignment=64)
            self.assertEqual(self._get_off(arena, p4), 64)
            self.assertEqual(arena.offset, 65)

        finally:
            arena.free()

    def test_default_alignment(self):
        """Verifies the system-wide default alignment (16 bytes)."""
        arena = Arena(1024)
        try:
            p1 = arena.alloc(1) # Uses default
            self.assertEqual(self._get_off(arena, p1), 0)
            
            p2 = arena.alloc(1) # Uses default
            # Should skip to offset 16
            self.assertEqual(self._get_off(arena, p2), 16)
        finally:
            arena.free()

    def test_overflow(self):
        """Verifies that the arena correctly signals exhaustion."""
        arena = Arena(10)
        try:
            # Allocate half
            arena.alloc(5)
            
            # Attempt to allocate more than remains
            with self.assertRaises(errors.MemTableFullError):
                arena.alloc(10)
                
            # Exact fit should still work if alignment allows
            # Current offset is 5. If we ask for 5 more with alignment=1:
            arena.alloc(5, alignment=1)
            self.assertEqual(arena.offset, 10)
        finally:
            arena.free()

    def test_lifecycle(self):
        """Verifies memory can be allocated and freed."""
        arena = Arena(1024)
        ptr = arena.alloc(100)
        self.assertNotEqual(rffi.cast(lltype.Signed, ptr), 0)
        arena.free()
        # Verify base_ptr is nulled (using private access for verification)
        self.assertEqual(rffi.cast(lltype.Signed, arena.base_ptr), 0)

if __name__ == '__main__':
    unittest.main()
