import unittest
from gnitz.storage import layout

class TestLayout(unittest.TestCase):
    def test_magic_number(self):
        """Test that magic number is set correctly."""
        self.assertEqual(layout.MAGIC_NUMBER, 0x31305F5A54494E47)

    def test_alignment_const(self):
        """Test that header size is aligned to 64-byte boundary."""
        self.assertEqual(layout.HEADER_SIZE % layout.ALIGNMENT, 0)
        self.assertEqual(layout.HEADER_SIZE, 64)
    
    def test_header_field_offsets(self):
        """Test that header fields are properly spaced and within bounds."""
        # Fields should be 8 bytes apart
        self.assertEqual(layout.OFF_MAGIC, 0)
        self.assertEqual(layout.OFF_COUNT, 8)
        self.assertEqual(layout.OFF_CHECKSUM_E, 16)
        self.assertEqual(layout.OFF_CHECKSUM_W, 24)
        
    def test_region_offsets(self):
        """Test that region pointer offsets are properly spaced and within bounds."""
        self.assertEqual(layout.OFF_REG_E_ECS, 32)
        self.assertEqual(layout.OFF_REG_C_ECS, 40)
        self.assertEqual(layout.OFF_REG_B_ECS, 48)
        self.assertEqual(layout.OFF_REG_W_ECS, 56)
        
    def test_all_offsets_within_header(self):
        """Test that all header offsets fit within the 64-byte header."""
        offsets = [
            layout.OFF_MAGIC,
            layout.OFF_COUNT,
            layout.OFF_CHECKSUM_E,
            layout.OFF_CHECKSUM_W,
            layout.OFF_REG_E_ECS,
            layout.OFF_REG_C_ECS,
            layout.OFF_REG_B_ECS,
            layout.OFF_REG_W_ECS,
        ]
        
        for offset in offsets:
            # Each offset + 8 bytes (u64 size) must fit in header
            self.assertLessEqual(offset + 8, layout.HEADER_SIZE)
    
    def test_no_overlapping_offsets(self):
        """Test that header field offsets don't overlap."""
        offsets = [
            layout.OFF_MAGIC,
            layout.OFF_COUNT,
            layout.OFF_CHECKSUM_E,
            layout.OFF_CHECKSUM_W,
            layout.OFF_REG_E_ECS,
            layout.OFF_REG_C_ECS,
            layout.OFF_REG_B_ECS,
            layout.OFF_REG_W_ECS,
        ]
        
        # Check that all offsets are unique
        self.assertEqual(len(offsets), len(set(offsets)))
        
        # Check that they're properly spaced (8 bytes apart)
        sorted_offsets = sorted(offsets)
        for i in range(len(sorted_offsets) - 1):
            diff = sorted_offsets[i + 1] - sorted_offsets[i]
            self.assertGreaterEqual(diff, 8)

if __name__ == '__main__':
    unittest.main()
