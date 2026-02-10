import unittest
from gnitz.storage import layout

class TestLayout(unittest.TestCase):
    def test_magic_number(self):
        self.assertEqual(layout.MAGIC_NUMBER, 0x31305F5A54494E47)

    def test_alignment_const(self):
        self.assertEqual(layout.HEADER_SIZE % layout.ALIGNMENT, 0)
        self.assertEqual(layout.HEADER_SIZE, 64)
    
    def test_header_field_offsets(self):
        self.assertEqual(layout.OFF_MAGIC, 0)
        self.assertEqual(layout.OFF_VERSION, 8)
        self.assertEqual(layout.OFF_ROW_COUNT, 16)
        self.assertEqual(layout.OFF_DIR_OFFSET, 24)
        self.assertEqual(layout.OFF_TABLE_ID, 32)
