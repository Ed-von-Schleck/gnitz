import unittest
from gnitz.storage import layout

class TestLayout(unittest.TestCase):
    def test_magic_number(self):
        self.assertEqual(layout.MAGIC_NUMBER, 0x31305F5A54494E47)

    def test_header_offsets(self):
        self.assertTrue(layout.OFF_COUNT >= layout.OFF_MAGIC + 8)
        self.assertTrue(layout.OFF_REG_W >= layout.OFF_COUNT + 8)

    def test_alignment_const(self):
        self.assertEqual(layout.HEADER_SIZE % layout.ALIGNMENT, 0)

if __name__ == '__main__':
    unittest.main()
