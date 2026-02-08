import unittest
from gnitz.core import types

class TestComponentLayout(unittest.TestCase):
    def test_simple_layout(self):
        # Two 8-byte fields. Should be tightly packed.
        layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_F64])
        self.assertEqual(layout.get_field_offset(0), 0)
        self.assertEqual(layout.get_field_offset(1), 8)
        self.assertEqual(layout.stride, 16)

    def test_padding_and_alignment(self):
        # Layout: [i8, i64]. Should have 7 bytes of padding after the i8.
        layout = types.ComponentLayout([types.TYPE_I8, types.TYPE_I64])
        self.assertEqual(layout.get_field_offset(0), 0) # i8 at start
        self.assertEqual(layout.get_field_offset(1), 8) # i64 must be 8-byte aligned
        self.assertEqual(layout.stride, 16)             # Total size is 8+8

    def test_mixed_types(self):
        # [i64, i8, i32]
        layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_I8, types.TYPE_I32])
        self.assertEqual(layout.get_field_offset(0), 0)  # i64
        self.assertEqual(layout.get_field_offset(1), 8)  # i8
        # i32 needs 4-byte alignment. 8+1=9. Next 4-byte boundary is 12.
        self.assertEqual(layout.get_field_offset(2), 12)
        # Total size is 12+4=16. Max alignment is 8. Stride must be multiple of 8.
        self.assertEqual(layout.stride, 16)
        
    def test_string_type(self):
        # String is 16 bytes with 8-byte alignment.
        layout = types.ComponentLayout([types.TYPE_I8, types.TYPE_STRING])
        self.assertEqual(layout.get_field_offset(0), 0)
        self.assertEqual(layout.get_field_offset(1), 8) # String needs 8-byte alignment
        self.assertEqual(layout.stride, 24)             # 8 + 16 = 24

if __name__ == '__main__':
    unittest.main()
