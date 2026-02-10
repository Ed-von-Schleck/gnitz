import unittest
from gnitz.core import types

class TestTableSchema(unittest.TestCase):
    def test_simple_layout(self):
        # Two 8-byte fields. Should be tightly packed.
        layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_I64),
            types.ColumnDefinition(types.TYPE_F64)
        ], 0)
        self.assertEqual(layout.get_column_offset(1), 0) # Col 1 offset relative to data start
        self.assertEqual(layout.memtable_stride, 8)

    def test_padding_and_alignment(self):
        # PK is Col 0. Payload is [i8, i64]. 
        # i64 must be 8-byte aligned. i8 is at 0, next 8 is 8.
        layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64), # PK
            types.ColumnDefinition(types.TYPE_I8),
            types.ColumnDefinition(types.TYPE_I64)
        ], 0)
        self.assertEqual(layout.get_column_offset(1), 0)
        self.assertEqual(layout.get_column_offset(2), 8)
        self.assertEqual(layout.memtable_stride, 16)

    def test_string_type(self):
        # String is 16 bytes with 8-byte alignment.
        layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64), # PK
            types.ColumnDefinition(types.TYPE_I8),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        self.assertEqual(layout.get_column_offset(1), 0)
        self.assertEqual(layout.get_column_offset(2), 8)
        self.assertEqual(layout.memtable_stride, 24)

if __name__ == '__main__':
    unittest.main()
