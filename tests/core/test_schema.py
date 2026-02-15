import unittest
from gnitz.core import types
from gnitz.storage import layout

class TestSchemaAndLayout(unittest.TestCase):
    def test_physical_constants(self):
        """Validate authoritative binary format constants."""
        self.assertEqual(layout.MAGIC_NUMBER, 0x31305F5A54494E47)
        self.assertEqual(layout.HEADER_SIZE, 64)
        self.assertEqual(layout.DIR_ENTRY_SIZE, 24)
        self.assertEqual(layout.OFF_ROW_COUNT, 16)

    def test_schema_offset_calculation(self):
        """Verify AoS packing and field alignment logic."""
        # Schema: PK(u64) [Idx 0], I8 [Idx 1], String [Idx 2]
        # In AoS (MemTable), Col 0 is PK, Payload is [Col 1, Col 2]
        schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_I8),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        
        # Col 1 (I8) starts at 0 relative to payload
        self.assertEqual(schema.get_column_offset(1), 0)
        
        # Col 2 (String) must be 8-byte aligned. I8 at 0 + size 1 = 1. Next 8 is 8.
        self.assertEqual(schema.get_column_offset(2), 8)
        
        # Stride: 8 (Col 1) + 16 (Col 2) = 24
        self.assertEqual(schema.stride, 24)

    def test_u128_alignment(self):
        """Verify u128 forces 16-byte alignment for SkipList nodes."""
        schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_I8),
            types.ColumnDefinition(types.TYPE_U128)
        ], 0)
        
        # Col 1 (I8) at 0
        # Col 2 (U128) must be 16-byte aligned. Next 16-byte boundary after 1 is 16.
        self.assertEqual(schema.get_column_offset(2), 16)
        
        # Stride: 16 (Col 1 padding) + 16 (Col 2) = 32
        self.assertEqual(schema.stride, 32)

    def test_pk_identification(self):
        """Verify PK column correctly identified regardless of index."""
        schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_I64),
            types.ColumnDefinition(types.TYPE_U128) # PK
        ], 1)
        self.assertEqual(schema.get_pk_column().field_type, types.TYPE_U128)
        # Col 0 is part of the payload
        self.assertEqual(schema.get_column_offset(0), 0)

if __name__ == '__main__':
    unittest.main()
