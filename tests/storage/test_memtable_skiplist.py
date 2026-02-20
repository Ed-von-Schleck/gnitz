# tests/storage/test_memtable_skiplist.py
import unittest
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.storage import memtable, memtable_node
from gnitz.core import types
from tests.row_helpers import create_test_row

class TestMemTableSkipList(unittest.TestCase):
    def setUp(self):
        # Schema: PK (u128), Column 1 (i64)
        self.schema_u128 = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128), # PK (Index 0)
            types.ColumnDefinition(types.TYPE_I64)   # Payload Col 0
        ], 0)

    def test_key_alignment_geometry(self):
        """Verifies PK alignment relative to node start across SkipList heights."""
        for h in range(1, 17):
            key_off = memtable_node.get_key_offset(h)
            # Alignment must be 16 to support native u128 registers
            self.assertEqual(key_off % 16, 0, "Key offset must be 16-byte aligned for height %d" % h)
            # Ensure pointers (4 bytes each) don't overflow into the key
            pointer_end = 12 + (h * 4)
            self.assertGreaterEqual(key_off, pointer_end)

    def test_active_annihilation(self):
        """Verifies that records summing to zero are removed from SkipList traversal."""
        mt = memtable.MemTable(self.schema_u128, 1024 * 1024)
        try:
            # Create a PayloadRow instead of a list of TaggedValues
            row = create_test_row(self.schema_u128, [100])
            key = r_uint128(1)
            
            # Initial insertion: weight 1
            mt.upsert(key, 1, row)
            # Verify head pointer level 0 is not null (points to the new node)
            self.assertNotEqual(memtable_node.node_get_next_off(mt.arena.base_ptr, mt.head_off, 0), 0)
            
            # Algebraic annihilation: 1 + (-1) = 0
            # The MemTable should physically unlink the node when weight hits 0
            mt.upsert(key, -1, row)
            
            # Head pointer for level 0 should now point back to NULL (sentinel 0)
            self.assertEqual(memtable_node.node_get_next_off(mt.arena.base_ptr, mt.head_off, 0), 0)
        finally:
            mt.free()
