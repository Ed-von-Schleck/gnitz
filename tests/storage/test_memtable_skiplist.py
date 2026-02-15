import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.storage import memtable, memtable_node
from gnitz.core import types, values as db_values

class TestMemTableSkipList(unittest.TestCase):
    def setUp(self):
        self.schema_u128 = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128), # PK
            types.ColumnDefinition(types.TYPE_I64)   # Col 1
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

    def test_u128_node_access(self):
        mt = memtable.MemTable(self.schema_u128, 1024 * 1024)
        try:
            key = (r_uint128(0xAAAA) << 64) | r_uint128(0xBBBB)
            mt.upsert(key, 1, [db_values.IntValue(42)])
            
            node_off = mt._find_first_key(key)
            self.assertNotEqual(node_off, 0)
            
            # Verify physical 16-byte alignment in the arena
            height = ord(mt.arena.base_ptr[node_off + 8])
            key_off = memtable_node.get_key_offset(height)
            self.assertEqual((node_off + key_off) % 16, 0)
            
            read_key = memtable_node.node_get_key(mt.arena.base_ptr, node_off, 16)
            self.assertEqual(read_key, key)
        finally:
            mt.free()

    def test_active_annihilation(self):
        """Verifies that records summing to zero are removed from SkipList traversal."""
        mt = memtable.MemTable(self.schema_u128, 1024 * 1024)
        try:
            p = [db_values.IntValue(100)]
            key = r_uint128(1)
            
            mt.upsert(key, 1, p)
            self.assertNotEqual(memtable_node.node_get_next_off(mt.arena.base_ptr, mt.head_off, 0), 0)
            
            # Algebraic annihilation: 1 + (-1) = 0
            mt.upsert(key, -1, p)
            
            # Head pointer for level 0 should now point to NULL (sentinel 0)
            self.assertEqual(memtable_node.node_get_next_off(mt.arena.base_ptr, mt.head_off, 0), 0)
        finally:
            mt.free()
