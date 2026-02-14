import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.storage import memtable, memtable_node
from gnitz.core import types, values as db_values

class TestMemTableGeometry(unittest.TestCase):
    def setUp(self):
        self.schema_u64 = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64), # PK
            types.ColumnDefinition(types.TYPE_I64)  # Col 1
        ], 0)
        
        self.schema_u128 = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128), # PK
            types.ColumnDefinition(types.TYPE_I64)   # Col 1
        ], 0)

    def test_key_alignment_padding(self):
        """
        Verifies that padding is inserted to keep the PK 16-byte aligned 
        relative to the node start across various SkipList heights.
        """
        # Testing heights 1 through 16
        for h in range(1, 17):
            key_off = memtable_node.get_key_offset(h)
            
            # 1. Verification of the alignment contract
            self.assertEqual(key_off % 16, 0, "Key offset must be 16-byte aligned for height %d" % h)
            
            # 2. Verification that pointers don't overlap the key
            # Header is 12 bytes, pointers are h * 4 bytes
            pointer_end = 12 + (h * 4)
            self.assertGreaterEqual(key_off, pointer_end, "Key offset overlaps pointer array for height %d" % h)

    def test_node_geometry_accessors(self):
        """
        End-to-end check: Insert data and verify that the low-level 
        memory access matches the high-level logic using the restored traversal.
        """
        mt = memtable.MemTable(self.schema_u128, 1024 * 1024)
        try:
            # Insert a u128 key
            key = (r_uint128(0xAAAA) << 64) | r_uint128(0xBBBB)
            mt.upsert(key, 1, [db_values.IntValue(42)])
            
            # FIXED: Use the restored _find_first_key method to locate the node
            node_off = mt._find_first_key(key)
            self.assertNotEqual(node_off, 0, "Node should be found via _find_first_key")
            
            # Verify weight
            self.assertEqual(memtable_node.node_get_weight(mt.arena.base_ptr, node_off), 1)
            
            # Verify Key via accessor (handles lo/hi split and alignment)
            read_key = memtable_node.node_get_key(mt.arena.base_ptr, node_off, 16)
            self.assertEqual(read_key, key)
            
            # Verify that the key pointer is 16-byte aligned in raw arena memory.
            # Because the Arena base is 16-byte aligned and nodes are allocated with 
            # 16-byte alignment, the node_off + key_off must be 16-byte aligned.
            height = ord(mt.arena.base_ptr[node_off + 8])
            key_off = memtable_node.get_key_offset(height)
            total_phys_offset = node_off + key_off
            self.assertEqual(total_phys_offset % 16, 0, "Physical key address in arena is not 16-byte aligned")
            
        finally:
            mt.free()

if __name__ == '__main__':
    unittest.main()
