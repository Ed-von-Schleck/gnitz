import unittest
from gnitz.storage import memtable_manager, memtable_node
from gnitz.core import types, values as db_values

class TestAnnihilation(unittest.TestCase):
    def test_active_annihilation(self):
        """
        Verifies that nodes are physically unlinked from the SkipList
        when their net weight reaches zero.
        """
        layout = types.ComponentLayout([types.TYPE_I64])
        mgr = memtable_manager.MemTableManager(layout, 1024*1024)
        
        p = [db_values.IntValue(100)]
        key = 1
        
        # 1. Insert record (weight +1)
        mgr.put(key, 1, p)
        
        # 2. Verify node exists in the internal structure
        table = mgr.active_table
        base = table.arena.base_ptr
        head = table.head_off
        
        def is_skip_list_empty():
            # Check all forward pointers of the head node
            for i in range(16): # MAX_HEIGHT
                if memtable_node.node_get_next_off(base, head, i) != 0:
                    return False
            return True

        self.assertFalse(is_skip_list_empty(), "SkipList should not be empty after insert")
        
        # 3. Insert annihilation (weight -1)
        mgr.put(key, -1, p)
        
        # 4. Verify physical unlinking
        # In GnitzDB, if new_w == 0, the node is immediately unlinked.
        self.assertTrue(is_skip_list_empty(), "All pointers should be zeroed after annihilation")
            
        mgr.close()

if __name__ == '__main__':
    unittest.main()
