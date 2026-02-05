import unittest
import os
from gnitz.storage.memtable import MemTable, MemTableManager
from gnitz.storage.shard import ShardView

class TestMemTable(unittest.TestCase):
    def test_upsert_and_coalesce(self):
        mt = MemTable(1024 * 1024)
        mt.upsert("a", "v", 1)
        mt.upsert("a", "v", 2)
        
        # Access weight via optimized static helper logic
        from gnitz.storage.memtable import node_get_weight, node_get_next_off
        first_off = node_get_next_off(mt.arena.base_ptr, mt.head_off, 0)
        weight = node_get_weight(mt.arena.base_ptr, first_off)
        
        self.assertEqual(weight, 3)
        mt.free()

    def test_sorting(self):
        mt = MemTable(1024 * 1024)
        mt.upsert("z", "", 1)
        mt.upsert("a", "", 1)
        
        from gnitz.storage.memtable import node_get_next_off, node_get_key_str
        off1 = node_get_next_off(mt.arena.base_ptr, mt.head_off, 0)
        self.assertEqual(node_get_key_str(mt.arena.base_ptr, off1), "a")
        mt.free()

    def test_manager_rotation(self):
        mgr = MemTableManager(1024)
        mgr.put("k", "v", 1)
        fn = "test_mgr.db"
        try:
            mgr.flush_and_rotate(fn)
            view = ShardView(fn)
            self.assertEqual(view.count, 1)
            view.close()
        finally:
            mgr.close()
            if os.path.exists(fn): os.unlink(fn)

if __name__ == '__main__':
    unittest.main()
