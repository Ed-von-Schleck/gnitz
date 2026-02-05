import unittest
import os
from gnitz.core import types
from gnitz.storage import memtable, shard_ecs

class TestMemTableECS(unittest.TestCase):
    def setUp(self):
        self.fn = "test_mt_ecs.db"
        self.layout = types.ComponentLayout([types.TYPE_U64, types.TYPE_STRING])
        self.mgr = memtable.MemTableManager(self.layout, 1024 * 1024)

    def tearDown(self):
        self.mgr.close()
        if os.path.exists(self.fn): os.unlink(self.fn)

    def test_upsert_and_flush(self):
        # Insert 3 entities
        self.mgr.put(10, 1, 100, "short")
        self.mgr.put(20, 1, 200, "this is a very long string that should be in the heap")
        self.mgr.put(30, 1, 300, "another")
        
        # Test algebraic coalescing (annihilation)
        self.mgr.put(30, -1, 300, "another") # Weight of 30 becomes 0
        
        self.mgr.flush_and_rotate(self.fn)
        
        view = shard_ecs.ECSShardView(self.fn, self.layout)
        # Should only have 2 entities (10 and 20). 30 was pruned.
        self.assertEqual(view.count, 2)
        
        self.assertEqual(view.get_entity_id(0), 10)
        self.assertEqual(view.read_field_i64(0, 0), 100)
        self.assertTrue(view.string_field_equals(0, 1, "short"))
        
        self.assertEqual(view.get_entity_id(1), 20)
        self.assertTrue(view.string_field_equals(1, 1, "this is a very long string that should be in the heap"))
        
        view.close()

if __name__ == '__main__':
    unittest.main()
