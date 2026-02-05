import unittest
import os
from gnitz.storage.memtable import MemTableManager
from gnitz.storage.spine import ShardHandle, Spine
from gnitz.storage.engine import Engine
from gnitz.storage.writer import ShardWriter

class TestEngine(unittest.TestCase):
    def setUp(self):
        self.fn = "engine_test.db"
        self.mem_mgr = MemTableManager(1024 * 1024)
        
        sw = ShardWriter()
        sw.add_entry("apple", "v1", 5)
        sw.add_entry("cherry", "v2", 5)
        sw.finalize(self.fn)
        
        self.spine = Spine([ShardHandle(self.fn)])
        self.engine = Engine(self.mem_mgr, self.spine)

    def tearDown(self):
        self.engine.close()
        if os.path.exists(self.fn):
            os.unlink(self.fn)

    def test_point_query_summation(self):
        # Key in Spine only
        self.assertEqual(self.engine.get_weight("cherry"), 5)
        
        # Key in MemTable only
        self.engine.mem_manager.put("banana", "v3", 10)
        self.assertEqual(self.engine.get_weight("banana"), 10)
        
        # Key in BOTH (Annihilation)
        self.engine.mem_manager.put("apple", "v1", -5)
        self.assertEqual(self.engine.get_weight("apple"), 0)

if __name__ == '__main__':
    unittest.main()
