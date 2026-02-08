"""
test_zset.py
"""

import unittest
import os
import shutil
from gnitz.core import zset, types, values as db_values

class TestPersistentZSet(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_zset_env"
        self.db_name = "test_db"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.db = zset.PersistentZSet(self.test_dir, self.db_name, self.layout)

    def tearDown(self):
        if self.db:
            self.db.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_strict_zset_behavior(self):
        self.db.insert(10, 100, "X")
        self.assertEqual(self.db.get_weight(10, 100, "X"), 1)
        
        self.db.insert(10, 200, "Y")
        
        self.assertEqual(self.db.get_weight(10, 100, "X"), 1)
        self.assertEqual(self.db.get_weight(10, 200, "Y"), 1)
        
        self.db.remove(10, 200, "Y")
        
        self.assertEqual(self.db.get_weight(10, 100, "X"), 1)
        self.assertEqual(self.db.get_weight(10, 200, "Y"), 0)

    def test_weight_accumulation(self):
        self.db.insert(20, 100, "A")
        self.db.insert(20, 100, "A")
        
        self.assertEqual(self.db.get_weight(20, 100, "A"), 2)
        
        self.db.remove(20, 100, "A")
        self.assertEqual(self.db.get_weight(20, 100, "A"), 1)

    def test_persistence_flush(self):
        self.db.insert(40, 400, "Persist")
        
        shard_filename = self.db.flush()
        self.assertIsNotNone(shard_filename)
        self.assertTrue(os.path.exists(shard_filename))
        
        self.assertEqual(self.db.get_weight(40, 400, "Persist"), 1)

    def test_crash_recovery(self):
        self.db.insert(50, 500, "Crash")
        self.db.close()
        self.db = None
        
        self.db = zset.PersistentZSet(self.test_dir, self.db_name, self.layout)
        
        self.assertEqual(self.db.get_weight(50, 500, "Crash"), 1)

    def test_compaction_preserves_distinct_payloads(self):
        self.db.insert(60, 10, "A")
        self.db.flush()
        
        self.db.insert(60, 20, "B")
        self.db.flush()
        
        self.db.insert(60, 30, "C")
        self.db.flush()
        
        for i in range(2):
            self.db.insert(60, 40+i, "D")
            self.db.flush()

        self.assertEqual(self.db.get_weight(60, 10, "A"), 1)
        self.assertEqual(self.db.get_weight(60, 20, "B"), 1)
        self.assertEqual(self.db.get_weight(60, 30, "C"), 1)

    def test_ghost_property(self):
        self.db.insert(70, 700, "Ghost")
        self.db.remove(70, 700, "Ghost")
        
        self.db.flush()
        self.assertEqual(self.db.get_weight(70, 700, "Ghost"), 0)

if __name__ == '__main__':
    unittest.main()
