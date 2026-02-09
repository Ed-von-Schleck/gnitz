import unittest
import os
import shutil
from gnitz.core import zset, types

class TestPersistentZSet(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_zset_env"
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.db = zset.PersistentZSet(self.test_dir, "test_db", self.layout)

    def tearDown(self):
        self.db.close()
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_weight_accumulation_no_boxing(self):
        self.db.insert(10, 100, "data")
        self.db.insert(10, 100, "data")
        self.assertEqual(self.db.get_weight(10, 100, "data"), 2)
        
        self.db.remove(10, 100, "data")
        self.assertEqual(self.db.get_weight(10, 100, "data"), 1)

    def test_distinct_payloads(self):
        self.db.insert(10, 100, "A")
        self.db.insert(10, 200, "B")
        self.assertEqual(self.db.get_weight(10, 100, "A"), 1)
        self.assertEqual(self.db.get_weight(10, 200, "B"), 1)
