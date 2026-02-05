import unittest
import os
from gnitz.storage.writer import ShardWriter
from gnitz.storage.shard import ShardView

class TestShard(unittest.TestCase):
    def setUp(self):
        self.fn = "test_shard_unittest.db"
        sw = ShardWriter()
        sw.add_entry("key1", "val1", 100)
        sw.finalize(self.fn)

    def tearDown(self):
        if os.path.exists(self.fn):
            os.unlink(self.fn)

    def test_shard_read(self):
        view = ShardView(self.fn)
        self.assertEqual(view.count, 1)
        self.assertEqual(view.get_weight(0), 100)
        self.assertEqual(view.materialize_key(0), "key1")
        self.assertEqual(view.materialize_value(0), "val1")
        view.close()

if __name__ == '__main__':
    unittest.main()
