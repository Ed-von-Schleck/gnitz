import unittest
import os
from gnitz.storage.writer import ShardWriter
from gnitz.storage.spine import ShardHandle

class TestSpine(unittest.TestCase):
    def setUp(self):
        self.filename = "test_spine_shard.db"
        sw = ShardWriter()
        sw.add_entry("apple", "red", 1)
        sw.add_entry("banana", "yellow", 1)
        sw.add_entry("cherry", "dark red", 1)
        sw.finalize(self.filename)

    def tearDown(self):
        if os.path.exists(self.filename):
            os.unlink(self.filename)

    def test_shard_handle_metadata(self):
        handle = ShardHandle(self.filename)
        # Check if boundaries were correctly extracted
        self.assertEqual(handle.min_key, "apple")
        self.assertEqual(handle.max_key, "cherry")
        self.assertEqual(handle.count, 3)
        
        # Verify data access through the handle
        self.assertEqual(handle.materialize_value(1), "yellow")
        handle.close()

if __name__ == '__main__':
    unittest.main()
