import unittest
import os
import shutil
from gnitz.core import zset, types, values as db_values

class TestWALCheckpoint(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_checkpoint_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        self.db_name = "check_db"
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.db = zset.PersistentZSet(self.test_dir, self.db_name, self.layout)

    def tearDown(self):
        self.db.close()
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_wal_truncation_after_flush(self):
        """Verifies WAL size reduction after checkpointing flushed data."""
        p = [db_values.IntValue(100)]
        
        # 1. Fill WAL with some entries
        for i in range(10):
            self.db.insert(i, p)
            
        size_before = os.path.getsize(self.db.wal_path)
        self.assertTrue(size_before > 0)
        
        # 2. Flush to Shard (LSNs are now durable in .db file)
        self.db.flush()
        
        # 3. Checkpoint (Should trigger truncation)
        self.db.checkpoint()
        
        size_after = os.path.getsize(self.db.wal_path)
        # WAL should be smaller because LSNs were removed
        self.assertLess(size_after, size_before)
        
        # 4. Verify data is still reachable via the shard image
        self.assertEqual(self.db.get_weight(1, p), 1)

if __name__ == '__main__':
    unittest.main()
