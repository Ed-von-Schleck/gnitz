import unittest
import os
import shutil
from gnitz.storage import writer_ecs, shard_ecs
from gnitz.core import types

class TestAtomicShards(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_atomic_env"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.shard_path = os.path.join(self.test_dir, "atomic.db")

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_atomic_swap_mechanics(self):
        """
        Verifies that finalize uses a .tmp file and rename mechanics.
        While we cannot easily 'crash' the process mid-way in a unit test, 
        we can verify the end-to-end integrity and existence of the finalized file.
        """
        writer = writer_ecs.ECSShardWriter(self.layout)
        writer.add_entity(123, 456)
        
        # Finalize should create the file atomically
        writer.finalize(self.shard_path)
        
        self.assertTrue(os.path.exists(self.shard_path))
        self.assertFalse(os.path.exists(self.shard_path + ".tmp"))
        
        # Verify readability
        view = shard_ecs.ECSShardView(self.shard_path, self.layout)
        self.assertEqual(view.count, 1)
        self.assertEqual(view.get_entity_id(0), 123)
        self.assertEqual(view.read_field_i64(0, 0), 456)
        view.close()

    def test_overwriting_is_atomic(self):
        """
        Verifies that finalizing a shard over an existing one 
        replaces it safely via rename.
        """
        # Create version 1
        w1 = writer_ecs.ECSShardWriter(self.layout)
        w1.add_entity(1, 100)
        w1.finalize(self.shard_path)
        
        # Create version 2 (same filename)
        w2 = writer_ecs.ECSShardWriter(self.layout)
        w2.add_entity(2, 200)
        w2.finalize(self.shard_path)
        
        view = shard_ecs.ECSShardView(self.shard_path, self.layout)
        self.assertEqual(view.count, 1)
        # Should be version 2
        self.assertEqual(view.get_entity_id(0), 2)
        view.close()

if __name__ == '__main__':
    unittest.main()
