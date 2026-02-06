import unittest
import os
from gnitz.storage import refcount, compactor, spine, writer_ecs
from gnitz.core import types

class TestCompactionCleanup(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.rc = refcount.RefCounter()
        self.files_to_clean = []
        
        # Create 2 dummy shards
        self.shard1 = "cleanup_test_s1.db"
        self.shard2 = "cleanup_test_s2.db"
        self._create_shard(self.shard1, [10], [100])
        self._create_shard(self.shard2, [10], [200]) # Overlapping
        self.files_to_clean.extend([self.shard1, self.shard2])

    def tearDown(self):
        # Force close everything to be safe
        if hasattr(self, 'spine'):
            self.spine.close_all()
        
        for f in self.files_to_clean:
            if os.path.exists(f):
                os.unlink(f)

    def _create_shard(self, filename, entities, values):
        w = writer_ecs.ECSShardWriter(self.layout)
        for e, v in zip(entities, values):
            w.add_entity(e, v)
        w.finalize(filename)

    def test_deferred_cleanup(self):
        # 1. Load shards into a Spine with the RefCounter
        # Manually create handles since we aren't using a manifest here
        h1 = spine.ShardHandle(self.shard1, self.layout)
        h2 = spine.ShardHandle(self.shard2, self.layout)
        self.spine = spine.Spine([h1, h2], self.rc)
        
        # Verify references acquired
        self.assertEqual(self.rc.get_refcount(self.shard1), 1)
        self.assertEqual(self.rc.get_refcount(self.shard2), 1)

        # 2. Simulate Compaction Finalization
        # (We skip the physical merge and just test the cleanup logic)
        old_shards = [self.shard1, self.shard2]
        
        compactor.finalize_compaction(old_shards, self.rc)
        
        # 3. Verify files still exist (because Spine has them open)
        self.assertTrue(os.path.exists(self.shard1))
        self.assertTrue(os.path.exists(self.shard2))
        
        # 4. Close Spine (Releases refs and triggers cleanup)
        self.spine.close_all()
        
        # 5. Verify files are gone
        self.assertFalse(os.path.exists(self.shard1))
        self.assertFalse(os.path.exists(self.shard2))

if __name__ == '__main__':
    unittest.main()
