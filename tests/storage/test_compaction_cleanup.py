import unittest
import os
from gnitz.storage import refcount, compactor, spine, writer_ecs
from gnitz.core import types

class TestCompactionCleanup(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.rc = refcount.RefCounter()
        self.files_to_clean = []
        self.shard1 = "cleanup_test_s1.db"
        self.shard2 = "cleanup_test_s2.db"
        self._create_shard(self.shard1, [10], [100])
        self._create_shard(self.shard2, [10], [200])
        self.files_to_clean.extend([self.shard1, self.shard2])

    def tearDown(self):
        if hasattr(self, 'spine'):
            self.spine.close_all()
        for f in self.files_to_clean:
            if os.path.exists(f): os.unlink(f)

    def _create_shard(self, filename, entities, values):
        w = writer_ecs.ECSShardWriter(self.layout)
        for e, v in zip(entities, values):
            w.add_entity(e, v)
        w.finalize(filename)

    def test_deferred_cleanup(self):
        h1 = spine.ShardHandle(self.shard1, self.layout, 0)
        h2 = spine.ShardHandle(self.shard2, self.layout, 0)
        self.spine = spine.Spine([h1, h2], self.rc)
        
        # Verify shard is locked (cannot be deleted)
        self.assertFalse(self.rc.can_delete(self.shard1))
        
        old_shards = [self.shard1, self.shard2]
        compactor.finalize_compaction(old_shards, self.rc)
        
        # File should still exist because Spine holds a reference
        self.assertTrue(os.path.exists(self.shard1))
        
        # Closing spine releases references and triggers cleanup
        self.spine.close_all()
        self.assertFalse(os.path.exists(self.shard1))

if __name__ == '__main__':
    unittest.main()
