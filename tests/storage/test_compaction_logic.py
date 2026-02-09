import unittest
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import writer_ecs, shard_ecs, compaction_logic
from gnitz.core import types
from gnitz.storage import tournament_tree

class TestCompactionLogic(unittest.TestCase):
    def setUp(self):
        self.files = ["test_logic1.db", "test_logic2.db"]
        self.layout = types.ComponentLayout([types.TYPE_I64])
        
    def tearDown(self):
        for f in self.files:
            if os.path.exists(f): os.unlink(f)

    def test_merge_annihilation(self):
        w1 = writer_ecs.ECSShardWriter(self.layout)
        w1._add_entity_weighted(1, 1, 100)
        w1.finalize(self.files[0])
        
        w2 = writer_ecs.ECSShardWriter(self.layout)
        w2._add_entity_weighted(1, -1, 100)
        w2.finalize(self.files[1])
        
        v1 = shard_ecs.ECSShardView(self.files[0], self.layout)
        v2 = shard_ecs.ECSShardView(self.files[1], self.layout)
        cursors = [tournament_tree.StreamCursor(v1), tournament_tree.StreamCursor(v2)]
        
        result = compaction_logic.merge_entity_contributions(cursors, self.layout)
        self.assertEqual(len(result), 0)
        
        v1.close()
        v2.close()

if __name__ == '__main__':
    unittest.main()
