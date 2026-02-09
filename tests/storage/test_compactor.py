import unittest
import os
from gnitz.core import types
from gnitz.storage import writer_ecs, shard_ecs, compactor

class TestCompactor(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.files = ["in1.db", "in2.db", "out.db"]

    def tearDown(self):
        for f in self.files:
            if os.path.exists(f): os.unlink(f)

    def test_compaction_annihilation(self):
        w1 = writer_ecs.ECSShardWriter(self.layout)
        w1._add_entity_weighted(1, 1, 10)
        w1.finalize("in1.db")
        
        w2 = writer_ecs.ECSShardWriter(self.layout)
        w2._add_entity_weighted(1, -1, 10)
        w2.finalize("in2.db")
        
        compactor.compact_shards(["in1.db", "in2.db"], "out.db", self.layout)
        
        res = shard_ecs.ECSShardView("out.db", self.layout)
        self.assertEqual(res.count, 0)
        res.close()

if __name__ == '__main__':
    unittest.main()
