"""
tests/test_compactor.py
"""
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

    def test_basic_compaction(self):
        # Shard 1: E1(W=1, V=10), E2(W=1, V=20)
        w1 = writer_ecs.ECSShardWriter(self.layout)
        w1._add_entity_weighted(1, 1, 10)
        w1._add_entity_weighted(2, 1, 20)
        w1.finalize("in1.db")
        
        # Shard 2: E2(W=1, V=200), E3(W=1, V=30)
        w2 = writer_ecs.ECSShardWriter(self.layout)
        w2._add_entity_weighted(2, 1, 200)
        w2._add_entity_weighted(3, 1, 30)
        w2.finalize("in2.db")
        
        # Compact: Updated signature (no lsns)
        compactor.compact_shards(["in1.db", "in2.db"], "out.db", self.layout)
        
        res = shard_ecs.ECSShardView("out.db", self.layout)
        # Expected: E1(10), E2(20), E2(200), E3(30) -> 4 records
        # Because E2(20) and E2(200) are distinct elements in Z-Set.
        self.assertEqual(res.count, 4)
        
        res.close()

    def test_compaction_annihilation(self):
        # Shard 1: E1(W=1)
        w1 = writer_ecs.ECSShardWriter(self.layout)
        w1._add_entity_weighted(1, 1, 10)
        w1.finalize("in1.db")
        
        # Shard 2: E1(W=-1) -> Deletion
        w2 = writer_ecs.ECSShardWriter(self.layout)
        w2._add_entity_weighted(1, -1, 10)
        w2.finalize("in2.db")
        
        compactor.compact_shards(["in1.db", "in2.db"], "out.db", self.layout)
        
        res = shard_ecs.ECSShardView("out.db", self.layout)
        self.assertEqual(res.count, 0) # Ghost property: annihilated
        res.close()

    def test_partial_annihilation(self):
        """Test that +2 and -1 results in +1 (record preserved)."""
        # Shard 1: E1(W=2)
        w1 = writer_ecs.ECSShardWriter(self.layout)
        w1._add_entity_weighted(1, 2, 10)
        w1.finalize("in1.db")
        
        # Shard 2: E1(W=-1)
        w2 = writer_ecs.ECSShardWriter(self.layout)
        w2._add_entity_weighted(1, -1, 10)
        w2.finalize("in2.db")
        
        compactor.compact_shards(["in1.db", "in2.db"], "out.db", self.layout)
        
        res = shard_ecs.ECSShardView("out.db", self.layout)
        self.assertEqual(res.count, 1)
        self.assertEqual(res.get_weight(0), 1)
        res.close()

if __name__ == '__main__':
    unittest.main()
