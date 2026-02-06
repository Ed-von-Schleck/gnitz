"""
tests/test_engine_summation.py
"""
import unittest
import os
from gnitz.core import types
from gnitz.storage import writer_ecs, manifest, spine, engine, memtable

class TestEngineSummation(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.files = ["s1.db", "s2.db"]
        
    def tearDown(self):
        for f in self.files:
            if os.path.exists(f): os.unlink(f)

    def _create_shard(self, filename, eid, weight, val):
        w = writer_ecs.ECSShardWriter(self.layout)
        w._add_entity_weighted(eid, weight, val)
        w.finalize(filename)

    def test_overlapping_weight_summation(self):
        """Test that Engine sums weights across multiple overlapping shards."""
        # Create two shards both having Entity 1 with W=1
        self._create_shard("s1.db", 1, 1, 100)
        self._create_shard("s2.db", 1, 1, 200)
        
        # Load spine with both shards
        h1 = spine.ShardHandle("s1.db", self.layout)
        h2 = spine.ShardHandle("s2.db", self.layout)
        sp = spine.Spine([h1, h2])
        
        mgr = memtable.MemTableManager(self.layout, 1024)
        db = engine.Engine(mgr, sp)
        
        # Weight should be 1 + 1 = 2
        self.assertEqual(db.get_effective_weight(1), 2)
        
        # Value resolution: Last one wins strategy (assuming s2 is latest)
        self.assertEqual(db.read_component_i64(1, 0), 200)
        
        db.close()

    def test_memtable_shard_annihilation(self):
        """Test that MemTable updates annihilate persistent shard records."""
        # Shard has W=1
        self._create_shard("s1.db", 1, 1, 100)
        
        h1 = spine.ShardHandle("s1.db", self.layout)
        sp = spine.Spine([h1])
        
        mgr = memtable.MemTableManager(self.layout, 1024)
        db = engine.Engine(mgr, sp)
        
        # MemTable has W=-1
        db.mem_manager.put(1, -1, 100)
        
        # Net weight should be 1 + (-1) = 0
        self.assertEqual(db.get_effective_weight(1), 0)
        
        db.close()

if __name__ == '__main__':
    unittest.main()
