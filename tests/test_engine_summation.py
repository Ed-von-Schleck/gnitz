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
        self._create_shard("s1.db", 1, 1, 100)
        self._create_shard("s2.db", 1, 1, 200)
        
        # Pass LSNs (1 and 2) explicitly
        h1 = spine.ShardHandle("s1.db", self.layout, 1)
        h2 = spine.ShardHandle("s2.db", self.layout, 2)
        sp = spine.Spine([h1, h2])
        
        mgr = memtable.MemTableManager(self.layout, 1024)
        db = engine.Engine(mgr, sp)
        
        self.assertEqual(db.get_effective_weight(1), 2)
        self.assertEqual(db.read_component_i64(1, 0), 200) # LSN 2 wins
        
        db.close()

    def test_memtable_shard_annihilation(self):
        self._create_shard("s1.db", 1, 1, 100)
        
        h1 = spine.ShardHandle("s1.db", self.layout, 1)
        sp = spine.Spine([h1])
        
        mgr = memtable.MemTableManager(self.layout, 1024)
        db = engine.Engine(mgr, sp)
        
        db.mem_manager.put(1, -1, 100)
        self.assertEqual(db.get_effective_weight(1), 0)
        
        db.close()

if __name__ == '__main__':
    unittest.main()
