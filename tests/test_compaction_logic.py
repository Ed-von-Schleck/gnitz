"""
tests/test_compaction_logic.py
"""
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
        # Shard 1: Entity 1, Weight +1
        w1 = writer_ecs.ECSShardWriter(self.layout)
        # Pass fields as individual args, not a list
        w1._add_entity_weighted(1, 1, 100)
        w1.finalize(self.files[0])
        
        # Shard 2: Entity 1, Weight -1
        w2 = writer_ecs.ECSShardWriter(self.layout)
        w2._add_entity_weighted(1, -1, 100)
        w2.finalize(self.files[1])
        
        v1 = shard_ecs.ECSShardView(self.files[0], self.layout)
        v2 = shard_ecs.ECSShardView(self.files[1], self.layout)
        
        c1 = tournament_tree.StreamCursor(v1)
        c2 = tournament_tree.StreamCursor(v2)
        
        cursors = [c1, c2]
        lsns = [10, 20] 
        
        result = compaction_logic.merge_entity_contributions(cursors, lsns)
        net_weight, _, _ = result
        
        # In RPython version, we check net_weight == 0 instead of None
        self.assertEqual(net_weight, 0)
        
        v1.close()
        v2.close()

    def test_merge_accumulation(self):
        # Shard 1: Entity 1, Weight +1, Val 100
        w1 = writer_ecs.ECSShardWriter(self.layout)
        w1._add_entity_weighted(1, 1, 100)
        w1.finalize(self.files[0])
        
        # Shard 2: Entity 1, Weight +1, Val 200 (Duplicate/Update)
        w2 = writer_ecs.ECSShardWriter(self.layout)
        w2._add_entity_weighted(1, 1, 200)
        w2.finalize(self.files[1])
        
        v1 = shard_ecs.ECSShardView(self.files[0], self.layout)
        v2 = shard_ecs.ECSShardView(self.files[1], self.layout)
        
        c1 = tournament_tree.StreamCursor(v1)
        c2 = tournament_tree.StreamCursor(v2)
        
        # Shard 2 has higher LSN (20), so its value (200) should win
        cursors = [c1, c2]
        lsns = [10, 20]
        
        net_weight, payload_ptr, _ = compaction_logic.merge_entity_contributions(cursors, lsns)
        
        self.assertEqual(net_weight, 2)
        
        # Check payload
        val_ptr = rffi.cast(rffi.LONGLONGP, payload_ptr)
        self.assertEqual(val_ptr[0], 200)
        
        v1.close()
        v2.close()

if __name__ == '__main__':
    unittest.main()
