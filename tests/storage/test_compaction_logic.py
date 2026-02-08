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
        
        # Updated: Pass layout instead of lsns
        result = compaction_logic.merge_entity_contributions(cursors, self.layout)
        
        # In strict Z-Set, weights are summed for exact payloads.
        # Since payload 100 == payload 100, weights merge. 1 - 1 = 0.
        # Result list should be empty (annihilated).
        self.assertEqual(len(result), 0)
        
        v1.close()
        v2.close()

    def test_merge_accumulation(self):
        # Shard 1: Entity 1, Weight +1, Val 100
        w1 = writer_ecs.ECSShardWriter(self.layout)
        w1._add_entity_weighted(1, 1, 100)
        w1.finalize(self.files[0])
        
        # Shard 2: Entity 1, Weight +1, Val 200
        # In pure Z-Set, this is a distinct record (1, 200).
        w2 = writer_ecs.ECSShardWriter(self.layout)
        w2._add_entity_weighted(1, 1, 200)
        w2.finalize(self.files[1])
        
        v1 = shard_ecs.ECSShardView(self.files[0], self.layout)
        v2 = shard_ecs.ECSShardView(self.files[1], self.layout)
        
        c1 = tournament_tree.StreamCursor(v1)
        c2 = tournament_tree.StreamCursor(v2)
        
        cursors = [c1, c2]
        
        # Should return two results: (1, 100) and (1, 200)
        results = compaction_logic.merge_entity_contributions(cursors, self.layout)
        
        self.assertEqual(len(results), 2)
        
        # Check values
        # Note: Order depends on cursor iteration, but logically we have both.
        # We can collect payloads.
        payloads = []
        for w, ptr, _ in results:
            self.assertEqual(w, 1)
            val = rffi.cast(rffi.LONGLONGP, ptr)[0]
            payloads.append(val)
        
        self.assertTrue(100 in payloads)
        self.assertTrue(200 in payloads)
        
        v1.close()
        v2.close()

if __name__ == '__main__':
    unittest.main()
