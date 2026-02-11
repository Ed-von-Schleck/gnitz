import unittest
import os
from gnitz.core import types, values as db_values
from gnitz.storage import (
    memtable, engine, spine, memtable_manager
)

class TestEngineSummation(unittest.TestCase):
    def setUp(self):
        # Setup schema: PK(i64), Val1(i64)
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.mgr = memtable_manager.MemTableManager(self.layout, 1024 * 1024)
        self.db = engine.Engine(self.mgr, spine.Spine([]), table_id=1)

    def tearDown(self):
        self.db.close()

    def test_raw_weight_summation(self):
        """
        Tests that weight is summed correctly between the MemTable and Shards
        without using higher-level Z-Set abstractions.
        """
        payload = [db_values.IntValue(100)]
        
        # 1. Add to MemTable
        self.mgr.put(1, 1, payload)
        
        # 2. Reconcile weight
        self.assertEqual(self.db.get_effective_weight_raw(1, payload), 1)
        
        # 3. Flush to a shard
        shard_fn = "test_summation.db"
        self.db.flush_and_rotate(shard_fn)
        
        try:
            # 4. Add weight to the new active MemTable
            self.mgr.put(1, 1, payload)
            
            # 5. Verify sum (1 from shard + 1 from MemTable)
            self.assertEqual(self.db.get_effective_weight_raw(1, payload), 2)
        finally:
            if os.path.exists(shard_fn):
                os.unlink(shard_fn)

if __name__ == '__main__':
    unittest.main()
