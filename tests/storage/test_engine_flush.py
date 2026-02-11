import unittest
import os
from gnitz.core import types, values as db_values
from gnitz.storage import (
    engine, shard_registry, spine, memtable_manager
)

class TestEngineFlush(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.manifest_fn = "test_engine_flush.manifest"
        self.test_files = [self.manifest_fn]

    def tearDown(self):
        for f in self.test_files:
            if os.path.exists(f): os.unlink(f)
    
    def _put(self, db, pk, w, *vals):
        wrapped = [db_values.wrap(v) for v in vals]
        db.mem_manager.put(pk, w, wrapped)

    def test_compaction_trigger_detection(self):
        """
        Verifies that the engine correctly detects when read-amplification
        thresholds are met to trigger compaction.
        """
        shards = ["test_compact_%d.db" % i for i in range(5)]
        self.test_files.extend(shards)
        
        mgr = memtable_manager.MemTableManager(self.layout, 1024 * 1024)
        reg = shard_registry.ShardRegistry()
        db = engine.Engine(mgr, spine.Spine([]), registry=reg, table_id=1)
        
        compaction_triggered = False
        try:
            for i, shard_file in enumerate(shards):
                # Ensure each insert is unique enough to not just collapse weights
                self._put(db, 100, 1, i * 10, "overlap_%d" % i)
                _, _, triggered = db.flush_and_rotate(shard_file)
                if triggered: 
                    compaction_triggered = True
            
            self.assertTrue(compaction_triggered, "Compaction should have been triggered")
            self.assertEqual(reg.get_read_amplification(1, 100), 5)
        finally:
            db.close()

if __name__ == '__main__':
    unittest.main()
