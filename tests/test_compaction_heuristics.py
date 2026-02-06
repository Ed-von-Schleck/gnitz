"""
tests/test_compaction_heuristics.py
"""
import unittest
import os
from gnitz.core import types
from gnitz.storage import (
    writer_ecs, shard_registry, manifest, 
    refcount, compactor
)

class TestCompactionHeuristics(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.manifest_fn = "test_heuristics_manifest.db"
        self.registry = shard_registry.ShardRegistry()
        self.ref_counter = refcount.RefCounter()
        self.files = [self.manifest_fn]

    def tearDown(self):
        for f in self.files:
            if os.path.exists(f): os.unlink(f)

    def _create_shard(self, filename, eid, weight, val, lsn):
        w = writer_ecs.ECSShardWriter(self.layout)
        w._add_entity_weighted(eid, weight, val)
        w.finalize(filename)
        self.files.append(filename)
        
        meta = shard_registry.ShardMetadata(filename, 1, eid, eid, lsn, lsn)
        self.registry.register_shard(meta)
        return meta

    def test_compaction_trigger_and_execution(self):
        # 1. Setup Manifest with 5 overlapping shards to trigger threshold (default 4)
        m_mgr = manifest.ManifestManager(self.manifest_fn)
        entries = []
        
        for i in range(5):
            fn = "s_h_%d.db" % i
            meta = self._create_shard(fn, 100, 1, 10 * i, i)
            entries.append(manifest.ManifestEntry(1, fn, 100, 100, i, i))
        
        m_mgr.publish_new_version(entries)
        
        # 2. Verify Trigger
        policy = compactor.CompactionPolicy(self.registry)
        self.assertFalse(self.registry.needs_compaction(1))
        
        # Check amplification - entity 100 is in 5 shards
        self.assertEqual(self.registry.get_read_amplification(1, 100), 5)
        
        # Mark and check
        self.registry.mark_for_compaction(1)
        self.assertTrue(policy.should_compact(1))
        
        # 3. Execute Compaction
        new_file = compactor.execute_compaction(1, policy, m_mgr, self.ref_counter, self.layout)
        self.assertIsNotNone(new_file)
        self.files.append(new_file)
        
        # 4. Verify Manifest State
        reader = m_mgr.load_current()
        self.assertEqual(reader.get_entry_count(), 1) # 5 merged into 1
        entry = reader.read_entry(0)
        self.assertEqual(entry.shard_filename, new_file)
        self.assertEqual(entry.min_lsn, 0)
        self.assertEqual(entry.max_lsn, 4)
        reader.close()
        
        # 5. Verify Registry State
        self.assertEqual(len(self.registry.shards), 1)
        self.assertFalse(self.registry.needs_compaction(1))
        self.assertEqual(self.registry.get_read_amplification(1, 100), 1)
        
        # 6. Verify Deletion Marking
        self.assertTrue(self.ref_counter.get_refcount("s_h_0.db") == 0)
        # It's marked for deletion, so try_cleanup would remove it if it exists
        deleted = self.ref_counter.try_cleanup()
        self.assertEqual(len(deleted), 5)

if __name__ == '__main__':
    unittest.main()
