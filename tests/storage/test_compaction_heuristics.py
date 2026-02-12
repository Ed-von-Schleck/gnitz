import unittest
import os
from gnitz.core import types, values as db_values
from gnitz.storage import (
    writer_table, shard_registry, manifest, 
    refcount, compactor
)

class TestCompactionHeuristics(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.manifest_fn = "test_heuristics_manifest.db"
        self.registry = shard_registry.ShardRegistry()
        self.ref_counter = refcount.RefCounter()
        self.files = [self.manifest_fn]
        self.shard_filenames = []

    def tearDown(self):
        for f in self.files:
            if os.path.exists(f): os.unlink(f)

    def _create_shard(self, filename, pk, weight, val, lsn):
        w = writer_table.TableShardWriter(self.layout)
        w.add_row_from_values(pk, weight, [db_values.wrap(x) for x in [val]])
        w.finalize(filename)
        self.files.append(filename)
        self.shard_filenames.append(filename)
        
        meta = shard_registry.ShardMetadata(filename, 1, pk, pk, lsn, lsn)
        self.registry.register_shard(meta)
        return meta

    def test_compaction_trigger_and_execution(self):
        m_mgr = manifest.ManifestManager(self.manifest_fn)
        entries = []
        for i in range(5):
            fn = "s_h_%d.db" % i
            meta = self._create_shard(fn, 100, 1, 10 * i, i)
            entries.append(manifest.ManifestEntry(1, fn, 100, 100, i, i))
        m_mgr.publish_new_version(entries)
        
        policy = compactor.CompactionPolicy(self.registry)
        self.registry.mark_for_compaction(1)
        self.assertTrue(policy.should_compact(1))
        
        new_file = compactor.execute_compaction(1, policy, m_mgr, self.ref_counter, self.layout)
        self.assertIsNotNone(new_file)
        self.files.append(new_file)
        
        reader = m_mgr.load_current()
        self.assertEqual(reader.get_entry_count(), 1)
        reader.close()
        
        self.assertEqual(len(self.registry.shards), 1)
        for fn in self.shard_filenames:
            self.assertFalse(os.path.exists(fn))

if __name__ == '__main__':
    unittest.main()
