import unittest
import os
import shutil
from gnitz.core import types, values as db_values
from gnitz.storage import writer_table, manifest, index, refcount

class TestShardIndexing(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_index_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.layout = types.TableSchema([types.ColumnDefinition(types.TYPE_U64)], 0)
        self.manifest_file = os.path.join(self.test_dir, "sync.manifest")
    
    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_index_resolution(self):
        """Verifies that the index correctly resolves keys to specific shards."""
        shard_fn = os.path.join(self.test_dir, "shard1.db")
        writer = writer_table.TableShardWriter(self.layout, table_id=1)
        writer.add_row_from_values(10, 1, [])
        writer.finalize(shard_fn)
        
        m_writer = manifest.ManifestWriter(self.manifest_file)
        # Entry: tid=1, file, min_k=10, max_k=10, min_lsn=0, max_lsn=1
        m_writer.add_entry_values(1, shard_fn, 10, 10, 0, 1)
        m_writer.finalize()
        
        rc = refcount.RefCounter()
        idx = index.index_from_manifest(self.manifest_file, table_id=1, schema=self.layout, ref_counter=rc)
        try:
            results = idx.find_all_shards_and_indices(10)
            self.assertEqual(len(results), 1)
            h, row_idx = results[0]
            self.assertEqual(h.filename, shard_fn)
            self.assertEqual(row_idx, 0)
        finally:
            idx.close_all()

if __name__ == '__main__':
    unittest.main()
