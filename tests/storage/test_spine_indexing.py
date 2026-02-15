import unittest
import os
import shutil
from gnitz.core import types, values as db_values
from gnitz.storage import writer_table, manifest, spine

class TestSpineIndexing(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_spine_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.layout = types.TableSchema([types.ColumnDefinition(types.TYPE_U64)], 0)
        self.manifest_file = os.path.join(self.test_dir, "sync.manifest")
    
    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_spine_resolution(self):
        """Verifies that the spine correctly resolves keys to specific shards."""
        shard_fn = os.path.join(self.test_dir, "shard1.db")
        writer = writer_table.TableShardWriter(self.layout, table_id=1)
        writer.add_row_from_values(10, 1, [])
        writer.finalize(shard_fn)
        
        m_writer = manifest.ManifestWriter(self.manifest_file)
        m_writer.add_entry_values(1, shard_fn, 10, 10, 0, 1)
        m_writer.finalize()
        
        sp = spine.spine_from_manifest(self.manifest_file, table_id=1, schema=self.layout)
        try:
            results = sp.find_all_shards_and_indices(10)
            self.assertEqual(len(results), 1)
            h, idx = results[0]
            self.assertEqual(h.filename, shard_fn)
        finally:
            sp.close_all()
