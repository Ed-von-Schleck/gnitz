import unittest
import os
from gnitz.core import types, values as db_values
from gnitz.storage import writer_table, manifest, spine

class TestSpineManifest(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.manifest_file = "test_spine_manifest.db"
        self.shard_files = []
    
    def tearDown(self):
        if os.path.exists(self.manifest_file):
            os.unlink(self.manifest_file)
        for fn in self.shard_files:
            if os.path.exists(fn):
                os.unlink(fn)
    
    def _create_shard(self, filename, records):
        writer = writer_table.TableShardWriter(self.layout)
        for pk, i64_val, str_val in records:
            writer.add_row_from_values(pk, 1, [
                db_values.wrap(i64_val),
                db_values.wrap(str_val)
            ])
        writer.finalize(filename)
        self.shard_files.append(filename)
    
    def test_load_spine_from_manifest(self):
        """Reconstructed spine verification using the split-key API."""
        shard_fn = "shard_spine_test.db"
        self._create_shard(shard_fn, [
            (10, 100, "alpha"), 
            (20, 200, "beta")
        ])
        
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry_values(1, shard_fn, 10, 20, 0, 1)
        writer.finalize()
        
        sp = spine.Spine.from_manifest(
            self.manifest_file, 
            table_id=1, 
            schema=self.layout
        )
        
        try:
            self.assertEqual(len(sp.handles), 1)
            # FIXED: Call get_min_key() instead of accessing .min_key() attribute
            self.assertEqual(sp.handles[0].get_min_key(), 10)
            
            results = sp.find_all_shards_and_indices(20)
            self.assertEqual(len(results), 1)
            
            h, idx = results[0]
            self.assertEqual(h.view.read_field_i64(idx, 1), 200)
            
        finally:
            sp.close_all()

if __name__ == '__main__':
    unittest.main()
