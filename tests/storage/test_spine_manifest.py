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
    
    def _create_shard(self, filename, entities_and_values):
        writer = writer_table.TableShardWriter(self.layout)
        for pk, i64_val, str_val in entities_and_values:
            writer.add_row_from_values(pk, 1, [
                db_values.wrap(i64_val),
                db_values.wrap(str_val)
            ])
        writer.finalize(filename)
        self.shard_files.append(filename)
    
    def test_load_spine_with_single_shard(self):
        shard_fn = "shard_single.db"
        self._create_shard(shard_fn, [(10, 100, "alpha"), (20, 200, "beta")])
        
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry_values(1, shard_fn, 10, 20, 0, 1)
        writer.finalize()
        
        sp = spine.Spine.from_manifest(
            self.manifest_file, 
            table_id=1, 
            schema=self.layout,
            ref_counter=None,
            validate_checksums=False
        )
        
        self.assertEqual(sp.handles[0].min_key, 10)
        
        results = sp.find_all_shards_and_indices(20)
        shard, idx = results[0]
        self.assertEqual(shard.view.read_field_i64(idx, 1), 200)
        
        sp.close_all()
