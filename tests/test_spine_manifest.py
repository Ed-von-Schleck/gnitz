import unittest
import os
from gnitz.core import types
from gnitz.storage import writer_ecs, manifest, spine

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
        writer = writer_ecs.ECSShardWriter(self.layout)
        for eid, i64_val, str_val in entities_and_values:
            writer.add_entity(eid, i64_val, str_val)
        writer.finalize(filename)
        self.shard_files.append(filename)
    
    def test_load_spine_from_empty_manifest(self):
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.finalize()
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        self.assertEqual(sp.count, 0)  # Fixed: shard_count -> count
        self.assertEqual(len(sp.handles), 0)
        sp.close_all()
    
    def test_load_spine_with_single_shard(self):
        shard_fn = "shard_single.db"
        self._create_shard(shard_fn, [(10, 100, "alpha"), (20, 200, "beta"), (30, 300, "gamma")])
        
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry(1, shard_fn, 10, 30, 0, 1)
        writer.finalize()
        
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        
        self.assertEqual(sp.count, 1) # Fixed: shard_count -> count
        self.assertEqual(sp.handles[0].min_eid, 10)
        self.assertEqual(sp.handles[0].max_eid, 30)
        
        results = sp.find_all_shards_and_indices(20) # Updated API usage
        self.assertEqual(len(results), 1)
        shard, idx = results[0]
        self.assertEqual(shard.read_field_i64(idx, 0), 200)
        
        sp.close_all()
    
    def test_load_spine_with_multiple_shards(self):
        shard1_fn = "shard_multi_1.db"
        self._create_shard(shard1_fn, [(10, 100, "a"), (20, 200, "b")])
        shard2_fn = "shard_multi_2.db"
        self._create_shard(shard2_fn, [(30, 300, "c"), (40, 400, "d")])
        shard3_fn = "shard_multi_3.db"
        self._create_shard(shard3_fn, [(50, 500, "e"), (60, 600, "f")])
        
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry(1, shard1_fn, 10, 20, 0, 1)
        writer.add_entry(1, shard2_fn, 30, 40, 1, 2)
        writer.add_entry(1, shard3_fn, 50, 60, 2, 3)
        writer.finalize()
        
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        self.assertEqual(sp.count, 3) # Fixed
        
        self.assertEqual(sp.handles[0].min_eid, 10)
        self.assertEqual(sp.handles[0].max_eid, 20)
        
        results = sp.find_all_shards_and_indices(10)
        self.assertEqual(results[0][0].read_field_i64(results[0][1], 0), 100)

        results = sp.find_all_shards_and_indices(40)
        self.assertEqual(results[0][0].read_field_i64(results[0][1], 0), 400)
        
        sp.close_all()
    
    def test_component_id_filtering(self):
        shard_comp1_a = "shard_comp1_a.db"
        self._create_shard(shard_comp1_a, [(10, 100, "comp1a")])
        shard_comp1_b = "shard_comp1_b.db"
        self._create_shard(shard_comp1_b, [(20, 200, "comp1b")])
        shard_comp2 = "shard_comp2.db"
        self._create_shard(shard_comp2, [(30, 300, "comp2")])
        shard_comp3 = "shard_comp3.db"
        self._create_shard(shard_comp3, [(40, 400, "comp3")])
        
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry(1, shard_comp1_a, 10, 10, 0, 1)
        writer.add_entry(2, shard_comp2, 30, 30, 0, 1)
        writer.add_entry(1, shard_comp1_b, 20, 20, 1, 2)
        writer.add_entry(3, shard_comp3, 40, 40, 0, 1)
        writer.finalize()
        
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        self.assertEqual(sp.count, 2) # Fixed
        
        self.assertEqual(len(sp.find_all_shards_and_indices(10)), 1)
        self.assertEqual(len(sp.find_all_shards_and_indices(30)), 0)
        
        sp.close_all()
        
        sp2 = spine.Spine.from_manifest(self.manifest_file, component_id=2, layout=self.layout)
        self.assertEqual(sp2.count, 1)
        self.assertEqual(len(sp2.find_all_shards_and_indices(30)), 1)
        sp2.close_all()
    
    def test_overlapping_shards(self):
        shard1 = "shard_overlap_1.db"
        self._create_shard(shard1, [(10, 100, "s1_10"), (20, 200, "s1_20"), (30, 300, "s1_30")])
        shard2 = "shard_overlap_2.db"
        self._create_shard(shard2, [(25, 250, "s2_25"), (35, 350, "s2_35")])
        
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry(1, shard1, 10, 30, 0, 1)
        writer.add_entry(1, shard2, 25, 35, 1, 2)
        writer.finalize()
        
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        self.assertEqual(sp.count, 2)
        
        # Entity 20 is only in shard1
        res = sp.find_all_shards_and_indices(20)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][0].read_field_i64(res[0][1], 0), 200)
        
        # Entity 25 is only in shard2 (despite overlap range)
        res = sp.find_all_shards_and_indices(25)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][0].read_field_i64(res[0][1], 0), 250)
        
        sp.close_all()

    def test_find_all_shards_and_indices(self):
        shard1 = "shard_overlap_1.db"
        self._create_shard(shard1, [(10, 100, "val1")])
        shard2 = "shard_overlap_2.db"
        self._create_shard(shard2, [(10, 200, "val2")])
        
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry(1, shard1, 10, 10, 0, 1)
        writer.add_entry(1, shard2, 10, 10, 1, 2)
        writer.finalize()
        
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        results = sp.find_all_shards_and_indices(10)
        self.assertEqual(len(results), 2)
        
        handle1, idx1 = results[0]
        handle2, idx2 = results[1]
        
        val1 = handle1.read_field_i64(idx1, 0)
        val2 = handle2.read_field_i64(idx2, 0)
        values = sorted([val1, val2])
        self.assertEqual(values, [100, 200])
        sp.close_all()
    
    def test_manifest_with_no_matching_component(self):
        shard_fn = "shard_other_comp.db"
        self._create_shard(shard_fn, [(10, 100, "other")])
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry(5, shard_fn, 10, 10, 0, 1)
        writer.finalize()
        
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        self.assertEqual(sp.count, 0)
        sp.close_all()

if __name__ == '__main__':
    unittest.main()
