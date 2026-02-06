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
        # Clean up all test files
        if os.path.exists(self.manifest_file):
            os.unlink(self.manifest_file)
        for fn in self.shard_files:
            if os.path.exists(fn):
                os.unlink(fn)
    
    def _create_shard(self, filename, entities_and_values):
        """
        Helper to create a shard file with specified entities.
        entities_and_values: list of (entity_id, i64_value, string_value)
        """
        writer = writer_ecs.ECSShardWriter(self.layout)
        for eid, i64_val, str_val in entities_and_values:
            writer.add_entity(eid, i64_val, str_val)
        writer.finalize(filename)
        self.shard_files.append(filename)
    
    def test_load_spine_from_empty_manifest(self):
        """Test loading a Spine from a manifest with no entries."""
        # Create empty manifest
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.finalize()
        
        # Load Spine for component_id=1
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        
        self.assertEqual(sp.shard_count, 0)
        self.assertEqual(len(sp.handles), 0)
        
        sp.close_all()
    
    def test_load_spine_with_single_shard(self):
        """Test loading a Spine with one shard."""
        # Create a shard
        shard_fn = "shard_single.db"
        self._create_shard(shard_fn, [
            (10, 100, "alpha"),
            (20, 200, "beta"),
            (30, 300, "gamma"),
        ])
        
        # Create manifest referencing this shard
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry(
            component_id=1,
            shard_filename=shard_fn,
            min_eid=10,
            max_eid=30,
            min_lsn=0,
            max_lsn=1
        )
        writer.finalize()
        
        # Load Spine
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        
        self.assertEqual(sp.shard_count, 1)
        self.assertEqual(sp.min_eids[0], 10)
        self.assertEqual(sp.max_eids[0], 30)
        
        # Verify we can query entities
        shard, idx = sp.find_shard_and_index(20)
        self.assertIsNotNone(shard)
        self.assertEqual(shard.read_field_i64(idx, 0), 200)
        
        sp.close_all()
    
    def test_load_spine_with_multiple_shards(self):
        """Test loading a Spine with multiple shards."""
        # Create three shards with different entity ranges
        shard1_fn = "shard_multi_1.db"
        self._create_shard(shard1_fn, [
            (10, 100, "a"),
            (20, 200, "b"),
        ])
        
        shard2_fn = "shard_multi_2.db"
        self._create_shard(shard2_fn, [
            (30, 300, "c"),
            (40, 400, "d"),
        ])
        
        shard3_fn = "shard_multi_3.db"
        self._create_shard(shard3_fn, [
            (50, 500, "e"),
            (60, 600, "f"),
        ])
        
        # Create manifest with all three shards
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry(1, shard1_fn, 10, 20, 0, 1)
        writer.add_entry(1, shard2_fn, 30, 40, 1, 2)
        writer.add_entry(1, shard3_fn, 50, 60, 2, 3)
        writer.finalize()
        
        # Load Spine
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        
        self.assertEqual(sp.shard_count, 3)
        
        # Verify entity ranges
        self.assertEqual(sp.min_eids[0], 10)
        self.assertEqual(sp.max_eids[0], 20)
        self.assertEqual(sp.min_eids[1], 30)
        self.assertEqual(sp.max_eids[1], 40)
        self.assertEqual(sp.min_eids[2], 50)
        self.assertEqual(sp.max_eids[2], 60)
        
        # Query entities from different shards
        shard, idx = sp.find_shard_and_index(10)
        self.assertIsNotNone(shard)
        self.assertEqual(shard.read_field_i64(idx, 0), 100)
        
        shard, idx = sp.find_shard_and_index(40)
        self.assertIsNotNone(shard)
        self.assertEqual(shard.read_field_i64(idx, 0), 400)
        
        shard, idx = sp.find_shard_and_index(60)
        self.assertIsNotNone(shard)
        self.assertEqual(shard.read_field_i64(idx, 0), 600)
        
        sp.close_all()
    
    def test_component_id_filtering(self):
        """Test that Spine only loads shards for the specified component_id."""
        # Create shards for different components
        shard_comp1_a = "shard_comp1_a.db"
        self._create_shard(shard_comp1_a, [(10, 100, "comp1a")])
        
        shard_comp1_b = "shard_comp1_b.db"
        self._create_shard(shard_comp1_b, [(20, 200, "comp1b")])
        
        shard_comp2 = "shard_comp2.db"
        self._create_shard(shard_comp2, [(30, 300, "comp2")])
        
        shard_comp3 = "shard_comp3.db"
        self._create_shard(shard_comp3, [(40, 400, "comp3")])
        
        # Create manifest with mixed component IDs
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry(1, shard_comp1_a, 10, 10, 0, 1)
        writer.add_entry(2, shard_comp2, 30, 30, 0, 1)
        writer.add_entry(1, shard_comp1_b, 20, 20, 1, 2)
        writer.add_entry(3, shard_comp3, 40, 40, 0, 1)
        writer.finalize()
        
        # Load Spine for component_id=1 only
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        
        # Should only have 2 shards (both with component_id=1)
        self.assertEqual(sp.shard_count, 2)
        
        # Verify we can find entities from component 1
        shard, idx = sp.find_shard_and_index(10)
        self.assertIsNotNone(shard)
        
        shard, idx = sp.find_shard_and_index(20)
        self.assertIsNotNone(shard)
        
        # Entities from other components should not be found
        shard, idx = sp.find_shard_and_index(30)
        self.assertIsNone(shard)
        
        shard, idx = sp.find_shard_and_index(40)
        self.assertIsNone(shard)
        
        sp.close_all()
        
        # Now load for component_id=2
        sp2 = spine.Spine.from_manifest(self.manifest_file, component_id=2, layout=self.layout)
        
        self.assertEqual(sp2.shard_count, 1)
        
        shard, idx = sp2.find_shard_and_index(30)
        self.assertIsNotNone(shard)
        
        sp2.close_all()
    
    def test_overlapping_shards(self):
        """Test loading Spine with overlapping entity ranges."""
        # Create shards with overlapping ranges
        shard1 = "shard_overlap_1.db"
        self._create_shard(shard1, [
            (10, 100, "s1_10"),
            (20, 200, "s1_20"),
            (30, 300, "s1_30"),
        ])
        
        shard2 = "shard_overlap_2.db"
        self._create_shard(shard2, [
            (25, 250, "s2_25"),
            (35, 350, "s2_35"),
        ])
        
        # Create manifest
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry(1, shard1, 10, 30, 0, 1)
        writer.add_entry(1, shard2, 25, 35, 1, 2)
        writer.finalize()
        
        # Load Spine
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        
        self.assertEqual(sp.shard_count, 2)
        
        # Entity 20 is only in shard1
        shard, idx = sp.find_shard_and_index(20)
        self.assertIsNotNone(shard)
        self.assertEqual(shard.read_field_i64(idx, 0), 200)
        
        # Entity 25 is only in shard2 (even though both shards overlap this range)
        shard, idx = sp.find_shard_and_index(25)
        self.assertIsNotNone(shard)
        self.assertEqual(shard.read_field_i64(idx, 0), 250)
        
        sp.close_all()
    
    def test_manifest_with_no_matching_component(self):
        """Test loading Spine when no shards match the component_id."""
        # Create shard for component 5
        shard_fn = "shard_other_comp.db"
        self._create_shard(shard_fn, [(10, 100, "other")])
        
        writer = manifest.ManifestWriter(self.manifest_file)
        writer.add_entry(5, shard_fn, 10, 10, 0, 1)
        writer.finalize()
        
        # Try to load for component_id=1 (doesn't exist)
        sp = spine.Spine.from_manifest(self.manifest_file, component_id=1, layout=self.layout)
        
        # Should result in empty Spine
        self.assertEqual(sp.shard_count, 0)
        
        sp.close_all()

if __name__ == '__main__':
    unittest.main()
