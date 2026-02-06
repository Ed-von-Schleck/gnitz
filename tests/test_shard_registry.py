import unittest
from gnitz.storage.shard_registry import ShardMetadata, ShardRegistry

class TestShardRegistry(unittest.TestCase):
    def setUp(self):
        self.registry = ShardRegistry()
    
    def test_register_single_shard(self):
        """Test registering a single shard."""
        meta = ShardMetadata("shard_001.db", 1, 0, 100, 0, 5)
        self.registry.register_shard(meta)
        
        self.assertEqual(len(self.registry.shards), 1)
        self.assertEqual(self.registry.shards[0].filename, "shard_001.db")
    
    def test_register_multiple_shards(self):
        """Test registering multiple shards."""
        meta1 = ShardMetadata("shard_001.db", 1, 0, 100, 0, 5)
        meta2 = ShardMetadata("shard_002.db", 1, 101, 200, 5, 10)
        meta3 = ShardMetadata("shard_003.db", 2, 0, 50, 0, 3)
        
        self.registry.register_shard(meta1)
        self.registry.register_shard(meta2)
        self.registry.register_shard(meta3)
        
        self.assertEqual(len(self.registry.shards), 3)
    
    def test_unregister_shard(self):
        """Test unregistering a shard."""
        meta1 = ShardMetadata("shard_001.db", 1, 0, 100, 0, 5)
        meta2 = ShardMetadata("shard_002.db", 1, 101, 200, 5, 10)
        
        self.registry.register_shard(meta1)
        self.registry.register_shard(meta2)
        
        # Unregister first shard
        result = self.registry.unregister_shard("shard_001.db")
        self.assertTrue(result)
        self.assertEqual(len(self.registry.shards), 1)
        self.assertEqual(self.registry.shards[0].filename, "shard_002.db")
        
        # Try to unregister non-existent shard
        result = self.registry.unregister_shard("nonexistent.db")
        self.assertFalse(result)
    
    def test_find_overlapping_shards_no_overlap(self):
        """Test finding overlapping shards with non-overlapping ranges."""
        meta1 = ShardMetadata("shard_001.db", 1, 0, 100, 0, 5)
        meta2 = ShardMetadata("shard_002.db", 1, 200, 300, 5, 10)
        
        self.registry.register_shard(meta1)
        self.registry.register_shard(meta2)
        
        # Query range between the two shards
        overlapping = self.registry.find_overlapping_shards(1, 101, 199)
        self.assertEqual(len(overlapping), 0)
    
    def test_find_overlapping_shards_single_overlap(self):
        """Test finding overlapping shards with one overlapping shard."""
        meta1 = ShardMetadata("shard_001.db", 1, 0, 100, 0, 5)
        meta2 = ShardMetadata("shard_002.db", 1, 200, 300, 5, 10)
        
        self.registry.register_shard(meta1)
        self.registry.register_shard(meta2)
        
        # Query range that overlaps first shard
        overlapping = self.registry.find_overlapping_shards(1, 50, 150)
        self.assertEqual(len(overlapping), 1)
        self.assertEqual(overlapping[0].filename, "shard_001.db")
    
    def test_find_overlapping_shards_multiple_overlap(self):
        """Test finding overlapping shards with multiple overlaps."""
        meta1 = ShardMetadata("shard_001.db", 1, 0, 100, 0, 5)
        meta2 = ShardMetadata("shard_002.db", 1, 50, 150, 5, 10)
        meta3 = ShardMetadata("shard_003.db", 1, 100, 200, 10, 15)
        
        self.registry.register_shard(meta1)
        self.registry.register_shard(meta2)
        self.registry.register_shard(meta3)
        
        # Query range that overlaps all three
        overlapping = self.registry.find_overlapping_shards(1, 75, 125)
        self.assertEqual(len(overlapping), 3)
    
    def test_find_overlapping_shards_component_filtering(self):
        """Test that component ID filtering works."""
        meta1 = ShardMetadata("shard_001.db", 1, 0, 100, 0, 5)
        meta2 = ShardMetadata("shard_002.db", 2, 0, 100, 0, 5)
        
        self.registry.register_shard(meta1)
        self.registry.register_shard(meta2)
        
        # Query component 1
        overlapping = self.registry.find_overlapping_shards(1, 0, 100)
        self.assertEqual(len(overlapping), 1)
        self.assertEqual(overlapping[0].component_id, 1)
        
        # Query component 2
        overlapping = self.registry.find_overlapping_shards(2, 0, 100)
        self.assertEqual(len(overlapping), 1)
        self.assertEqual(overlapping[0].component_id, 2)
    
    def test_read_amplification_single_shard(self):
        """Test read amplification with a single shard."""
        meta = ShardMetadata("shard_001.db", 1, 0, 100, 0, 5)
        self.registry.register_shard(meta)
        
        # Entity within shard
        amp = self.registry.get_read_amplification(1, 50)
        self.assertEqual(amp, 1)
        
        # Entity outside shard
        amp = self.registry.get_read_amplification(1, 200)
        self.assertEqual(amp, 0)
    
    def test_read_amplification_overlapping_shards(self):
        """Test read amplification with overlapping shards."""
        meta1 = ShardMetadata("shard_001.db", 1, 0, 100, 0, 5)
        meta2 = ShardMetadata("shard_002.db", 1, 50, 150, 5, 10)
        meta3 = ShardMetadata("shard_003.db", 1, 100, 200, 10, 15)
        
        self.registry.register_shard(meta1)
        self.registry.register_shard(meta2)
        self.registry.register_shard(meta3)
        
        # Entity in only first shard
        amp = self.registry.get_read_amplification(1, 25)
        self.assertEqual(amp, 1)
        
        # Entity in first two shards
        amp = self.registry.get_read_amplification(1, 75)
        self.assertEqual(amp, 2)
        
        # Entity in all three shards
        amp = self.registry.get_read_amplification(1, 100)
        self.assertEqual(amp, 3)
        
        # Entity in last two shards
        amp = self.registry.get_read_amplification(1, 125)
        self.assertEqual(amp, 2)
    
    def test_max_read_amplification(self):
        """Test calculating maximum read amplification."""
        meta1 = ShardMetadata("shard_001.db", 1, 0, 100, 0, 5)
        meta2 = ShardMetadata("shard_002.db", 1, 50, 150, 5, 10)
        meta3 = ShardMetadata("shard_003.db", 1, 100, 200, 10, 15)
        
        self.registry.register_shard(meta1)
        self.registry.register_shard(meta2)
        self.registry.register_shard(meta3)
        
        max_amp = self.registry.get_max_read_amplification(1)
        self.assertEqual(max_amp, 3)  # At entity 100
    
    def test_max_read_amplification_empty(self):
        """Test max read amplification with no shards."""
        max_amp = self.registry.get_max_read_amplification(1)
        self.assertEqual(max_amp, 0)
    
    def test_mark_for_compaction_below_threshold(self):
        """Test that compaction is not triggered below threshold."""
        meta1 = ShardMetadata("shard_001.db", 1, 0, 100, 0, 5)
        meta2 = ShardMetadata("shard_002.db", 1, 101, 200, 5, 10)
        
        self.registry.register_shard(meta1)
        self.registry.register_shard(meta2)
        
        # Read amp is 1, below default threshold of 4
        marked = self.registry.mark_for_compaction(1)
        self.assertFalse(marked)
        self.assertFalse(self.registry.needs_compaction(1))
    
    def test_mark_for_compaction_above_threshold(self):
        """Test that compaction is triggered above threshold."""
        # Create 5 overlapping shards (read amp = 5)
        for i in range(5):
            meta = ShardMetadata("shard_%03d.db" % i, 1, 0, 100, i, i+1)
            self.registry.register_shard(meta)
        
        # Read amp is 5, above default threshold of 4
        marked = self.registry.mark_for_compaction(1)
        self.assertTrue(marked)
        self.assertTrue(self.registry.needs_compaction(1))
    
    def test_mark_for_compaction_at_threshold(self):
        """Test behavior at exactly the threshold."""
        # Create 4 overlapping shards (read amp = 4)
        for i in range(4):
            meta = ShardMetadata("shard_%03d.db" % i, 1, 0, 100, i, i+1)
            self.registry.register_shard(meta)
        
        # Read amp is 4, at threshold (should not trigger)
        marked = self.registry.mark_for_compaction(1)
        self.assertFalse(marked)
    
    def test_clear_compaction_flag(self):
        """Test clearing compaction flag."""
        # Create 5 overlapping shards
        for i in range(5):
            meta = ShardMetadata("shard_%03d.db" % i, 1, 0, 100, i, i+1)
            self.registry.register_shard(meta)
        
        self.registry.mark_for_compaction(1)
        self.assertTrue(self.registry.needs_compaction(1))
        
        self.registry.clear_compaction_flag(1)
        self.assertFalse(self.registry.needs_compaction(1))
    
    def test_get_shards_for_component(self):
        """Test getting all shards for a specific component."""
        meta1 = ShardMetadata("shard_001.db", 1, 0, 100, 0, 5)
        meta2 = ShardMetadata("shard_002.db", 2, 0, 100, 0, 5)
        meta3 = ShardMetadata("shard_003.db", 1, 101, 200, 5, 10)
        
        self.registry.register_shard(meta1)
        self.registry.register_shard(meta2)
        self.registry.register_shard(meta3)
        
        # Get shards for component 1
        comp1_shards = self.registry.get_shards_for_component(1)
        self.assertEqual(len(comp1_shards), 2)
        
        # Get shards for component 2
        comp2_shards = self.registry.get_shards_for_component(2)
        self.assertEqual(len(comp2_shards), 1)
    
    def test_shards_sorted_by_min_entity_id(self):
        """Test that shards are sorted correctly."""
        meta1 = ShardMetadata("shard_001.db", 1, 200, 300, 0, 5)
        meta2 = ShardMetadata("shard_002.db", 1, 0, 100, 5, 10)
        meta3 = ShardMetadata("shard_003.db", 1, 100, 150, 10, 15)
        
        self.registry.register_shard(meta1)
        self.registry.register_shard(meta2)
        self.registry.register_shard(meta3)
        
        # Should be sorted by min_entity_id
        comp1_shards = self.registry.get_shards_for_component(1)
        self.assertEqual(comp1_shards[0].min_entity_id, 0)
        self.assertEqual(comp1_shards[1].min_entity_id, 100)
        self.assertEqual(comp1_shards[2].min_entity_id, 200)

if __name__ == '__main__':
    unittest.main()
