import unittest
import os
import shutil
from gnitz.core import zset, types, values as db_values

class TestSemanticMerges(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_semantic_merge_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.layout = types.ComponentLayout([types.TYPE_STRING])
        self.db = zset.PersistentZSet(self.test_dir, "semantic", self.layout)

    def tearDown(self):
        self.db.close()
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_cross_shard_relocation(self):
        """
        Creates two shards where the same string has different offsets in Region B.
        Verifies that compaction correctly coalesces them and relocates data.
        """
        long_str = "this_string_is_very_long_and_will_be_relocated_multiple_times"
        p = [db_values.StringValue(long_str)]
        
        # 1. Shard 1: Entity 100 has Weight +1
        self.db.insert(100, p)
        self.db.flush()
        
        # 2. Add some "noise" data to ensure Shard 2 has a different Region B layout
        self.db.insert(50, [db_values.StringValue("some_other_blob_data_here")])
        self.db.flush()
        
        # 3. Shard 3: Entity 100 has Weight +1 again
        self.db.insert(100, p)
        self.db.flush()
        
        # 4. Total weight should be 2
        self.assertEqual(self.db.get_weight(100, p), 2)
        
        # 5. Trigger Compaction (Merges 3 shards into 1 Guard Shard)
        self.db._trigger_compaction()
        
        # 6. Verify result
        self.assertEqual(self.db.get_weight(100, p), 2)
        
        # 7. Check physical entry count (Should be 2 entries: Entity 50 and Entity 100)
        reader = self.db.manifest_manager.load_current()
        # Should have 1 shard entry in manifest after compaction
        self.assertEqual(reader.get_entry_count(), 1)
        reader.close()

    def test_ghost_purging_in_compaction(self):
        """
        Verifies that records summing to zero are physically removed 
        from shards during compaction.
        """
        p = [db_values.StringValue("purge_me")]
        
        # Shard 1: Weight +1
        self.db.insert(200, p)
        self.db.flush()
        
        # Shard 2: Weight -1 (Annihilation)
        self.db.remove(200, p)
        self.db.flush()
        
        self.assertEqual(self.db.get_weight(200, p), 0)
        
        # Trigger compaction
        self.db._trigger_compaction()
        
        # Net weight is 0. Guard shard should be essentially empty (header only)
        self.assertEqual(self.db.get_weight(200, p), 0)
        
        # Verification of zero entries
        reader = self.db.manifest_manager.load_current()
        self.assertEqual(reader.get_entry_count(), 1)
        entries = list(reader.iterate_entries())
        entry = entries[0]
        
        # Entry range should still exist, but we check if the file has 0 records
        from gnitz.storage import shard_table
        view = shard_table.TableShardView(entry.shard_filename, self.layout)
        self.assertEqual(view.count, 0)
        view.close()
        reader.close()

if __name__ == '__main__':
    unittest.main()
