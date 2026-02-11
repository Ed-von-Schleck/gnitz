import unittest
import os
from gnitz.storage import memtable, shard_table
from gnitz.core import types, values as db_values

class TestSurvivorBlobCompaction(unittest.TestCase):
    def setUp(self):
        # Schema: [String]
        self.layout = types.ComponentLayout([types.TYPE_STRING])
        self.filename = "test_blob_compaction.db"
        if os.path.exists(self.filename):
            os.unlink(self.filename)

    def tearDown(self):
        if os.path.exists(self.filename):
            os.unlink(self.filename)

    def _put(self, mgr, pk, w, *vals):
        wrapped = [db_values.wrap(v) for v in vals]
        mgr.put(pk, w, wrapped)

    def test_annihilated_blobs_are_pruned(self):
        """
        Verifies that strings belonging to annihilated entities (Weight=0)
        are NOT copied to the persistent shard's Blob Region.
        """
        # 1. Setup MemTable
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        
        # Create strings larger than 12 bytes to force Blob Heap usage
        long_str_annihilated = "DEAD" * 5  # 20 bytes
        long_str_survivor = "LIVE" * 5     # 20 bytes
        
        # 2. Insert Entity 1 (To be annihilated)
        # In MemTable, this allocates 20 bytes in blob_arena
        self._put(mgr, 1, 1, long_str_annihilated)
        
        # 3. Annihilate Entity 1
        # Net weight becomes 0. The MemTable node remains but w=0.
        self._put(mgr, 1, -1, long_str_annihilated)
        
        # 4. Insert Entity 2 (Survivor)
        # Allocates another 20 bytes in blob_arena
        self._put(mgr, 2, 1, long_str_survivor)
        
        # At this point, MemTable blob_arena holds BOTH strings (~40 bytes + overhead).
        # We assume the MemTable holds the raw data until flush.
        
        # 5. Flush to Shard
        # The transmutation pipeline should skip Entity 1 entirely.
        mgr.flush_and_rotate(self.filename)
        mgr.close()
        
        # 6. Verify Shard Structure
        view = shard_table.TableShardView(self.filename, self.layout)
        try:
            # Check Entity Count
            self.assertEqual(view.count, 1)
            # Fixed: use get_pk_u64 instead of get_primary_key
            self.assertEqual(view.get_pk_u64(0), 2)
            
            # Check Region B Size
            # Region B should strictly contain only the survivor string.
            # German String Blob Heap is just a packed byte array.
            # Expected size: len(long_str_survivor) = 20 bytes.
            # If logic failed and copied both, it would be >= 40 bytes.
            
            expected_size = len(long_str_survivor)
            actual_size = view.buf_b.size
            
            self.assertEqual(actual_size, expected_size, 
                "Blob Region size mismatch. Expected %d (Survivor only), got %d" % 
                (expected_size, actual_size))
                
            # verify content of blob (optional but good)
            # Read first 20 bytes of buf_b
            read_str = ""
            for i in range(20):
                read_str += chr(view.buf_b.read_u8(i))
            self.assertEqual(read_str, long_str_survivor)

        finally:
            view.close()

if __name__ == '__main__':
    unittest.main()
