import unittest
import os
from gnitz.storage import memtable, shard_table
from gnitz.core import types, values as db_values

class TestSurvivorBlobCompaction(unittest.TestCase):
    def setUp(self):
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        self.filename = "test_blob_compaction.db"

    def test_annihilated_blobs_are_pruned(self):
        table = memtable.MemTable(self.layout, 1024 * 1024)
        long_str_annihilated = "DEAD" * 10
        long_str_survivor = "LIVE" * 10
        
        # Insert and Annihilate 1
        table.upsert(1, 1, [db_values.StringValue(long_str_annihilated)])
        table.upsert(1, -1, [db_values.StringValue(long_str_annihilated)])
        
        # Survivor 2
        table.upsert(2, 1, [db_values.StringValue(long_str_survivor)])
        
        # Flush
        table.flush(self.filename, table_id=1)
        
        view = shard_table.TableShardView(self.filename, self.layout)
        try:
            self.assertEqual(view.count, 1)
            # Region B should only contain the 40 bytes of the survivor
            self.assertEqual(view.buf_b.size, 40) 
        finally:
            view.close()
            table.free()
