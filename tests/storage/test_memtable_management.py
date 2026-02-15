import unittest
import os
from gnitz.core import types, values as db_values
from gnitz.storage import memtable_manager, shard_table, memtable

class TestMemTableManagement(unittest.TestCase):
    def setUp(self):
        self.fn = "test_mt_mgmt.db"
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64), 
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)

    def tearDown(self):
        if os.path.exists(self.fn): os.unlink(self.fn)

    def test_transmutation_roundtrip(self):
        mgr = memtable_manager.MemTableManager(self.layout, 1024 * 1024)
        try:
            mgr.put(10, 1, [db_values.StringValue("short")])
            mgr.put(20, 1, [db_values.StringValue("long_blob_payload_relocation_test")])
            mgr.flush_and_rotate(self.fn)
            
            view = shard_table.TableShardView(self.fn, self.layout)
            self.assertEqual(view.count, 2)
            self.assertEqual(view.get_pk_u64(0), 10)
            self.assertTrue(view.string_field_equals(1, 1, "long_blob_payload_relocation_test"))
            view.close()
        finally:
            mgr.close()

    def test_survivor_blob_pruning(self):
        """Verifies that blobs for annihilated records are not written to shards."""
        table = memtable.MemTable(self.layout, 1024 * 1024)
        try:
            dead_str = "ANNIHILATE" * 10
            live_str = "SURVIVE" * 10
            
            # PK 1: Sums to zero
            table.upsert(1, 1, [db_values.StringValue(dead_str)])
            table.upsert(1, -1, [db_values.StringValue(dead_str)])
            
            # PK 2: Survives
            table.upsert(2, 1, [db_values.StringValue(live_str)])
            
            table.flush(self.fn, table_id=1)
            
            view = shard_table.TableShardView(self.fn, self.layout)
            # The shard should only physically contain the live blob
            self.assertEqual(view.count, 1)
            self.assertEqual(view.blob_buf.size, len(live_str))
            view.close()
        finally:
            table.free()
