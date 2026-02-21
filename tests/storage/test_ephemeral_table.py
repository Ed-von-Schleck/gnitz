# tests/test_ephemeral_table.py

import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from gnitz.core import types
from gnitz.storage.ephemeral_table import EphemeralTable
from tests.row_helpers import create_test_row

class TestEphemeralTable(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_ephemeral_env"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        
        # Schema: PK(u64), Value(i64), Label(String)
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64), 
            types.ColumnDefinition(types.TYPE_I64),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        self.table = EphemeralTable(self.test_dir, "trace_01", self.layout)

    def tearDown(self):
        if not self.table.is_closed:
            self.table.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_in_memory_only_operation(self):
        """Verifies ingestion works and no persistent artifacts are created by default."""
        key = r_uint128(42)
        row = create_test_row(self.layout, [r_int64(100), "mem_only"])
        
        self.table.ingest(key, r_int64(1), row)
        
        # Verify visibility
        self.assertEqual(self.table.get_weight(key, row), 1)
        
        # Verify lack of persistent logs/manifests
        files = os.listdir(self.test_dir)
        for f in files:
            self.assertFalse(f.endswith(".wal"), "EphemeralTable should not create WAL")
            self.assertFalse(f == "MANIFEST", "EphemeralTable should not create MANIFEST")

    def test_spill_to_disk_and_cleanup(self):
        """Verifies that data survives flushes (spills) and is cleaned up on close."""
        key = r_uint128(99)
        row = create_test_row(self.layout, [r_int64(-5), "spill_test"])
        
        # 1. Ingest and flush to trigger a "spill"
        self.table.ingest(key, r_int64(2), row)
        shard_path = self.table.flush()
        
        self.assertTrue(os.path.exists(shard_path), "Temporary shard should exist on disk")
        self.assertIn("temp_trace", shard_path, "Shard should follow ephemeral naming convention")
        
        # 2. Verify weight is still correct across memory and shard
        # Add another weight in memory
        self.table.ingest(key, r_int64(1), row)
        self.assertEqual(self.table.get_weight(key, row), 3)
        
        # 3. Close and verify cleanup
        self.table.close()
        self.assertFalse(os.path.exists(shard_path), "Temporary shard should be deleted on close")

    def test_algebraic_summation(self):
        """Tests Z-Set summation (additions and retractions) in the ephemeral layer."""
        key = r_uint128(1)
        row = create_test_row(self.layout, [r_int64(500), "sum_test"])
        
        # +5
        self.table.ingest(key, r_int64(5), row)
        # -3 (retraction)
        self.table.ingest(key, r_int64(-3), row)
        
        self.assertEqual(self.table.get_weight(key, row), 2)
        
        # Flush and verify summation still works
        self.table.flush()
        self.assertEqual(self.table.get_weight(key, row), 2)

    def test_unified_cursor_navigation(self):
        """Tests that UnifiedCursor correctly merges memory and spilled shards for the VM."""
        # Key 1 in Shard
        k1 = r_uint128(1)
        r1 = create_test_row(self.layout, [r_int64(10), "a"])
        self.table.ingest(k1, r_int64(1), r1)
        self.table.flush()
        
        # Key 2 in Memory
        k2 = r_uint128(2)
        r2 = create_test_row(self.layout, [r_int64(20), "b"])
        self.table.ingest(k2, r_int64(1), r2)
        
        cursor = self.table.create_cursor()
        
        # First record (Key 1)
        self.assertTrue(cursor.is_valid())
        self.assertEqual(cursor.key(), k1)
        self.assertEqual(cursor.weight(), 1)
        
        # Advance to Key 2
        cursor.advance()
        self.assertTrue(cursor.is_valid())
        self.assertEqual(cursor.key(), k2)
        
        # End of stream
        cursor.advance()
        self.assertFalse(cursor.is_valid())
        
        cursor.close()

    def test_scratch_table_creation(self):
        """Verifies scratch tables are created correctly for recursive VM operators."""
        scratch = self.table.create_scratch_table("recursive_trace", self.layout)
        
        self.assertIsInstance(scratch, EphemeralTable)
        self.assertEqual(scratch.name, "recursive_trace")
        self.assertTrue(os.path.isdir(os.path.join(self.test_dir, "recursive_trace")))
        
        scratch.close()

    def test_ghost_annihilation_on_flush(self):
        """Verifies that records summing to zero are discarded during the spill process."""
        key = r_uint128(555)
        row = create_test_row(self.layout, [r_int64(0), "ghost"])
        
        # Add and subtract to make net weight 0
        self.table.ingest(key, r_int64(1), row)
        self.table.ingest(key, r_int64(-1), row)
        
        # Flush
        shard_path = self.table.flush()
        
        # Because the net weight was 0, the shard should either not exist or be empty
        # (PersistentTable/MemTable.flush logic discards ghosts)
        if os.path.exists(shard_path):
            from gnitz.storage.shard_table import TableShardView
            view = TableShardView(shard_path, self.layout)
            self.assertEqual(view.count, 0, "Ghost record should not be materialized in shard")
            view.close()

if __name__ == '__main__':
    unittest.main()
