# tests/storage/test_memtable_management.py

import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128
from gnitz.core import types
from gnitz.core.batch import make_singleton_batch
from gnitz.storage.table import PersistentTable
from gnitz.storage import shard_table, memtable
from tests.row_helpers import create_test_row

class TestMemTableManagement(unittest.TestCase):
    def setUp(self):
        self.db_dir = "test_engine_db"
        if os.path.exists(self.db_dir):
            shutil.rmtree(self.db_dir)
        os.mkdir(self.db_dir)
        
        self.fn = "test_survivor.db"
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64), 
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)

    def tearDown(self):
        if os.path.exists(self.db_dir):
            shutil.rmtree(self.db_dir)
        if os.path.exists(self.fn): 
            os.unlink(self.fn)

    def test_transmutation_roundtrip_via_engine(self):
        # Use the high-level PersistentTable, which uses the Engine internally
        db = PersistentTable(self.db_dir, "test_table", self.layout)
        try:
            # Create PayloadRows using helper
            row1 = create_test_row(self.layout, ["short"])
            db.insert(10, row1)
            
            row2 = create_test_row(self.layout, ["long_blob_payload_relocation_test"])
            db.insert(20, row2)
            
            # This triggers engine.flush_and_rotate()
            shard_filename = db.flush()
            
            self.assertTrue(os.path.exists(shard_filename))
            
            view = shard_table.TableShardView(shard_filename, self.layout)
            self.assertEqual(view.count, 2)
            self.assertEqual(view.get_pk_u64(0), 10)
            self.assertTrue(view.string_field_equals(1, 1, "long_blob_payload_relocation_test"))
            view.close()
        finally:
            db.close()

    def test_survivor_blob_pruning(self):
        """
        Verifies that blobs for annihilated records are not written to shards.
        This is a direct unit test of MemTable's flush logic.
        """
        table = memtable.MemTable(self.layout, 1024 * 1024)
        try:
            dead_str = "ANNIHILATE" * 10
            live_str = "SURVIVE" * 10
            
            dead_row = create_test_row(self.layout, [dead_str])
            live_row = create_test_row(self.layout, [live_str])
            
            # PK 1: Sums to zero
            # We must use the batch-oriented API (upsert_batch) even for single rows.
            table.upsert_batch(
                make_singleton_batch(self.layout, r_uint128(1), r_int64(1), dead_row)
            )
            table.upsert_batch(
                make_singleton_batch(self.layout, r_uint128(1), r_int64(-1), dead_row)
            )
            
            # PK 2: Survives
            table.upsert_batch(
                make_singleton_batch(self.layout, r_uint128(2), r_int64(1), live_row)
            )
            
            table.flush(self.fn, table_id=1)
            
            view = shard_table.TableShardView(self.fn, self.layout)
            # The shard should only physically contain the live blob
            self.assertEqual(view.count, 1)
            self.assertEqual(view.blob_buf.size, len(live_str))
            view.close()
        finally:
            table.free()
