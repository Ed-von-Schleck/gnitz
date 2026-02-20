# tests/storage/test_wal_storage.py

import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128
from gnitz.core import types
from gnitz.core import errors
from gnitz.storage import wal, wal_format
from tests.row_helpers import create_test_row

class TestWALStorage(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_wal_storage_env"
        if os.path.exists(self.test_dir): 
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        
        # Schema: PK (U64) at index 0, String at index 1
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        self.wal_path = os.path.join(self.test_dir, "test.wal")

    def tearDown(self):
        if os.path.exists(self.test_dir): 
            shutil.rmtree(self.test_dir)

    def test_wal_binary_roundtrip(self):
        """Verifies binary block serialization and checksum validation."""
        writer = wal.WALWriter(self.wal_path, self.layout)
        
        # Construct PayloadRow and WALRecord
        row = create_test_row(self.layout, ["block1"])
        recs = [wal_format.WALRecord(r_uint128(10), r_int64(1), row)]
        
        writer.append_block(1, 1, recs) # LSN 1, TID 1
        writer.close()

        reader = wal.WALReader(self.wal_path, self.layout)
        blocks = list(reader.iterate_blocks())
        
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].lsn, 1)
        # Verify PK and data via PayloadRow API
        self.assertEqual(blocks[0].records[0].get_key(), r_uint128(10))
        self.assertEqual(blocks[0].records[0].component_data.get_str(0), "block1")
        reader.close()

    def test_single_writer_lock(self):
        """Ensures the WAL enforces a strict filesystem lock to prevent corruption."""
        writer1 = wal.WALWriter(self.wal_path, self.layout)
        with self.assertRaises(errors.StorageError):
            wal.WALWriter(self.wal_path, self.layout)
        writer1.close()

    def test_physical_truncation(self):
        """Verifies that truncation resets the file for checkpointing."""
        writer = wal.WALWriter(self.wal_path, self.layout)
        
        row = create_test_row(self.layout, ["x"])
        recs = [wal_format.WALRecord(r_uint128(1), r_int64(1), row)]
        
        writer.append_block(1, 1, recs)
        self.assertGreater(os.path.getsize(self.wal_path), 0)
        
        writer.truncate_before_lsn(2) 
        self.assertEqual(os.path.getsize(self.wal_path), 0)
        writer.close()

    def test_wal_multi_record_blob_alignment(self):
        """
        CRITICAL BUG REPRODUCTION:
        Writes a block with 2 records. The first has a long string (blob).
        The decoder must correctly skip the blob to find the second record.
        """
        writer = wal.WALWriter(self.wal_path, self.layout)
        
        # 1. Long string to force Blob allocation
        long_str = "A" * 50 
        row1 = create_test_row(self.layout, [long_str])
        rec1 = wal_format.WALRecord(r_uint128(1), r_int64(1), row1)
        
        # 2. Short string (Inline)
        row2 = create_test_row(self.layout, ["short"])
        rec2 = wal_format.WALRecord(r_uint128(2), r_int64(1), row2)
        
        # Write both in a single block
        writer.append_block(100, 1, [rec1, rec2])
        writer.close()

        # Read back
        reader = wal.WALReader(self.wal_path, self.layout)
        blocks = list(reader.iterate_blocks())
        reader.close()
        
        self.assertEqual(len(blocks), 1)
        decoded_recs = blocks[0].records
        self.assertEqual(len(decoded_recs), 2)
        
        # Verify Record 1
        self.assertEqual(decoded_recs[0].get_key(), r_uint128(1))
        self.assertEqual(decoded_recs[0].component_data.get_str(0), long_str)
        
        # Verify Record 2 (Validates that the decoder successfully skipped the blob of Rec 1)
        self.assertEqual(decoded_recs[1].get_key(), r_uint128(2))
        self.assertEqual(decoded_recs[1].component_data.get_str(0), "short")
