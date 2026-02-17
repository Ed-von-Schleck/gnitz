import unittest
import os
import shutil
from gnitz.core import types, values as db_values
from gnitz.storage import wal, wal_format, errors

class TestWALStorage(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_wal_storage_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        self.wal_path = os.path.join(self.test_dir, "test.wal")

    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_wal_binary_roundtrip(self):
        """Verifies binary block serialization and checksum validation."""
        writer = wal.WALWriter(self.wal_path, self.layout)
        recs = [wal_format.WALRecord.from_key(10, 1, [db_values.TaggedValue.make_string("block1")])]
        writer.append_block(1, 1, recs) # LSN 1, TID 1
        writer.close()

        reader = wal.WALReader(self.wal_path, self.layout)
        blocks = list(reader.iterate_blocks())
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].lsn, 1)
        self.assertEqual(blocks[0].records[0].get_key(), 10)
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
        writer.append_block(1, 1, [wal_format.WALRecord.from_key(1, 1, [db_values.TaggedValue.make_string("x")])])
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
        rec1 = wal_format.WALRecord.from_key(1, 1, [db_values.TaggedValue.make_string(long_str)])
        
        # 2. Short string (Inline)
        rec2 = wal_format.WALRecord.from_key(2, 1, [db_values.TaggedValue.make_string("short")])
        
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
        self.assertEqual(decoded_recs[0].get_key(), 1)
        val1 = decoded_recs[0].component_data[0]
        self.assertEqual(val1.str_val, long_str)
        
        # Verify Record 2 (This fails if the decoder offsets are wrong)
        self.assertEqual(decoded_recs[1].get_key(), 2)
        val2 = decoded_recs[1].component_data[0]
        self.assertEqual(val2.str_val, "short")
