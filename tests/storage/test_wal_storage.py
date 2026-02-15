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
        recs = [wal_format.WALRecord.from_key(10, 1, [db_values.wrap("block1")])]
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
        writer.append_block(1, 1, [wal_format.WALRecord.from_key(1, 1, [db_values.wrap("x")])])
        self.assertGreater(os.path.getsize(self.wal_path), 0)
        
        writer.truncate_before_lsn(2) 
        self.assertEqual(os.path.getsize(self.wal_path), 0)
        writer.close()
