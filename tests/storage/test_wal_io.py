import unittest
import os
import shutil
from gnitz.core import types, values as db_values
from gnitz.storage import wal, wal_format, errors

class TestWALIO(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_wal_io_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_I64),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        self.wal_path = os.path.join(self.test_dir, "test.wal")

    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_wal_roundtrip(self):
        """Verifies multiple block appends and sequential reading."""
        writer = wal.WALWriter(self.wal_path, self.layout)
        
        # Block 1
        recs1 = [wal_format.WALRecord.from_key(10, 1, [db_values.wrap(100), db_values.wrap("block1")])]
        writer.append_block(1, 1, recs1) # LSN 1, TID 1
        
        # Block 2
        recs2 = [wal_format.WALRecord.from_key(20, -1, [db_values.wrap(200), db_values.wrap("block2")])]
        writer.append_block(2, 1, recs2) # LSN 2, TID 1
        
        writer.close()

        reader = wal.WALReader(self.wal_path, self.layout)
        blocks = list(reader.iterate_blocks())
        
        self.assertEqual(len(blocks), 2)
        self.assertEqual(blocks[0].lsn, 1)
        self.assertEqual(blocks[1].records[0].get_key(), 20)
        self.assertEqual(blocks[1].records[0].weight, -1)
        self.assertEqual(blocks[1].records[0].component_data[1].get_string(), "block2")
        reader.close()

    def test_single_writer_lock(self):
        """Ensures the WAL enforces a strict single-writer filesystem lock."""
        writer1 = wal.WALWriter(self.wal_path, self.layout)
        with self.assertRaises(errors.StorageError):
            wal.WALWriter(self.wal_path, self.layout)
        writer1.close()
        
        # Should work after close
        writer2 = wal.WALWriter(self.wal_path, self.layout)
        writer2.close()

    def test_wal_truncation(self):
        """Verifies that truncation resets the WAL for checkpointing."""
        writer = wal.WALWriter(self.wal_path, self.layout)
        writer.append_block(1, 1, [wal_format.WALRecord.from_key(1, 1, [db_values.wrap(0), db_values.wrap("tmp")])])
        self.assertGreater(os.path.getsize(self.wal_path), 0)
        
        writer.truncate_before_lsn(2) # LSN 1 is now persistent in shards
        self.assertEqual(os.path.getsize(self.wal_path), 0)
        writer.close()

if __name__ == '__main__':
    unittest.main()
