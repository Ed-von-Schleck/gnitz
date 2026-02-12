import unittest
import os
from rpython.rtyper.lltypesystem import rffi
from gnitz.storage import wal, wal_format, errors
from gnitz.core import types, values as db_values

class TestWALWriter(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.test_wal = "test_wal.log"
        if os.path.exists(self.test_wal):
            os.unlink(self.test_wal)
    
    def tearDown(self):
        if os.path.exists(self.test_wal):
            os.unlink(self.test_wal)
    
    def test_create_wal_file(self):
        """Test that WAL file is created on initialization."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        writer.close()
        
        # File should exist
        self.assertTrue(os.path.exists(self.test_wal))
    
    def test_write_empty_block(self):
        """Test writing a block with no records."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Write empty block
        writer.append_block(1, 1, [])
        writer.close()
        
        # File should exist and have content (header only)
        self.assertTrue(os.path.exists(self.test_wal))
        
        file_size = os.path.getsize(self.test_wal)
        expected_size = wal_format.WAL_BLOCK_HEADER_SIZE
        self.assertEqual(file_size, expected_size)
    
    def test_write_single_block(self):
        """Test writing a single block with records."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Use DBValue objects as required by wal_format
        records = [wal_format.WALRecord(10, 1, [db_values.IntValue(42), db_values.StringValue("test")])]
        
        # Write block
        writer.append_block(1, 1, records)
        writer.close()
        
        # Verify file size
        file_size = os.path.getsize(self.test_wal)
        expected_size = wal_format.WAL_BLOCK_HEADER_SIZE + (8 + 8 + self.layout.stride)
        self.assertEqual(file_size, expected_size)
    
    def test_write_multiple_blocks(self):
        """Test writing multiple blocks sequentially."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Write 3 blocks
        for lsn in range(1, 4):
            recs = [db_values.IntValue(lsn * 10), db_values.StringValue("block_%d" % lsn)]
            records = [wal_format.WALRecord(lsn, 1, recs)]
            writer.append_block(lsn, 1, records)
        
        writer.close()
        
        # Verify file size (3 blocks, each with 1 record)
        file_size = os.path.getsize(self.test_wal)
        block_size = wal_format.WAL_BLOCK_HEADER_SIZE + (8 + 8 + self.layout.stride)
        expected_size = block_size * 3
        self.assertEqual(file_size, expected_size)
    
    def test_file_grows_correctly(self):
        """Test that file size grows as blocks are appended."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Write first block
        recs1 = [db_values.IntValue(1), db_values.StringValue("first")]
        records = [wal_format.WALRecord(1, 1, recs1)]
        writer.append_block(1, 1, records)
        
        size_after_first = os.path.getsize(self.test_wal)
        
        # Write second block
        recs2 = [db_values.IntValue(2), db_values.StringValue("second")]
        records = [wal_format.WALRecord(2, 1, recs2)]
        writer.append_block(2, 1, records)
        
        size_after_second = os.path.getsize(self.test_wal)
        
        # Second size should be exactly double the first (fixed-stride payload used)
        self.assertEqual(size_after_second, size_after_first * 2)
        
        writer.close()
    
    def test_fsync_enforcement(self):
        """Test that data is synced to disk (file exists after write)."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        recs = [db_values.IntValue(999), db_values.StringValue("durable")]
        records = [wal_format.WALRecord(100, 1, recs)]
        
        # Write block (should fsync)
        writer.append_block(1, 1, records)
        
        # File should exist and have content even before close
        self.assertTrue(os.path.exists(self.test_wal))
        self.assertGreater(os.path.getsize(self.test_wal), 0)
        
        writer.close()
    
    def test_read_back_written_blocks(self):
        """Test that written blocks can be read back correctly."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        for lsn in range(1, 3):
            recs = [db_values.IntValue(lsn * 100), db_values.StringValue("lsn_%d" % lsn)]
            records = [wal_format.WALRecord(lsn * 10, 1, recs)]
            writer.append_block(lsn, lsn, records)
        writer.close()
        
        with open(self.test_wal, 'rb') as f:
            data = f.read()
        
        block_size = wal_format.WAL_BLOCK_HEADER_SIZE + (8 + 8 + self.layout.stride)
        
        # Block 1
        block1_data = data[:block_size]
        p1 = rffi.str2charp(block1_data)
        try:
            lsn1, comp_id1, records1 = wal_format.decode_wal_block(p1, len(block1_data), self.layout)
            self.assertEqual(lsn1, 1)
            self.assertEqual(comp_id1, 1)
            self.assertEqual(records1[0].primary_key, 10)
            self.assertEqual(records1[0].component_data[0].get_int(), 100)
        finally:
            rffi.free_charp(p1)
        
        # Block 2
        block2_data = data[block_size:block_size * 2]
        p2 = rffi.str2charp(block2_data)
        try:
            lsn2, comp_id2, records2 = wal_format.decode_wal_block(p2, len(block2_data), self.layout)
            self.assertEqual(lsn2, 2)
            self.assertEqual(comp_id2, 2)
            self.assertEqual(records2[0].primary_key, 20)
            self.assertEqual(records2[0].component_data[0].get_int(), 200)
        finally:
            rffi.free_charp(p2)

    def test_checksums_written_correctly(self):
        """Test that checksums in written blocks are valid."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        records = []
        for i in range(5):
            recs = [db_values.IntValue(i), db_values.StringValue("rec_%d" % i)]
            records.append(wal_format.WALRecord(100 + i, 1, recs))
        writer.append_block(10, 5, records)
        writer.close()
        
        with open(self.test_wal, 'rb') as f:
            data = f.read()
        
        p = rffi.str2charp(data)
        try:
            lsn, comp_id, decoded_records = wal_format.decode_wal_block(p, len(data), self.layout)
            self.assertEqual(len(decoded_records), 5)
        finally:
            rffi.free_charp(p)
    
    def test_append_mode_preservation(self):
        """Test that O_APPEND mode preserves existing data."""
        # Write initial block
        writer1 = wal.WALWriter(self.test_wal, self.layout)
        recs1 = [db_values.IntValue(1), db_values.StringValue("first")]
        records = [wal_format.WALRecord(1, 1, recs1)]
        writer1.append_block(1, 1, records)
        writer1.close()
        
        size_after_first = os.path.getsize(self.test_wal)
        
        # Open again and append
        writer2 = wal.WALWriter(self.test_wal, self.layout)
        recs2 = [db_values.IntValue(2), db_values.StringValue("second")]
        records = [wal_format.WALRecord(2, 1, recs2)]
        writer2.append_block(2, 1, records)
        writer2.close()
        
        size_after_second = os.path.getsize(self.test_wal)
        
        # Should have both blocks
        self.assertEqual(size_after_second, size_after_first * 2)
    
    def test_write_after_close_fails(self):
        """Test that writing after close raises an error."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        writer.close()
        
        recs = [db_values.IntValue(1), db_values.StringValue("fail")]
        records = [wal_format.WALRecord(1, 1, recs)]
        
        # Should raise StorageError
        with self.assertRaises(errors.StorageError):
            writer.append_block(1, 1, records)
    
    def test_multiple_records_per_block(self):
        """Test writing blocks with varying numbers of records."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Block 1: 1 record
        records = [wal_format.WALRecord(1, 1, [db_values.IntValue(1), db_values.StringValue("one")])]
        writer.append_block(1, 1, records)
        
        # Block 2: 3 records
        records = [
            wal_format.WALRecord(2, 1, [db_values.IntValue(2), db_values.StringValue("two")]),
            wal_format.WALRecord(3, 1, [db_values.IntValue(3), db_values.StringValue("three")]),
            wal_format.WALRecord(4, 1, [db_values.IntValue(4), db_values.StringValue("four")])
        ]
        writer.append_block(2, 1, records)
        
        # Block 3: 5 records
        records = []
        for i in range(5):
            recs = [db_values.IntValue(i), db_values.StringValue("r%d" % i)]
            records.append(wal_format.WALRecord(10 + i, 1, recs))
        writer.append_block(3, 1, records)
        
        writer.close()
        
        # Verify file exists
        self.assertTrue(os.path.exists(self.test_wal))


if __name__ == '__main__':
    unittest.main()
