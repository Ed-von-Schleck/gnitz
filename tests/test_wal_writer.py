import unittest
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import wal, wal_format, errors
from gnitz.core import types, strings as string_logic


class TestWALWriter(unittest.TestCase):
    def setUp(self):
        # Simple layout: [Value (I64), Label (String)]
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.test_wal = "test_wal.log"
        
        # Clean up any existing test file
        if os.path.exists(self.test_wal):
            os.unlink(self.test_wal)
    
    def tearDown(self):
        # Clean up test file
        if os.path.exists(self.test_wal):
            os.unlink(self.test_wal)
    
    def _create_packed_component(self, value, label):
        """Helper to create packed component data."""
        stride = self.layout.stride
        buf = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
        try:
            for i in range(stride):
                buf[i] = '\x00'
            
            # Pack value (i64)
            rffi.cast(rffi.LONGLONGP, buf)[0] = rffi.cast(rffi.LONGLONG, value)
            
            # Pack string
            string_ptr = rffi.ptradd(buf, 8)
            string_logic.pack_string(string_ptr, label, 0)
            
            return rffi.charpsize2str(buf, stride)
        finally:
            lltype.free(buf, flavor='raw')
    
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
        
        # Create records
        component_data = self._create_packed_component(42, "test")
        records = [wal_format.WALRecord(10, 1, component_data)]
        
        # Write block
        writer.append_block(1, 1, records)
        writer.close()
        
        # Verify file size
        file_size = os.path.getsize(self.test_wal)
        expected_size = wal_format.WAL_BLOCK_HEADER_SIZE + (16 + self.layout.stride)
        self.assertEqual(file_size, expected_size)
    
    def test_write_multiple_blocks(self):
        """Test writing multiple blocks sequentially."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Write 3 blocks
        for lsn in range(1, 4):
            component_data = self._create_packed_component(lsn * 10, "block_%d" % lsn)
            records = [wal_format.WALRecord(lsn, 1, component_data)]
            writer.append_block(lsn, 1, records)
        
        writer.close()
        
        # Verify file size (3 blocks, each with 1 record)
        file_size = os.path.getsize(self.test_wal)
        block_size = wal_format.WAL_BLOCK_HEADER_SIZE + (16 + self.layout.stride)
        expected_size = block_size * 3
        self.assertEqual(file_size, expected_size)
    
    def test_file_grows_correctly(self):
        """Test that file size grows as blocks are appended."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Write first block
        component_data = self._create_packed_component(1, "first")
        records = [wal_format.WALRecord(1, 1, component_data)]
        writer.append_block(1, 1, records)
        
        size_after_first = os.path.getsize(self.test_wal)
        
        # Write second block
        component_data = self._create_packed_component(2, "second")
        records = [wal_format.WALRecord(2, 1, component_data)]
        writer.append_block(2, 1, records)
        
        size_after_second = os.path.getsize(self.test_wal)
        
        # Second size should be exactly double the first
        self.assertEqual(size_after_second, size_after_first * 2)
        
        writer.close()
    
    def test_fsync_enforcement(self):
        """Test that data is synced to disk (file exists after write)."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        component_data = self._create_packed_component(999, "durable")
        records = [wal_format.WALRecord(100, 1, component_data)]
        
        # Write block (should fsync)
        writer.append_block(1, 1, records)
        
        # File should exist and have content even before close
        self.assertTrue(os.path.exists(self.test_wal))
        self.assertGreater(os.path.getsize(self.test_wal), 0)
        
        writer.close()
    
    def test_read_back_written_blocks(self):
        """Test that written blocks can be read back correctly."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Write 2 blocks
        for lsn in range(1, 3):
            component_data = self._create_packed_component(lsn * 100, "lsn_%d" % lsn)
            records = [wal_format.WALRecord(lsn * 10, 1, component_data)]
            writer.append_block(lsn, lsn, records)
        
        writer.close()
        
        # Read file back
        with open(self.test_wal, 'rb') as f:
            data = f.read()
        
        # Decode first block
        block_size = wal_format.WAL_BLOCK_HEADER_SIZE + (16 + self.layout.stride)
        block1_data = data[:block_size]
        lsn1, comp_id1, records1 = wal_format.decode_wal_block(block1_data, self.layout)
        
        self.assertEqual(lsn1, 1)
        self.assertEqual(comp_id1, 1)
        self.assertEqual(len(records1), 1)
        self.assertEqual(records1[0].entity_id, 10)
        
        # Decode second block
        block2_data = data[block_size:block_size * 2]
        lsn2, comp_id2, records2 = wal_format.decode_wal_block(block2_data, self.layout)
        
        self.assertEqual(lsn2, 2)
        self.assertEqual(comp_id2, 2)
        self.assertEqual(len(records2), 1)
        self.assertEqual(records2[0].entity_id, 20)
    
    def test_checksums_written_correctly(self):
        """Test that checksums in written blocks are valid."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Write block with multiple records
        records = []
        for i in range(5):
            component_data = self._create_packed_component(i, "rec_%d" % i)
            records.append(wal_format.WALRecord(100 + i, 1, component_data))
        
        writer.append_block(10, 5, records)
        writer.close()
        
        # Read and decode - should not raise checksum error
        with open(self.test_wal, 'rb') as f:
            data = f.read()
        
        # This will validate the checksum internally
        lsn, comp_id, decoded_records = wal_format.decode_wal_block(data, self.layout)
        
        self.assertEqual(len(decoded_records), 5)
    
    def test_append_mode_preservation(self):
        """Test that O_APPEND mode preserves existing data."""
        # Write initial block
        writer1 = wal.WALWriter(self.test_wal, self.layout)
        component_data = self._create_packed_component(1, "first")
        records = [wal_format.WALRecord(1, 1, component_data)]
        writer1.append_block(1, 1, records)
        writer1.close()
        
        size_after_first = os.path.getsize(self.test_wal)
        
        # Open again and append
        writer2 = wal.WALWriter(self.test_wal, self.layout)
        component_data = self._create_packed_component(2, "second")
        records = [wal_format.WALRecord(2, 1, component_data)]
        writer2.append_block(2, 1, records)
        writer2.close()
        
        size_after_second = os.path.getsize(self.test_wal)
        
        # Should have both blocks
        self.assertEqual(size_after_second, size_after_first * 2)
    
    def test_write_after_close_fails(self):
        """Test that writing after close raises an error."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        writer.close()
        
        component_data = self._create_packed_component(1, "fail")
        records = [wal_format.WALRecord(1, 1, component_data)]
        
        # Should raise StorageError
        with self.assertRaises(errors.StorageError):
            writer.append_block(1, 1, records)
    
    def test_multiple_records_per_block(self):
        """Test writing blocks with varying numbers of records."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Block 1: 1 record
        records = [wal_format.WALRecord(1, 1, self._create_packed_component(1, "one"))]
        writer.append_block(1, 1, records)
        
        # Block 2: 3 records
        records = [
            wal_format.WALRecord(2, 1, self._create_packed_component(2, "two")),
            wal_format.WALRecord(3, 1, self._create_packed_component(3, "three")),
            wal_format.WALRecord(4, 1, self._create_packed_component(4, "four"))
        ]
        writer.append_block(2, 1, records)
        
        # Block 3: 5 records
        records = []
        for i in range(5):
            records.append(wal_format.WALRecord(10 + i, 1, self._create_packed_component(i, "r%d" % i)))
        writer.append_block(3, 1, records)
        
        writer.close()
        
        # Verify file exists
        self.assertTrue(os.path.exists(self.test_wal))


if __name__ == '__main__':
    unittest.main()
