import unittest
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import wal, wal_format, errors
from gnitz.core import types, strings as string_logic


class TestWALReader(unittest.TestCase):
    def setUp(self):
        # Simple layout: [Value (I64), Label (String)]
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.test_wal = "test_reader.log"
        
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
    
    def _write_test_wal(self, num_blocks):
        """Helper to write a test WAL with specified number of blocks."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        for lsn in range(1, num_blocks + 1):
            component_data = self._create_packed_component(lsn * 10, "block_%d" % lsn)
            records = [wal_format.WALRecord(lsn * 100, 1, component_data)]
            writer.append_block(lsn, 1, records)
        writer.close()
    
    def test_read_empty_wal(self):
        """Test reading from an empty WAL (EOF immediately)."""
        # Create empty WAL
        writer = wal.WALWriter(self.test_wal, self.layout)
        writer.close()
        
        # Read from it
        reader = wal.WALReader(self.test_wal, self.layout)
        is_valid, _, _, _ = reader.read_next_block()
        
        # Should get False (EOF)
        self.assertFalse(is_valid)
        reader.close()
    
    def test_read_single_block(self):
        """Test reading a WAL with a single block."""
        self._write_test_wal(1)
        
        # Read it back
        reader = wal.WALReader(self.test_wal, self.layout)
        is_valid, lsn, component_id, records = reader.read_next_block()
        
        # Should get the block
        self.assertTrue(is_valid)
        
        self.assertEqual(lsn, 1)
        self.assertEqual(component_id, 1)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0][0], 100) # entity_id
        self.assertEqual(records[0][1], 1)   # weight
        
        # Next read should be EOF
        is_valid, _, _, _ = reader.read_next_block()
        self.assertFalse(is_valid)
        
        reader.close()
    
    def test_read_three_blocks_sequentially(self):
        """Test reading 3 blocks in sequence."""
        self._write_test_wal(3)
        
        reader = wal.WALReader(self.test_wal, self.layout)
        
        # Read block 1
        is_valid1, lsn1, comp_id1, records1 = reader.read_next_block()
        self.assertTrue(is_valid1)
        self.assertEqual(lsn1, 1)
        self.assertEqual(records1[0][0], 100) # entity_id
        
        # Read block 2
        is_valid2, lsn2, comp_id2, records2 = reader.read_next_block()
        self.assertTrue(is_valid2)
        self.assertEqual(lsn2, 2)
        self.assertEqual(records2[0][0], 200) # entity_id
        
        # Read block 3
        is_valid3, lsn3, comp_id3, records3 = reader.read_next_block()
        self.assertTrue(is_valid3)
        self.assertEqual(lsn3, 3)
        self.assertEqual(records3[0][0], 300) # entity_id
        
        # EOF
        is_valid_eof, _, _, _ = reader.read_next_block()
        self.assertFalse(is_valid_eof)
        
        reader.close()
    
    def test_lsn_sequence_validation(self):
        """Test that LSN sequence is preserved correctly."""
        # Write blocks with specific LSN sequence
        writer = wal.WALWriter(self.test_wal, self.layout)
        lsn_sequence = [1, 5, 10, 15, 100]
        
        for lsn in lsn_sequence:
            component_data = self._create_packed_component(lsn, "lsn_%d" % lsn)
            records = [wal_format.WALRecord(lsn, 1, component_data)]
            writer.append_block(lsn, 1, records)
        writer.close()
        
        # Read back and verify sequence
        reader = wal.WALReader(self.test_wal, self.layout)
        read_lsns = []
        
        while True:
            is_valid, lsn, _, _ = reader.read_next_block()
            if not is_valid:
                break
            read_lsns.append(lsn)
        
        self.assertEqual(read_lsns, lsn_sequence)
        reader.close()
    
    def test_checksum_validation_pass(self):
        """Test that valid blocks pass checksum validation."""
        self._write_test_wal(5)
        
        # Read all blocks - should not raise
        reader = wal.WALReader(self.test_wal, self.layout)
        count = 0
        while True:
            is_valid, _, _, _ = reader.read_next_block()
            if not is_valid:
                break
            count += 1
        
        self.assertEqual(count, 5)
        reader.close()
    
    def test_checksum_validation_fail(self):
        """Test that corrupted blocks raise CorruptShardError."""
        self._write_test_wal(1)
        
        # Corrupt the WAL file (flip a bit in the body)
        with open(self.test_wal, 'rb') as f:
            data = f.read()
        
        data_list = list(data)
        # Corrupt a byte in the body (entity ID)
        corrupt_idx = wal_format.WAL_BLOCK_HEADER_SIZE + 5
        data_list[corrupt_idx] = chr(ord(data_list[corrupt_idx]) ^ 0xFF)
        
        with open(self.test_wal, 'wb') as f:
            f.write(''.join(data_list))
        
        # Try to read - should raise CorruptShardError
        reader = wal.WALReader(self.test_wal, self.layout)
        with self.assertRaises(errors.CorruptShardError):
            reader.read_next_block()
        
        reader.close()
    
    def test_truncated_header_detection(self):
        """Test that truncated headers are detected."""
        # Write a valid block
        self._write_test_wal(1)
        
        # Truncate the file to only 16 bytes (incomplete header)
        with open(self.test_wal, 'rb') as f:
            data = f.read(16)
        
        with open(self.test_wal, 'wb') as f:
            f.write(data)
        
        # Try to read - should raise CorruptShardError
        reader = wal.WALReader(self.test_wal, self.layout)
        with self.assertRaises(errors.CorruptShardError):
            reader.read_next_block()
        
        reader.close()
    
    def test_truncated_body_detection(self):
        """Test that truncated bodies are detected."""
        # Write a valid block
        self._write_test_wal(1)
        
        # Truncate the file to header + partial body
        truncate_size = wal_format.WAL_BLOCK_HEADER_SIZE + 10
        with open(self.test_wal, 'rb') as f:
            data = f.read(truncate_size)
        
        with open(self.test_wal, 'wb') as f:
            f.write(data)
        
        # Try to read - should raise CorruptShardError
        reader = wal.WALReader(self.test_wal, self.layout)
        with self.assertRaises(errors.CorruptShardError):
            reader.read_next_block()
        
        reader.close()
    
    def test_iterate_blocks_generator(self):
        """Test the iterate_blocks generator interface."""
        self._write_test_wal(4)
        
        reader = wal.WALReader(self.test_wal, self.layout)
        
        # Collect all blocks via generator
        # The generator handles the is_valid logic internally, yielding 3-tuples
        blocks = []
        for lsn, component_id, records in reader.iterate_blocks():
            blocks.append((lsn, component_id, records))
        
        # Should have 4 blocks
        self.assertEqual(len(blocks), 4)
        
        # Verify LSN sequence
        for i, (lsn, _, _) in enumerate(blocks):
            self.assertEqual(lsn, i + 1)
        
        reader.close()
    
    def test_iterate_empty_wal(self):
        """Test iterating over an empty WAL."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        writer.close()
        
        reader = wal.WALReader(self.test_wal, self.layout)
        
        # Should yield nothing
        blocks = list(reader.iterate_blocks())
        self.assertEqual(len(blocks), 0)
        
        reader.close()
    
    def test_multiple_records_per_block(self):
        """Test reading blocks with varying numbers of records."""
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Block 1: 1 record
        records = [wal_format.WALRecord(1, 1, self._create_packed_component(1, "one"))]
        writer.append_block(1, 1, records)
        
        # Block 2: 3 records
        records = [
            wal_format.WALRecord(2, 1, self._create_packed_component(2, "two")),
            wal_format.WALRecord(3, -1, self._create_packed_component(3, "three")),
            wal_format.WALRecord(4, 2, self._create_packed_component(4, "four"))
        ]
        writer.append_block(2, 1, records)
        
        # Block 3: 5 records
        records = []
        for i in range(5):
            records.append(wal_format.WALRecord(10 + i, 1, self._create_packed_component(i, "r%d" % i)))
        writer.append_block(3, 1, records)
        
        writer.close()
        
        # Read back
        reader = wal.WALReader(self.test_wal, self.layout)
        
        # Block 1: 1 record
        is_valid1, lsn1, _, records1 = reader.read_next_block()
        self.assertTrue(is_valid1)
        self.assertEqual(lsn1, 1)
        self.assertEqual(len(records1), 1)
        
        # Block 2: 3 records
        is_valid2, lsn2, _, records2 = reader.read_next_block()
        self.assertTrue(is_valid2)
        self.assertEqual(lsn2, 2)
        self.assertEqual(len(records2), 3)
        self.assertEqual(records2[1][1], -1)  # Verify negative weight
        
        # Block 3: 5 records
        is_valid3, lsn3, _, records3 = reader.read_next_block()
        self.assertTrue(is_valid3)
        self.assertEqual(lsn3, 3)
        self.assertEqual(len(records3), 5)
        
        reader.close()
    
    def test_component_data_integrity(self):
        """Test that component data is preserved correctly."""
        # Write block with specific component data
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        test_value = 12345
        test_label = "integrity"
        component_data = self._create_packed_component(test_value, test_label)
        records = [wal_format.WALRecord(999, 1, component_data)]
        writer.append_block(1, 1, records)
        writer.close()
        
        # Read back and verify component data
        reader = wal.WALReader(self.test_wal, self.layout)
        is_valid, lsn, comp_id, records = reader.read_next_block()
        self.assertTrue(is_valid)
        
        # Extract component data
        rec = records[0]
        buf = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')
        try:
            # rec[2] is component_data
            for i in range(self.layout.stride):
                buf[i] = rec[2][i]
            
            # Read value
            value = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, buf)[0])
            self.assertEqual(value, test_value)
            
            # Read string (simplified check - just verify length)
            string_ptr = rffi.ptradd(buf, 8)
            length = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, string_ptr)[0])
            self.assertEqual(length, len(test_label))
        finally:
            lltype.free(buf, flavor='raw')
        
        reader.close()
    
    def test_read_after_close_returns_eof(self):
        """Test that reading after close returns False (EOF)."""
        self._write_test_wal(1)
        
        reader = wal.WALReader(self.test_wal, self.layout)
        reader.close()
        
        # Should return (False, 0, 0, [])
        is_valid, _, _, _ = reader.read_next_block()
        self.assertFalse(is_valid)


if __name__ == '__main__':
    unittest.main()

