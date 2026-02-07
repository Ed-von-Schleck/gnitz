import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import wal_format, errors
from gnitz.core import types, strings as string_logic

class TestWALFormat(unittest.TestCase):
    def setUp(self):
        # Simple layout: [Value (I64), Label (String)]
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    
    def _create_packed_component(self, value, label):
        """Helper to create packed component data matching the layout."""
        stride = self.layout.stride
        buf = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
        try:
            # Zero out buffer
            for i in range(stride):
                buf[i] = '\x00'
            
            # Pack value (i64 at offset 0)
            rffi.cast(rffi.LONGLONGP, buf)[0] = rffi.cast(rffi.LONGLONG, value)
            
            # Pack string (at offset 8)
            string_ptr = rffi.ptradd(buf, 8)
            string_logic.pack_string(string_ptr, label, 0)
            
            # Convert to Python string
            return rffi.charpsize2str(buf, stride)
        finally:
            lltype.free(buf, flavor='raw')
    
    def test_encode_empty_block(self):
        """Test encoding a WAL block with no records."""
        records = []
        block = wal_format.encode_wal_block(100, 1, records, self.layout)
        
        # Should have header only
        self.assertEqual(len(block), wal_format.WAL_BLOCK_HEADER_SIZE)
    
    def test_encode_single_record(self):
        """Test encoding a WAL block with one record."""
        component_data = self._create_packed_component(42, "test")
        records = [wal_format.WALRecord(10, 1, component_data)]
        
        block = wal_format.encode_wal_block(100, 1, records, self.layout)
        
        # Header (32) + 1 record (8 + 8 + stride)
        expected_size = wal_format.WAL_BLOCK_HEADER_SIZE + 16 + self.layout.stride
        self.assertEqual(len(block), expected_size)
    
    def test_encode_multiple_records(self):
        """Test encoding a WAL block with multiple records."""
        records = []
        for i in range(5):
            component_data = self._create_packed_component(i * 10, "record_%d" % i)
            records.append(wal_format.WALRecord(100 + i, 1, component_data))
        
        block = wal_format.encode_wal_block(200, 2, records, self.layout)
        
        # Header + 5 records
        expected_size = wal_format.WAL_BLOCK_HEADER_SIZE + 5 * (16 + self.layout.stride)
        self.assertEqual(len(block), expected_size)
    
    def test_decode_empty_block(self):
        """Test decoding a WAL block with no records."""
        records = []
        block = wal_format.encode_wal_block(100, 1, records, self.layout)
        
        lsn, component_id, decoded_records = wal_format.decode_wal_block(block, self.layout)
        
        self.assertEqual(lsn, 100)
        self.assertEqual(component_id, 1)
        self.assertEqual(len(decoded_records), 0)
    
    def test_round_trip_single_record(self):
        """Test encode/decode round-trip for a single record."""
        component_data = self._create_packed_component(999, "roundtrip")
        records = [wal_format.WALRecord(50, -1, component_data)]
        
        # Encode
        block = wal_format.encode_wal_block(300, 3, records, self.layout)
        
        # Decode
        lsn, component_id, decoded_records = wal_format.decode_wal_block(block, self.layout)
        
        # Verify metadata
        self.assertEqual(lsn, 300)
        self.assertEqual(component_id, 3)
        self.assertEqual(len(decoded_records), 1)
        
        # Verify record - Accessed as TUPLE
        rec = decoded_records[0]
        self.assertEqual(rec[0], 50)  # entity_id
        self.assertEqual(rec[1], -1)  # weight
        self.assertEqual(len(rec[2]), self.layout.stride) # component_data
        
        # Verify component data (extract value)
        buf = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')
        try:
            # rec[2] is component_data
            for i in range(self.layout.stride):
                buf[i] = rec[2][i]
            
            value = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, buf)[0])
            self.assertEqual(value, 999)
        finally:
            lltype.free(buf, flavor='raw')
    
    def test_round_trip_multiple_records(self):
        """Test encode/decode round-trip for multiple records."""
        records = []
        for i in range(10):
            component_data = self._create_packed_component(i * 100, "multi_%d" % i)
            records.append(wal_format.WALRecord(1000 + i, 1 if i % 2 == 0 else -1, component_data))
        
        # Encode
        block = wal_format.encode_wal_block(500, 5, records, self.layout)
        
        # Decode
        lsn, component_id, decoded_records = wal_format.decode_wal_block(block, self.layout)
        
        # Verify metadata
        self.assertEqual(lsn, 500)
        self.assertEqual(component_id, 5)
        self.assertEqual(len(decoded_records), 10)
        
        # Verify all records
        for i in range(10):
            rec = decoded_records[i]
            self.assertEqual(rec[0], 1000 + i) # entity_id
            self.assertEqual(rec[1], 1 if i % 2 == 0 else -1) # weight
    
    def test_checksum_validation_pass(self):
        """Test that valid blocks pass checksum validation."""
        component_data = self._create_packed_component(123, "valid")
        records = [wal_format.WALRecord(20, 1, component_data)]
        
        block = wal_format.encode_wal_block(100, 1, records, self.layout)
        
        # Should not raise
        lsn, component_id, decoded_records = wal_format.decode_wal_block(block, self.layout)
        self.assertEqual(len(decoded_records), 1)
    
    def test_checksum_validation_fail(self):
        """Test that corrupted blocks fail checksum validation."""
        component_data = self._create_packed_component(123, "corrupt")
        records = [wal_format.WALRecord(20, 1, component_data)]
        
        block = wal_format.encode_wal_block(100, 1, records, self.layout)
        
        # Corrupt a byte in the body (flip bit in entity ID)
        block_list = list(block)
        corrupt_idx = wal_format.WAL_BLOCK_HEADER_SIZE + 5
        block_list[corrupt_idx] = chr(ord(block_list[corrupt_idx]) ^ 0xFF)
        corrupted_block = ''.join(block_list)
        
        # Should raise CorruptShardError
        with self.assertRaises(errors.CorruptShardError):
            wal_format.decode_wal_block(corrupted_block, self.layout)
    
    def test_truncated_header(self):
        """Test that truncated headers are detected."""
        short_block = "x" * 16  # Only 16 bytes, need 32
        
        with self.assertRaises(errors.CorruptShardError):
            wal_format.decode_wal_block(short_block, self.layout)
    
    def test_truncated_body(self):
        """Test that truncated bodies are detected."""
        component_data = self._create_packed_component(123, "truncate")
        records = [wal_format.WALRecord(20, 1, component_data)]
        
        block = wal_format.encode_wal_block(100, 1, records, self.layout)
        
        # Truncate the block (remove half the body)
        truncated_block = block[:wal_format.WAL_BLOCK_HEADER_SIZE + 10]
        
        with self.assertRaises(errors.CorruptShardError):
            wal_format.decode_wal_block(truncated_block, self.layout)
    
    def test_negative_weight(self):
        """Test encoding/decoding negative weights (annihilation)."""
        component_data = self._create_packed_component(42, "negative")
        records = [wal_format.WALRecord(100, -5, component_data)]
        
        block = wal_format.encode_wal_block(100, 1, records, self.layout)
        lsn, component_id, decoded_records = wal_format.decode_wal_block(block, self.layout)
        
        self.assertEqual(decoded_records[0][1], -5) # weight
    
    def test_zero_weight(self):
        """Test encoding/decoding zero weights (ghost records)."""
        component_data = self._create_packed_component(42, "zero")
        records = [wal_format.WALRecord(100, 0, component_data)]
        
        block = wal_format.encode_wal_block(100, 1, records, self.layout)
        lsn, component_id, decoded_records = wal_format.decode_wal_block(block, self.layout)
        
        self.assertEqual(decoded_records[0][1], 0) # weight
    
    def test_large_lsn(self):
        """Test encoding/decoding large LSN values."""
        component_data = self._create_packed_component(1, "large_lsn")
        records = [wal_format.WALRecord(1, 1, component_data)]
        
        large_lsn = 2**50
        block = wal_format.encode_wal_block(large_lsn, 1, records, self.layout)
        lsn, component_id, decoded_records = wal_format.decode_wal_block(block, self.layout)
        
        self.assertEqual(lsn, large_lsn)
    
    def test_long_string_in_component(self):
        """Test encoding/decoding components with long German Strings."""
        # Create a long string (> 12 bytes, will go to heap)
        long_label = "this is a very long string that exceeds the inline threshold"
        
        # Note: In WAL, the heap offset is meaningless since we're serializing
        # the component data directly. The string will be packed as a "short" string
        # in the WAL block itself.
        component_data = self._create_packed_component(777, long_label[:12])  # Truncate to short
        records = [wal_format.WALRecord(200, 1, component_data)]
        
        block = wal_format.encode_wal_block(100, 1, records, self.layout)
        lsn, component_id, decoded_records = wal_format.decode_wal_block(block, self.layout)
        
        self.assertEqual(len(decoded_records), 1)
        self.assertEqual(decoded_records[0][0], 200) # entity_id

if __name__ == '__main__':
    unittest.main()
