import unittest
import os
from rpython.rtyper.lltypesystem import rffi
from gnitz.storage import wal_format
from gnitz.storage.wal_format import WALRecord
from gnitz.core import types, values as db_values

class TestWALFormat(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.tmp = "test_wal_format.bin"

    def tearDown(self):
        if os.path.exists(self.tmp): 
            os.unlink(self.tmp)

    def test_write_and_decode_roundtrip(self):
        """
        Verifies that WAL records containing mixed types (int, string)
        can be serialized and reconstructed with algebraic weight preservation.
        """
        # FIXED: Use the from_key factory for more robust test setup
        recs = [
            WALRecord.from_key(10, 1, [db_values.IntValue(42), db_values.StringValue("one")]),
            WALRecord.from_key(20, -1, [db_values.IntValue(99), db_values.StringValue("two")])
        ]
        
        # Write block to disk
        fd = os.open(self.tmp, os.O_WRONLY | os.O_CREAT, 0o644)
        try:
            wal_format.write_wal_block(fd, 100, 1, recs, self.layout)
        finally:
            os.close(fd)
        
        # Read back and decode
        with open(self.tmp, 'rb') as f:
            data = f.read()
        
        # Convert string to raw C pointer for the decoder
        block_ptr = rffi.str2charp(data)
        try:
            # FIXED: decode_wal_block returns an object, not a tuple
            block = wal_format.decode_wal_block(block_ptr, len(data), self.layout)
            lsn, cid, decoded = block.lsn, block.tid, block.records
            
            # Verify Metadata
            self.assertEqual(lsn, 100)
            self.assertEqual(cid, 1)
            self.assertEqual(len(decoded), 2)
            
            # Verify Record 1
            # FIXED: Access key via get_key() to support the 128-bit split architecture
            self.assertEqual(decoded[0].get_key(), 10)
            self.assertEqual(decoded[0].weight, 1)
            self.assertEqual(decoded[0].component_data[0].get_int(), 42)
            self.assertEqual(decoded[0].component_data[1].get_string(), "one")
            
            # Verify Record 2 (Negative Weight / Annihilation delta)
            self.assertEqual(decoded[1].get_key(), 20)
            self.assertEqual(decoded[1].weight, -1)
            self.assertEqual(decoded[1].component_data[0].get_int(), 99)
            self.assertEqual(decoded[1].component_data[1].get_string(), "two")
            
        finally:
            rffi.free_charp(block_ptr)

    def test_corrupt_checksum_detection(self):
        """Ensures the decoder rejects blocks with tampered body data."""
        recs = [WALRecord.from_key(1, 1, [db_values.IntValue(123), db_values.StringValue("test")])]
        
        fd = os.open(self.tmp, os.O_WRONLY | os.O_CREAT, 0o644)
        wal_format.write_wal_block(fd, 1, 1, recs, self.layout)
        os.close(fd)
        
        with open(self.tmp, 'rb') as f:
            data = list(f.read())
            
        # Tamper with the body (data after the 32-byte header)
        data[35] = chr((ord(data[35]) + 1) % 256)
        corrupt_data = "".join(data)
        
        block_ptr = rffi.str2charp(corrupt_data)
        try:
            with self.assertRaises(Exception): # Errors.CorruptShardError
                wal_format.decode_wal_block(block_ptr, len(corrupt_data), self.layout)
        finally:
            rffi.free_charp(block_ptr)

if __name__ == '__main__':
    unittest.main()
