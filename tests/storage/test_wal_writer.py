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
    
    def test_write_single_block(self):
        writer = wal.WALWriter(self.test_wal, self.layout)
        
        # WALRecord(pk_lo, pk_hi, weight, component_data)
        records = [wal_format.WALRecord(10, 0, 1, [db_values.IntValue(42), db_values.StringValue("test")])]
        writer.append_block(1, 1, records)
        writer.close()
        
        # Use simple os.path check for existence, size is verified by round-trip
        self.assertTrue(os.path.exists(self.test_wal))
        self.assertGreater(os.path.getsize(self.test_wal), 32) # header size

    def test_read_back_written_blocks(self):
        writer = wal.WALWriter(self.test_wal, self.layout)
        for i in range(1, 3):
            recs = [db_values.IntValue(i * 100), db_values.StringValue("lsn_%d" % i)]
            records = [wal_format.WALRecord(i * 10, 0, 1, recs)]
            writer.append_block(i, i, records) # table_id = i
        writer.close()
        
        reader = wal.WALReader(self.test_wal, self.layout)
        
        # Block 1
        block1 = reader.read_next_block()
        self.assertIsNotNone(block1)
        self.assertEqual(block1.lsn, 1)
        self.assertEqual(block1.tid, 1)
        self.assertEqual(block1.records[0].get_key(), 10)
        self.assertEqual(block1.records[0].component_data[0].get_int(), 100)
        
        # Block 2
        block2 = reader.read_next_block()
        self.assertIsNotNone(block2)
        self.assertEqual(block2.lsn, 2)
        self.assertEqual(block2.tid, 2)
        self.assertEqual(block2.records[0].get_key(), 20)
        
        reader.close()

    def test_write_after_close_fails(self):
        writer = wal.WALWriter(self.test_wal, self.layout)
        writer.close()
        
        records = [wal_format.WALRecord(1, 0, 1, [db_values.IntValue(1), db_values.StringValue("fail")])]
        with self.assertRaises(errors.StorageError):
            writer.append_block(1, 1, records)

if __name__ == '__main__':
    unittest.main()
