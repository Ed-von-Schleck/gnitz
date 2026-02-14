import unittest
import os
from gnitz.storage import wal, errors
from gnitz.storage.wal_format import WALRecord
from gnitz.core import types, values as db_values

class TestWALReader(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.test_wal = "test_reader.log"
        if os.path.exists(self.test_wal): 
            os.unlink(self.test_wal)
    
    def tearDown(self):
        if os.path.exists(self.test_wal): 
            os.unlink(self.test_wal)
    
    def _write_test_wal(self, num_blocks):
        writer = wal.WALWriter(self.test_wal, self.layout)
        for lsn in range(1, num_blocks + 1):
            # PK is split into lo/hi in constructor, we pass full u128 here (simulated)
            pk = lsn * 100
            vals = [db_values.IntValue(lsn * 10), db_values.StringValue("block_%d" % lsn)]
            # Note: WALRecord constructor takes (pk_lo, pk_hi, weight, component_data)
            writer.append_block(lsn, 1, [WALRecord(pk, 0, 1, vals)])
        writer.close()
    
    def test_component_data_integrity(self):
        writer = wal.WALWriter(self.test_wal, self.layout)
        test_value = 12345
        test_label = "integrity"
        vals = [db_values.IntValue(test_value), db_values.StringValue(test_label)]
        writer.append_block(1, 1, [WALRecord(999, 0, 1, vals)])
        writer.close()
        
        reader = wal.WALReader(self.test_wal, self.layout)
        block = reader.read_next_block()
        
        self.assertIsNotNone(block, "Should have read one block")
        self.assertEqual(block.lsn, 1)
        self.assertEqual(block.tid, 1)
        
        rec = block.records[0]
        self.assertEqual(rec.get_key(), 999)
        self.assertEqual(rec.component_data[0].get_int(), test_value)
        self.assertEqual(rec.component_data[1].get_string(), test_label)
        reader.close()

    def test_checksum_validation_fail(self):
        self._write_test_wal(1)
        with open(self.test_wal, 'rb') as f: 
            data = f.read()
        
        data_list = list(data)
        # Flip bit in the body (after 32-byte header)
        data_list[35] = chr(ord(data_list[35]) ^ 0xFF)
        with open(self.test_wal, 'wb') as f: 
            f.write("".join(data_list))
        
        reader = wal.WALReader(self.test_wal, self.layout)
        # decoder raises CorruptShardError on checksum mismatch
        with self.assertRaises(errors.CorruptShardError):
            reader.read_next_block()
        reader.close()

    def test_iterate_blocks_generator(self):
        self._write_test_wal(4)
        reader = wal.WALReader(self.test_wal, self.layout)
        
        count = 0
        for block in reader.iterate_blocks():
            count += 1
            self.assertEqual(block.lsn, count)
            self.assertEqual(block.tid, 1)
            self.assertEqual(len(block.records), 1)
            
        self.assertEqual(count, 4)
        reader.close()

if __name__ == '__main__':
    unittest.main()
