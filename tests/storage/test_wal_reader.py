import unittest
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import wal, wal_format, errors, memtable
from gnitz.storage.wal_format import WALRecord
from gnitz.core import types, strings as string_logic, values as db_values

class TestWALReader(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.test_wal = "test_reader.log"
        if os.path.exists(self.test_wal): os.unlink(self.test_wal)
    
    def tearDown(self):
        if os.path.exists(self.test_wal): os.unlink(self.test_wal)
    
    def _write_test_wal(self, num_blocks):
        writer = wal.WALWriter(self.test_wal, self.layout)
        for lsn in range(1, num_blocks + 1):
            vals = [db_values.IntValue(lsn * 10), db_values.StringValue("block_%d" % lsn)]
            # Wrap data in WALRecord
            writer.append_block(lsn, 1, [WALRecord(lsn * 100, 1, vals)])
        writer.close()
    
    def test_component_data_integrity(self):
        writer = wal.WALWriter(self.test_wal, self.layout)
        test_value = 12345
        test_label = "integrity"
        vals = [db_values.IntValue(test_value), db_values.StringValue(test_label)]
        # Wrap data in WALRecord
        writer.append_block(1, 1, [WALRecord(999, 1, vals)])
        writer.close()
        
        reader = wal.WALReader(self.test_wal, self.layout)
        is_valid, lsn, comp_id, records = reader.read_next_block()
        self.assertTrue(is_valid)
        
        rec = records[0]
        # Use attribute access
        self.assertEqual(rec.primary_key, 999)
        self.assertEqual(rec.component_data[0].v, test_value)
        self.assertEqual(rec.component_data[1].v, test_label)
        reader.close()

    def test_checksum_validation_fail(self):
        self._write_test_wal(1)
        with open(self.test_wal, 'rb') as f: data = f.read()
        
        data_list = list(data)
        # Flip bit in the body (after 32-byte header)
        data_list[35] = chr(ord(data_list[35]) ^ 0xFF)
        with open(self.test_wal, 'wb') as f: f.write("".join(data_list))
        
        reader = wal.WALReader(self.test_wal, self.layout)
        with self.assertRaises(errors.CorruptShardError):
            reader.read_next_block()
        reader.close()

    def test_iterate_blocks_generator(self):
        self._write_test_wal(4)
        reader = wal.WALReader(self.test_wal, self.layout)
        blocks = []
        for lsn, cid, records in reader.iterate_blocks():
            blocks.append((lsn, cid, records))
        self.assertEqual(len(blocks), 4)
        for i, (lsn, _, _) in enumerate(blocks):
            self.assertEqual(lsn, i + 1)
        reader.close()

if __name__ == '__main__':
    unittest.main()
