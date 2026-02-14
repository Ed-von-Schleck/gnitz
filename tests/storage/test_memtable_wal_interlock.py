import unittest
import os
from gnitz.storage import memtable_manager, wal
from gnitz.core import types, values as db_values

class TestMemTableWALInterlock(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.test_wal = "test_interlock.log"
        if os.path.exists(self.test_wal):
            os.unlink(self.test_wal)
    
    def tearDown(self):
        if os.path.exists(self.test_wal):
            os.unlink(self.test_wal)
            
    def _put(self, mgr, pk, w, *vals):
        wrapped = [db_values.wrap(v) for v in vals]
        mgr.put(pk, w, wrapped)
    
    def test_memtable_with_wal_integration(self):
        wal_writer = wal.WALWriter(self.test_wal, self.layout)
        mgr = memtable_manager.MemTableManager(self.layout, 1024 * 1024, wal_writer, table_id=1)
        
        self._put(mgr, 10, 1, 100, "first")
        self._put(mgr, 20, 1, 200, "second")
        
        mgr.close()
        wal_writer.close()
        
        reader = wal.WALReader(self.test_wal, self.layout)
        
        # Block 1
        block1 = reader.read_next_block()
        self.assertIsNotNone(block1)
        self.assertEqual(block1.tid, 1)
        self.assertEqual(block1.records[0].get_key(), 10)
        self.assertEqual(block1.records[0].weight, 1)
        
        # Block 2
        block2 = reader.read_next_block()
        self.assertIsNotNone(block2)
        self.assertEqual(block2.records[0].get_key(), 20)
        
        reader.close()
    
    def test_lsn_increments(self):
        wal_writer = wal.WALWriter(self.test_wal, self.layout)
        mgr = memtable_manager.MemTableManager(self.layout, 1024 * 1024, wal_writer, table_id=1)
        
        for i in range(5):
            self._put(mgr, i, 1, i * 10, "rec_%d" % i)
        
        mgr.close()
        wal_writer.close()
        
        reader = wal.WALReader(self.test_wal, self.layout)
        last_lsn = -1
        for block in reader.iterate_blocks():
            self.assertGreater(block.lsn, last_lsn)
            last_lsn = block.lsn
        
        self.assertEqual(last_lsn, 5) # Assuming sequence starts at 1
        reader.close()

if __name__ == '__main__':
    unittest.main()
