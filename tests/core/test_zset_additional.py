import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_uint64
from gnitz.core import zset, types, values as db_values
from gnitz.storage import errors, manifest, shard_table

class TestPersistentZSetHardened(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_zset_hardened_env"
        self.db_name = "hardened_db"
        self.layout = types.ComponentLayout([
            types.TYPE_I8, 
            types.TYPE_I64, 
            types.TYPE_STRING
        ])
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        self.db = zset.PersistentZSet(self.test_dir, self.db_name, self.layout)

    def tearDown(self):
        if hasattr(self, 'db'): self.db.close()
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def _p(self, v1, v2, v3):
        return [db_values.IntValue(v1), db_values.IntValue(v2), db_values.StringValue(v3)]

    def test_odd_alignment_packing(self):
        p = self._p(7, 999, "alignment_test")
        self.db.insert(1, p)
        self.db.flush()
        self.assertEqual(self.db.get_weight(1, p), 1)

    def test_shard_binary_search_boundaries(self):
        entities = [r_uint64(10), r_uint64(20), r_uint64(30), r_uint64(40), r_uint64(50)]
        for e in entities:
            self.db.insert(e, self._p(0, int(e), "data"))
        self.db.flush()
        self.assertEqual(self.db.get_weight(r_uint64(10), self._p(0, 10, "data")), 1)
        self.assertEqual(self.db.get_weight(r_uint64(50), self._p(0, 50, "data")), 1)
        self.assertEqual(self.db.get_weight(r_uint64(30), self._p(0, 30, "data")), 1)

    def test_empty_flush_logic(self):
        p = self._p(1, 1, "to_be_deleted")
        self.db.insert(1, p)
        self.db.remove(1, p)
        shard_path = self.db.flush()
        self.assertFalse(os.path.exists(shard_path))

    def test_table_id_isolation(self):
        p = self._p(1, 1, "comp1")
        self.db.insert(100, p)
        self.db.flush()

        # Here we simulate a second process reading the first.
        db2 = zset.PersistentZSet(self.test_dir, self.db_name, self.layout, table_id=2, read_only=True)
        try:
            self.assertEqual(self.db.get_weight(100, p), 1)
            self.assertEqual(db2.get_weight(100, p), 0) # Comp 2 shouldn't see Comp 1 data
        finally:
            db2.close()

    def test_wal_truncation(self):
        p1 = self._p(1, 1, "old_data")
        self.db.insert(1, p1)
        self.db.flush() 
        p2 = self._p(2, 2, "new_data")
        self.db.insert(2, p2) 
        self.db.checkpoint()
        self.assertEqual(self.db.get_weight(1, p1), 1)
        self.assertEqual(self.db.get_weight(2, p2), 1)

    def test_reader_visibility_after_flush(self):
        p = self._p(5, 5, "visible")
        self.db.insert(500, p)
        self.db.flush()
        
        reader_db = zset.PersistentZSet(self.test_dir, self.db_name, self.layout, read_only=True)
        try:
            self.assertEqual(reader_db.get_weight(500, p), 1)
        finally:
            reader_db.close()

    def test_n_way_merge_compaction(self):
        self.db.registry.compaction_threshold = 10 
        p = self._p(10, 10, "common_payload")
        for i in range(5):
            self.db.insert(1000, p)
            self.db.flush()
        self.assertEqual(self.db.get_weight(1000, p), 5)
        self.db._trigger_compaction()
        self.assertEqual(self.db.get_weight(1000, p), 5)

    def test_wal_checksum_failure(self):
        p = self._p(9, 9, "to_be_corrupted")
        self.db.insert(99, p)
        self.db.close() 
        
        with open(self.db.wal_path, "r+b") as f:
            f.seek(-5, os.SEEK_END)
            f.write(b'\xFF')
            
        with self.assertRaises(errors.CorruptShardError):
            zset.PersistentZSet(self.test_dir, self.db_name, self.layout)

if __name__ == '__main__':
    unittest.main()
