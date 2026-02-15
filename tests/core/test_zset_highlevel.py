import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.core import zset, types, values as db_values

class TestZSetHighLevel(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_zset_highlevel_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128), 
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        self.db = zset.PersistentTable(self.test_dir, "test", self.layout)

    def tearDown(self):
        if not self.db.is_closed: self.db.close()
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_u128_persistence_and_recovery(self):
        """Exercises the full stack with 128-bit keys and German Strings."""
        key = (r_uint128(0xAAAA) << 64) | r_uint128(0xBBBB)
        long_str = "this_is_a_long_string_that_must_survive_crash_recovery"
        p = [db_values.StringValue(long_str)]
        
        # 1. Ingest and Crash (Close without flush)
        self.db.insert(key, p)
        self.db.close()
        
        # 2. Recovery
        self.db = zset.PersistentTable(self.test_dir, "test", self.layout)
        self.assertEqual(self.db.get_weight(key, p), 1)

    def test_cross_shard_summation(self):
        """Verifies Z-Set algebraic identity across persistent layers."""
        p = [db_values.StringValue("common")]
        # Shard 1
        self.db.insert(10, p)
        self.db.flush()
        # Shard 2
        self.db.insert(10, p)
        self.db.flush()
        # MemTable
        self.db.insert(10, p)
        
        # Total weight Q(S1 + S2 + MT) = 3
        self.assertEqual(self.db.get_weight(10, p), 3)

if __name__ == '__main__':
    unittest.main()
