import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.core import types
from gnitz.storage.table import PersistentTable
from tests.row_helpers import create_test_row

class TestZSetHighLevel(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_zset_highlevel_env"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        
        # Layout: PK(u128) [Idx 0], Col1(String) [Idx 1]
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128), 
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        self.db = PersistentTable(self.test_dir, "test", self.layout)

    def tearDown(self):
        if not self.db.is_closed:
            self.db.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_u128_persistence_and_recovery(self):
        """Exercises the full stack with 128-bit keys and German Strings."""
        key = (r_uint128(0xAAAA) << 64) | r_uint128(0xBBBB)
        long_str = "this_is_a_long_string_that_must_survive_crash_recovery"
        
        # Use PayloadRow instead of the removed TaggedValue union
        p = create_test_row(self.layout, [long_str])
        
        # 1. Ingest and Crash (Close without flush)
        self.db.insert(key, p)
        self.db.close()
        
        # 2. Recovery - ensure the WAL replay reconstructs state correctly
        self.db = PersistentTable(self.test_dir, "test", self.layout)
        self.assertEqual(self.db.get_weight(key, p), 1)

    def test_cross_shard_summation(self):
        """Verifies Z-Set algebraic identity across persistent layers."""
        # Use PayloadRow instead of the removed TaggedValue union
        p = create_test_row(self.layout, ["common"])
        key = r_uint128(10)

        # Shard 1
        self.db.insert(key, p)
        self.db.flush()
        
        # Shard 2
        self.db.insert(key, p)
        self.db.flush()
        
        # MemTable (unflushed)
        self.db.insert(key, p)
        
        # Total weight Q(S1 + S2 + MT) = 3
        # Validates that get_weight correctly traverses all storage layers
        self.assertEqual(self.db.get_weight(key, p), 3)

if __name__ == '__main__':
    unittest.main()
