import unittest
import os
import shutil
from gnitz.core import zset, types, values as db_values

class TestCrashRecoveryStrings(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_crash_strings"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_STRING)
        ])
        
    def test_long_string_recovery(self):
        db = zset.PersistentZSet(self.test_dir, "crashdb", self.layout)
        long_str = "this is a very long string that will be moved to the blob arena"
        p = [db_values.StringValue(long_str)]
        
        # 1. Insert and close without flushing (simulating crash)
        db.insert(1, p)
        db.close() # WAL is flushed, memory is gone
        
        # 2. Re-open
        db_new = zset.PersistentZSet(self.test_dir, "crashdb", self.layout)
        
        # 3. Verify recovery
        weight = db_new.get_weight(1, p)
        self.assertEqual(weight, 1, "Record should be recovered from WAL")
        db_new.close()

    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

if __name__ == '__main__':
    unittest.main()
