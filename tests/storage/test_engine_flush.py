import unittest
import os
import shutil
from gnitz.core import zset, types, values as db_values

class TestEngineFlush(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_engine_flush_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_I64),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        
        self.db = zset.PersistentTable(self.test_dir, "engine_test", self.layout)

    def tearDown(self):
        if not self.db.is_closed:
            self.db.close()
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_read_amplification_detection(self):
        """
        Verifies that the registry correctly tracks overlapping shards 
        without hardcoded path assumptions.
        """
        p1 = [db_values.IntValue(10), db_values.StringValue("A")]
        p2 = [db_values.IntValue(20), db_values.StringValue("B")]
        
        # Create overlapping shards
        for _ in range(3):
            self.db.insert(500, p1)
            self.db.insert(500, p2)
            self.db.flush()
            
        # Registry should report 3 overlapping shards for this key
        amp = self.db.registry.get_read_amplification(1, 500)
        self.assertEqual(amp, 3)
        
        # Verify algebraic summation across those 3 shards
        self.assertEqual(self.db.get_weight(500, p1), 3)
        self.assertEqual(self.db.get_weight(500, p2), 3)

if __name__ == '__main__':
    unittest.main()
