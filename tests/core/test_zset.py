import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_uint64
from gnitz.core import zset, types, values as db_values

class TestPersistentZSetMassive(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_zset_massive_env"
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_I64), 
            types.ColumnDefinition(types.TYPE_STRING), 
            types.ColumnDefinition(types.TYPE_I64)
        ], 0)
        self.db_name = "stress_db"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        self.db = zset.PersistentTable(self.test_dir, self.db_name, self.layout, cache_size=1024*1024)

    def tearDown(self):
        if hasattr(self, 'db'): self.db.close()
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def _wrap_payload(self, v1, v2, v3):
        return [db_values.IntValue(v1), db_values.StringValue(v2), db_values.IntValue(v3)]

    def test_algebraic_summation_single_payload(self):
        eid = 100
        payload = self._wrap_payload(42, "hello", 10)
        self.db.insert(eid, payload)
        self.db.insert(eid, payload)
        self.assertEqual(self.db.get_weight(eid, payload), 2)

    def test_the_ghost_property_memory(self):
        eid = 500
        payload = self._wrap_payload(99, "transient", 99)
        self.db.insert(eid, payload)
        self.db.remove(eid, payload)
        self.assertEqual(self.db.get_weight(eid, payload), 0)

if __name__ == '__main__':
    unittest.main()
