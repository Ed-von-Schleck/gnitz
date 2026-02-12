import unittest
import os
from gnitz.core import types, values as db_values
from gnitz.storage import writer_table, shard_table

class TestECSShardIntegration(unittest.TestCase):
    def setUp(self):
        self.fn = "test_ecs_shard.db"
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_from_values(10, 1, [db_values.wrap(x) for x in [100, "alpha"]])
        writer.add_row_from_values(20, -1, [db_values.wrap(x) for x in [200, "beta"]])
        writer.add_row_from_values(30, 1, [db_values.wrap(x) for x in [300, "gamma"]])
        writer.finalize(self.fn)
        
        self.view = shard_table.TableShardView(self.fn, self.layout)

    def tearDown(self):
        self.view.close()
        if os.path.exists(self.fn):
            os.unlink(self.fn)

    def test_read_header(self):
        self.assertEqual(self.view.count, 3)

    def test_find_row(self):
        self.assertEqual(self.view.find_row_index(10), 0)
        self.assertEqual(self.view.find_row_index(20), 1)
        self.assertEqual(self.view.find_row_index(30), 2)
        self.assertEqual(self.view.find_row_index(99), -1)

    def test_read_primitive_field(self):
        # Column 1 is I64 (index 0 in TableSchema payload, but index 1 in schema)
        val = self.view.read_field_i64(1, 1)
        self.assertEqual(val, 200)

    def test_read_string_field(self):
        # Column 2 is String
        self.assertTrue(self.view.string_field_equals(0, 2, "alpha"))
        self.assertTrue(self.view.string_field_equals(1, 2, "beta"))
        self.assertTrue(self.view.string_field_equals(2, 2, "gamma"))

    def test_read_weight(self):
        self.assertEqual(self.view.get_weight(0), 1)
        self.assertEqual(self.view.get_weight(1), -1)
        self.assertEqual(self.view.get_weight(2), 1)

if __name__ == '__main__':
    unittest.main()
