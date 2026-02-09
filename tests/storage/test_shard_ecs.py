import unittest
import os
from gnitz.core import types
from gnitz.storage import writer_ecs, shard_ecs

class TestECSShardIntegration(unittest.TestCase):
    def setUp(self):
        self.fn = "test_ecs_shard.db"
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        
        # Use the actual writer to create our test data
        writer = writer_ecs.ECSShardWriter(self.layout)
        writer._add_entity_weighted(10, 1, 100, "alpha")
        writer._add_entity_weighted(20, -1, 200, "beta")
        writer._add_entity_weighted(30, 1, 300, "gamma")
        writer.finalize(self.fn)
        
        self.view = shard_ecs.ECSShardView(self.fn, self.layout)

    def tearDown(self):
        self.view.close()
        if os.path.exists(self.fn):
            os.unlink(self.fn)

    def test_read_header(self):
        self.assertEqual(self.view.count, 3)

    def test_find_entity(self):
        self.assertEqual(self.view.find_entity_index(10), 0)
        self.assertEqual(self.view.find_entity_index(20), 1)
        self.assertEqual(self.view.find_entity_index(30), 2)
        self.assertEqual(self.view.find_entity_index(99), -1)

    def test_read_primitive_field(self):
        # Entity 20 is at index 1
        val = self.view.read_field_i64(1, 0)
        self.assertEqual(val, 200)

    def test_read_string_field(self):
        # This triggers lazy Region C validation
        self.assertTrue(self.view.string_field_equals(0, 1, "alpha"))
        self.assertTrue(self.view.string_field_equals(1, 1, "beta"))
        self.assertTrue(self.view.string_field_equals(2, 1, "gamma"))

    def test_read_weight(self):
        self.assertEqual(self.view.get_weight(0), 1)
        self.assertEqual(self.view.get_weight(1), -1)
        self.assertEqual(self.view.get_weight(2), 1)

if __name__ == '__main__':
    unittest.main()
