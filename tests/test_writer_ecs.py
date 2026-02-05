import unittest
import os
from gnitz.core import types
from gnitz.storage import writer_ecs, shard_ecs

class TestECSShardWriter(unittest.TestCase):
    def setUp(self):
        self.fn = "test_writer_ecs.db"
        self.layout = types.ComponentLayout([
            types.TYPE_U64, 
            types.TYPE_STRING, 
            types.TYPE_I32
        ])

    def tearDown(self):
        if os.path.exists(self.fn):
            os.unlink(self.fn)

    def test_write_and_read_back(self):
        writer = writer_ecs.ECSShardWriter(self.layout)
        
        # Row 1: Short string
        writer.add_entity(1, 100, "short", 10)
        # Row 2: Long string (> 12 chars)
        long_str = "this is a very long string that won't be inlined"
        writer.add_entity(2, 200, long_str, 20)
        
        writer.finalize(self.fn)
        
        view = shard_ecs.ECSShardView(self.fn, self.layout)
        self.assertEqual(view.count, 2)
        
        self.assertEqual(view.get_entity_id(0), 1)
        self.assertEqual(view.read_field_i64(0, 0), 100)
        self.assertTrue(view.string_field_equals(0, 1, "short"))
        
        self.assertEqual(view.get_entity_id(1), 2)
        self.assertEqual(view.read_field_i64(1, 0), 200)
        self.assertTrue(view.string_field_equals(1, 1, long_str))
        
        view.close()

if __name__ == '__main__':
    unittest.main()
