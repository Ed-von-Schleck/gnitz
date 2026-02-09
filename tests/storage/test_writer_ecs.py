import unittest
import os
from gnitz.core import types
from gnitz.storage import writer_ecs, shard_ecs

class TestECSShardWriter(unittest.TestCase):
    def setUp(self):
        self.fn = "test_writer_ecs.db"
        self.layout = types.ComponentLayout([types.TYPE_U64, types.TYPE_STRING])

    def tearDown(self):
        if os.path.exists(self.fn): os.unlink(self.fn)

    def test_ghost_barrier_in_writer(self):
        writer = writer_ecs.ECSShardWriter(self.layout)
        # Should be ignored (weight 0)
        writer._add_entity_weighted(1, 0, 100, "ghost")
        # Should be written
        writer._add_entity_weighted(2, 1, 200, "alive")
        writer.finalize(self.fn)
        
        view = shard_ecs.ECSShardView(self.fn, self.layout)
        self.assertEqual(view.count, 1)
        self.assertEqual(view.get_entity_id(0), 2)
        view.close()

if __name__ == '__main__':
    unittest.main()
