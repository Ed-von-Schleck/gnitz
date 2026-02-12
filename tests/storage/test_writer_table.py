import unittest
import os
from gnitz.core import types, values as db_values
from gnitz.storage import writer_table, shard_table

class TestTableShardWriter(unittest.TestCase):
    def setUp(self):
        self.fn = "test_writer_table.db"
        self.layout = types.ComponentLayout([types.TYPE_U64, types.TYPE_STRING])

    def tearDown(self):
        if os.path.exists(self.fn): os.unlink(self.fn)

    def test_ghost_barrier_in_writer(self):
        writer = writer_table.TableShardWriter(self.layout)
        # Should be ignored (weight 0)
        writer.add_row_from_values(1, 0, [db_values.wrap(x) for x in [100, "ghost"]])
        # Should be written
        writer.add_row_from_values(2, 1, [db_values.wrap(x) for x in [200, "alive"]])
        writer.finalize(self.fn)
        
        view = shard_table.TableShardView(self.fn, self.layout)
        self.assertEqual(view.count, 1)
        # Fixed: use get_pk_u64 instead of get_primary_key
        self.assertEqual(view.get_pk_u64(0), 2)
        view.close()

if __name__ == '__main__':
    unittest.main()
