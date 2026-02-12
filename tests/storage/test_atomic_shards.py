import unittest
import os
import shutil
from gnitz.storage import writer_table, shard_table
from gnitz.core import types, values as db_values

class TestAtomicShards(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_atomic_env"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.shard_path = os.path.join(self.test_dir, "atomic.db")

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_atomic_swap_mechanics(self):
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_from_values(123, 1, [db_values.wrap(x) for x in [456]])
        writer.finalize(self.shard_path)
        
        self.assertTrue(os.path.exists(self.shard_path))
        self.assertFalse(os.path.exists(self.shard_path + ".tmp"))
        
        view = shard_table.TableShardView(self.shard_path, self.layout)
        self.assertEqual(view.count, 1)
        # Fixed: use get_pk_u64 instead of get_primary_key
        self.assertEqual(view.get_pk_u64(0), 123)
        # Column 1 is the payload I64
        self.assertEqual(view.read_field_i64(0, 1), 456)
        view.close()

    def test_overwriting_is_atomic(self):
        w1 = writer_table.TableShardWriter(self.layout)
        w1.add_row_from_values(1, 1, [db_values.wrap(x) for x in [100]])
        w1.finalize(self.shard_path)
        
        w2 = writer_table.TableShardWriter(self.layout)
        w2.add_row_from_values(2, 1, [db_values.wrap(x) for x in [200]])
        w2.finalize(self.shard_path)
        
        view = shard_table.TableShardView(self.shard_path, self.layout)
        self.assertEqual(view.count, 1)
        # Fixed: use get_pk_u64 instead of get_primary_key
        self.assertEqual(view.get_pk_u64(0), 2)
        view.close()

if __name__ == '__main__':
    unittest.main()
