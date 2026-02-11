import unittest
import os
from gnitz.core import types, values as db_values
from gnitz.storage import memtable, shard_table

class TestMemTableECS(unittest.TestCase):
    def setUp(self):
        self.fn = "test_mt_ecs.db"
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64), 
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        self.mgr = memtable.MemTableManager(self.layout, 1024 * 1024)

    def tearDown(self):
        self.mgr.close()
        if os.path.exists(self.fn): os.unlink(self.fn)

    def _put(self, pk, w, *vals):
        wrapped = [db_values.wrap(v) for v in vals]
        # FIX: upsert expects Payload Only (excluding PK).
        # We pass only the non-PK columns.
        self.mgr.put(pk, w, wrapped)

    def test_upsert_and_flush(self):
        self._put(10, 1, "short")
        self._put(20, 1, "long long string for blob arena testing")
        
        self.mgr.flush_and_rotate(self.fn)
        
        view = shard_table.TableShardView(self.fn, self.layout)
        self.assertEqual(view.count, 2)
        self.assertEqual(view.get_pk_u64(0), 10)
        view.close()

if __name__ == '__main__':
    unittest.main()
