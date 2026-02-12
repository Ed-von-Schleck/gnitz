import unittest
import os
from gnitz.core import types, values as db_values
from gnitz.storage import writer_table, shard_table, compactor

class TestCompactor(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.files = ["in1.db", "in2.db", "out.db"]

    def tearDown(self):
        for f in self.files:
            if os.path.exists(f): os.unlink(f)

    def test_compaction_annihilation(self):
        w1 = writer_table.TableShardWriter(self.layout)
        # PK=1, Weight=1, ColumnValue=10
        w1.add_row_from_values(1, 1, [db_values.wrap(10)])
        w1.finalize("in1.db")
        
        w2 = writer_table.TableShardWriter(self.layout)
        # PK=1, Weight=-1, ColumnValue=10
        w2.add_row_from_values(1, -1, [db_values.wrap(10)])
        w2.finalize("in2.db")
        
        compactor.compact_shards(["in1.db", "in2.db"], "out.db", self.layout, validate_checksums=False)
        
        res = shard_table.TableShardView("out.db", self.layout, validate_checksums=False)
        self.assertEqual(res.count, 0)
        res.close()

if __name__ == '__main__':
    unittest.main()
