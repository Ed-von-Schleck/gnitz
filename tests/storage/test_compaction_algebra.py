# tests/storage/test_compaction_algebra.py
import unittest
import os
import shutil
from gnitz.core import types
from gnitz.storage import writer_table, shard_table, compactor
from tests.row_helpers import create_test_row

class TestCompactionAlgebra(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_algebra_env"
        if not os.path.exists(self.test_dir): 
            os.makedirs(self.test_dir)
        # Schema: PK (U64) at index 0, String Column at index 1
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)

    def tearDown(self):
        if os.path.exists(self.test_dir): 
            shutil.rmtree(self.test_dir)

    def _create_shard(self, name, pk, weight, val):
        path = os.path.join(self.test_dir, name)
        w = writer_table.TableShardWriter(self.layout)
        
        # Use helper to create a PayloadRow for the non-PK columns
        row = create_test_row(self.layout, [val])
        
        w.add_row_from_values(pk, weight, row)
        w.finalize(path)
        return path

    def test_compaction_annihilation(self):
        s1 = self._create_shard("s1.db", 100, 1, "ghost")
        s2 = self._create_shard("s2.db", 100, -1, "ghost")
        out = os.path.join(self.test_dir, "merged.db")
        compactor.compact_shards([s1, s2], out, self.layout)
        view = shard_table.TableShardView(out, self.layout)
        self.assertEqual(view.count, 0)
        view.close()

    def test_multiset_preservation(self):
        s1 = self._create_shard("m1.db", 50, 1, "A")
        s2 = self._create_shard("m2.db", 50, 1, "B")
        out = os.path.join(self.test_dir, "multi.db")
        compactor.compact_shards([s1, s2], out, self.layout)
        view = shard_table.TableShardView(out, self.layout)
        self.assertEqual(view.count, 2)
        view.close()
