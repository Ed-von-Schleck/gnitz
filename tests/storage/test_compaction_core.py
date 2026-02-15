import unittest
import os
import shutil
from gnitz.core import types, values as db_values
from gnitz.storage import writer_table, shard_table, compactor, tournament_tree

class TestCompactionCore(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_compaction_core_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)

    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def _create_shard(self, name, pk, weight, val):
        path = os.path.join(self.test_dir, name)
        w = writer_table.TableShardWriter(self.layout)
        w.add_row_from_values(pk, weight, [db_values.wrap(val)])
        w.finalize(path)
        return path

    def test_algebraic_annihilation(self):
        """Verifies that records summing to zero are physically removed."""
        s1 = self._create_shard("s1.db", 100, 1, "ghost")
        s2 = self._create_shard("s2.db", 100, -1, "ghost")
        out = os.path.join(self.test_dir, "merged.db")

        compactor.compact_shards([s1, s2], out, self.layout)
        
        view = shard_table.TableShardView(out, self.layout)
        self.assertEqual(view.count, 0)
        view.close()

    def test_multiset_preservation(self):
        """Verifies that distinct payloads for the same PK are preserved."""
        s1 = self._create_shard("m1.db", 50, 1, "payload_a")
        s2 = self._create_shard("m2.db", 50, 1, "payload_b")
        out = os.path.join(self.test_dir, "multiset.db")

        compactor.compact_shards([s1, s2], out, self.layout)
        
        view = shard_table.TableShardView(out, self.layout)
        self.assertEqual(view.count, 2)
        self.assertEqual(view.get_weight(0), 1)
        self.assertEqual(view.get_weight(1), 1)
        view.close()

    def test_tournament_tree_order(self):
        """Verifies the min-heap yields Primary Keys in strict ascending order."""
        s1 = self._create_shard("t1.db", 30, 1, "high")
        s2 = self._create_shard("t2.db", 10, 1, "low")
        
        v1 = shard_table.TableShardView(s1, self.layout)
        v2 = shard_table.TableShardView(s2, self.layout)
        
        tree = tournament_tree.TournamentTree([
            tournament_tree.StreamCursor(v1), 
            tournament_tree.StreamCursor(v2)
        ])
        
        self.assertEqual(tree.get_min_key(), 10)
        tree.advance_min_cursors()
        self.assertEqual(tree.get_min_key(), 30)
        
        tree.close()
        v1.close()
        v2.close()

if __name__ == '__main__':
    unittest.main()
