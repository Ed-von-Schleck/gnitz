import unittest
import os
from gnitz.core import types
from gnitz.storage import writer_table, shard_table, tournament_tree

class TestTournamentTree(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.test_files = []
    
    def tearDown(self):
        for fn in self.test_files:
            if os.path.exists(fn): os.unlink(fn)
    
    def _create_shard(self, filename, entities_and_values):
        writer = writer_table.TableShardWriter(self.layout)
        for pk, i64_val, str_val in entities_and_values:
            writer.add_row_values(pk, i64_val, str_val)
        writer.finalize(filename)
        self.test_files.append(filename)
    
    def test_tournament_tree_sorting(self):
        fn1, fn2 = "tt1.db", "tt2.db"
        self._create_shard(fn1, [(10, 100, "a"), (30, 300, "c")])
        self._create_shard(fn2, [(20, 200, "b"), (40, 400, "d")])
        
        v1 = shard_table.TableShardView(fn1, self.layout)
        v2 = shard_table.TableShardView(fn2, self.layout)
        
        tree = tournament_tree.TournamentTree([tournament_tree.StreamCursor(v1), tournament_tree.StreamCursor(v2)])
        
        res = []
        while not tree.is_exhausted():
            res.append(int(tree.get_min_primary_key()))
            tree.advance_min_cursors()
        
        self.assertEqual(res, [10, 20, 30, 40])
        tree.close()
        v1.close()
        v2.close()

    def test_tournament_tree_overlaps(self):
        fn1, fn2 = "ov1.db", "ov2.db"
        self._create_shard(fn1, [(5, 100, "x"), (10, 200, "y")])
        self._create_shard(fn2, [(5, 150, "z"), (15, 300, "w")])
        
        v1 = shard_table.TableShardView(fn1, self.layout)
        v2 = shard_table.TableShardView(fn2, self.layout)
        
        tree = tournament_tree.TournamentTree([tournament_tree.StreamCursor(v1), tournament_tree.StreamCursor(v2)])
        
        self.assertEqual(tree.get_min_primary_key(), 5)
        self.assertEqual(len(tree.get_all_cursors_at_min()), 2)
        
        tree.advance_min_cursors()
        self.assertEqual(tree.get_min_primary_key(), 10)
        
        tree.close()
        v1.close()
        v2.close()
