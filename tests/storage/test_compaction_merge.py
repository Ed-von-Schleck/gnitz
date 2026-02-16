# tests/storage/test_compaction_merge.py
import unittest
import os
from gnitz.core import types, values as db_values
from gnitz.storage import writer_table, shard_table, tournament_tree

class TestCompactionMerge(unittest.TestCase):
    def setUp(self):
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_I64)
        ], 0)
        self.files = []

    def tearDown(self):
        for f in self.files:
            if os.path.exists(f): os.unlink(f)

    def _create_shard(self, name, records):
        path = name + ".db"
        w = writer_table.TableShardWriter(self.layout)
        for pk, weight, val in records:
            # Fix: use TaggedValue.make_int
            w.add_row_from_values(pk, weight, [db_values.TaggedValue.make_int(val)])
        w.finalize(path)
        self.files.append(path)
        return path

    def test_tournament_tree_sorting(self):
        s1 = self._create_shard("tt1", [(10, 1, 100), (30, 1, 300)])
        s2 = self._create_shard("tt2", [(20, 1, 200)])
        v1 = shard_table.TableShardView(s1, self.layout)
        v2 = shard_table.TableShardView(s2, self.layout)
        tree = tournament_tree.TournamentTree([
            tournament_tree.StreamCursor(v1), 
            tournament_tree.StreamCursor(v2)
        ])
        self.assertEqual(tree.get_min_key(), 10)
        tree.advance_min_cursors()
        self.assertEqual(tree.get_min_key(), 20)
        tree.advance_min_cursors()
        self.assertEqual(tree.get_min_key(), 30)
        tree.close()
        v1.close(); v2.close()

    def test_cursor_at_min_alignment(self):
        s1 = self._create_shard("c1", [(50, 1, 1)])
        s2 = self._create_shard("c2", [(50, 1, 2)])
        v1 = shard_table.TableShardView(s1, self.layout)
        v2 = shard_table.TableShardView(s2, self.layout)
        tree = tournament_tree.TournamentTree([
            tournament_tree.StreamCursor(v1), 
            tournament_tree.StreamCursor(v2)
        ])
        self.assertEqual(len(tree.get_all_cursors_at_min()), 2)
        tree.close()
        v1.close(); v2.close()
