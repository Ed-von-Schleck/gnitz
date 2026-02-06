import unittest
import os
from gnitz.core import types
from gnitz.storage import writer_ecs, shard_ecs, tournament_tree

class TestTournamentTree(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.test_files = []
    
    def tearDown(self):
        for fn in self.test_files:
            if os.path.exists(fn):
                os.unlink(fn)
    
    def _create_shard(self, filename, entities_and_values):
        """
        Helper to create a shard with specified entities.
        entities_and_values: list of (entity_id, i64_value, string_value)
        """
        writer = writer_ecs.ECSShardWriter(self.layout)
        for eid, i64_val, str_val in entities_and_values:
            writer.add_entity(eid, i64_val, str_val)
        writer.finalize(filename)
        self.test_files.append(filename)
    
    def test_stream_cursor_basic(self):
        """Test basic StreamCursor operations."""
        # Create a shard with 3 entities
        fn = "test_cursor_basic.db"
        self._create_shard(fn, [
            (10, 100, "a"),
            (20, 200, "b"),
            (30, 300, "c"),
        ])
        
        view = shard_ecs.ECSShardView(fn, self.layout)
        cursor = tournament_tree.StreamCursor(view)
        
        # Initial state
        self.assertFalse(cursor.is_exhausted())
        self.assertEqual(cursor.peek_entity_id(), 10)
        self.assertEqual(cursor.get_current_index(), 0)
        
        # Advance once
        cursor.advance()
        self.assertFalse(cursor.is_exhausted())
        self.assertEqual(cursor.peek_entity_id(), 20)
        self.assertEqual(cursor.get_current_index(), 1)
        
        # Advance again
        cursor.advance()
        self.assertFalse(cursor.is_exhausted())
        self.assertEqual(cursor.peek_entity_id(), 30)
        self.assertEqual(cursor.get_current_index(), 2)
        
        # Advance to exhaustion
        cursor.advance()
        self.assertTrue(cursor.is_exhausted())
        self.assertEqual(cursor.peek_entity_id(), -1)
        
        view.close()
    
    def test_stream_cursor_empty_shard(self):
        """Test StreamCursor with empty shard."""
        fn = "test_cursor_empty.db"
        self._create_shard(fn, [])
        
        view = shard_ecs.ECSShardView(fn, self.layout)
        cursor = tournament_tree.StreamCursor(view)
        
        # Should be immediately exhausted
        self.assertTrue(cursor.is_exhausted())
        self.assertEqual(cursor.peek_entity_id(), -1)
        
        view.close()
    
    def test_stream_cursor_single_entity(self):
        """Test StreamCursor with single entity."""
        fn = "test_cursor_single.db"
        self._create_shard(fn, [(42, 420, "single")])
        
        view = shard_ecs.ECSShardView(fn, self.layout)
        cursor = tournament_tree.StreamCursor(view)
        
        self.assertFalse(cursor.is_exhausted())
        self.assertEqual(cursor.peek_entity_id(), 42)
        
        cursor.advance()
        self.assertTrue(cursor.is_exhausted())
        
        view.close()
    
    def test_tournament_tree_non_overlapping(self):
        """Test tournament tree with non-overlapping shards."""
        # Create 3 shards with sorted, non-overlapping entities
        fn1 = "test_tt_nonoverlap_1.db"
        fn2 = "test_tt_nonoverlap_2.db"
        fn3 = "test_tt_nonoverlap_3.db"
        
        self._create_shard(fn1, [(1, 10, "a"), (5, 50, "e"), (9, 90, "i")])
        self._create_shard(fn2, [(2, 20, "b"), (6, 60, "f"), (10, 100, "j")])
        self._create_shard(fn3, [(3, 30, "c"), (7, 70, "g"), (11, 110, "k")])
        
        # Create cursors
        view1 = shard_ecs.ECSShardView(fn1, self.layout)
        view2 = shard_ecs.ECSShardView(fn2, self.layout)
        view3 = shard_ecs.ECSShardView(fn3, self.layout)
        
        cursor1 = tournament_tree.StreamCursor(view1)
        cursor2 = tournament_tree.StreamCursor(view2)
        cursor3 = tournament_tree.StreamCursor(view3)
        
        # Create tournament tree
        tree = tournament_tree.TournamentTree([cursor1, cursor2, cursor3])
        
        # Iterate through all entities in sorted order
        expected_order = [1, 2, 3, 5, 6, 7, 9, 10, 11]
        result = []
        
        while not tree.is_exhausted():
            min_eid = tree.get_min_entity_id()
            result.append(min_eid)
            tree.advance_min_cursors()
        
        self.assertEqual(result, expected_order)
        
        view1.close()
        view2.close()
        view3.close()
    
    def test_tournament_tree_overlapping_entities(self):
        """Test tournament tree with same entity in multiple shards."""
        # Create shards where entity 5 appears in all three
        fn1 = "test_tt_overlap_1.db"
        fn2 = "test_tt_overlap_2.db"
        fn3 = "test_tt_overlap_3.db"
        
        self._create_shard(fn1, [(5, 100, "shard1"), (10, 110, "a")])
        self._create_shard(fn2, [(5, 200, "shard2"), (15, 115, "b")])
        self._create_shard(fn3, [(5, 300, "shard3"), (20, 120, "c")])
        
        view1 = shard_ecs.ECSShardView(fn1, self.layout)
        view2 = shard_ecs.ECSShardView(fn2, self.layout)
        view3 = shard_ecs.ECSShardView(fn3, self.layout)
        
        cursor1 = tournament_tree.StreamCursor(view1)
        cursor2 = tournament_tree.StreamCursor(view2)
        cursor3 = tournament_tree.StreamCursor(view3)
        
        tree = tournament_tree.TournamentTree([cursor1, cursor2, cursor3])
        
        # First iteration - should get entity 5
        self.assertEqual(tree.get_min_entity_id(), 5)
        
        # Get all cursors at entity 5
        cursors_at_min = tree.get_all_cursors_at_min()
        self.assertEqual(len(cursors_at_min), 3)  # All three shards
        
        # Advance - this should advance all three cursors
        tree.advance_min_cursors()
        
        # Next should be entity 10
        self.assertEqual(tree.get_min_entity_id(), 10)
        cursors_at_min = tree.get_all_cursors_at_min()
        self.assertEqual(len(cursors_at_min), 1)
        
        tree.advance_min_cursors()
        
        # Then 15
        self.assertEqual(tree.get_min_entity_id(), 15)
        
        tree.advance_min_cursors()
        
        # Then 20
        self.assertEqual(tree.get_min_entity_id(), 20)
        
        tree.advance_min_cursors()
        
        # Should be exhausted
        self.assertTrue(tree.is_exhausted())
        
        view1.close()
        view2.close()
        view3.close()
    
    def test_tournament_tree_single_shard(self):
        """Test tournament tree with a single shard."""
        fn = "test_tt_single.db"
        self._create_shard(fn, [(1, 10, "a"), (2, 20, "b"), (3, 30, "c")])
        
        view = shard_ecs.ECSShardView(fn, self.layout)
        cursor = tournament_tree.StreamCursor(view)
        tree = tournament_tree.TournamentTree([cursor])
        
        result = []
        while not tree.is_exhausted():
            result.append(tree.get_min_entity_id())
            tree.advance_min_cursors()
        
        self.assertEqual(result, [1, 2, 3])
        
        view.close()
    
    def test_tournament_tree_empty_shards(self):
        """Test tournament tree with all empty shards."""
        fn1 = "test_tt_empty_1.db"
        fn2 = "test_tt_empty_2.db"
        
        self._create_shard(fn1, [])
        self._create_shard(fn2, [])
        
        view1 = shard_ecs.ECSShardView(fn1, self.layout)
        view2 = shard_ecs.ECSShardView(fn2, self.layout)
        
        cursor1 = tournament_tree.StreamCursor(view1)
        cursor2 = tournament_tree.StreamCursor(view2)
        
        tree = tournament_tree.TournamentTree([cursor1, cursor2])
        
        # Should be immediately exhausted
        self.assertTrue(tree.is_exhausted())
        self.assertEqual(tree.get_min_entity_id(), -1)
        
        view1.close()
        view2.close()
    
    def test_tournament_tree_mixed_empty_nonempty(self):
        """Test tournament tree with mix of empty and non-empty shards."""
        fn1 = "test_tt_mixed_1.db"
        fn2 = "test_tt_mixed_2.db"
        fn3 = "test_tt_mixed_3.db"
        
        self._create_shard(fn1, [])
        self._create_shard(fn2, [(10, 100, "a"), (20, 200, "b")])
        self._create_shard(fn3, [])
        
        view1 = shard_ecs.ECSShardView(fn1, self.layout)
        view2 = shard_ecs.ECSShardView(fn2, self.layout)
        view3 = shard_ecs.ECSShardView(fn3, self.layout)
        
        cursor1 = tournament_tree.StreamCursor(view1)
        cursor2 = tournament_tree.StreamCursor(view2)
        cursor3 = tournament_tree.StreamCursor(view3)
        
        tree = tournament_tree.TournamentTree([cursor1, cursor2, cursor3])
        
        result = []
        while not tree.is_exhausted():
            result.append(tree.get_min_entity_id())
            tree.advance_min_cursors()
        
        self.assertEqual(result, [10, 20])
        
        view1.close()
        view2.close()
        view3.close()
    
    def test_tournament_tree_complex_overlaps(self):
        """Test tournament tree with complex overlapping patterns."""
        fn1 = "test_tt_complex_1.db"
        fn2 = "test_tt_complex_2.db"
        fn3 = "test_tt_complex_3.db"
        
        # Shard 1: [1, 5, 10]
        # Shard 2: [5, 10, 15]
        # Shard 3: [10, 15, 20]
        # Expected order: 1 (from 1), 5 (from 1,2), 10 (from 1,2,3), 15 (from 2,3), 20 (from 3)
        
        self._create_shard(fn1, [(1, 10, "a"), (5, 50, "e"), (10, 100, "j")])
        self._create_shard(fn2, [(5, 55, "e2"), (10, 105, "j2"), (15, 150, "o")])
        self._create_shard(fn3, [(10, 110, "j3"), (15, 155, "o2"), (20, 200, "t")])
        
        view1 = shard_ecs.ECSShardView(fn1, self.layout)
        view2 = shard_ecs.ECSShardView(fn2, self.layout)
        view3 = shard_ecs.ECSShardView(fn3, self.layout)
        
        cursor1 = tournament_tree.StreamCursor(view1)
        cursor2 = tournament_tree.StreamCursor(view2)
        cursor3 = tournament_tree.StreamCursor(view3)
        
        tree = tournament_tree.TournamentTree([cursor1, cursor2, cursor3])
        
        # Collect entities with their cursor counts
        results = []
        while not tree.is_exhausted():
            min_eid = tree.get_min_entity_id()
            cursor_count = len(tree.get_all_cursors_at_min())
            results.append((min_eid, cursor_count))
            tree.advance_min_cursors()
        
        expected = [
            (1, 1),   # Only in shard 1
            (5, 2),   # In shards 1 and 2
            (10, 3),  # In all three shards
            (15, 2),  # In shards 2 and 3
            (20, 1),  # Only in shard 3
        ]
        
        self.assertEqual(results, expected)
        
        view1.close()
        view2.close()
        view3.close()
    
    def test_tournament_tree_large_merge(self):
        """Test tournament tree with many shards."""
        num_shards = 10
        views = []
        cursors = []
        
        # Create 10 shards, each with 5 entities
        # Entities are interleaved: shard 0 has [0,10,20,30,40], shard 1 has [1,11,21,31,41], etc.
        for i in range(num_shards):
            fn = "test_tt_large_%d.db" % i
            entities = [(i + j * num_shards, (i + j * num_shards) * 10, "s%d" % i) 
                       for j in range(5)]
            self._create_shard(fn, entities)
            
            view = shard_ecs.ECSShardView(fn, self.layout)
            cursor = tournament_tree.StreamCursor(view)
            views.append(view)
            cursors.append(cursor)
        
        tree = tournament_tree.TournamentTree(cursors)
        
        # Should get all 50 entities in sorted order
        result = []
        while not tree.is_exhausted():
            result.append(tree.get_min_entity_id())
            tree.advance_min_cursors()
        
        expected = list(range(50))
        self.assertEqual(result, expected)
        
        for view in views:
            view.close()

if __name__ == '__main__':
    unittest.main()
