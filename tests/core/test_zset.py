import unittest
import os
import shutil
from gnitz.core import zset, types, values as db_values
from gnitz.storage import errors, manifest

class TestPersistentZSetMassive(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_zset_massive_env"
        # Layout: [Integer, String, Integer]
        self.layout = types.ComponentLayout([
            types.TYPE_I64, 
            types.TYPE_STRING, 
            types.TYPE_I64
        ])
        self.db_name = "stress_db"
        self.db = zset.PersistentZSet(self.test_dir, self.db_name, self.layout, cache_size=1024*1024)

    def tearDown(self):
        if hasattr(self, 'db'):
            self.db.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def _wrap_payload(self, v1, v2, v3):
        return [db_values.IntValue(v1), db_values.StringValue(v2), db_values.IntValue(v3)]

    # --- 1. ALGEBRAIC CORE TESTS (Z-SET MATH) ---

    def test_algebraic_summation_single_payload(self):
        """Tests that weights coalesce correctly for identical payloads."""
        eid = 100
        payload = self._wrap_payload(42, "hello", 10)
        
        self.db.insert(eid, payload)
        self.db.insert(eid, payload)
        for _ in range(5): self.db.insert(eid, payload)
        self.db.remove(eid, payload)
        self.db.remove(eid, payload)
        
        self.assertEqual(self.db.get_weight(eid, payload), 5)

    def test_multiset_behavior_distinct_payloads(self):
        """Tests that the same EntityID can hold multiple distinct states (Z-Set multiset)."""
        eid = 100
        state_a = self._wrap_payload(1, "state_a", 100)
        state_b = self._wrap_payload(2, "state_b", 200)
        
        self.db.insert(eid, state_a)
        self.db.insert(eid, state_a)
        self.db.insert(eid, state_b)
        
        self.assertEqual(self.db.get_weight(eid, state_a), 2)
        self.assertEqual(self.db.get_weight(eid, state_b), 1)
        
        state_c = self._wrap_payload(3, "state_c", 300)
        self.assertEqual(self.db.get_weight(eid, state_c), 0)

    def test_the_ghost_property_memory(self):
        """Verifies that records summing to zero are invisible (Annihilation)."""
        eid = 500
        payload = self._wrap_payload(99, "transient", 99)
        
        self.db.insert(eid, payload)
        self.assertEqual(self.db.get_weight(eid, payload), 1)
        
        self.db.remove(eid, payload)
        self.assertEqual(self.db.get_weight(eid, payload), 0)

    # --- 2. STRING BOUNDARY TESTS (GERMAN STRINGS) ---

    def test_german_string_thresholds(self):
        """Tests short, boundary, and long strings to hit inline vs blob paths."""
        eid = 777
        short_str = "abc"
        boundary_str = "123456789012"
        long_str = "this is a very long string that must go to the blob arena"
        
        p1 = [db_values.IntValue(1), db_values.StringValue(short_str), db_values.IntValue(1)]
        p2 = [db_values.IntValue(2), db_values.StringValue(boundary_str), db_values.IntValue(2)]
        p3 = [db_values.IntValue(3), db_values.StringValue(long_str), db_values.IntValue(3)]

        self.db.insert(eid, p1)
        self.db.insert(eid, p2)
        self.db.insert(eid, p3)
        
        self.assertEqual(self.db.get_weight(eid, p1), 1)
        self.assertEqual(self.db.get_weight(eid, p2), 1)
        self.assertEqual(self.db.get_weight(eid, p3), 1)

    # --- 3. PERSISTENCE & LIFECYCLE TESTS ---

    def test_flush_and_spine_query(self):
        """Tests querying data after it has been moved from MemTable to an ECS Shard."""
        eid = 1000
        payload = self._wrap_payload(10, "persisted", 10)
        
        self.db.insert(eid, payload)
        shard_path = self.db.flush()
        
        self.assertTrue(os.path.exists(shard_path))
        self.assertEqual(self.db.get_weight(eid, payload), 1)

    def test_multiple_shards_overlapping_range(self):
        """Tests resolving weights across multiple physical shards for the same entity."""
        eid = 2000
        payload = self._wrap_payload(20, "multi-shard", 20)
        
        self.db.insert(eid, payload)
        self.db.flush()
        
        self.db.insert(eid, payload)
        self.db.flush()
        
        self.db.insert(eid, payload)
        
        self.assertEqual(self.db.get_weight(eid, payload), 3)

    # --- 4. CRASH RECOVERY (WAL) ---

    def test_wal_recovery_on_restart(self):
        """Simulates a crash by closing the DB without flushing and recovering via WAL."""
        eid = 3000
        payload = self._wrap_payload(30, "recovery_test", 30)
        
        self.db.insert(eid, payload)
        self.db.close()
        
        new_db = zset.PersistentZSet(self.test_dir, self.db_name, self.layout)
        try:
            self.assertEqual(new_db.get_weight(eid, payload), 1)
        finally:
            new_db.close()

    # --- 5. COMPACTION TESTS ---

    def test_manual_compaction_and_annihilation(self):
        """Verifies that compaction merges shards and applies the Ghost Property."""
        eid = 4000
        payload = self._wrap_payload(40, "compact_me", 40)
        
        self.db.insert(eid, payload)
        self.db.flush()
        
        self.db.insert(eid, payload)
        self.db.flush()
        
        self.db.remove(eid, payload)
        self.db.remove(eid, payload)
        self.db.flush()
        
        self.assertEqual(self.db.get_weight(eid, payload), 0)
        self.db._trigger_compaction()
        self.assertEqual(self.db.get_weight(eid, payload), 0)
        
        reader = self.db.manifest_manager.load_current()
        self.assertEqual(reader.get_entry_count(), 1)
        reader.close()

    def test_automated_compaction_trigger(self):
        """Tests that adding many overlapping shards triggers compaction automatically."""
        self.db.registry.compaction_threshold = 2
        eid = 5000
        for i in range(4):
            p = [db_values.IntValue(i), db_values.StringValue("trigger"), db_values.IntValue(i)]
            self.db.insert(eid, p)
            self.db.flush()
            
        reader = self.db.manifest_manager.load_current()
        self.assertLess(reader.get_entry_count(), 4)
        reader.close()

    # --- 6. ARENA & OVERFLOW TESTS ---

    def test_memtable_arena_overflow_flush(self):
        """Tests that the MemTable handles large volumes of data and arena pressure."""
        long_str = "A" * 500 
        for i in range(500):
            p = [db_values.IntValue(i), db_values.StringValue(long_str), db_values.IntValue(i)]
            self.db.insert(i, p)
        
        p_check = [db_values.IntValue(10), db_values.StringValue(long_str), db_values.IntValue(10)]
        self.assertEqual(self.db.get_weight(10, p_check), 1)
        self.db.flush()
        self.assertEqual(self.db.get_weight(10, p_check), 1)

    # --- 7. EDGE CASES ---

    def test_empty_database_queries(self):
        """Verifies behavior when querying an empty database."""
        p = [db_values.IntValue(0), db_values.StringValue(""), db_values.IntValue(0)]
        self.assertEqual(self.db.get_weight(999, p), 0)

    def test_delete_non_existent_record(self):
        """Z-Sets allow negative weights. Verifies that we can remove something we never added."""
        eid = 9999
        payload = [db_values.IntValue(0), db_values.StringValue("void"), db_values.IntValue(0)]
        self.db.remove(eid, payload)
        self.assertEqual(self.db.get_weight(eid, payload), -1)
        
        self.db.insert(eid, payload)
        self.assertEqual(self.db.get_weight(eid, payload), 0)

    def test_large_entity_id_range(self):
        """Tests EntityIDs at the boundaries of u64."""
        min_eid = 0
        max_eid = 0xFFFFFFFFFFFFFFFF
        
        p1 = [db_values.IntValue(0), db_values.StringValue("min"), db_values.IntValue(0)]
        p2 = [db_values.IntValue(1), db_values.StringValue("max"), db_values.IntValue(1)]
        
        self.db.insert(min_eid, p1)
        self.db.insert(max_eid, p2)
        self.db.flush()
        
        self.assertEqual(self.db.get_weight(min_eid, p1), 1)
        self.assertEqual(self.db.get_weight(max_eid, p2), 1)

    def test_semantic_equality_across_shards_compaction(self):
        """Verifies content-based equality for strings during compaction."""
        eid = 6000
        shared_string = "COMPACTION_SEMANTIC_TEST_LONG_STRING_HEH"
        p = [db_values.IntValue(1), db_values.StringValue(shared_string), db_values.IntValue(1)]
        
        self.db.insert(eid, p)
        self.db.flush()
        
        self.db.insert(eid, p)
        self.db.flush()
        
        self.assertEqual(self.db.get_weight(eid, p), 2)
        self.db._trigger_compaction()
        self.assertEqual(self.db.get_weight(eid, p), 2)

if __name__ == '__main__':
    unittest.main()
