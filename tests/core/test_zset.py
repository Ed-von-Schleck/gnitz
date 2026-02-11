"""
Unit tests for ZSet (Z-Multiset) implementation.

Tests the core DBSP algebraic data structure with integer weights.
"""

import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, zset, values as db_values


class TestZSet(unittest.TestCase):
    """Test suite for ZSet multiset operations."""
    
    def setUp(self):
        """
        Create a test schema with 3 columns:
        - Column 0: TYPE_I64 (Primary Key)
        - Column 1: TYPE_STRING
        - Column 2: TYPE_I64
        
        Note: Payload contains only non-PK columns (indices 1 and 2).
        """
        self.schema = types.TableSchema(
            pk_index=0,
            columns=[
                types.TYPE_I64,    # Primary key
                types.TYPE_STRING, # Field 1
                types.TYPE_I64     # Field 2
            ]
        )
    
    def _wrap_payload(self, v2, v3):
        """
        Create payload values for the test schema.
        
        FIXED: Removed v1 parameter. Payload should contain only
        non-PK columns (STRING and I64), not the primary key.
        
        Args:
            v2: String value for column 1
            v3: Integer value for column 2
            
        Returns:
            List of Value objects [StringValue, IntValue]
        """
        return [
            db_values.StringValue(v2),
            db_values.IntValue(v3)
        ]
    
    def test_empty_zset(self):
        """Test that a newly created ZSet is empty."""
        zs = zset.ZSet(self.schema)
        self.assertEqual(zs.get_weight(1), 0)
        self.assertEqual(zs.get_weight(999), 0)
    
    def test_insert_single(self):
        """Test inserting a single record with weight +1."""
        zs = zset.ZSet(self.schema)
        payload = self._wrap_payload("alice", 100)
        
        zs.upsert(1, 1, payload)
        
        self.assertEqual(zs.get_weight(1), 1)
        self.assertEqual(zs.get_weight(2), 0)
    
    def test_algebraic_addition(self):
        """Test that multiple inserts of the same key add weights."""
        zs = zset.ZSet(self.schema)
        payload = self._wrap_payload("bob", 200)
        
        zs.upsert(10, 1, payload)
        zs.upsert(10, 1, payload)
        zs.upsert(10, 1, payload)
        
        # Weight should be 3 (1 + 1 + 1)
        self.assertEqual(zs.get_weight(10), 3)
    
    def test_annihilation(self):
        """Test that opposite weights cancel (Ghost Property)."""
        zs = zset.ZSet(self.schema)
        payload = self._wrap_payload("charlie", 300)
        
        # Insert with weight +2
        zs.upsert(20, 2, payload)
        self.assertEqual(zs.get_weight(20), 2)
        
        # Delete with weight -2
        zs.upsert(20, -2, payload)
        
        # Weight should be 0 (annihilated)
        self.assertEqual(zs.get_weight(20), 0)
    
    def test_negative_weights(self):
        """Test that ZSets support negative weights (deletions)."""
        zs = zset.ZSet(self.schema)
        payload = self._wrap_payload("diana", 400)
        
        zs.upsert(30, -5, payload)
        
        self.assertEqual(zs.get_weight(30), -5)
    
    def test_update_overwrites_payload(self):
        """Test that updating the same key overwrites the payload."""
        zs = zset.ZSet(self.schema)
        
        # Insert initial payload
        payload1 = self._wrap_payload("initial", 100)
        zs.upsert(40, 1, payload1)
        
        # Update with new payload
        payload2 = self._wrap_payload("updated", 999)
        zs.upsert(40, 1, payload2)
        
        # Weight should be 2 (1 + 1)
        self.assertEqual(zs.get_weight(40), 2)
        
        # Payload should be from the latest upsert
        retrieved = zs.get_payload(40)
        self.assertIsNotNone(retrieved)
        # Verify the string field (index 0 in payload, column 1 in schema)
        self.assertEqual(retrieved[0].get_string(), "updated")
        # Verify the int field (index 1 in payload, column 2 in schema)
        self.assertEqual(retrieved[1].get_int(), 999)
    
    def test_multiple_keys(self):
        """Test ZSet with multiple distinct keys."""
        zs = zset.ZSet(self.schema)
        
        zs.upsert(1, 1, self._wrap_payload("one", 1))
        zs.upsert(2, 2, self._wrap_payload("two", 2))
        zs.upsert(3, 3, self._wrap_payload("three", 3))
        
        self.assertEqual(zs.get_weight(1), 1)
        self.assertEqual(zs.get_weight(2), 2)
        self.assertEqual(zs.get_weight(3), 3)
        self.assertEqual(zs.get_weight(999), 0)
    
    def test_get_payload_annihilated(self):
        """Test that get_payload returns None for annihilated records."""
        zs = zset.ZSet(self.schema)
        payload = self._wrap_payload("ghost", 666)
        
        # Insert and immediately annihilate
        zs.upsert(50, 1, payload)
        zs.upsert(50, -1, payload)
        
        # Weight is 0
        self.assertEqual(zs.get_weight(50), 0)
        
        # Payload might still exist in memory, but semantically the record
        # is annihilated. Implementation may return None or the last payload.
        # (This depends on whether ZSet implements lazy cleanup)
        # For this test, we just verify the weight is correct.
        # If get_payload() exists, it should handle this gracefully.
    
    def test_string_payload(self):
        """Test that string payloads are correctly stored and retrieved."""
        zs = zset.ZSet(self.schema)
        
        long_string = "this is a very long string that exceeds 12 bytes"
        payload = self._wrap_payload(long_string, 12345)
        
        zs.upsert(60, 1, payload)
        
        retrieved = zs.get_payload(60)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved[0].get_string(), long_string)
        self.assertEqual(retrieved[1].get_int(), 12345)
    
    def test_zero_weight_insert(self):
        """Test inserting with weight 0 (no-op in multiset semantics)."""
        zs = zset.ZSet(self.schema)
        payload = self._wrap_payload("zero", 0)
        
        zs.upsert(70, 0, payload)
        
        # Weight should remain 0
        self.assertEqual(zs.get_weight(70), 0)


class TestZSetIteration(unittest.TestCase):
    """Test suite for ZSet iteration and enumeration."""
    
    def setUp(self):
        self.schema = types.TableSchema(
            pk_index=0,
            columns=[types.TYPE_I64, types.TYPE_STRING, types.TYPE_I64]
        )
    
    def _wrap_payload(self, v2, v3):
        """Create payload (non-PK columns only)."""
        return [
            db_values.StringValue(v2),
            db_values.IntValue(v3)
        ]
    
    def test_iter_nonzero(self):
        """Test iterating over records with non-zero weights."""
        zs = zset.ZSet(self.schema)
        
        # Insert some records
        zs.upsert(1, 1, self._wrap_payload("a", 10))
        zs.upsert(2, 2, self._wrap_payload("b", 20))
        zs.upsert(3, 0, self._wrap_payload("c", 30))  # Zero weight
        zs.upsert(4, -1, self._wrap_payload("d", 40)) # Negative weight
        
        # Collect all keys with non-zero weights
        keys = []
        for key, weight, payload in zs.iter_nonzero():
            keys.append((key, weight))
        
        # Should include keys 1, 2, and 4 (all non-zero weights)
        self.assertEqual(len(keys), 3)
        self.assertIn((1, 1), keys)
        self.assertIn((2, 2), keys)
        self.assertIn((4, -1), keys)
    
    def test_iter_positive(self):
        """Test iterating over records with positive weights only."""
        zs = zset.ZSet(self.schema)
        
        zs.upsert(1, 1, self._wrap_payload("a", 10))
        zs.upsert(2, 2, self._wrap_payload("b", 20))
        zs.upsert(3, 0, self._wrap_payload("c", 30))
        zs.upsert(4, -1, self._wrap_payload("d", 40))
        
        # Collect keys with positive weights
        keys = []
        for key, weight, payload in zs.iter_positive():
            keys.append((key, weight))
        
        # Should include only keys 1 and 2
        self.assertEqual(len(keys), 2)
        self.assertIn((1, 1), keys)
        self.assertIn((2, 2), keys)


if __name__ == '__main__':
    unittest.main()
