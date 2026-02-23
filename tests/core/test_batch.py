# gnitz/tests/core/test_batch.py

import unittest
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, r_int64, r_uint64
from gnitz.core import types, batch
from gnitz.storage import comparator as storage_comparator
from tests.row_helpers import create_test_row

class TestZSetBatch(unittest.TestCase):
    def setUp(self):
        # Schema: PK (u128) | String | Int64
        self.schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128), # Primary Key (index 0)
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_I64)
        ], pk_index=0)

    def _mk_payload(self, s, i):
        """Helper to create a PayloadRow (excluding the PK column)."""
        return create_test_row(self.schema, [s, r_int64(i)])

    def test_batch_append_and_clear(self):
        """Basic lifecycle test for the Arena-based ZSetBatch."""
        b = batch.ZSetBatch(self.schema)
        try:
            self.assertEqual(b.length(), 0)
            
            payload = self._mk_payload("a", 10)
            b.append(r_uint128(1), r_int64(1), payload)
            self.assertEqual(b.length(), 1)
            
            # Verify we can read data back via the PayloadRow API
            self.assertEqual(b.get_pk(0), r_uint128(1))
            self.assertEqual(b.get_weight(0), r_int64(1))
            row = b.get_row(0)
            self.assertEqual(row.get_str(0), "a")
            self.assertEqual(row.get_int_signed(1), r_int64(10))
            
            b.clear()
            self.assertEqual(b.length(), 0)
        finally:
            b.free()

    def test_batch_sorting_logic(self):
        """Verify sorting by PK then by Payload content (lexicographical)."""
        b = batch.ZSetBatch(self.schema)
        try:
            pk1 = r_uint128(100)
            pk2 = r_uint128(200)
            
            # Insert out of order
            b.append(pk2, r_int64(1), self._mk_payload("zzz", 1)) # Last by PK
            b.append(pk1, r_int64(1), self._mk_payload("bbb", 1)) # Second
            b.append(pk1, r_int64(1), self._mk_payload("aaa", 1)) # First (same PK, lower payload)
            
            # sort() performs an in-place mergesort and repacks arenas contiguously
            b.sort()
            
            self.assertEqual(b.length(), 3)
            # Check PK order
            self.assertEqual(b.get_pk(0), pk1)
            self.assertEqual(b.get_pk(1), pk1)
            self.assertEqual(b.get_pk(2), pk2)
            
            # Check Payload order within same PK group
            self.assertEqual(b.get_row(0).get_str(0), "aaa")
            self.assertEqual(b.get_row(1).get_str(0), "bbb")
            self.assertEqual(b.get_row(2).get_str(0), "zzz")
        finally:
            b.free()

    def test_algebraic_consolidation(self):
        """Verify that weights sum up for identical (Key, Payload) pairs."""
        b = batch.ZSetBatch(self.schema)
        try:
            pk = r_uint128(500)
            payload = self._mk_payload("data", 1)
            
            # Add the same logical record 3 times with different weights
            b.append(pk, r_int64(2), payload)
            b.append(pk, r_int64(5), payload)
            b.append(pk, r_int64(-1), payload)
            
            # consolidate() performs sort and weight summation across duplicate entries
            b.consolidate()
            
            # Should result in exactly 1 physical record with net weight 6
            self.assertEqual(b.length(), 1)
            self.assertEqual(b.get_pk(0), pk)
            self.assertEqual(b.get_weight(0), r_int64(6))
            self.assertEqual(b.get_row(0).get_str(0), "data")
        finally:
            b.free()

    def test_the_ghost_property(self):
        """Annihilation: records with net weight 0 must be physically removed."""
        b = batch.ZSetBatch(self.schema)
        try:
            pk = r_uint128(1)
            payload = self._mk_payload("ghost", 0)
            
            # +1 followed by -1 for the same key/payload pair
            b.append(pk, r_int64(1), payload)
            b.append(pk, r_int64(-1), payload)
            
            b.consolidate()
            
            # The Ghost Property: net-zero records are annihilated during consolidation
            self.assertEqual(b.length(), 0)
        finally:
            b.free()

    def test_multiset_preservation(self):
        """Distinct payloads for the same key should NOT be merged."""
        b = batch.ZSetBatch(self.schema)
        try:
            pk = r_uint128(10)
            
            payload_a = self._mk_payload("type_A", 100)
            payload_b = self._mk_payload("type_B", 200)
            
            b.append(pk, r_int64(1), payload_a)
            b.append(pk, r_int64(1), payload_b)
            
            b.consolidate()
            
            # Both must survive because payloads differ (Z-Sets are multisets of key-payload pairs)
            self.assertEqual(b.length(), 2)
            self.assertEqual(b.get_pk(0), pk)
            self.assertEqual(b.get_pk(1), pk)
            self.assertEqual(b.get_row(0).get_str(0), "type_A")
            self.assertEqual(b.get_row(1).get_str(0), "type_B")
        finally:
            b.free()

    def test_u128_sorting_boundaries(self):
        """Ensure 128-bit comparison works correctly for high bits in arena storage."""
        b = batch.ZSetBatch(self.schema)
        try:
            # k1 has high bits set, k2 is smaller but has max 64-bit low segment
            k1 = (r_uint128(0xFF) << 64) | r_uint128(0)
            k2 = (r_uint128(0x0F) << 64) | r_uint128(0xFFFFFFFFFFFFFFFF)
            
            b.append(k1, r_int64(1), self._mk_payload("high", 1))
            b.append(k2, r_int64(1), self._mk_payload("low", 1))
            
            b.sort()
            
            # k2 < k1 based on lexicographical 128-bit unsigned comparison
            self.assertEqual(b.get_pk(0), k2)
            self.assertEqual(b.get_pk(1), k1)
        finally:
            b.free()

    def test_append_from_accessor(self):
        """Test zero-copy append path using the RowAccessor interface."""
        b1 = batch.ZSetBatch(self.schema)
        b2 = batch.ZSetBatch(self.schema)
        try:
            pk = r_uint128(123)
            payload = self._mk_payload("acc-test", 99)
            b1.append(pk, r_int64(5), payload)
            
            # Transfer from b1 to b2 via accessor (simulating internal VM propagation)
            acc = b1.get_accessor(0)
            b2.append_from_accessor(pk, r_int64(10), acc)
            
            self.assertEqual(b2.length(), 1)
            self.assertEqual(b2.get_weight(0), r_int64(10))
            self.assertEqual(b2.get_row(0).get_str(0), "acc-test")
            self.assertEqual(b2.get_row(0).get_int_signed(1), r_int64(99))
        finally:
            b1.free()
            b2.free()

    def test_arena_capacity_expansion(self):
        """Verify that the batch correctly grows its arenas for larger volumes of data."""
        # Force a small initial capacity to trigger resizing logic
        b = batch.ZSetBatch(self.schema, initial_capacity=2)
        try:
            count = 100
            for i in range(count):
                b.append(r_uint128(i), r_int64(1), self._mk_payload("item", i))
            
            self.assertEqual(b.length(), count)
            for i in range(count):
                self.assertEqual(b.get_pk(i), r_uint128(i))
                self.assertEqual(b.get_row(i).get_int_signed(1), r_int64(i))
        finally:
            b.free()

    def test_null_value_handling(self):
        """Verify sorting and consolidation correctly handles columns with NULL values."""
        schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_I64, is_nullable=True)
        ], pk_index=0)
        
        b = batch.ZSetBatch(schema)
        try:
            pk = r_uint128(1)
            row_null = create_test_row(schema, [None])
            row_val = create_test_row(schema, [r_int64(10)])
            
            b.append(pk, r_int64(1), row_val)
            b.append(pk, r_int64(1), row_null)
            
            # Sort: NULL should come before non-NULL values in the payload
            b.sort()
            self.assertTrue(b.get_row(0).is_null(0))
            self.assertFalse(b.get_row(1).is_null(0))
            
            # Test consolidation of NULL rows
            b.append(pk, r_int64(2), row_null)
            b.consolidate()
            
            self.assertEqual(b.length(), 2)
            # The Null group should have summed to weight 3 (1 + 2)
            self.assertEqual(b.get_weight(0), r_int64(3))
            self.assertTrue(b.get_row(0).is_null(0))
        finally:
            b.free()

if __name__ == '__main__':
    unittest.main()
