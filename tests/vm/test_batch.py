# gnitz/tests/vm/test_batch.py

import unittest
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, r_int64
from gnitz.core import types, values as db_values
from gnitz.vm import batch

class TestZSetBatch(unittest.TestCase):
    def setUp(self):
        # Schema: PK (u128) | String | Int64
        self.schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_I64)
        ], pk_index=0)

    def _mk_payload(self, s, i):
        """Helper to create the List[TaggedValue] payload (excluding PK)."""
        return [
            db_values.TaggedValue.make_string(s),
            db_values.TaggedValue.make_int(i)
        ]

    def test_batch_append_and_clear(self):
        """Basic lifecycle test."""
        b = batch.ZSetBatch(self.schema)
        self.assertEqual(b.row_count(), 0)
        
        b.append(r_uint128(1), 1, self._mk_payload("a", 10))
        self.assertEqual(b.row_count(), 1)
        
        b.clear()
        self.assertEqual(b.row_count(), 0)

    def test_batch_sorting_logic(self):
        """Verify sorting by PK then by Payload content."""
        b = batch.ZSetBatch(self.schema)
        
        pk1 = r_uint128(100)
        pk2 = r_uint128(200)
        
        # Insert out of order
        b.append(pk2, 1, self._mk_payload("zzz", 1)) # Last by PK
        b.append(pk1, 1, self._mk_payload("bbb", 1)) # Second
        b.append(pk1, 1, self._mk_payload("aaa", 1)) # First (same PK, lower string)
        
        b.sort()
        
        # Check PK order
        self.assertEqual(b.get_key(0), pk1)
        self.assertEqual(b.get_key(1), pk1)
        self.assertEqual(b.get_key(2), pk2)
        
        # Check Payload order within same PK
        self.assertEqual(b.get_payload(0)[0].str_val, "aaa")
        self.assertEqual(b.get_payload(1)[0].str_val, "bbb")

    def test_algebraic_consolidation(self):
        """Verify that weights sum up for identical (Key, Payload) pairs."""
        b = batch.ZSetBatch(self.schema)
        pk = r_uint128(500)
        payload = self._mk_payload("data", 1)
        
        # Add the same record 3 times with different weights
        b.append(pk, 2, payload)
        b.append(pk, 5, payload)
        b.append(pk, -1, payload)
        
        b.sort()
        b.consolidate()
        
        # Should result in exactly 1 record with weight 6
        self.assertEqual(b.row_count(), 1)
        self.assertEqual(b.get_weight(0), r_int64(6))

    def test_the_ghost_property(self):
        """Annihilation: records with net weight 0 must be removed."""
        b = batch.ZSetBatch(self.schema)
        pk = r_uint128(1)
        payload = self._mk_payload("ghost", 0)
        
        # +1 followed by -1
        b.append(pk, 1, payload)
        b.append(pk, -1, payload)
        
        b.sort()
        b.consolidate()
        
        # Batch should be empty
        self.assertEqual(b.row_count(), 0)

    def test_multiset_preservation(self):
        """Distinct payloads for the same key should not be merged."""
        b = batch.ZSetBatch(self.schema)
        pk = r_uint128(10)
        
        payload_a = self._mk_payload("type_A", 100)
        payload_b = self._mk_payload("type_B", 200)
        
        b.append(pk, 1, payload_a)
        b.append(pk, 1, payload_b)
        
        b.sort()
        b.consolidate()
        
        # Both must survive because payloads are different
        self.assertEqual(b.row_count(), 2)
        self.assertEqual(b.get_weight(0), r_int64(1))
        self.assertEqual(b.get_weight(1), r_int64(1))

    def test_u128_sorting_boundaries(self):
        """Ensure 128-bit comparison works for high bits."""
        b = batch.ZSetBatch(self.schema)
        
        # k1 is very large, k2 is smaller but with high bits set
        k1 = (r_uint128(0xFF) << 64) | r_uint128(0)
        k2 = (r_uint128(0x0F) << 64) | r_uint128(0xFFFFFFFFFFFFFFFF)
        
        b.append(k1, 1, self._mk_payload("high", 1))
        b.append(k2, 1, self._mk_payload("low", 1))
        
        b.sort()
        
        # k2 should be first
        self.assertEqual(b.get_key(0), k2)
        self.assertEqual(b.get_key(1), k1)

    def test_complex_consolidation_interleaving(self):
        """Test mixing survivors and ghosts in one batch."""
        b = batch.ZSetBatch(self.schema)
        
        # 1. Survives with weight 2
        b.append(r_uint128(10), 1, self._mk_payload("survive", 1))
        b.append(r_uint128(10), 1, self._mk_payload("survive", 1))
        
        # 2. Becomes a ghost
        b.append(r_uint128(20), 1, self._mk_payload("die", 1))
        b.append(r_uint128(20), -1, self._mk_payload("die", 1))
        
        # 3. Survives with weight -1
        b.append(r_uint128(30), -1, self._mk_payload("retract", 1))
        
        b.sort()
        b.consolidate()
        
        self.assertEqual(b.row_count(), 2)
        self.assertEqual(b.get_key(0), r_uint128(10))
        self.assertEqual(b.get_weight(0), r_int64(2))
        self.assertEqual(b.get_key(1), r_uint128(30))
        self.assertEqual(b.get_weight(1), r_int64(-1))

if __name__ == '__main__':
    unittest.main()
