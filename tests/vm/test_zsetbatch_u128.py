# tests/vm/test_zsetbatch_u128.py

"""
ZSetBatch sort() and consolidate() correctness tests with TYPE_U128 payload columns.

Key invariants under test:

  1. Two entries with the SAME (pk, u128_payload) must be consolidated
     (weights summed). Pre-fix this worked only if the high word happened
     to be zero, because _compare_payload_rows used val.i64 for all types.

  2. Two entries with the SAME pk but DIFFERENT u128 hi-words must NOT be
     consolidated — they are distinct multiset members.

  3. sort() must place rows in a deterministic order that consolidate() can
     act on; the order must be consistent with PayloadRowComparator.

  4. The existing sort/consolidate logic for unsigned TYPE_U64 columns must
     also order values with the top bit set correctly (unsigned semantics).
"""

import unittest
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128

from gnitz.core import types, values
from gnitz.core.row_logic import make_payload_row
from gnitz.vm.batch import ZSetBatch


def _schema_u128_payload():
    """PK=U64, payload=[U128]"""
    return types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_U128),
        ],
        pk_index=0,
    )


def _schema_u64_payload():
    """PK=U64, payload=[U64]"""
    return types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_U64),
        ],
        pk_index=0,
    )


def _u128_row(lo, hi):
    row = make_payload_row(1)
    row.append(values.TaggedValue.make_u128(r_uint64(lo), r_uint64(hi)))
    return row


def _u64_row(val):
    row = make_payload_row(1)
    row.append(values.TaggedValue.make_u64(r_uint64(val)))
    return row


def _pk(n):
    return r_uint128(n)


class TestZSetBatchU128Consolidate(unittest.TestCase):

    def setUp(self):
        self.schema = _schema_u128_payload()

    def _make_batch(self):
        return ZSetBatch(self.schema)

    def test_identical_u128_rows_consolidate(self):
        """Two entries with the same pk and same u128 payload → one entry, summed weight."""
        b = self._make_batch()
        row = _u128_row(0xDEAD, 0xBEEF)
        b.append(_pk(1), r_int64(1), row)
        b.append(_pk(1), r_int64(1), _u128_row(0xDEAD, 0xBEEF))
        b.sort()
        b.consolidate()

        self.assertEqual(b.row_count(), 1)
        self.assertEqual(b.get_weight(0), r_int64(2))

    def test_different_hi_word_not_consolidated(self):
        """
        Two entries with same pk and same lo but different hi must remain separate.
        Pre-fix: both would truncate to the same i64 and be wrongly merged.
        """
        b = self._make_batch()
        b.append(_pk(1), r_int64(1), _u128_row(0xDEAD, 0xBEEF))
        b.append(_pk(1), r_int64(1), _u128_row(0xDEAD, 0xBEEF + 1))
        b.sort()
        b.consolidate()

        self.assertEqual(b.row_count(), 2)
        self.assertEqual(b.get_weight(0), r_int64(1))
        self.assertEqual(b.get_weight(1), r_int64(1))

    def test_annihilation_removes_row(self):
        """Insert +1 then -1 for the same u128 payload → row annihilated, count 0."""
        b = self._make_batch()
        row = _u128_row(0xCAFE, 0xBABE)
        b.append(_pk(2), r_int64(1), row)
        b.append(_pk(2), r_int64(-1), _u128_row(0xCAFE, 0xBABE))
        b.sort()
        b.consolidate()

        self.assertEqual(b.row_count(), 0)

    def test_high_word_only_match_not_consolidated(self):
        """Same hi but different lo → two distinct entries."""
        b = self._make_batch()
        b.append(_pk(1), r_int64(1), _u128_row(0x0001, 0xFFFF))
        b.append(_pk(1), r_int64(1), _u128_row(0x0002, 0xFFFF))
        b.sort()
        b.consolidate()

        self.assertEqual(b.row_count(), 2)

    def test_sort_order_high_word_dominates(self):
        """
        After sort, (lo=MAX, hi=0) must come before (lo=0, hi=1) because
        hi is the more significant word in the 128-bit value.
        """
        b = self._make_batch()
        b.append(_pk(1), r_int64(1), _u128_row(0xFFFFFFFFFFFFFFFF, 0))
        b.append(_pk(1), r_int64(1), _u128_row(0, 1))
        b.sort()

        # After sort: entry with hi=0 must come first
        first_hi = b.payloads[0][0].u128_hi
        second_hi = b.payloads[1][0].u128_hi
        self.assertEqual(r_uint64(first_hi), r_uint64(0))
        self.assertEqual(r_uint64(second_hi), r_uint64(1))

    def test_zero_u128_consolidates_correctly(self):
        """The all-zero u128 value is a valid payload and must consolidate normally."""
        b = self._make_batch()
        b.append(_pk(1), r_int64(3), _u128_row(0, 0))
        b.append(_pk(1), r_int64(2), _u128_row(0, 0))
        b.sort()
        b.consolidate()

        self.assertEqual(b.row_count(), 1)
        self.assertEqual(b.get_weight(0), r_int64(5))


class TestZSetBatchU64UnsignedOrdering(unittest.TestCase):
    """
    Unsigned TYPE_U64 payload ordering must be correct for values with top bit set.

    Before the fix, _compare_payload_rows did signed i64 comparison, causing
    values > 0x7FFF...FFFF to sort before smaller unsigned values.
    After the fix, PayloadRowComparator delegates to compare_rows which uses
    unsigned comparison via rffi.ULONGLONG.
    """

    def setUp(self):
        self.schema = _schema_u64_payload()

    def test_max_u64_does_not_consolidate_with_zero(self):
        """MAX_U64 and 0 are distinct values and must not be merged."""
        b = ZSetBatch(self.schema)
        b.append(_pk(1), r_int64(1), _u64_row(0xFFFFFFFFFFFFFFFF))
        b.append(_pk(1), r_int64(1), _u64_row(0))
        b.sort()
        b.consolidate()

        self.assertEqual(b.row_count(), 2)

    def test_max_u64_sorts_after_zero(self):
        """After sort, 0 must come before MAX_U64 (unsigned ordering)."""
        b = ZSetBatch(self.schema)
        b.append(_pk(1), r_int64(1), _u64_row(0xFFFFFFFFFFFFFFFF))
        b.append(_pk(1), r_int64(1), _u64_row(0))
        b.sort()

        # payloads[0] should be 0, payloads[1] should be MAX
        first_val = r_uint64(b.payloads[0][0].i64)
        second_val = r_uint64(b.payloads[1][0].i64)
        self.assertEqual(first_val, r_uint64(0))
        self.assertEqual(second_val, r_uint64(0xFFFFFFFFFFFFFFFF))


if __name__ == "__main__":
    unittest.main()
