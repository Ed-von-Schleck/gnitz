# tests/core/test_payload_row_comparator.py

"""
Unit tests for PayloadRowComparator.

Verifies that the API proxy in gnitz/core/row_logic.py correctly delegates
to the canonical storage comparator for all column types, with particular
focus on:

  1. U128 columns — high-word differences must be detected (pre-fix they
     were invisible because the accessor reconstructed only from the low 64 bits).

  2. Unsigned integer ordering — TYPE_U64 values with the top bit set must
     sort after 0, not before (signed i64 comparison would invert this).

  3. Strings — basic lexicographic ordering must be preserved through the
     proxy layer.

These tests exercise the comparator directly using in-memory PayloadRow objects.
"""

import unittest
from rpython.rlib.rarithmetic import r_uint64

from gnitz.core import types
from gnitz.core.row_logic import PayloadRowComparator
from tests.row_helpers import create_test_row


def _schema(*col_types):
    """Build a TableSchema with a TYPE_U64 PK followed by the given column types."""
    cols = [types.ColumnDefinition(types.TYPE_U64)]  # PK
    for ct in col_types:
        cols.append(types.ColumnDefinition(ct))
    return types.TableSchema(cols, pk_index=0)


class TestPayloadRowComparatorU128(unittest.TestCase):

    def setUp(self):
        self.schema = _schema(types.TYPE_U128)
        self.cmp = PayloadRowComparator(self.schema)

    def _row(self, lo, hi):
        return create_test_row(self.schema, [(lo, hi)])

    def _cmp(self, lo_a, hi_a, lo_b, hi_b):
        return self.cmp.compare(self._row(lo_a, hi_a), self._row(lo_b, hi_b))

    def test_equal_values_return_zero(self):
        self.assertEqual(self._cmp(0xDEAD, 0xBEEF, 0xDEAD, 0xBEEF), 0)

    def test_high_word_difference_detected(self):
        """Core regression: rows differing only in u128_hi must not compare equal."""
        result = self._cmp(0xDEAD, 0xBEEF, 0xDEAD, 0xBEEF + 1)
        self.assertNotEqual(result, 0)

    def test_high_word_ordering_less(self):
        """(lo, hi) < (lo, hi+1) when lo is identical."""
        result = self._cmp(0xDEAD, 0xBEEF, 0xDEAD, 0xBEEF + 1)
        self.assertLess(result, 0)

    def test_high_word_ordering_greater(self):
        result = self._cmp(0xDEAD, 0xBEEF + 1, 0xDEAD, 0xBEEF)
        self.assertGreater(result, 0)

    def test_low_word_difference_detected(self):
        result = self._cmp(0xDEAD + 1, 0xBEEF, 0xDEAD, 0xBEEF)
        self.assertGreater(result, 0)

    def test_zero_vs_max(self):
        """Zero < max u128 value."""
        result = self._cmp(0, 0, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)
        self.assertLess(result, 0)

    def test_high_word_dominates_low_word(self):
        """(lo=MAX, hi=0) < (lo=0, hi=1) because hi is the more significant word."""
        result = self._cmp(0xFFFFFFFFFFFFFFFF, 0, 0, 1)
        self.assertLess(result, 0)


class TestPayloadRowComparatorUnsignedInt(unittest.TestCase):
    """
    Unsigned integer column ordering must be correct for values with top bit set.

    Pre-fix, the engine used signed i64 comparison, causing 0xFFFF...FFFF
    to sort BEFORE 0. PayloadRowComparator delegates to compare_rows which 
    uses unsigned comparison, so 0xFFFF...FFFF must sort AFTER 0.
    """

    def setUp(self):
        self.schema = _schema(types.TYPE_U64)
        self.cmp = PayloadRowComparator(self.schema)

    def _row(self, val):
        return create_test_row(self.schema, [val])

    def test_max_u64_sorts_after_zero(self):
        row_max = self._row(r_uint64(0xFFFFFFFFFFFFFFFF))
        row_zero = self._row(r_uint64(0))
        result = self.cmp.compare(row_max, row_zero)
        self.assertGreater(result, 0)

    def test_high_bit_set_sorts_after_zero(self):
        row_high = self._row(r_uint64(1) << 63)
        row_zero = self._row(r_uint64(0))
        result = self.cmp.compare(row_high, row_zero)
        self.assertGreater(result, 0)

    def test_equal_ints(self):
        row_a = self._row(42)
        row_b = self._row(42)
        result = self.cmp.compare(row_a, row_b)
        self.assertEqual(result, 0)


class TestPayloadRowComparatorString(unittest.TestCase):

    def setUp(self):
        self.schema = _schema(types.TYPE_STRING)
        self.cmp = PayloadRowComparator(self.schema)

    def _cmp(self, s1, s2):
        row_a = create_test_row(self.schema, [s1])
        row_b = create_test_row(self.schema, [s2])
        return self.cmp.compare(row_a, row_b)

    def test_equal_strings(self):
        self.assertEqual(self._cmp("hello", "hello"), 0)

    def test_less_than(self):
        self.assertLess(self._cmp("apple", "banana"), 0)

    def test_greater_than(self):
        self.assertGreater(self._cmp("zebra", "apple"), 0)

    def test_empty_string_less_than_nonempty(self):
        self.assertLess(self._cmp("", "a"), 0)


class TestPayloadRowComparatorMultiColumn(unittest.TestCase):
    """Verify that multi-column schemas compare lexicographically left-to-right."""

    def setUp(self):
        self.schema = _schema(types.TYPE_U128, types.TYPE_STRING)
        self.cmp = PayloadRowComparator(self.schema)

    def _row(self, u128_lo, u128_hi, s_val):
        return create_test_row(self.schema, [(u128_lo, u128_hi), s_val])

    def test_first_column_takes_priority(self):
        """Row with smaller u128 wins even if its string is lexicographically larger."""
        row_a = self._row(1, 0, "zzz")
        row_b = self._row(2, 0, "aaa")
        self.assertLess(self.cmp.compare(row_a, row_b), 0)

    def test_second_column_breaks_tie(self):
        """When u128 values are equal, the string column decides the order."""
        row_a = self._row(5, 5, "aaa")
        row_b = self._row(5, 5, "zzz")
        self.assertLess(self.cmp.compare(row_a, row_b), 0)

    def test_fully_equal(self):
        row_a = self._row(5, 5, "same")
        row_b = self._row(5, 5, "same")
        self.assertEqual(self.cmp.compare(row_a, row_b), 0)


if __name__ == "__main__":
    unittest.main()
