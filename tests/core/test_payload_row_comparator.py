# tests/core/test_payload_row_comparator.py

"""
Unit tests for PayloadRowComparator.

Verifies that the API proxy in gnitz/core/row_logic.py correctly delegates
to the canonical storage comparator for all column types, with particular
focus on:

  1. TAG_U128 columns — high-word differences must be detected (pre-fix they
     were invisible because ValueAccessor.get_u128 reconstructed only from
     val.i64, leaving val.u128_hi unread).

  2. Unsigned integer ordering — TYPE_U64 values with the top bit set must
     sort after 0, not before (signed i64 comparison would invert this).

  3. Strings — basic lexicographic ordering must be preserved through the
     proxy layer.

These tests do NOT require a PersistentTable; they exercise the comparator
directly using in-memory TaggedValue lists.
"""

import unittest
from rpython.rlib.rarithmetic import r_uint64

from gnitz.core import types, values
from gnitz.core.row_logic import make_payload_row, PayloadRowComparator


def _schema(*col_types):
    """Build a TableSchema with a TYPE_U64 PK followed by the given column types."""
    cols = [types.ColumnDefinition(types.TYPE_U64)]  # PK
    for ct in col_types:
        cols.append(types.ColumnDefinition(ct))
    return types.TableSchema(cols, pk_index=0)


def _row(*tagged_values):
    row = make_payload_row(len(tagged_values))
    for v in tagged_values:
        row.append(v)
    return row


def _u128(lo, hi):
    return values.TaggedValue.make_u128(r_uint64(lo), r_uint64(hi))


class TestPayloadRowComparatorU128(unittest.TestCase):

    def setUp(self):
        self.schema = _schema(types.TYPE_U128)
        self.cmp = PayloadRowComparator(self.schema)

    def _cmp(self, lo_a, hi_a, lo_b, hi_b):
        return self.cmp.compare(_row(_u128(lo_a, hi_a)), _row(_u128(lo_b, hi_b)))

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

    Pre-fix, _compare_payload_rows used signed i64 comparison, causing
    0xFFFF...FFFF to sort BEFORE 0 (since it's -1 in signed arithmetic).
    PayloadRowComparator delegates to compare_rows which uses rffi.ULONGLONG
    (unsigned), so 0xFFFF...FFFF must sort AFTER 0.
    """

    def setUp(self):
        self.schema = _schema(types.TYPE_U64)
        self.cmp = PayloadRowComparator(self.schema)

    def test_max_u64_sorts_after_zero(self):
        max_val = values.TaggedValue.make_u64(r_uint64(0xFFFFFFFFFFFFFFFF))
        zero = values.TaggedValue.make_u64(r_uint64(0))
        result = self.cmp.compare(_row(max_val), _row(zero))
        self.assertGreater(result, 0)

    def test_high_bit_set_sorts_after_zero(self):
        high_bit = values.TaggedValue.make_u64(r_uint64(1) << 63)
        zero = values.TaggedValue.make_u64(r_uint64(0))
        result = self.cmp.compare(_row(high_bit), _row(zero))
        self.assertGreater(result, 0)

    def test_equal_ints(self):
        v = values.TaggedValue.make_u64(r_uint64(42))
        result = self.cmp.compare(_row(v), _row(v))
        self.assertEqual(result, 0)


class TestPayloadRowComparatorString(unittest.TestCase):

    def setUp(self):
        self.schema = _schema(types.TYPE_STRING)
        self.cmp = PayloadRowComparator(self.schema)

    def _cmp(self, s1, s2):
        return self.cmp.compare(
            _row(values.TaggedValue.make_string(s1)),
            _row(values.TaggedValue.make_string(s2)),
        )

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

    def test_first_column_takes_priority(self):
        """Row with smaller u128 wins even if its string is lexicographically larger."""
        row_a = _row(_u128(1, 0), values.TaggedValue.make_string("zzz"))
        row_b = _row(_u128(2, 0), values.TaggedValue.make_string("aaa"))
        self.assertLess(self.cmp.compare(row_a, row_b), 0)

    def test_second_column_breaks_tie(self):
        """When u128 values are equal, the string column decides the order."""
        row_a = _row(_u128(5, 5), values.TaggedValue.make_string("aaa"))
        row_b = _row(_u128(5, 5), values.TaggedValue.make_string("zzz"))
        self.assertLess(self.cmp.compare(row_a, row_b), 0)

    def test_fully_equal(self):
        row_a = _row(_u128(5, 5), values.TaggedValue.make_string("same"))
        row_b = _row(_u128(5, 5), values.TaggedValue.make_string("same"))
        self.assertEqual(self.cmp.compare(row_a, row_b), 0)


if __name__ == "__main__":
    unittest.main()
