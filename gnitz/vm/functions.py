# gnitz/vm/functions.py

from rpython.rlib.rarithmetic import r_int64, r_uint64
from rpython.rtyper.lltypesystem import rffi
from gnitz.core import values, types


class ScalarFunction(object):
    """
    Base class for logic executed inside MAP or FILTER.
    During translation, concrete implementations of this will be created.
    """
    _immutable_fields_ = []

    def evaluate_predicate(self, row_accessor):
        """Used by OP_FILTER. Returns True to keep row."""
        return True

    def evaluate_map(self, row_accessor, output_row_list):
        """Used by OP_MAP. Appends transformed TaggedValues to output_row_list."""
        pass


class AggregateFunction(object):
    """
    Base class for incremental aggregation functions used by REDUCE.

    An AggregateFunction defines how to fold Z-set rows into a single
    aggregate value per group. It must satisfy the abelian group structure
    required by DBSP.
    """

    def create_accumulator(self):
        # type: () -> values.TaggedValue
        """Return the identity element (zero) for this aggregate group."""
        raise NotImplementedError

    def step(self, acc, row, weight):
        # type: (values.TaggedValue, row_logic.BaseRowAccessor, r_int64) -> values.TaggedValue
        """
        Fold one weighted row into the accumulator.
        Linear aggregates: step(acc, row, w) == step(zero, row, w) + acc.
        weight may be negative (retraction).
        """
        raise NotImplementedError

    def finalize(self, acc):
        # type: (values.TaggedValue) -> values.TaggedValue
        """Extract final output value from accumulator. Default is identity."""
        return acc

    def is_linear(self):
        # type: () -> bool
        """
        Return True if this aggregate is a group homomorphism.
        Linear aggregates do NOT need trace_in; they compute
        new_agg = old_agg + agg(delta) directly.
        """
        raise NotImplementedError

    def output_column_type(self):
        # type: () -> types.FieldType
        """Return the FieldType for the output column."""
        raise NotImplementedError

    def is_accumulator_zero(self, acc):
        # type: (values.TaggedValue) -> bool
        """
        Return True if the accumulator represents the identity (zero) value.
        Used to suppress emission of (key, 0) -> +1 rows.
        """
        raise NotImplementedError


class CountAggregateFunction(AggregateFunction):
    """COUNT(*) — sums the Z-weights of all rows in the group."""

    def create_accumulator(self):
        return values.TaggedValue(values.TAG_INT, r_int64(0), r_uint64(0), 0.0, "")

    def step(self, acc, row, weight):
        return values.TaggedValue(values.TAG_INT, acc.i64 + weight, r_uint64(0), 0.0, "")

    def is_linear(self):
        return True

    def output_column_type(self):
        return types.TYPE_I64

    def is_accumulator_zero(self, acc):
        return acc.i64 == 0


class SumI64AggregateFunction(AggregateFunction):
    """SUM(col) for signed 64-bit integer columns."""

    _immutable_fields_ = ['col_idx']

    def __init__(self, col_idx):
        self.col_idx = col_idx

    def create_accumulator(self):
        return values.TaggedValue(values.TAG_INT, r_int64(0), r_uint64(0), 0.0, "")

    def step(self, acc, row, weight):
        val = rffi.cast(rffi.LONGLONG, row.get_int(self.col_idx))
        return values.TaggedValue(values.TAG_INT, acc.i64 + val * weight, r_uint64(0), 0.0, "")

    def is_linear(self):
        return True

    def output_column_type(self):
        return types.TYPE_I64

    def is_accumulator_zero(self, acc):
        return acc.i64 == 0


class MinI64AggregateFunction(AggregateFunction):
    """MIN(col) — non-linear. Requires trace_in."""

    _immutable_fields_ = ['col_idx']
    # INT64_MAX sentinel
    _SENTINEL = 9223372036854775807

    def __init__(self, col_idx):
        self.col_idx = col_idx

    def create_accumulator(self):
        return values.TaggedValue(values.TAG_INT, r_int64(self._SENTINEL), r_uint64(0), 0.0, "")

    def step(self, acc, row, weight):
        val = rffi.cast(rffi.LONGLONG, row.get_int(self.col_idx))
        if val < acc.i64:
            return values.TaggedValue(values.TAG_INT, val, r_uint64(0), 0.0, "")
        return acc

    def is_linear(self):
        return False

    def output_column_type(self):
        return types.TYPE_I64

    def is_accumulator_zero(self, acc):
        return acc.i64 == self._SENTINEL


class MaxI64AggregateFunction(AggregateFunction):
    """MAX(col) — non-linear. Requires trace_in."""

    _immutable_fields_ = ['col_idx']
    # INT64_MIN sentinel
    _SENTINEL = -9223372036854775808

    def __init__(self, col_idx):
        self.col_idx = col_idx

    def create_accumulator(self):
        return values.TaggedValue(values.TAG_INT, r_int64(self._SENTINEL), r_uint64(0), 0.0, "")

    def step(self, acc, row, weight):
        val = rffi.cast(rffi.LONGLONG, row.get_int(self.col_idx))
        if val > acc.i64:
            return values.TaggedValue(values.TAG_INT, val, r_uint64(0), 0.0, "")
        return acc

    def is_linear(self):
        return False

    def output_column_type(self):
        return types.TYPE_I64

    def is_accumulator_zero(self, acc):
        return acc.i64 == self._SENTINEL
