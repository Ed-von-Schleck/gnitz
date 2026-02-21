# gnitz/vm/functions.py

from rpython.rlib.rarithmetic import r_int64, intmask, r_uint64
from rpython.rtyper.lltypesystem import rffi
from gnitz.core import types

class ScalarFunction(object):
    _immutable_fields_ = []

    def evaluate_predicate(self, row_accessor):
        raise NotImplementedError

    def evaluate_map(self, row_accessor, output_row):
        raise NotImplementedError


class AggregateFunction(object):
    _immutable_fields_ = []

    def reset(self):
        raise NotImplementedError

    def step(self, row_accessor, weight):
        raise NotImplementedError

    def merge_accumulated(self, value_bits, weight):
        """
        Merges a pre-calculated aggregate result (from the trace) into the current state.
        Used for linear aggregation optimizations (Agg(Old + New) = Agg(Old) + Agg(New)).
        
        value_bits: r_uint64 (raw bits from storage)
        weight: r_int64
        """
        raise NotImplementedError

    def emit(self, output_row):
        raise NotImplementedError

    def is_linear(self):
        raise NotImplementedError

    def output_column_type(self):
        raise NotImplementedError

    def is_accumulator_zero(self):
        raise NotImplementedError


class CountAggregateFunction(AggregateFunction):
    def __init__(self):
        self._count = r_int64(0)

    def reset(self):
        self._count = r_int64(0)

    def step(self, row_accessor, weight):
        self._count = r_int64(intmask(self._count + weight))

    def merge_accumulated(self, value_bits, weight):
        # value_bits represents the previous count. 
        # We add (prev_count * weight).
        prev_cnt = rffi.cast(rffi.LONGLONG, value_bits)
        self._count = r_int64(intmask(self._count + (prev_cnt * weight)))

    def emit(self, output_row):
        output_row.append_int(self._count)

    def is_linear(self):
        return True

    def output_column_type(self):
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return self._count == 0


class SumI64AggregateFunction(AggregateFunction):
    _immutable_fields_ = ["col_idx"]

    def __init__(self, col_idx):
        self.col_idx = col_idx
        self._sum = r_int64(0)

    def reset(self):
        self._sum = r_int64(0)

    def step(self, row_accessor, weight):
        if row_accessor.is_null(self.col_idx):
            return
        val_u = row_accessor.get_int(self.col_idx)
        val = rffi.cast(rffi.LONGLONG, val_u)
        weighted_val = r_int64(intmask(val * weight))
        self._sum = r_int64(intmask(self._sum + weighted_val))

    def merge_accumulated(self, value_bits, weight):
        # value_bits represents the previous sum.
        prev_sum = rffi.cast(rffi.LONGLONG, value_bits)
        # We add (prev_sum * weight)
        weighted_prev = r_int64(intmask(prev_sum * weight))
        self._sum = r_int64(intmask(self._sum + weighted_prev))

    def emit(self, output_row):
        output_row.append_int(self._sum)

    def is_linear(self):
        return True

    def output_column_type(self):
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return self._sum == 0


class MinI64AggregateFunction(AggregateFunction):
    _immutable_fields_ = ["col_idx"]
    _MAX_I64 = r_int64(9223372036854775807)

    def __init__(self, col_idx):
        self.col_idx = col_idx
        self._val = self._MAX_I64

    def reset(self):
        self._val = self._MAX_I64

    def step(self, row_accessor, weight):
        if row_accessor.is_null(self.col_idx):
            return
        val_u = row_accessor.get_int(self.col_idx)
        val = rffi.cast(rffi.LONGLONG, val_u)
        if val < self._val:
            self._val = val

    def merge_accumulated(self, value_bits, weight):
        # Not linear; this path is never taken by ReduceOp
        pass

    def emit(self, output_row):
        output_row.append_int(self._val)

    def is_linear(self):
        return False

    def output_column_type(self):
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return self._val == self._MAX_I64


class MaxI64AggregateFunction(AggregateFunction):
    _immutable_fields_ = ["col_idx"]
    _MIN_I64 = r_int64(-9223372036854775808)

    def __init__(self, col_idx):
        self.col_idx = col_idx
        self._val = self._MIN_I64

    def reset(self):
        self._val = self._MIN_I64

    def step(self, row_accessor, weight):
        if row_accessor.is_null(self.col_idx):
            return
        val_u = row_accessor.get_int(self.col_idx)
        val = rffi.cast(rffi.LONGLONG, val_u)
        if val > self._val:
            self._val = val

    def merge_accumulated(self, value_bits, weight):
        pass

    def emit(self, output_row):
        output_row.append_int(self._val)

    def is_linear(self):
        return False

    def output_column_type(self):
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return self._val == self._MIN_I64
