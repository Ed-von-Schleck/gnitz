# gnitz/vm/functions.py

from rpython.rlib.rarithmetic import r_int64, intmask, r_uint64
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rtyper.lltypesystem import rffi
from gnitz.core import types


class ScalarFunction(object):
    """
    Base class for User Defined Functions (UDFs) and Standard Operators.
    
    The VM JIT-compiles these classes directly. For high performance, 
    specific queries are often compiled into specific subclasses of this 
    (as seen in vm_comprehensive_test.py), but a standard library is 
    provided below for general relational algebra.
    """
    _immutable_fields_ = []

    def evaluate_predicate(self, row_accessor):
        """
        Evaluates a boolean condition (FILTER).
        Returns True to keep the row, False to drop it.
        """
        raise NotImplementedError

    def evaluate_map(self, row_accessor, output_row):
        """
        Transforms input row to output row (MAP/PROJECT).
        Must call append_* methods on output_row in schema order.
        """
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Standard Predicates (Filters)
# ---------------------------------------------------------------------------

class TruePredicate(ScalarFunction):
    def evaluate_predicate(self, row_accessor):
        return True


class FloatGt(ScalarFunction):
    """Filter: col[idx] > constant"""
    _immutable_fields_ = ['col_idx', 'val']

    def __init__(self, col_idx, val):
        self.col_idx = col_idx
        self.val = val

    def evaluate_predicate(self, row_accessor):
        return row_accessor.get_float(self.col_idx) > self.val


class IntEq(ScalarFunction):
    """Filter: col[idx] == constant"""
    _immutable_fields_ = ['col_idx', 'val']

    def __init__(self, col_idx, val):
        self.col_idx = col_idx
        self.val = val

    def evaluate_predicate(self, row_accessor):
        # Use signed comparison for standard integers
        return row_accessor.get_int_signed(self.col_idx) == self.val


# ---------------------------------------------------------------------------
# Standard Mappers (Projections)
# ---------------------------------------------------------------------------

class IdentityMapper(ScalarFunction):
    """Selects all columns from input to output (SELECT *)."""
    _immutable_fields_ = ['schema', 'col_types[*]']

    def __init__(self, schema):
        self.schema = schema
        # Cache types to avoid schema lookup in the hot loop
        self.col_types = [c.field_type.code for c in schema.columns 
                          if c != schema.get_pk_column()]

    def evaluate_map(self, row_accessor, output_row):
        # Iterate over payload columns (indices 0..N-1 in accessor)
        for i in range(len(self.col_types)):
            t = self.col_types[i]
            if t == types.TYPE_STRING.code:
                output_row.append_string(row_accessor.get_str_struct(i)[4])
            elif t == types.TYPE_F64.code or t == types.TYPE_F32.code:
                output_row.append_float(row_accessor.get_float(i))
            elif t == types.TYPE_U128.code:
                val = row_accessor.get_u128(i)
                # Split u128 back to u64 parts for appending
                output_row.append_u128(r_uint64(val), r_uint64(val >> 64))
            elif row_accessor.is_null(i):
                output_row.append_null(i)
            else:
                # Default integer path
                output_row.append_int(row_accessor.get_int_signed(i))


class ProjectionMapper(ScalarFunction):
    """
    Selects specific columns (SELECT col_A, col_B).
    Defined by a list of source indices and their types.
    """
    _immutable_fields_ = ['src_indices[*]', 'src_types[*]']

    def __init__(self, src_indices, src_types):
        assert len(src_indices) == len(src_types)
        self.src_indices = src_indices
        self.src_types = src_types

    def evaluate_map(self, row_accessor, output_row):
        for i in range(len(self.src_indices)):
            src_idx = self.src_indices[i]
            t = self.src_types[i]

            if row_accessor.is_null(src_idx):
                # Note: MapOutputAccessor.append_null ignores the arg index 
                # and appends to the current position, which is correct here.
                output_row.append_null(0)
                continue

            if t == types.TYPE_STRING.code:
                output_row.append_string(row_accessor.get_str_struct(src_idx)[4])
            elif t == types.TYPE_F64.code or t == types.TYPE_F32.code:
                output_row.append_float(row_accessor.get_float(src_idx))
            elif t == types.TYPE_U128.code:
                val = row_accessor.get_u128(src_idx)
                output_row.append_u128(r_uint64(val), r_uint64(val >> 64))
            else:
                output_row.append_int(row_accessor.get_int_signed(src_idx))


# ---------------------------------------------------------------------------
# Aggregates (Existing)
# ---------------------------------------------------------------------------

class AggregateFunction(object):
    _immutable_fields_ = []

    def reset(self):
        raise NotImplementedError

    def step(self, row_accessor, weight):
        raise NotImplementedError

    def merge_accumulated(self, value_bits, weight):
        """
        Merges a pre-calculated aggregate result from the trace into the
        current accumulator.  Only called when is_linear() is True.

        value_bits: r_uint64 — raw bit pattern from storage (same encoding
                    as get_value_bits() returns).
        weight:     r_int64 — multiplier to apply before merging.
        """
        raise NotImplementedError

    def get_value_bits(self):
        """
        Returns the current accumulator state as a raw r_uint64 bit pattern.

        For integer accumulators the pattern is the two's-complement r_int64
        value reinterpreted as unsigned.  For float accumulators it is the
        IEEE 754 double-precision bit pattern produced by float2longlong and
        reinterpreted as unsigned.

        This is the sole read surface consumed by ReduceAccessor to serialise
        results through the zero-copy batch path.  It replaces the former
        emit(output_row) path which was removed after the zero-copy refactor.
        """
        raise NotImplementedError

    def output_is_float(self):
        """
        Returns True when the accumulator holds a floating-point bit pattern.

        ReduceAccessor uses this flag to route the aggregate column through
        get_float() — which reconstructs the value via longlong2float — rather
        than get_int().  Must be consistent with output_column_type().
        """
        raise NotImplementedError

    def is_linear(self):
        raise NotImplementedError

    def output_column_type(self):
        raise NotImplementedError

    def is_accumulator_zero(self):
        raise NotImplementedError


# ---------------------------------------------------------------------------
# COUNT
# ---------------------------------------------------------------------------


class CountAggregateFunction(AggregateFunction):
    """
    Counts the weighted number of records in each group.

    Linear: COUNT(A + B) = COUNT(A) + COUNT(B).
    is_accumulator_zero() returns True when the weighted count is zero,
    meaning the group contributes nothing to the Z-Set output.
    """

    def __init__(self):
        self._count = r_int64(0)

    def reset(self):
        self._count = r_int64(0)

    def step(self, row_accessor, weight):
        self._count = r_int64(intmask(self._count + weight))

    def merge_accumulated(self, value_bits, weight):
        prev_cnt = rffi.cast(rffi.LONGLONG, value_bits)
        self._count = r_int64(intmask(self._count + (prev_cnt * weight)))

    def get_value_bits(self):
        return r_uint64(intmask(self._count))

    def output_is_float(self):
        return False

    def is_linear(self):
        return True

    def output_column_type(self):
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return self._count == 0


# ---------------------------------------------------------------------------
# SUM
# ---------------------------------------------------------------------------


class SumAggregateFunction(AggregateFunction):
    """
    Computes the weighted algebraic sum of a column.

    Supported column types: all integer types (I8..U64) and float types
    (F32, F64).  TYPE_STRING and TYPE_U128 are rejected at construction
    time with an assertion.

    Accumulator (_acc: r_int64):
      - Integer types: stores two's-complement I64-widened bits.
        Unsigned column types are reinterpreted as signed via rffi.cast
        before accumulation — this matches the existing pattern for large U64
        values and is consistent with the I64 output type.
      - Float types: stores the IEEE 754 double-precision bit pattern via
        float2longlong.  Crucially, float2longlong(0.0) == r_int64(0), so the
        zero initialiser r_int64(0) is shared between integer and float paths
        with no special casing.

    Output column type:
      - F32 / F64 input  →  TYPE_F64  (widened to double)
      - all integer input →  TYPE_I64  (widened to signed 64-bit)

    is_linear() returns True: SUM(A + B) = SUM(A) + SUM(B).

    is_accumulator_zero() checks _acc == r_int64(0).  For float types the
    -0.0 edge case (bit pattern 0x8000000000000000) would not trigger this
    check, but -0.0 is unreachable via ordinary weighted summation starting
    from +0.0, so this is acceptable.

    col_type_code is stored in _immutable_fields_ so the JIT promotes it to
    a compile-time constant per trace, folding all dispatch branches in step()
    and producing type-specialised machine code equivalent to a separate class
    per type.
    """

    _immutable_fields_ = ["col_idx", "col_type_code"]

    def __init__(self, col_idx, field_type):
        assert field_type.code != types.TYPE_STRING.code
        assert field_type.code != types.TYPE_U128.code
        self.col_idx = col_idx
        self.col_type_code = field_type.code
        self._acc = r_int64(0)

    def reset(self):
        self._acc = r_int64(0)

    def step(self, row_accessor, weight):
        if row_accessor.is_null(self.col_idx):
            return
        code = self.col_type_code
        if code == types.TYPE_F64.code or code == types.TYPE_F32.code:
            val_f = row_accessor.get_float(self.col_idx)
            w_f = float(intmask(weight))
            cur_f = longlong2float(self._acc)
            self._acc = float2longlong(cur_f + val_f * w_f)
        elif (
            code == types.TYPE_I8.code
            or code == types.TYPE_I16.code
            or code == types.TYPE_I32.code
            or code == types.TYPE_I64.code
        ):
            val = row_accessor.get_int_signed(self.col_idx)
            weighted = r_int64(intmask(val * weight))
            self._acc = r_int64(intmask(self._acc + weighted))
        else:
            # Unsigned integer types: U8, U16, U32, U64.
            # Reinterpret unsigned bits as signed for I64-widened accumulation.
            # Large U64 values (> MAX_I64) will appear negative in the I64
            # output, consistent with the declared output type.
            val_u = row_accessor.get_int(self.col_idx)
            val = rffi.cast(rffi.LONGLONG, val_u)
            weighted = r_int64(intmask(val * weight))
            self._acc = r_int64(intmask(self._acc + weighted))

    def merge_accumulated(self, value_bits, weight):
        code = self.col_type_code
        if code == types.TYPE_F64.code or code == types.TYPE_F32.code:
            prev_f = longlong2float(rffi.cast(rffi.LONGLONG, value_bits))
            w_f = float(intmask(weight))
            cur_f = longlong2float(self._acc)
            self._acc = float2longlong(cur_f + prev_f * w_f)
        else:
            prev = rffi.cast(rffi.LONGLONG, value_bits)
            weighted = r_int64(intmask(prev * weight))
            self._acc = r_int64(intmask(self._acc + weighted))

    def get_value_bits(self):
        return r_uint64(intmask(self._acc))

    def output_is_float(self):
        return (
            self.col_type_code == types.TYPE_F64.code
            or self.col_type_code == types.TYPE_F32.code
        )

    def is_linear(self):
        return True

    def output_column_type(self):
        if (
            self.col_type_code == types.TYPE_F64.code
            or self.col_type_code == types.TYPE_F32.code
        ):
            return types.TYPE_F64
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return self._acc == r_int64(0)


# ---------------------------------------------------------------------------
# MIN / MAX
# ---------------------------------------------------------------------------


class MinAggregateFunction(AggregateFunction):
    """
    Computes the minimum value of a column over the group.

    Supported column types: all integer types (I8..U64) and float types
    (F32, F64).  TYPE_STRING and TYPE_U128 are rejected at construction
    time with an assertion.
    """

    _immutable_fields_ = ["col_idx", "col_type_code"]

    def __init__(self, col_idx, field_type):
        assert field_type.code != types.TYPE_STRING.code
        assert field_type.code != types.TYPE_U128.code
        self.col_idx = col_idx
        self.col_type_code = field_type.code
        self._acc = r_int64(0)
        self._has_value = False

    def reset(self):
        self._acc = r_int64(0)
        self._has_value = False

    def step(self, row_accessor, weight):
        if row_accessor.is_null(self.col_idx):
            return
        code = self.col_type_code
        if code == types.TYPE_F64.code or code == types.TYPE_F32.code:
            v = row_accessor.get_float(self.col_idx)
            if not self._has_value or v < longlong2float(self._acc):
                self._acc = float2longlong(v)
                self._has_value = True
        elif (
            code == types.TYPE_I8.code
            or code == types.TYPE_I16.code
            or code == types.TYPE_I32.code
            or code == types.TYPE_I64.code
        ):
            v = row_accessor.get_int_signed(self.col_idx)
            if not self._has_value or v < self._acc:
                self._acc = v
                self._has_value = True
        else:
            # Unsigned: compare as r_uint64, store bits reinterpreted as signed.
            v_u = row_accessor.get_int(self.col_idx)
            cur_u = r_uint64(intmask(self._acc))
            if not self._has_value or v_u < cur_u:
                self._acc = rffi.cast(rffi.LONGLONG, v_u)
                self._has_value = True

    def merge_accumulated(self, value_bits, weight):
        pass

    def get_value_bits(self):
        return r_uint64(intmask(self._acc))

    def output_is_float(self):
        return (
            self.col_type_code == types.TYPE_F64.code
            or self.col_type_code == types.TYPE_F32.code
        )

    def is_linear(self):
        return False

    def output_column_type(self):
        if (
            self.col_type_code == types.TYPE_F64.code
            or self.col_type_code == types.TYPE_F32.code
        ):
            return types.TYPE_F64
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return not self._has_value


class MaxAggregateFunction(AggregateFunction):
    """
    Computes the maximum value of a column over the group.

    Supported column types: all integer types (I8..U64) and float types
    (F32, F64).  TYPE_STRING and TYPE_U128 are rejected at construction
    time with an assertion.
    """

    _immutable_fields_ = ["col_idx", "col_type_code"]

    def __init__(self, col_idx, field_type):
        assert field_type.code != types.TYPE_STRING.code
        assert field_type.code != types.TYPE_U128.code
        self.col_idx = col_idx
        self.col_type_code = field_type.code
        self._acc = r_int64(0)
        self._has_value = False

    def reset(self):
        self._acc = r_int64(0)
        self._has_value = False

    def step(self, row_accessor, weight):
        if row_accessor.is_null(self.col_idx):
            return
        code = self.col_type_code
        if code == types.TYPE_F64.code or code == types.TYPE_F32.code:
            v = row_accessor.get_float(self.col_idx)
            if not self._has_value or v > longlong2float(self._acc):
                self._acc = float2longlong(v)
                self._has_value = True
        elif (
            code == types.TYPE_I8.code
            or code == types.TYPE_I16.code
            or code == types.TYPE_I32.code
            or code == types.TYPE_I64.code
        ):
            v = row_accessor.get_int_signed(self.col_idx)
            if not self._has_value or v > self._acc:
                self._acc = v
                self._has_value = True
        else:
            v_u = row_accessor.get_int(self.col_idx)
            cur_u = r_uint64(intmask(self._acc))
            if not self._has_value or v_u > cur_u:
                self._acc = rffi.cast(rffi.LONGLONG, v_u)
                self._has_value = True

    def merge_accumulated(self, value_bits, weight):
        pass

    def get_value_bits(self):
        return r_uint64(intmask(self._acc))

    def output_is_float(self):
        return (
            self.col_type_code == types.TYPE_F64.code
            or self.col_type_code == types.TYPE_F32.code
        )

    def is_linear(self):
        return False

    def output_column_type(self):
        if (
            self.col_type_code == types.TYPE_F64.code
            or self.col_type_code == types.TYPE_F32.code
        ):
            return types.TYPE_F64
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return not self._has_value
