# gnitz/dbsp/functions.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, intmask, r_uint64
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rtyper.lltypesystem import rffi
from gnitz.core import types, strings

# --- Function Opcodes ---
OP_EQ = 1
OP_GT = 2
OP_LT = 3

# --- Aggregate Opcodes ---
AGG_COUNT = 1
AGG_SUM   = 2
AGG_MIN   = 3
AGG_MAX   = 4

class ScalarFunction(object):
    """
    Base class for logic operators. 
    Standardized to prevent None-poisoning in the annotator.
    """
    _immutable_fields_ = []

    def evaluate_predicate(self, row_accessor):
        return True

    def evaluate_map(self, row_accessor, output_row):
        pass


class NullPredicate(ScalarFunction):
    """The Null Object for ScalarFunction."""
    def evaluate_predicate(self, row_accessor):
        return True


class UniversalPredicate(ScalarFunction):
    """
    Unified logic for filtering. 
    Handles EQ, GT, LT for all numeric types.
    """
    _immutable_fields_ = ['col_idx', 'op', 'val_bits', 'is_float']

    def __init__(self, col_idx, op, val_bits, is_float=False):
        self.col_idx = col_idx
        self.op = op
        self.val_bits = val_bits # Constant stored as raw u64 bits
        self.is_float = is_float

    def evaluate_predicate(self, row_accessor):
        # Promoting the op/type allows the JIT to compile a specialized 
        # branch for this specific circuit node.
        op = jit.promote(self.op)
        is_float = jit.promote(self.is_float)
        
        if is_float:
            val = row_accessor.get_float(self.col_idx)
            const = longlong2float(rffi.cast(rffi.LONGLONG, self.val_bits))
            if op == OP_EQ: return val == const
            if op == OP_GT: return val > const
            if op == OP_LT: return val < const
        else:
            val = row_accessor.get_int_signed(self.col_idx)
            const = rffi.cast(rffi.LONGLONG, self.val_bits)
            if op == OP_EQ: return val == const
            if op == OP_GT: return val > const
            if op == OP_LT: return val < const
        return True


class UniversalProjection(ScalarFunction):
    """
    Unified logic for SELECT/MAP.
    Replaces IdentityMapper and ProjectionMapper with a plan-driven loop.
    """
    _immutable_fields_ = ['src_indices[*]', 'src_types[*]']

    def __init__(self, indices, types_list):
        # [*] marks fixed-size arrays for JIT unrolling
        self.src_indices = indices
        self.src_types = types_list

    @jit.unroll_safe
    def evaluate_map(self, row_accessor, output_row):
        for i in range(len(self.src_indices)):
            src_idx = self.src_indices[i]
            t = self.src_types[i]

            if row_accessor.is_null(src_idx):
                output_row.append_null(i)
                continue

            # Standard type-dispatch. JIT unrolls this into tight moves.
            if t == types.TYPE_STRING.code:
                res = row_accessor.get_str_struct(src_idx)
                s = strings.resolve_string(res[2], res[3], res[4])
                output_row.append_string(s)
            elif t == types.TYPE_F64.code or t == types.TYPE_F32.code:
                output_row.append_float(row_accessor.get_float(src_idx))
            elif t == types.TYPE_U128.code:
                val = row_accessor.get_u128(src_idx)
                output_row.append_u128(r_uint64(val), r_uint64(val >> 64))
            else:
                output_row.append_int(row_accessor.get_int_signed(src_idx))


# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------

class AggregateFunction(object):
    """Base class for incremental aggregators."""
    _immutable_fields_ = []

    def reset(self): pass
    def step(self, row_accessor, weight): pass
    def merge_accumulated(self, value_bits, weight): pass
    def get_value_bits(self): return r_uint64(0)
    def output_is_float(self): return False
    def is_linear(self): return True
    def output_column_type(self): return types.TYPE_I64
    def is_accumulator_zero(self): return True


class NullAggregate(AggregateFunction):
    """The Null Object for AggregateFunction."""
    pass


class UniversalAccumulator(AggregateFunction):
    """
    One class that handles Count, Sum, Min, and Max.
    Internal state is bit-packed into self._acc to allow float/int reuse.
    """
    _immutable_fields_ = ['col_idx', 'agg_op', 'col_type_code']

    def __init__(self, col_idx, agg_op, col_type):
        self.col_idx = col_idx
        self.agg_op = agg_op
        self.col_type_code = col_type.code
        self._acc = r_int64(0)
        self._has_value = False

    def reset(self):
        self._acc = r_int64(0)
        self._has_value = False

    def step(self, row_accessor, weight):
        op = jit.promote(self.agg_op)
        if op != AGG_COUNT and row_accessor.is_null(self.col_idx):
            return

        self._has_value = True
        code = self.col_type_code
        is_f = (code == types.TYPE_F64.code or code == types.TYPE_F32.code)

        if op == AGG_COUNT:
            self._acc = r_int64(intmask(self._acc + weight))
        
        elif op == AGG_SUM:
            if is_f:
                val_f = row_accessor.get_float(self.col_idx)
                w_f = float(intmask(weight))
                cur_f = longlong2float(self._acc)
                self._acc = float2longlong(cur_f + (val_f * w_f))
            else:
                val = row_accessor.get_int_signed(self.col_idx)
                self._acc = r_int64(intmask(self._acc + (val * weight)))

        elif op == AGG_MIN:
            if is_f:
                v = row_accessor.get_float(self.col_idx)
                if not self._has_value or v < longlong2float(self._acc):
                    self._acc = float2longlong(v)
            else:
                v = row_accessor.get_int_signed(self.col_idx)
                if not self._has_value or v < self._acc:
                    self._acc = v

        elif op == AGG_MAX:
            if is_f:
                v = row_accessor.get_float(self.col_idx)
                if not self._has_value or v > longlong2float(self._acc):
                    self._acc = float2longlong(v)
            else:
                v = row_accessor.get_int_signed(self.col_idx)
                if not self._has_value or v > self._acc:
                    self._acc = v

    def merge_accumulated(self, value_bits, weight):
        """Used by the linear shortcut in op_reduce."""
        op = jit.promote(self.agg_op)
        if op == AGG_COUNT:
            prev = rffi.cast(rffi.LONGLONG, value_bits)
            self._acc = r_int64(intmask(self._acc + (prev * weight)))
            self._has_value = True
        elif op == AGG_SUM:
            code = self.col_type_code
            if code == types.TYPE_F64.code or code == types.TYPE_F32.code:
                prev_f = longlong2float(rffi.cast(rffi.LONGLONG, value_bits))
                w_f = float(intmask(weight))
                cur_f = longlong2float(self._acc)
                self._acc = float2longlong(cur_f + (prev_f * w_f))
            else:
                prev = rffi.cast(rffi.LONGLONG, value_bits)
                self._acc = r_int64(intmask(self._acc + (prev * weight)))
            self._has_value = True

    def get_value_bits(self):
        return r_uint64(intmask(self._acc))

    def output_is_float(self):
        code = self.col_type_code
        return (code == types.TYPE_F64.code or code == types.TYPE_F32.code) and self.agg_op != AGG_COUNT

    def is_linear(self):
        op = jit.promote(self.agg_op)
        return op == AGG_COUNT or op == AGG_SUM

    def output_column_type(self):
        if self.agg_op == AGG_COUNT: return types.TYPE_I64
        if self.output_is_float(): return types.TYPE_F64
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return not self._has_value


NULL_PREDICATE = NullPredicate()
NULL_AGGREGATE = NullAggregate()
