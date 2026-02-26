# gnitz/dbsp/functions.py

from rpython.rlib.rarithmetic import r_int64, intmask, r_uint64
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rtyper.lltypesystem import rffi
from gnitz.core import types, strings


class ScalarFunction(object):
    """
    Base class for User Defined Functions (UDFs) and Standard Operators.
    """
    _immutable_fields_ = []

    def evaluate_predicate(self, row_accessor):
        raise NotImplementedError

    def evaluate_map(self, row_accessor, output_row):
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Standard Predicates (Filters)
# ---------------------------------------------------------------------------

class TruePredicate(ScalarFunction):
    def evaluate_predicate(self, row_accessor):
        return True


class FloatGt(ScalarFunction):
    _immutable_fields_ = ['col_idx', 'val']

    def __init__(self, col_idx, val):
        self.col_idx = col_idx
        self.val = val

    def evaluate_predicate(self, row_accessor):
        return row_accessor.get_float(self.col_idx) > self.val


class IntEq(ScalarFunction):
    _immutable_fields_ = ['col_idx', 'val']

    def __init__(self, col_idx, val):
        self.col_idx = col_idx
        self.val = val

    def evaluate_predicate(self, row_accessor):
        return row_accessor.get_int_signed(self.col_idx) == self.val


# ---------------------------------------------------------------------------
# Standard Mappers (Projections)
# ---------------------------------------------------------------------------

class IdentityMapper(ScalarFunction):
    """Selects all non-PK columns from input to output (SELECT *)."""
    
    # The [*] hint requires the assigned list to be fixed-size (non-resizable).
    _immutable_fields_ = ['src_indices[*]', 'src_types[*]']

    def __init__(self, schema):
        num_cols = len(schema.columns)
        payload_count = 0
        for i in range(num_cols):
            if i != schema.pk_index:
                payload_count += 1
        
        # Initialization via multiplication creates a fixed-size (non-resizable)
        # list that satisfies the [*] immutable field hint.
        indices = [0] * payload_count
        t_list = [0] * payload_count
        
        curr = 0
        for i in range(num_cols):
            if i != schema.pk_index:
                indices[curr] = i
                t_list[curr] = schema.columns[i].field_type.code
                curr += 1
        
        self.src_indices = indices
        self.src_types = t_list

    def evaluate_map(self, row_accessor, output_row):
        for i in range(len(self.src_indices)):
            phys_idx = self.src_indices[i]
            t = self.src_types[i]

            if row_accessor.is_null(phys_idx):
                output_row.append_null(i)
                continue

            if t == types.TYPE_STRING.code:
                res = row_accessor.get_str_struct(phys_idx)
                # resolve_string handles the union between Python strings 
                # and raw German String pointers/heaps.
                s = strings.resolve_string(res[2], res[3], res[4])
                output_row.append_string(s)
            elif t == types.TYPE_F64.code or t == types.TYPE_F32.code:
                output_row.append_float(row_accessor.get_float(phys_idx))
            elif t == types.TYPE_U128.code:
                val = row_accessor.get_u128(phys_idx)
                output_row.append_u128(r_uint64(val), r_uint64(val >> 64))
            else:
                output_row.append_int(row_accessor.get_int_signed(phys_idx))


class ProjectionMapper(ScalarFunction):
    """Selects specific columns (SELECT col_A, col_B)."""
    
    _immutable_fields_ = ['src_indices[*]', 'src_types[*]']

    def __init__(self, src_indices, src_types):
        assert len(src_indices) == len(src_types)
        n = len(src_indices)
        
        # Copy input lists (which might be resizable) into fixed-size arrays.
        idx_fixed = [0] * n
        typ_fixed = [0] * n
        for i in range(n):
            idx_fixed[i] = src_indices[i]
            typ_fixed[i] = src_types[i]
            
        self.src_indices = idx_fixed
        self.src_types = typ_fixed

    def evaluate_map(self, row_accessor, output_row):
        for i in range(len(self.src_indices)):
            src_idx = self.src_indices[i]
            t = self.src_types[i]

            if row_accessor.is_null(src_idx):
                output_row.append_null(i)
                continue

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
    _immutable_fields_ = []

    def reset(self):
        raise NotImplementedError

    def step(self, row_accessor, weight):
        raise NotImplementedError

    def merge_accumulated(self, value_bits, weight):
        raise NotImplementedError

    def get_value_bits(self):
        raise NotImplementedError

    def output_is_float(self):
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


class SumAggregateFunction(AggregateFunction):
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
        else:
            val = row_accessor.get_int_signed(self.col_idx)
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
        return (self.col_type_code == types.TYPE_F64.code or 
                self.col_type_code == types.TYPE_F32.code)

    def is_linear(self):
        return True

    def output_column_type(self):
        if (self.col_type_code == types.TYPE_F64.code or 
            self.col_type_code == types.TYPE_F32.code):
            return types.TYPE_F64
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return self._acc == r_int64(0)


class MinAggregateFunction(AggregateFunction):
    _immutable_fields_ = ["col_idx", "col_type_code"]

    def __init__(self, col_idx, field_type):
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
        else:
            v = row_accessor.get_int_signed(self.col_idx)
            if not self._has_value or v < self._acc:
                self._acc = v
                self._has_value = True

    def merge_accumulated(self, value_bits, weight):
        pass

    def get_value_bits(self):
        return r_uint64(intmask(self._acc))

    def output_is_float(self):
        return (self.col_type_code == types.TYPE_F64.code or 
                self.col_type_code == types.TYPE_F32.code)

    def is_linear(self):
        return False

    def output_column_type(self):
        if (self.col_type_code == types.TYPE_F64.code or 
            self.col_type_code == types.TYPE_F32.code):
            return types.TYPE_F64
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return not self._has_value


class MaxAggregateFunction(AggregateFunction):
    _immutable_fields_ = ["col_idx", "col_type_code"]

    def __init__(self, col_idx, field_type):
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
        else:
            v = row_accessor.get_int_signed(self.col_idx)
            if not self._has_value or v > self._acc:
                self._acc = v
                self._has_value = True

    def merge_accumulated(self, value_bits, weight):
        pass

    def get_value_bits(self):
        return r_uint64(intmask(self._acc))

    def output_is_float(self):
        return (self.col_type_code == types.TYPE_F64.code or 
                self.col_type_code == types.TYPE_F32.code)

    def is_linear(self):
        return False

    def output_column_type(self):
        if (self.col_type_code == types.TYPE_F64.code or 
            self.col_type_code == types.TYPE_F32.code):
            return types.TYPE_F64
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return not self._has_value
