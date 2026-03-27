# gnitz/dbsp/functions.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, intmask, r_uint64
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, strings

# --- Function Opcodes ---
OP_EQ = 1
OP_GT = 2
OP_LT = 3

# --- Aggregate Opcodes ---
AGG_COUNT          = 1
AGG_SUM            = 2
AGG_MIN            = 3
AGG_MAX            = 4
AGG_COUNT_NON_NULL = 5

class ScalarFunction(object):
    """
    Base class for logic operators. 
    Standardized to prevent None-poisoning in the annotator.
    """
    _immutable_fields_ = []

    def evaluate_predicate(self, row_accessor):
        return True

    def evaluate_predicate_direct(self, in_batch, row_idx):
        accessor = in_batch.get_accessor(row_idx)
        return self.evaluate_predicate(accessor)

    def evaluate_map(self, row_accessor, output_row):
        pass

    def evaluate_map_batch(self, in_batch, out_batch, out_schema):
        """Batch-level map. Returns True if handled, False to fall back to per-row."""
        return False


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

    def evaluate_predicate_direct(self, in_batch, row_idx):
        op = jit.promote(self.op)
        is_float = jit.promote(self.is_float)
        if is_float:
            val = in_batch._read_col_float(row_idx, self.col_idx)
            const = longlong2float(rffi.cast(rffi.LONGLONG, self.val_bits))
            if op == OP_EQ: return val == const
            if op == OP_GT: return val > const
            if op == OP_LT: return val < const
        else:
            val = rffi.cast(rffi.LONGLONG, in_batch._read_col_int(row_idx, self.col_idx))
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
        # copy: [*] requires never-resized lists for JIT unrolling
        self.src_indices = indices[:]
        self.src_types = types_list[:]

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
                output_row.append_u128(row_accessor.get_u128_lo(src_idx), row_accessor.get_u128_hi(src_idx))
            else:
                output_row.append_int(row_accessor.get_int_signed(src_idx))

    @jit.unroll_safe
    def evaluate_map_batch(self, in_batch, out_batch, out_schema):
        from gnitz.storage import engine_ffi, buffer as buf_mod

        n = in_batch.length()
        if n == 0:
            return True

        in_schema = in_batch._schema

        # Allocate system columns (pk_lo, pk_hi, weight, null) in output
        engine_ffi._batch_alloc_system(out_batch._handle, rffi.cast(rffi.UINT, n))
        out_count = out_batch.length()
        sys_off = (out_count - n) * 8

        # Copy pk_lo, pk_hi, weight from input
        for ri in range(3):  # 0=pk_lo, 1=pk_hi, 2=weight
            src_ptr = engine_ffi._batch_region_ptr(
                in_batch._handle, rffi.cast(rffi.UINT, ri))
            dst_ptr = engine_ffi._batch_region_ptr(
                out_batch._handle, rffi.cast(rffi.UINT, ri))
            buf_mod.c_memmove(
                rffi.cast(rffi.VOIDP, rffi.ptradd(dst_ptr, sys_off)),
                rffi.cast(rffi.VOIDP, src_ptr),
                rffi.cast(rffi.SIZE_T, n * 8),
            )

        # Payload columns — per-column memcpy or string relocation
        for i in range(len(self.src_indices)):
            src_ci = self.src_indices[i]
            out_pi = i  # output payload index
            tc = self.src_types[i]

            if src_ci == in_schema.pk_index:
                stride = out_schema.columns[
                    out_pi if out_pi < out_schema.pk_index else out_pi + 1
                ].field_type.size
                dest_base = engine_ffi._batch_col_extend(
                    out_batch._handle,
                    rffi.cast(rffi.UINT, out_pi),
                    rffi.cast(rffi.UINT, n * stride),
                )
                dest_arr = rffi.cast(rffi.ULONGLONGP, dest_base)
                for row in range(n):
                    dest_arr[row] = rffi.cast(
                        rffi.ULONGLONG, in_batch._read_pk_lo(row))
            elif tc == types.TYPE_STRING.code:
                in_pi = src_ci if src_ci < in_schema.pk_index else src_ci - 1
                stride = in_schema.columns[src_ci].field_type.size
                n_bytes = n * stride
                dest_block = engine_ffi._batch_col_extend(
                    out_batch._handle,
                    rffi.cast(rffi.UINT, out_pi),
                    rffi.cast(rffi.UINT, n_bytes),
                )
                src_block = engine_ffi._batch_col_ptr(
                    in_batch._handle,
                    rffi.cast(rffi.UINT, 0),
                    rffi.cast(rffi.UINT, in_pi),
                    rffi.cast(rffi.UINT, stride),
                )
                buf_mod.c_memmove(
                    rffi.cast(rffi.VOIDP, dest_block),
                    rffi.cast(rffi.VOIDP, src_block),
                    rffi.cast(rffi.SIZE_T, n_bytes),
                )
                src_blob_base = engine_ffi._batch_blob_ptr(in_batch._handle)
                for row in range(n):
                    src_ptr = rffi.ptradd(src_block, row * stride)
                    length = rffi.cast(
                        lltype.Signed, rffi.cast(rffi.UINTP, src_ptr)[0])
                    if length > strings.SHORT_STRING_THRESHOLD:
                        dest_ptr = rffi.ptradd(dest_block, row * stride)
                        old_offset = rffi.cast(
                            rffi.ULONGLONGP, rffi.ptradd(src_ptr, 8))[0]
                        src_data_ptr = rffi.ptradd(
                            src_blob_base,
                            rffi.cast(lltype.Signed, old_offset))
                        new_blob_off = engine_ffi._batch_blob_extend(
                            out_batch._handle,
                            rffi.cast(rffi.UINT, length),
                        )
                        out_blob_ptr = engine_ffi._batch_blob_ptr(out_batch._handle)
                        buf_mod.c_memmove(
                            rffi.cast(rffi.VOIDP, rffi.ptradd(
                                out_blob_ptr,
                                rffi.cast(lltype.Signed, new_blob_off))),
                            rffi.cast(rffi.VOIDP, src_data_ptr),
                            rffi.cast(rffi.SIZE_T, length),
                        )
                        rffi.cast(
                            rffi.ULONGLONGP,
                            rffi.ptradd(dest_ptr, 8)
                        )[0] = rffi.cast(rffi.ULONGLONG, new_blob_off)
            else:
                in_pi = src_ci if src_ci < in_schema.pk_index else src_ci - 1
                stride = in_schema.columns[src_ci].field_type.size
                dest_block = engine_ffi._batch_col_extend(
                    out_batch._handle,
                    rffi.cast(rffi.UINT, out_pi),
                    rffi.cast(rffi.UINT, n * stride),
                )
                src_block = engine_ffi._batch_col_ptr(
                    in_batch._handle,
                    rffi.cast(rffi.UINT, 0),
                    rffi.cast(rffi.UINT, in_pi),
                    rffi.cast(rffi.UINT, stride),
                )
                buf_mod.c_memmove(
                    rffi.cast(rffi.VOIDP, dest_block),
                    rffi.cast(rffi.VOIDP, src_block),
                    rffi.cast(rffi.SIZE_T, n * stride),
                )

        # Null bitmap — per-row bit shuffling
        null_dst = rffi.ptradd(
            engine_ffi._batch_region_ptr(
                out_batch._handle, rffi.cast(rffi.UINT, 3)),
            sys_off,
        )
        null_arr = rffi.cast(rffi.ULONGLONGP, null_dst)
        for row in range(n):
            in_null = in_batch._read_null_word(row)
            out_null = r_uint64(0)
            for i in range(len(self.src_indices)):
                src_ci = self.src_indices[i]
                if src_ci == in_schema.pk_index:
                    continue  # PK is never null
                in_pi = src_ci if src_ci < in_schema.pk_index else src_ci - 1
                out_null |= ((in_null >> in_pi) & r_uint64(1)) << i
            null_arr[row] = rffi.cast(rffi.ULONGLONG, out_null)

        out_batch._invalidate_cache()
        return True


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
    def seed_from_raw_bits(self, bits): pass
    def combine(self, other_bits): pass


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

        first = not self._has_value
        self._has_value = True
        code = self.col_type_code
        is_f = (code == types.TYPE_F64.code or code == types.TYPE_F32.code)

        if op == AGG_COUNT:
            self._acc = r_int64(intmask(self._acc + weight))

        elif op == AGG_COUNT_NON_NULL:
            if not row_accessor.is_null(self.col_idx):
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
                if first or v < longlong2float(self._acc):
                    self._acc = float2longlong(v)
            else:
                v = row_accessor.get_int_signed(self.col_idx)
                if first or v < self._acc:
                    self._acc = v

        elif op == AGG_MAX:
            if is_f:
                v = row_accessor.get_float(self.col_idx)
                if first or v > longlong2float(self._acc):
                    self._acc = float2longlong(v)
            else:
                v = row_accessor.get_int_signed(self.col_idx)
                if first or v > self._acc:
                    self._acc = v

    def merge_accumulated(self, value_bits, weight):
        """Used by the linear shortcut in op_reduce."""
        op = jit.promote(self.agg_op)
        if op == AGG_COUNT or op == AGG_COUNT_NON_NULL:
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

    def combine(self, other_bits):
        """Merge a partial aggregate (other_bits = get_value_bits() from a non-zero
        accumulator on another shard) into this accumulator.
        Caller must verify is_accumulator_zero() == False on the source before calling.
        """
        op = jit.promote(self.agg_op)
        if op == AGG_COUNT or op == AGG_COUNT_NON_NULL:
            prev = rffi.cast(rffi.LONGLONG, other_bits)
            self._acc = r_int64(intmask(self._acc + prev))
            self._has_value = True
        elif op == AGG_SUM:
            code = self.col_type_code
            if code == types.TYPE_F64.code or code == types.TYPE_F32.code:
                prev_f = longlong2float(rffi.cast(rffi.LONGLONG, other_bits))
                self._acc = float2longlong(longlong2float(self._acc) + prev_f)
            else:
                prev = rffi.cast(rffi.LONGLONG, other_bits)
                self._acc = r_int64(intmask(self._acc + prev))
            self._has_value = True
        elif op == AGG_MIN:
            first = not self._has_value
            self._has_value = True
            code = self.col_type_code
            if code == types.TYPE_F64.code or code == types.TYPE_F32.code:
                other_f = longlong2float(rffi.cast(rffi.LONGLONG, other_bits))
                if first or other_f < longlong2float(self._acc):
                    self._acc = float2longlong(other_f)
            else:
                other_v = rffi.cast(rffi.LONGLONG, other_bits)
                if first or other_v < self._acc:
                    self._acc = other_v
        elif op == AGG_MAX:
            first = not self._has_value
            self._has_value = True
            code = self.col_type_code
            if code == types.TYPE_F64.code or code == types.TYPE_F32.code:
                other_f = longlong2float(rffi.cast(rffi.LONGLONG, other_bits))
                if first or other_f > longlong2float(self._acc):
                    self._acc = float2longlong(other_f)
            else:
                other_v = rffi.cast(rffi.LONGLONG, other_bits)
                if first or other_v > self._acc:
                    self._acc = other_v

    def seed_from_raw_bits(self, bits):
        self._acc = r_int64(intmask(bits))
        self._has_value = True

    def get_value_bits(self):
        return r_uint64(intmask(self._acc))

    def output_is_float(self):
        code = self.col_type_code
        return (code == types.TYPE_F64.code or code == types.TYPE_F32.code) and self.agg_op != AGG_COUNT and self.agg_op != AGG_COUNT_NON_NULL

    def is_linear(self):
        op = jit.promote(self.agg_op)
        return op == AGG_COUNT or op == AGG_SUM or op == AGG_COUNT_NON_NULL

    def output_column_type(self):
        if self.agg_op == AGG_COUNT or self.agg_op == AGG_COUNT_NON_NULL:
            return types.TYPE_I64
        if self.output_is_float(): return types.TYPE_F64
        return types.TYPE_I64

    def is_accumulator_zero(self):
        return not self._has_value


NULL_PREDICATE = NullPredicate()
NULL_AGGREGATE = NullAggregate()
