# gnitz/dbsp/ops/reduce.py

from rpython.rlib import jit
from rpython.rlib.longlong2float import longlong2float
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.core import types, strings, xxh, errors
from gnitz.core.comparator import RowAccessor
from gnitz.core.batch import (
    ArenaZSetBatch, ConsolidatedScope, BatchWriter, ColumnarBatchAccessor,
)
from gnitz.dbsp.functions import NULL_AGGREGATE

"""
Non-linear Reduce Operator for the DBSP algebra.

Implements incremental GROUP BY + aggregation.
Formula: δ_out = Agg(history + δ_in) - Agg(history).
"""


class ReduceAccessor(RowAccessor):
    """
    Virtual RowAccessor that assembles one output record for the REDUCE operator.
    Mapps output column indices to either the group exemplar columns or 
    the result of the aggregate function.
    """

    _immutable_fields_ = [
        "input_schema",
        "output_schema",
        "mapping_to_input[*]",
    ]

    def __init__(self, input_schema, output_schema, group_indices):
        self.input_schema = input_schema
        self.output_schema = output_schema

        num_out = len(output_schema.columns)
        mapping = [0] * num_out

        pk_code = output_schema.columns[0].field_type.code
        # A 'natural' PK is used if we group by a single U64/U128 column.
        use_natural_pk = (
            len(group_indices) == 1
            and output_schema.pk_index == 0
            and (
                pk_code == types.TYPE_U64.code
                or pk_code == types.TYPE_U128.code
            )
        )

        if use_natural_pk:
            mapping[0] = group_indices[0]
            mapping[1] = -1  # -1 represents the aggregate column
        else:
            mapping[0] = -2  # -2 represents the synthetic PK (group hash)
            for i in range(len(group_indices)):
                mapping[i + 1] = group_indices[i]
            mapping[num_out - 1] = -1

        self.mapping_to_input = mapping

        # Mutable state for context switching during iteration
        self.exemplar = None 
        self.agg_func = NULL_AGGREGATE
        self.old_val_bits = r_uint64(0)
        self.use_old_val = False

    def set_context(self, exemplar, agg_func, old_val_bits, use_old_val):
        """Prepares the accessor to represent a specific group output."""
        self.exemplar = exemplar
        if agg_func is not None:
            self.agg_func = agg_func
        else:
            self.agg_func = NULL_AGGREGATE
        self.old_val_bits = old_val_bits
        self.use_old_val = use_old_val

    @jit.unroll_safe
    def is_null(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1:
            return self.agg_func.is_accumulator_zero()
        if src < 0:
            return False
        if self.exemplar is not None:
            return self.exemplar.is_null(src)
        return True

    @jit.unroll_safe
    def get_int(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1:
            if self.use_old_val:
                return self.old_val_bits
            return self.agg_func.get_value_bits()
        if src < 0:
            return r_uint64(0)
        if self.exemplar is not None:
            return self.exemplar.get_int(src)
        return r_uint64(0)

    @jit.unroll_safe
    def get_int_signed(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1:
            bits = self.old_val_bits if self.use_old_val else self.agg_func.get_value_bits()
            return rffi.cast(rffi.LONGLONG, bits)
        if src < 0:
            return r_int64(0)
        if self.exemplar is not None:
            return self.exemplar.get_int_signed(src)
        return r_int64(0)

    @jit.unroll_safe
    def get_float(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1:
            bits = self.old_val_bits if self.use_old_val else self.agg_func.get_value_bits()
            return longlong2float(rffi.cast(rffi.LONGLONG, bits))
        if src < 0:
            return 0.0
        if self.exemplar is not None:
            return self.exemplar.get_float(src)
        return 0.0

    @jit.unroll_safe
    def get_u128(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src >= 0 and self.exemplar is not None:
            return self.exemplar.get_u128(src)
        return r_uint128(0)

    @jit.unroll_safe
    def get_str_struct(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src >= 0 and self.exemplar is not None:
            return self.exemplar.get_str_struct(src)
        return (0, 0, lltype.nullptr(rffi.CCHARP.TO), 
                lltype.nullptr(rffi.CCHARP.TO), None)

    @jit.unroll_safe
    def get_col_ptr(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src < 0:
            return strings.NULL_PTR
        return self.exemplar.get_col_ptr(src)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


@jit.unroll_safe
def _compare_by_cols(accessor_a, accessor_b, schema, col_indices):
    for i in range(len(col_indices)):
        col_idx = col_indices[i]
        col_type = schema.columns[col_idx].field_type.code

        if col_type == types.TYPE_STRING.code:
            l_len, l_pref, l_ptr, l_heap, l_str = accessor_a.get_str_struct(col_idx)
            r_len, r_pref, r_ptr, r_heap, r_str = accessor_b.get_str_struct(col_idx)
            res = strings.compare_structures(
                l_len, l_pref, l_ptr, l_heap, l_str,
                r_len, r_pref, r_ptr, r_heap, r_str,
            )
            if res != 0:
                return res
        elif col_type == types.TYPE_F64.code or col_type == types.TYPE_F32.code:
            va = accessor_a.get_float(col_idx)
            vb = accessor_b.get_float(col_idx)
            if va < vb: return -1
            if va > vb: return 1
        elif col_type == types.TYPE_U128.code:
            va = accessor_a.get_u128(col_idx)
            vb = accessor_b.get_u128(col_idx)
            if va < vb: return -1
            if va > vb: return 1
        elif (
            col_type == types.TYPE_I8.code or col_type == types.TYPE_I16.code or
            col_type == types.TYPE_I32.code or col_type == types.TYPE_I64.code
        ):
            va = accessor_a.get_int_signed(col_idx)
            vb = accessor_b.get_int_signed(col_idx)
            if va < vb: return -1
            if va > vb: return 1
        else:
            va = accessor_a.get_int(col_idx)
            vb = accessor_b.get_int(col_idx)
            if va < vb: return -1
            if va > vb: return 1
    return 0


def _argsort_delta(batch, schema, col_indices):
    count = batch.length()
    indices = newlist_hint(count)
    for i in range(count):
        indices.append(i)

    if count <= 1:
        return indices

    scratch = newlist_hint(count)
    for _ in range(count):
        scratch.append(0)

    acc_a = ColumnarBatchAccessor(schema)
    acc_b = ColumnarBatchAccessor(schema)
    _mergesort_by_cols(indices, batch, schema, col_indices, 0, count, scratch, acc_a, acc_b)
    return indices


def _mergesort_by_cols(indices, batch, schema, col_indices, lo, hi, scratch, acc_a, acc_b):
    if hi - lo <= 1:
        return
    mid = (lo + hi) >> 1
    _mergesort_by_cols(indices, batch, schema, col_indices, lo, mid, scratch, acc_a, acc_b)
    _mergesort_by_cols(indices, batch, schema, col_indices, mid, hi, scratch, acc_a, acc_b)
    _merge_by_cols(indices, batch, schema, col_indices, lo, mid, hi, scratch, acc_a, acc_b)


def _merge_by_cols(indices, batch, schema, col_indices, lo, mid, hi, scratch, acc_a, acc_b):
    for k in range(lo, mid):
        scratch[k] = indices[k]

    i = lo
    j = mid
    k = lo

    while i < mid and j < hi:
        batch.bind_accessor(scratch[i], acc_a)
        batch.bind_accessor(indices[j], acc_b)
        if _compare_by_cols(acc_a, acc_b, schema, col_indices) <= 0:
            indices[k] = scratch[i]
            i += 1
        else:
            indices[k] = indices[j]
            j += 1
        k += 1

    while i < mid:
        indices[k] = scratch[i]
        i += 1
        k += 1


def _mix64(v):
    """Murmur3 64-bit finalizer. Pure RPython, JIT-inlinable."""
    v = r_uint64(v)
    v ^= v >> 33
    v = r_uint64(v * r_uint64(0xFF51AFD7ED558CCD))
    v ^= v >> 33
    v = r_uint64(v * r_uint64(0xC4CEB9FE1A85EC53))
    v ^= v >> 33
    return v


@jit.unroll_safe
def _extract_group_key(accessor, schema, col_indices):
    """Computes a 128-bit key identifying the group."""
    if len(col_indices) == 1:
        c_idx = col_indices[0]
        t = schema.columns[c_idx].field_type.code
        if t == types.TYPE_U64.code:
            return r_uint128(accessor.get_int(c_idx))
        if t == types.TYPE_U128.code:
            return accessor.get_u128(c_idx)

    h = r_uint64(0x9E3779B97F4A7C15)  # golden ratio seed
    for i in range(len(col_indices)):
        c_idx = col_indices[i]
        t = schema.columns[c_idx].field_type.code
        if t == types.TYPE_STRING.code:
            length, _, struct_ptr, heap_ptr, py_string = accessor.get_str_struct(c_idx)
            if length == 0:
                col_hash = r_uint64(0)
            elif py_string is not None:
                col_hash = xxh.compute_checksum_bytes(py_string)
            else:
                if length <= strings.SHORT_STRING_THRESHOLD:
                    target_ptr = rffi.ptradd(struct_ptr, 4)
                else:
                    u64_p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(struct_ptr, 8))
                    heap_off = rffi.cast(lltype.Signed, u64_p[0])
                    target_ptr = rffi.ptradd(heap_ptr, heap_off)
                col_hash = xxh.compute_checksum(target_ptr, length)
        else:
            col_hash = _mix64(accessor.get_int(c_idx))
        h = _mix64(h ^ col_hash ^ r_uint64(i))

    h_hi = _mix64(h ^ r_uint64(len(col_indices)))
    return (r_uint128(h_hi) << 64) | r_uint128(h)


# ---------------------------------------------------------------------------
# Reduce operator implementation
# ---------------------------------------------------------------------------


def op_reduce(
    delta_in,
    input_schema,
    trace_in_cursor,
    trace_out_cursor,
    out_writer,
    group_by_cols,
    agg_func,
    output_schema,
):
    """
    Incremental DBSP REDUCE: δ_out = Agg(history + δ_in) - Agg(history).
    """
    if agg_func is None:
        return

    # 1. Obtain a consolidated view of the input delta
    with ConsolidatedScope(delta_in) as b:
        n = b.length()
        if n == 0:
            return

        # 2. Grouping
        group_by_pk = (
            len(group_by_cols) == 1
            and group_by_cols[0] == input_schema.pk_index
        )

        if group_by_pk:
            # Batch is already sorted by PK
            sorted_indices = newlist_hint(n)
            for i in range(n):
                sorted_indices.append(i)
        else:
            # Sort the delta by the requested group columns
            sorted_indices = _argsort_delta(b, input_schema, group_by_cols)

        acc_in = ColumnarBatchAccessor(input_schema)
        acc_exemplar = ColumnarBatchAccessor(input_schema)
        reduce_acc = ReduceAccessor(input_schema, output_schema, group_by_cols)

        idx = 0
        while idx < n:
            # --- Start of Group ---
            group_start_pos = idx
            group_start_idx = sorted_indices[group_start_pos]

            b.bind_accessor(group_start_idx, acc_exemplar)
            if group_by_pk:
                group_key = b.get_pk(group_start_idx)
            else:
                group_key = _extract_group_key(acc_exemplar, input_schema, group_by_cols)

            # 3. Calculate Delta contribution for this group
            agg_func.reset()
            while idx < n:
                curr_idx = sorted_indices[idx]
                b.bind_accessor(curr_idx, acc_in)
                
                if group_by_pk:
                    if b.get_pk(curr_idx) != group_key:
                        break
                else:
                    if _compare_by_cols(acc_in, acc_exemplar, input_schema, group_by_cols) != 0:
                        break
                
                agg_func.step(acc_in, b.get_weight(curr_idx))
                idx += 1

            # 4. Retraction: Agg(history)
            trace_out_cursor.seek(group_key)
            has_old = False
            old_val_bits = r_uint64(0)
            if trace_out_cursor.is_valid() and trace_out_cursor.key() == group_key:
                has_old = True
                agg_col_idx = len(output_schema.columns) - 1
                old_val_bits = trace_out_cursor.get_accessor().get_int(agg_col_idx)

                # Emit retraction of the previous aggregate value
                reduce_acc.set_context(acc_exemplar, agg_func, old_val_bits, True)
                out_writer.append_from_accessor(group_key, r_int64(-1), reduce_acc)

            # 5. New Value Calculation: Agg(history + delta)
            if agg_func.is_linear() and has_old:
                # Optimized path for SUM/COUNT: Agg(H+D) = Agg(H) + Agg(D)
                agg_func.merge_accumulated(old_val_bits, r_int64(1))
            else:
                # Non-linear path (MIN/MAX): consolidate history + delta,
                # then aggregate only over positive-weight records.
                # This is necessary because MIN/MAX cannot algebraically
                # cancel retractions — a retracted value must be excluded,
                # not merely "stepped" with a negative weight.
                replay = ArenaZSetBatch(input_schema, initial_capacity=32)

                if trace_in_cursor is not None:
                    if group_by_pk:
                        # Group key matches table PK — direct seek
                        trace_in_cursor.seek(group_key)
                        while trace_in_cursor.is_valid() and trace_in_cursor.key() == group_key:
                            replay.append_from_accessor(
                                trace_in_cursor.key(), trace_in_cursor.weight(),
                                trace_in_cursor.get_accessor(),
                            )
                            trace_in_cursor.advance()
                    else:
                        # Group key differs from table PK — scan and
                        # filter by group column match.
                        trace_in_cursor.seek(r_uint128(0))
                        while trace_in_cursor.is_valid():
                            trace_acc = trace_in_cursor.get_accessor()
                            if _compare_by_cols(trace_acc, acc_exemplar, input_schema, group_by_cols) == 0:
                                replay.append_from_accessor(
                                    trace_in_cursor.key(), trace_in_cursor.weight(),
                                    trace_acc,
                                )
                            trace_in_cursor.advance()

                for k in range(group_start_pos, idx):
                    d_idx = sorted_indices[k]
                    b.bind_accessor(d_idx, acc_in)
                    replay.append_from_accessor(
                        b.get_pk(d_idx), b.get_weight(d_idx), acc_in,
                    )

                merged = replay.to_consolidated()
                agg_func.reset()
                replay_acc = ColumnarBatchAccessor(input_schema)
                for m in range(merged.length()):
                    w = merged.get_weight(m)
                    if w > r_int64(0):
                        merged.bind_accessor(m, replay_acc)
                        agg_func.step(replay_acc, w)
                if merged is not replay:
                    merged.free()
                replay.free()

            # 6. Emission: +1 Agg(history + delta)
            if not agg_func.is_accumulator_zero():
                reduce_acc.set_context(acc_exemplar, agg_func, r_uint64(0), False)
                out_writer.append_from_accessor(group_key, r_int64(1), reduce_acc)
