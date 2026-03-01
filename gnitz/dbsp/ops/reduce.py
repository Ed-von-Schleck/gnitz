# gnitz/dbsp/ops/reduce.py

from rpython.rlib import jit
from rpython.rlib.longlong2float import longlong2float
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.core import types, strings, xxh
from gnitz.core.comparator import RowAccessor
from gnitz.storage.comparator import RawWALAccessor
from gnitz.dbsp.functions import NULL_AGGREGATE

"""
Non-linear Reduce Operator for the DBSP algebra.

Implements incremental GROUP BY + aggregation.
"""


class ReduceAccessor(RowAccessor):
    """
    Virtual RowAccessor that assembles one output record for the REDUCE operator.
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
            mapping[1] = -1
        else:
            mapping[0] = -2
            for i in range(len(group_indices)):
                mapping[i + 1] = group_indices[i]
            mapping[num_out - 1] = -1

        self.mapping_to_input = mapping

        # FIX: Initialize to Null Object instead of None to satisfy annotator
        self.exemplar = None 
        self.agg_func = NULL_AGGREGATE
        self.old_val_bits = r_uint64(0)
        self.use_old_val = False

    def set_context(self, exemplar, agg_func, old_val_bits, use_old_val):
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
        # Guard exemplar access
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
        # Guard exemplar access
        if self.exemplar is not None:
            return self.exemplar.get_int(src)
        return r_uint64(0)

    @jit.unroll_safe
    def get_int_signed(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1:
            if self.use_old_val:
                return rffi.cast(rffi.LONGLONG, self.old_val_bits)
            return rffi.cast(rffi.LONGLONG, self.agg_func.get_value_bits())
        if src < 0:
            return r_int64(0)
        if self.exemplar is not None:
            return self.exemplar.get_int_signed(src)
        return r_int64(0)

    @jit.unroll_safe
    def get_float(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1:
            if self.use_old_val:
                return longlong2float(rffi.cast(rffi.LONGLONG, self.old_val_bits))
            return longlong2float(rffi.cast(rffi.LONGLONG, self.agg_func.get_value_bits()))
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
        # Return empty string struct
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
    """
    Compares two rows on a specified subset of schema columns.
    """
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
            if va < vb:
                return -1
            if va > vb:
                return 1
        elif col_type == types.TYPE_U128.code:
            va = accessor_a.get_u128(col_idx)
            vb = accessor_b.get_u128(col_idx)
            if va < vb:
                return -1
            if va > vb:
                return 1
        elif (
            col_type == types.TYPE_I8.code
            or col_type == types.TYPE_I16.code
            or col_type == types.TYPE_I32.code
            or col_type == types.TYPE_I64.code
        ):
            va = accessor_a.get_int_signed(col_idx)
            vb = accessor_b.get_int_signed(col_idx)
            if va < vb:
                return -1
            if va > vb:
                return 1
        else:
            va = accessor_a.get_int(col_idx)
            vb = accessor_b.get_int(col_idx)
            if va < vb:
                return -1
            if va > vb:
                return 1

    return 0


def _argsort_delta(batch, schema, col_indices):
    """
    Returns a permutation of [0, count) sorted by the specified group-by columns.

    Optimization: Uses two local RawWALAccessors and bind_raw_accessor to 
    eliminate PayloadRow allocations during the sort.
    """
    count = batch.length()
    indices = newlist_hint(count)
    for i in range(count):
        indices.append(i)

    if count <= 1:
        return indices

    acc_a = RawWALAccessor(schema)
    acc_b = RawWALAccessor(schema)

    for i in range(1, count):
        j = i
        while j > 0:
            batch.bind_raw_accessor(indices[j], acc_a)
            batch.bind_raw_accessor(indices[j - 1], acc_b)
            if _compare_by_cols(acc_a, acc_b, schema, col_indices) < 0:
                tmp = indices[j]
                indices[j] = indices[j - 1]
                indices[j - 1] = tmp
                j -= 1
            else:
                break

    return indices


@jit.unroll_safe
def _extract_group_key(accessor, schema, col_indices):
    """
    Derives the 128-bit group key from the group-by column values.
    """
    if len(col_indices) == 1:
        c_idx = col_indices[0]
        t = schema.columns[c_idx].field_type.code
        if t == types.TYPE_U64.code:
            return r_uint128(accessor.get_int(c_idx))
        if t == types.TYPE_U128.code:
            return accessor.get_u128(c_idx)

    h_lo = r_uint64(0)
    h_hi = r_uint64(0)
    for i in range(len(col_indices)):
        c_idx = col_indices[i]
        t = schema.columns[c_idx].field_type.code
        if t == types.TYPE_STRING.code:
            res = accessor.get_str_struct(c_idx)
            s = strings.resolve_string(res[2], res[3], res[4])
            h_lo ^= xxh.compute_checksum_bytes(s)
        else:
            h_lo ^= accessor.get_int(c_idx)
        h_hi = (h_hi << 1) | (h_lo >> 63)
    return (r_uint128(h_hi) << 64) | r_uint128(h_lo)


# ---------------------------------------------------------------------------
# Reduce operator implementation
# ---------------------------------------------------------------------------


def op_reduce(
    delta_in,
    input_schema,
    trace_in_cursor,
    trace_out_cursor,
    out_batch,
    group_by_cols,
    agg_func,
    output_schema,
):
    """
    Incremental DBSP REDUCE: δ_out = Agg(history + δ_in) - Agg(history).
    
    This function implements the core of incremental aggregation. It calculates
    the change in aggregate values for each modified group.
    """
    if delta_in.length() == 0:
        return

    # 1. Pre-processing: Consolidation is required for algebraic correctness.
    if not delta_in.is_sorted():
        delta_in.sort()
    delta_in.consolidate()

    n = delta_in.length()
    if n == 0:
        return

    # Null-guard for agg_func to satisfy RPython Annotator
    if agg_func is None:
        return

    # 2. Grouping: Sort the incoming delta by the group-by columns.
    # Optimization: If we are grouping by the Primary Key, we can skip the argsort.
    group_by_pk = (
        len(group_by_cols) == 1
        and group_by_cols[0] == input_schema.pk_index
    )

    if group_by_pk:
        sorted_indices = newlist_hint(n)
        for i in range(n):
            sorted_indices.append(i)
    else:
        # _argsort_delta uses RawWALAccessors to sort without allocations
        sorted_indices = _argsort_delta(delta_in, input_schema, group_by_cols)

    # Reusable accessors for the loop
    acc_in = RawWALAccessor(input_schema)
    acc_exemplar = RawWALAccessor(input_schema)
    reduce_acc = ReduceAccessor(input_schema, output_schema, group_by_cols)

    idx = 0
    while idx < n:
        # --- Start of Group ---
        group_start_pos = idx
        group_start_idx = sorted_indices[group_start_pos]

        delta_in.bind_raw_accessor(group_start_idx, acc_exemplar)
        if group_by_pk:
            group_key = delta_in.get_pk(group_start_idx)
        else:
            group_key = _extract_group_key(acc_exemplar, input_schema, group_by_cols)

        # 3. Delta Contribution: Accumulate only the changes from this tick.
        agg_func.reset()
        while idx < n:
            curr_idx = sorted_indices[idx]
            delta_in.bind_raw_accessor(curr_idx, acc_in)
            
            # Check if we are still in the same group
            if group_by_pk:
                if delta_in.get_pk(curr_idx) != group_key:
                    break
            else:
                if _compare_by_cols(acc_in, acc_exemplar, input_schema, group_by_cols) != 0:
                    break
            
            agg_func.step(acc_in, delta_in.get_weight(curr_idx))
            idx += 1

        # 4. Retraction: Find the previously emitted aggregate for this group.
        trace_out_cursor.seek(group_key)

        has_old = False
        old_val_bits = r_uint64(0)
        if trace_out_cursor.is_valid() and trace_out_cursor.key() == group_key:
            has_old = True
            # The aggregate result is always the last column in the output schema.
            agg_col_idx = len(output_schema.columns) - 1
            old_val_bits = trace_out_cursor.get_accessor().get_int(agg_col_idx)

            # Emit the retraction of the previous aggregate value (-1 weight).
            reduce_acc.set_context(acc_exemplar, agg_func, old_val_bits, True)
            out_batch.append_from_accessor(group_key, r_int64(-1), reduce_acc)

        # 5. New Value Calculation
        if agg_func.is_linear() and has_old:
            # Linear shortcut (Count/Sum): new_agg = old_agg + delta_contribution.
            agg_func.merge_accumulated(old_val_bits, r_int64(1))
        
        elif not agg_func.is_linear():
            # Non-linear (Min/Max): must replay the full history for this group.
            agg_func.reset()
            if trace_in_cursor is not None:
                trace_in_cursor.seek(group_key)
                while trace_in_cursor.is_valid() and trace_in_cursor.key() == group_key:
                    agg_func.step(trace_in_cursor.get_accessor(), trace_in_cursor.weight())
                    trace_in_cursor.advance()
            
            # Then apply the current tick's delta on top of the recovered history.
            for k in range(group_start_pos, idx):
                d_idx = sorted_indices[k]
                delta_in.bind_raw_accessor(d_idx, acc_in)
                agg_func.step(acc_in, delta_in.get_weight(d_idx))

        # 6. Emission: Emit the updated aggregate value (+1 weight).
        # Note: In DBSP, if the new count is 0, we simply don't emit an insertion,
        # effectively leaving only the retraction of the previous value.
        if not agg_func.is_accumulator_zero():
            reduce_acc.set_context(acc_exemplar, agg_func, r_uint64(0), False)
            out_batch.append_from_accessor(group_key, r_int64(1), reduce_acc)

    # Clean up output batch
    out_batch.sort()
    out_batch.consolidate()
