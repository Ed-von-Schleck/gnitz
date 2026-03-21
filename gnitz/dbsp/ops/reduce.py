# gnitz/dbsp/ops/reduce.py

from rpython.rlib import jit
from rpython.rlib.longlong2float import longlong2float
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask

from gnitz.core import types, strings, errors
from gnitz.core.comparator import RowAccessor
from gnitz.core.batch import (
    ArenaZSetBatch, SortedScope, BatchWriter, ColumnarBatchAccessor,
    pk_lt, pk_eq,
)
from gnitz.dbsp.functions import NULL_AGGREGATE, AggregateFunction
from gnitz.dbsp.ops.group_index import (
    promote_group_col_to_u64 as _gi_promote,
    _extract_group_key,
    _extract_gc_u64,
)

"""
Non-linear Reduce Operator for the DBSP algebra.

Implements incremental GROUP BY + aggregation.
Formula: δ_out = Agg(history + δ_in) - Agg(history).
"""


class ReduceAccessor(RowAccessor):
    """
    Virtual RowAccessor that assembles one output record for the REDUCE operator.
    Maps output column indices to either group exemplar columns or
    aggregate function results.

    Sentinel encoding in mapping_to_input:
      >= 0  : input column index (group exemplar)
      -1    : aggregate slot 0
      -2    : synthetic PK (group hash)
      <= -3 : aggregate slot (-src) - 2
    """

    _immutable_fields_ = [
        "input_schema",
        "output_schema",
        "mapping_to_input[*]",
    ]

    def __init__(self, input_schema, output_schema, group_indices, num_aggs):
        self.input_schema = input_schema
        self.output_schema = output_schema

        num_out = len(output_schema.columns)
        mapping = [0] * num_out

        use_natural_pk = False
        if len(group_indices) == 1:
            grp_col_type = input_schema.columns[group_indices[0]].field_type.code
            if (grp_col_type == types.TYPE_U64.code
                    or grp_col_type == types.TYPE_U128.code):
                use_natural_pk = True

        if use_natural_pk:
            mapping[0] = group_indices[0]
            for i in range(num_aggs):
                mapping[1 + i] = -1 if i == 0 else -(i + 2)
        else:
            mapping[0] = -2
            for i in range(len(group_indices)):
                mapping[i + 1] = group_indices[i]
            agg_base = 1 + len(group_indices)
            for i in range(num_aggs):
                mapping[agg_base + i] = -1 if i == 0 else -(i + 2)

        self.mapping_to_input = mapping

        # Mutable state for context switching during iteration
        self.exemplar = None
        self.agg_funcs = [NULL_AGGREGATE]
        self.old_vals_bits = [r_uint64(0)]
        self.use_old_val = False

    def set_context(self, exemplar, agg_funcs, old_vals_bits, use_old_val):
        """Prepares the accessor to represent a specific group output."""
        self.exemplar = exemplar
        self.agg_funcs = agg_funcs
        self.old_vals_bits = old_vals_bits
        self.use_old_val = use_old_val

    def _agg_idx(self, src):
        if src == -1:
            return 0
        return (-src) - 2

    @jit.unroll_safe
    def is_null(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1 or src <= -3:
            if self.use_old_val:
                return False  # Old value from trace is never null
            return self.agg_funcs[self._agg_idx(src)].is_accumulator_zero()
        if src < 0:
            return False
        if self.exemplar is not None:
            return self.exemplar.is_null(src)
        return True

    @jit.unroll_safe
    def get_int(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1 or src <= -3:
            idx = self._agg_idx(src)
            if self.use_old_val:
                return self.old_vals_bits[idx]
            return self.agg_funcs[idx].get_value_bits()
        if src < 0:
            return r_uint64(0)
        if self.exemplar is not None:
            return self.exemplar.get_int(src)
        return r_uint64(0)

    @jit.unroll_safe
    def get_int_signed(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1 or src <= -3:
            idx = self._agg_idx(src)
            bits = self.old_vals_bits[idx] if self.use_old_val else self.agg_funcs[idx].get_value_bits()
            return rffi.cast(rffi.LONGLONG, bits)
        if src < 0:
            return r_int64(0)
        if self.exemplar is not None:
            return self.exemplar.get_int_signed(src)
        return r_int64(0)

    @jit.unroll_safe
    def get_float(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1 or src <= -3:
            idx = self._agg_idx(src)
            bits = self.old_vals_bits[idx] if self.use_old_val else self.agg_funcs[idx].get_value_bits()
            return longlong2float(rffi.cast(rffi.LONGLONG, bits))
        if src < 0:
            return 0.0
        if self.exemplar is not None:
            return self.exemplar.get_float(src)
        return 0.0

    @jit.unroll_safe
    def get_u128_lo(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src >= 0 and self.exemplar is not None:
            return self.exemplar.get_u128_lo(src)
        return r_uint64(0)

    @jit.unroll_safe
    def get_u128_hi(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src >= 0 and self.exemplar is not None:
            return self.exemplar.get_u128_hi(src)
        return r_uint64(0)

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
            va_lo = accessor_a.get_u128_lo(col_idx)
            va_hi = accessor_a.get_u128_hi(col_idx)
            vb_lo = accessor_b.get_u128_lo(col_idx)
            vb_hi = accessor_b.get_u128_hi(col_idx)
            if pk_lt(va_lo, va_hi, vb_lo, vb_hi): return -1
            if pk_lt(vb_lo, vb_hi, va_lo, va_hi): return 1
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


def _insertion_sort_i64(indices, keys, lo, hi):
    for i in range(lo + 1, hi):
        key_idx = indices[i]
        key_val = keys[key_idx]
        j = i - 1
        while j >= lo and keys[indices[j]] > key_val:
            indices[j + 1] = indices[j]
            j -= 1
        indices[j + 1] = key_idx


def _merge_i64(indices, keys, lo, mid, hi, scratch):
    if keys[indices[mid - 1]] <= keys[indices[mid]]:
        return
    for k in range(lo, mid):
        scratch[k] = indices[k]
    i = lo
    j = mid
    k = lo
    while i < mid and j < hi:
        if keys[scratch[i]] <= keys[indices[j]]:
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


def _mergesort_i64(indices, keys, lo, hi, scratch):
    if hi - lo <= 32:
        _insertion_sort_i64(indices, keys, lo, hi)
        return
    mid = (lo + hi) >> 1
    _mergesort_i64(indices, keys, lo, mid, scratch)
    _mergesort_i64(indices, keys, mid, hi, scratch)
    _merge_i64(indices, keys, lo, mid, hi, scratch)


def _argsort_delta(batch, schema, col_indices):
    count = batch.length()
    indices = newlist_hint(count)
    for i in range(count):
        indices.append(i)

    if count <= 1:
        return indices

    # Fast path: single I64 group column — pre-extract keys, sort with direct int comparison
    if len(col_indices) == 1:
        ci = col_indices[0]
        col_type = schema.columns[ci].field_type
        if col_type.code == types.TYPE_I64.code:
            col_base = batch.col_bufs[ci].base_ptr
            keys = newlist_hint(count)
            for i in range(count):
                ptr = rffi.ptradd(col_base, i * 8)
                keys.append(intmask(rffi.cast(rffi.LONGLONGP, ptr)[0]))
            if count <= 32:
                _insertion_sort_i64(indices, keys, 0, count)
            else:
                scratch = newlist_hint(count)
                for _ in range(count):
                    scratch.append(0)
                _mergesort_i64(indices, keys, 0, count, scratch)
            return indices

    # General path: accessor dispatch (multi-column or non-integer types)
    acc_a = ColumnarBatchAccessor(schema)
    acc_b = ColumnarBatchAccessor(schema)
    if count <= 32:
        _insertion_sort_by_cols(indices, batch, schema, col_indices, 0, count, acc_a, acc_b)
    else:
        scratch = newlist_hint(count)
        for _ in range(count):
            scratch.append(0)
        _mergesort_by_cols(indices, batch, schema, col_indices, 0, count, scratch, acc_a, acc_b)
    return indices


def _insertion_sort_by_cols(indices, batch, schema, col_indices, lo, hi, acc_a, acc_b):
    for i in range(lo + 1, hi):
        key = indices[i]
        j = i - 1
        batch.bind_accessor(key, acc_b)
        while j >= lo:
            batch.bind_accessor(indices[j], acc_a)
            if _compare_by_cols(acc_a, acc_b, schema, col_indices) <= 0:
                break
            indices[j + 1] = indices[j]
            j -= 1
        indices[j + 1] = key


def _mergesort_by_cols(indices, batch, schema, col_indices, lo, hi, scratch, acc_a, acc_b):
    if hi - lo <= 32:
        _insertion_sort_by_cols(indices, batch, schema, col_indices, lo, hi, acc_a, acc_b)
        return
    mid = (lo + hi) >> 1
    _mergesort_by_cols(indices, batch, schema, col_indices, lo, mid, scratch, acc_a, acc_b)
    _mergesort_by_cols(indices, batch, schema, col_indices, mid, hi, scratch, acc_a, acc_b)
    _merge_by_cols(indices, batch, schema, col_indices, lo, mid, hi, scratch, acc_a, acc_b)


def _merge_by_cols(indices, batch, schema, col_indices, lo, mid, hi, scratch, acc_a, acc_b):
    batch.bind_accessor(indices[mid - 1], acc_a)
    batch.bind_accessor(indices[mid], acc_b)
    if _compare_by_cols(acc_a, acc_b, schema, col_indices) <= 0:
        return
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


# ---------------------------------------------------------------------------
# Reduce operator implementation
# ---------------------------------------------------------------------------


def _apply_agg_from_value_index(avi_cursor, gc_u64, for_max, agg_col_type, agg_func):
    """
    Seek AVI to group prefix; apply the decoded min/max to agg_func.
    Resets agg_func if the group is empty (no positive-weight entry found).
    Returns True if a value was found.
    """
    avi_cursor.seek(r_uint64(0), gc_u64)
    while avi_cursor.is_valid():
        k_hi = avi_cursor.key_hi()
        if k_hi != gc_u64:
            break
        if avi_cursor.weight() > r_int64(0):
            encoded = avi_cursor.key_lo()
            if for_max:
                encoded = r_uint64(~intmask(encoded))
            code = agg_col_type.code
            if (code == types.TYPE_I64.code or code == types.TYPE_I32.code
                    or code == types.TYPE_I16.code or code == types.TYPE_I8.code):
                encoded = r_uint64(intmask(encoded) - intmask(r_uint64(1) << 63))
            elif code == types.TYPE_F64.code or code == types.TYPE_F32.code:
                if encoded >> 63:
                    encoded = r_uint64(intmask(encoded) ^ intmask(r_uint64(1) << 63))
                else:
                    encoded = r_uint64(~intmask(encoded))
            agg_func.seed_from_raw_bits(encoded)
            return True
        avi_cursor.advance()
    agg_func.reset()
    return False


def _emit_reduce_row(out_writer, fin_out_writer, group_key_lo, group_key_hi, weight, reduce_acc,
                     finalize_prog):
    """Emit one raw row + optionally a finalized row (for AVG etc.)."""
    out_writer.append_from_accessor(group_key_lo, group_key_hi, weight, reduce_acc)
    if finalize_prog is not None and fin_out_writer is not None:
        from gnitz.core.batch import RowBuilder
        from gnitz.dbsp.expr import eval_expr_map
        finalized_schema = fin_out_writer._batch._schema
        builder = RowBuilder(finalized_schema, fin_out_writer._batch)
        eval_expr_map(finalize_prog, reduce_acc, builder)
        builder.commit_row(group_key_lo, group_key_hi, weight)


def op_reduce(
    delta_in,
    input_schema,
    trace_in_cursor,
    trace_out_cursor,
    out_writer,
    group_by_cols,
    agg_funcs,
    output_schema,
    trace_in_group_idx=None,
    agg_value_idx=None,
    finalize_prog=None,
    fin_out_writer=None,
):
    """
    Incremental DBSP REDUCE: δ_out = Agg(history + δ_in) - Agg(history).

    agg_funcs: list of AggregateFunction (one per aggregate column).
    finalize_prog: optional ExprProgram applied to reduce_acc at both emission
                   sites, writing finalized rows to fin_out_writer.
    fin_out_writer: BatchWriter for the finalized output register (or None).
    """
    if agg_funcs is None:
        return

    num_aggs = len(agg_funcs)
    num_out_cols = len(output_schema.columns)

    # Pre-allocate dummy old_vals for emission path (use_old_val=False)
    dummy_old_vals = newlist_hint(num_aggs)
    for _i in range(num_aggs):
        dummy_old_vals.append(r_uint64(0))

    # Hoist linearity flags early: determines whether consolidation is needed.
    # is_linear() is a compile-time constant per agg_func (Opt 5).
    agg_lin_flags = newlist_hint(num_aggs)
    all_linear = True
    for af in agg_funcs:
        lin = jit.promote(af.is_linear())
        agg_lin_flags.append(lin)
        if not lin:
            all_linear = False
    all_linear = jit.promote(all_linear)

    # 1. Obtain a view of the input delta.
    # For linear aggregates, consolidation is semantically unnecessary:
    # Σ(w_i * f(x_i)) is weight-linear and order-independent, so PK merging
    # gives the same result. Skipping it avoids an O(N log N) sort+copy.
    if all_linear:
        b = delta_in
        b_to_free = None
    else:
        b = delta_in.to_consolidated()
        b_to_free = b if b is not delta_in else None

    n = b.length()
    if n == 0:
        if b_to_free is not None:
            b_to_free.free()
        return

    # 2. Grouping
    group_by_pk = (
        len(group_by_cols) == 1
        and group_by_cols[0] == input_schema.pk_index
    )

    if group_by_pk:
        sorted_indices = newlist_hint(n)
        for i in range(n):
            sorted_indices.append(i)
    else:
        sorted_indices = _argsort_delta(b, input_schema, group_by_cols)

    acc_in = ColumnarBatchAccessor(input_schema)
    acc_exemplar = ColumnarBatchAccessor(input_schema)
    reduce_acc = ReduceAccessor(input_schema, output_schema, group_by_cols, num_aggs)

    # Hoist cursors: create once outside the per-group loop (Opt 3)
    gi_cursor = None
    avi_cursor = None
    if trace_in_group_idx is not None:
        gi_cursor = trace_in_group_idx.create_cursor()
    if agg_value_idx is not None:
        avi_cursor = agg_value_idx.create_cursor()

    idx = 0
    while idx < n:
        # --- Start of Group ---
        group_start_pos = idx
        group_start_idx = sorted_indices[group_start_pos]

        b.bind_accessor(group_start_idx, acc_exemplar)
        if group_by_pk:
            group_key_lo = b.get_pk_lo(group_start_idx)
            group_key_hi = b.get_pk_hi(group_start_idx)
        else:
            group_key_lo, group_key_hi = _extract_group_key(acc_exemplar, input_schema, group_by_cols)

        # 3. Calculate Delta contribution for this group
        for af in agg_funcs:
            af.reset()
        while idx < n:
            curr_idx = sorted_indices[idx]
            b.bind_accessor(curr_idx, acc_in)

            if group_by_pk:
                if not pk_eq(b.get_pk_lo(curr_idx), b.get_pk_hi(curr_idx), group_key_lo, group_key_hi):
                    break
            else:
                if _compare_by_cols(acc_in, acc_exemplar, input_schema, group_by_cols) != 0:
                    break

            w = b.get_weight(curr_idx)
            for k in range(num_aggs):
                if agg_lin_flags[k]:
                    agg_funcs[k].step(acc_in, w)
            idx += 1

        # 4. Retraction: Agg(history)
        trace_out_cursor.seek(group_key_lo, group_key_hi)
        has_old = False
        old_vals_bits = dummy_old_vals
        if trace_out_cursor.is_valid() and pk_eq(trace_out_cursor.key_lo(), trace_out_cursor.key_hi(), group_key_lo, group_key_hi):
            has_old = True
            old_vals_bits = newlist_hint(num_aggs)
            trace_acc = trace_out_cursor.get_accessor()
            for k in range(num_aggs):
                agg_col_idx = num_out_cols - num_aggs + k
                old_vals_bits.append(trace_acc.get_int(agg_col_idx))

            reduce_acc.set_context(acc_exemplar, agg_funcs, old_vals_bits, True)
            _emit_reduce_row(out_writer, fin_out_writer, group_key_lo, group_key_hi, r_int64(-1),
                             reduce_acc, finalize_prog)

        # 5. New Value Calculation: Agg(history + delta)
        if all_linear and has_old:
            for k in range(num_aggs):
                agg_funcs[k].merge_accumulated(old_vals_bits[k], r_int64(1))
        else:
            if not all_linear:
                if avi_cursor is not None:
                    # AVI path: O(log N + 1) — seed accumulator from index
                    gc_u64 = _extract_gc_u64(
                        acc_exemplar, agg_value_idx.input_schema,
                        agg_value_idx.group_by_cols,
                    )
                    _apply_agg_from_value_index(
                        avi_cursor, gc_u64, agg_value_idx.for_max,
                        agg_value_idx.agg_col_type, agg_funcs[0],
                    )
                else:
                    replay = ArenaZSetBatch(input_schema, initial_capacity=32)

                    if trace_in_cursor is not None:
                        if group_by_pk:
                            trace_in_cursor.seek(group_key_lo, group_key_hi)
                            while trace_in_cursor.is_valid() and pk_eq(trace_in_cursor.key_lo(), trace_in_cursor.key_hi(), group_key_lo, group_key_hi):
                                replay.append_from_accessor(
                                    trace_in_cursor.key_lo(), trace_in_cursor.key_hi(),
                                    trace_in_cursor.weight(),
                                    trace_in_cursor.get_accessor(),
                                )
                                trace_in_cursor.advance()
                        else:
                            if gi_cursor is not None:
                                gc_u64 = _gi_promote(
                                    acc_exemplar,
                                    trace_in_group_idx.col_idx,
                                    trace_in_group_idx.col_type,
                                )
                                gi_cursor.seek(r_uint64(0), gc_u64)
                                while gi_cursor.is_valid():
                                    gk_hi = gi_cursor.key_hi()
                                    if gk_hi != gc_u64:
                                        break
                                    if gi_cursor.weight() > r_int64(0):
                                        spk_lo_u64 = gi_cursor.key_lo()
                                        spk_hi_u64 = r_uint64(rffi.cast(
                                            rffi.ULONGLONG,
                                            gi_cursor.get_accessor().get_int_signed(1),
                                        ))
                                        trace_in_cursor.seek(spk_lo_u64, spk_hi_u64)
                                        if (trace_in_cursor.is_valid()
                                                and pk_eq(trace_in_cursor.key_lo(), trace_in_cursor.key_hi(), spk_lo_u64, spk_hi_u64)):
                                            replay.append_from_accessor(
                                                trace_in_cursor.key_lo(),
                                                trace_in_cursor.key_hi(),
                                                trace_in_cursor.weight(),
                                                trace_in_cursor.get_accessor(),
                                            )
                                    gi_cursor.advance()
                            else:
                                trace_in_cursor.seek(r_uint64(0), r_uint64(0))
                                while trace_in_cursor.is_valid():
                                    trace_acc = trace_in_cursor.get_accessor()
                                    if _compare_by_cols(trace_acc, acc_exemplar,
                                                        input_schema, group_by_cols) == 0:
                                        replay.append_from_accessor(
                                            trace_in_cursor.key_lo(),
                                            trace_in_cursor.key_hi(),
                                            trace_in_cursor.weight(),
                                            trace_acc,
                                        )
                                    trace_in_cursor.advance()

                    for k in range(group_start_pos, idx):
                        d_idx = sorted_indices[k]
                        b.bind_accessor(d_idx, acc_in)
                        replay.append_from_accessor(
                            b.get_pk_lo(d_idx), b.get_pk_hi(d_idx), b.get_weight(d_idx), acc_in,
                        )

                    merged = replay.to_consolidated()
                    for af in agg_funcs:
                        af.reset()
                    replay_acc = ColumnarBatchAccessor(input_schema)
                    for m in range(merged.length()):
                        w = merged.get_weight(m)
                        if w > r_int64(0):
                            merged.bind_accessor(m, replay_acc)
                            for af in agg_funcs:
                                af.step(replay_acc, w)
                    if merged is not replay:
                        merged.free()
                    replay.free()

        # 6. Emission: +1 Agg(history + delta)
        any_nonzero = False
        for af in agg_funcs:
            if not af.is_accumulator_zero():
                any_nonzero = True
                break
        if any_nonzero:
            reduce_acc.set_context(acc_exemplar, agg_funcs, dummy_old_vals, False)
            _emit_reduce_row(out_writer, fin_out_writer, group_key_lo, group_key_hi, r_int64(1),
                             reduce_acc, finalize_prog)

    if gi_cursor is not None:
        gi_cursor.close()
    if avi_cursor is not None:
        avi_cursor.close()

    out_writer.mark_sorted(group_by_pk)
    if group_by_pk:
        out_writer.mark_consolidated(True)

    if b_to_free is not None:
        b_to_free.free()


def op_gather_reduce(
    partial_batch,
    partial_schema,
    trace_out_cursor,
    trace_out_table,
    out_writer,
    agg_funcs,
):
    """
    Gather-reduce: merge partial aggregate deltas from workers that had local
    changes this tick, fold with the old global value from trace_out, and emit
    the net global delta.

    Correct only for linear aggregates (COUNT, SUM, COUNT_NON_NULL).
    trace_out_table is used by the integrate_op instruction that follows.
    """
    if agg_funcs is None:
        return

    num_aggs = len(agg_funcs)
    num_out_cols = len(partial_schema.columns)

    dummy_old_vals = newlist_hint(num_aggs)
    for _i in range(num_aggs):
        dummy_old_vals.append(r_uint64(0))

    # Derive group_indices for ReduceAccessor from partial_schema layout.
    # Agg cols = last num_aggs positions; non-agg non-PK cols = 1..K.
    num_group_cols = num_out_cols - 1 - num_aggs
    group_indices_in_partial = newlist_hint(1)
    if num_group_cols == 0:
        # use_natural_pk=True: group col IS the PK at col 0.
        group_indices_in_partial.append(0)
    else:
        for _gi in range(num_group_cols):
            group_indices_in_partial.append(1 + _gi)

    reduce_acc = ReduceAccessor(partial_schema, partial_schema,
                                group_indices_in_partial, num_aggs)

    # SortedScope: sort by PK without merging rows — multiple workers may
    # contribute rows with the same group PK and different agg column values.
    with SortedScope(partial_batch) as b:
        n = b.length()
        if n == 0:
            return

        acc_in = ColumnarBatchAccessor(partial_schema)
        acc_exemplar = ColumnarBatchAccessor(partial_schema)

        idx = 0
        while idx < n:
            group_key_lo = b.get_pk_lo(idx)
            group_key_hi = b.get_pk_hi(idx)
            b.bind_accessor(idx, acc_exemplar)

            for af in agg_funcs:
                af.reset()

            # Accumulate all partial deltas for this group.
            while idx < n and pk_eq(b.get_pk_lo(idx), b.get_pk_hi(idx),
                                    group_key_lo, group_key_hi):
                b.bind_accessor(idx, acc_in)
                w = b.get_weight(idx)
                for k in range(num_aggs):
                    bits = acc_in.get_int(num_out_cols - num_aggs + k)
                    if w > r_int64(0):
                        agg_funcs[k].combine(bits)
                    elif w < r_int64(0):
                        agg_funcs[k].merge_accumulated(bits, r_int64(-1))
                idx += 1

            # Read old global from trace_out and fold in.
            trace_out_cursor.seek(group_key_lo, group_key_hi)
            old_vals_bits = dummy_old_vals
            if trace_out_cursor.is_valid() and pk_eq(trace_out_cursor.key_lo(),
                                                     trace_out_cursor.key_hi(),
                                                     group_key_lo, group_key_hi):
                old_vals_bits = newlist_hint(num_aggs)
                trace_acc = trace_out_cursor.get_accessor()
                for k in range(num_aggs):
                    agg_col_idx = num_out_cols - num_aggs + k
                    old_vals_bits.append(trace_acc.get_int(agg_col_idx))

                # Emit retraction of old global.
                reduce_acc.set_context(acc_exemplar, agg_funcs, old_vals_bits, True)
                out_writer.append_from_accessor(group_key_lo, group_key_hi,
                                                r_int64(-1), reduce_acc)

                # Fold old global into accumulator: new_global = delta + old_global.
                for k in range(num_aggs):
                    agg_funcs[k].merge_accumulated(old_vals_bits[k], r_int64(1))

            # Emit new global if non-zero.
            any_nonzero = False
            for af in agg_funcs:
                if not af.is_accumulator_zero():
                    any_nonzero = True
                    break
            if any_nonzero:
                reduce_acc.set_context(acc_exemplar, agg_funcs, dummy_old_vals, False)
                out_writer.append_from_accessor(group_key_lo, group_key_hi,
                                                r_int64(1), reduce_acc)

        out_writer.mark_sorted(True)
