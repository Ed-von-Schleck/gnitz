# gnitz/dbsp/ops/reduce.py

from rpython.rlib import jit
from rpython.rlib.longlong2float import longlong2float
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.core import types, strings, xxh
from gnitz.core.comparator import RowAccessor, PayloadRowAccessor

"""
Non-linear Reduce Operator for the DBSP algebra.

Implements incremental GROUP BY + aggregation.  The core identity is:

    δ_out = Agg(I(δ_in)) - Agg(z^{-1}(I(δ_in)))
          = Agg(history + δ_in) - Agg(history)

For linear aggregates (COUNT, SUM) the new aggregate value is computed by
merging the old stored value with the delta contribution, avoiding a full
replay of history.

For non-linear aggregates (MIN, MAX) the new value must be recomputed from
scratch by replaying the full history trace (trace_in_cursor) plus the
current delta.

State (the current aggregate per group) is maintained by the caller in a
persistent trace table.  op_reduce reads that state via trace_out_cursor.
The caller is responsible for updating the trace after this call — the
QueryBuilder emits a separate IntegrateOp for that purpose.
"""


class ReduceAccessor(RowAccessor):
    """
    Virtual RowAccessor that assembles one output record for the REDUCE operator.

    The output schema has two sources per column:
      - Group-by columns: forwarded from the exemplar (a PayloadRowAccessor
        over the input schema).
      - Aggregate result column: taken from agg_func.get_value_bits() for the
        new value, or from old_val_bits for the retraction of the previous value.

    The mapping array translates output column indices to input column indices
    for group-by columns, or to sentinel values:
      -1  aggregate result column
      -2  synthetic hash PK (col 0 in the non-natural-PK case; skipped by
          serialize_row because pk_index == 0, so never reached by get_* calls)

    Output schema layout (mirrors _build_reduce_output_schema exactly):

    Natural-PK case (single group-by col of type U64 or U128):
      col 0 (pk): the group column          mapping → group_indices[0]
      col 1:      aggregate result           mapping → -1

    Synthetic-PK case (everything else):
      col 0 (pk): synthetic U128 hash       mapping → -2
      col 1..N:   group-by columns          mapping → group_indices[0..N-1]
      col N+1:    aggregate result           mapping → -1

    set_context() is called twice per group when a retraction is needed:
      first with use_old_val=True  (emit the old value with weight -1)
      then  with use_old_val=False (emit the new value with weight +1)
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
            mapping[0] = group_indices[0]  # pk col — skipped by serialize_row
            mapping[1] = -1               # aggregate result
        else:
            mapping[0] = -2               # synthetic hash PK — skipped by serialize_row
            for i in range(len(group_indices)):
                mapping[i + 1] = group_indices[i]
            mapping[num_out - 1] = -1     # aggregate result

        self.mapping_to_input = mapping

        self.exemplar = None
        self.agg_func = None
        self.old_val_bits = r_uint64(0)
        self.use_old_val = False

    def set_context(self, exemplar, agg_func, old_val_bits, use_old_val):
        """Binds the accessor to a specific group and emission mode."""
        self.exemplar = exemplar
        self.agg_func = agg_func
        self.old_val_bits = old_val_bits
        self.use_old_val = use_old_val

    @jit.unroll_safe
    def is_null(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src < 0:
            return False
        return self.exemplar.is_null(src)

    @jit.unroll_safe
    def get_int(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1:
            if self.use_old_val:
                return self.old_val_bits
            return self.agg_func.get_value_bits()
        if src < 0:
            return r_uint64(0)
        return self.exemplar.get_int(src)

    @jit.unroll_safe
    def get_int_signed(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1:
            if self.use_old_val:
                return rffi.cast(rffi.LONGLONG, self.old_val_bits)
            return rffi.cast(rffi.LONGLONG, self.agg_func.get_value_bits())
        if src < 0:
            return r_int64(0)
        return self.exemplar.get_int_signed(src)

    @jit.unroll_safe
    def get_float(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1:
            if self.use_old_val:
                return longlong2float(rffi.cast(rffi.LONGLONG, self.old_val_bits))
            return longlong2float(rffi.cast(rffi.LONGLONG, self.agg_func.get_value_bits()))
        if src < 0:
            return 0.0
        return self.exemplar.get_float(src)

    @jit.unroll_safe
    def get_u128(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src < 0:
            return r_uint128(0)
        return self.exemplar.get_u128(src)

    @jit.unroll_safe
    def get_str_struct(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src < 0:
            return (0, 0, strings.NULL_PTR, strings.NULL_PTR, None)
        return self.exemplar.get_str_struct(src)

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
    Returns negative, zero, or positive in the usual comparison convention.
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

    Uses insertion sort, which is acceptable because group-by columns are
    typically low-cardinality and the delta batch is small per tick.

    get_row() is used rather than get_accessor() because comparing two records
    simultaneously requires two independent accessors; the batch's single shared
    RawWALAccessor cannot serve both roles at once.
    """
    count = batch.length()
    indices = newlist_hint(count)
    for i in range(count):
        indices.append(i)

    if count <= 1:
        return indices

    acc_a = PayloadRowAccessor(schema)
    acc_b = PayloadRowAccessor(schema)

    for i in range(1, count):
        j = i
        while j > 0:
            acc_a.set_row(batch.get_row(indices[j]))
            acc_b.set_row(batch.get_row(indices[j - 1]))
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

    For the natural-PK case (single U64 or U128 group-by column) the key is
    taken directly.  For all other cases a hash of the group column values is
    computed and used as a synthetic key.
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

    delta_in:         ArenaZSetBatch   — consolidated input delta for this tick
    input_schema:     TableSchema      — schema of delta_in
    trace_in_cursor:  AbstractCursor or None
                        Seekable cursor over the full input history I(δ_in).
                        Required (non-None) only for non-linear aggregates
                        (MIN, MAX) that must recompute from scratch.
                        Must be None for linear aggregates (COUNT, SUM).
    trace_out_cursor: AbstractCursor
                        Seekable cursor over the current aggregate trace, i.e.
                        the stored result of Agg(history) per group key.
                        Used to retrieve the old aggregate value for retraction.
    out_batch:        ArenaZSetBatch   — output destination (caller clears beforehand)
    group_by_cols:    list[int]        — column indices in input_schema to group by
    agg_func:         AggregateFunction
    output_schema:    TableSchema      — schema of out_batch

    The caller (the VM interpreter) is responsible for persisting out_batch into
    the aggregate trace table after this call returns, typically via a subsequent
    IntegrateOp instruction.
    """
    if delta_in.length() == 0:
        return

    if not delta_in.is_sorted():
        delta_in.sort()
    delta_in.consolidate()

    n = delta_in.length()
    if n == 0:
        return

    group_by_pk = (
        len(group_by_cols) == 1
        and group_by_cols[0] == input_schema.pk_index
    )

    if group_by_pk:
        sorted_indices = newlist_hint(n)
        for i in range(n):
            sorted_indices.append(i)
    else:
        sorted_indices = _argsort_delta(delta_in, input_schema, group_by_cols)

    acc_in = PayloadRowAccessor(input_schema)
    acc_exemplar = PayloadRowAccessor(input_schema)
    reduce_acc = ReduceAccessor(input_schema, output_schema, group_by_cols)

    idx = 0
    while idx < n:
        group_start_pos = idx
        group_start_idx = sorted_indices[group_start_pos]

        acc_exemplar.set_row(delta_in.get_row(group_start_idx))
        if group_by_pk:
            group_key = delta_in.get_pk(group_start_idx)
        else:
            group_key = _extract_group_key(acc_exemplar, input_schema, group_by_cols)

        # Accumulate the delta contribution for this group.
        agg_func.reset()
        while idx < n:
            curr_idx = sorted_indices[idx]
            acc_in.set_row(delta_in.get_row(curr_idx))
            if group_by_pk:
                if delta_in.get_pk(curr_idx) != group_key:
                    break
            else:
                if _compare_by_cols(acc_in, acc_exemplar, input_schema, group_by_cols) != 0:
                    break
            agg_func.step(acc_in, delta_in.get_weight(curr_idx))
            idx += 1

        # Look up the stored aggregate for this group so we can retract it.
        trace_out_cursor.seek(group_key)

        has_old = False
        old_val_bits = r_uint64(0)
        if trace_out_cursor.is_valid() and trace_out_cursor.key() == group_key:
            has_old = True
            agg_col_idx = len(output_schema.columns) - 1
            old_val_bits = trace_out_cursor.get_accessor().get_int(agg_col_idx)

            # Emit the retraction of the previous aggregate value.
            reduce_acc.set_context(acc_exemplar, agg_func, old_val_bits, True)
            out_batch.append_from_accessor(group_key, r_int64(-1), reduce_acc)

        # Compute the new aggregate value.
        if agg_func.is_linear() and has_old:
            # Linear shortcut: new_agg = old_agg + delta_contribution.
            # delta_contribution is already in agg_func from the loop above.
            agg_func.merge_accumulated(old_val_bits, r_int64(1))
        elif not agg_func.is_linear():
            # Non-linear: must replay full history plus the current delta.
            # trace_in_cursor must be non-None when this branch is reachable.
            agg_func.reset()
            trace_in_cursor.seek(group_key)
            while trace_in_cursor.is_valid() and trace_in_cursor.key() == group_key:
                agg_func.step(trace_in_cursor.get_accessor(), trace_in_cursor.weight())
                trace_in_cursor.advance()
            for k in range(group_start_pos, idx):
                d_idx = sorted_indices[k]
                acc_in.set_row(delta_in.get_row(d_idx))
                agg_func.step(acc_in, delta_in.get_weight(d_idx))
        # Linear agg without a stored old value: agg_func already holds the
        # delta contribution from the accumulation loop above; nothing to merge.

        # Emit the new aggregate value if the accumulator is non-zero.
        if not agg_func.is_accumulator_zero():
            reduce_acc.set_context(acc_exemplar, agg_func, r_uint64(0), False)
            out_batch.append_from_accessor(group_key, r_int64(1), reduce_acc)

    out_batch.sort()
    out_batch.consolidate()
