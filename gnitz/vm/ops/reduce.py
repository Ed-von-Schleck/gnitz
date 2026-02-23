# gnitz/vm/ops/reduce.py

from rpython.rlib import jit
from rpython.rlib.longlong2float import longlong2float
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.core import types, strings, xxh
from gnitz.core.comparator import RowAccessor, PayloadRowAccessor
from gnitz.vm import runtime


class ReduceAccessor(RowAccessor):
    """
    Virtual RowAccessor that assembles one output record for the REDUCE operator.

    Why this exists
    ---------------
    op_reduce emits results through out_batch.append_from_accessor(), which
    requires a RowAccessor over the OUTPUT schema.  The output record has two
    distinct sources:

      1. Group-by columns — read from the exemplar (the first input row of the
         current group), forwarded via self.exemplar (a PayloadRowAccessor over
         the INPUT schema).

      2. The aggregate result column — read from self.agg_func.get_value_bits()
         (current tick) or self.old_val_bits (retraction of the previous result).

    The mapping array translates OUTPUT column indices to INPUT column indices
    (for group-by columns) or to sentinel values (-1 for the agg result, -2
    for the synthetic hash PK which is never read by serialize_row).

    Output schema structure (mirrors _build_reduce_output_schema exactly)
    ---------------------------------------------------------------------
    Natural-PK case — single group-by column of type U64 or U128:
      col 0  (pk_index=0): the group column itself          mapping → group_indices[0]
      col 1:               the aggregate result             mapping → -1

    Synthetic-PK case — everything else:
      col 0  (pk_index=0): synthetic U128 hash PK          mapping → -2
      col 1..N:            group-by columns                 mapping → group_indices[0..N-1]
      col N+1:             the aggregate result             mapping → -1

    Since pk_index is always 0, serialize_row skips col 0 entirely, so mappings
    for col 0 (-2 or group_indices[0]) are never reached by get_* calls from
    the serialisation path.  They exist only to keep the mapping array correctly
    indexed.

    set_context() is called once per group before append_from_accessor().  It
    is called twice per group when a retraction is needed — first with
    use_old_val=True (emit the old value to retract it), then with
    use_old_val=False (emit the new value to insert it).
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

        # Detect whether the output uses a natural PK (mirrors the condition in
        # _build_reduce_output_schema so the two are always in sync).
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
            mapping[0] = group_indices[0]  # skipped by serialize_row (pk)
            mapping[1] = -1  # agg result
        else:
            mapping[0] = -2  # synthetic hash PK, skipped by serialize_row
            for i in range(len(group_indices)):
                mapping[i + 1] = group_indices[i]
            mapping[num_out - 1] = -1  # agg result

        self.mapping_to_input = mapping

        # Mutable context, refreshed per group via set_context().
        self.exemplar = None
        self.agg_func = None
        self.old_val_bits = r_uint64(0)
        self.use_old_val = False

    def set_context(self, exemplar, agg_func, old_val_bits, use_old_val):
        """
        Binds the accessor to a specific group and emission mode.
        """
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


@jit.unroll_safe
def _compare_by_cols(accessor_a, accessor_b, schema, col_indices):
    """
    Compares two rows on the specified subset of schema columns.
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
    Returns a permutation of [0, count) sorted by the group-by columns.
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


def op_reduce(
    reg_in,
    reg_trace_in,
    reg_trace_out,
    reg_out,
    group_by_cols,
    agg_func,
    output_schema,
):
    """
    Incremental DBSP REDUCE: δout = Agg(I + δin) - Agg(I).
    """
    assert isinstance(reg_in, runtime.DeltaRegister)
    delta_in = reg_in.batch
    if delta_in.length() == 0:
        return

    assert isinstance(reg_out, runtime.DeltaRegister)
    out_batch = reg_out.batch
    out_batch.clear()

    input_schema = reg_in.vm_schema.table_schema

    if not delta_in.is_sorted():
        delta_in.sort()
    delta_in.consolidate()

    # Capture record count AFTER consolidation.
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

        trace_out_cursor = reg_trace_out.cursor
        trace_out_cursor.seek(group_key)

        has_old = False
        old_val_bits = r_uint64(0)
        if trace_out_cursor.is_valid() and trace_out_cursor.key() == group_key:
            has_old = True
            old_row_accessor = trace_out_cursor.get_accessor()
            agg_col_idx = len(output_schema.columns) - 1
            old_val_bits = old_row_accessor.get_int(agg_col_idx)

            reduce_acc.set_context(acc_exemplar, agg_func, old_val_bits, True)
            out_batch.append_from_accessor(group_key, r_int64(-1), reduce_acc)

        if agg_func.is_linear() and has_old:
            agg_func.merge_accumulated(old_val_bits, r_int64(1))
        elif not agg_func.is_linear():
            agg_func.reset()
            trace_in = reg_trace_in.cursor
            trace_in.seek(group_key)
            while trace_in.is_valid() and trace_in.key() == group_key:
                agg_func.step(trace_in.get_accessor(), trace_in.weight())
                trace_in.advance()
            for k in range(group_start_pos, idx):
                d_idx = sorted_indices[k]
                acc_in.set_row(delta_in.get_row(d_idx))
                agg_func.step(acc_in, delta_in.get_weight(d_idx))

        if not agg_func.is_accumulator_zero():
            reduce_acc.set_context(acc_exemplar, agg_func, r_uint64(0), False)
            out_batch.append_from_accessor(group_key, r_int64(1), reduce_acc)

    out_batch.sort()
    out_batch.consolidate()
