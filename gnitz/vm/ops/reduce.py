# gnitz/vm/ops/reduce.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.core import types, values, strings, xxh
from gnitz.core.values import make_payload_row
from gnitz.core.comparator import PayloadRowAccessor
from gnitz.vm import instructions, runtime


@jit.unroll_safe
def _compare_by_cols(accessor_a, accessor_b, schema, col_indices):
    """
    Compares two records across a specific subset of columns.
    Used for group-boundary detection in the sorted delta batch.
    """
    for i in range(len(col_indices)):
        col_idx = col_indices[i]
        col_type = schema.columns[col_idx].field_type.code

        if col_type == types.TYPE_STRING.code:
            l_len, l_pref, l_ptr, l_heap, l_str = accessor_a.get_str_struct(col_idx)
            r_len, r_pref, r_ptr, r_heap, r_str = accessor_b.get_str_struct(col_idx)
            res = strings.compare_structures(
                l_len, l_pref, l_ptr, l_heap, l_str,
                r_len, r_pref, r_ptr, r_heap, r_str
            )
            if res != 0:
                return res
        elif col_type == types.TYPE_F64.code or col_type == types.TYPE_F32.code:
            va, vb = accessor_a.get_float(col_idx), accessor_b.get_float(col_idx)
            if va < vb: return -1
            if va > vb: return 1
        elif col_type == types.TYPE_U128.code:
            va, vb = accessor_a.get_u128(col_idx), accessor_b.get_u128(col_idx)
            if va < vb: return -1
            if va > vb: return 1
        else:
            # All integer types
            va, vb = accessor_a.get_int(col_idx), accessor_b.get_int(col_idx)
            if va < vb: return -1
            if va > vb: return 1
    return 0


def _argsort_delta(batch, schema, col_indices):
    """
    Returns an index array for the delta batch sorted by grouping columns.
    Standard insertion sort is used as VM batches are typically small.
    """
    count = batch.length()
    indices = [i for i in range(count)]
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
                indices[j], indices[j - 1] = indices[j - 1], indices[j]
                j -= 1
            else:
                break
    return indices


@jit.unroll_safe
def _extract_group_key(accessor, schema, col_indices):
    """
    Derives the Primary Key for the aggregate record.
    If grouping by a single U64/U128, it is used directly.
    Otherwise, the group columns are hashed to a U128 synthetic PK.
    """
    if len(col_indices) == 1:
        c_idx = col_indices[0]
        t = schema.columns[c_idx].field_type.code
        if t == types.TYPE_U64.code:
            return r_uint128(accessor.get_int(c_idx))
        if t == types.TYPE_U128.code:
            return accessor.get_u128(c_idx)

    # Composite or Non-Integer Key: Mix into 128-bit hash
    h_lo = r_uint64(0)
    h_hi = r_uint64(0)
    for i in range(len(col_indices)):
        c_idx = col_indices[i]
        t = schema.columns[c_idx].field_type.code
        if t == types.TYPE_STRING.code:
            _, _, _, _, s = accessor.get_str_struct(c_idx)
            h_lo ^= xxh.compute_checksum_bytes(s)
        else:
            h_lo ^= accessor.get_int(c_idx)
        # Simple mixing for hi-word
        h_hi = (h_hi << 1) | (h_lo >> 63)
    
    return (r_uint128(h_hi) << 64) | r_uint128(h_lo)


@jit.unroll_safe
def _copy_group_cols(input_accessor, input_schema, output_row, col_indices):
    """Copies grouping values from the input record to the output payload."""
    for i in range(len(col_indices)):
        c_idx = col_indices[i]
        t = input_schema.columns[c_idx].field_type.code
        if t == types.TYPE_STRING.code:
            _, _, _, _, s = input_accessor.get_str_struct(c_idx)
            output_row.append_string(s)
        elif t == types.TYPE_F64.code or t == types.TYPE_F32.code:
            output_row.append_float(input_accessor.get_float(c_idx))
        elif t == types.TYPE_U128.code:
            v = input_accessor.get_u128(c_idx)
            output_row.append_u128(r_uint64(v), r_uint64(v >> 64))
        else:
            output_row.append_int(r_int64(intmask(input_accessor.get_int(c_idx))))


def op_reduce(instr):
    """
    Incremental DBSP REDUCE: δout = Agg(I + δin) - Agg(I).
    """
    assert isinstance(instr, instructions.ReduceOp)
    
    reg_in = instr.reg_in
    assert isinstance(reg_in, runtime.DeltaRegister)
    delta_in = reg_in.batch
    if delta_in.length() == 0:
        return

    reg_out = instr.reg_out
    assert isinstance(reg_out, runtime.DeltaRegister)
    out_batch = reg_out.batch
    out_batch.clear()

    input_schema = instr.reg_in.vm_schema.table_schema
    output_schema = instr.output_schema
    agg_func = instr.agg_func
    group_cols = instr.group_by_cols

    # 1. Sort the delta by group columns
    sorted_indices = _argsort_delta(delta_in, input_schema, group_cols)
    
    acc_in = PayloadRowAccessor(input_schema)
    acc_trace_out = PayloadRowAccessor(output_schema)
    
    idx = 0
    n = delta_in.length()
    while idx < n:
        group_start_idx = sorted_indices[idx]
        acc_in.set_row(delta_in.get_row(group_start_idx))
        group_key = _extract_group_key(acc_in, input_schema, group_cols)

        # 2. Aggregate the Delta portion for this group
        agg_func.reset()
        while idx < n:
            curr_idx = sorted_indices[idx]
            acc_in.set_row(delta_in.get_row(curr_idx))
            
            # Check for group boundary
            acc_boundary = PayloadRowAccessor(input_schema)
            acc_boundary.set_row(delta_in.get_row(group_start_idx))
            if _compare_by_cols(acc_in, acc_boundary, input_schema, group_cols) != 0:
                break
            
            agg_func.step(acc_in, delta_in.get_weight(curr_idx))
            idx += 1
        
        # 3. Handle Retraction: Emit -Agg(I)
        # We look up the existing aggregate in the output trace
        instr.reg_trace_out.cursor.seek(group_key)
        has_old = False
        if instr.reg_trace_out.cursor.is_valid() and instr.reg_trace_out.cursor.key() == group_key:
            has_old = True
            old_row_accessor = instr.reg_trace_out.cursor.get_accessor()
            
            # Emit retraction (-1)
            # The output row structure for REDUCE is: [PK: GroupKey] + [Group Cols] + [Agg Result]
            # However, if GroupKey is natural, it's just [PK] + [Agg Result].
            retract_row = make_payload_row(output_schema)
            # We copy group columns from the input record (it's the same group)
            acc_in.set_row(delta_in.get_row(group_start_idx))
            _copy_group_cols(acc_in, input_schema, retract_row, group_cols)
            
            # Copy the old aggregate result from the trace accessor
            agg_col_idx = len(output_schema.columns) - 1
            # Note: Aggregate result is assumed to be numeric or count
            old_val = r_int64(intmask(old_row_accessor.get_int(agg_col_idx)))
            retract_row.append_int(old_val)
            
            out_batch.append(group_key, r_int64(-1), retract_row)

        # 4. Compute New Aggregate: Agg(I + δin)
        if agg_func.is_linear() and has_old:
            # For linear aggs, we add the delta to the previous total
            # Load old value back into aggregate function
            old_row_accessor = instr.reg_trace_out.cursor.get_accessor()
            agg_func.step(old_row_accessor, r_int64(1))
        elif not agg_func.is_linear():
            # Non-linear (e.g., MIN/MAX): Must rescan historical input trace
            agg_func.reset()
            trace_in = instr.reg_trace_in.cursor
            trace_in.seek(group_key)
            while trace_in.is_valid() and trace_in.key() == group_key:
                agg_func.step(trace_in.get_accessor(), trace_in.weight())
                trace_in.advance()
            # Then apply the delta sum we computed earlier
            # (Note: This requires agg_func to track the delta sum internally or re-scan delta)
            # Simplification: we re-apply the delta records here
            for k in range(group_start_idx, idx):
                d_idx = sorted_indices[k]
                acc_in.set_row(delta_in.get_row(d_idx))
                agg_func.step(acc_in, delta_in.get_weight(d_idx))

        # 5. Emit Insertion (+1)
        if not agg_func.is_accumulator_zero(): # Check if result is identity/zero
            insert_row = make_payload_row(output_schema)
            acc_in.set_row(delta_in.get_row(group_start_idx))
            _copy_group_cols(acc_in, input_schema, insert_row, group_cols)
            agg_func.emit(insert_row)
            out_batch.append(group_key, r_int64(1), insert_row)

    out_batch.sort()
    out_batch.consolidate()
