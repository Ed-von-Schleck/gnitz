# gnitz/vm/ops/reduce.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.core import types, values, strings, xxh
from gnitz.core.values import make_payload_row
from gnitz.core.comparator import PayloadRowAccessor
from gnitz.vm import runtime


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
            res = accessor.get_str_struct(c_idx)
            s = strings.resolve_string(res[2], res[3], res[4])
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
            res = input_accessor.get_str_struct(c_idx)
            s = strings.resolve_string(res[2], res[3], res[4])
            output_row.append_string(s)
        elif t == types.TYPE_F64.code or t == types.TYPE_F32.code:
            output_row.append_float(input_accessor.get_float(c_idx))
        elif t == types.TYPE_U128.code:
            v = input_accessor.get_u128(c_idx)
            output_row.append_u128(r_uint64(v), r_uint64(v >> 64))
        else:
            output_row.append_int(r_int64(intmask(input_accessor.get_int(c_idx))))


def op_reduce(
    reg_in, reg_trace_in, reg_trace_out, reg_out, 
    group_by_cols, agg_func, output_schema
):
    """
    Incremental DBSP REDUCE: δout = Agg(I + δin) - Agg(I).
    
    This operator implements the incremental aggregation logic by:
    1. Grouping incoming deltas by the specified columns.
    2. Looking up the current aggregate state (retraction).
    3. Computing the updated aggregate state (insertion).
    """
    assert isinstance(reg_in, runtime.DeltaRegister)
    delta_in = reg_in.batch
    if delta_in.length() == 0:
        return

    assert isinstance(reg_out, runtime.DeltaRegister)
    out_batch = reg_out.batch
    out_batch.clear()

    input_schema = reg_in.vm_schema.table_schema
    
    # 1. First, ensure the input delta is consolidated so we have net 
    # weights per (PK, Payload) before we group by the aggregate columns.
    if not delta_in.is_sorted():
        delta_in.sort()
    delta_in.consolidate()

    # 2. Sort the delta by group columns to process group boundaries
    sorted_indices = _argsort_delta(delta_in, input_schema, group_by_cols)
    
    acc_in = PayloadRowAccessor(input_schema)
    acc_exemplar = PayloadRowAccessor(input_schema)
    
    idx = 0
    n = delta_in.length()
    while idx < n:
        # Capture the sorted position and batch index of the group start
        group_start_pos = idx
        group_start_idx = sorted_indices[group_start_pos]
        
        # Set the exemplar accessor to the start of this group
        acc_exemplar.set_row(delta_in.get_row(group_start_idx))
        group_key = _extract_group_key(acc_exemplar, input_schema, group_by_cols)

        # 3. Reset aggregator and fold the Delta portion for this group
        agg_func.reset()
        while idx < n:
            curr_idx = sorted_indices[idx]
            acc_in.set_row(delta_in.get_row(curr_idx))
            
            # Check for group boundary by comparing current to group exemplar
            if _compare_by_cols(acc_in, acc_exemplar, input_schema, group_by_cols) != 0:
                break
            
            agg_func.step(acc_in, delta_in.get_weight(curr_idx))
            idx += 1
        
        # 4. Handle Retraction: Emit -Agg(I) if history exists in output trace
        # reg_trace_out contains the current aggregates.
        trace_out_cursor = reg_trace_out.cursor
        trace_out_cursor.seek(group_key)
        
        has_old = False
        if trace_out_cursor.is_valid() and trace_out_cursor.key() == group_key:
            has_old = True
            old_row_accessor = trace_out_cursor.get_accessor()
            
            retract_row = make_payload_row(output_schema)
            _copy_group_cols(acc_exemplar, input_schema, retract_row, group_by_cols)
            
            # Agg result is the last column in the output schema
            agg_col_idx = len(output_schema.columns) - 1
            old_val_bits = old_row_accessor.get_int(agg_col_idx)
            retract_row.append_int(r_int64(intmask(old_val_bits)))
            
            out_batch.append(group_key, r_int64(-1), retract_row)

        # 5. Compute New Aggregate: Agg(I + δin)
        if agg_func.is_linear() and has_old:
            # OPTIMIZATION: Agg(Old + Delta) = Agg(Old) + Agg(Delta)
            # old_val_bits was already fetched in step 4
            agg_func.merge_accumulated(old_val_bits, r_int64(1))
            
        elif not agg_func.is_linear():
            # NON-LINEAR (e.g., MIN/MAX): Must rescan full history for this key.
            # reg_trace_in contains the multiset history of the input stream.
            agg_func.reset()
            trace_in = reg_trace_in.cursor
            trace_in.seek(group_key)
            while trace_in.is_valid() and trace_in.key() == group_key:
                agg_func.step(trace_in.get_accessor(), trace_in.weight())
                trace_in.advance()
                
            # Now apply the current delta on top of that history
            for k in range(group_start_pos, idx):
                d_idx = sorted_indices[k]
                acc_in.set_row(delta_in.get_row(d_idx))
                agg_func.step(acc_in, delta_in.get_weight(d_idx))

        # 6. Emit Insertion (+1) of the updated aggregate
        if not agg_func.is_accumulator_zero():
            insert_row = make_payload_row(output_schema)
            _copy_group_cols(acc_exemplar, input_schema, insert_row, group_by_cols)
            
            agg_func.emit(insert_row)
            out_batch.append(group_key, r_int64(1), insert_row)

    # Consolidate the output to remove any internal retractions/insertions 
    # that might have cancelled each other out.
    out_batch.sort()
    out_batch.consolidate()
