# gnitz/vm/ops.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_uint, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, values, row_logic, strings, xxh
from gnitz.core.row_logic import make_payload_row


# -----------------------------------------------------------------------------
# High-Performance Comparison Kernels
# -----------------------------------------------------------------------------

@jit.unroll_safe
def _compare_accessors(acc_a, acc_b, schema):
    """Directly compares two RowAccessors without materializing TaggedValues."""
    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue

        col_def = schema.columns[i]
        t = col_def.field_type.code

        if (t == types.TYPE_I64.code or t == types.TYPE_U64.code or 
            t == types.TYPE_I32.code or t == types.TYPE_U32.code or
            t == types.TYPE_I16.code or t == types.TYPE_I16.code or
            t == types.TYPE_I8.code or t == types.TYPE_U8.code):
            if acc_a.get_int(i) != acc_b.get_int(i):
                return -1
        
        elif t == types.TYPE_F64.code or t == types.TYPE_F32.code:
            if acc_a.get_float(i) != acc_b.get_float(i):
                return -1

        elif t == types.TYPE_U128.code:
            if acc_a.get_u128(i) != acc_b.get_u128(i):
                return -1

        elif t == types.TYPE_STRING.code:
            meta_a = acc_a.get_str_struct(i)
            meta_b = acc_b.get_str_struct(i)
            if meta_a[0] != meta_b[0] or meta_a[1] != meta_b[1]:
                return -1
            if meta_a[0] > 12:
                val_a = meta_a[4] if meta_a[4] is not None else strings.unpack_string(meta_a[2], meta_a[3])
                val_b = meta_b[4] if meta_b[4] is not None else strings.unpack_string(meta_b[2], meta_b[3])
                if val_a != val_b:
                    return -1
    return 0

@jit.unroll_safe
def _compare_by_cols(batch, idx_a, idx_b, col_indices):
    """Compare two rows in a batch only on the specified column subset."""
    acc_a = batch.left_acc
    acc_b = batch.right_acc # Re-using secondary accessor for comparison
    acc_a.set_row(batch, idx_a)
    acc_b.set_row(batch, idx_b)
    
    for i in range(len(col_indices)):
        col_idx = col_indices[i]
        t = batch.schema.columns[col_idx].field_type.code
        
        # We perform a basic equality/ordering check
        if t == types.TYPE_I64.code or t == types.TYPE_U64.code:
            va, vb = acc_a.get_int(col_idx), acc_b.get_int(col_idx)
            if va < vb: return -1
            if va > vb: return 1
        elif t == types.TYPE_STRING.code:
            # For strings in group-by, we just use the logic from row_logic/strings
            meta_a = acc_a.get_str_struct(col_idx)
            meta_b = acc_b.get_str_struct(col_idx)
            # Use unified comparison kernel
            res = strings.compare_structures(
                meta_a[0], meta_a[1], meta_a[2], meta_a[3], meta_a[4],
                meta_b[0], meta_b[1], meta_b[2], meta_b[3], meta_b[4]
            )
            if res != 0: return res
        # ... other types as needed ...
    return 0

# -----------------------------------------------------------------------------
# Reduce Helpers: Key Hashing and Sorting
# -----------------------------------------------------------------------------

def _argsort_by_cols(batch, col_indices):
    """Returns a list of indices [0..N-1] sorted by group_by columns."""
    count = batch.row_count()
    indices = [i for i in range(count)]
    if count <= 1:
        return indices

    # Simple insertion sort for typically small batch sizes
    for i in range(1, count):
        j = i
        while j > 0 and _compare_by_cols(batch, indices[j], indices[j-1], col_indices) < 0:
            tmp = indices[j]
            indices[j] = indices[j-1]
            indices[j-1] = tmp
            j -= 1
    return indices

@jit.unroll_safe
def _extract_group_key_u128(batch, row_idx, col_indices):
    """
    Maps the grouping columns to a u128 Primary Key.
    Uses XXHash for composite/string keys; passes through for natural PKs.
    """
    if len(col_indices) == 1:
        col_idx = col_indices[0]
        t = batch.schema.columns[col_idx].field_type.code
        acc = batch.left_acc
        acc.set_row(batch, row_idx)
        if t == types.TYPE_U64.code:
            return r_uint128(acc.get_int(col_idx))
        if t == types.TYPE_U128.code:
            return acc.get_u128(col_idx)

    # Composite or Non-Int Key: Use XXHash
    # We mix the values of the columns into a single hash
    h_acc = r_uint64(0)
    acc = batch.left_acc
    acc.set_row(batch, row_idx)
    
    for i in range(len(col_indices)):
        c_idx = col_indices[i]
        t = batch.schema.columns[c_idx].field_type.code
        # Simplified mixing: in real impl, we'd hash the actual bytes
        if t == types.TYPE_STRING.code:
            s = acc.get_str_struct(c_idx)[4]
            if s is not None:
                h_acc ^= xxh.compute_checksum_bytes(s)
        else:
            h_acc ^= acc.get_int(c_idx) # Basic XOR-mix for ints
            
    return r_uint128(h_acc)

# -----------------------------------------------------------------------------
# Core Operators
# -----------------------------------------------------------------------------

def op_reduce(op, reg_file):
    """
    Incremental Group-By and Aggregate.
    δout = Agg(I + δin) - Agg(I)
    """
    delta_in = reg_file.get_register(op.reg_in).batch
    trace_out = reg_file.get_register(op.reg_trace_out).cursor
    out_batch = reg_file.get_register(op.reg_out).batch
    out_batch.clear()

    n = delta_in.row_count()
    if n == 0:
        return

    agg_func = op.agg_func
    group_cols = op.group_by_cols
    
    # 1. Sort the incoming delta so groups are contiguous
    sorted_indices = _argsort_by_cols(delta_in, group_cols)

    # 2. Iterate through delta groups
    idx = 0
    while idx < n:
        group_start = idx
        group_key_u128 = _extract_group_key_u128(delta_in, sorted_indices[idx], group_cols)
        
        # Calculate aggregate for the delta portion
        delta_acc = agg_func.create_accumulator()
        while idx < n:
            current_idx = sorted_indices[idx]
            if _compare_by_cols(delta_in, sorted_indices[group_start], current_idx, group_cols) != 0:
                break
            
            w = delta_in.get_weight(current_idx)
            if w != 0:
                delta_in.left_acc.set_row(delta_in, current_idx)
                delta_acc = agg_func.step(delta_acc, delta_in.left_acc, w)
            idx += 1
        
        # 3. Retraction Logic: Look up old value in trace_out
        trace_out.seek(group_key_u128)
        old_val_tv = values.TaggedValue.make_int(0)
        has_old = False
        
        if trace_out.is_valid() and trace_out.key() == group_key_u128:
            # Existing group found. Emit retraction (-1)
            has_old = True
            acc_out = trace_out.get_accessor()
            # The agg value is the last column in the trace schema (len - 1)
            agg_col_in_trace = len(op.output_schema.columns) - 1
            old_val_tv = values.TaggedValue.make_int(acc_out.get_int(agg_col_in_trace))
            
            if not agg_func.is_accumulator_zero(old_val_tv):
                # Emit retraction of the old value
                payload = make_payload_row(len(op.output_schema.columns) - 1)
                # Fill group columns + old agg value
                # (Simplified: we use the row from delta_in as a template)
                delta_in.left_acc.set_row(delta_in, sorted_indices[group_start])
                retract_row = materialize_row(delta_in.left_acc, op.output_schema)
                # Replace the last column with the old value
                retract_row[len(retract_row)-1] = old_val_tv
                out_batch.append(group_key_u128, -1, retract_row)

        # 4. Compute New Aggregate
        if agg_func.is_linear():
            # Linear optimization: new = old + delta
            if has_old:
                new_acc = agg_func.step(old_val_tv, delta_in.left_acc, 0) # setup
                new_acc = values.TaggedValue.make_int(old_val_tv.i64 + delta_acc.i64)
            else:
                new_acc = delta_acc
        else:
            # Non-linear (MIN/MAX): Must scan trace_in
            trace_in = reg_file.get_register(op.reg_trace_in).cursor
            # Note: For Phase 2, we assume group_cols matches trace_in PK
            trace_in.seek(group_key_u128)
            new_acc = agg_func.create_accumulator()
            while trace_in.is_valid() and trace_in.key() == group_key_u128:
                new_acc = agg_func.step(new_acc, trace_in.get_accessor(), trace_in.weight())
                trace_in.advance()

        # 5. Emit New Value
        if not agg_func.is_accumulator_zero(new_acc):
            delta_in.left_acc.set_row(delta_in, sorted_indices[group_start])
            insert_row = materialize_row(delta_in.left_acc, op.output_schema)
            insert_row[len(insert_row)-1] = new_acc
            out_batch.append(group_key_u128, 1, insert_row)

    # Output MUST be sorted and consolidated for the subsequent INTEGRATE
    out_batch.sort()
    out_batch.consolidate()


# -----------------------------------------------------------------------------
# Existing Operators (Filter, Map, Negate, etc.)
# -----------------------------------------------------------------------------

def op_filter(reg_in, reg_out, func):
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()

    count = input_batch.row_count()
    accessor = input_batch.left_acc

    for i in range(count):
        accessor.set_row(input_batch, i)
        if func.evaluate_predicate(accessor):
            output_batch.append(input_batch.get_key(i), input_batch.get_weight(i), input_batch.get_payload(i))

def op_map(reg_in, reg_out, func):
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()

    count = input_batch.row_count()
    accessor = input_batch.left_acc
    n_out_cols = len(reg_out.batch.schema.columns) - 1

    for i in range(count):
        accessor.set_row(input_batch, i)
        new_payload = make_payload_row(n_out_cols)
        func.evaluate_map(accessor, new_payload)
        output_batch.append(input_batch.get_key(i), input_batch.get_weight(i), new_payload)

def op_negate(reg_in, reg_out):
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()
    for i in range(input_batch.row_count()):
        output_batch.append(input_batch.get_key(i), -input_batch.get_weight(i), input_batch.get_payload(i))

def op_union(reg_in_a, reg_in_b, reg_out):
    output_batch = reg_out.batch
    output_batch.clear()
    batch_a = reg_in_a.batch
    for i in range(batch_a.row_count()):
        output_batch.append(batch_a.get_key(i), batch_a.get_weight(i), batch_a.get_payload(i))
    if reg_in_b is not None:
        batch_b = reg_in_b.batch
        for i in range(batch_b.row_count()):
            output_batch.append(batch_b.get_key(i), batch_b.get_weight(i), batch_b.get_payload(i))

def op_delay(reg_in, reg_out):
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()
    for i in range(input_batch.row_count()):
        output_batch.append(input_batch.get_key(i), input_batch.get_weight(i), input_batch.get_payload(i))

def op_join_delta_trace(reg_delta, reg_trace, reg_out):
    delta_batch = reg_delta.batch
    trace_cursor = reg_trace.cursor
    output_batch = reg_out.batch
    output_batch.clear()

    delta_batch.sort()
    count = delta_batch.row_count()
    if count == 0: return

    for i in range(count):
        d_key = delta_batch.get_key(i)
        d_weight = delta_batch.get_weight(i)
        if d_weight == 0: continue

        trace_cursor.seek(d_key)
        while trace_cursor.is_valid():
            if trace_cursor.key() != d_key: break
            
            t_weight = trace_cursor.weight()
            final_weight = d_weight * t_weight
            
            if final_weight != 0:
                accessor = trace_cursor.get_accessor()
                t_payload = materialize_row(accessor, reg_trace.vm_schema.table_schema)
                d_payload = delta_batch.get_payload(i)
                output_batch.append(d_key, final_weight, _concat_payloads(d_payload, t_payload))

            trace_cursor.advance()

def op_join_delta_delta(reg_a, reg_b, reg_out):
    batch_a = reg_a.batch
    batch_b = reg_b.batch
    output_batch = reg_out.batch
    output_batch.clear()

    batch_a.sort()
    batch_b.sort()

    idx_a = 0
    idx_b = 0
    count_a = batch_a.row_count()
    count_b = batch_b.row_count()

    while idx_a < count_a and idx_b < count_b:
        key_a = batch_a.get_key(idx_a)
        key_b = batch_b.get_key(idx_b)

        if key_a < key_b: idx_a += 1
        elif key_b < key_a: idx_b += 1
        else:
            match_key = key_a
            start_a = idx_a
            while idx_a < count_a and batch_a.get_key(idx_a) == match_key: idx_a += 1
            start_b = idx_b
            while idx_b < count_b and batch_b.get_key(idx_b) == match_key: idx_b += 1

            for i in range(start_a, idx_a):
                for j in range(start_b, idx_b):
                    w_out = batch_a.get_weight(i) * batch_b.get_weight(j)
                    if w_out != 0:
                        output_batch.append(match_key, w_out, _concat_payloads(batch_a.get_payload(i), batch_b.get_payload(j)))

def op_distinct(reg_in, reg_history, reg_out):
    delta_batch = reg_in.batch
    history = reg_history
    out_batch = reg_out.batch

    for i in range(delta_batch.row_count()):
        key = delta_batch.get_key(i)
        d_weight = delta_batch.get_weight(i)
        payload = delta_batch.get_payload(i)
        t_weight = history.get_weight(key, payload)
        new_weight = t_weight + d_weight
        old_sign = 1 if t_weight > 0 else 0
        new_sign = 1 if new_weight > 0 else 0
        out_weight = new_sign - old_sign
        if out_weight != 0:
            out_batch.append(key, out_weight, payload)
        history.update_weight(key, d_weight, payload)

def op_integrate(reg_in, target_engine):
    input_batch = reg_in.batch
    for i in range(input_batch.row_count()):
        w = input_batch.get_weight(i)
        if w != 0:
            target_engine.ingest(input_batch.get_key(i), w, input_batch.get_payload(i))

def _concat_payloads(left, right):
    n = len(left) + len(right)
    result = make_payload_row(n)
    for k in range(len(left)):
        result.append(left[k])
    for k in range(len(right)):
        result.append(right[k])
    return result

@jit.unroll_safe
def materialize_row(accessor, schema):
    n_payload_cols = len(schema.columns) - 1
    result = make_payload_row(n_payload_cols)
    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue
        type_code = schema.columns[i].field_type.code
        if (type_code == types.TYPE_I64.code or type_code == types.TYPE_U64.code or 
            type_code == types.TYPE_I32.code or type_code == types.TYPE_U32.code or
            type_code == types.TYPE_I16.code or type_code == types.TYPE_U16.code or
            type_code == types.TYPE_I8.code or type_code == types.TYPE_U8.code):
            result.append(values.TaggedValue.make_int(accessor.get_int(i)))
        elif type_code == types.TYPE_F64.code or type_code == types.TYPE_F32.code:
            result.append(values.TaggedValue.make_float(accessor.get_float(i)))
        elif type_code == types.TYPE_STRING.code:
            meta = accessor.get_str_struct(i)
            s_val = meta[4]
            if s_val is None:
                s_val = strings.unpack_string(meta[2], meta[3])
            result.append(values.TaggedValue.make_string(s_val))
        elif type_code == types.TYPE_U128.code:
            val_u128 = accessor.get_u128(i)
            result.append(values.TaggedValue.make_u128(r_uint64(val_u128), r_uint64(val_u128 >> 64)))
        else:
            result.append(values.TaggedValue.make_null())
    return result
