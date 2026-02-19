# gnitz/vm/ops.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, values, row_logic, strings
from gnitz.core.row_logic import make_payload_row

# -----------------------------------------------------------------------------
# High-Performance Comparison Kernels
# -----------------------------------------------------------------------------

@jit.unroll_safe
def _compare_accessors(acc_a, acc_b, schema):
    """
    Directly compares two RowAccessors without materializing TaggedValues.
    Optimized for the RPython JIT to fold column offsets into immediates.
    
    Returns 0 if equal, non-zero otherwise.
    """
    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue

        col_def = schema.columns[i]
        t = col_def.field_type.code

        # Numeric Comparisons (Fast Path)
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

        # German String Comparison (Fast Failure Path)
        elif t == types.TYPE_STRING.code:
            meta_a = acc_a.get_str_struct(i) # (len, prefix, ptr, blob_ptr, s)
            meta_b = acc_b.get_str_struct(i)
            
            # O(1) Length check
            if meta_a[0] != meta_b[0]:
                return -1
            # O(1) Prefix check
            if meta_a[1] != meta_b[1]:
                return -1
            
            # Full check only for long strings (> 12 bytes)
            if meta_a[0] > 12:
                s_a = meta_a[4]
                s_b = meta_b[4]
                
                if s_a is not None and s_b is not None:
                    if s_a != s_b: return -1
                else:
                    # FIX: Change index 0 (length) to index 3 (heap pointer)
                    val_a = s_a if s_a is not None else strings.unpack_string(meta_a[2], meta_a[3])
                    val_b = s_b if s_b is not None else strings.unpack_string(meta_b[2], meta_b[3])
                    if val_a != val_b:
                        return -1
        else:
            # Fallback for unknown types or NULLs
            pass
            
    return 0

# -----------------------------------------------------------------------------
# Row Materialization (Used for Join materialization or Sinks)
# -----------------------------------------------------------------------------

@jit.unroll_safe
def materialize_row(accessor, schema):
    """
    Extracts a full row from a RowAccessor and boxes it into a list of TaggedValues.
    Should only be called when a record is confirmed to survive (w_net != 0).
    """
    n_payload_cols = len(schema.columns) - 1
    result = make_payload_row(n_payload_cols)

    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue

        col_def = schema.columns[i]
        type_code = col_def.field_type.code

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
                # FIX: Change index 0 (length) to index 3 (heap pointer)
                s_val = strings.unpack_string(meta[2], meta[3])
            result.append(values.TaggedValue.make_string(s_val))

        elif type_code == types.TYPE_U128.code:
            val_u128 = accessor.get_u128(i)
            lo = r_uint64(val_u128)
            hi = r_uint64(val_u128 >> 64)
            result.append(values.TaggedValue.make_u128(lo, hi))
        else:
            result.append(values.TaggedValue.make_null())

    return result

def _concat_payloads(left, right):
    n = len(left) + len(right)
    result = make_payload_row(n)
    for k in range(len(left)):
        result.append(left[k])
    for k in range(len(right)):
        result.append(right[k])
    return result

# -----------------------------------------------------------------------------
# Operators
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
    # Logic note: This implementation requires a stateful register to be DBSP-complete.
    # Currently acts as a simple pass-through/copy.
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()
    for i in range(input_batch.row_count()):
        output_batch.append(input_batch.get_key(i), input_batch.get_weight(i), input_batch.get_payload(i))

# -----------------------------------------------------------------------------
# Bilinear / Join
# -----------------------------------------------------------------------------

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
                # Materialize trace side only on confirmed match
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
            # Range finding for A
            start_a = idx_a
            while idx_a < count_a and batch_a.get_key(idx_a) == match_key: idx_a += 1
            # Range finding for B
            start_b = idx_b
            while idx_b < count_b and batch_b.get_key(idx_b) == match_key: idx_b += 1

            for i in range(start_a, idx_a):
                for j in range(start_b, idx_b):
                    w_out = batch_a.get_weight(i) * batch_b.get_weight(j)
                    if w_out != 0:
                        output_batch.append(match_key, w_out, _concat_payloads(batch_a.get_payload(i), batch_b.get_payload(j)))

# -----------------------------------------------------------------------------
# Incremental Distinct
# -----------------------------------------------------------------------------

def op_distinct(reg_in, reg_history, reg_out):
    """
    Incremental Distinct operator.
    δ(Distinct(I)) = sign(T + d) - sign(T)
    where T is current weight and d is incoming delta weight.
    """
    delta_batch = reg_in.batch
    history = reg_history
    out_batch = reg_out.batch

    for i in range(delta_batch.row_count()):
        key = delta_batch.get_key(i)
        d_weight = delta_batch.get_weight(i)
        payload = delta_batch.get_payload(i)

        # 1. Get current accumulated weight from history using Key + Payload
        t_weight = history.get_weight(key, payload)
        new_weight = t_weight + d_weight

        # 2. sign(x) is 1 if x > 0 else 0
        old_sign = 1 if t_weight > 0 else 0
        new_sign = 1 if new_weight > 0 else 0
        
        out_weight = new_sign - old_sign

        if out_weight != 0:
            out_batch.append(key, out_weight, payload)
        
        # 3. Update history for the next record or epoch
        history.update_weight(key, d_weight, payload)

def op_distinct_stateless(reg_in, reg_out):
    """
    Non-incremental version for simple batch processing.
    Simply squashes all positive weights to 1.
    """
    delta_batch = reg_in.batch
    out_batch = reg_out.batch
    for i in range(delta_batch.row_count()):
        key = delta_batch.get_key(i)
        w = delta_batch.get_weight(i)
        if w > 0:
            out_batch.append(key, 1, delta_batch.get_payload(i))

# -----------------------------------------------------------------------------
# Sinks
# -----------------------------------------------------------------------------

def op_integrate(reg_in, target_engine):
    input_batch = reg_in.batch
    for i in range(input_batch.row_count()):
        w = input_batch.get_weight(i)
        if w != 0:
            target_engine.ingest(input_batch.get_key(i), w, input_batch.get_payload(i))
