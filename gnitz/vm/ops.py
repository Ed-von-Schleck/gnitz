from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, values, row_logic, strings
from gnitz.vm import batch

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

@jit.unroll_safe
def materialize_row(accessor, schema):
    """
    Extracts a full row from a RowAccessor and boxes it into a list of TaggedValues.
    Used when moving data from a Cursor (Storage) to a Batch (VM).
    
    Args:
        accessor: row_logic.BaseRowAccessor (Cursor or BatchAccessor)
        schema: TableSchema
    
    Returns:
        List[TaggedValue] containing non-PK columns.
    """
    result = []
    # Columns in accessor are indexed physically.
    # We must skip the PK column if it's stored in the payload part, 
    # but BaseRowAccessor usually abstracts purely based on column index.
    # The TableSchema defines the columns.
    
    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue
            
        col_def = schema.columns[i]
        type_code = col_def.field_type.code
        
        if type_code == types.TYPE_I64.code or \
           type_code == types.TYPE_U64.code or \
           type_code == types.TYPE_I32.code or \
           type_code == types.TYPE_U32.code or \
           type_code == types.TYPE_I16.code or \
           type_code == types.TYPE_U16.code or \
           type_code == types.TYPE_I8.code or \
           type_code == types.TYPE_U8.code:
            val = accessor.get_int(i)
            result.append(values.TaggedValue.make_int(r_int64(val)))
            
        elif type_code == types.TYPE_F64.code or \
             type_code == types.TYPE_F32.code:
            val = accessor.get_float(i)
            result.append(values.TaggedValue.make_float(val))
            
        elif type_code == types.TYPE_STRING.code:
            # get_str_struct returns (len, prefix, struct_ptr, heap_ptr, py_str)
            # We need to extract the actual string content.
            meta = accessor.get_str_struct(i)
            # meta[4] is the python string if available (from Batch)
            # meta[2] is struct_ptr, meta[3] is heap_ptr (from Storage)
            
            s_val = meta[4]
            if s_val is None:
                # Reconstruct from raw pointers
                s_val = strings.unpack_string(meta[2], meta[3])
            
            result.append(values.TaggedValue.make_string(s_val))
            
        elif type_code == types.TYPE_U128.code:
             # U128 in payload is currently stored as int64 for TaggedValue simplicity
             # or we might need a make_u128. For now, assuming only PK is U128 
             # or we cast.
             # Note: TaggedValue doesn't explicitly support u128 yet in spec phase 1.
             # We map it to int for now.
             val_u128 = accessor.get_u128(i)
             # WARNING: Truncation if not handled. 
             # Assuming purely opaque passthrough or u64 payloads for now.
             result.append(values.TaggedValue.make_int(r_int64(val_u128)))
             
        else:
            # Fallback / Null
            result.append(values.TaggedValue.make_null())
            
    return result

# -----------------------------------------------------------------------------
# Linear Operators
# -----------------------------------------------------------------------------

def op_filter(reg_in, reg_out, func):
    """
    Implements: Out = { (k, v, w) | (k, v, w) in In AND func(v) }
    """
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()
    
    count = input_batch.row_count()
    accessor = input_batch.left_acc
    
    for i in range(count):
        accessor.set_row(input_batch.payloads, i)
        if func.evaluate_predicate(accessor):
            output_batch.append(
                input_batch.keys[i],
                input_batch.weights[i],
                input_batch.payloads[i] # Reference copy (cheap)
            )

def op_map(reg_in, reg_out, func):
    """
    Implements: Out = { (k, func(v), w) | (k, v, w) in In }
    Note: MAP in DBSP usually preserves the Key. 
    If Key modification is needed, a specific Re-Key operator is used.
    Here we assume Key is preserved.
    """
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()
    
    count = input_batch.row_count()
    accessor = input_batch.left_acc
    
    for i in range(count):
        accessor.set_row(input_batch.payloads, i)
        
        # Allocate new payload list
        new_payload = []
        func.evaluate_map(accessor, new_payload)
        
        output_batch.append(
            input_batch.keys[i],
            input_batch.weights[i],
            new_payload
        )

def op_negate(reg_in, reg_out):
    """
    Implements: Out = { (k, v, -w) | (k, v, w) in In }
    """
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()
    
    count = input_batch.row_count()
    for i in range(count):
        output_batch.append(
            input_batch.keys[i],
            -input_batch.weights[i],
            input_batch.payloads[i]
        )

def op_union(reg_in_a, reg_in_b, reg_out):
    """
    Implements: Out = InA + InB (Concatenation)
    Consolidation is deferred.
    """
    output_batch = reg_out.batch
    output_batch.clear()
    
    # Copy A
    batch_a = reg_in_a.batch
    count_a = batch_a.row_count()
    for i in range(count_a):
        output_batch.append(
            batch_a.keys[i],
            batch_a.weights[i],
            batch_a.payloads[i]
        )
        
    # Copy B
    batch_b = reg_in_b.batch
    count_b = batch_b.row_count()
    for i in range(count_b):
        output_batch.append(
            batch_b.keys[i],
            batch_b.weights[i],
            batch_b.payloads[i]
        )

# -----------------------------------------------------------------------------
# Bilinear Operators (Joins)
# -----------------------------------------------------------------------------

def op_join_delta_trace(reg_delta, reg_trace, reg_out):
    """
    Index-Nested-Loop Join (INLJ): DeltaBatch (x) TraceCursor
    Semantics: For each tuple in Delta, find matches in Trace by PK.
    Output Weight = Delta.w * Trace.w
    Output Payload = Delta.payload + Trace.payload
    """
    delta_batch = reg_delta.batch
    trace_cursor = reg_trace.cursor
    output_batch = reg_out.batch
    output_batch.clear()
    
    # 1. Sort Delta for cache locality (optional but good for cursor)
    delta_batch.sort()
    
    count = delta_batch.row_count()
    if count == 0:
        return
        
    # 2. Iterate Delta
    for i in range(count):
        d_key = delta_batch.keys[i]
        d_weight = delta_batch.weights[i]
        
        # Ghost check: Skip input ghosts
        if d_weight == 0:
            continue
            
        # 3. Seek in Trace
        trace_cursor.seek(d_key)
        
        # 4. Scan matching keys in Trace
        while trace_cursor.is_valid():
            t_key = trace_cursor.get_key()
            
            # Since both are sorted (locally for cursor), 
            # if t_key > d_key, we are done with this delta key.
            # But UnifiedCursor logic might vary. 
            # Assuming strictly sorted seek:
            if t_key != d_key:
                break
                
            t_weight = trace_cursor.get_weight()
            
            # Algebraic Weight Product
            final_weight = d_weight * t_weight
            
            if final_weight != 0:
                # Materialize Trace payload
                # Note: trace_cursor implements BaseRowAccessor
                t_payload = materialize_row(trace_cursor, reg_trace.vm_schema.table_schema)
                
                # Concatenate Payloads: [Delta Cols] + [Trace Cols]
                d_payload = delta_batch.payloads[i]
                final_payload = d_payload + t_payload
                
                output_batch.append(d_key, final_weight, final_payload)
            
            trace_cursor.next()

def op_join_delta_delta(reg_a, reg_b, reg_out):
    """
    Sort-Merge Join: DeltaBatchA (x) DeltaBatchB
    Both batches are in-memory.
    """
    batch_a = reg_a.batch
    batch_b = reg_b.batch
    output_batch = reg_out.batch
    output_batch.clear()
    
    # 1. Sort both inputs
    batch_a.sort()
    batch_b.sort()
    
    count_a = batch_a.row_count()
    count_b = batch_b.row_count()
    
    idx_a = 0
    idx_b = 0
    
    while idx_a < count_a and idx_b < count_b:
        key_a = batch_a.keys[idx_a]
        key_b = batch_b.keys[idx_b]
        
        if key_a < key_b:
            idx_a += 1
        elif key_b < key_a:
            idx_b += 1
        else:
            # Keys match! Perform Cross Product of the runs.
            match_key = key_a
            
            # Identify run in A
            start_a = idx_a
            while idx_a < count_a and batch_a.keys[idx_a] == match_key:
                idx_a += 1
            end_a = idx_a
            
            # Identify run in B
            start_b = idx_b
            while idx_b < count_b and batch_b.keys[idx_b] == match_key:
                idx_b += 1
            end_b = idx_b
            
            # Cross Product A[start_a:end_a] x B[start_b:end_b]
            for i in range(start_a, end_a):
                w_a = batch_a.weights[i]
                if w_a == 0: continue
                
                for j in range(start_b, end_b):
                    w_b = batch_b.weights[j]
                    final_weight = w_a * w_b
                    
                    if final_weight != 0:
                        final_payload = batch_a.payloads[i] + batch_b.payloads[j]
                        output_batch.append(match_key, final_weight, final_payload)

# -----------------------------------------------------------------------------
# Integral / Misc Operators
# -----------------------------------------------------------------------------

def op_integrate(reg_in, table_id, engine):
    """
    Dumps the batch into the storage engine.
    Effectively: PersistentTable += DeltaBatch
    """
    batch = reg_in.batch
    count = batch.row_count()
    
    # We must iterate and insert individually
    for i in range(count):
        weight = batch.weights[i]
        if weight == 0:
            continue
            
        key = batch.keys[i]
        payload = batch.payloads[i]
        
        # Engine expects (key, weight, payload)
        # Note: The Engine will serialize this to MemTable/WAL
        engine.ingest(key, weight, payload)

def op_distinct(reg_in, reg_out):
    """
    Consolidates the batch and sets all positive weights to 1.
    This enforces Set Semantics on a Multiset stream.
    """
    input_batch = reg_in.batch
    output_batch = reg_out.batch
    output_batch.clear()
    
    # 1. Consolidate locally first to sum up duplicates
    input_batch.sort()
    input_batch.consolidate()
    
    count = input_batch.row_count()
    for i in range(count):
        w = input_batch.weights[i]
        if w > 0:
            output_batch.append(
                input_batch.keys[i],
                1, # Force weight to 1
                input_batch.payloads[i]
            )
        # What about w < 0? 
        # In DBSP DISTINCT usually means (w > 0 -> 1, w <= 0 -> 0) 
        # effectively filtering retractions unless implementing specialized distinct.
        # Simple set projection: if it exists, it's 1.
