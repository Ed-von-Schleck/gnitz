# gnitz/vm/ops/reduce.py

from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.core import types, strings, xxh
from gnitz.core.comparator import RowAccessor, PayloadRowAccessor
from gnitz.vm import runtime


class ReduceAccessor(RowAccessor):
    """
    Virtual accessor for reduction results.
    Maps output schema columns to input group-by columns or aggregate results.
    """
    _immutable_fields_ = [
        "input_schema", "output_schema", 
        "mapping_to_input[*]", "agg_col_idx"
    ]

    def __init__(self, input_schema, output_schema, group_indices):
        self.input_schema = input_schema
        self.output_schema = output_schema
        self.agg_col_idx = len(output_schema.columns) - 1
        
        # Build mapping: output_col_idx -> input_col_idx (or -1 for agg)
        num_out = len(output_schema.columns)
        mapping = [-1] * num_out
        
        # Identify group columns in output. 
        # Output PK is handled by batch.append, so we map payload cols.
        # Logic matches types._build_reduce_output_schema
        use_natural_pk = (len(group_indices) == 1 and 
                         output_schema.pk_index == 0 and
                         output_schema.columns[0].field_type.code in (types.TYPE_U64.code, types.TYPE_U128.code))

        if use_natural_pk:
            # Output: [PK(GroupCol), Agg]
            mapping[0] = group_indices[0]
            mapping[1] = -1
        else:
            # Output: [PK(Hash), GroupCol_0...GroupCol_N, Agg]
            mapping[0] = -2 # PK is hash, not in input payload
            for i in range(len(group_indices)):
                mapping[i + 1] = group_indices[i]
            mapping[num_out - 1] = -1

        self.mapping_to_input = mapping
        self.exemplar = None
        self.agg_func = None
        self.old_val_bits = r_uint64(0)
        self.use_old_val = False

    def set_context(self, exemplar, agg_func, old_val_bits=r_uint64(0), use_old_val=False):
        self.exemplar = exemplar
        self.agg_func = agg_func
        self.old_val_bits = old_val_bits
        self.use_old_val = use_old_val

    @jit.unroll_safe
    def is_null(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1: return False # Aggregates are not null in this version
        if src < 0: return False
        return self.exemplar.is_null(src)

    @jit.unroll_safe
    def get_int(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1:
            if self.use_old_val: return self.old_val_bits
            # Note: agg_func.get_value_bits() returns r_uint64 bit pattern
            return self.agg_func.get_value_bits()
        return self.exemplar.get_int(src)

    @jit.unroll_safe
    def get_float(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1: return 0.0 # Agg functions return ints for now
        return self.exemplar.get_float(src)

    @jit.unroll_safe
    def get_u128(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1: return r_uint128(0)
        if src == -2: return r_uint128(0) # Hash handled by batch.append
        return self.exemplar.get_u128(src)

    @jit.unroll_safe
    def get_str_struct(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src < 0:
            return (0, 0, strings.NULL_PTR, strings.NULL_PTR, None)
        return self.exemplar.get_str_struct(src)
        
    @jit.unroll_safe
    def get_int_signed(self, col_idx):
        src = self.mapping_to_input[col_idx]
        if src == -1:
            # Aggregates are always stored as r_int64 bits in this engine
            if self.use_old_val: 
                return rffi.cast(rffi.LONGLONG, self.old_val_bits)
            return rffi.cast(rffi.LONGLONG, self.agg_func.get_value_bits())
        return self.exemplar.get_int_signed(src)

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
                r_len, r_pref, r_ptr, r_heap, r_str
            )
            if res != 0: return res
        elif col_type == types.TYPE_F64.code or col_type == types.TYPE_F32.code:
            va, vb = accessor_a.get_float(col_idx), accessor_b.get_float(col_idx)
            if va < vb: return -1
            if va > vb: return 1
        elif col_type == types.TYPE_U128.code:
            va, vb = accessor_a.get_u128(col_idx), accessor_b.get_u128(col_idx)
            if va < vb: return -1
            if va > vb: return 1
        else:
            va, vb = accessor_a.get_int(col_idx), accessor_b.get_int(col_idx)
            if va < vb: return -1
            if va > vb: return 1
    return 0


def _argsort_delta(batch, schema, col_indices):
    count = batch.length()
    indices = [i for i in range(count)]
    if count <= 1: return indices

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
    reg_in, reg_trace_in, reg_trace_out, reg_out, 
    group_by_cols, agg_func, output_schema
):
    """
    Incremental DBSP REDUCE: δout = Agg(I + δin) - Agg(I).
    Zero-copy: uses ReduceAccessor to emit results directly to batch arenas.
    """
    assert isinstance(reg_in, runtime.DeltaRegister)
    delta_in = reg_in.batch
    if delta_in.length() == 0: return

    assert isinstance(reg_out, runtime.DeltaRegister)
    out_batch = reg_out.batch
    out_batch.clear()

    input_schema = reg_in.vm_schema.table_schema
    
    if not delta_in.is_sorted():
        delta_in.sort()
    delta_in.consolidate()

    sorted_indices = _argsort_delta(delta_in, input_schema, group_by_cols)
    
    acc_in = PayloadRowAccessor(input_schema)
    acc_exemplar = PayloadRowAccessor(input_schema)
    reduce_acc = ReduceAccessor(input_schema, output_schema, group_by_cols)
    
    idx = 0
    n = delta_in.length()
    while idx < n:
        group_start_pos = idx
        group_start_idx = sorted_indices[group_start_pos]
        
        acc_exemplar.set_row(delta_in.get_row(group_start_idx))
        group_key = _extract_group_key(acc_exemplar, input_schema, group_by_cols)

        agg_func.reset()
        while idx < n:
            curr_idx = sorted_indices[idx]
            acc_in.set_row(delta_in.get_row(curr_idx))
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
            
            # Emit Retraction (-1)
            reduce_acc.set_context(acc_exemplar, agg_func, old_val_bits, use_old_val=True)
            out_batch.append_from_accessor(group_key, r_int64(-1), reduce_acc)

        # Compute New Aggregate
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

        # Emit Insertion (+1)
        if not agg_func.is_accumulator_zero():
            reduce_acc.set_context(acc_exemplar, agg_func, use_old_val=False)
            out_batch.append_from_accessor(group_key, r_int64(1), reduce_acc)

    out_batch.sort()
    out_batch.consolidate()
