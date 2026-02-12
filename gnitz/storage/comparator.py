from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, strings as string_logic, values as db_values

_MAX_COLUMNS = 64
_COLUMN_ITERABLE = jit.unrolling_iterable(range(_MAX_COLUMNS))

@jit.unroll_safe
def compare_values_to_packed(schema, values, packed_ptr, heap_ptr):
    """Dry-run comparison: DBValue list vs Packed Node payload."""
    val_idx = 0
    for i in _COLUMN_ITERABLE:
        if i >= len(schema.columns): break
        if i == schema.pk_index: continue
        
        val_obj = values[val_idx]
        val_idx += 1
        
        f_type = schema.columns[i].field_type
        f_off = schema.get_column_offset(i)
        p_node = rffi.ptradd(packed_ptr, f_off)
        
        if f_type == types.TYPE_STRING:
            res = string_logic.compare_db_value_to_german(val_obj, p_node, heap_ptr)
            if res != 0: return res
        elif f_type == types.TYPE_F64:
            v_obj = val_obj.get_float()
            v_node = float(rffi.cast(rffi.DOUBLEP, p_node)[0])
            if v_node < v_obj: return 1
            if v_node > v_obj: return -1
        elif f_type == types.TYPE_U128:
            v_obj = val_obj.get_u128()
            lo = rffi.cast(rffi.ULONGLONGP, p_node)[0]
            hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(p_node, 8))[0]
            v_node = (r_uint128(hi) << 64) | r_uint128(lo)
            if v_node < v_obj: return 1
            if v_node > v_obj: return -1
        else:
            v_obj = val_obj.get_int()
            v_node = rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.LONGLONGP, p_node)[0])
            if v_node < v_obj: return 1
            if v_node > v_obj: return -1
    return 0

@jit.unroll_safe
def compare_payloads(schema, ptr1, heap1, ptr2, heap2):
    for i in _COLUMN_ITERABLE:
        if i >= len(schema.columns): break
        if i == schema.pk_index: continue
        f_type, f_off = schema.columns[i].field_type, schema.get_column_offset(i)
        p1, p2 = rffi.ptradd(ptr1, f_off), rffi.ptradd(ptr2, f_off)
        if f_type == types.TYPE_STRING:
            res = string_logic.string_compare(p1, heap1, p2, heap2)
            if res != 0: return res
        else:
            for j in range(f_type.size):
                if p1[j] < p2[j]: return -1
                if p1[j] > p2[j]: return 1
    return 0

@jit.unroll_safe
def compare_soa_rows(schema, view1, idx1, view2, idx2):
    for i in _COLUMN_ITERABLE:
        if i >= len(schema.columns): break
        if i == schema.pk_index: continue
        p1, p2 = view1.get_col_ptr(idx1, i), view2.get_col_ptr(idx2, i)
        h1, h2 = view1.blob_buf.ptr, view2.blob_buf.ptr
        if schema.columns[i].field_type == types.TYPE_STRING:
            res = string_logic.string_compare(p1, h1, p2, h2)
            if res != 0: return res
        else:
            for j in range(schema.columns[i].field_type.size):
                if p1[j] < p2[j]: return -1
                if p1[j] > p2[j]: return 1
    return 0

@jit.unroll_safe
def compare_soa_to_values(schema, view, idx, values):
    val_idx = 0
    for i in _COLUMN_ITERABLE:
        if i >= len(schema.columns): break
        if i == schema.pk_index: continue
        val_obj = values[val_idx]
        val_idx += 1
        f_type = schema.columns[i].field_type
        p_shard = view.get_col_ptr(idx, i)
        if f_type == types.TYPE_STRING:
            res = string_logic.compare_db_value_to_german(val_obj, p_shard, view.blob_buf.ptr)
            if res != 0: return res
        elif f_type == types.TYPE_F64:
            v_obj = val_obj.get_float()
            v_shard = float(rffi.cast(rffi.DOUBLEP, p_shard)[0])
            if v_shard < v_obj: return 1
            if v_shard > v_obj: return -1
        elif f_type == types.TYPE_U128:
            v_obj = val_obj.get_u128()
            lo = rffi.cast(rffi.ULONGLONGP, p_shard)[0]
            hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(p_shard, 8))[0]
            v_shard = (r_uint128(hi) << 64) | r_uint128(lo)
            if v_shard < v_obj: return 1
            if v_shard > v_obj: return -1
        else:
            v_obj = val_obj.get_int()
            v_shard = rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.LONGLONGP, p_shard)[0])
            if v_shard < v_obj: return 1
            if v_shard > v_obj: return -1
    return 0
