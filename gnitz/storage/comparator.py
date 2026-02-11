from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, strings as string_logic

_MAX_COLUMNS = 64
_COLUMN_ITERABLE = jit.unrolling_iterable(range(_MAX_COLUMNS))

@jit.unroll_safe
def compare_payloads(schema, ptr1, heap1, ptr2, heap2):
    """Compare two packed (AoS) rows."""
    for i in _COLUMN_ITERABLE:
        if i >= len(schema.columns): break
        if i == schema.pk_index: continue
        
        f_type = schema.columns[i].field_type
        f_off = schema.get_column_offset(i)
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
    """Compare two rows residing in SoA Shards."""
    for i in _COLUMN_ITERABLE:
        if i >= len(schema.columns): break
        if i == schema.pk_index: continue
        
        p1 = view1.get_col_ptr(idx1, i)
        p2 = view2.get_col_ptr(idx2, i)
        h1, h2 = view1.blob_buf.ptr, view2.blob_buf.ptr
        
        f_type = schema.columns[i].field_type
        if f_type == types.TYPE_STRING:
            res = string_logic.string_compare(p1, h1, p2, h2)
            if res != 0: return res
        else:
            for j in range(f_type.size):
                if p1[j] < p2[j]: return -1
                if p1[j] > p2[j]: return 1
    return 0

@jit.unroll_safe
def compare_soa_to_packed(schema, view, idx, packed_ptr, packed_heap):
    """Compare a Shard row (SoA) against a packed (AoS) row."""
    for i in _COLUMN_ITERABLE:
        if i >= len(schema.columns): break
        if i == schema.pk_index: continue
        
        p1 = view.get_col_ptr(idx, i)
        h1 = view.blob_buf.ptr
        
        f_off = schema.get_column_offset(i)
        p2 = rffi.ptradd(packed_ptr, f_off)
        
        f_type = schema.columns[i].field_type
        if f_type == types.TYPE_STRING:
            res = string_logic.string_compare(p1, h1, p2, packed_heap)
            if res != 0: return res
        else:
            for j in range(f_type.size):
                if p1[j] < p2[j]: return -1
                if p1[j] > p2[j]: return 1
    return 0
