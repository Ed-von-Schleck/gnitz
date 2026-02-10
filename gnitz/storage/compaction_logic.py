from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, strings as string_logic

FIELD_INDICES = jit.unrolling_iterable(range(64))

@jit.unroll_safe
def rows_equal(schema, cursor1, cursor2):
    """Checks if the data columns of two rows (across different shards) are identical."""
    idx1 = cursor1.position
    idx2 = cursor2.position
    
    for i in FIELD_INDICES:
        if i >= len(schema.columns): break
        if i == schema.pk_index: continue
        
        col_def = schema.columns[i]
        ptr1 = cursor1.view.get_col_ptr(idx1, i)
        ptr2 = cursor2.view.get_col_ptr(idx2, i)
        
        if col_def.field_type == types.TYPE_STRING:
            h1 = cursor1.view.blob_buf.ptr
            h2 = cursor2.view.blob_buf.ptr
            if not string_logic.string_equals_dual(ptr1, h1, ptr2, h2):
                return False
        else:
            # Primitive comparison
            for b in range(col_def.field_type.size):
                if ptr1[b] != ptr2[b]: return False
    return True

def merge_row_contributions(active_cursors, schema):
    """
    Groups inputs by Semantic Row Payload and sums weights.
    Only returns non-zero net weights.
    """
    n = len(active_cursors)
    results = []
    processed_mask = 0
    
    for i in range(n):
        if (processed_mask >> i) & 1: continue
        
        base_cursor = active_cursors[i]
        total_weight = base_cursor.view.get_weight(base_cursor.position)
        processed_mask |= (1 << i)
        
        for j in range(i + 1, n):
            if (processed_mask >> j) & 1: continue
            
            other_cursor = active_cursors[j]
            if rows_equal(schema, base_cursor, other_cursor):
                total_weight += other_cursor.view.get_weight(other_cursor.position)
                processed_mask |= (1 << j)
        
        if total_weight != 0:
            results.append((total_weight, i)) # Weight and index of the 'exemplar' cursor
            
    return results
