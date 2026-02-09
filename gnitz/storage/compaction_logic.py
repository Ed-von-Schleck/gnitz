from rpython.rlib import jit
from gnitz.storage import memtable

@jit.unroll_safe
def merge_entity_contributions(cursors, layout):
    """
    Groups inputs by Semantic Payload and sums weights.
    Crucial: Uses memtable.compare_payloads to follow pointers into 
    the Region B of different source shards.
    """
    # candidates stores (view, index) pairs for all cursors currently at min_eid
    candidates = []
    for cursor in cursors:
        candidates.append((cursor.view, cursor.get_current_index()))
    
    results = []
    while len(candidates) > 0:
        base_view, base_idx = candidates[0]
        base_payload = base_view.get_data_ptr(base_idx)
        base_blob = base_view.buf_b.ptr
        
        total_weight = 0
        remaining = []
        
        # O(N^2) grouping is fine here as N (payloads per Entity) is typically 1 or 2
        for view, idx in candidates:
            payload = view.get_data_ptr(idx)
            blob = view.buf_b.ptr
            
            # Semantic equality check (follows German String offsets)
            if memtable.compare_payloads(layout, base_payload, base_blob, payload, blob) == 0:
                total_weight += view.get_weight(idx)
            else:
                remaining.append((view, idx))
        
        # THE GHOST PROPERTY:
        # Only materialize this payload group if the net algebraic weight is non-zero.
        if total_weight != 0:
            results.append((total_weight, base_payload, base_blob))
            
        candidates = remaining

    return results
