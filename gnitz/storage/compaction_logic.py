from rpython.rlib import jit
from gnitz.storage import memtable

@jit.unroll_safe
def merge_entity_contributions(cursors, layout):
    """
    Groups inputs by Semantic Payload and sums weights.
    Optimization: Uses Hash-First comparison to avoid Region B fetches.
    """
    # candidates stores StreamCursor objects
    candidates = []
    for cursor in cursors:
        candidates.append(cursor)
    
    results = []
    while len(candidates) > 0:
        base_cursor = candidates[0]
        base_view = base_cursor.view
        base_idx = base_cursor.get_current_index()
        base_payload = base_view.get_data_ptr(base_idx)
        base_blob = base_view.buf_b.ptr
        
        # Cache hashes for the base payload strings to accelerate the loop
        base_hashes = base_cursor.get_current_payload_hashes(layout)
        
        total_weight = 0
        remaining = []
        
        for cursor in candidates:
            view = cursor.view
            idx = cursor.get_current_index()
            payload = view.get_data_ptr(idx)
            blob = view.buf_b.ptr
            
            # Optimization: Compare cached hashes first
            other_hashes = cursor.get_current_payload_hashes(layout)
            
            match = True
            if len(base_hashes) == len(other_hashes):
                for h_idx in range(len(base_hashes)):
                    if base_hashes[h_idx] != other_hashes[h_idx]:
                        match = False; break
            else:
                match = False
            
            # If hashes match, perform rigorous semantic check
            if match and memtable.compare_payloads(layout, base_payload, base_blob, payload, blob) == 0:
                total_weight += view.get_weight(idx)
            else:
                remaining.append(cursor)
        
        # THE GHOST PROPERTY:
        # Only materialize this payload group if the net algebraic weight is non-zero.
        if total_weight != 0:
            results.append((total_weight, base_payload, base_blob))
            
        candidates = remaining

    return results
