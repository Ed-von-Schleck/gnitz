"""
gnitz/storage/compaction_logic.py
"""
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit
from gnitz.storage import memtable

@jit.unroll_safe
def merge_entity_contributions(cursors, layout):
    """
    Groups inputs by Payload and sums weights for each unique (EntityID, Payload).
    Returns a list of tuples: (weight, payload_ptr, blob_ptr)
    """
    # Simple algorithm: Collect all, then group.
    # Since N is small (num shards), simple linear scan is fine.
    
    candidates = [] # List of (view, index)
    
    for cursor in cursors:
        idx = cursor.get_current_index()
        candidates.append((cursor.view, idx))
    
    results = []
    
    # Process candidates until all are handled
    while len(candidates) > 0:
        base_view, base_idx = candidates[0]
        base_payload = base_view.get_data_ptr(base_idx)
        base_blob = base_view.buf_b.ptr
        
        total_weight = 0
        
        remaining = []
        
        for view, idx in candidates:
            payload = view.get_data_ptr(idx)
            blob = view.buf_b.ptr
            
            # Check strict equality
            if memtable.compare_payloads(layout, base_payload, base_blob, payload, blob) == 0:
                total_weight += view.get_weight(idx)
            else:
                remaining.append((view, idx))
        
        if total_weight != 0:
            results.append((total_weight, base_payload, base_blob))
            
        candidates = remaining

    return results
