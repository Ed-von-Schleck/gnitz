from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit
from gnitz.storage import memtable

@jit.unroll_safe
def merge_entity_contributions(cursors, layout):
    """
    Groups inputs by Payload and sums weights. 
    Returns list of (weight, payload_ptr, blob_ptr) where weight != 0.
    """
    candidates = []
    for cursor in cursors:
        candidates.append((cursor.view, cursor.get_current_index()))
    
    results = []
    while len(candidates) > 0:
        base_view, base_idx = candidates[0]
        # Weight-Gated Materialization: We must fetch the payload to group,
        # but the check is done as soon as the sum for that group is complete.
        base_payload = base_view.get_data_ptr(base_idx)
        base_blob = base_view.buf_b.ptr
        
        total_weight = 0
        remaining = []
        
        for view, idx in candidates:
            payload = view.get_data_ptr(idx)
            blob = view.buf_b.ptr
            if memtable.compare_payloads(layout, base_payload, base_blob, payload, blob) == 0:
                total_weight += view.get_weight(idx)
            else:
                remaining.append((view, idx))
        
        # Ghost Property: Only materialize if net weight is non-zero
        if total_weight != 0:
            results.append((total_weight, base_payload, base_blob))
            
        candidates = remaining

    return results
