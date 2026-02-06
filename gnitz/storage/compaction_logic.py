"""
gnitz/storage/compaction_logic.py
"""
from rpython.rtyper.lltypesystem import rffi, lltype

def merge_entity_contributions(cursors, cursor_lsns):
    """
    Algebraically merges weights and resolves values via LWW for a single Entity ID.
    Implemented as a zero-allocation procedure to prevent heap pressure during 
    large-scale vertical merges.
    
    Args:
        cursors: List of StreamCursor objects pointing to the same Entity ID.
        cursor_lsns: List of LSNs corresponding to each cursor.
        
    Returns:
        Tuple of (net_weight, payload_ptr, blob_ptr). 
        If net_weight is 0, the record is annihilated (The Ghost Property).
    """
    net_weight = 0
    max_lsn = -1
    best_payload = lltype.nullptr(rffi.CCHARP.TO)
    best_blob = lltype.nullptr(rffi.CCHARP.TO)

    # Hot loop: Sum weights and track the pointer with the highest LSN
    for i in range(len(cursors)):
        cursor = cursors[i]
        lsn = cursor_lsns[i]
        view = cursor.view
        idx = cursor.get_current_index()

        # 1. Algebraic Summation
        net_weight += view.get_weight(idx)

        # 2. Last-Write-Wins (LWW) resolution
        if lsn > max_lsn:
            max_lsn = lsn
            best_payload = view.get_data_ptr(idx)
            best_blob = view.buf_b.ptr

    return net_weight, best_payload, best_blob
