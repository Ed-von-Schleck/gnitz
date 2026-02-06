from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit

# The number of cursors (input shards) during a merge is typically small (4-10) and 
# constant for the duration of the merge function. Unrolling this removes loop 
# overhead in the hot path.
@jit.unroll_safe
def merge_entity_contributions(cursors, cursor_lsns):
    """
    Algebraically merges weights and resolves values via LWW for a single Entity ID.
    """
    net_weight = 0
    max_lsn = -1
    best_payload = lltype.nullptr(rffi.CCHARP.TO)
    best_blob = lltype.nullptr(rffi.CCHARP.TO)

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
