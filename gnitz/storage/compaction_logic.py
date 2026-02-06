"""
gnitz/storage/compaction_logic.py
"""
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import errors

class MergeAccumulator(object):
    """
    Accumulates weights and resolves the latest payload for a single Entity ID
    across multiple shards.
    """
    def __init__(self):
        self.net_weight = 0
        self.max_lsn = -1
        # Explicitly initialize as null pointers for RPython type inference
        self.best_payload_ptr = lltype.nullptr(rffi.CCHARP.TO)
        self.best_blob_ptr = lltype.nullptr(rffi.CCHARP.TO)
        self.has_value = False

    def add_contribution(self, weight, lsn, payload_ptr, blob_ptr):
        """
        Adds a contribution from a shard.
        """
        self.net_weight += weight
        
        # Last-Write-Wins: Keep the payload from the highest LSN
        if lsn > self.max_lsn:
            self.max_lsn = lsn
            self.best_payload_ptr = payload_ptr
            self.best_blob_ptr = blob_ptr
            self.has_value = True

    def is_annihilated(self):
        """Returns True if net weight is 0 (Ghost Property)."""
        return self.net_weight == 0

    def get_result(self):
        """
        Returns the resolved state.
        
        Returns:
            Tuple (net_weight, payload_ptr, blob_ptr)
        """
        return self.net_weight, self.best_payload_ptr, self.best_blob_ptr

def merge_entity_contributions(cursors, cursor_lsns):
    """
    Merges contributions for the current entity from all cursors.
    Assumes all provided cursors are pointing to the same Entity ID.
    
    Args:
        cursors: List of StreamCursor objects (from TournamentTree)
        cursor_lsns: List of LSNs corresponding to the cursors
        
    Returns:
        Tuple (net_weight, payload_ptr, blob_ptr).
        If net_weight == 0, the entity is annihilated.
    """
    accumulator = MergeAccumulator()
    
    for i in range(len(cursors)):
        cursor = cursors[i]
        lsn = cursor_lsns[i]
        
        # Access the underlying view via the cursor
        view = cursor.view
        idx = cursor.get_current_index()
        
        weight = view.get_weight(idx)
        payload_ptr = view.get_data_ptr(idx)
        blob_ptr = view.buf_b.ptr
        
        accumulator.add_contribution(weight, lsn, payload_ptr, blob_ptr)
    
    # RPython fix: Always return the tuple. 
    # Caller must check weight == 0 to handle annihilation.
    return accumulator.get_result()
