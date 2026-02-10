from rpython.rlib import jit
from gnitz.storage import memtable

@jit.unroll_safe
def merge_entity_contributions(cursors, layout):
    """
    Groups inputs by Semantic Payload and sums weights.
    """
    n = len(cursors)
    
    # --- Fast Path: Single Participant ---
    # If the entity only exists in one shard, no grouping is required.
    if n == 1:
        cursor = cursors[0]
        idx = cursor.get_current_index()
        weight = cursor.view.get_weight(idx)
        # The Ghost Property: Annihilated records (w=0) are not yielded.
        if weight == 0:
            return []
        return [(weight, cursor.view.get_data_ptr(idx), cursor.view.buf_b.ptr)]

    # --- Multi-cursor Grouping Logic ---
    results = []
    
    # Use an integer as a bitmask.
    # This avoids allocating a list object on the heap for every Entity ID.
    # For N <= 64, this fits entirely in a CPU register.
    processed_mask = 0
    
    for i in range(n):
        # Check bit at position i
        if (processed_mask >> i) & 1:
            continue
            
        base_cursor = cursors[i]
        base_view = base_cursor.view
        base_idx = base_cursor.get_current_index()
        base_payload = base_view.get_data_ptr(base_idx)
        base_blob = base_view.buf_b.ptr
        
        # Mark i as processed
        processed_mask |= (1 << i)
        
        total_weight = base_view.get_weight(base_idx)
        
        # Cache hashes for the base payload to accelerate inner matching
        base_hashes = base_cursor.get_current_payload_hashes(layout)
        
        # Inner loop: find all matching payloads in other shards
        for j in range(i + 1, n):
            # Check bit at position j
            if (processed_mask >> j) & 1:
                continue
                
            other_cursor = cursors[j]
            other_view = other_cursor.view
            other_idx = other_cursor.get_current_index()
            
            # 1. Cheap Hash Check
            other_hashes = other_cursor.get_current_payload_hashes(layout)
            match = True
            if len(base_hashes) == len(other_hashes):
                for h_idx in range(len(base_hashes)):
                    if base_hashes[h_idx] != other_hashes[h_idx]:
                        match = False
                        break
            else:
                match = False
            
            # 2. Semantic Comparison (Field-by-field and German String Heap)
            if match:
                other_payload = other_view.get_data_ptr(other_idx)
                other_blob = other_view.buf_b.ptr
                
                # If payloads are semantically identical, coalesce weights
                if memtable.compare_payloads(layout, base_payload, base_blob, 
                                             other_payload, other_blob) == 0:
                    total_weight += other_view.get_weight(other_idx)
                    # Mark j as processed
                    processed_mask |= (1 << j)
        
        # Only materialize this payload group if the net algebraic weight is non-zero.
        # This physically reclaims storage for annihilated state.
        if total_weight != 0:
            results.append((total_weight, base_payload, base_blob))

    return results
