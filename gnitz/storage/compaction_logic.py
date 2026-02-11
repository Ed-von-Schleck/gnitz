from rpython.rlib import jit
from gnitz.storage import comparator

def merge_row_contributions(active_cursors, schema):
    """
    Groups inputs by Semantic Row Payload and sums weights.
    
    In DBSP, compaction is a pure Z-Set merge: 
    Q(A + B) = Q(A) + Q(B).
    """
    n = len(active_cursors)
    results = []
    processed_mask = 0
    
    for i in range(n):
        # Check if this cursor's current row has already been merged
        if (processed_mask >> i) & 1: 
            continue
        
        base_cursor = active_cursors[i]
        total_weight = base_cursor.view.get_weight(base_cursor.position)
        processed_mask |= (1 << i)
        
        for j in range(i + 1, n):
            if (processed_mask >> j) & 1: 
                continue
            
            other_cursor = active_cursors[j]
            
            # Use centralized SoA-to-SoA comparator to determine semantic equality
            if comparator.compare_soa_rows(schema, base_cursor.view, base_cursor.position, 
                                           other_cursor.view, other_cursor.position) == 0:
                total_weight += other_cursor.view.get_weight(other_cursor.position)
                processed_mask |= (1 << j)
        
        # Only materialize surviving records (The Ghost Property)
        if total_weight != 0:
            results.append((total_weight, i)) 
            
    return results
