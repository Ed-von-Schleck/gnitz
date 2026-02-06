"""
gnitz/storage/compactor.py
"""
from gnitz.storage import shard_ecs, writer_ecs, tournament_tree, compaction_logic

def compact_shards(input_files, input_lsns, output_file, layout):
    """
    Performs a vertical merge of multiple shards into a single consolidated shard.
    
    Args:
        input_files: List of paths to input shard files.
        input_lsns: List of LSNs corresponding to the input files.
        output_file: Path to the resulting compacted shard.
        layout: ComponentLayout for these shards.
    """
    views = []
    cursors = []
    
    try:
        # 1. Initialize cursors for all input shards
        for filename in input_files:
            view = shard_ecs.ECSShardView(filename, layout)
            views.append(view)
            cursors.append(tournament_tree.StreamCursor(view))
        
        # 2. Build the Tournament Tree for N-way merge
        tree = tournament_tree.TournamentTree(cursors)
        writer = writer_ecs.ECSShardWriter(layout)
        
        # 3. Process entities in sorted order
        while not tree.is_exhausted():
            min_eid = tree.get_min_entity_id()
            
            # Find which cursors/shards contribute to this Entity ID
            indices = tree.get_all_cursors_at_min()
            
            active_cursors = []
            active_lsns = []
            for idx in indices:
                active_cursors.append(cursors[idx])
                active_lsns.append(input_lsns[idx])
            
            # 4. Algebraic merge (Weight Summation + LSN Resolution)
            res = compaction_logic.merge_entity_contributions(active_cursors, active_lsns)
            weight, payload_ptr, blob_ptr = res
            
            # 5. Annihilation Check (Ghost Property)
            if weight != 0:
                # Write to the new shard (packed to avoid re-serialization)
                writer.add_packed_row(min_eid, weight, payload_ptr, blob_ptr)
            
            # 6. Advance all processed cursors
            tree.advance_min_cursors()
            
        # 7. Finalize the new shard
        writer.finalize(output_file)
        
    finally:
        # Ensure all mapped views are closed
        for view in views:
            view.close()
