"""
gnitz/storage/compactor.py
"""
import os
from gnitz.storage import shard_ecs, writer_ecs, tournament_tree, compaction_logic
from gnitz.storage.manifest import ManifestEntry

def compact_shards(input_files, input_lsns, output_file, layout):
    """
    Performs a vertical merge of multiple shards into a single consolidated shard.
    """
    views = []
    cursors = []
    try:
        for filename in input_files:
            view = shard_ecs.ECSShardView(filename, layout)
            views.append(view)
            cursors.append(tournament_tree.StreamCursor(view))
        
        tree = tournament_tree.TournamentTree(cursors)
        writer = writer_ecs.ECSShardWriter(layout)
        
        while not tree.is_exhausted():
            min_eid = tree.get_min_entity_id()
            indices = tree.get_all_cursors_at_min()
            
            active_cursors = []
            active_lsns = []
            for idx in indices:
                active_cursors.append(cursors[idx])
                active_lsns.append(input_lsns[idx])
            
            res = compaction_logic.merge_entity_contributions(active_cursors, active_lsns)
            weight, payload_ptr, blob_ptr = res
            
            if weight != 0:
                writer.add_packed_row(min_eid, weight, payload_ptr, blob_ptr)
            
            tree.advance_min_cursors()
        writer.finalize(output_file)
    finally:
        for view in views:
            view.close()

class CompactionPolicy(object):
    """
    Logic for deciding when and what to compact.
    """
    def __init__(self, registry):
        self.registry = registry

    def should_compact(self, component_id):
        """Checks if a component has exceeded read amplification thresholds."""
        return self.registry.needs_compaction(component_id)

    def select_shards_to_compact(self, component_id):
        """
        Selects the set of shards to merge.
        For now, we use a simple 'Full L0' strategy: merge all shards 
        for the component into one 'Guard' shard.
        """
        return self.registry.get_shards_for_component(component_id)

def finalize_compaction(old_shard_filenames, ref_counter):
    """
    Marks old shards for deletion and attempts cleanup.
    
    Args:
        old_shard_filenames: List of filenames to remove
        ref_counter: RefCounter instance
    """
    for fn in old_shard_filenames:
        ref_counter.mark_for_deletion(fn)
    
    # Try to delete immediately (will succeed if no active readers)
    ref_counter.try_cleanup()

def execute_compaction(component_id, policy, manifest_manager, ref_counter, layout, output_dir="."):
    """
    Orchestrates the full compaction lifecycle.
    """
    # 1. Identify shards to compact
    shard_metas = policy.select_shards_to_compact(component_id)
    if len(shard_metas) < 2:
        return None # Nothing to merge

    input_files = []
    input_lsns = []
    min_lsn = -1
    max_lsn = -1
    min_eid = -1
    max_eid = -1

    for meta in shard_metas:
        input_files.append(meta.filename)
        input_lsns.append(meta.max_lsn) # Use max_lsn for resolution
        
        # Track ranges for the new shard metadata
        if min_lsn == -1 or meta.min_lsn < min_lsn: min_lsn = meta.min_lsn
        if meta.max_lsn > max_lsn: max_lsn = meta.max_lsn
        if min_eid == -1 or meta.min_entity_id < min_eid: min_eid = meta.min_entity_id
        if meta.max_entity_id > max_eid: max_eid = meta.max_entity_id

    # 2. Generate new shard filename
    # In a real system, we'd use a UUID or LSN-based name
    new_filename = os.path.join(output_dir, "comp_c%d_lsn%d.db" % (component_id, max_lsn))

    # 3. Run the physical merge
    compact_shards(input_files, input_lsns, new_filename, layout)

    # 4. Prepare new Manifest entries
    # We load the current manifest and swap out the old files for the new one
    current_reader = manifest_manager.load_current()
    new_entries = []
    
    # Track the global max from the existing manifest header
    global_max = current_reader.global_max_lsn
    
    # Keep entries for other components, skip the ones we just compacted
    for entry in current_reader.iterate_entries():
        is_compacted = False
        for old_file in input_files:
            if entry.shard_filename == old_file:
                is_compacted = True
                break
        
        if not is_compacted:
            new_entries.append(entry)
    current_reader.close()
    
    if max_lsn > global_max:
        global_max = max_lsn

    # Add the newly created shard to the manifest
    # We re-verify the eid range from the file if needed, but here we trust the aggregate

    new_entries.append(ManifestEntry(component_id, new_filename, min_eid, max_eid, min_lsn, max_lsn))

    # 5. Atomic Manifest Update
    manifest_manager.publish_new_version(new_entries, global_max)

    # 6. Lifecycle Management
    # Update Registry and mark old files for deletion
    old_filenames = []
    for meta in shard_metas:
        policy.registry.unregister_shard(meta.filename)
        old_filenames.append(meta.filename)
    
    from gnitz.storage.shard_registry import ShardMetadata
    new_meta = ShardMetadata(new_filename, component_id, min_eid, max_eid, min_lsn, max_lsn)
    policy.registry.register_shard(new_meta)
    policy.registry.clear_compaction_flag(component_id)

    # Finalize: Mark for deletion and try cleanup
    finalize_compaction(old_filenames, ref_counter)

    return new_filename
