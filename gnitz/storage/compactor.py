import os
from rpython.rlib.rarithmetic import r_uint64
from gnitz.storage import shard_ecs, writer_ecs, tournament_tree, compaction_logic
from gnitz.storage.manifest import ManifestEntry

def compact_shards(input_files, output_file, layout):
    views = []
    cursors = []
    tree = None
    writer = None
    
    try:
        # 1. Open Shard Views
        for filename in input_files:
            view = shard_ecs.ECSShardView(filename, layout)
            views.append(view)
            cursors.append(tournament_tree.StreamCursor(view))
        
        # 2. Initialize Compaction Structures
        tree = tournament_tree.TournamentTree(cursors)
        writer = writer_ecs.ECSShardWriter(layout)
        
        # 3. Execution Loop
        while not tree.is_exhausted():
            min_eid = tree.get_min_entity_id()
            indices = tree.get_all_cursors_at_min()
            
            active_cursors = []
            for idx in indices: active_cursors.append(cursors[idx])
            
            # The logic returns only non-zero algebraic results
            merged_results = compaction_logic.merge_entity_contributions(active_cursors, layout)
            
            for weight, payload_ptr, blob_ptr in merged_results:
                writer.add_packed_row(min_eid, weight, payload_ptr, blob_ptr)
            
            tree.advance_min_cursors()
            
        writer.finalize(output_file)
        
    finally:
        # 4. Strict Resource Cleanup
        if tree:
            tree.close()
        if writer:
            writer.close()
        for view in views: 
            view.close()

class CompactionPolicy(object):
    def __init__(self, registry):
        self.registry = registry
    def should_compact(self, cid): return self.registry.needs_compaction(cid)
    def select_shards_to_compact(self, cid): return self.registry.get_shards_for_component(cid)

def finalize_compaction(old_shards, ref_counter):
    for fn in old_shards: ref_counter.mark_for_deletion(fn)
    ref_counter.try_cleanup()
    
def execute_compaction(cid, policy, manifest_manager, ref_counter, layout, output_dir="."):
    shard_metas = policy.select_shards_to_compact(cid)
    if len(shard_metas) < 2: return None

    input_files = []
    # RPython fix: Initialize with unsigned 0 to match meta types
    min_lsn = r_uint64(0)
    max_lsn = r_uint64(0)
    min_eid = r_uint64(0) 
    max_eid = r_uint64(0)
    first_shard = True

    for meta in shard_metas:
        input_files.append(meta.filename)
        
        if first_shard or meta.min_lsn < min_lsn: 
            min_lsn = meta.min_lsn
        if first_shard or meta.max_lsn > max_lsn: 
            max_lsn = meta.max_lsn
        
        if first_shard or meta.min_entity_id < min_eid: 
            min_eid = meta.min_entity_id
        if first_shard or meta.max_entity_id > max_eid: 
            max_eid = meta.max_entity_id
            
        first_shard = False

    new_fn = os.path.join(output_dir, "comp_c%d_lsn%d.db" % (cid, max_lsn))
    compact_shards(input_files, new_fn, layout)

    current_reader = manifest_manager.load_current()
    new_entries = []
    global_max = current_reader.global_max_lsn
    for entry in current_reader.iterate_entries():
        is_compacted = False
        for old_file in input_files:
            if entry.shard_filename == old_file:
                is_compacted = True; break
        if not is_compacted: new_entries.append(entry)
    current_reader.close()
    
    if max_lsn > global_max: global_max = max_lsn
    new_entries.append(ManifestEntry(cid, new_fn, min_eid, max_eid, min_lsn, max_lsn))
    manifest_manager.publish_new_version(new_entries, global_max)

    old_filenames = []
    for meta in shard_metas:
        policy.registry.unregister_shard(meta.filename)
        old_filenames.append(meta.filename)
    
    from gnitz.storage.shard_registry import ShardMetadata
    policy.registry.register_shard(ShardMetadata(new_fn, cid, min_eid, max_eid, min_lsn, max_lsn))
    policy.registry.clear_compaction_flag(cid)
    finalize_compaction(old_filenames, ref_counter)
    return new_fn
