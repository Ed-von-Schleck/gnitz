"""
gnitz/storage/compactor.py
"""
import os
from gnitz.storage import shard_ecs, writer_ecs, tournament_tree, compaction_logic
from gnitz.storage.manifest import ManifestEntry

def compact_shards(input_files, output_file, layout):
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
            for idx in indices:
                active_cursors.append(cursors[idx])
            
            # Returns list of (weight, payload, blob) tuples
            merged_results = compaction_logic.merge_entity_contributions(active_cursors, layout)
            
            for weight, payload_ptr, blob_ptr in merged_results:
                writer.add_packed_row(min_eid, weight, payload_ptr, blob_ptr)
            
            tree.advance_min_cursors()
        writer.finalize(output_file)
    finally:
        for view in views:
            view.close()

class CompactionPolicy(object):
    def __init__(self, registry):
        self.registry = registry

    def should_compact(self, component_id):
        return self.registry.needs_compaction(component_id)

    def select_shards_to_compact(self, component_id):
        return self.registry.get_shards_for_component(component_id)

def finalize_compaction(old_shard_filenames, ref_counter):
    for fn in old_shard_filenames:
        ref_counter.mark_for_deletion(fn)
    ref_counter.try_cleanup()

def execute_compaction(component_id, policy, manifest_manager, ref_counter, layout, output_dir="."):
    shard_metas = policy.select_shards_to_compact(component_id)
    if len(shard_metas) < 2:
        return None

    input_files = []
    min_lsn = -1
    max_lsn = -1
    min_eid = -1
    max_eid = -1

    for meta in shard_metas:
        input_files.append(meta.filename)
        if min_lsn == -1 or meta.min_lsn < min_lsn: min_lsn = meta.min_lsn
        if meta.max_lsn > max_lsn: max_lsn = meta.max_lsn
        if min_eid == -1 or meta.min_entity_id < min_eid: min_eid = meta.min_entity_id
        if meta.max_entity_id > max_eid: max_eid = meta.max_entity_id

    new_filename = os.path.join(output_dir, "comp_c%d_lsn%d.db" % (component_id, max_lsn))

    compact_shards(input_files, new_filename, layout)

    current_reader = manifest_manager.load_current()
    new_entries = []
    global_max = current_reader.global_max_lsn
    
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

    new_entries.append(ManifestEntry(component_id, new_filename, min_eid, max_eid, min_lsn, max_lsn))

    manifest_manager.publish_new_version(new_entries, global_max)

    old_filenames = []
    for meta in shard_metas:
        policy.registry.unregister_shard(meta.filename)
        old_filenames.append(meta.filename)
    
    from gnitz.storage.shard_registry import ShardMetadata
    new_meta = ShardMetadata(new_filename, component_id, min_eid, max_eid, min_lsn, max_lsn)
    policy.registry.register_shard(new_meta)
    policy.registry.clear_compaction_flag(component_id)

    finalize_compaction(old_filenames, ref_counter)
    return new_filename
