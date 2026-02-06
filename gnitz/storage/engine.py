"""
gnitz/storage/engine.py
"""
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage.memtable import (
    node_get_weight, node_get_entity_id, node_get_next_off, 
    node_get_payload_ptr, skip_list_find
)

class Engine(object):
    def __init__(self, mem_manager, spine, manifest_manager=None, registry=None, component_id=1, current_lsn=1):
        self.mem_manager = mem_manager
        self.spine = spine
        self.layout = mem_manager.layout
        self.manifest_manager = manifest_manager
        self.registry = registry
        self.component_id = component_id
        self.current_lsn = current_lsn

    def get_effective_weight(self, entity_id):
        # 1. MemTable Lookup (Unified navigation)
        base = self.mem_manager.active_table.arena.base_ptr
        head = self.mem_manager.active_table.head_off
        
        mem_weight = 0
        pred_off = skip_list_find(base, head, entity_id)
        next_off = node_get_next_off(base, pred_off, 0)
        
        if next_off != 0 and node_get_entity_id(base, next_off) == entity_id:
            mem_weight = node_get_weight(base, next_off)

        # 2. Spine Lookup (Aggregation)
        spine_weight = 0
        results = self.spine.find_all_shards_and_indices(entity_id)
        for shard, row_idx in results:
            spine_weight += shard.get_weight(row_idx)

        return mem_weight + spine_weight

    def read_component_i64(self, entity_id, field_idx):
        # 1. MemTable Lookup (Unified navigation)
        base = self.mem_manager.active_table.arena.base_ptr
        head = self.mem_manager.active_table.head_off
        
        pred_off = skip_list_find(base, head, entity_id)
        next_off = node_get_next_off(base, pred_off, 0)
        
        if next_off != 0 and node_get_entity_id(base, next_off) == entity_id:
            payload_ptr = node_get_payload_ptr(base, next_off)
            f_off = self.layout.get_field_offset(field_idx)
            return rffi.cast(rffi.LONGLONGP, rffi.ptradd(payload_ptr, f_off))[0]

        # 2. Spine Lookup
        results = self.spine.find_all_shards_and_indices(entity_id)
        
        val = 0
        # Last-Write-Wins: results are sorted by the Spine naturally if the shards were registered in order
        if len(results) > 0:
             for shard, row_idx in results:
                 val = shard.read_field_i64(row_idx, field_idx)
        return val

    def flush_and_rotate(self, filename):
        min_eid, max_eid = self.mem_manager.flush_and_rotate(filename)
        if min_eid == -1:
            return False
        
        if self.registry is not None:
            from gnitz.storage.shard_registry import ShardMetadata
            new_meta = ShardMetadata(filename, self.component_id, min_eid, max_eid, self.current_lsn, self.current_lsn)
            self.registry.register_shard(new_meta)
        
        if self.manifest_manager is not None:
            from gnitz.storage.manifest import ManifestEntry
            existing_entries = []
            if self.manifest_manager.exists():
                reader = self.manifest_manager.load_current()
                for e in reader.iterate_entries(): existing_entries.append(e)
                reader.close()
            
            new_entry = ManifestEntry(self.component_id, filename, min_eid, max_eid, self.current_lsn, self.current_lsn)
            existing_entries.append(new_entry)
            self.manifest_manager.publish_new_version(existing_entries)
        
        self.current_lsn += 1
        compaction_triggered = False
        if self.registry is not None:
            if self.registry.mark_for_compaction(self.component_id):
                compaction_triggered = True
        
        return compaction_triggered

    def close(self):
        self.mem_manager.close()
        self.spine.close_all()
