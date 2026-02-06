"""
gnitz/storage/engine.py
"""
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage.memtable import node_get_weight, node_get_entity_id, node_get_next_off, node_get_payload_ptr

class Engine(object):
    def __init__(self, mem_manager, spine):
        self.mem_manager = mem_manager
        self.spine = spine
        self.layout = mem_manager.layout

    def get_effective_weight(self, entity_id):
        # 1. MemTable Lookup
        base = self.mem_manager.active_table.arena.base_ptr
        curr_off = self.mem_manager.active_table.head_off
        
        mem_weight = 0
        for i in range(15, -1, -1):
            next_off = node_get_next_off(base, curr_off, i)
            while next_off != 0:
                eid = node_get_entity_id(base, next_off)
                if eid < entity_id:
                    curr_off = next_off
                    next_off = node_get_next_off(base, curr_off, i)
                else:
                    break
        
        next_off = node_get_next_off(base, curr_off, 0)
        if next_off != 0 and node_get_entity_id(base, next_off) == entity_id:
            mem_weight = node_get_weight(base, next_off)

        # 2. Spine Lookup (Aggregation)
        spine_weight = 0
        # Use new method to find ALL contributing shards
        results = self.spine.find_all_shards_and_indices(entity_id)
        for shard, row_idx in results:
            spine_weight += shard.get_weight(row_idx)

        return mem_weight + spine_weight

    def read_component_i64(self, entity_id, field_idx):
        # 1. MemTable (Highest Priority)
        base = self.mem_manager.active_table.arena.base_ptr
        curr_off = self.mem_manager.active_table.head_off
        
        for i in range(15, -1, -1):
            next_off = node_get_next_off(base, curr_off, i)
            while next_off != 0:
                eid = node_get_entity_id(base, next_off)
                if eid < entity_id:
                    curr_off = next_off
                    next_off = node_get_next_off(base, curr_off, i)
                else:
                    break

        next_off = node_get_next_off(base, curr_off, 0)
        if next_off != 0 and node_get_entity_id(base, next_off) == entity_id:
            payload_ptr = node_get_payload_ptr(base, next_off)
            f_off = self.layout.get_field_offset(field_idx)
            return rffi.cast(rffi.LONGLONGP, rffi.ptradd(payload_ptr, f_off))[0]

        # 2. Spine Lookup
        # We need the value from the shard that is "latest".
        # Without explicit LSNs in handles, we assume the last handle in the list is the newest
        # (Manifest appends new shards).
        results = self.spine.find_all_shards_and_indices(entity_id)
        
        # If multiple shards have it, we currently take the last one found.
        # This behavior depends on the order handles are stored in the Spine.
        val = 0
        if len(results) > 0:
             # Iterate to get the last one
             for shard, row_idx in results:
                 val = shard.read_field_i64(row_idx, field_idx)
        
        return val

    def close(self):
        self.mem_manager.close()
        self.spine.close_all()
