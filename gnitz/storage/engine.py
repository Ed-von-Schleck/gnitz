"""
gnitz/storage/engine.py
"""
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage.memtable import node_get_weight, node_get_entity_id, node_get_next_off, node_get_payload_ptr

class Engine(object):
    def __init__(self, mem_manager, spine, manifest_manager=None, registry=None, component_id=1, current_lsn=1):
        """
        Initialize the Engine with integrated manifest and compaction support.
        
        Args:
            mem_manager: MemTableManager instance
            spine: Spine instance
            manifest_manager: ManifestManager instance (optional)
            registry: ShardRegistry instance (optional)
            component_id: Component type ID for this engine (default: 1)
            current_lsn: Starting LSN for new shards (default: 1)
        """
        self.mem_manager = mem_manager
        self.spine = spine
        self.layout = mem_manager.layout
        self.manifest_manager = manifest_manager
        self.registry = registry
        self.component_id = component_id
        self.current_lsn = current_lsn

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
        results = self.spine.find_all_shards_and_indices(entity_id)
        
        val = 0
        if len(results) > 0:
             # Iterate to get the last one
             for shard, row_idx in results:
                 val = shard.read_field_i64(row_idx, field_idx)
        
        return val

    def flush_and_rotate(self, filename):
        """
        Integrated flush operation that:
        1. Flushes MemTable to a new shard file
        2. Registers the new shard in the registry
        3. Updates the manifest with the new entry
        4. Checks if compaction is needed
        5. Optionally triggers compaction
        
        Args:
            filename: Shard file to create
            
        Returns:
            Boolean indicating if compaction was triggered
        """
        # 1. Flush MemTable and get metadata
        min_eid, max_eid = self.mem_manager.flush_and_rotate(filename)
        
        # If no entities were written, skip manifest/registry updates
        if min_eid == -1:
            return False
        
        # 2. Register new shard in registry (if available)
        if self.registry is not None:
            from gnitz.storage.shard_registry import ShardMetadata
            new_meta = ShardMetadata(
                filename, 
                self.component_id, 
                min_eid, 
                max_eid, 
                self.current_lsn, 
                self.current_lsn
            )
            self.registry.register_shard(new_meta)
        
        # 3. Update manifest (if available)
        if self.manifest_manager is not None:
            from gnitz.storage.manifest import ManifestEntry
            
            # Load current manifest entries
            if self.manifest_manager.exists():
                reader = self.manifest_manager.load_current()
                existing_entries = []
                for e in reader.iterate_entries():
                     existing_entries.append(e)
                reader.close()
            else:
                existing_entries = []
            
            # Add new entry
            new_entry = ManifestEntry(
                self.component_id,
                filename,
                min_eid,
                max_eid,
                self.current_lsn,
                self.current_lsn
            )
            existing_entries.append(new_entry)
            
            # Publish updated manifest
            self.manifest_manager.publish_new_version(existing_entries)
        
        # Increment LSN for next shard
        self.current_lsn += 1
        
        # 4. Check if compaction is needed
        compaction_triggered = False
        if self.registry is not None:
            if self.registry.mark_for_compaction(self.component_id):
                compaction_triggered = True
        
        return compaction_triggered

    def close(self):
        self.mem_manager.close()
        self.spine.close_all()
