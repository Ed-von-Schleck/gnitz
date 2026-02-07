from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage.memtable import (
    node_get_weight, node_get_entity_id, node_get_next_off, 
    node_get_payload_ptr, skip_list_find
)
from gnitz.storage import wal

class Engine(object):
    _immutable_fields_ = ['mem_manager', 'spine', 'layout', 'manifest_manager', 'registry', 'component_id', 'current_lsn']

    def __init__(self, mem_manager, spine, manifest_manager=None, registry=None, component_id=1, current_lsn=1, recover_wal_filename=None):
        self.mem_manager = mem_manager
        self.spine = spine
        self.layout = mem_manager.layout
        self.manifest_manager = manifest_manager
        self.registry = registry
        self.component_id = component_id
        self.current_lsn = current_lsn
        
        if recover_wal_filename:
            self.recover_from_wal(recover_wal_filename)

    def recover_from_wal(self, filename):
        """
        Replays the Write-Ahead Log to restore MemTable state.
        This reads the log and injects data directly into the MemTable,
        bypassing the WAL writer (to avoid duplication).
        """
        try:
            reader = wal.WALReader(filename, self.layout)
        except OSError:
            # File might not exist yet, which is fine for a fresh start
            return

        max_lsn_found = 0
        try:
            for lsn, comp_id, records in reader.iterate_blocks():
                # Filter by component ID (basic check)
                if comp_id != self.component_id:
                    continue
                
                # Update max LSN seen
                if lsn > max_lsn_found:
                    max_lsn_found = lsn
                
                # Apply records
                for eid, weight, c_data in records:
                    self.mem_manager.put_from_recovery(eid, weight, c_data)
        finally:
            reader.close()
        
        # Advance current LSN to be ahead of what we recovered
        if max_lsn_found >= self.current_lsn:
            self.current_lsn = max_lsn_found + 1
            # Also update MemManager's LSN tracker
            self.mem_manager.current_lsn = self.current_lsn

    def get_effective_weight(self, entity_id):
        base = self.mem_manager.active_table.arena.base_ptr
        head = self.mem_manager.active_table.head_off
        
        mem_weight = 0
        pred_off = skip_list_find(base, head, entity_id)
        next_off = node_get_next_off(base, pred_off, 0)
        if next_off != 0 and node_get_entity_id(base, next_off) == entity_id:
            mem_weight = node_get_weight(base, next_off)

        spine_weight = 0
        results = self.spine.find_all_shards_and_indices(entity_id)
        for shard, row_idx in results:
            spine_weight += shard.get_weight(row_idx)

        return mem_weight + spine_weight

    def read_component_i64(self, entity_id, field_idx):
        # 1. Check MemTable (always has highest LSN)
        base = self.mem_manager.active_table.arena.base_ptr
        head = self.mem_manager.active_table.head_off
        
        pred_off = skip_list_find(base, head, entity_id)
        next_off = node_get_next_off(base, pred_off, 0)
        
        if next_off != 0 and node_get_entity_id(base, next_off) == entity_id:
            payload_ptr = node_get_payload_ptr(base, next_off)
            f_off = self.layout.get_field_offset(field_idx)
            return rffi.cast(rffi.LONGLONGP, rffi.ptradd(payload_ptr, f_off))[0]

        # 2. Check Spine with LSN-based resolution
        results = self.spine.find_all_shards_and_indices(entity_id)
        
        max_lsn = -1
        latest_val = rffi.cast(rffi.LONGLONG, 0)
        
        for i in range(len(results)):
            shard_handle, row_idx = results[i]
            if shard_handle.lsn > max_lsn:
                max_lsn = shard_handle.lsn
                latest_val = shard_handle.read_field_i64(row_idx, field_idx)
        
        return rffi.cast(lltype.Signed, latest_val)

    def flush_and_rotate(self, filename):
        min_eid, max_eid = self.mem_manager.flush_and_rotate(filename)
        if min_eid == -1: return False
        
        if self.registry is not None:
            from gnitz.storage.shard_registry import ShardMetadata
            new_meta = ShardMetadata(filename, self.component_id, min_eid, max_eid, self.current_lsn, self.current_lsn)
            self.registry.register_shard(new_meta)
        
        if self.manifest_manager is not None:
            from gnitz.storage.manifest import ManifestEntry
            entries = []
            if self.manifest_manager.exists():
                reader = self.manifest_manager.load_current()
                for e in reader.iterate_entries(): entries.append(e)
                reader.close()
            entries.append(ManifestEntry(self.component_id, filename, min_eid, max_eid, self.current_lsn, self.current_lsn))
            self.manifest_manager.publish_new_version(entries)
        
        self.current_lsn += 1
        if self.registry is not None:
            return self.registry.mark_for_compaction(self.component_id)
        return False

    def close(self):
        self.mem_manager.close()
        self.spine.close_all()
