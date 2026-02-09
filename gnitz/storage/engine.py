from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from gnitz.storage.memtable import (
    node_get_weight, node_get_entity_id, node_get_next_off, 
    node_get_payload_ptr, skip_list_find_exact, compare_payloads
)
from gnitz.storage import wal, manifest, errors, spine

class Engine(object):
    _immutable_fields_ = [
        'mem_manager', 'spine', 'layout', 
        'manifest_manager', 'registry', 'component_id'
    ]

    def __init__(self, mem_manager, spine, manifest_manager=None, 
                 registry=None, component_id=1, recover_wal_filename=None):
        self.mem_manager = mem_manager
        self.spine = spine
        self.layout = mem_manager.layout
        self.manifest_manager = manifest_manager
        self.registry = registry
        self.component_id = component_id
        
        if manifest_manager and manifest_manager.exists():
            reader = manifest_manager.load_current()
            self.current_lsn = reader.global_max_lsn + r_uint64(1)
            reader.close()
        else:
            self.current_lsn = r_uint64(1)

        if recover_wal_filename:
            self.recover_from_wal(recover_wal_filename)
        
        self.mem_manager.current_lsn = self.current_lsn
        self.mem_manager.starting_lsn = self.current_lsn

    def recover_from_wal(self, filename):
        try:
            reader = wal.WALReader(filename, self.layout)
        except OSError:
            return

        max_lsn_finalized = self.current_lsn - r_uint64(1)
        max_lsn_seen = max_lsn_finalized
        first_lsn_seen = r_uint64(0)
        has_seen = False
        
        expected_lsn = r_uint64(0)
        lsn_init = False

        for lsn, comp_id, records in reader.iterate_blocks():
            if not lsn_init:
                lsn_init = True
                expected_lsn = lsn
            
            if lsn != expected_lsn:
                reader.close()
                raise errors.CorruptShardError("LSN gap detected in WAL")
            
            expected_lsn = lsn + r_uint64(1)

            if comp_id != self.component_id:
                continue
            if lsn <= max_lsn_finalized:
                continue
            
            if not has_seen and lsn > (max_lsn_finalized + r_uint64(1)):
                reader.close()
                raise errors.CorruptShardError("Gap between Manifest LSN and WAL LSN")

            if not has_seen:
                first_lsn_seen = lsn
                has_seen = True
            
            if lsn > max_lsn_seen:
                max_lsn_seen = lsn
            
            for eid, weight, c_data in records:
                self.mem_manager.put_from_recovery(eid, weight, c_data)
        
        self.current_lsn = max_lsn_seen + r_uint64(1)
        self.mem_manager.current_lsn = self.current_lsn
        if has_seen:
            self.mem_manager.starting_lsn = first_lsn_seen
        reader.close()

    def get_effective_weight_raw(self, entity_id, packed_payload_ptr, payload_heap_ptr):
        mem_weight = 0
        base = self.mem_manager.active_table.arena.base_ptr
        blob_base = self.mem_manager.active_table.blob_arena.base_ptr
        head = self.mem_manager.active_table.head_off
        
        pred_off = skip_list_find_exact(
            base, head, entity_id, self.layout, packed_payload_ptr, payload_heap_ptr
        )
        curr_off = node_get_next_off(base, pred_off, 0)
        
        while curr_off != 0:
            if node_get_entity_id(base, curr_off) != entity_id:
                break
            payload = node_get_payload_ptr(base, curr_off)
            if compare_payloads(self.layout, payload, blob_base, packed_payload_ptr, payload_heap_ptr) == 0:
                mem_weight += node_get_weight(base, curr_off)
            curr_off = node_get_next_off(base, curr_off, 0)

        spine_weight = 0
        results = self.spine.find_all_shards_and_indices(entity_id)
        for shard_handle, start_idx in results:
            idx = start_idx
            while idx < shard_handle.view.count:
                if shard_handle.view.get_entity_id(idx) != entity_id:
                    break
                s_payload = shard_handle.view.get_data_ptr(idx)
                s_blob = shard_handle.view.buf_b.ptr
                if compare_payloads(self.layout, s_payload, s_blob, packed_payload_ptr, payload_heap_ptr) == 0:
                     spine_weight += shard_handle.get_weight(idx)
                idx += 1
        return mem_weight + spine_weight

    def flush_and_rotate(self, filename):
        if not self.mem_manager.active_table.has_active_data():
            return r_uint64(0), r_uint64(0), False

        lsn_min = self.mem_manager.starting_lsn
        lsn_max = self.mem_manager.current_lsn - r_uint64(1)
        min_eid, max_eid = self.mem_manager.flush_and_rotate(filename)
        
        entries = []
        new_global_max = lsn_max
        if self.manifest_manager and self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            for e in reader.iterate_entries():
                entries.append(e)
            if reader.global_max_lsn > new_global_max:
                new_global_max = reader.global_max_lsn
            reader.close()
        
        if self.registry is not None:
            from gnitz.storage.shard_registry import ShardMetadata
            self.registry.register_shard(ShardMetadata(filename, self.component_id, min_eid, max_eid, lsn_min, lsn_max))
        
        if self.manifest_manager is not None:
            from gnitz.storage.manifest import ManifestEntry
            entries.append(ManifestEntry(self.component_id, filename, min_eid, max_eid, lsn_min, lsn_max))
            self.manifest_manager.publish_new_version(entries, new_global_max)
        
        self.current_lsn = self.mem_manager.current_lsn
        new_handle = spine.ShardHandle(filename, self.layout, lsn_max)
        self.spine = spine.Spine(self.spine.handles + [new_handle], self.spine.ref_counter)
        
        needs_compaction = False
        if self.registry is not None:
            needs_compaction = self.registry.mark_for_compaction(self.component_id)
        return min_eid, max_eid, needs_compaction

    def close(self):
        self.mem_manager.close()
        self.spine.close_all()
