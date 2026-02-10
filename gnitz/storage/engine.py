from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long
from gnitz.storage.memtable import (
    node_get_weight, node_get_next_off, 
    node_get_payload_ptr, compare_payloads
)
from gnitz.storage import wal, manifest, errors, spine, mmap_posix
from gnitz.core import types
import os

class Engine(object):
    _immutable_fields_ = ['mem_manager', 'spine', 'schema', 'manifest_manager', 'registry', 'table_id']

    def __init__(self, mem_manager, spine_obj, manifest_manager=None, registry=None, table_id=1, recover_wal_filename=None, **kwargs):
        self.mem_manager = mem_manager
        self.spine = spine_obj
        self.schema = mem_manager.schema
        self.manifest_manager = manifest_manager
        self.registry = registry
        self.table_id = kwargs.get('component_id', kwargs.get('table_id', table_id))
        
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
            reader = wal.WALReader(filename, self.schema)
            max_lsn_seen = int(self.current_lsn) - 1
            last_recovered_lsn = max_lsn_seen
            
            for lsn, tid, records in reader.iterate_blocks():
                if tid != self.table_id: continue
                if int(lsn) <= max_lsn_seen: continue
                
                last_recovered_lsn = int(lsn)
                for key, weight, field_values in records:
                    self.mem_manager.active_table.upsert(key, weight, field_values)
            
            self.current_lsn = r_uint64(last_recovered_lsn + 1)
            self.mem_manager.current_lsn = self.current_lsn
            reader.close()
        except OSError: pass

    def get_effective_weight_raw(self, key, packed_payload_ptr, payload_heap_ptr):
        mem_weight = 0
        table = self.mem_manager.active_table
        pred_off = table._find_exact(key, packed_payload_ptr, payload_heap_ptr)
        curr_off = node_get_next_off(table.arena.base_ptr, pred_off, 0)
        while curr_off != 0:
            if table._get_node_key(curr_off) != key: break
            payload = node_get_payload_ptr(table.arena.base_ptr, curr_off, table.key_size)
            if compare_payloads(self.schema, payload, table.blob_arena.base_ptr, packed_payload_ptr, payload_heap_ptr) == 0:
                mem_weight += node_get_weight(table.arena.base_ptr, curr_off)
            curr_off = node_get_next_off(table.arena.base_ptr, curr_off, 0)

        spine_weight = 0
        results = self.spine.find_all_shards_and_indices(key)
        for shard_handle, row_idx in results:
            idx = row_idx
            while idx < shard_handle.view.count:
                is_u128 = shard_handle.view.schema.get_pk_column().field_type.size == 16
                mid_key = shard_handle.view.get_pk_u128(idx) if is_u128 else r_uint128(shard_handle.view.get_pk_u64(idx))
                if mid_key != key: break
                
                match = True
                for i in range(len(self.schema.columns)):
                    if i == self.schema.pk_index: continue
                    col_ptr = shard_handle.view.get_col_ptr(idx, i)
                    p_ptr = rffi.ptradd(packed_payload_ptr, self.schema.get_column_offset(i))
                    ftype = self.schema.columns[i].field_type
                    
                    if ftype.code == 11:
                        from gnitz.core.strings import string_equals_dual
                        if not string_equals_dual(col_ptr, shard_handle.view.blob_buf.ptr, p_ptr, payload_heap_ptr):
                            match = False; break
                    else:
                        for b in range(ftype.size):
                            if col_ptr[b] != p_ptr[b]: match = False; break
                        if not match: break
                if match: spine_weight += shard_handle.get_weight(idx)
                idx += 1
        return mem_weight + spine_weight

    def get_effective_weight(self, key, packed_payload_ptr, payload_heap_ptr):
        return self.get_effective_weight_raw(key, packed_payload_ptr, payload_heap_ptr)

    def flush_and_rotate(self, filename):
        # SYNC POINT: Capture current ingestion LSN before flush
        self.current_lsn = self.mem_manager.current_lsn
        
        lsn_min = self.mem_manager.starting_lsn
        lsn_max = self.current_lsn - r_uint64(1)
        
        self.mem_manager.flush_and_rotate(filename)
        
        entries = []
        if self.manifest_manager and self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            for e in reader.iterate_entries(): entries.append(e)
            reader.close()
        
        from gnitz.storage.spine import ShardHandle
        min_k = r_uint128(0)
        max_k = r_uint128(0)
        needs_comp = False
        
        if os.path.exists(filename):
            handle = ShardHandle(filename, self.schema, lsn_max)
            if handle.view.count > 0:
                min_k = handle.min_key
                max_k = handle.max_key
                new_entry = manifest.ManifestEntry(self.table_id, filename, min_k, max_k, lsn_min, lsn_max)
                entries.append(new_entry)
                self.spine.add_handle(handle)
                
                if self.registry:
                    from gnitz.storage.shard_registry import ShardMetadata
                    self.registry.register_shard(ShardMetadata(filename, self.table_id, min_k, max_k, lsn_min, lsn_max))
                    needs_comp = self.registry.mark_for_compaction(self.table_id)
            else:
                handle.close()
                os.unlink(filename)
            
        if self.manifest_manager:
            self.manifest_manager.publish_new_version(entries, lsn_max)
            
        # Monotonic step: current_lsn is already synchronized at start of method
        self.mem_manager.starting_lsn = self.current_lsn
        return min_k, max_k, needs_comp

    def close(self):
        self.mem_manager.close()
        self.spine.close_all()
