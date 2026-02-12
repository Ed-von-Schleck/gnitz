import os
import errno
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.storage.memtable_node import node_get_weight
from gnitz.storage import wal, manifest, errors, spine, mmap_posix, comparator
from gnitz.core import types

class Engine(object):
    _immutable_fields_ = ['mem_manager', 'spine', 'schema', 'manifest_manager', 'registry', 'table_id']

    def __init__(self, mem_manager, spine_obj, manifest_manager=None, registry=None, table_id=1, recover_wal_filename=None, validate_checksums=False):
        self.mem_manager = mem_manager
        self.spine = spine_obj
        self.schema = mem_manager.schema
        self.manifest_manager = manifest_manager
        self.registry = registry
        self.table_id = table_id
        self.validate_checksums = validate_checksums
        
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
                for rec in records:
                    self.mem_manager.active_table.upsert(rec.primary_key, rec.weight, rec.component_data)
            
            self.current_lsn = r_uint64(last_recovered_lsn + 1)
            self.mem_manager.current_lsn = self.current_lsn
            reader.close()
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise e

    def get_effective_weight_raw(self, key, field_values):
        """
        Calculates net weight for a specific key and payload across the 
        hierarchy using DBValue comparisons.
        """
        # 1. Check MemTable
        mem_weight = 0
        table = self.mem_manager.active_table
        match_off = table._find_exact_values(key, field_values)
        if match_off != 0:
            mem_weight = node_get_weight(table.arena.base_ptr, match_off)

        # 2. Check Persistent Shards
        spine_weight = 0
        results = self.spine.find_all_shards_and_indices(key)
        for shard_handle, row_idx in results:
            if comparator.compare_soa_to_values(self.schema, shard_handle.view, row_idx, field_values) == 0:
                spine_weight += shard_handle.get_weight(row_idx)
                
        return mem_weight + spine_weight

    def flush_and_rotate(self, filename):
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
            handle = ShardHandle(filename, self.schema, lsn_max, validate_checksums=self.validate_checksums)
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
            
        self.mem_manager.starting_lsn = self.current_lsn
        return min_k, max_k, needs_comp

    def close(self):
        self.mem_manager.close()
        self.spine.close_all()
