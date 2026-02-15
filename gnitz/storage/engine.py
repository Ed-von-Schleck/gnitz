# gnitz/storage/engine.py

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
            max_lsn_seen = self.current_lsn - r_uint64(1)
            last_recovered_lsn = max_lsn_seen
            
            while True:
                block = reader.read_next_block()
                if block is None: 
                    break
                
                if block.tid != self.table_id: 
                    continue
                if block.lsn <= max_lsn_seen: 
                    continue
                
                last_recovered_lsn = block.lsn
                for rec in block.records:
                    self.mem_manager.active_table.upsert(rec.get_key(), rec.weight, rec.component_data)
            
            self.current_lsn = last_recovered_lsn + r_uint64(1)
            self.mem_manager.current_lsn = self.current_lsn
            reader.close()
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise e
        except Exception as e:
            import os
            os.write(2, "FATAL ERROR during WAL recovery: " + str(e) + "\n")
            raise e

    def get_effective_weight_raw(self, key, field_values):
        mem_weight = 0
        table = self.mem_manager.active_table
        match_off = table._find_exact_values(key, field_values)
        if match_off != 0:
            mem_weight = node_get_weight(table.arena.base_ptr, match_off)

        spine_weight = 0
        results = self.spine.find_all_shards_and_indices(key)
        
        is_u128 = self.schema.get_pk_column().field_type.size == 16
        
        for shard_handle, start_idx in results:
            curr_idx = start_idx
            view = shard_handle.view
            
            while curr_idx < view.count:
                if is_u128:
                    k = view.get_pk_u128(curr_idx)
                else:
                    k = r_uint128(view.get_pk_u64(curr_idx))
                
                if k != key:
                    break 
                
                if comparator.compare_soa_to_values(self.schema, view, curr_idx, field_values) == 0:
                    spine_weight += shard_handle.get_weight(curr_idx)
                    break 
                
                curr_idx += 1
                
        return mem_weight + spine_weight

    def flush_and_rotate(self, filename):
        """
        Flushes MemTable to disk and rotates state.
        FIXED: Returns only 'needs_comp' to avoid unaligned 128-bit tuple field segfaults.
        """
        self.current_lsn = self.mem_manager.current_lsn
        lsn_min = self.mem_manager.starting_lsn
        lsn_max = self.current_lsn - r_uint64(1)
        
        self.mem_manager.flush_and_rotate(filename)
        
        entries = []
        if self.manifest_manager and self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            it = reader.iterate_entries()
            while True:
                try:
                    e = it.next()
                    entries.append(e)
                except StopIteration:
                    break
            reader.close()
        
        from gnitz.storage.spine import ShardHandle
        needs_comp = False
        
        if os.path.exists(filename):
            handle = ShardHandle(filename, self.schema, lsn_max, validate_checksums=self.validate_checksums)
            if handle.view.count > 0:
                min_k = handle.get_min_key()
                max_k = handle.get_max_key()
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
        return needs_comp

    def close(self):
        self.mem_manager.close()
        self.spine.close_all()
