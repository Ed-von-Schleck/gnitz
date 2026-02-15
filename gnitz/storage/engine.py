import os
import errno
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.storage.memtable_node import node_get_weight
from gnitz.storage import wal, manifest, index, mmap_posix, comparator
from gnitz.core import types

class Engine(object):
    """
    The DBSP Execution Engine.
    Coordinates the Write-Head (MemTable) and the Persistent Tail (ShardIndex).
    """
    _immutable_fields_ = [
        'mem_manager', 'index', 'schema', 
        'manifest_manager', 'table_id'
    ]

    def __init__(self, mem_manager, shard_index, manifest_manager=None, table_id=1, recover_wal_filename=None, validate_checksums=False):
        self.mem_manager = mem_manager
        self.index = shard_index  # Unified ShardIndex replaces Spine and Registry
        self.schema = mem_manager.schema
        self.manifest_manager = manifest_manager
        self.table_id = table_id
        self.validate_checksums = validate_checksums
        
        # Initialize LSN from Manifest authority
        if manifest_manager and manifest_manager.exists():
            reader = manifest_manager.load_current()
            self.current_lsn = reader.global_max_lsn + r_uint64(1)
            reader.close()
        else:
            self.current_lsn = r_uint64(1)

        # Recover uncommitted state from WAL
        if recover_wal_filename:
            self.recover_from_wal(recover_wal_filename)
        
        self.mem_manager.current_lsn = self.current_lsn
        self.mem_manager.starting_lsn = self.current_lsn

    def recover_from_wal(self, filename):
        """Reconstructs MemTable state by replaying Z-Set deltas from the WAL."""
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

    def get_effective_weight_raw(self, key, field_values):
        """
        Calculates the net algebraic weight of a record across all layers.
        Identity: W_net = W_mem + sum(W_shards)
        """
        # 1. Check mutable layer (MemTable)
        mem_weight = 0
        table = self.mem_manager.active_table
        match_off = table._find_exact_values(key, field_values)
        if match_off != 0:
            mem_weight = node_get_weight(table.arena.base_ptr, match_off)

        # 2. Check persistent layer (ShardIndex)
        spine_weight = 0
        # The index prunes shards by Min/Max PK before doing binary search
        results = self.index.find_all_shards_and_indices(key)
        
        is_u128 = self.schema.get_pk_column().field_type.size == 16
        
        for shard_handle, start_idx in results:
            curr_idx = start_idx
            view = shard_handle.view
            
            # Shards may contain multiple payloads per PK (Z-Set multiset semantics).
            # We must scan consecutive entries matching the PK.
            while curr_idx < view.count:
                if is_u128:
                    k = view.get_pk_u128(curr_idx)
                else:
                    k = r_uint128(view.get_pk_u64(curr_idx))
                
                if k != key:
                    break 
                
                # Perform full-row semantic equality check
                if comparator.compare_soa_to_values(self.schema, view, curr_idx, field_values) == 0:
                    spine_weight += shard_handle.view.get_weight(curr_idx)
                    break # PK+Payload is unique within a single Shard
                
                curr_idx += 1
                
        return mem_weight + spine_weight

    def flush_and_rotate(self, filename):
        """
        Transitions MemTable to an immutable Shard.
        Updates the index and the Manifest atomically.
        """
        # Capture LSN range before rotation
        lsn_max = self.mem_manager.current_lsn - r_uint64(1)
        lsn_min = self.mem_manager.starting_lsn
        
        # 1. Materialize MemTable to columnar file
        self.mem_manager.flush_and_rotate(filename)
        
        # 2. Register the new shard with the Unified Index
        if os.path.exists(filename):
            new_handle = index.ShardHandle(
                filename, 
                self.schema, 
                lsn_min,   # Capture start of MemTable epoch
                lsn_max,   # Capture end of MemTable epoch
                validate_checksums=self.validate_checksums
            )
            # Only index if shard contains data
            if new_handle.view.count > 0:
                self.index.add_handle(new_handle)
            else:
                new_handle.close()
                os.unlink(filename)
            
        # 3. Synchronize Manifest with the updated Index state
        if self.manifest_manager:
            # We generate the new manifest entries directly from the index handles
            metadata_entries = self.index.get_metadata_list()
            
            # Map index-metadata back to ManifestEntry objects for serialization
            manifest_entries = []
            for meta in metadata_entries:
                manifest_entries.append(manifest.ManifestEntry(
                    meta.table_id,
                    meta.filename,
                    meta.get_min_key(),
                    meta.get_max_key(),
                    meta.min_lsn,
                    meta.max_lsn
                ))
            
            self.manifest_manager.publish_new_version(manifest_entries, lsn_max)
            
        # 4. Prepare next epoch
        self.current_lsn = self.mem_manager.current_lsn
        self.mem_manager.starting_lsn = self.current_lsn
        
        # Return health status to trigger potential background compaction
        return self.index.needs_compaction

    def close(self):
        """Graceful shutdown of memory and handles."""
        self.mem_manager.close()
        self.index.close_all()
