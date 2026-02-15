# gnitz/storage/engine.py

import os
import errno
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.storage.memtable import MemTable
from gnitz.storage.memtable_node import node_get_weight
from gnitz.storage.wal_format import WALRecord
from gnitz.storage import wal, manifest, index, mmap_posix, comparator
from gnitz.core import types

class Engine(object):
    """
    The DBSP Execution Engine.
    Coordinates the Write-Head (MemTable) and the Persistent Tail (ShardIndex).
    
    Acts as the single authoritative manager for:
    1. Mutable State (MemTable + WAL)
    2. Immutable State (ShardIndex + Manifest)
    3. The transition between them (Flush/Rotate)
    """
    _immutable_fields_ = [
        'index', 'schema', 'manifest_manager', 'table_id', 
        'memtable_capacity', 'wal_writer'
    ]

    def __init__(self, schema, shard_index, memtable_capacity, wal_writer=None, 
                 manifest_manager=None, table_id=1, recover_wal_filename=None, 
                 validate_checksums=False):
        
        self.schema = schema
        self.index = shard_index
        self.memtable_capacity = memtable_capacity
        self.wal_writer = wal_writer
        self.manifest_manager = manifest_manager
        self.table_id = table_id
        self.validate_checksums = validate_checksums
        
        # Initialize the mutable write buffer
        self.active_table = MemTable(self.schema, self.memtable_capacity)

        # Initialize LSN from Manifest authority (Persistent High-Water Mark)
        if manifest_manager and manifest_manager.exists():
            reader = manifest_manager.load_current()
            self.current_lsn = reader.global_max_lsn + r_uint64(1)
            reader.close()
        else:
            self.current_lsn = r_uint64(1)

        # The LSN where the current MemTable began (used for Shard Metadata)
        self.starting_lsn = self.current_lsn

        # Recover uncommitted state from WAL (Volatile High-Water Mark)
        if recover_wal_filename:
            self.recover_from_wal(recover_wal_filename)
        
        # After recovery, ensure starting_lsn aligns with the next write
        # Note: If we recovered data, starting_lsn remains what it was 
        # (effectively the start of the log), or strictly follows manifest if log was empty.
        
    def ingest(self, key, weight, field_values):
        """
        Ingests a Z-Set delta into the system.
        1. Assigns LSN.
        2. Persists to WAL (Durability).
        3. Applies to MemTable (Visibility).
        """
        lsn = self.current_lsn
        self.current_lsn += r_uint64(1)
        
        # 1. Write-Ahead Log
        if self.wal_writer:
            # Create a WAL record (Key, Weight, Payload)
            rec = WALRecord.from_key(key, weight, field_values)
            self.wal_writer.append_block(lsn, self.table_id, [rec])
            
        # 2. Update In-Memory State
        self.active_table.upsert(r_uint128(key), weight, field_values)

    def recover_from_wal(self, filename):
        """Reconstructs MemTable state by replaying Z-Set deltas from the WAL."""
        try:
            reader = wal.WALReader(filename, self.schema)
            # We ignore any WAL entries covered by the Manifest (snapshot)
            max_lsn_seen = self.current_lsn - r_uint64(1)
            last_recovered_lsn = max_lsn_seen
            
            while True:
                block = reader.read_next_block()
                if block is None: 
                    break
                
                # Filter by Table ID and LSN > Manifest.MaxLSN
                if block.tid != self.table_id: 
                    continue
                if block.lsn <= max_lsn_seen: 
                    continue
                
                last_recovered_lsn = block.lsn
                for rec in block.records:
                    self.active_table.upsert(rec.get_key(), rec.weight, rec.component_data)
            
            # Advance the global clock to the next available LSN
            self.current_lsn = last_recovered_lsn + r_uint64(1)
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
        table = self.active_table
        # Check specific payload in MemTable
        match_off = table._find_exact_values(key, field_values)
        if match_off != 0:
            mem_weight = node_get_weight(table.arena.base_ptr, match_off)

        # 2. Check persistent layer (ShardIndex)
        spine_weight = 0
        # The index prunes shards by Min/Max PK before doing binary search
        results = self.index.find_all_shards_and_indices(key)
        
        # Create reusable accessors for the shard scan loop
        soa_accessor = comparator.SoAAccessor(self.schema)
        value_accessor = comparator.ValueAccessor(self.schema)
        value_accessor.set_row(field_values) # Set once for the target values

        is_u128 = self.schema.get_pk_column().field_type.size == 16
        
        for shard_handle, start_idx in results:
            curr_idx = start_idx
            view = shard_handle.view
            
            # Shards may contain multiple payloads per PK.
            # Scan consecutive entries matching the PK.
            while curr_idx < view.count:
                if is_u128:
                    k = view.get_pk_u128(curr_idx)
                else:
                    k = r_uint128(view.get_pk_u64(curr_idx))
                
                if k != key:
                    break 
                
                # Set the SoA accessor for the current row in the shard
                soa_accessor.set_row(view, curr_idx)
                
                # Use the unified comparator for full-row semantic equality check
                if comparator.compare_rows(self.schema, soa_accessor, value_accessor) == 0:
                    spine_weight += shard_handle.view.get_weight(curr_idx)
                    break # PK+Payload is unique within a single Shard
                
                curr_idx += 1
                
        return mem_weight + spine_weight

    def flush_and_rotate(self, filename):
        """
        Transitions MemTable to an immutable Shard.
        1. Writes MemTable to N-Partition file.
        2. Updates Index and Manifest atomically.
        3. Frees old MemTable memory and allocates a new one.
        """
        # Capture LSN range for this epoch
        lsn_max = self.current_lsn - r_uint64(1)
        lsn_min = self.starting_lsn
        
        # 1. Materialize MemTable to columnar file
        self.active_table.flush(filename, self.table_id)
        
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
            
        # 4. Rotate Memory: Free old, allocate new
        self.active_table.free()
        self.active_table = MemTable(self.schema, self.memtable_capacity)
        
        # 5. Advance Epoch Tracking
        self.starting_lsn = self.current_lsn
        
        # Return health status to trigger potential background compaction
        return self.index.needs_compaction

    def close(self):
        """Graceful shutdown of memory and handles."""
        if self.active_table:
            self.active_table.free()
            self.active_table = None
            
        self.index.close_all()
