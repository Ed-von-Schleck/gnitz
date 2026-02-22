# gnitz/storage/engine.py

import os
import errno
from rpython.rlib.rarithmetic import r_uint64, r_int64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint

from gnitz.storage.cursor import MemTableCursor, ShardCursor, UnifiedCursor
from gnitz.storage.memtable import MemTable
from gnitz.storage.memtable_node import node_get_weight
from gnitz.storage.wal_format import WALRecord
from gnitz.storage import wal, index, comparator as storage_comparator
from gnitz.core import types, values, comparator as core_comparator

class Engine(object):
    """
    The DBSP Execution Engine.
    
    Acts as the central authority for:
    1. Mutable State: Ingestion via MemTable and WAL.
    2. Immutable State: Reading via ShardIndex.
    3. Persistence: Managing the Flush/Rotate lifecycle and Manifest updates.
    """
    _immutable_fields_ = [
        "schema",
        "index",
        "memtable_capacity",
        "wal_writer",
        "manifest_manager",
        "table_id",
        "validate_checksums",
        "value_accessor",
        "soa_accessor",
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
        
        # Pre-allocate accessors to avoid allocation in hot read/merge loops.
        self.value_accessor = core_comparator.ValueAccessor(self.schema)
        # soa_accessor is used for reading from ShardView (SoA format).
        self.soa_accessor = storage_comparator.SoAAccessor(self.schema)
        
        # Initialize the mutable write buffer
        self.active_table = MemTable(self.schema, self.memtable_capacity)

        # Initialize LSN from Manifest authority (Persistent High-Water Mark)
        if manifest_manager and manifest_manager.exists():
            reader = manifest_manager.load_current()
            self.current_lsn = reader.global_max_lsn + r_uint64(1)
            reader.close()
        else:
            self.current_lsn = r_uint64(1)

        # The LSN where the current MemTable began.
        self.starting_lsn = self.current_lsn

        # Recover uncommitted state from WAL (Volatile High-Water Mark)
        if recover_wal_filename:
            self.recover_from_wal(recover_wal_filename)
        
    def ingest(self, key, weight, row):
        """
        Ingests a single Z-Set delta. 
        Note: For high-velocity ingestion, use ingest_batch.
        """
        # Fix: Avoid mr-poisoning by using newlist_hint instead of literals
        pks = newlist_hint(1)
        pks.append(r_uint128(key))
        weights = newlist_hint(1)
        weights.append(weight)
        rows = newlist_hint(1)
        rows.append(row)
        self.ingest_batch(pks, weights, rows)

    def ingest_batch(self, pks, weights, rows):
        """
        Ingests a batch of Z-Set deltas as a single atomic DBSP tick.
        
        Args:
            pks: List[r_uint128]
            weights: List[r_int64]
            rows: List[PayloadRow]
        """
        lsn = self.current_lsn
        self.current_lsn += r_uint64(1)

        # 1. Write-Ahead Log (Durability)
        if self.wal_writer:
            count = len(pks)
            # Use newlist_hint to satisfy RPython resizability constraints
            wal_records = newlist_hint(count)
            for i in range(count):
                w = weights[i]
                # Only log records that contribute state (Ghost Property)
                if w != r_int64(0):
                    wal_records.append(WALRecord(pks[i], w, rows[i]))
            
            if len(wal_records) > 0:
                self.wal_writer.append_batch(lsn, self.table_id, wal_records)
            
        # 2. Update In-Memory State (Visibility)
        for i in range(len(pks)):
            w = weights[i]
            # Ghost Property: Algebraic annihilation prevents zero-weight records
            # from consuming Arena memory or SkipList nodes.
            if w != r_int64(0):
                self.active_table.upsert(pks[i], w, rows[i])

    def recover_from_wal(self, filename):
        """
        Reconstructs MemTable state by replaying Z-Set deltas from the WAL.
        Utilizes zero-copy recovery path for high startup performance.
        """
        if not os.path.exists(filename):
            return

        try:
            reader = wal.WALReader(filename, self.schema)
            # Volatile state starts after what is already persisted in Shards
            max_lsn_seen = self.starting_lsn - r_uint64(1)
            last_recovered_lsn = max_lsn_seen
            
            while True:
                block = reader.read_next_block()
                if block is None: 
                    break
                
                try:
                    # Check TID and LSN to determine if this block belongs to us
                    if block.tid != self.table_id or block.lsn <= max_lsn_seen: 
                        continue
                    
                    if block.lsn > last_recovered_lsn:
                        last_recovered_lsn = block.lsn

                    for rec in block.records:
                        # Optimized zero-copy path: moves bytes from WAL buffer directly
                        # to MemTable node without intermediate PayloadRow objects.
                        self.active_table.upsert_raw(rec.pk, rec.weight, rec)
                finally:
                    # CRITICAL: Free the block's raw buffer after records are ingested
                    block.free()
            
            # Update high-water mark so subsequent writes use the correct LSN sequence
            if last_recovered_lsn >= self.current_lsn:
                self.current_lsn = last_recovered_lsn + r_uint64(1)
            
            reader.close()
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise e

    def get_effective_weight_raw(self, key, row):
        """
        Calculates the net algebraic weight of a record across all layers.
        Identity: W_net = W_mem + sum(W_shards)
        """
        r_key = r_uint128(key)
        
        # 1. Check mutable layer (MemTable)
        mem_weight = r_int64(0)
        table = self.active_table
        
        match_off = table._find_exact_values(r_key, row)
        if match_off != 0:
            mem_weight = node_get_weight(table.arena.base_ptr, match_off)

        # 2. Check persistent layer (ShardIndex)
        spine_weight = r_int64(0)
        results = self.index.find_all_shards_and_indices(r_key)
        
        self.value_accessor.set_row(row)
        is_u128 = self.schema.get_pk_column().field_type.size == 16
        
        for shard_handle, start_idx in results:
            curr_idx = start_idx
            view = shard_handle.view
            
            while curr_idx < view.count:
                if is_u128:
                    k = view.get_pk_u128(curr_idx)
                else:
                    k = r_uint128(view.get_pk_u64(curr_idx))
                
                if k != r_key:
                    break 
                
                self.soa_accessor.set_row(view, curr_idx)
                if core_comparator.compare_rows(self.schema, self.soa_accessor, self.value_accessor) == 0:
                    spine_weight += shard_handle.view.get_weight(curr_idx)
                
                curr_idx += 1
                
        return mem_weight + spine_weight

    def flush_and_rotate(self, filename):
        """
        Transitions MemTable to an immutable Shard.
        """
        if self.current_lsn == self.starting_lsn:
            return self.index.needs_compaction

        lsn_max = self.current_lsn - r_uint64(1)
        lsn_min = self.starting_lsn
        
        self.active_table.flush(filename, self.table_id)
        
        if os.path.exists(filename):
            new_handle = index.ShardHandle(
                filename, 
                self.schema, 
                lsn_min,
                lsn_max,
                validate_checksums=self.validate_checksums
            )
            
            if new_handle.view.count > 0:
                self.index.add_handle(new_handle)
                if self.manifest_manager:
                    manifest_entries = self.index.get_metadata_list()
                    self.manifest_manager.publish_new_version(manifest_entries, lsn_max)
            else:
                new_handle.close()
                try:
                    os.unlink(filename)
                except OSError:
                    pass
                if self.manifest_manager:
                    manifest_entries = self.index.get_metadata_list()
                    self.manifest_manager.publish_new_version(manifest_entries, lsn_max)

        self.active_table.free()
        self.active_table = MemTable(self.schema, self.memtable_capacity)
        self.starting_lsn = self.current_lsn
        
        return self.index.needs_compaction

    def open_trace_cursor(self):
        """Returns a UnifiedCursor for the current net state."""
        # Fix: Avoid mr-poisoning by using newlist_hint
        num_shards = len(self.index.handles)
        cs = newlist_hint(1 + num_shards)
        
        cs.append(MemTableCursor(self.active_table))
        for i in range(num_shards):
            cs.append(ShardCursor(self.index.handles[i].view))
            
        return UnifiedCursor(self.schema, cs)

    def close(self):
        """Graceful shutdown of memory and handles."""
        if self.active_table:
            self.active_table.free()
            self.active_table = None
            
        self.index.close_all()
