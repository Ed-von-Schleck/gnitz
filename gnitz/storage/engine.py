# gnitz/storage/engine.py

import os
import errno
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.storage.cursor import MemTableCursor, ShardCursor, UnifiedCursor
from gnitz.storage.memtable import MemTable
from gnitz.storage.memtable_node import node_get_weight
from gnitz.storage.wal_format import WALRecord
from gnitz.storage import wal, index, comparator
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
        
        # Pre-allocate accessors to avoid allocation in hot read/merge loops
        self.value_accessor = comparator.ValueAccessor(self.schema)
        self.soa_accessor = comparator.SoAAccessor(self.schema)
        
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
        # This defines the lower bound for the next shard.
        self.starting_lsn = self.current_lsn

        # Recover uncommitted state from WAL (Volatile High-Water Mark)
        if recover_wal_filename:
            self.recover_from_wal(recover_wal_filename)
        
    def ingest(self, key, weight, row):
        """
        Ingests a Z-Set delta into the system.
        
        Args:
            key: The Primary Key (u64 or u128).
            weight: The algebraic weight (int64).
            row: PayloadRow representing the row payload.
        """
        lsn = self.current_lsn
        self.current_lsn += r_uint64(1)
        
        # 1. Write-Ahead Log (Durability)
        if self.wal_writer:
            rec = WALRecord(r_uint128(key), weight, row)
            self.wal_writer.write_record(lsn, self.table_id, rec)
            
        # 2. Update In-Memory State (Visibility)
        self.active_table.upsert(r_uint128(key), weight, row)

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
                    self.active_table.upsert(rec.get_key(), rec.weight, rec.component_data)
            
            self.current_lsn = last_recovered_lsn + r_uint64(1)
            reader.close()
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise e

    def get_effective_weight_raw(self, key, row):
        """
        Calculates the net algebraic weight of a record across all layers.
        Identity: W_net = W_mem + sum(W_shards)
        
        Args:
            row: PayloadRow
        """
        # 1. Check mutable layer (MemTable)
        mem_weight = 0
        table = self.active_table
        
        match_off = table._find_exact_values(key, row)
        if match_off != 0:
            mem_weight = node_get_weight(table.arena.base_ptr, match_off)

        # 2. Check persistent layer (ShardIndex)
        spine_weight = 0
        results = self.index.find_all_shards_and_indices(key)
        
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
                
                if k != key:
                    break 
                
                self.soa_accessor.set_row(view, curr_idx)
                
                if core_comparator.compare_rows(self.schema, self.soa_accessor, self.value_accessor) == 0:
                    spine_weight += shard_handle.view.get_weight(curr_idx)
                    break
                
                curr_idx += 1
                
        return mem_weight + spine_weight

    def flush_and_rotate(self, filename):
        """
        Transitions MemTable to an immutable Shard.
        1. Writes MemTable to N-Partition file (if not empty).
        2. Updates Index and Manifest atomically.
        3. Frees old MemTable memory and allocates a new one.
        """
        if self.current_lsn == self.starting_lsn:
            self.active_table.free()
            self.active_table = MemTable(self.schema, self.memtable_capacity)
            return self.index.needs_compaction

        lsn_max = self.current_lsn - r_uint64(1)
        lsn_min = self.starting_lsn
        
        # 1. Materialize MemTable to columnar file
        self.active_table.flush(filename, self.table_id)
        
        # 2. Register the new shard with the Unified Index
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
                
                # 3. Synchronize Manifest with the updated Index state.
                # get_metadata_list() now returns ManifestEntry objects directly.
                if self.manifest_manager:
                    manifest_entries = self.index.get_metadata_list()
                    self.manifest_manager.publish_new_version(manifest_entries, lsn_max)
            else:
                new_handle.close()
                os.unlink(filename)
                if self.manifest_manager:
                    manifest_entries = self.index.get_metadata_list()
                    self.manifest_manager.publish_new_version(manifest_entries, lsn_max)

        # 4. Rotate Memory
        self.active_table.free()
        self.active_table = MemTable(self.schema, self.memtable_capacity)
        
        # 5. Advance Epoch Tracking
        self.starting_lsn = self.current_lsn
        
        return self.index.needs_compaction

    def open_trace_cursor(self):
        """
        Returns a UnifiedCursor representing the current net state 
        of the table (MemTable + Shards).
        """
        
        # Build the cursor list using append, never * n.
        #
        # * (n + 1) initializes a List that widens to
        # List[Optional[BaseCursor]] when cursor objects are assigned into it.
        # UnifiedCursor._immutable_fields_ = ["cursors[*]"] tells RPython to
        # store cursors as a raw C array with direct non-nullable pointer loads.
        # The Optional element type conflicts with that expectation: the C code
        # reads each slot as a BaseCursor* but gets a nullable representation,
        # causing a SIGSEGV when the first virtual dispatch is attempted.
        #
        # Using append on an empty list keeps the element type as List[BaseCursor]
        # (non-nullable) throughout, which matches the array contract.
        cs = []
        cs.append(MemTableCursor(self.active_table))
        for i in range(len(self.index.handles)):
            cs.append(ShardCursor(self.index.handles[i].view))
            
        return UnifiedCursor(self.schema, cs)

    def close(self):
        """Graceful shutdown of memory and handles."""
        if self.active_table:
            self.active_table.free()
            self.active_table = None
            
        self.index.close_all()
