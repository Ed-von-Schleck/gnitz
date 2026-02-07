"""
gnitz/storage/engine.py

The core Engine orchestrating MemTable, Spine, Manifest, and WAL recovery.
Implements LSN monotonicity and WAL checkpointing.
"""

from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage.memtable import (
    node_get_weight, node_get_entity_id, node_get_next_off, 
    node_get_payload_ptr, skip_list_find
)
from gnitz.storage import wal, manifest, errors

class Engine(object):
    """
    Orchestrates the lifecycle of a single component type.
    Handles ingestion, point lookups, and crash recovery.
    """
    
    # RPython JIT Hint: These fields do not change after initialization,
    # allowing the JIT to fold lookups into constants in the trace.
    _immutable_fields_ = [
        'mem_manager', 'spine', 'layout', 
        'manifest_manager', 'registry', 'component_id'
    ]

    def __init__(self, mem_manager, spine, manifest_manager=None, 
                 registry=None, component_id=1, recover_wal_filename=None):
        """
        Initializes the Engine. 
        
        Monotonicity Check: If a manifest exists, current_lsn starts at
        global_max_lsn + 1 to prevent LSN collision after restart.
        """
        self.mem_manager = mem_manager
        self.spine = spine
        self.layout = mem_manager.layout
        self.manifest_manager = manifest_manager
        self.registry = registry
        self.component_id = component_id
        
        # 1. Initialize LSN from Manifest (Item 1 & 6 Fix)
        if manifest_manager and manifest_manager.exists():
            reader = manifest_manager.load_current()
            self.current_lsn = reader.global_max_lsn + 1
            reader.close()
        else:
            self.current_lsn = 1

        # 2. Perform Recovery (Item 1 Fix)
        # Replaying WAL will advance current_lsn further if the WAL has
        # data that wasn't yet finalized into a Shard.
        if recover_wal_filename:
            self.recover_from_wal(recover_wal_filename)
        
        # Sync the MemManager's LSN tracker with the Engine
        self.mem_manager.current_lsn = self.current_lsn

    def recover_from_wal(self, filename):
        """
        Replays the Write-Ahead Log to restore MemTable state.
        Ensures monotonicity by skipping LSNs already present in persistent shards.
        """
        try:
            reader = wal.WALReader(filename, self.layout)
        except OSError:
            # If log doesn't exist, nothing to recover
            return

        # Any LSN <= current_lsn - 1 is already in a Shard
        max_lsn_finalized = self.current_lsn - 1
        max_lsn_seen = max_lsn_finalized

        # Use the generator interface (handles EOF and valid flags internally)
        for lsn, comp_id, records in reader.iterate_blocks():
            # Filter for this engine's component
            if comp_id != self.component_id:
                continue
            
            # Skip blocks already finalized into shards (Item 1 Fix)
            if lsn <= max_lsn_finalized:
                continue

            if lsn > max_lsn_seen:
                max_lsn_seen = lsn
            
            # Apply records to the active MemTable
            for eid, weight, c_data in records:
                self.mem_manager.put_from_recovery(eid, weight, c_data)
        
        # Update trackers to the next available LSN
        self.current_lsn = max_lsn_seen + 1
        self.mem_manager.current_lsn = self.current_lsn
        reader.close()

    def checkpoint(self):
        """
        Prunes the Write-Ahead Log (Step 9).
        Triggers truncation for all LSNs that are now safely inside Manifest-tracked shards.
        """
        if not self.manifest_manager or not self.manifest_manager.exists():
            return
            
        if not self.mem_manager.wal_writer:
            return

        # Determine what is safe to prune from the Manifest
        reader = self.manifest_manager.load_current()
        safe_lsn = reader.global_max_lsn
        reader.close()
        
        # Truncate: Remove everything <= safe_lsn
        # The writer will keep everything >= safe_lsn + 1
        if safe_lsn > 0:
            self.mem_manager.wal_writer.truncate_before_lsn(safe_lsn + 1)

    def get_effective_weight(self, entity_id):
        """
        Algebraically sums weights across memory and shards.
        """
        base = self.mem_manager.active_table.arena.base_ptr
        head = self.mem_manager.active_table.head_off
        
        mem_weight = 0
        pred_off = skip_list_find(base, head, entity_id)
        next_off = node_get_next_off(base, pred_off, 0)
        if next_off != 0 and node_get_entity_id(base, next_off) == entity_id:
            mem_weight = node_get_weight(base, next_off)

        spine_weight = 0
        results = self.spine.find_all_shards_and_indices(entity_id)
        for shard_handle, row_idx in results:
            spine_weight += shard_handle.get_weight(row_idx)

        return mem_weight + spine_weight

    def read_component_i64(self, entity_id, field_idx):
        """
        Reads a component field with Last-Write-Wins (LWW) resolution.
        """
        # 1. Check MemTable (always contains the most recent LSNs)
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
        """
        Flushes the current MemTable to a persistent shard and updates the Manifest.
        Updates the global_max_lsn in the manifest header.
        """
        # Determine the LSN range represented in the current MemTable
        # current_lsn is the NEXT LSN to be assigned.
        # Thus, the highest LSN in the current table is current_lsn - 1.
        # We don't track the exact min_lsn in the MemTable yet, so we use
        # the last finalized LSN + 1 as an approximation or zero.
        
        current_max_lsn = self.current_lsn - 1
        
        min_eid, max_eid = self.mem_manager.flush_and_rotate(filename)
        if min_eid == -1: 
            return False # Table was empty
        
        # Collect existing entries and determine new global LSN high-water mark
        entries = []
        new_global_max = current_max_lsn
        
        if self.manifest_manager and self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            for e in reader.iterate_entries():
                entries.append(e)
            
            # Ensure the high-water mark only moves forward
            if reader.global_max_lsn > new_global_max:
                new_global_max = reader.global_max_lsn
            reader.close()
        
        # Register shard in memory (Registry)
        if self.registry is not None:
            from gnitz.storage.shard_registry import ShardMetadata
            # Use current_max_lsn as both min/max for this simple batch
            new_meta = ShardMetadata(filename, self.component_id, min_eid, max_eid, 
                                   current_max_lsn, current_max_lsn)
            self.registry.register_shard(new_meta)
        
        # Persist to Manifest with new global_max_lsn (Item 6 Fix)
        if self.manifest_manager is not None:
            from gnitz.storage.manifest import ManifestEntry
            new_entry = ManifestEntry(self.component_id, filename, min_eid, max_eid, 
                                    current_max_lsn, current_max_lsn)
            entries.append(new_entry)
            self.manifest_manager.publish_new_version(entries, new_global_max)
        
        # If registry is active, check if compaction is now required
        if self.registry is not None:
            return self.registry.mark_for_compaction(self.component_id)
        
        return False

    def close(self):
        """
        Closes the engine and releases all file handles.
        """
        self.mem_manager.close()
        self.spine.close_all()
