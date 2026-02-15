# gnitz/core/zset.py

import os
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.storage import index, engine, manifest, refcount, wal, compactor, memtable_manager
from gnitz.core import values, types

class PersistentTable(object):
    """
    Persistent Z-Set Table.
    The primary user-facing interface for GnitzDB. It integrates the 
    ingestion pipeline (WAL/MemTable) with the persistent storage layer (ShardIndex).
    """
    def __init__(self, directory, name, schema, table_id=1, cache_size=1048576, read_only=False, validate_checksums=False):
        self.directory = directory
        self.name = name
        self.schema = schema
        self.table_id = table_id
        self.read_only = read_only
        self.validate_checksums = validate_checksums
        self.is_closed = False
        
        if not os.path.exists(directory): 
            if read_only: raise OSError("Directory does not exist")
            os.mkdir(directory)
            
        self.manifest_path = os.path.join(directory, "%s.manifest" % name)
        self.wal_path = os.path.join(directory, "%s.wal" % name)
        
        # 1. Initialize Core Subsystems
        self.ref_counter = refcount.RefCounter()
        self.manifest_manager = manifest.ManifestManager(self.manifest_path)
        
        if read_only:
            self.wal_writer = None
        else:
            self.wal_writer = wal.WALWriter(self.wal_path, schema)
        
        self.mem_manager = memtable_manager.MemTableManager(
            schema, cache_size, wal_writer=self.wal_writer, table_id=self.table_id
        )
        
        # 2. Initialize the Unified Shard Index
        # This replaces both the old Spine and ShardRegistry
        if self.manifest_manager.exists():
            self.index = index.index_from_manifest(
                self.manifest_path, 
                self.table_id, 
                self.schema,
                self.ref_counter,
                self.validate_checksums
            )
        else:
            self.index = index.ShardIndex(self.table_id, self.schema, self.ref_counter)
            
        # 3. Initialize Execution Engine
        self.engine = engine.Engine(
            self.mem_manager, 
            self.index, # Pass unified index to engine
            manifest_manager=self.manifest_manager, 
            table_id=self.table_id, 
            recover_wal_filename=self.wal_path,
            validate_checksums=self.validate_checksums
        )

    def insert(self, key, db_values_list):
        """Appends a positive Z-Set delta (addition)."""
        self.mem_manager.put(r_uint128(key), 1, db_values_list)

    def remove(self, key, db_values_list):
        """Appends a negative Z-Set delta (removal)."""
        self.mem_manager.put(r_uint128(key), -1, db_values_list)

    def get_weight(self, key, db_values_list):
        """Retrieves the net algebraic weight of a specific record."""
        return self.engine.get_effective_weight_raw(r_uint128(key), db_values_list)

    def flush(self):
        """
        Materializes current MemTable to an immutable Shard.
        Triggers checkpointing and potential compaction.
        """
        if self.read_only:
            return ""

        lsn_val = intmask(self.mem_manager.starting_lsn)
        filename = os.path.join(self.directory, "%s_shard_%d.db" % (
            self.name, lsn_val)
        )
        
        # Engine handles shard writing, indexing, and manifest updates
        needs_compaction = self.engine.flush_and_rotate(filename)
        
        # Perform WAL maintenance
        self.checkpoint()
        
        # If the ShardIndex detects high read-amplification, trigger merge
        if needs_compaction:
            self._trigger_compaction()
            
        return filename

    def checkpoint(self):
        """Truncates the WAL after a successful Manifest commit."""
        if not self.read_only and self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            lsn = reader.global_max_lsn
            reader.close()
            # Safety: WAL is only cleared if the state is fully durable in shards
            self.wal_writer.truncate_before_lsn(lsn + r_uint64(1))

    def _trigger_compaction(self):
        """Orchestrates an N-way merge of overlapping shards."""
        if self.read_only:
            return
            
        compactor.execute_compaction(
            self.index, 
            self.manifest_manager, 
            self.directory, 
            self.validate_checksums
        )

    def close(self):
        """Gracefully closes all mmaps, locks, and files."""
        if self.is_closed: return
        self.engine.close()
        if self.wal_writer: self.wal_writer.close()
        self.is_closed = True
