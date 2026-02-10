import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long

from gnitz.storage import memtable, spine, engine, manifest, shard_registry, refcount, wal, compactor
from gnitz.core import values as db_values, types

class PersistentTable(object):
    def __init__(self, directory, name, schema, table_id=1, cache_size=1048576, read_only=False, **kwargs):
        self.directory = directory
        self.name = name
        self.schema = schema
        self.table_id = kwargs.get('component_id', table_id)
        self.read_only = read_only
        self.is_closed = False
        
        if not os.path.exists(directory): 
            if read_only: raise OSError("Directory does not exist")
            os.mkdir(directory)
            
        self.manifest_path = os.path.join(directory, "%s.manifest" % name)
        self.wal_path = os.path.join(directory, "%s.wal" % name)
        
        self.ref_counter = refcount.RefCounter()
        self.registry = shard_registry.ShardRegistry()
        self.manifest_manager = manifest.ManifestManager(self.manifest_path)
        
        if read_only:
            self.wal_writer = None
        else:
            self.wal_writer = wal.WALWriter(self.wal_path, schema)
        
        self.mem_manager = memtable.MemTableManager(schema, cache_size, wal_writer=self.wal_writer, table_id=self.table_id)
        
        if self.manifest_manager.exists():
            self.spine = spine.Spine.from_manifest(self.manifest_path, self.table_id, schema, ref_counter=self.ref_counter)
        else:
            self.spine = spine.Spine([], self.ref_counter)
            
        self.engine = engine.Engine(self.mem_manager, self.spine, self.manifest_manager, self.registry, table_id=self.table_id, recover_wal_filename=self.wal_path)
        self.compaction_policy = compactor.CompactionPolicy(self.registry)
        self._query_scratch = lltype.malloc(rffi.CCHARP.TO, self.schema.memtable_stride, flavor='raw')

    def insert(self, key, db_values_list):
        self.engine.mem_manager.put(r_uint128(key), 1, db_values_list)

    def remove(self, key, db_values_list):
        self.engine.mem_manager.put(r_uint128(key), -1, db_values_list)

    def get_weight(self, key, db_values_list):
        for i in range(self.schema.memtable_stride): self._query_scratch[i] = '\x00'
        self.mem_manager.active_table._pack_to_buf(self._query_scratch, db_values_list)
        blob_base = self.mem_manager.active_table.blob_arena.base_ptr
        return self.engine.get_effective_weight(r_uint128(key), self._query_scratch, blob_base)

    def flush(self):
        # Name the shard based on its starting LSN
        filename = os.path.join(self.directory, "%s_shard_%d.db" % (self.name, int(self.engine.mem_manager.starting_lsn)))
        min_key, max_key, needs_compaction = self.engine.flush_and_rotate(filename)
        return filename

    def checkpoint(self):
        """ Re-writes WAL, keeping only data newer than the last persisted LSN. """
        if not self.read_only and self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            lsn = reader.global_max_lsn
            reader.close()
            # Truncate all blocks that are already persisted in the manifest
            self.wal_writer.truncate_before_lsn(lsn + r_uint64(1))

    def _trigger_compaction(self):
        compactor.execute_compaction(self.table_id, self.compaction_policy, self.manifest_manager, self.ref_counter, self.schema, output_dir=self.directory, spine_obj=self.spine)

    def close(self):
        if self.is_closed: return
        if self._query_scratch:
            lltype.free(self._query_scratch, flavor='raw')
            self._query_scratch = lltype.nullptr(rffi.CCHARP.TO)
        
        self.engine.close()
        if self.wal_writer:
            self.wal_writer.close()
        self.is_closed = True

PersistentZSet = PersistentTable
