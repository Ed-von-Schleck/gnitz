import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from gnitz.storage import memtable, spine, engine, manifest, shard_registry, refcount, wal, compactor
from gnitz.core import values as db_values

class PersistentZSet(object):
    def __init__(self, directory, name, layout, component_id=1, cache_size=1048576, read_only=False):
        self.directory = directory
        self.name = name
        self.layout = layout
        self.component_id = component_id
        self.read_only = read_only
        
        if not os.path.exists(directory): 
            if read_only: raise OSError("Directory does not exist")
            os.mkdir(directory)
            
        self.manifest_path = os.path.join(directory, "%s.manifest" % name)
        self.wal_path = os.path.join(directory, "%s.wal" % name)
        
        self.ref_counter = refcount.RefCounter()
        self.registry = shard_registry.ShardRegistry()
        self.manifest_manager = manifest.ManifestManager(self.manifest_path)
        
        # Advisory Locking: only the writer attempts to acquire the lock
        if read_only:
            self.wal_writer = None
        else:
            self.wal_writer = wal.WALWriter(self.wal_path, layout)
        
        self.mem_manager = memtable.MemTableManager(layout, cache_size, wal_writer=self.wal_writer, component_id=component_id)
        
        if self.manifest_manager.exists():
            self.spine = spine.Spine.from_manifest(self.manifest_path, component_id, layout, ref_counter=self.ref_counter)
        else:
            self.spine = spine.Spine([], self.ref_counter)
            
        self.engine = engine.Engine(self.mem_manager, self.spine, self.manifest_manager, self.registry, component_id=component_id, recover_wal_filename=self.wal_path)
        self.compaction_policy = compactor.CompactionPolicy(self.registry)
        self._query_scratch = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')

    def insert(self, entity_id, db_values_list):
        if self.read_only: raise Exception("Cannot write to read-only Z-Set")
        self.engine.mem_manager.put(entity_id, 1, db_values_list)

    def remove(self, entity_id, db_values_list):
        if self.read_only: raise Exception("Cannot write to read-only Z-Set")
        self.engine.mem_manager.put(entity_id, -1, db_values_list)

    def get_weight(self, entity_id, db_values_list):
        for i in range(self.layout.stride): self._query_scratch[i] = '\x00'
        self.mem_manager.active_table._pack_values_to_buf(self._query_scratch, db_values_list)
        blob_base = self.mem_manager.active_table.blob_arena.base_ptr
        return self.engine.get_effective_weight_raw(entity_id, self._query_scratch, blob_base)

    def flush(self):
        if self.read_only: raise Exception("Cannot flush read-only Z-Set")
        filename = os.path.join(self.directory, "%s_shard_%d.db" % (self.name, self.engine.current_lsn))
        min_eid, max_eid, needs_compaction = self.engine.flush_and_rotate(filename)
        if needs_compaction: self._trigger_compaction()
        return filename

    def checkpoint(self):
        if self.read_only or not self.manifest_manager.exists(): return
        reader = self.manifest_manager.load_current()
        lsn_limit = reader.global_max_lsn + r_uint64(1)
        reader.close()
        
        # We can only truncate up to what has been safely moved to shards
        if lsn_limit < self.mem_manager.starting_lsn:
            self.wal_writer.truncate_before_lsn(lsn_limit)
        else:
            self.wal_writer.truncate_before_lsn(self.mem_manager.starting_lsn)

    def _trigger_compaction(self):
        compactor.execute_compaction(self.component_id, self.compaction_policy, self.manifest_manager, self.ref_counter, self.layout, output_dir=self.directory)

    def close(self):
        if self._query_scratch:
            lltype.free(self._query_scratch, flavor='raw')
            self._query_scratch = lltype.nullptr(rffi.CCHARP.TO)
        self.engine.close()
