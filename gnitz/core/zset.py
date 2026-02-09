import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import memtable, spine, engine, manifest, shard_registry, refcount, wal, compactor
from gnitz.core import values as db_values

class PersistentZSet(object):
    def __init__(self, directory, name, layout, component_id=1, cache_size=1048576):
        self.directory = directory
        self.name = name
        self.layout = layout
        self.component_id = component_id
        
        if not os.path.exists(directory): os.makedirs(directory)
            
        self.manifest_path = os.path.join(directory, "%s.manifest" % name)
        self.wal_path = os.path.join(directory, "%s.wal" % name)
        
        self.ref_counter = refcount.RefCounter()
        self.registry = shard_registry.ShardRegistry()
        self.manifest_manager = manifest.ManifestManager(self.manifest_path)
        self.wal_writer = wal.WALWriter(self.wal_path, layout)
        
        self.mem_manager = memtable.MemTableManager(layout, cache_size, wal_writer=self.wal_writer, component_id=component_id)
        
        if self.manifest_manager.exists():
            self.spine = spine.Spine.from_manifest(self.manifest_path, component_id, layout, ref_counter=self.ref_counter)
        else:
            self.spine = spine.Spine([], self.ref_counter)
            
        self.engine = engine.Engine(self.mem_manager, self.spine, self.manifest_manager, self.registry, component_id=component_id, recover_wal_filename=self.wal_path)
        self.compaction_policy = compactor.CompactionPolicy(self.registry)
        
        # Permanent scratch buffer for zero-boxing queries
        self._query_scratch = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')

    def insert(self, entity_id, *values):
        wrapped = [db_values.wrap(v) for v in values]
        self.engine.mem_manager.put(entity_id, 1, wrapped)

    def remove(self, entity_id, *values):
        wrapped = [db_values.wrap(v) for v in values]
        self.engine.mem_manager.put(entity_id, -1, wrapped)

    def get_weight(self, entity_id, *values):
        wrapped = [db_values.wrap(v) for v in values]
        # Pack into scratch buffer using the memtable logic
        for i in range(self.layout.stride): self._query_scratch[i] = '\x00'
        self.mem_manager.active_table._pack_values_to_buf(self._query_scratch, wrapped)
        
        # Use blob_arena.base_ptr because the scratch packing might have allocated strings
        blob_base = self.mem_manager.active_table.blob_arena.base_ptr
        return self.engine.get_effective_weight_raw(entity_id, self._query_scratch, blob_base)

    def flush(self):
        filename = os.path.join(self.directory, "%s_shard_%d.db" % (self.name, self.engine.current_lsn))
        min_eid, max_eid, needs_compaction = self.engine.flush_and_rotate(filename)
        if needs_compaction: self._trigger_compaction()
        return filename

    def _trigger_compaction(self):
        compactor.execute_compaction(self.component_id, self.compaction_policy, self.manifest_manager, self.ref_counter, self.layout, output_dir=self.directory)

    def close(self):
        if self._query_scratch:
            lltype.free(self._query_scratch, flavor='raw')
            self._query_scratch = lltype.nullptr(rffi.CCHARP.TO)
        self.engine.close()
