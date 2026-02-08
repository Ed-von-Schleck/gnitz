"""
gnitz/core/zset.py
"""

import os
from gnitz.storage import (
    memtable, spine, engine, manifest, 
    shard_registry, refcount, wal, compactor
)
from gnitz.core import values as db_values

class PersistentZSet(object):
    def __init__(self, directory, name, layout, component_id=1, cache_size=1048576):
        self.directory = directory
        self.name = name
        self.layout = layout
        self.component_id = component_id
        
        if not os.path.exists(directory):
            os.makedirs(directory)
            
        self.manifest_path = os.path.join(directory, "%s.manifest" % name)
        self.wal_path = os.path.join(directory, "%s.wal" % name)
        
        self.ref_counter = refcount.RefCounter()
        self.registry = shard_registry.ShardRegistry()
        self.manifest_manager = manifest.ManifestManager(self.manifest_path)
        
        self.wal_writer = wal.WALWriter(self.wal_path, layout)
        
        self.mem_manager = memtable.MemTableManager(
            layout, 
            cache_size, 
            wal_writer=self.wal_writer,
            component_id=component_id
        )
        
        if self.manifest_manager.exists():
            self.spine = spine.Spine.from_manifest(
                self.manifest_path, 
                component_id, 
                layout, 
                ref_counter=self.ref_counter
            )
        else:
            self.spine = spine.Spine([], self.ref_counter)
            
        self.engine = engine.Engine(
            self.mem_manager, 
            self.spine, 
            self.manifest_manager, 
            self.registry, 
            component_id=component_id,
            recover_wal_filename=self.wal_path
        )
        
        self.compaction_policy = compactor.CompactionPolicy(self.registry)

    def _wrap_values(self, raw_values):
        wrapped = []
        for v in raw_values:
            wrapped.append(db_values.wrap(v))
        return wrapped

    def insert(self, entity_id, *values):
        self.engine.mem_manager.put(entity_id, 1, self._wrap_values(values))

    def remove(self, entity_id, *values):
        self.engine.mem_manager.put(entity_id, -1, self._wrap_values(values))

    def push_delta(self, entity_id, weight, *values):
        self.engine.mem_manager.put(entity_id, weight, self._wrap_values(values))

    def get_weight(self, entity_id, *values):
        # Pass values as a list to avoid RPython tuple unification errors
        return self.engine.get_effective_weight(entity_id, self._wrap_values(values))

    def flush(self):
        next_lsn = self.engine.current_lsn
        filename = os.path.join(
            self.directory, 
            "%s_shard_%d.db" % (self.name, next_lsn)
        )
        
        min_eid, max_eid, needs_compaction = self.engine.flush_and_rotate(filename)
        
        if needs_compaction:
            self._trigger_compaction()
            
        return filename

    def checkpoint(self):
        self.engine.checkpoint()

    def _trigger_compaction(self):
        import time
        t = int(time.time())
        output_name = os.path.join(
            self.directory, 
            "%s_level1_%d.db" % (self.name, t)
        )
        
        compactor.execute_compaction(
            self.component_id,
            self.compaction_policy,
            self.manifest_manager,
            self.ref_counter,
            self.layout,
            output_dir=self.directory
        )

    def close(self):
        self.engine.close()
