import unittest
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, values as db_values
from gnitz.storage import (
    memtable, engine, manifest, 
    shard_registry, spine, shard_ecs
)

class TestEngineFlush(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.manifest_fn = "test_engine_flush.manifest"
        self.test_files = [self.manifest_fn]

    def tearDown(self):
        for f in self.test_files:
            if os.path.exists(f): os.unlink(f)
    
    def _put(self, db, eid, w, *vals):
        wrapped = [db_values.wrap(v) for v in vals]
        db.mem_manager.put(eid, w, wrapped)

    def _get_weight(self, db, eid, *vals):
        wrapped = [db_values.wrap(v) for v in vals]
        scratch = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')
        try:
            for i in range(self.layout.stride): scratch[i] = '\x00'
            db.mem_manager.active_table._pack_values_to_buf(scratch, wrapped)
            blob_base = db.mem_manager.active_table.blob_arena.base_ptr
            return db.get_effective_weight_raw(eid, scratch, blob_base)
        finally:
            lltype.free(scratch, flavor='raw')

    def test_flush_creates_shard_file(self):
        shard_file = "test_flush_shard.db"
        self.test_files.append(shard_file)
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        db = engine.Engine(mgr, spine.Spine([]))
        self._put(db, 100, 1, 42, "test")
        min_eid, max_eid, _ = db.flush_and_rotate(shard_file)
        self.assertTrue(os.path.exists(shard_file))
        self.assertEqual(min_eid, 100)
        db.close()

    def test_compaction_trigger_detection(self):
        shards = ["test_compact_%d.db" % i for i in range(5)]
        self.test_files.extend(shards)
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        reg = shard_registry.ShardRegistry()
        db = engine.Engine(mgr, spine.Spine([]), registry=reg, component_id=1)
        
        compaction_triggered = False
        for i, shard_file in enumerate(shards):
            self._put(db, 100, 1, i * 10, "overlap_%d" % i)
            _, _, triggered = db.flush_and_rotate(shard_file)
            if triggered: compaction_triggered = True
        
        self.assertTrue(compaction_triggered)
        self.assertEqual(reg.get_read_amplification(1, 100), 5)
        db.close()

if __name__ == '__main__':
    unittest.main()
