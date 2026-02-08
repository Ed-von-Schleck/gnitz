"""
tests/test_engine_flush.py
"""
import unittest
import os
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
            if os.path.exists(f):
                os.unlink(f)
    
    def _put(self, db, eid, w, *vals):
        wrapped = [db_values.wrap(v) for v in vals]
        db.mem_manager.put(eid, w, wrapped)

    def test_flush_creates_shard_file(self):
        shard_file = "test_flush_shard.db"
        self.test_files.append(shard_file)
        
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        db = engine.Engine(mgr, sp)
        
        self._put(db, 100, 1, 42, "test")
        
        min_eid, max_eid, _ = db.flush_and_rotate(shard_file)
        
        self.assertTrue(os.path.exists(shard_file))
        self.assertEqual(min_eid, 100)
        self.assertEqual(max_eid, 100)

    def test_integrated_flush_updates_manifest(self):
        shard_file = "test_integrated_shard.db"
        self.test_files.append(shard_file)
        
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        m_mgr = manifest.ManifestManager(self.manifest_fn)
        reg = shard_registry.ShardRegistry()
        
        db = engine.Engine(mgr, sp, m_mgr, reg, component_id=1)
        
        # LSNs start at 1. 
        self._put(db, 100, 1, 42, "first")  # LSN 1
        self._put(db, 200, 1, 99, "second") # LSN 2
        
        db.flush_and_rotate(shard_file)
        
        self.assertTrue(os.path.exists(shard_file))
        self.assertTrue(m_mgr.exists())
        reader = m_mgr.load_current()
        self.assertEqual(reader.get_entry_count(), 1)
        
        entry = reader.read_entry(0)
        # Verify 1-based LSN range for this batch
        self.assertEqual(entry.min_lsn, 1)
        self.assertEqual(entry.max_lsn, 2)
        
        reader.close()

    def test_integrated_flush_updates_registry(self):
        shard_file = "test_registry_shard.db"
        self.test_files.append(shard_file)
        
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        reg = shard_registry.ShardRegistry()
        
        db = engine.Engine(mgr, sp, registry=reg, component_id=1)
        self._put(db, 150, 1, 77, "registry_test")
        db.flush_and_rotate(shard_file)
        
        self.assertEqual(len(reg.shards), 1)
        shard_meta = reg.shards[0]
        self.assertEqual(shard_meta.filename, shard_file)

    def test_multiple_flushes_increment_lsn(self):
        shard1 = "test_lsn_shard1.db"
        shard2 = "test_lsn_shard2.db"
        self.test_files.extend([shard1, shard2])
        
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        m_mgr = manifest.ManifestManager(self.manifest_fn)
        reg = shard_registry.ShardRegistry()
        
        db = engine.Engine(mgr, sp, m_mgr, reg, component_id=1)
        
        # First Batch
        self._put(db, 100, 1, 10, "first")  # LSN 1
        db.flush_and_rotate(shard1)         # Range 1-1
        
        # Second Batch
        self._put(db, 200, 1, 20, "second") # LSN 2
        db.flush_and_rotate(shard2)         # Range 2-2
        
        reader = m_mgr.load_current()
        self.assertEqual(reader.get_entry_count(), 2)
        
        entry1 = reader.read_entry(0)
        entry2 = reader.read_entry(1)
        
        self.assertEqual(entry1.min_lsn, 1)
        self.assertEqual(entry1.max_lsn, 1)
        self.assertEqual(entry2.min_lsn, 2)
        self.assertEqual(entry2.max_lsn, 2)
        
        reader.close()

    def test_compaction_trigger_detection(self):
        shards = ["test_compact_%d.db" % i for i in range(5)]
        self.test_files.extend(shards)
        
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        reg = shard_registry.ShardRegistry()
        
        db = engine.Engine(mgr, sp, registry=reg, component_id=1)
        
        compaction_triggered = False
        for i, shard_file in enumerate(shards):
            self._put(db, 100, 1, i * 10, "overlap_%d" % i)
            min_e, max_e, triggered = db.flush_and_rotate(shard_file)
            if triggered:
                compaction_triggered = True
        
        self.assertTrue(compaction_triggered)
        self.assertTrue(reg.needs_compaction(1))
        
        read_amp = reg.get_read_amplification(1, 100)
        self.assertEqual(read_amp, 5)

    def test_flush_empty_memtable(self):
        shard_file = "test_empty_shard.db"
        self.test_files.append(shard_file)
        
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        m_mgr = manifest.ManifestManager(self.manifest_fn)
        
        db = engine.Engine(mgr, sp, m_mgr)
        
        # Shoul not create file or manifest entry if memtable is pristine
        db.flush_and_rotate(shard_file)
        
        self.assertFalse(os.path.exists(shard_file))
        self.assertFalse(m_mgr.exists())

    def test_shard_content_verification(self):
        shard_file = "test_content_shard.db"
        self.test_files.append(shard_file)
        
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        
        db = engine.Engine(mgr, sp)
        
        self._put(db, 100, 1, 42, "value_100")
        self._put(db, 200, 1, 84, "value_200")
        self._put(db, 150, 1, 63, "value_150")
        
        db.flush_and_rotate(shard_file)
        
        view = shard_ecs.ECSShardView(shard_file, self.layout)
        self.assertEqual(view.count, 3)
        self.assertEqual(view.get_entity_id(0), 100)
        self.assertEqual(view.read_field_i64(0, 0), 42)
        view.close()

if __name__ == '__main__':
    unittest.main()
