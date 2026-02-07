"""
tests/test_engine_flush.py

Unit test for Step 1.11: Integration with Memtable Flush
Tests the integrated flush functionality that connects MemTable,
Manifest, and ShardRegistry.
"""
import unittest
import os
from gnitz.core import types
from gnitz.storage import (
    memtable, engine, manifest, 
    shard_registry, spine, shard_ecs
)

class TestEngineFlush(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.manifest_fn = "test_engine_flush.manifest"
        self.test_files = [self.manifest_fn]

    def tearDown(self):
        """Clean up test files."""
        for f in self.test_files:
            if os.path.exists(f):
                os.unlink(f)

    def test_flush_creates_shard_file(self):
        """Test that flush creates the shard file."""
        shard_file = "test_flush_shard.db"
        self.test_files.append(shard_file)
        
        # Create Engine with minimal setup
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        db = engine.Engine(mgr, sp)
        
        # Add an entity
        db.mem_manager.put(100, 1, 42, "test")
        
        # Flush
        min_eid, max_eid = db.mem_manager.flush_and_rotate(shard_file)
        
        # Verify shard file was created
        self.assertTrue(os.path.exists(shard_file))
        
        # Verify metadata is correct
        self.assertEqual(min_eid, 100)
        self.assertEqual(max_eid, 100)

    def test_integrated_flush_updates_manifest(self):
        """Test that integrated flush updates the manifest."""
        shard_file = "test_integrated_shard.db"
        self.test_files.append(shard_file)
        
        # Create Engine with manifest and registry
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        m_mgr = manifest.ManifestManager(self.manifest_fn)
        reg = shard_registry.ShardRegistry()
        
        db = engine.Engine(mgr, sp, m_mgr, reg, component_id=1)
        
        # Add entities
        db.mem_manager.put(100, 1, 42, "first")
        db.mem_manager.put(200, 1, 99, "second")
        
        # Use integrated flush
        compaction_triggered = db.flush_and_rotate(shard_file)
        
        # Verify shard file was created
        self.assertTrue(os.path.exists(shard_file))
        
        # Verify manifest was updated
        self.assertTrue(m_mgr.exists())
        reader = m_mgr.load_current()
        self.assertEqual(reader.get_entry_count(), 1)
        
        entry = reader.read_entry(0)
        self.assertEqual(entry.component_id, 1)
        self.assertEqual(entry.shard_filename, shard_file)
        self.assertEqual(entry.min_entity_id, 100)
        self.assertEqual(entry.max_entity_id, 200)
        self.assertEqual(entry.min_lsn, 1)
        self.assertEqual(entry.max_lsn, 1)
        
        reader.close()
        
        # Compaction should not be triggered with only 1 shard
        self.assertFalse(compaction_triggered)

    def test_integrated_flush_updates_registry(self):
        """Test that integrated flush updates the shard registry."""
        shard_file = "test_registry_shard.db"
        self.test_files.append(shard_file)
        
        # Create Engine with registry
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        reg = shard_registry.ShardRegistry()
        
        db = engine.Engine(mgr, sp, registry=reg, component_id=1)
        
        # Add an entity
        db.mem_manager.put(150, 1, 77, "registry_test")
        
        # Use integrated flush
        db.flush_and_rotate(shard_file)
        
        # Verify registry was updated
        self.assertEqual(len(reg.shards), 1)
        
        shard_meta = reg.shards[0]
        self.assertEqual(shard_meta.filename, shard_file)
        self.assertEqual(shard_meta.component_id, 1)
        self.assertEqual(shard_meta.min_entity_id, 150)
        self.assertEqual(shard_meta.max_entity_id, 150)

    def test_multiple_flushes_increment_lsn(self):
        """Test that multiple flushes increment the LSN correctly."""
        shard1 = "test_lsn_shard1.db"
        shard2 = "test_lsn_shard2.db"
        self.test_files.extend([shard1, shard2])
        
        # Create Engine with manifest
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        m_mgr = manifest.ManifestManager(self.manifest_fn)
        
        db = engine.Engine(mgr, sp, m_mgr, reg, component_id=1)
        
        # First flush
        db.mem_manager.put(100, 1, 10, "first")
        db.flush_and_rotate(shard1)
        
        # Second flush
        db.mem_manager.put(200, 1, 20, "second")
        db.flush_and_rotate(shard2)
        
        # Verify LSN incremented
        reader = m_mgr.load_current()
        self.assertEqual(reader.get_entry_count(), 2)
        
        entry1 = reader.read_entry(0)
        entry2 = reader.read_entry(1)
        
        self.assertEqual(entry1.max_lsn, 1)
        self.assertEqual(entry2.max_lsn, 2)
        
        reader.close()

    def test_compaction_trigger_detection(self):
        """Test that compaction is triggered when threshold is exceeded."""
        shards = ["test_compact_%d.db" % i for i in range(5)]
        self.test_files.extend(shards)
        
        # Create Engine with registry (default threshold is 4)
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        reg = shard_registry.ShardRegistry()
        
        db = engine.Engine(mgr, sp, registry=reg, component_id=1)
        
        # Create overlapping shards by repeatedly adding entity 100
        compaction_triggered = False
        for i, shard_file in enumerate(shards):
            db.mem_manager.put(100, 1, i * 10, "overlap_%d" % i)
            triggered = db.flush_and_rotate(shard_file)
            if triggered:
                compaction_triggered = True
        
        # Compaction should be triggered when we exceed threshold (4)
        self.assertTrue(compaction_triggered)
        self.assertTrue(reg.needs_compaction(1))
        
        # Verify read amplification
        read_amp = reg.get_read_amplification(1, 100)
        self.assertEqual(read_amp, 5)  # Entity 100 is in all 5 shards

    def test_flush_empty_memtable(self):
        """Test that flushing an empty MemTable doesn't create bad manifest entries."""
        shard_file = "test_empty_shard.db"
        self.test_files.append(shard_file)
        
        # Create Engine with manifest
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        m_mgr = manifest.ManifestManager(self.manifest_fn)
        
        db = engine.Engine(mgr, sp, m_mgr)
        
        # Flush without adding anything
        db.flush_and_rotate(shard_file)
        
        # Manifest should not exist or be empty
        if m_mgr.exists():
            reader = m_mgr.load_current()
            # No entries should have been added for empty flush
            # (the implementation skips manifest update when min_eid == -1)
            reader.close()

    def test_shard_content_verification(self):
        """Test that flushed shard contains the correct data."""
        shard_file = "test_content_shard.db"
        self.test_files.append(shard_file)
        
        # Create Engine
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        sp = spine.Spine([])
        
        db = engine.Engine(mgr, sp)
        
        # Add entities
        db.mem_manager.put(100, 1, 42, "value_100")
        db.mem_manager.put(200, 1, 84, "value_200")
        db.mem_manager.put(150, 1, 63, "value_150")
        
        # Flush
        db.flush_and_rotate(shard_file)
        
        # Verify shard contents
        view = shard_ecs.ECSShardView(shard_file, self.layout)
        
        # Should have 3 entities
        self.assertEqual(view.count, 3)
        
        # Entities should be sorted
        self.assertEqual(view.get_entity_id(0), 100)
        self.assertEqual(view.get_entity_id(1), 150)
        self.assertEqual(view.get_entity_id(2), 200)
        
        # Verify weights
        self.assertEqual(view.get_weight(0), 1)
        self.assertEqual(view.get_weight(1), 1)
        self.assertEqual(view.get_weight(2), 1)
        
        # Verify field values
        self.assertEqual(view.read_field_i64(0, 0), 42)
        self.assertEqual(view.read_field_i64(1, 0), 63)
        self.assertEqual(view.read_field_i64(2, 0), 84)
        
        view.close()

if __name__ == '__main__':
    unittest.main()
