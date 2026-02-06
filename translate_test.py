import sys
import os
from gnitz.storage import (
    memtable, spine, engine, writer_ecs, manifest, 
    shard_registry, refcount, compactor, shard_ecs
)
from gnitz.core import types

def entry_point(argv):
    print "--- GnitzDB Full Lifecycle Translation Test ---"
    
    # 1. Setup Schema: [Value (I64), Label (String)]
    layout_obj = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    
    # Registry of test files for the test runner
    db_manifest = "test_lifecycle.manifest"
    shard_a = "shard_a.db"
    shard_b = "shard_b.db"
    
    test_files = [db_manifest, shard_a, shard_b]
    for f in test_files:
        if os.path.exists(f): os.unlink(f)
    
    try:
        # === PHASE 1: Integrated Ingestion & Persistence ===
        print "[Phase 1] Ingesting with integrated flush..."
        
        # Setup integrated components
        mgr = memtable.MemTableManager(layout_obj, 1024 * 1024)
        m_mgr = manifest.ManifestManager(db_manifest)
        reg = shard_registry.ShardRegistry()
        sp = spine.Spine([])
        
        # Create Engine with integrated manifest and registry
        db = engine.Engine(mgr, sp, m_mgr, reg, component_id=1, current_lsn=1)
        
        # Entity 100: First version (Weight +1)
        db.mem_manager.put(100, 1, 10, "version_1")
        db.flush_and_rotate(shard_a)  # Uses integrated flush
        
        # Entity 100: Second version (Weight +1, Net weight should become 2)
        # Entity 200: To be deleted (Annihilated)
        db.mem_manager.put(100, 1, 20, "version_2")
        db.mem_manager.put(200, 1, 99, "to_delete")
        db.flush_and_rotate(shard_b)  # Uses integrated flush
        
        # Manually create Shard C to annihilate Entity 200
        # (This is outside the normal flow, so we do it manually)
        shard_c = "shard_c.db"
        test_files.append(shard_c)
        wc = writer_ecs.ECSShardWriter(layout_obj)
        wc._add_entity_weighted(200, -1, 99, "deletion_marker")
        wc.finalize(shard_c)
        
        # Manually register shard C (since it was created outside Engine)
        reg.register_shard(shard_registry.ShardMetadata(shard_c, 1, 200, 200, 3, 3))
        
        # Manually add to manifest
        # RPython Fix: Manual loop instead of list(generator)
        reader = m_mgr.load_current()
        entries = []
        for entry in reader.iterate_entries():
            entries.append(entry)
        reader.close()
        
        entries.append(manifest.ManifestEntry(1, shard_c, 200, 200, 3, 3))
        m_mgr.publish_new_version(entries)

        # === PHASE 2: Verify Integrated State ===
        print "[Phase 2] Verifying integrated state..."
        
        # Verify manifest was updated automatically
        reader = m_mgr.load_current()
        if reader.get_entry_count() != 3: return 1  # Should have 3 entries (A, B, C)
        reader.close()
        
        # Verify registry was updated automatically
        if len(reg.shards) != 3: return 1  # Should have 3 shards registered
        
        # Reload spine from manifest and verify algebraic summation
        sp = spine.Spine.from_manifest(db_manifest, 1, layout_obj)
        db_engine = engine.Engine(mgr, sp, m_mgr, reg)
        
        # Entity 100 exists in two shards (1+1=2). Latest value is "version_2"
        if db_engine.get_effective_weight(100) != 2: return 1
        if db_engine.read_component_i64(100, 0) != 20: return 1
        
        # Entity 200 is annihilated (1 in shard B, -1 in shard C)
        if db_engine.get_effective_weight(200) != 0: return 1
        
        # Check read amplification for Entity 100 (Found in Shard A and B)
        if reg.get_read_amplification(1, 100) != 2: return 1
        
        print "  [OK] Integrated state verified"

        # === PHASE 3: Automated Compaction Execution ===
        print "[Phase 3] Executing automated compaction..."
        
        rc = refcount.RefCounter()
        reg.mark_for_compaction(1) # Simulate heuristic trigger
        policy = compactor.CompactionPolicy(reg)
        
        # Merge shards A, B, and C into one consolidated shard
        new_shard = compactor.execute_compaction(1, policy, m_mgr, rc, layout_obj)
        if new_shard is None: return 1
        test_files.append(new_shard)
        
        # Verify Manifest was updated (should have exactly 1 entry now)
        reader = m_mgr.load_current()
        if reader.get_entry_count() != 1: return 1
        reader.close()
        
        # Verify physical pruning: Entity 200 should be PHYSICALLY GONE
        # Entity 100 should have a single record with weight 2
        vout = shard_ecs.ECSShardView(new_shard, layout_obj)
        if vout.count != 1: return 1
        if vout.get_entity_id(0) != 100: return 1
        if vout.get_weight(0) != 2: return 1
        vout.close()
        
        print "  [OK] Compaction physically realized the Ghost Property"

        # === PHASE 4: Reference Counting & Cleanup ===
        print "[Phase 4] Verifying reference-tracked deletion..."
        
        # Old shards were marked for deletion by the compactor
        # try_cleanup should physically unlink them now as their refcounts are 0
        deleted = rc.try_cleanup()
        if len(deleted) != 3: return 1 # A, B, and C
        
        if os.path.exists(shard_a): return 1
        if os.path.exists(shard_b): return 1
        if os.path.exists(shard_c): return 1
        
        print "  [OK] Obsolete shards physically deleted"
        
        db_engine.close()
        print ""
        print "=== All Translation Tests Passed ==="
        
    finally:
        for f in test_files:
            if os.path.exists(f): os.unlink(f)
            
    return 0

def target(driver, args):
    return entry_point, None

if __name__ == '__main__':
    entry_point(sys.argv)
