import sys
import os
from gnitz.storage import (
    memtable, spine, engine, writer_ecs, manifest, 
    shard_registry, refcount, compactor, compaction_logic, shard_ecs
)
from gnitz.core import types

def entry_point(argv):
    print "--- GnitzDB End-to-End Lifecycle Test ---"
    
    # 1. Setup Schema: [ID (I64), Name (String)]
    layout_obj = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    
    # Test file registry for cleanup
    db_manifest = "test_manifest.db"
    shard_a = "shard_a.db"
    shard_b = "shard_b.db"
    shard_compacted = "shard_final.db"
    
    all_files = [db_manifest, shard_a, shard_b, shard_compacted]
    for f in all_files:
        if os.path.exists(f): os.unlink(f)
    
    try:
        # === PHASE 1: Ingestion & Weight Persistence ===
        print "[Phase 1] Ingestion and basic persistence..."
        
        mgr = memtable.MemTableManager(layout_obj, 1024 * 1024)
        db = engine.Engine(mgr, spine.Spine([]))
        
        # Entity 1: Initial state
        db.mem_manager.put(1, 1, 100, "original")
        # Entity 2: To be updated later
        db.mem_manager.put(2, 1, 200, "v1")
        
        # Flush to first immutable shard
        db.mem_manager.flush_and_rotate(shard_a)
        
        # Verify weight persistence on disk (Direct Shard Access)
        view_a = shard_ecs.ECSShardView(shard_a, layout_obj)
        if view_a.get_weight(0) != 1 or view_a.read_field_i64(0, 0) != 100:
            print "  [FAIL] Shard A data incorrect"
            return 1
        view_a.close()
        print "  [OK] Shard A persisted"

        # === PHASE 2: Updates, Deletions & Manifest Versioning ===
        print "[Phase 2] Multi-shard updates and manifest versioning..."
        
        # Entity 1: Annihilate (Weight -1)
        db.mem_manager.put(1, -1, 100, "annihilated")
        # Entity 2: Update (Higher weight/different value)
        db.mem_manager.put(2, 1, 250, "v2")
        # Entity 3: New entry
        db.mem_manager.put(3, 1, 300, "v1")
        
        db.mem_manager.flush_and_rotate(shard_b)
        
        # Create Manifest v1 (Just shard A)
        man_mgr = manifest.ManifestManager(db_manifest)
        man_mgr.publish_new_version([
            manifest.ManifestEntry(1, shard_a, 1, 2, 1, 1)
        ])
        
        # Atomically Update to Manifest v2 (Shard A + Shard B)
        man_mgr.publish_new_version([
            manifest.ManifestEntry(1, shard_a, 1, 2, 1, 1),
            manifest.ManifestEntry(1, shard_b, 1, 3, 2, 2)
        ])
        
        # Load unified view from manifest
        db_spine = spine.Spine.from_manifest(db_manifest, component_id=1, layout=layout_obj)
        db_engine = engine.Engine(mgr, db_spine)
        
        # Verify Algebraic Result:
        # Entity 1: 1 (A) + -1 (B) = 0 (Annihilated)
        if db_engine.get_effective_weight(1) != 0:
            print "  [FAIL] Entity 1 not annihilated"
            return 1
        
        # Entity 2: 1 (A) + 1 (B) = 2 (Accumulated)
        if db_engine.get_effective_weight(2) != 2:
            print "  [FAIL] Entity 2 weight incorrect"
            return 1
            
        print "  [OK] Manifest and multi-shard resolution working"

        # === PHASE 3: Vertical Compaction & Ghost Property ===
        print "[Phase 3] Compaction and algebraic pruning..."
        
        # Perform vertical merge sort of Shard A and Shard B
        compactor.compact_shards(
            [shard_a, shard_b], 
            [1, 2], # LSNs
            shard_compacted, 
            layout_obj
        )
        
        # Verify Physical Pruning (The Ghost Property)
        view_final = shard_ecs.ECSShardView(shard_compacted, layout_obj)
        
        # Entity 1 should be PHYSICALLY GONE from the new shard
        if view_final.find_entity_index(1) != -1:
            print "  [FAIL] Annihilated entity 1 still exists in physical shard"
            return 1
            
        # Entity 2 should exist with accumulated weight 2
        idx2 = view_final.find_entity_index(2)
        if view_final.get_weight(idx2) != 2 or view_final.read_field_i64(idx2, 0) != 250:
            print "  [FAIL] Compacted entity 2 data incorrect (LSN resolution check)"
            return 1
            
        view_final.close()
        print "  [OK] Compaction successfully pruned dead records"

        # === PHASE 4: Shard Registry & Read Amplification ===
        print "[Phase 4] Registry and read amplification..."
        
        reg = shard_registry.ShardRegistry()
        reg.register_shard(shard_registry.ShardMetadata(shard_a, 1, 1, 2, 1, 1))
        reg.register_shard(shard_registry.ShardMetadata(shard_b, 1, 1, 3, 2, 2))
        
        # Max read amp for component 1 is 2 (shards A and B overlap at entity 1 and 2)
        if reg.get_max_read_amplification(1) != 2:
            print "  [FAIL] Read amplification calculation incorrect"
            return 1
            
        print "  [OK] Registry correctly identified overlap"

        # === PHASE 5: Reference Counting & Safe Deletion ===
        print "[Phase 5] Reference counting and cleanup..."
        
        rc = refcount.RefCounter()
        rc.acquire(shard_a)
        rc.mark_for_deletion(shard_a)
        
        # Should not be deleted yet (ref > 0)
        rc.try_cleanup()
        if not os.path.exists(shard_a):
            print "  [FAIL] Shard deleted while reference active"
            return 1
            
        rc.release(shard_a)
        rc.try_cleanup()
        if os.path.exists(shard_a):
            print "  [FAIL] Shard not deleted after ref release"
            return 1
            
        print "  [OK] Reference counting protected active file"
        
        db_engine.close()
        print ""
        print "=== All End-to-End Tests Passed ==="
        
    finally:
        # Final cleanup of remaining test files
        for f in all_files:
            if os.path.exists(f): 
                try: os.unlink(f)
                except: pass
        # Cleanup high-amp dummy shards if they exist
        for i in range(5):
            fn = "shard_%d.db" % i
            if os.path.exists(fn): os.unlink(fn)

    return 0

def target(driver, args):
    return entry_point, None

if __name__ == '__main__':
    entry_point(sys.argv)
