import sys
import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import memtable, spine, engine, writer_ecs, layout, shard_ecs, manifest, shard_registry
from gnitz.core import types, strings

def entry_point(argv):
    print "--- GnitzDB Translation Test ---"
    
    layout_obj = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    
    # Test files
    test_files = [
        "test_engine.db",
        "test_manifest.db", 
        "test_shard1.db",
        "test_shard2.db",
        "test_shard3.db"
    ]
    
    # Cleanup
    for f in test_files:
        if os.path.exists(f): os.unlink(f)
    
    try:
        # === PHASE 0: Core ECS Engine ===
        print "[Phase 0] Testing Core ECS Engine..."
        
        mgr = memtable.MemTableManager(layout_obj, 1024 * 1024)
        sp = spine.Spine([])
        db = engine.Engine(mgr, sp)
        
        # Insert and query
        db.mem_manager.put(1, 1, 100, "test")
        if db.get_effective_weight(1) != 1: return 1
        if db.read_component_i64(1, 0) != 100: return 1
        
        # Flush and reload
        db.mem_manager.flush_and_rotate(test_files[0])
        sp.close_all()
        
        handle = spine.ShardHandle(test_files[0], layout_obj)
        sp = spine.Spine([handle])
        db = engine.Engine(mgr, sp)
        
        if db.get_effective_weight(1) != 1: return 1
        
        # Test annihilation
        db.mem_manager.put(1, -1, 100, "test")
        if db.get_effective_weight(1) != 0: return 1
        
        sp.close_all()
        print "  [OK] Core engine working"
        
        # === PHASE 1.1-1.4: Manifest System ===
        print "[Phase 1] Testing Manifest System..."
        
        # Create test shards
        writer1 = writer_ecs.ECSShardWriter(layout_obj)
        writer1.add_entity(10, 100, "shard1")
        writer1.finalize(test_files[2])
        
        writer2 = writer_ecs.ECSShardWriter(layout_obj)
        writer2.add_entity(50, 500, "shard2")
        writer2.finalize(test_files[3])
        
        # Test manifest manager - create initial manifest
        manager = manifest.ManifestManager(test_files[1])
        entries = [manifest.ManifestEntry(1, test_files[2], 10, 10, 0, 1)]
        manager.publish_new_version(entries)
        
        if not manager.exists(): return 1
        
        # Test atomic update
        entries.append(manifest.ManifestEntry(1, test_files[3], 50, 50, 1, 2))
        manager.publish_new_version(entries)
        
        reader = manager.load_current()
        if reader.get_entry_count() != 2: 
            reader.close()
            return 1
        reader.close()
        
        # Test spine loading from manifest
        sp = spine.Spine.from_manifest(test_files[1], component_id=1, layout=layout_obj)
        if sp.shard_count != 2: 
            sp.close_all()
            return 1
        
        shard, idx = sp.find_shard_and_index(10)
        if shard is None:
            sp.close_all()
            return 1
        
        sp.close_all()
        print "  [OK] Manifest system working"
        
        # === PHASE 1.5: Shard Registry ===
        print "[Phase 1.5] Testing Shard Registry..."
        
        registry = shard_registry.ShardRegistry()
        
        # Register shards
        meta1 = shard_registry.ShardMetadata(test_files[2], 1, 10, 10, 0, 1)
        meta2 = shard_registry.ShardMetadata(test_files[3], 1, 50, 50, 1, 2)
        registry.register_shard(meta1)
        registry.register_shard(meta2)
        
        if len(registry.shards) != 2: return 1
        
        # Test unregister
        if not registry.unregister_shard(test_files[2]): return 1
        if len(registry.shards) != 1: return 1
        
        # Test overlapping detection
        overlapping = registry.find_overlapping_shards(1, 40, 60)
        if len(overlapping) != 1: return 1
        
        # Test read amplification
        amp = registry.get_read_amplification(1, 50)
        if amp != 1: return 1
        
        max_amp = registry.get_max_read_amplification(1)
        if max_amp != 1: return 1
        
        # Create overlapping shards to test high read amplification
        for i in range(5):
            meta = shard_registry.ShardMetadata("shard_%d.db" % i, 1, 10, 10, i, i+1)
            registry.register_shard(meta)
        
        amp = registry.get_read_amplification(1, 10)
        if amp != 5: return 1
        
        # Test compaction marking
        if not registry.mark_for_compaction(1): return 1
        if not registry.needs_compaction(1): return 1
        
        registry.clear_compaction_flag(1)
        if registry.needs_compaction(1): return 1
        
        # Test component filtering
        meta_comp2 = shard_registry.ShardMetadata("comp2.db", 2, 10, 10, 0, 1)
        registry.register_shard(meta_comp2)
        
        comp1_shards = registry.get_shards_for_component(1)
        if len(comp1_shards) != 6: return 1
        
        comp2_shards = registry.get_shards_for_component(2)
        if len(comp2_shards) != 1: return 1
        
        print "  [OK] Shard registry working"
        
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
