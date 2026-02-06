import sys
import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import memtable, spine, engine, writer_ecs, layout, shard_ecs, manifest, shard_registry, refcount, tournament_tree
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
        "test_shard3.db",
        "test_refcount1.db",
        "test_refcount2.db",
        "test_tt1.db",
        "test_tt2.db",
        "test_tt3.db"
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
        
        # === PHASE 1.6: Reference Counting ===
        print "[Phase 1.6] Testing Reference Counting..."
        
        rc = refcount.RefCounter()
        
        # Create test files
        with open(test_files[5], 'w') as f: f.write("test1")
        with open(test_files[6], 'w') as f: f.write("test2")
        
        # Test acquire/release
        rc.acquire(test_files[5])
        if rc.get_refcount(test_files[5]) != 1: return 1
        if rc.can_delete(test_files[5]): return 1
        
        rc.release(test_files[5])
        if rc.get_refcount(test_files[5]) != 0: return 1
        if not rc.can_delete(test_files[5]): return 1
        
        # Test mark for deletion with active refs
        rc.acquire(test_files[5])
        rc.mark_for_deletion(test_files[5])
        
        deleted = rc.try_cleanup()
        if len(deleted) != 0: return 1
        if not os.path.exists(test_files[5]): return 1
        
        # Release and cleanup
        rc.release(test_files[5])
        deleted = rc.try_cleanup()
        if len(deleted) != 1: return 1
        if os.path.exists(test_files[5]): return 1
        
        # Test multiple files
        rc.mark_for_deletion(test_files[6])
        deleted = rc.try_cleanup()
        if len(deleted) != 1: return 1
        if os.path.exists(test_files[6]): return 1
        
        print "  [OK] Reference counting working"
        
        # === PHASE 1.7: Tournament Tree ===
        print "[Phase 1.7] Testing Tournament Tree..."
        
        # Create test shards for tournament tree
        # Shard 1: [1, 5, 9]
        # Shard 2: [2, 6, 10]
        # Shard 3: [3, 7, 11]
        w1 = writer_ecs.ECSShardWriter(layout_obj)
        w1.add_entity(1, 10, "a")
        w1.add_entity(5, 50, "e")
        w1.add_entity(9, 90, "i")
        w1.finalize(test_files[7])
        
        w2 = writer_ecs.ECSShardWriter(layout_obj)
        w2.add_entity(2, 20, "b")
        w2.add_entity(6, 60, "f")
        w2.add_entity(10, 100, "j")
        w2.finalize(test_files[8])
        
        w3 = writer_ecs.ECSShardWriter(layout_obj)
        w3.add_entity(3, 30, "c")
        w3.add_entity(7, 70, "g")
        w3.add_entity(11, 110, "k")
        w3.finalize(test_files[9])
        
        # Create views and cursors
        view1 = shard_ecs.ECSShardView(test_files[7], layout_obj)
        view2 = shard_ecs.ECSShardView(test_files[8], layout_obj)
        view3 = shard_ecs.ECSShardView(test_files[9], layout_obj)
        
        cursor1 = tournament_tree.StreamCursor(view1)
        cursor2 = tournament_tree.StreamCursor(view2)
        cursor3 = tournament_tree.StreamCursor(view3)
        
        # Create tournament tree and merge
        tree = tournament_tree.TournamentTree([cursor1, cursor2, cursor3])
        
        expected = [1, 2, 3, 5, 6, 7, 9, 10, 11]
        result = []
        
        while not tree.is_exhausted():
            min_eid = tree.get_min_entity_id()
            result.append(min_eid)
            tree.advance_min_cursors()
        
        if result != expected:
            view1.close()
            view2.close()
            view3.close()
            return 1
        
        view1.close()
        view2.close()
        view3.close()
        
        # Test overlapping entities
        w4 = writer_ecs.ECSShardWriter(layout_obj)
        w4.add_entity(5, 100, "overlap1")
        w4.finalize("test_overlap1.db")
        
        w5 = writer_ecs.ECSShardWriter(layout_obj)
        w5.add_entity(5, 200, "overlap2")
        w5.finalize("test_overlap2.db")
        
        v4 = shard_ecs.ECSShardView("test_overlap1.db", layout_obj)
        v5 = shard_ecs.ECSShardView("test_overlap2.db", layout_obj)
        
        c4 = tournament_tree.StreamCursor(v4)
        c5 = tournament_tree.StreamCursor(v5)
        
        tree2 = tournament_tree.TournamentTree([c4, c5])
        
        if tree2.get_min_entity_id() != 5: 
            v4.close()
            v5.close()
            return 1
        
        cursors_at_min = tree2.get_all_cursors_at_min()
        if len(cursors_at_min) != 2:
            v4.close()
            v5.close()
            return 1
        
        v4.close()
        v5.close()
        
        if os.path.exists("test_overlap1.db"): os.unlink("test_overlap1.db")
        if os.path.exists("test_overlap2.db"): os.unlink("test_overlap2.db")
        
        print "  [OK] Tournament tree working"
        
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
