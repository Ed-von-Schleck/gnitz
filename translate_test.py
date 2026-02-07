import sys
import os
from gnitz.storage import (
    memtable, spine, engine, writer_ecs, manifest, 
    shard_registry, refcount, compactor, shard_ecs, wal
)
from gnitz.core import types

def entry_point(argv):
    print "--- GnitzDB Full Lifecycle Translation Test ---"
    
    # 1. Setup Schema: [Value (I64), Label (String)]
    layout_obj = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    
    # Registry of test files
    db_manifest = "test_lifecycle.manifest"
    db_wal = "test_lifecycle.wal"
    shard_a = "shard_a.db"
    shard_b = "shard_b.db"
    shard_c = "shard_c.db"
    shard_blob = "shard_blob.db"
    
    test_files = [db_manifest, db_wal, shard_a, shard_b, shard_c, shard_blob]
    for f in test_files:
        if os.path.exists(f): os.unlink(f)
    
    try:
        # Create RefCounter at start
        rc = refcount.RefCounter()

        # === PHASE 1: Integrated Ingestion & Persistence ===
        print "[Phase 1] Ingesting with integrated flush..."
        
        wal_writer = wal.WALWriter(db_wal, layout_obj)
        mgr = memtable.MemTableManager(layout_obj, 1024 * 1024, wal_writer=wal_writer)
        m_mgr = manifest.ManifestManager(db_manifest)
        reg = shard_registry.ShardRegistry()
        sp = spine.Spine([], rc)
        
        db = engine.Engine(mgr, sp, m_mgr, reg, component_id=1, current_lsn=1)
        
        # Entity 100: First version (Weight +1)
        db.mem_manager.put(100, 1, 10, "version_1")
        db.flush_and_rotate(shard_a)
        
        # Entity 100: Second version (Weight +1, Net weight should become 2)
        # Entity 200: To be deleted (Annihilated)
        db.mem_manager.put(100, 1, 20, "version_2")
        db.mem_manager.put(200, 1, 99, "to_delete")
        db.flush_and_rotate(shard_b)
        
        # === PHASE 1.5: CRASH & RECOVERY SIMULATION ===
        print "[Phase 1.5] Simulating Crash and WAL Recovery..."
        db.mem_manager.put(300, 1, 300, "recovery_check")
        db.close() 
        
        # --- RESTART ---
        print "  Restarting Engine with WAL Recovery..."
        wal_writer_rec = wal.WALWriter(db_wal, layout_obj)
        mgr_rec = memtable.MemTableManager(layout_obj, 1024 * 1024, wal_writer=wal_writer_rec)
        m_mgr_rec = manifest.ManifestManager(db_manifest)
        reg_rec = shard_registry.ShardRegistry()
        sp_rec = spine.Spine.from_manifest(db_manifest, 1, layout_obj, ref_counter=rc)
        db_rec = engine.Engine(mgr_rec, sp_rec, m_mgr_rec, reg_rec, component_id=1, recover_wal_filename=db_wal)
        
        if db_rec.get_effective_weight(300) != 1: return 1
        print "  [OK] WAL Recovery successful."

        # === PHASE 2: Verify Integrated State ===
        print "[Phase 2] Verifying integrated state..."
        if db_rec.get_effective_weight(100) != 2: return 1
        if db_rec.get_effective_weight(200) != 1: return 1
        print "  [OK] Integrated state verified"

        # === PHASE 3: Automated Compaction Execution ===
        print "[Phase 3] Executing automated compaction..."
        reg_rec.mark_for_compaction(1)
        policy = compactor.CompactionPolicy(reg_rec)
        db_rec.flush_and_rotate(shard_c) # Flush the recovered memtable
        
        new_shard = compactor.execute_compaction(1, policy, m_mgr_rec, rc, layout_obj)
        if new_shard is None: return 1
        test_files.append(new_shard)
        print "  [OK] Compaction completed"
        
        # === PHASE 4: Survivor-Aware Blob Compaction Verification ===
        print "[Phase 4] Verifying Survivor-Aware Blob Compaction..."
        
        # Insert Entity 400 (Survivor) with Long String
        long_str_survivor = "SURVIVOR_" * 5 # 45 bytes
        db_rec.mem_manager.put(400, 1, 400, long_str_survivor)
        
        # Insert Entity 500 (Annihilated) with Long String
        long_str_dead = "DEAD_____" * 5 # 45 bytes
        db_rec.mem_manager.put(500, 1, 500, long_str_dead)
        db_rec.mem_manager.put(500, -1, 500, long_str_dead)
        
        # Flush to shard_blob
        db_rec.flush_and_rotate(shard_blob)
        
        # Inspect shard_blob
        view = shard_ecs.ECSShardView(shard_blob, layout_obj)
        
        # Expect 1 entity (400)
        if view.count != 1: 
            print "FAILURE: Expected 1 entity in blob shard, got %d" % view.count
            return 1
        if view.get_entity_id(0) != 400: return 1
        
        # Check Blob Region Size
        # Should be exactly 45 bytes (len of long_str_survivor)
        # If ghost property failed, it would be 90 bytes
        if view.buf_b.size != 45:
            print "FAILURE: Blob region size mismatch. Expected 45, got %d" % view.buf_b.size
            return 1
            
        view.close()
        print "  [OK] Ghost Property verified: Annihilated blobs pruned."

        db_rec.close()
        
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
