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
    
    # Registry of test files for the test runner
    db_manifest = "test_lifecycle.manifest"
    db_wal = "test_lifecycle.wal"
    shard_a = "shard_a.db"
    shard_b = "shard_b.db"
    
    test_files = [db_manifest, db_wal, shard_a, shard_b]
    for f in test_files:
        if os.path.exists(f): os.unlink(f)
    
    try:
        # Create RefCounter at start
        rc = refcount.RefCounter()

        # === PHASE 1: Integrated Ingestion & Persistence ===
        print "[Phase 1] Ingesting with integrated flush..."
        
        # Setup integrated components WITH WAL
        wal_writer = wal.WALWriter(db_wal, layout_obj)
        mgr = memtable.MemTableManager(layout_obj, 1024 * 1024, wal_writer=wal_writer)
        m_mgr = manifest.ManifestManager(db_manifest)
        reg = shard_registry.ShardRegistry()
        sp = spine.Spine([], rc)
        
        # Create Engine
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
        
        # Write data to WAL via MemTable, but DO NOT FLUSH
        # Entity 300: Weight 1
        db.mem_manager.put(300, 1, 300, "recovery_check")
        
        # Simulate Crash: Close DB (closes WAL) but do NOT flush memtable to disk
        db.close() 
        # wal_writer.close() was called by db.close() via mem_manager
        
        # --- RESTART ---
        print "  Restarting Engine with WAL Recovery..."
        
        # New components
        wal_writer_rec = wal.WALWriter(db_wal, layout_obj) # Re-open for append (or new file in real life)
        mgr_rec = memtable.MemTableManager(layout_obj, 1024 * 1024, wal_writer=wal_writer_rec)
        m_mgr_rec = manifest.ManifestManager(db_manifest)
        reg_rec = shard_registry.ShardRegistry()
        
        # Load existing spine
        sp_rec = spine.Spine.from_manifest(db_manifest, 1, layout_obj, ref_counter=rc)
        
        # Initialize Engine with recovery
        db_rec = engine.Engine(mgr_rec, sp_rec, m_mgr_rec, reg_rec, component_id=1, recover_wal_filename=db_wal)
        
        # Verify Recovery
        # Entity 300 should exist in MemTable (recovered from WAL)
        w300 = db_rec.get_effective_weight(300)
        if w300 != 1:
            print "FAILURE: Recovery failed for Entity 300. Expected weight 1, got %d" % w300
            return 1
            
        print "  [OK] WAL Recovery successful. Data restored."

        # === PHASE 2: Verify Integrated State ===
        print "[Phase 2] Verifying integrated state..."
        
        # Entity 100 exists in two shards (1+1=2)
        if db_rec.get_effective_weight(100) != 2: return 1
        if db_rec.read_component_i64(100, 0) != 20: return 1
        
        # Entity 200 is annihilated in MemTable? No, it was flushed to shard_b.
        # But wait, earlier logic had shard_c manually added.
        # In this run, we didn't add shard_c manually.
        # Entity 200 has weight 1 in shard_b.
        if db_rec.get_effective_weight(200) != 1: return 1
        
        print "  [OK] Integrated state verified"

        # === PHASE 3: Automated Compaction Execution ===
        print "[Phase 3] Executing automated compaction..."
        
        # Trigger compaction
        reg_rec.mark_for_compaction(1)
        policy = compactor.CompactionPolicy(reg_rec)
        
        # Merge shards A, B (and potentially recovered memtable if we flushed it? No, compaction only merges registered shards)
        # Flush the recovered memtable to make it a shard
        shard_c = "shard_c.db"
        test_files.append(shard_c)
        db_rec.flush_and_rotate(shard_c)
        
        # Now we have A, B, C. Compact them.
        new_shard = compactor.execute_compaction(1, policy, m_mgr_rec, rc, layout_obj)
        if new_shard is None: return 1
        test_files.append(new_shard)
        
        print "  [OK] Compaction completed"
        
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
