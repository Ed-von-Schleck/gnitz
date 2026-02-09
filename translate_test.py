import sys
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import (
    memtable, spine, engine, writer_ecs, manifest, 
    shard_registry, refcount, compactor, shard_ecs, wal
)
from gnitz.core import types, values as db_values

def get_weight_helper(engine_inst, eid, vals):
    """Helper to query engine without boxing overhead in the storage layer."""
    layout = engine_inst.layout
    scratch = lltype.malloc(rffi.CCHARP.TO, layout.stride, flavor='raw')
    try:
        for i in range(layout.stride): scratch[i] = '\x00'
        engine_inst.mem_manager.active_table._pack_values_to_buf(scratch, vals)
        blob_base = engine_inst.mem_manager.active_table.blob_arena.base_ptr
        return engine_inst.get_effective_weight_raw(eid, scratch, blob_base)
    finally:
        lltype.free(scratch, flavor='raw')

def entry_point(argv):
    print "--- GnitzDB Full Lifecycle Translation Test ---"
    
    layout_obj = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    
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
        rc = refcount.RefCounter()

        # === PHASE 1: Integrated Ingestion & Persistence ===
        print "[Phase 1] Ingesting with integrated flush..."
        
        wal_writer = wal.WALWriter(db_wal, layout_obj)
        mgr = memtable.MemTableManager(layout_obj, 1024 * 1024, wal_writer=wal_writer)
        m_mgr = manifest.ManifestManager(db_manifest)
        reg = shard_registry.ShardRegistry()
        sp = spine.Spine([], rc)
        
        db = engine.Engine(mgr, sp, m_mgr, reg, component_id=1)
        
        vals1 = [db_values.IntValue(10), db_values.StringValue("version_1")]
        db.mem_manager.put(100, 1, vals1)
        db.flush_and_rotate(shard_a)
        
        vals2 = [db_values.IntValue(20), db_values.StringValue("version_2")]
        db.mem_manager.put(100, 1, vals2)
        
        vals3 = [db_values.IntValue(99), db_values.StringValue("to_delete")]
        db.mem_manager.put(200, 1, vals3)
        db.flush_and_rotate(shard_b)
        
        # === PHASE 1.5: CRASH & RECOVERY SIMULATION ===
        print "[Phase 1.5] Simulating Crash and WAL Recovery..."
        vals4 = [db_values.IntValue(300), db_values.StringValue("recovery_check")]
        db.mem_manager.put(300, 1, vals4)
        db.close() 
        
        print "  Restarting Engine with WAL Recovery..."
        wal_writer_rec = wal.WALWriter(db_wal, layout_obj)
        mgr_rec = memtable.MemTableManager(layout_obj, 1024 * 1024, wal_writer=wal_writer_rec)
        m_mgr_rec = manifest.ManifestManager(db_manifest)
        reg_rec = shard_registry.ShardRegistry()
        sp_rec = spine.Spine.from_manifest(db_manifest, 1, layout_obj, ref_counter=rc)
        db_rec = engine.Engine(mgr_rec, sp_rec, m_mgr_rec, reg_rec, component_id=1, recover_wal_filename=db_wal)
        
        vals_chk = [db_values.IntValue(300), db_values.StringValue("recovery_check")]
        if get_weight_helper(db_rec, 300, vals_chk) != 1: return 1
        print "  [OK] WAL Recovery successful."

        # === PHASE 2: Verify Integrated State ===
        print "[Phase 2] Verifying integrated state..."
        
        vals_100_v1 = [db_values.IntValue(10), db_values.StringValue("version_1")]
        vals_100_v2 = [db_values.IntValue(20), db_values.StringValue("version_2")]
        
        if get_weight_helper(db_rec, 100, vals_100_v1) != 1: return 1
        if get_weight_helper(db_rec, 100, vals_100_v2) != 1: return 1
        
        vals_200 = [db_values.IntValue(99), db_values.StringValue("to_delete")]
        if get_weight_helper(db_rec, 200, vals_200) != 1: return 1
        print "  [OK] Integrated state verified"

        # === PHASE 3: Automated Compaction Execution ===
        print "[Phase 3] Executing automated compaction..."
        reg_rec.mark_for_compaction(1)
        policy = compactor.CompactionPolicy(reg_rec)
        db_rec.flush_and_rotate(shard_c)
        
        new_shard = compactor.execute_compaction(1, policy, m_mgr_rec, rc, layout_obj)
        if new_shard is None: return 1
        test_files.append(new_shard)
        print "  [OK] Compaction completed"
        
        # === PHASE 4: Survivor-Aware Blob Compaction Verification ===
        print "[Phase 4] Verifying Survivor-Aware Blob Compaction..."
        
        long_str_survivor = "SURVIVOR_" * 5 
        vals_surv = [db_values.IntValue(400), db_values.StringValue(long_str_survivor)]
        db_rec.mem_manager.put(400, 1, vals_surv)
        
        long_str_dead = "DEAD_____" * 5 
        vals_dead = [db_values.IntValue(500), db_values.StringValue(long_str_dead)]
        db_rec.mem_manager.put(500, 1, vals_dead)
        db_rec.mem_manager.put(500, -1, vals_dead)
        
        db_rec.flush_and_rotate(shard_blob)
        
        view = shard_ecs.ECSShardView(shard_blob, layout_obj)
        if view.count != 1: return 1
        if view.get_entity_id(0) != 400: return 1
        if view.buf_b.size != 45: return 1
            
        view.close()
        print "  [OK] Ghost Property verified: Annihilated blobs pruned."
        db_rec.close()
        print "=== All Translation Tests Passed ==="
        
    finally:
        for f in test_files:
            if os.path.exists(f): os.unlink(f)
            
    return 0

def target(driver, args):
    return entry_point, None

if __name__ == '__main__':
    entry_point(sys.argv)
