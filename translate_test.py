import sys
import os
import shutil
from gnitz.core import zset
from gnitz.core import types, values as db_values

def entry_point(argv):
    print "--- GnitzDB Lifecycle Translation Test (High-Level API) ---"
    
    db_dir = "test_run_dir"
    db_name = "translate_db"
    layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    
    if os.path.exists(db_dir): shutil.rmtree(db_dir)

    try:
        print "[Phase 1] Ingesting and Persisting..."
        db = zset.PersistentZSet(db_dir, db_name, layout)
        
        # Wrapping values into a homogeneous List[DBValue]
        vals_100a = [db_values.IntValue(10), db_values.StringValue("short")]
        db.insert(100, vals_100a)
        
        vals_100b = [db_values.IntValue(20), db_values.StringValue("this_is_a_long_string_to_trigger_blob_arena")]
        db.insert(100, vals_100b)
        
        max_u64 = 0xFFFFFFFFFFFFFFFF
        vals_max = [db_values.IntValue(99), db_values.StringValue("boundary")]
        db.insert(max_u64, vals_max)
        
        db.flush()
        
        if db.get_weight(100, vals_100a) != 1: return 1
        if db.get_weight(max_u64, vals_max) != 1: return 1
        print "  [OK] Persistence and u64 boundaries verified."

        print "[Phase 2] WAL Recovery Simulation..."
        vals_200 = [db_values.IntValue(22), db_values.StringValue("wal_recovery_check")]
        db.insert(200, vals_200)
        db.close() 
        
        db = zset.PersistentZSet(db_dir, db_name, layout)
        if db.get_weight(200, vals_200) != 1: return 1
        print "  [OK] WAL Recovery successful."

        print "[Phase 3] Executing Pure Z-Set Compaction..."
        vals_300 = [db_values.IntValue(33), db_values.StringValue("to_be_annihilated")]
        db.insert(300, vals_300)
        db.flush()
        
        db.remove(300, vals_300)
        db.flush()
        
        if db.get_weight(300, vals_300) != 0: return 1
        
        db._trigger_compaction()
        
        if db.get_weight(300, vals_300) != 0: return 1
        
        reader = db.manifest_manager.load_current()
        shard_count = reader.get_entry_count()
        reader.close()
        if shard_count > 3: return 1
        
        print "  [OK] Compaction and Ghost Property verified."
        
        db.close()
        print "=== All Translation Tests Passed ==="
        
    finally:
        if os.path.exists(db_dir): shutil.rmtree(db_dir)
            
    return 0

def target(driver, args):
    return entry_point, None

if __name__ == '__main__':
    entry_point(sys.argv)
