import os
import shutil
from gnitz.core import zset, types, values as db_values

def run():
    test_dir = "debug_env"
    if os.path.exists(test_dir): shutil.rmtree(test_dir)
    if not os.path.exists(test_dir): os.makedirs(test_dir)
    
    layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    db = zset.PersistentZSet(test_dir, "debug_db", layout)
    
    print "Insert A..."
    db.insert(60, 10, "A")
    f1 = db.flush()
    print "Flush 1: %s" % f1
    w = db.get_weight(60, 10, "A")
    print "Weight A (Pre-Compact): %d (Expected 1)" % w
    
    print "Insert B..."
    db.insert(60, 20, "B")
    f2 = db.flush()
    print "Flush 2: %s" % f2
    
    print "Insert C..."
    db.insert(60, 30, "C")
    f3 = db.flush()
    print "Flush 3: %s" % f3
    
    for i in range(2):
        print "Insert D%d..." % i
        db.insert(60, 40+i, "D")
        fx = db.flush()
        print "Flush D%d: %s" % (i, fx)
    
    # At this point, compaction should have triggered (threshold 4)
    # Check if compaction file exists
    files = os.listdir(test_dir)
    comp_files = [f for f in files if "comp" in f]
    print "Compacted files found: %s" % comp_files
    
    # Check Manifest
    reader = db.manifest_manager.load_current()
    print "Manifest Entries: %d" % reader.get_entry_count()
    for e in reader.iterate_entries():
        print " - %s (LSN %d-%d)" % (e.shard_filename, e.min_lsn, e.max_lsn)
    reader.close()
    
    # Query via Engine (which should still use OLD shards if not reloaded)
    # Wait, the failure is that Engine DOES find it (if old shards) or DOES NOT (if corrupted)?
    # Test says 0 != 1.
    
    wA = db.get_weight(60, 10, "A")
    wB = db.get_weight(60, 20, "B")
    
    print "Query Results:"
    print "A: %d" % wA
    print "B: %d" % wB
    
    if wA != 1:
        print "FAILURE: A lost!"
        return 1
    
    print "SUCCESS"
    db.close()
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(run())
