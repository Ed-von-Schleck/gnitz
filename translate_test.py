import sys
import os
from gnitz.storage import memtable, spine, engine, writer

def entry_point(argv):
    print "--- Step 3.4: The Global Engine ---"
    
    fn = "engine_final.db"
    sw = writer.ShardWriter()
    # Create a shard with a range: "a" to "c"
    # This ensures "b" falls inside the range
    sw.add_entry("a", "val_a", 10)
    sw.add_entry("c", "val_c", 10)
    sw.finalize(fn)
    
    mem_mgr = memtable.MemTableManager(1024 * 1024)
    s = spine.Spine([spine.ShardHandle(fn)])
    db = engine.Engine(mem_mgr, s)
    
    try:
        print "[1/3] Testing Spine Lookup (Range Check)..."
        # "b" is not in the shard, but is in the range ["a", "c"]
        # So lookup_candidate_index should return 0, 
        # but find_key_index should return -1.
        if db.get_weight("b") != 0:
            print "Error: 'b' should have weight 0 (not in shard)"
            return 1
            
        if db.get_weight("a") != 10:
            print "Error: 'a' should have weight 10"
            return 1

        print "[2/3] Testing DBSP Summation (Annihilation)..."
        # Spine has a=10. We put a=-10 in MemTable.
        db.mem_manager.put("a", "val_a", -10)
        
        res = db.get_weight("a")
        print "Weight of 'a' after retraction: %d" % res
        if res != 0:
            print "Error: Annihilation failed"
            return 1
            
        print "[3/3] Testing MemTable Only Lookup..."
        db.mem_manager.put("z", "val_z", 42)
        if db.get_weight("z") != 42:
            print "Error: MemTable lookup failed"
            return 1
            
    finally:
        db.close()
        if os.path.exists(fn): os.unlink(fn)
            
    print "Step 3.4 Validation Complete."
    return 0

def target(driver, args):
    return entry_point, None

if __name__ == '__main__':
    entry_point(sys.argv)
