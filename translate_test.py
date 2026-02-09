import sys
import os
from rpython.rlib.rarithmetic import r_uint64
from gnitz.core import zset, types, values as db_values

def entry_point(argv):
    print "--- GnitzDB Integrated Lifecycle Test ---"
    db_dir = "translate_test_db"
    if os.path.exists(db_dir): os.system("rm -rf " + db_dir)
    
    # Layout: [ID (i64), Payload (String)]
    layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    db = zset.PersistentZSet(db_dir, "test", layout)
    
    try:
        # Helper to create payloads
        def mk_payload(i, s): return [db_values.IntValue(i), db_values.StringValue(s)]
        
        # 1. FLUSHED TIER: Insert Entity 100 (+1) and flush to Shard A
        print "[Step 1] Creating Shard A..."
        db.insert(r_uint64(100), mk_payload(1, "base_state"))
        db.flush() 

        # 2. ACTIVE TIER: Insert Entity 100 (+2) into MemTable
        # Total weight for (100, "base_state") should be 1 (Shard) + 2 (Mem) = 3
        print "[Step 2] Modifying in MemTable..."
        db.insert(r_uint64(100), mk_payload(1, "base_state"))
        db.insert(r_uint64(100), mk_payload(1, "base_state"))

        # 3. GHOST TIER: Insert Entity 200 (+1) in Shard, (-1) in MemTable
        # Net weight should be 0 (Annihilation across tiers)
        db.insert(r_uint64(200), mk_payload(2, "ghost"))
        db.flush() # Move +1 to Shard B
        db.remove(r_uint64(200), mk_payload(2, "ghost")) # -1 stays in MemTable

        # 4. BLOB TIER: Test German String Heap Logic
        long_str = "x" * 50 # Longer than 12 bytes
        db.insert(r_uint64(300), mk_payload(3, long_str))

        # --- VERIFICATION ---
        print "[Step 3] Verifying Algebraic Summation..."
        
        # Check 1: Multi-tier summation
        w_100 = db.get_weight(r_uint64(100), mk_payload(1, "base_state"))
        if w_100 != 3: 
            print "ERR: Entity 100 weight mismatch. Expected 3, got %d" % w_100
            return 1
            
        # Check 2: Cross-tier annihilation
        w_200 = db.get_weight(r_uint64(200), mk_payload(2, "ghost"))
        if w_200 != 0:
            print "ERR: Entity 200 ghost failed. Expected 0, got %d" % w_200
            return 1
            
        # Check 3: Long string retrieval (validates blob arena/heap pointers)
        w_300 = db.get_weight(r_uint64(300), mk_payload(3, long_str))
        if w_300 != 1:
            print "ERR: Entity 300 blob failed. Expected 1, got %d" % w_300
            return 1

        print "  [OK] All algebraic variants verified."

    finally:
        db.close()
        if os.path.exists(db_dir): os.system("rm -rf " + db_dir)

    return 0

def target(driver, args): return entry_point, None
if __name__ == '__main__': entry_point(sys.argv)
