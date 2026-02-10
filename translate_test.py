import sys
import os
from rpython.rlib.rarithmetic import r_uint64
from gnitz.core import zset, types, values as db_values

def entry_point(argv):
    print "--- GnitzDB Integrated Lifecycle Test ---"
    db_dir = "translate_test_db"
    if os.path.exists(db_dir): os.system("rm -rf " + db_dir)
    
    # Table Schema: [PK(i64), Payload(String)]
    layout = types.TableSchema([
        types.ColumnDefinition(types.TYPE_I64), 
        types.ColumnDefinition(types.TYPE_STRING)
    ], 0)
    db = zset.PersistentTable(db_dir, "test", layout)
    
    try:
        def mk_payload(i, s): return [db_values.IntValue(i), db_values.StringValue(s)]
        
        print "[Step 1] Creating Shard A..."
        db.insert(100, mk_payload(100, "base_state"))
        db.flush() 

        print "[Step 2] Modifying in MemTable..."
        db.insert(100, mk_payload(100, "base_state"))
        db.insert(100, mk_payload(100, "base_state"))

        print "[Step 3] Verifying Algebraic Summation..."
        w_100 = db.get_weight(100, mk_payload(100, "base_state"))
        if w_100 != 3: 
            print "ERR: Entity 100 weight mismatch. Expected 3, got %d" % w_100
            return 1
            
        print "  [OK] Integrated lifecycle verified."

    finally:
        db.close()
        if os.path.exists(db_dir): os.system("rm -rf " + db_dir)

    return 0

if __name__ == '__main__': entry_point(sys.argv)
