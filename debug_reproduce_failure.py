import os
import shutil
from gnitz.core import zset, types

def run():
    test_dir = "debug_env"
    if os.path.exists(test_dir): shutil.rmtree(test_dir)
    os.makedirs(test_dir)
    
    layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    db = zset.PersistentZSet(test_dir, "debug_db", layout)
    
    print "Insert A..."
    db.insert(60, 10, "A")
    db.flush()
    w = db.get_weight(60, 10, "A")
    print "Weight A: %d (Expected 1)" % w
    
    print "Insert B..."
    db.insert(60, 20, "B")
    db.flush()
    
    print "Insert C..."
    db.insert(60, 30, "C")
    db.flush()
    
    for i in range(2):
        db.insert(60, 40+i, "D")
        db.flush()
    
    wA = db.get_weight(60, 10, "A")
    wB = db.get_weight(60, 20, "B")
    
    print "Query Results:"
    print "A: %d" % wA
    print "B: %d" % wB
    
    if wA != 1:
        print "FAILURE: A lost!"
        db.close()
        return 1
    
    print "SUCCESS"
    db.close()
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(run())
