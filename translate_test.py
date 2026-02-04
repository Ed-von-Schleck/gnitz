import sys
import os
from gnitz.storage import writer, shard, errors

def entry_point(argv):
    print "--- Phase 1: Storage Core Final Validation ---"
    
    filename = "final_test.db"
    sw = writer.ShardWriter()
    sw.add_entry("id:001", "Alice", 1)
    sw.add_entry("id:002", "Bob", 1)
    
    try:
        print "[1/3] Writing Shard..."
        sw.finalize(filename)
        
        print "[2/3] Opening ShardView..."
        view = shard.ShardView(filename)
        
        if view.count != 2:
            print "Error: Count mismatch"
            return 1
            
        print "[3/3] Verifying Data..."
        w1 = view.get_weight(0)
        k1 = view.materialize_key(0)
        v1 = view.materialize_value(0)
        
        print "Entry 0: Key=%s, Val=%s, Weight=%d" % (k1, v1, w1)
        
        if k1 != "id:001" or v1 != "Alice" or w1 != 1:
            print "Error: Data corruption"
            return 1
            
        view.close()
        print "Validation Successful."
        
    except Exception as e:
        print "Unexpected error during validation"
        return 1
    finally:
        if os.path.exists(filename):
            os.unlink(filename)
            
    return 0

def target(driver, args):
    return entry_point, None

if __name__ == '__main__':
    entry_point(sys.argv)
