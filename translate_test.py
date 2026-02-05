import sys
import os
from gnitz.storage import spine, writer

def entry_point(argv):
    print "--- Step 3.1: Shard Handles ---"
    fn = "spine_test.db"
    sw = writer.ShardWriter()
    sw.add_entry("key1", "val1", 1)
    sw.finalize(fn)
    
    try:
        print "Opening ShardHandle..."
        handle = spine.ShardHandle(fn)
        print "Handle Min Key: %s" % handle.min_key
        print "Handle Max Key: %s" % handle.max_key
        
        if handle.min_key != "key1":
            print "Error: MinKey mismatch"
            return 1
            
        val = handle.materialize_value(0)
        print "Value: %s" % val
        if val != "val1":
            print "Error: Value mismatch"
            return 1
            
        handle.close()
    except Exception as e:
        print "Exception in Step 3.1"
        return 1
    finally:
        if os.path.exists(fn):
            os.unlink(fn)
            
    print "Step 3.1 Validation Complete."
    return 0

def target(driver, args):
    return entry_point, None

if __name__ == '__main__':
    entry_point(sys.argv)
