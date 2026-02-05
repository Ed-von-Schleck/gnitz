import sys
import os
from gnitz.storage import memtable, shard

def entry_point(argv):
    print "--- Phase 2: MemTable and Management ---"
    
    mgr = memtable.MemTableManager(1024 * 1024)
    filename = "manager_test.db"
    
    try:
        print "[1/3] Writing via Manager..."
        mgr.put("sensor:001", "23.5", 1)
        mgr.put("sensor:002", "19.1", 1)
        mgr.put("sensor:001", "23.5", 1) # Coalesce to 2
        
        print "Current Usage: %d bytes" % mgr.active_table.get_usage()

        print "[2/3] Rotating and Flushing..."
        mgr.flush_and_rotate(filename)
        
        print "[3/3] Verifying Shard..."
        view = shard.ShardView(filename)
        print "Shard Count: %d" % view.count
        
        if view.count != 2:
            print "Error: Manager failed to flush all entries"
            return 1
            
        if view.get_weight(0) != 2:
            print "Error: Coalescing failed in flushed shard"
            return 1
            
        view.close()
        print "Validation Successful."
        
    except Exception as e:
        print "Manager test failed"
        return 1
    finally:
        mgr.close()
        if os.path.exists(filename):
            os.unlink(filename)
            
    return 0

def target(driver, args):
    return entry_point, None

if __name__ == '__main__':
    entry_point(sys.argv)
