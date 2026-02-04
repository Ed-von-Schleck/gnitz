import sys
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import layout, errors, arena

def entry_point(argv):
    print "--- Step 2.1: Arena Checks ---"
    
    # Allocate a small 1KB arena
    mem = arena.Arena(1024)
    try:
        print "Allocating blocks..."
        ptr1 = mem.alloc(10)
        ptr2 = mem.alloc(20)
        
        # Verify we can write to the allocated memory
        ptr1[0] = 'A'
        ptr2[0] = 'B'
        
        if ptr1[0] != 'A' or ptr2[0] != 'B':
            print "Error: Memory write/read failed"
            return 1
            
        print "Arena allocation and access successful."
        
        # Test reset
        mem.reset()
        if mem.offset != 0:
            print "Error: Reset failed"
            return 1
            
    except errors.StorageError:
        print "Error: Unexpected Arena exhaustion"
        return 1
    finally:
        mem.free()
        
    print "Step 2.1 Validation Complete."
    return 0

def target(driver, args):
    return entry_point, None

if __name__ == '__main__':
    entry_point(sys.argv)
