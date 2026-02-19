import sys
from rpython.rlib.rarithmetic import r_uint, r_uint64
# Note: Use the correct import path based on your package structure
from gnitz.core.xxh import compute_checksum_bytes

def get_hello_reference():
    # Finalized XXH3_64 reference for "hello" -> 0x9503647703396263
    hi = r_uint(0x95036477)
    lo = r_uint(0x03396263)
    return (r_uint64(hi) << 32) | r_uint64(lo)

def entry_point(argv):
    ref = get_hello_reference()
    result = compute_checksum_bytes("hello")
    
    print "Testing 'hello'..."
    print "  Expected: 0x%x" % ref
    print "  Actual:   0x%x" % result
    
    if result == ref:
        print "SUCCESS: Matches v0.8+ stable XXH3"
        return 0
    else:
        print "FAILURE: Mismatch!"
        return 1

def target(driver, args):
    return entry_point, None

if __name__ == "__main__":
    # Standard Python 2.7 execution
    sys.exit(entry_point(sys.argv))
