"""
gnitz/core/checksum.py
Fast 64-bit checksum for data integrity validation.
Optimized for ll2ctypes performance during testing.
"""
from rpython.rlib.rarithmetic import r_uint64
from rpython.rtyper.lltypesystem import rffi, lltype

# XXH64 Constants
P1 = r_uint64(11400714785074694791)
P2 = r_uint64(14029467366897019727)
P3 = r_uint64(1609587929392839161)
P4 = r_uint64(9650029242287828579)
P5 = r_uint64(2870177450012600261)

def rotl64(x, r):
    return (x << r) | (x >> (64 - r))

def compute_checksum(data_ptr, length):
    """
    64-bit hash optimized to minimize rffi.cast calls inside loops.
    """
    h64 = r_uint64(length) + P5
    
    # Optimization: Cast once to process 8-byte blocks.
    # This significantly speeds up ll2ctypes (untranslated tests).
    limit_8 = length // 8
    if limit_8 > 0:
        u64_ptr = rffi.cast(rffi.ULONGLONGP, data_ptr)
        for i in range(limit_8):
            k1 = u64_ptr[i]
            k1 *= P2
            k1 = rotl64(k1, 31)
            k1 *= P1
            h64 ^= k1
            h64 = rotl64(h64, 27) * P1 + P4
            
    idx = limit_8 * 8
        
    # Process 4-byte remainder
    if idx <= length - 4:
        ptr = rffi.ptradd(data_ptr, idx)
        k1 = rffi.cast(rffi.UINTP, ptr)[0]
        h64 ^= r_uint64(k1) * P1
        h64 = rotl64(h64, 23) * P2 + P3
        idx += 4
        
    # Process 1-byte remainders
    while idx < length:
        h64 ^= r_uint64(ord(data_ptr[idx])) * P5
        h64 = rotl64(h64, 11) * P1
        idx += 1
        
    # Final Mix
    h64 ^= h64 >> 33
    h64 *= P2
    h64 ^= h64 >> 29
    h64 *= P3
    h64 ^= h64 >> 32
    
    return h64

def verify_checksum(data_ptr, length, expected):
    return compute_checksum(data_ptr, length) == expected

def compute_checksum_bytes(data_str):
    length = len(data_str)
    buf = lltype.malloc(rffi.CCHARP.TO, length, flavor='raw')
    try:
        for i in range(length):
            buf[i] = data_str[i]
        return compute_checksum(buf, length)
    finally:
        lltype.free(buf, flavor='raw')
