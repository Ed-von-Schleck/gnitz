"""
gnitz/core/checksum.py
Fast checksum for data integrity validation using DJB2 hash.
"""
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64

CHECKSUM_SEED = 0x474E495442000000

def compute_checksum(data_ptr, length):
    """Compute DJB2-like hash for the given data."""
    h = r_uint64(5381)
    for i in range(length):
        byte_val = r_uint64(ord(data_ptr[i]))
        h = (h << r_uint64(5)) + h + byte_val
    return h

def verify_checksum(data_ptr, length, expected):
    """
    Verifies checksum matches expected value.
    
    Args:
        data_ptr: rffi.CCHARP pointer to data
        length: Size in bytes
        expected: Expected checksum value
    
    Returns:
        True if checksum matches
    """
    actual = compute_checksum(data_ptr, length)
    return actual == expected

def compute_checksum_bytes(data_str):
    """
    Computes checksum for a Python string (for testing).
    
    Args:
        data_str: Python string
    
    Returns:
        64-bit checksum
    """
    length = len(data_str)
    buf = lltype.malloc(rffi.CCHARP.TO, length, flavor='raw')
    try:
        for i in range(length):
            buf[i] = data_str[i]
        return compute_checksum(buf, length)
    finally:
        lltype.free(buf, flavor='raw')
