from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.core import types, values, strings as string_logic

def get_key_offset(height):
    """
    Calculates the 16-byte aligned offset for the Primary Key.
    Structure: [8: Weight] [1: Height] [3: Pad] [height*4: Pointers] [ALIGN to 16]
    """
    # Fixed header: 8 (weight) + 1 (height) + 3 (padding) = 12
    # Pointer array: height * 4
    raw_size = 12 + (height * 4)
    return (raw_size + 15) & ~15

def node_get_next_off(base_ptr, node_off, level):
    """Reads the arena offset for the next node at the given SkipList level."""
    ptr = rffi.ptradd(base_ptr, node_off + 12)
    u32_ptr = rffi.cast(rffi.UINTP, ptr)
    return rffi.cast(lltype.Signed, u32_ptr[level])

def node_set_next_off(base_ptr, node_off, level, target_off):
    """Sets the arena offset for the next node at the given SkipList level."""
    ptr = rffi.ptradd(base_ptr, node_off + 12)
    u32_ptr = rffi.cast(rffi.UINTP, ptr)
    u32_ptr[level] = rffi.cast(rffi.UINT, target_off)

def node_get_weight(base_ptr, node_off):
    """Reads the 64-bit signed weight of the record."""
    ptr = rffi.ptradd(base_ptr, node_off)
    return rffi.cast(rffi.LONGLONGP, ptr)[0]

def node_set_weight(base_ptr, node_off, weight):
    """Sets the 64-bit signed weight of the record."""
    ptr = rffi.ptradd(base_ptr, node_off)
    rffi.cast(rffi.LONGLONGP, ptr)[0] = rffi.cast(rffi.LONGLONG, weight)

def node_get_key(base_ptr, node_off, key_size):
    """Reads the Primary Key (u64 or u128) using aligned geometry."""
    # RPython: ord() is safe for height retrieval
    height = ord(base_ptr[node_off + 8])
    key_off = get_key_offset(height)
    ptr = rffi.ptradd(base_ptr, node_off + key_off)
    
    if key_size == 16:
        lo = rffi.cast(rffi.ULONGLONGP, ptr)[0]
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
        return (r_uint128(hi) << 64) | r_uint128(lo)
    
    return r_uint128(rffi.cast(rffi.ULONGLONGP, ptr)[0])

def node_get_payload_ptr(base_ptr, node_off, key_size):
    """Returns a pointer to the start of the packed row payload."""
    height = ord(base_ptr[node_off + 8])
    key_off = get_key_offset(height)
    return rffi.ptradd(base_ptr, node_off + key_off + key_size)
