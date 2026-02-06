"""
gnitz/core/strings.py
"""
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit

# The threshold for inlining a string. 4 bytes for length, 4 for prefix,
# and 8 for payload. The prefix and payload together hold up to 12 bytes.
SHORT_STRING_THRESHOLD = 12

def compute_prefix(s):
    """
    Precomputes the 4-byte prefix integer for a Python string.
    Used by the VM to avoid redundant calculations during scans.
    """
    length = len(s)
    prefix = 0
    max_prefix_len = 4 if length > 4 else length
    for i in range(max_prefix_len):
        prefix |= ord(s[i]) << (i * 8)
    return rffi.cast(rffi.UINT, prefix)

def pack_string(target_ptr, string_data, heap_offset_if_long):
    """
    Packs a Python string into a 16-byte German String struct.
    """
    length = len(string_data)
    u32_ptr = rffi.cast(rffi.UINTP, target_ptr)
    
    # 1. Write Length (Bytes 0-3)
    u32_ptr[0] = rffi.cast(rffi.UINT, length)
    
    # 2. Handle Short vs. Long String
    if length <= SHORT_STRING_THRESHOLD:
        # Inline everything (Prefix + Suffix) into bytes 4-15
        payload_ptr = rffi.ptradd(target_ptr, 4)
        for i in range(length):
            payload_ptr[i] = string_data[i]
        for i in range(length, SHORT_STRING_THRESHOLD):
            payload_ptr[i] = '\x00'
    else:
        # Store Prefix (Bytes 4-7)
        u32_ptr[1] = compute_prefix(string_data)
            
        # Write heap offset (Bytes 8-15)
        u64_payload_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target_ptr, 8))
        u64_payload_ptr[0] = rffi.cast(rffi.ULONGLONG, heap_offset_if_long)

def _memcmp(p1, p2, length):
    for i in range(length):
        if p1[i] != p2[i]:
            return False
    return True

@jit.elidable
def string_equals(struct_ptr, heap_base_ptr, search_str, search_len, search_prefix):
    """
    Optimized equality check using precomputed metadata.
    
    Args:
        struct_ptr: Pointer to the 16-byte German String struct.
        heap_base_ptr: Base pointer of the Blob Heap.
        search_str: The raw Python string for deep comparison.
        search_len: Precomputed length of search_str.
        search_prefix: Precomputed u32 prefix of search_str.
    """
    u32_struct_ptr = rffi.cast(rffi.UINTP, struct_ptr)
    
    # 1. O(1) Length check
    if rffi.cast(lltype.Signed, u32_struct_ptr[0]) != search_len:
        return False
        
    if search_len == 0:
        return True
    
    # 2. O(1) Prefix check
    if u32_struct_ptr[1] != search_prefix:
        return False
        
    if search_len <= 4:
        return True
        
    # 3. Resolution
    if search_len <= SHORT_STRING_THRESHOLD:
        # Compare inline suffix (struct bytes 8-15)
        suffix_ptr_struct = rffi.ptradd(struct_ptr, 8)
        # Comparing search_str from index 4 onwards
        for i in range(search_len - 4):
            if suffix_ptr_struct[i] != search_str[i + 4]:
                return False
        return True
    else:
        # Compare against Blob Heap
        u64_struct_ptr = rffi.cast(rffi.ULONGLONGP, struct_ptr)
        offset = rffi.cast(lltype.Signed, u64_struct_ptr[1])
        heap_ptr = rffi.ptradd(heap_base_ptr, offset)
        
        # Note: Prefix already matched, but for long strings we compare 
        # the full buffer to avoid complexity in offset math.
        return _memcmp(heap_ptr, search_str, search_len)
