from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit

# The threshold for inlining a string. 4 bytes for length, 4 for prefix,
# and 8 for payload. The prefix and payload can hold up to 12 bytes.
SHORT_STRING_THRESHOLD = 12

def pack_string(target_ptr, string_data, heap_offset_if_long):
    """
    Packs a Python string into a 16-byte German String struct.

    Args:
        target_ptr: A raw CCHARP pointer to the 16-byte destination buffer.
        string_data: The Python string to pack.
        heap_offset_if_long: The u64 offset into the Blob Heap to use if
                             the string is too long to be inlined.
    """
    length = len(string_data)
    
    # Cast target pointer to different integer types for easy manipulation
    u32_ptr = rffi.cast(rffi.UINTP, target_ptr)
    u64_ptr = rffi.cast(rffi.ULONGLONGP, target_ptr)
    
    # 1. Write Length (Bytes 0-3)
    u32_ptr[0] = rffi.cast(rffi.UINT, length)
    
    # 2. Handle Short vs. Long String
    if length <= SHORT_STRING_THRESHOLD:
        # Inline the string data directly into the struct
        # Copy prefix and suffix into bytes 4-15
        payload_ptr = rffi.ptradd(target_ptr, 4)
        for i in range(length):
            payload_ptr[i] = string_data[i]
        # Zero-pad the rest of the struct
        for i in range(length, SHORT_STRING_THRESHOLD):
            payload_ptr[i] = '\x00'
    else:
        # Store prefix and a pointer to the heap
        # Copy prefix into bytes 4-7
        prefix_ptr = rffi.ptradd(target_ptr, 4)
        for i in range(4):
            prefix_ptr[i] = string_data[i]
            
        # Write heap offset into bytes 8-15
        u64_payload_ptr = rffi.ptradd(rffi.cast(rffi.CCHARP, u64_ptr), 8)
        rffi.cast(rffi.ULONGLONGP, u64_payload_ptr)[0] = rffi.cast(rffi.ULONGLONG, heap_offset_if_long)

def _memcmp(p1, p2, length):
    """Helper for raw memory comparison."""
    for i in range(length):
        if p1[i] != p2[i]:
            return False
    return True

@jit.elidable
def string_equals(struct_ptr, heap_base_ptr, search_str):
    """
    Performs an optimized equality check between a 16-byte German String
    and a standard Python string.

    Args:
        struct_ptr: Pointer to the 16-byte string struct.
        heap_base_ptr: Base pointer of the Blob Heap (for long strings).
        search_str: The Python string to compare against.
    """
    search_len = len(search_str)
    
    u32_struct_ptr = rffi.cast(rffi.UINTP, struct_ptr)
    
    # 1. Check Length (fastest check)
    length_in_struct = rffi.cast(lltype.Signed, u32_struct_ptr[0])
    if length_in_struct != search_len:
        return False
        
    # If the string is empty, we are done.
    if search_len == 0:
        return True
    
    # 2. Check Prefix (eliminates most non-matches)
    # Read the 4-byte prefix from the struct (bytes 4-7)
    prefix_in_struct_uint = u32_struct_ptr[1]
    
    # Construct the prefix from the search string
    prefix_search_signed = 0
    max_prefix_len = 4 if search_len > 4 else search_len
    for i in range(max_prefix_len):
        prefix_search_signed |= ord(search_str[i]) << (i * 8)
        
    if rffi.cast(lltype.Signed, prefix_in_struct_uint) != prefix_search_signed:
        return False
        
    # If the string was <= 4 bytes, the prefix match is a full match.
    if search_len <= 4:
        return True
        
    # 3. Resolve Remainder
    if search_len <= SHORT_STRING_THRESHOLD:
        # Short String: Compare the remaining bytes inline
        suffix_ptr_struct = rffi.ptradd(struct_ptr, 8)
        return _memcmp(suffix_ptr_struct, search_str[4:], search_len - 4)
    else:
        # Long String: Compare against the heap
        u64_struct_ptr = rffi.cast(rffi.ULONGLONGP, struct_ptr)
        offset = rffi.cast(lltype.Signed, u64_struct_ptr[1])
        
        heap_ptr = rffi.ptradd(heap_base_ptr, offset)
        return _memcmp(heap_ptr, search_str, search_len)
