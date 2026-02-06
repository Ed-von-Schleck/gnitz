from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit

SHORT_STRING_THRESHOLD = 12

def compute_prefix(s):
    length = len(s)
    prefix = 0
    max_prefix_len = 4 if length > 4 else length
    for i in range(max_prefix_len):
        prefix |= ord(s[i]) << (i * 8)
    return rffi.cast(rffi.UINT, prefix)

def pack_string(target_ptr, string_data, heap_offset_if_long):
    length = len(string_data)
    u32_ptr = rffi.cast(rffi.UINTP, target_ptr)
    
    u32_ptr[0] = rffi.cast(rffi.UINT, length)
    
    if length <= SHORT_STRING_THRESHOLD:
        payload_ptr = rffi.ptradd(target_ptr, 4)
        for i in range(length):
            payload_ptr[i] = string_data[i]
        for i in range(length, SHORT_STRING_THRESHOLD):
            payload_ptr[i] = '\x00'
    else:
        u32_ptr[1] = compute_prefix(string_data)
        u64_payload_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target_ptr, 8))
        u64_payload_ptr[0] = rffi.cast(rffi.ULONGLONG, heap_offset_if_long)

def _memcmp(p1, p2, length):
    for i in range(length):
        if p1[i] != p2[i]:
            return False
    return True

def string_equals(struct_ptr, heap_base_ptr, search_str, search_len, search_prefix):
    u32_struct_ptr = rffi.cast(rffi.UINTP, struct_ptr)
    
    if rffi.cast(lltype.Signed, u32_struct_ptr[0]) != search_len:
        return False
        
    if search_len == 0:
        return True
    
    if u32_struct_ptr[1] != search_prefix:
        return False
        
    if search_len <= 4:
        return True
        
    if search_len <= SHORT_STRING_THRESHOLD:
        suffix_ptr_struct = rffi.ptradd(struct_ptr, 8)
        for i in range(search_len - 4):
            if suffix_ptr_struct[i] != search_str[i + 4]:
                return False
        return True
    else:
        u64_struct_ptr = rffi.cast(rffi.ULONGLONGP, struct_ptr)
        offset = rffi.cast(lltype.Signed, u64_struct_ptr[1])
        heap_ptr = rffi.ptradd(heap_base_ptr, offset)
        return _memcmp(heap_ptr, search_str, search_len)
