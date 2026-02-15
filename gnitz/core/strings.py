# gnitz/core/strings.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint, r_uint32

# German String Constants:
# 16-byte total: 4 (len) | 4 (prefix) | 8 (suffix OR heap offset)
SHORT_STRING_THRESHOLD = 12
NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)

def compute_prefix(s):
    """Computes a 4-byte prefix from a string for O(1) equality failure."""
    length = len(s)
    prefix = r_uint(0)
    # Pack up to 4 bytes into a uint32
    max_prefix_len = 4 if length > 4 else length
    for i in range(max_prefix_len):
        prefix |= r_uint(ord(s[i])) << (i * 8)
    return rffi.cast(rffi.UINT, prefix)

def pack_string(target_ptr, string_data, heap_offset_if_long):
    """Packs a string into the 16-byte German String structure."""
    length = len(string_data)
    u32_ptr = rffi.cast(rffi.UINTP, target_ptr)
    
    # Bytes 0-3: Length
    u32_ptr[0] = rffi.cast(rffi.UINT, length)
    
    # Bytes 4-7: Prefix
    u32_ptr[1] = compute_prefix(string_data)
    
    if length <= SHORT_STRING_THRESHOLD:
        # Bytes 8-15: Inline Suffix (starts from index 4)
        payload_ptr = rffi.ptradd(target_ptr, 8)
        start_idx = 4
        for i in range(length - start_idx if length > start_idx else 0):
            payload_ptr[i] = string_data[i + start_idx]
        # Zero-pad remaining bytes for deterministic memory
        for i in range(length - start_idx if length > start_idx else 0, 8):
            payload_ptr[i] = '\x00'
    else:
        # Bytes 8-15: 64-bit Heap Offset
        u64_payload_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target_ptr, 8))
        u64_payload_ptr[0] = rffi.cast(rffi.ULONGLONG, heap_offset_if_long)

@jit.unroll_safe
def compare_structures(len1, pref1, ptr1, heap1, str1,
                       len2, pref2, ptr2, heap2, str2):
    """
    Unified comparison kernel for German Strings.
    Handles (Struct vs Struct), (Struct vs String), (String vs String).
    Returns -1 if LHS < RHS, 1 if LHS > RHS, 0 if Equal.
    
    ptr1/ptr2: rffi.CCHARP to 16-byte struct (or nullptr if string)
    heap1/heap2: rffi.CCHARP to blob heap (or nullptr if string)
    str1/str2: python string (or None if struct)
    """
    # 1. Prefix Check for Optimization (Equality only)
    # If prefixes differ, we MUST iterate to find the lexicographical order
    # because integer comparison of Little Endian packed prefixes 
    # does not match lexicographical byte order.
    
    prefixes_equal = (pref1 == pref2)
    
    min_len = len1 if len1 < len2 else len2
    limit_prefix = 4 if min_len > 4 else min_len

    # 2. Iterate Prefix (0..3) if needed
    if not prefixes_equal:
        # Determine source types for unrolling
        s1_is_str = (str1 is not None)
        s2_is_str = (str2 is not None)
        
        for i in range(limit_prefix):
            c1 = ord(str1[i]) if s1_is_str else ord(ptr1[4 + i])
            c2 = ord(str2[i]) if s2_is_str else ord(ptr2[4 + i])
            if c1 != c2:
                return -1 if c1 < c2 else 1
        
        # Note: If loop finishes without return, prefixes match up to min_len.
        # This implies min_len < 4.

    # 3. Short String Termination
    if min_len <= 4:
        if len1 < len2: return -1
        if len1 > len2: return 1
        return 0

    # 4. Suffix / Heap Comparison (Bytes 4..min_len)
    
    # Resolve Cursor 1
    cursor1 = NULL_PTR
    offset1 = 0
    s1_is_str = (str1 is not None)
    
    if not s1_is_str:
        if len1 <= SHORT_STRING_THRESHOLD:
            # Inline suffix starts at struct offset 8
            # Logical index 4 maps to physical offset 0 relative to (ptr+8)
            cursor1 = rffi.ptradd(ptr1, 8)
            offset1 = -4 
        else:
            # Heap
            u64_1 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr1, 8))[0]
            cursor1 = rffi.ptradd(heap1, rffi.cast(lltype.Signed, u64_1))
            offset1 = 0
            
    # Resolve Cursor 2
    cursor2 = NULL_PTR
    offset2 = 0
    s2_is_str = (str2 is not None)
    
    if not s2_is_str:
        if len2 <= SHORT_STRING_THRESHOLD:
            cursor2 = rffi.ptradd(ptr2, 8)
            offset2 = -4
        else:
            u64_2 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr2, 8))[0]
            cursor2 = rffi.ptradd(heap2, rffi.cast(lltype.Signed, u64_2))
            offset2 = 0

    # Comparison Loop
    for i in range(4, min_len):
        c1 = ord(str1[i]) if s1_is_str else ord(cursor1[i + offset1])
        c2 = ord(str2[i]) if s2_is_str else ord(cursor2[i + offset2])
        if c1 != c2:
            return -1 if c1 < c2 else 1

    # 5. Length Tie-breaker
    if len1 < len2: return -1
    if len1 > len2: return 1
    return 0

@jit.unroll_safe
def string_equals(struct_ptr, heap_base_ptr, search_str, search_len, search_prefix):
    """Compares a packed German String structure against a Python string."""
    u32_ptr = rffi.cast(rffi.UINTP, struct_ptr)
    len1 = rffi.cast(lltype.Signed, u32_ptr[0])
    pref1 = rffi.cast(lltype.Signed, u32_ptr[1])
    
    return compare_structures(
        len1, pref1, struct_ptr, heap_base_ptr, None,
        search_len, rffi.cast(lltype.Signed, search_prefix), NULL_PTR, NULL_PTR, search_str
    ) == 0

@jit.unroll_safe
def string_equals_dual(ptr1, heap1, ptr2, heap2):
    """Compares two packed German String structures for equality."""
    u32_p1 = rffi.cast(rffi.UINTP, ptr1)
    len1 = rffi.cast(lltype.Signed, u32_p1[0])
    pref1 = rffi.cast(lltype.Signed, u32_p1[1])

    u32_p2 = rffi.cast(rffi.UINTP, ptr2)
    len2 = rffi.cast(lltype.Signed, u32_p2[0])
    pref2 = rffi.cast(lltype.Signed, u32_p2[1])

    return compare_structures(
        len1, pref1, ptr1, heap1, None,
        len2, pref2, ptr2, heap2, None
    ) == 0

@jit.unroll_safe
def compare_db_value_to_german(val_obj, german_ptr, heap_ptr):
    """
    Comparison: StringValue vs Packed Structure.
    Returns: -1 if Structure < Value, 1 if Structure > Value, 0 if Equal.
    """
    s_val = val_obj.get_string()
    len_v = len(s_val)
    # Calculate prefix for the python string for optimization
    pref_v = rffi.cast(lltype.Signed, compute_prefix(s_val))
    
    u32_ptr = rffi.cast(rffi.UINTP, german_ptr)
    len_g = rffi.cast(lltype.Signed, u32_ptr[0])
    pref_g = rffi.cast(lltype.Signed, u32_ptr[1])
    
    # compare_structures returns -1 if LHS < RHS.
    # We want -1 if German < Value.
    # LHS = German, RHS = Value.
    return compare_structures(
        len_g, pref_g, german_ptr, heap_ptr, None,
        len_v, pref_v, NULL_PTR, NULL_PTR, s_val
    )

@jit.unroll_safe
def string_compare(ptr1, heap1, ptr2, heap2):
    """Lexicographical comparison of two packed German Strings."""
    u32_p1 = rffi.cast(rffi.UINTP, ptr1)
    len1 = rffi.cast(lltype.Signed, u32_p1[0])
    pref1 = rffi.cast(lltype.Signed, u32_p1[1])

    u32_p2 = rffi.cast(rffi.UINTP, ptr2)
    len2 = rffi.cast(lltype.Signed, u32_p2[0])
    pref2 = rffi.cast(lltype.Signed, u32_p2[1])

    return compare_structures(
        len1, pref1, ptr1, heap1, None,
        len2, pref2, ptr2, heap2, None
    )
