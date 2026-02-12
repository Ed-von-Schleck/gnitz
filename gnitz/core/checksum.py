"""
gnitz/core/checksum.py

Implementation of XXH3_64bits (Scalar).
Compatible with RPython (no native 128-bit integer types).
References: https://github.com/Cyan4973/xxHash
"""

from rpython.rlib.rarithmetic import r_uint, r_uint32, r_uint64
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.jit import unroll_safe

# ===========================================================================
# Constants (XXH3 Primes & Secret)
# ===========================================================================

XXH_PRIME32_1 = r_uint32(0x9E3779B1)
XXH_PRIME32_2 = r_uint32(0x85EBCA77)
XXH_PRIME32_3 = r_uint32(0xC2B2AE3D)
XXH_PRIME32_4 = r_uint32(0x27D4EB2F)
XXH_PRIME32_5 = r_uint32(0x165667B1)

XXH_PRIME64_1 = r_uint64(0x9E3779B185EBCA87)
XXH_PRIME64_2 = r_uint64(0xC2B2AE3D27D4EB4F)
XXH_PRIME64_3 = r_uint64(0x165667B19E3779F9)
XXH_PRIME64_4 = r_uint64(0x85EBCA77C2B2AE63)
XXH_PRIME64_5 = r_uint64(0x27D4EB2F165667C5)

XXH3_SECRET_SIZE_MIN = 136
XXH_STRIPE_LEN = 64
XXH_ACC_NB = 8

# Default XXH3 Secret (192 bytes)
XXH3_kSecret = (
    "\xb8\xfe\x6c\x39\x23\xa4\x4b\xbe\x7c\x01\x81\x2c\xf7\x21\xad\x1c"
    "\xde\xd4\x6d\xe9\x83\x90\x97\xdb\x72\x40\xa4\xa4\xb7\xb3\x67\x1f"
    "\xcb\x79\xe6\x4e\xcc\xc0\xe5\x78\x82\x5a\xd0\x7d\xcc\xff\x72\x21"
    "\xb8\x08\x46\x74\xf7\x43\x24\x8e\xe0\x35\x90\xe6\x81\x3a\x26\x4c"
    "\x3c\x28\x52\xbb\x91\xc3\x00\xcb\x88\xd0\x65\x8b\x1b\x53\x2e\xa3"
    "\x71\x64\x48\x97\xa2\x0d\xf9\x4e\x38\x19\xef\x46\xa9\xde\xac\xd8"
    "\xa8\xfa\x76\x3f\xe3\x9c\x34\x3f\xf9\xdc\xbb\xc7\xc7\x0b\x4f\x1d"
    "\x8a\x51\xe0\x4b\xcd\xb4\x59\x31\xc8\x9f\x7e\xc9\xd9\x78\x73\x64"
    "\xea\xc5\xac\x83\x34\xd3\xeb\xc3\xc5\x81\xa0\xff\xfa\x13\x63\xeb"
    "\x17\x0d\xdd\x51\xb7\xf0\xda\x49\xd3\x16\x55\x26\x29\xd4\x68\x9e"
    "\x2b\x16\xbe\x58\x7d\x47\xa1\xfc\x8f\xf8\xb8\xd1\x7a\xd0\x31\xce"
    "\x45\xcb\x3a\x8f\x95\x16\x04\x28\xaf\xd7\xfb\xca\xbb\x4b\x40\x7e"
)

# ===========================================================================
# Bitwise Utilities
# ===========================================================================

def rotl64(x, r):
    return (x << r) | (x >> (64 - r))

def swap32(x):
    # Fix: Ensure strict 32-bit width by casting before operation.
    # r_uint32(x) truncates upper bits if x was 64-bit.
    # r_uint(...) promotes to native word for efficient shifting/masking.
    val = r_uint(r_uint32(x))
    res = ((val << 24) & r_uint(0xff000000)) | \
          ((val <<  8) & r_uint(0x00ff0000)) | \
          ((val >>  8) & r_uint(0x0000ff00)) | \
          ((val >> 24) & r_uint(0x000000ff))
    return r_uint32(res)

def swap64(x):
    return ((x << 56) & r_uint64(0xff00000000000000)) | \
           ((x << 40) & r_uint64(0x00ff000000000000)) | \
           ((x << 24) & r_uint64(0x0000ff0000000000)) | \
           ((x <<  8) & r_uint64(0x000000ff00000000)) | \
           ((x >>  8) & r_uint64(0x00000000ff000000)) | \
           ((x >> 24) & r_uint64(0x0000000000ff0000)) | \
           ((x >> 40) & r_uint64(0x000000000000ff00)) | \
           ((x >> 56) & r_uint64(0x00000000000000ff))

def read_le32(ptr):
    p = rffi.cast(rffi.UCHARP, ptr)
    # Cast components to native word (r_uint) before shifting/ORing
    v0 = r_uint(p[0])
    v1 = r_uint(p[1])
    v2 = r_uint(p[2])
    v3 = r_uint(p[3])
    return r_uint32(v0 | (v1 << 8) | (v2 << 16) | (v3 << 24))

def read_le64(ptr):
    p = rffi.cast(rffi.UCHARP, ptr)
    return (r_uint64(p[0])) | \
           (r_uint64(p[1]) << 8) | \
           (r_uint64(p[2]) << 16) | \
           (r_uint64(p[3]) << 24) | \
           (r_uint64(p[4]) << 32) | \
           (r_uint64(p[5]) << 40) | \
           (r_uint64(p[6]) << 48) | \
           (r_uint64(p[7]) << 56)

def read_secret_le64(offset):
    b0 = r_uint64(ord(XXH3_kSecret[offset + 0]))
    b1 = r_uint64(ord(XXH3_kSecret[offset + 1]))
    b2 = r_uint64(ord(XXH3_kSecret[offset + 2]))
    b3 = r_uint64(ord(XXH3_kSecret[offset + 3]))
    b4 = r_uint64(ord(XXH3_kSecret[offset + 4]))
    b5 = r_uint64(ord(XXH3_kSecret[offset + 5]))
    b6 = r_uint64(ord(XXH3_kSecret[offset + 6]))
    b7 = r_uint64(ord(XXH3_kSecret[offset + 7]))
    return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24) | \
           (b4 << 32) | (b5 << 40) | (b6 << 48) | (b7 << 56)

def read_secret_le32(offset):
    # Cast to native word (r_uint) before shifting/ORing
    b0 = r_uint(ord(XXH3_kSecret[offset + 0]))
    b1 = r_uint(ord(XXH3_kSecret[offset + 1]))
    b2 = r_uint(ord(XXH3_kSecret[offset + 2]))
    b3 = r_uint(ord(XXH3_kSecret[offset + 3]))
    return r_uint32(b0 | (b1 << 8) | (b2 << 16) | (b3 << 24))

# ===========================================================================
# Math Utilities (128-bit Emulation)
# ===========================================================================

def mult64to128(lhs, rhs):
    mask = r_uint64(0xFFFFFFFF)
    lo_lo = (lhs & mask) * (rhs & mask)
    hi_lo = (lhs >> 32)  * (rhs & mask)
    lo_hi = (lhs & mask) * (rhs >> 32)
    hi_hi = (lhs >> 32)  * (rhs >> 32)

    cross = (lo_lo >> 32) + (hi_lo & mask) + lo_hi
    upper = (hi_lo >> 32) + (cross >> 32)  + hi_hi
    lower = (cross << 32) | (lo_lo & mask)
    
    return lower, upper

def mul128_fold64(lhs, rhs):
    lower, upper = mult64to128(lhs, rhs)
    return lower ^ upper

# ===========================================================================
# Core Mixing Functions
# ===========================================================================

def xxh3_avalanche(h64):
    h64 = h64 ^ (h64 >> 37)
    h64 *= r_uint64(0x165667919E3779F9) 
    h64 = h64 ^ (h64 >> 32)
    return h64

def xxh3_rrmxmx(h64, length):
    h64 ^= rotl64(h64, 49) ^ rotl64(h64, 24)
    h64 *= r_uint64(0x9FB21C651E98DF25) 
    h64 ^= (h64 >> 35) + length
    h64 *= r_uint64(0x9FB21C651E98DF25) 
    return h64 ^ (h64 >> 28)

def xxh3_mix16b(input_ptr, secret_off, seed):
    input_lo = read_le64(input_ptr)
    input_hi = read_le64(rffi.ptradd(input_ptr, 8))
    
    return mul128_fold64(
        input_lo ^ (read_secret_le64(secret_off) + seed),
        input_hi ^ (read_secret_le64(secret_off + 8) - seed)
    )

# ===========================================================================
# Length-Specific Implementations
# ===========================================================================

def xxh3_len_0to16(input_ptr, length, seed):
    if length > 8:
        bitflip1 = (read_secret_le64(24) ^ read_secret_le64(32)) + seed
        bitflip2 = (read_secret_le64(40) ^ read_secret_le64(48)) - seed
        input_lo = read_le64(input_ptr) ^ bitflip1
        input_hi = read_le64(rffi.ptradd(input_ptr, length - 8)) ^ bitflip2
        
        lower, upper = mult64to128(input_lo, input_hi)
        acc = r_uint64(length) + swap64(input_lo) + input_hi + (lower ^ upper)
        return xxh3_avalanche(acc)
        
    if length >= 4:
        seed_low = r_uint(seed & 0xFFFFFFFF)
        # Fix: swap32 returns r_uint32, cast to r_uint64 is safe
        swapped_seed = r_uint64(swap32(r_uint32(seed_low)))
        seed_final = seed ^ (swapped_seed << 32)
        
        input1 = read_le32(input_ptr)
        input2 = read_le32(rffi.ptradd(input_ptr, length - 4))
        bitflip = (read_secret_le64(8) ^ read_secret_le64(16)) - seed_final
        input64 = r_uint64(input2) + (r_uint64(input1) << 32)
        return xxh3_rrmxmx(input64 ^ bitflip, r_uint64(length))
    
    if length > 0:
        c1 = r_uint(ord(rffi.cast(rffi.CCHARP, input_ptr)[0]))
        c2 = r_uint(ord(rffi.cast(rffi.CCHARP, input_ptr)[length >> 1]))
        c3 = r_uint(ord(rffi.cast(rffi.CCHARP, input_ptr)[length - 1]))
        combined = (c1 << 16) | (c2 << 24) | c3 | (r_uint(length) << 8)
        
        # FIX: Promote r_uint32 results of read_secret_le32 to r_uint BEFORE XORing
        sec0 = r_uint(read_secret_le32(0))
        sec4 = r_uint(read_secret_le32(4))
        bitflip = (sec0 ^ sec4) + r_uint(seed)
        return xxh3_avalanche(r_uint64(combined ^ bitflip))
        
    return xxh3_avalanche(seed ^ (read_secret_le64(56) ^ read_secret_le64(64)))

def xxh3_len_17to128(input_ptr, length, seed):
    acc = r_uint64(length) * XXH_PRIME64_1
    if length > 32:
        if length > 64:
            if length > 96:
                acc += xxh3_mix16b(rffi.ptradd(input_ptr, 48), 96, seed)
                acc += xxh3_mix16b(rffi.ptradd(input_ptr, length - 64), 112, seed)
            acc += xxh3_mix16b(rffi.ptradd(input_ptr, 32), 64, seed)
            acc += xxh3_mix16b(rffi.ptradd(input_ptr, length - 48), 80, seed)
        acc += xxh3_mix16b(rffi.ptradd(input_ptr, 16), 32, seed)
        acc += xxh3_mix16b(rffi.ptradd(input_ptr, length - 32), 48, seed)
    acc += xxh3_mix16b(input_ptr, 0, seed)
    acc += xxh3_mix16b(rffi.ptradd(input_ptr, length - 16), 16, seed)
    return xxh3_avalanche(acc)

def xxh3_len_129to240(input_ptr, length, seed):
    acc = r_uint64(length) * XXH_PRIME64_1
    i = 0
    while i < 8:
        acc += xxh3_mix16b(rffi.ptradd(input_ptr, 16 * i), 16 * i, seed)
        i += 1
    acc = xxh3_avalanche(acc)
    acc_end = xxh3_mix16b(rffi.ptradd(input_ptr, length - 16), 136 - 17, seed)
    nb_rounds = length // 16
    while i < nb_rounds:
        acc_end += xxh3_mix16b(rffi.ptradd(input_ptr, 16 * i), 16 * (i - 8) + 3, seed)
        i += 1
    return xxh3_avalanche(acc + acc_end)

# ===========================================================================
# Long Hash Implementation (Stripe Processing)
# ===========================================================================

def mult32to64_add64(lhs, rhs, acc):
    # Standard promotion to 64-bit before multiplication
    return (r_uint64(lhs) * r_uint64(rhs)) + acc

@unroll_safe
def xxh3_accumulate_512_scalar(acc, input_ptr, secret_off):
    for i in range(8):
        data_val = read_le64(rffi.ptradd(input_ptr, i * 8))
        data_key = data_val ^ read_secret_le64(secret_off + i * 8)
        acc[i ^ 1] += data_val
        acc[i] = mult32to64_add64(r_uint32(data_key & 0xFFFFFFFF), 
                                  r_uint32(data_key >> 32), 
                                  acc[i])

@unroll_safe
def xxh3_scramble_acc_scalar(acc, secret_off):
    for i in range(8):
        key64 = read_secret_le64(secret_off + i * 8)
        acc64 = acc[i]
        acc64 = acc64 ^ (acc64 >> 47)
        acc64 ^= key64
        acc64 *= XXH_PRIME64_1
        acc[i] = acc64

def xxh3_hash_long(input_ptr, length, seed):
    acc = [
        XXH_PRIME32_3, XXH_PRIME64_1, XXH_PRIME64_2, XXH_PRIME32_3,
        XXH_PRIME64_4, XXH_PRIME32_2, XXH_PRIME64_5, XXH_PRIME64_1
    ]
    secret_len = len(XXH3_kSecret)
    nb_stripes_per_block = (secret_len - XXH_STRIPE_LEN) // 8
    block_len = XXH_STRIPE_LEN * nb_stripes_per_block
    nb_blocks = (length - 1) // block_len
    
    n = 0
    while n < nb_blocks:
        block_ptr = rffi.ptradd(input_ptr, n * block_len)
        for s in range(nb_stripes_per_block):
            xxh3_accumulate_512_scalar(acc, rffi.ptradd(block_ptr, s * XXH_STRIPE_LEN), s * 8)
        xxh3_scramble_acc_scalar(acc, secret_len - XXH_STRIPE_LEN)
        n += 1
        
    nb_stripes = ((length - 1) - (block_len * nb_blocks)) // XXH_STRIPE_LEN
    block_ptr = rffi.ptradd(input_ptr, nb_blocks * block_len)
    for s in range(nb_stripes):
        xxh3_accumulate_512_scalar(acc, rffi.ptradd(block_ptr, s * XXH_STRIPE_LEN), s * 8)
                                   
    p_last = rffi.ptradd(input_ptr, length - XXH_STRIPE_LEN)
    xxh3_accumulate_512_scalar(acc, p_last, secret_len - XXH_STRIPE_LEN - 7)
    
    result64 = r_uint64(length) * XXH_PRIME64_1
    secret_merge_start = 11
    for i in range(4):
        acc0 = acc[2*i]
        acc1 = acc[2*i + 1]
        s_off = secret_merge_start + (16 * i)
        result64 += mul128_fold64(acc0 ^ read_secret_le64(s_off), acc1 ^ read_secret_le64(s_off + 8))
    return xxh3_avalanche(result64)

# ===========================================================================
# Public API
# ===========================================================================

def compute_checksum(data_ptr, length):
    length = r_uint64(length)
    seed = r_uint64(0)
    if length <= 16: return xxh3_len_0to16(data_ptr, length, seed)
    if length <= 128: return xxh3_len_17to128(data_ptr, length, seed)
    if length <= 240: return xxh3_len_129to240(data_ptr, length, seed)
    return xxh3_hash_long(data_ptr, length, seed)

def verify_checksum(data_ptr, length, expected):
    return compute_checksum(data_ptr, length) == expected

def compute_checksum_bytes(data_str):
    length = len(data_str)
    buf = lltype.malloc(rffi.CCHARP.TO, length, flavor='raw')
    try:
        for i in range(length): buf[i] = data_str[i]
        return compute_checksum(buf, length)
    finally: lltype.free(buf, flavor='raw')
