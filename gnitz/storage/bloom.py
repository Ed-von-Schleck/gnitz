# gnitz/storage/bloom.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib import jit

from gnitz.core import xxh
from gnitz.storage.buffer import c_memset


BITS_PER_KEY = 10
NUM_PROBES = 7


class BloomFilter(object):
    """
    Standard Bloom filter using double-hashing (Kirschner & Mitzenmacher 2006).
    Tuned for ~1% FPR at 10 bits/key with 7 hash probes.
    """

    _immutable_fields_ = ["num_bytes", "num_bits", "k"]

    def __init__(self, expected_n):
        if expected_n < 1:
            expected_n = 1
        m = expected_n * BITS_PER_KEY
        num_bytes = (m + 7) >> 3
        if num_bytes < 8:
            num_bytes = 8
        self.num_bytes = num_bytes
        self.num_bits = num_bytes * 8
        self.k = NUM_PROBES
        self.bits = lltype.malloc(rffi.CCHARP.TO, num_bytes, flavor="raw")
        i = 0
        while i < num_bytes:
            self.bits[i] = "\x00"
            i += 1

    @jit.elidable
    def _hash_key(self, key_lo, key_hi):
        return xxh.hash_u128_inline(key_lo, key_hi)

    @jit.unroll_safe
    def add(self, key_lo, key_hi):
        h_val = r_uint64(self._hash_key(key_lo, key_hi))
        h1 = h_val
        h2 = (h_val >> 32) | r_uint64(1)
        num_bits_u = r_uint64(self.num_bits)
        bits = self.bits

        i = 0
        while i < NUM_PROBES:
            # Fully unsigned math avoids the negative modulo trap in RPython
            # and prevents the 256MB capacity limit of the 31-bit signed mask.
            pos = intmask((h1 + r_uint64(i) * h2) % num_bits_u)
            byte_idx = pos >> 3
            bit_mask = 1 << (pos & 7)
            bits[byte_idx] = chr(ord(bits[byte_idx]) | bit_mask)
            i += 1

    @jit.unroll_safe
    def may_contain(self, key_lo, key_hi):
        h_val = r_uint64(self._hash_key(key_lo, key_hi))
        h1 = h_val
        h2 = (h_val >> 32) | r_uint64(1)
        num_bits_u = r_uint64(self.num_bits)
        bits = self.bits
        
        i = 0
        while i < NUM_PROBES:
            pos = intmask((h1 + r_uint64(i) * h2) % num_bits_u)
            byte_idx = pos >> 3
            bit_mask = 1 << (pos & 7)
            if not (ord(bits[byte_idx]) & bit_mask):
                return False
            i += 1
            
        return True

    def reset(self):
        """Zero all bits without reallocating."""
        c_memset(
            rffi.cast(rffi.VOIDP, self.bits),
            rffi.cast(rffi.INT, 0),
            rffi.cast(rffi.SIZE_T, self.num_bytes),
        )

    def free(self):
        if self.bits:
            lltype.free(self.bits, flavor="raw")
            self.bits = lltype.nullptr(rffi.CCHARP.TO)
