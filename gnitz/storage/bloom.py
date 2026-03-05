# gnitz/storage/bloom.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib import jit

from gnitz.core import xxh


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
        self._scratch = lltype.malloc(rffi.CCHARP.TO, 16, flavor="raw")

    def _hash_key(self, key):
        lo = r_uint64(key)
        hi = r_uint64(key >> 64)
        rffi.cast(rffi.ULONGLONGP, self._scratch)[0] = rffi.cast(
            rffi.ULONGLONG, lo ^ hi
        )
        return xxh.compute_checksum(self._scratch, 8)

    @jit.unroll_safe
    def add(self, key):
        h = self._hash_key(key)
        h1 = intmask(h) & 0x7FFFFFFF
        h2 = intmask(h >> 32) | 1
        num_bits = self.num_bits
        bits = self.bits
        i = 0
        while i < NUM_PROBES:
            pos = (h1 + i * h2) % num_bits
            byte_idx = pos >> 3
            bit_mask = 1 << (pos & 7)
            bits[byte_idx] = chr(ord(bits[byte_idx]) | bit_mask)
            i += 1

    @jit.unroll_safe
    def may_contain(self, key):
        h = self._hash_key(key)
        h1 = intmask(h) & 0x7FFFFFFF
        h2 = intmask(h >> 32) | 1
        num_bits = self.num_bits
        bits = self.bits
        i = 0
        while i < NUM_PROBES:
            pos = (h1 + i * h2) % num_bits
            byte_idx = pos >> 3
            bit_mask = 1 << (pos & 7)
            if not (ord(bits[byte_idx]) & bit_mask):
                return False
            i += 1
        return True

    def free(self):
        if self.bits:
            lltype.free(self.bits, flavor="raw")
            self.bits = lltype.nullptr(rffi.CCHARP.TO)
        if self._scratch:
            lltype.free(self._scratch, flavor="raw")
            self._scratch = lltype.nullptr(rffi.CCHARP.TO)
