# gnitz/storage/bloom.py
#
# Bloom filter backed by Rust (libgnitz_engine).

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import intmask

from gnitz.storage import engine_ffi


class BloomFilter(object):
    """
    Standard Bloom filter using double-hashing (Kirschner & Mitzenmacher 2006).
    Tuned for ~1% FPR at 10 bits/key with 7 hash probes.
    """

    _immutable_fields_ = ["_handle"]

    def __init__(self, expected_n):
        self._handle = engine_ffi._bloom_create(rffi.cast(rffi.UINT, expected_n))

    def add(self, key_lo, key_hi):
        engine_ffi._bloom_add(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )

    def may_contain(self, key_lo, key_hi):
        res = engine_ffi._bloom_may_contain(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )
        return intmask(res) != 0

    def reset(self):
        """Zero all bits without reallocating."""
        engine_ffi._bloom_reset(self._handle)

    def free(self):
        if self._handle:
            engine_ffi._bloom_free(self._handle)
            self._handle = lltype.nullptr(rffi.VOIDP.TO)
