# gnitz/storage/xor8.py

import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib import rposix, rposix_stat, jit, objectmodel

from gnitz.core import xxh
from gnitz.storage import mmap_posix

MAX_CONSTRUCTION_ATTEMPTS = 64


@objectmodel.always_inline
def rotl64(n, c):
    """64-bit bitwise left rotation. Inlined at the C level."""
    n = r_uint64(n)
    return (n << c) | (n >> (64 - c))


@jit.elidable
def fastrange32(hash32, size):
    """
    Lemire's fastrange. Maps a 32-bit hash evenly to [0, size).
    Marked elidable to allow the JIT to constant-fold the calculation.
    """
    return intmask((r_uint64(hash32) * r_uint64(size)) >> 32)


@jit.elidable
def _get_h0_h1_h2(h, segment_length):
    """
    Derives 3 independent block indices from a single 64-bit hash.
    Marked elidable so the JIT can remove redundant calls in a trace.
    """
    h_val = r_uint64(h)
    seg_u = r_uint64(segment_length)
    mask = r_uint64(0xFFFFFFFF)
    
    h0 = fastrange32(h_val & mask, seg_u)
    h1 = fastrange32(rotl64(h_val, 21) & mask, seg_u) + segment_length
    h2 = fastrange32(rotl64(h_val, 42) & mask, seg_u) + 2 * segment_length
    
    return h0, h1, h2


class Xor8Filter(object):
    """
    8-bit fingerprint XOR filter. Query-only structure.
    Construction via build_xor8().
    """

    _immutable_fields_ = ["segment_length", "seed_lo", "seed_hi", "total_size", "owned"]

    def __init__(self, fingerprints, segment_length, seed, total_size, owned):
        self.fingerprints = fingerprints
        self.segment_length = segment_length
        self.seed_lo = r_uint64(seed)
        self.seed_hi = r_uint64(seed >> 64)
        self.total_size = total_size
        self.owned = owned

    def _get_seed(self):
        return (r_uint128(self.seed_hi) << 64) | r_uint128(self.seed_lo)

    @jit.elidable
    def _hash_key(self, key):
        lo = r_uint64(key)
        hi = r_uint64(key >> 64)
        return xxh.hash_u128_inline(lo, hi, self.seed_lo, self.seed_hi)

    def may_contain(self, key):
        h = self._hash_key(key)
        f = intmask(r_uint64(h) >> 56) & 0xFF

        h0, h1, h2 = _get_h0_h1_h2(h, self.segment_length)

        fp = self.fingerprints
        got = (ord(fp[h0]) ^ ord(fp[h1]) ^ ord(fp[h2])) & 0xFF
        return f == got

    def free(self):
        if self.owned and self.fingerprints:
            lltype.free(self.fingerprints, flavor="raw")
            self.fingerprints = lltype.nullptr(rffi.CCHARP.TO)


def build_xor8(pk_lo_ptr, pk_hi_ptr, num_keys):
    if num_keys == 0:
        return None

    capacity = intmask(1 + (num_keys * 123 + 99) // 100 + 32)
    segment_length = intmask((capacity + 2) // 3)
    if segment_length < 4:
        segment_length = 4
    total_size = 3 * segment_length

    fingerprints = lltype.malloc(rffi.CCHARP.TO, total_size, flavor="raw")
    counts = lltype.malloc(rffi.CCHARP.TO, total_size * 4, flavor="raw")
    keys_at = lltype.malloc(rffi.CCHARP.TO, total_size * 8, flavor="raw")
    stack = lltype.malloc(rffi.CCHARP.TO, num_keys * 12, flavor="raw")
    queue_buf = lltype.malloc(rffi.CCHARP.TO, total_size * 4, flavor="raw")

    counts_p = rffi.cast(rffi.INTP, counts)
    keys_at_p = rffi.cast(rffi.ULONGLONGP, keys_at)
    queue_p = rffi.cast(rffi.INTP, queue_buf)
    lo_p = rffi.cast(rffi.ULONGLONGP, pk_lo_ptr)
    hi_p = rffi.cast(rffi.ULONGLONGP, pk_hi_ptr)

    seed = r_uint128(1)
    built = False

    try:
        attempt = 0
        while attempt < MAX_CONSTRUCTION_ATTEMPTS:
            i = 0
            while i < total_size:
                counts_p[i] = rffi.cast(rffi.INT, 0)
                keys_at_p[i] = rffi.cast(rffi.ULONGLONG, 0)
                i += 1

            seed_lo = r_uint64(seed)
            seed_hi = r_uint64(seed >> 64)

            i = 0
            while i < num_keys:
                h = xxh.hash_u128_inline(r_uint64(lo_p[i]), r_uint64(hi_p[i]), seed_lo, seed_hi)
                h0, h1, h2 = _get_h0_h1_h2(h, segment_length)

                counts_p[h0] = rffi.cast(rffi.INT, intmask(counts_p[h0]) + 1)
                keys_at_p[h0] = rffi.cast(rffi.ULONGLONG, r_uint64(keys_at_p[h0]) ^ h)
                counts_p[h1] = rffi.cast(rffi.INT, intmask(counts_p[h1]) + 1)
                keys_at_p[h1] = rffi.cast(rffi.ULONGLONG, r_uint64(keys_at_p[h1]) ^ h)
                counts_p[h2] = rffi.cast(rffi.INT, intmask(counts_p[h2]) + 1)
                keys_at_p[h2] = rffi.cast(rffi.ULONGLONG, r_uint64(keys_at_p[h2]) ^ h)
                i += 1

            queue_head = 0
            queue_tail = 0
            i = 0
            while i < total_size:
                if intmask(counts_p[i]) == 1:
                    queue_p[queue_tail] = rffi.cast(rffi.INT, i)
                    queue_tail += 1
                i += 1

            stack_size = 0
            stack_hash_p = rffi.cast(rffi.ULONGLONGP, stack)
            stack_slot_p = rffi.cast(rffi.INTP, rffi.ptradd(stack, num_keys * 8))

            while queue_head < queue_tail:
                s = intmask(queue_p[queue_head])
                queue_head += 1
                if intmask(counts_p[s]) != 1:
                    continue
                h = r_uint64(keys_at_p[s])
                stack_hash_p[stack_size] = rffi.cast(rffi.ULONGLONG, h)
                stack_slot_p[stack_size] = rffi.cast(rffi.INT, s)
                stack_size += 1

                h0, h1, h2 = _get_h0_h1_h2(h, segment_length)
                for slot in [h0, h1, h2]:
                    counts_p[slot] = rffi.cast(rffi.INT, intmask(counts_p[slot]) - 1)
                    keys_at_p[slot] = rffi.cast(rffi.ULONGLONG, r_uint64(keys_at_p[slot]) ^ h)
                    if intmask(counts_p[slot]) == 1:
                        queue_p[queue_tail] = rffi.cast(rffi.INT, slot)
                        queue_tail += 1

            if stack_size == num_keys:
                built = True
                break
            seed = seed + r_uint128(1)
            attempt += 1

        if not built: return None

        i = 0
        while i < total_size:
            fingerprints[i] = "\x00"
            i += 1

        i = stack_size - 1
        while i >= 0:
            h = r_uint64(stack_hash_p[i])
            solo_slot = intmask(stack_slot_p[i])
            f = intmask(h >> 56) & 0xFF
            h0, h1, h2 = _get_h0_h1_h2(h, segment_length)
            xored = ord(fingerprints[h0]) ^ ord(fingerprints[h1]) ^ ord(fingerprints[h2])
            fingerprints[solo_slot] = chr((f ^ xored) & 0xFF)
            i -= 1

        return Xor8Filter(fingerprints, segment_length, seed, total_size, owned=True)
    finally:
        lltype.free(counts, flavor="raw")
        lltype.free(keys_at, flavor="raw")
        lltype.free(stack, flavor="raw")
        lltype.free(queue_buf, flavor="raw")
        if not built:
            lltype.free(fingerprints, flavor="raw")


def save_xor8(xor_filter, filepath):
    """Write XOR8 filter to a sidecar file."""
    fd = rposix.open(filepath, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        header = lltype.malloc(rffi.CCHARP.TO, 16, flavor="raw")
        try:
            seed = xor_filter._get_seed()
            rffi.cast(rffi.ULONGLONGP, header)[0] = rffi.cast(
                rffi.ULONGLONG, r_uint64(seed)
            )
            rffi.cast(rffi.UINTP, rffi.ptradd(header, 8))[0] = rffi.cast(
                rffi.UINT, xor_filter.segment_length
            )
            rffi.cast(rffi.UINTP, rffi.ptradd(header, 12))[0] = rffi.cast(
                rffi.UINT, xor_filter.total_size
            )
            mmap_posix.write_c(fd, header, rffi.cast(rffi.SIZE_T, 16))
        finally:
            lltype.free(header, flavor="raw")

        mmap_posix.write_c(
            fd,
            xor_filter.fingerprints,
            rffi.cast(rffi.SIZE_T, xor_filter.total_size),
        )
        mmap_posix.fsync_c(fd)
    finally:
        rposix.close(fd)


def load_xor8(filepath):
    """Load XOR8 filter from a sidecar file. Returns None if file doesn't exist."""
    try:
        st = rposix_stat.stat(filepath)
    except OSError:
        return None

    file_size = intmask(st.st_size)
    if file_size < 16:
        return None

    fd = rposix.open(filepath, os.O_RDONLY, 0)
    try:
        header_data = rposix.read(fd, 16)
        if len(header_data) < 16:
            return None

        header = lltype.malloc(rffi.CCHARP.TO, 16, flavor="raw")
        try:
            i = 0
            while i < 16:
                header[i] = header_data[i]
                i += 1

            seed_val = r_uint64(rffi.cast(rffi.ULONGLONGP, header)[0])
            segment_length = intmask(
                rffi.cast(rffi.UINTP, rffi.ptradd(header, 8))[0]
            )
            total_size = intmask(
                rffi.cast(rffi.UINTP, rffi.ptradd(header, 12))[0]
            )
        finally:
            lltype.free(header, flavor="raw")

        if total_size <= 0 or segment_length <= 0:
            return None
        if file_size < 16 + total_size:
            return None

        fingerprints = lltype.malloc(rffi.CCHARP.TO, total_size, flavor="raw")
        bytes_read = mmap_posix.read_into_ptr(fd, fingerprints, total_size)
        if bytes_read < total_size:
            lltype.free(fingerprints, flavor="raw")
            return None

        return Xor8Filter(
            fingerprints,
            segment_length,
            r_uint128(seed_val),
            total_size,
            owned=True,
        )
    finally:
        rposix.close(fd)
