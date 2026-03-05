# gnitz/storage/xor8.py

import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib import rposix, rposix_stat, jit

from gnitz.core import xxh
from gnitz.storage import mmap_posix

MAX_CONSTRUCTION_ATTEMPTS = 64


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
        self._scratch = lltype.malloc(rffi.CCHARP.TO, 16, flavor="raw")

    def _get_seed(self):
        return (r_uint128(self.seed_hi) << 64) | r_uint128(self.seed_lo)

    def _hash_key(self, key):
        seed = self._get_seed()
        mixed = r_uint64(key ^ seed)
        rffi.cast(rffi.ULONGLONGP, self._scratch)[0] = rffi.cast(
            rffi.ULONGLONG, mixed
        )
        return xxh.compute_checksum(self._scratch, 8)

    def may_contain(self, key):
        h = self._hash_key(key)
        f = intmask(h >> 56) & 0xFF
        seg = self.segment_length
        h0 = intmask(h) % seg
        h1 = intmask(h >> 16) % seg + seg
        h2 = intmask(h >> 32) % seg + 2 * seg
        fp = self.fingerprints
        got = (ord(fp[h0]) ^ ord(fp[h1]) ^ ord(fp[h2])) & 0xFF
        return f == got

    def free(self):
        if self.owned and self.fingerprints:
            lltype.free(self.fingerprints, flavor="raw")
            self.fingerprints = lltype.nullptr(rffi.CCHARP.TO)
        if self._scratch:
            lltype.free(self._scratch, flavor="raw")
            self._scratch = lltype.nullptr(rffi.CCHARP.TO)


def _hash_with_seed(key_val, seed):
    """Hash a key value XOR'd with seed. Returns r_uint64."""
    mixed = r_uint64(key_val ^ seed)
    # We need a scratch buffer for this - use a temporary one
    scratch = lltype.malloc(rffi.CCHARP.TO, 8, flavor="raw")
    try:
        rffi.cast(rffi.ULONGLONGP, scratch)[0] = rffi.cast(rffi.ULONGLONG, mixed)
        return xxh.compute_checksum(scratch, 8)
    finally:
        lltype.free(scratch, flavor="raw")


def _read_key_from_buf(keys_ptr, i, key_size):
    """Read a key from the PK buffer as r_uint128."""
    offset = i * key_size
    ptr = rffi.ptradd(keys_ptr, offset)
    if key_size == 16:
        lo = r_uint64(rffi.cast(rffi.ULONGLONGP, ptr)[0])
        hi = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0])
        return (r_uint128(hi) << 64) | r_uint128(lo)
    else:
        return r_uint128(rffi.cast(rffi.ULONGLONGP, ptr)[0])


def build_xor8(keys_ptr, num_keys, key_size):
    """
    Constructs an Xor8Filter from a raw PK buffer using peeling-based construction.
    Returns Xor8Filter or None on failure.
    """
    if num_keys == 0:
        return None

    segment_length = intmask(1 + (num_keys * 123 + 99) // 100 // 3)
    if segment_length < 4:
        segment_length = 4
    total_size = 3 * segment_length

    fingerprints = lltype.malloc(rffi.CCHARP.TO, total_size, flavor="raw")
    # counts array (int per slot)
    counts = lltype.malloc(rffi.CCHARP.TO, total_size * 4, flavor="raw")
    # keys_at array (uint64 per slot - XOR of hashes)
    keys_at = lltype.malloc(rffi.CCHARP.TO, total_size * 8, flavor="raw")
    # stack: stores (hash: u64, solo_slot: i32) = 12 bytes per entry
    stack = lltype.malloc(rffi.CCHARP.TO, num_keys * 12, flavor="raw")

    counts_p = rffi.cast(rffi.INTP, counts)
    keys_at_p = rffi.cast(rffi.ULONGLONGP, keys_at)

    seed = r_uint128(1)
    built = False

    attempt = 0
    while attempt < MAX_CONSTRUCTION_ATTEMPTS:
        # Zero counts and keys_at
        i = 0
        while i < total_size:
            counts_p[i] = rffi.cast(rffi.INT, 0)
            keys_at_p[i] = rffi.cast(rffi.ULONGLONG, 0)
            i += 1

        # Populate
        i = 0
        while i < num_keys:
            key_val = _read_key_from_buf(keys_ptr, i, key_size)
            h = _hash_with_seed(key_val, seed)
            seg = segment_length
            h0 = intmask(h) % seg
            h1 = intmask(h >> 16) % seg + seg
            h2 = intmask(h >> 32) % seg + 2 * seg

            counts_p[h0] = rffi.cast(rffi.INT, intmask(counts_p[h0]) + 1)
            keys_at_p[h0] = rffi.cast(
                rffi.ULONGLONG, r_uint64(keys_at_p[h0]) ^ h
            )
            counts_p[h1] = rffi.cast(rffi.INT, intmask(counts_p[h1]) + 1)
            keys_at_p[h1] = rffi.cast(
                rffi.ULONGLONG, r_uint64(keys_at_p[h1]) ^ h
            )
            counts_p[h2] = rffi.cast(rffi.INT, intmask(counts_p[h2]) + 1)
            keys_at_p[h2] = rffi.cast(
                rffi.ULONGLONG, r_uint64(keys_at_p[h2]) ^ h
            )
            i += 1

        # Peel: find slots with count == 1
        queue_buf = lltype.malloc(rffi.CCHARP.TO, total_size * 4, flavor="raw")
        queue_p = rffi.cast(rffi.INTP, queue_buf)
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

            seg = segment_length
            h0 = intmask(h) % seg
            h1 = intmask(h >> 16) % seg + seg
            h2 = intmask(h >> 32) % seg + 2 * seg

            # Remove hash from all three positions
            slots_0 = h0
            slots_1 = h1
            slots_2 = h2

            counts_p[slots_0] = rffi.cast(rffi.INT, intmask(counts_p[slots_0]) - 1)
            keys_at_p[slots_0] = rffi.cast(
                rffi.ULONGLONG, r_uint64(keys_at_p[slots_0]) ^ h
            )
            if intmask(counts_p[slots_0]) == 1:
                queue_p[queue_tail] = rffi.cast(rffi.INT, slots_0)
                queue_tail += 1

            counts_p[slots_1] = rffi.cast(rffi.INT, intmask(counts_p[slots_1]) - 1)
            keys_at_p[slots_1] = rffi.cast(
                rffi.ULONGLONG, r_uint64(keys_at_p[slots_1]) ^ h
            )
            if intmask(counts_p[slots_1]) == 1:
                queue_p[queue_tail] = rffi.cast(rffi.INT, slots_1)
                queue_tail += 1

            counts_p[slots_2] = rffi.cast(rffi.INT, intmask(counts_p[slots_2]) - 1)
            keys_at_p[slots_2] = rffi.cast(
                rffi.ULONGLONG, r_uint64(keys_at_p[slots_2]) ^ h
            )
            if intmask(counts_p[slots_2]) == 1:
                queue_p[queue_tail] = rffi.cast(rffi.INT, slots_2)
                queue_tail += 1

        lltype.free(queue_buf, flavor="raw")

        if stack_size == num_keys:
            built = True
            break

        seed = seed + r_uint128(1)
        attempt += 1

    if not built:
        lltype.free(fingerprints, flavor="raw")
        lltype.free(counts, flavor="raw")
        lltype.free(keys_at, flavor="raw")
        lltype.free(stack, flavor="raw")
        return None

    # Assign fingerprints in reverse stack order
    i = 0
    while i < total_size:
        fingerprints[i] = "\x00"
        i += 1

    stack_hash_p = rffi.cast(rffi.ULONGLONGP, stack)
    stack_slot_p = rffi.cast(rffi.INTP, rffi.ptradd(stack, num_keys * 8))

    i = stack_size - 1
    while i >= 0:
        h = r_uint64(stack_hash_p[i])
        solo_slot = intmask(stack_slot_p[i])

        f = intmask(h >> 56) & 0xFF
        seg = segment_length
        h0 = intmask(h) % seg
        h1 = intmask(h >> 16) % seg + seg
        h2 = intmask(h >> 32) % seg + 2 * seg

        xored = ord(fingerprints[h0]) ^ ord(fingerprints[h1]) ^ ord(fingerprints[h2])
        fingerprints[solo_slot] = chr((f ^ xored) & 0xFF)
        i -= 1

    lltype.free(counts, flavor="raw")
    lltype.free(keys_at, flavor="raw")
    lltype.free(stack, flavor="raw")

    return Xor8Filter(fingerprints, segment_length, seed, total_size, owned=True)


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

        fp_data = rposix.read(fd, total_size)
        if len(fp_data) < total_size:
            return None

        fingerprints = lltype.malloc(rffi.CCHARP.TO, total_size, flavor="raw")
        i = 0
        while i < total_size:
            fingerprints[i] = fp_data[i]
            i += 1

        return Xor8Filter(
            fingerprints,
            segment_length,
            r_uint128(seed_val),
            total_size,
            owned=True,
        )
    finally:
        rposix.close(fd)
