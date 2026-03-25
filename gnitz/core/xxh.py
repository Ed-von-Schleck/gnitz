from rpython.rtyper.lltypesystem import rffi
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib import jit

from gnitz.storage import engine_ffi


@jit.elidable
def hash_u128_inline(lo, hi, seed_lo=r_uint64(0), seed_hi=r_uint64(0)):
    """Zero-allocation 128-bit hasher. All work happens on the Rust/C stack."""
    res = engine_ffi._xxh3_hash_u128(
        rffi.cast(rffi.ULONGLONG, lo),
        rffi.cast(rffi.ULONGLONG, hi),
        rffi.cast(rffi.ULONGLONG, seed_lo),
        rffi.cast(rffi.ULONGLONG, seed_hi),
    )
    return r_uint64(res)


def compute_checksum(data_ptr, length):
    """Computes a 64-bit XXH3 checksum from a pointer and length."""
    res = engine_ffi._xxh3_checksum(
        rffi.cast(rffi.CCHARP, data_ptr), rffi.cast(rffi.LONGLONG, length)
    )
    return r_uint64(res)


def verify_checksum(data_ptr, length, expected):
    """Compares the checksum of a buffer against an expected r_uint64."""
    return compute_checksum(data_ptr, length) == expected


def compute_checksum_bytes(data_str):
    """Computes a 64-bit XXH3 checksum for an RPython string."""
    length = len(data_str)
    with rffi.scoped_str2charp(data_str) as ptr:
        return compute_checksum(ptr, length)
