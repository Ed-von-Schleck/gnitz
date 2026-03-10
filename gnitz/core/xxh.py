import os
from rpython.rtyper.lltypesystem import rffi
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib import jit

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# We define a single C source file that includes the xxHash implementation.
# We provide a global (non-static) symbol 'gnitz_xxh3_64' because the 
# native XXH3_64bits is usually a static inline function in the header,
# which cannot be linked across object files by the RPython toolchain.
implementation_code = """
#include <stddef.h>
#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#include "xxhash.h"

unsigned long long gnitz_xxh3_64(const void* input, size_t length) {
    return XXH3_64bits(input, length);
}

unsigned long long gnitz_xxh3_128_inline(uint64_t lo, uint64_t hi, uint64_t seed_lo, uint64_t seed_hi) {
    uint64_t buf[2] = {lo ^ seed_lo, hi ^ seed_hi};
    return XXH3_64bits(buf, 16);
}
"""

eci = ExternalCompilationInfo(
    include_dirs=[CURRENT_DIR],
    # pre_include_bits goes into the global header seen by all modules.
    # We provide only the prototype for our linkable wrapper.
    pre_include_bits=[
        "#include <stddef.h>",
        "#include <stdint.h>",
        "unsigned long long gnitz_xxh3_64(const void* input, size_t length);",
        "unsigned long long gnitz_xxh3_128_inline(uint64_t lo, uint64_t hi, uint64_t seed_lo, uint64_t seed_hi);",
    ],
    separate_module_sources=[implementation_code],
)

_xxh3_64 = rffi.llexternal(
    "gnitz_xxh3_64",
    [rffi.VOIDP, rffi.SIZE_T],
    rffi.ULONGLONG,
    compilation_info=eci,
)


_xxh3_128_inline = rffi.llexternal(
    "gnitz_xxh3_128_inline",
    [rffi.ULONGLONG, rffi.ULONGLONG, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.ULONGLONG,
    compilation_info=eci,
    _nowrapper=True,
)


@jit.elidable
def hash_u128_inline(lo, hi, seed_lo=r_uint64(0), seed_hi=r_uint64(0)):
    """Zero-allocation 128-bit hasher. All work happens on the C stack."""
    res = _xxh3_128_inline(
        rffi.cast(rffi.ULONGLONG, lo),
        rffi.cast(rffi.ULONGLONG, hi),
        rffi.cast(rffi.ULONGLONG, seed_lo),
        rffi.cast(rffi.ULONGLONG, seed_hi),
    )
    return r_uint64(res)


def compute_checksum(data_ptr, length):
    """Computes a 64-bit XXH3 checksum from a pointer and length."""
    res = _xxh3_64(rffi.cast(rffi.VOIDP, data_ptr), rffi.cast(rffi.SIZE_T, length))
    return r_uint64(res)


def verify_checksum(data_ptr, length, expected):
    """Compares the checksum of a buffer against an expected r_uint64."""
    return compute_checksum(data_ptr, length) == expected


def compute_checksum_bytes(data_str):
    """Computes a 64-bit XXH3 checksum for an RPython string."""
    length = len(data_str)
    with rffi.scoped_str2charp(data_str) as ptr:
        return compute_checksum(ptr, length)
