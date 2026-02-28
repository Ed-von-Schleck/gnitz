import os
from rpython.rtyper.lltypesystem import rffi
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from rpython.rlib.rarithmetic import r_uint64

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
"""

eci = ExternalCompilationInfo(
    include_dirs=[CURRENT_DIR],
    # pre_include_bits goes into the global header seen by all modules.
    # We provide only the prototype for our linkable wrapper.
    pre_include_bits=[
        "#include <stddef.h>",
        "unsigned long long gnitz_xxh3_64(const void* input, size_t length);",
    ],
    separate_module_sources=[implementation_code],
)

_xxh3_64 = rffi.llexternal(
    "gnitz_xxh3_64",
    [rffi.VOIDP, rffi.SIZE_T],
    rffi.ULONGLONG,
    compilation_info=eci,
)


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
