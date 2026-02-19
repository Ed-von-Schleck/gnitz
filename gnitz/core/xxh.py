import os
import sys
from rpython.rtyper.lltypesystem import rffi
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from rpython.rlib.rarithmetic import r_uint, r_uint64

# Dynamically find the path to the header relative to this file
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# 1. Define the Implementation Code
# This block contains the actual C function bodies.
implementation_code = """
#include <stddef.h>
#define XXH_NAMESPACE GNZ_STABLE_
#define XXH_STATIC_LINKING_ONLY
#define XXH_IMPLEMENTATION
#include "xxhash.h"

#if defined(_WIN32)
#define GNZ_EXPORT __declspec(dllexport)
#else
#define GNZ_EXPORT __attribute__((visibility("default")))
#endif

/* Our stable wrapper function */
GNZ_EXPORT
unsigned long long gnitz_xxh3_64(const void* input, size_t length) {
    return GNZ_STABLE_XXH3_64bits(input, length);
}
"""

# 2. Environment Detection
# We check if we are currently inside the RPython translation pipeline.
is_translating = False
# Standard check: if the translator modules are loaded
for mod_name in sys.modules:
    if mod_name.startswith('rpython.translator') or mod_name.startswith('pypy.translator'):
        is_translating = True
        break
# Supplemental check for the translation entry point
if any('translate' in arg for arg in sys.argv):
    is_translating = True

# 3. Dynamic ExternalCompilationInfo Configuration
if is_translating:
    # --- TRANSLATION MODE ---
    # We must isolate the implementation to a single object file.
    # We only put the PROTOTYPE in the global header (pre_include_bits).
    eci = ExternalCompilationInfo(
        includes=['xxhash.h'],
        include_dirs=[CURRENT_DIR],
        pre_include_bits=[
            '#include <stddef.h>',
            '#define XXH_NAMESPACE GNZ_STABLE_',
            '#define XXH_STATIC_LINKING_ONLY',
            'unsigned long long gnitz_xxh3_64(const void* input, size_t length);'
        ],
        separate_module_sources=[implementation_code]
    )
else:
    # --- UNIT TEST MODE (ll2ctypes / Python 2.7) ---
    # We put the implementation directly into pre_include_bits.
    # ll2ctypes compiles exactly one C file, so no multiple definition issues.
    # This ensures the symbol is exported to the ephemeral .so library.
    eci = ExternalCompilationInfo(
        includes=['xxhash.h'],
        include_dirs=[CURRENT_DIR],
        pre_include_bits=[implementation_code]
    )

# 4. Define the FFI binding
_xxh3_64 = rffi.llexternal(
    'gnitz_xxh3_64',
    [rffi.VOIDP, rffi.SIZE_T],
    rffi.ULONGLONG,
    compilation_info=eci
)

def compute_checksum(data_ptr, length):
    """
    Computes a 64-bit XXH3 checksum from a pointer and length.
    Safe for both translated binaries and Python 2.7 unit tests.
    """
    # rffi.SIZE_T handles the correct pointer-width for the length field
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
