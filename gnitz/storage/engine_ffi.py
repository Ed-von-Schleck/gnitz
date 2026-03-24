# gnitz/storage/engine_ffi.py
#
# RPython FFI bindings for libgnitz_engine (Rust staticlib).
# Provides xor8 filter, bloom filter, WAL encode/decode, and xxh3 checksum.

import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo

_lib_dir = os.environ.get("GNITZ_ENGINE_LIB", "")
_lib_path = os.path.join(_lib_dir, "libgnitz_engine.a") if _lib_dir else ""

eci = ExternalCompilationInfo(
    pre_include_bits=[
        "#include <stdint.h>",
        # xor8
        "void *gnitz_xor8_build(const uint64_t *pk_lo, const uint64_t *pk_hi, uint32_t count);",
        "int gnitz_xor8_may_contain(const void *handle, uint64_t key_lo, uint64_t key_hi);",
        "int64_t gnitz_xor8_serialize(const void *handle, uint8_t *out, int64_t cap);",
        "void *gnitz_xor8_deserialize(const uint8_t *buf, int64_t len);",
        "void gnitz_xor8_free(void *handle);",
        # bloom
        "void *gnitz_bloom_create(uint32_t expected_n);",
        "void gnitz_bloom_add(void *handle, uint64_t key_lo, uint64_t key_hi);",
        "int gnitz_bloom_may_contain(const void *handle, uint64_t key_lo, uint64_t key_hi);",
        "void gnitz_bloom_reset(void *handle);",
        "void gnitz_bloom_free(void *handle);",
        # xxh3 checksum
        "uint64_t gnitz_xxh3_checksum(const uint8_t *data, int64_t len);",
        # wal encode/decode
        "int64_t gnitz_wal_encode("
        "  uint8_t *out_buf, int64_t out_offset, int64_t out_capacity,"
        "  uint64_t lsn, uint32_t table_id, uint32_t entry_count,"
        "  void **region_ptrs, uint32_t *region_sizes,"
        "  uint32_t num_regions, uint64_t blob_size);",
        "int32_t gnitz_wal_validate_and_parse("
        "  uint8_t *block, int64_t block_len,"
        "  uint64_t *out_lsn, uint32_t *out_tid, uint32_t *out_count,"
        "  uint32_t *out_num_regions, uint64_t *out_blob_size,"
        "  uint32_t *out_region_offsets, uint32_t *out_region_sizes,"
        "  uint32_t max_regions);",
    ],
    link_files=[_lib_path] if _lib_path else [],
)

# ---------------------------------------------------------------------------
# XOR8
# ---------------------------------------------------------------------------

_xor8_build = rffi.llexternal(
    "gnitz_xor8_build",
    [rffi.ULONGLONGP, rffi.ULONGLONGP, rffi.UINT],
    rffi.VOIDP,
    compilation_info=eci,
)

_xor8_may_contain = rffi.llexternal(
    "gnitz_xor8_may_contain",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_xor8_serialize = rffi.llexternal(
    "gnitz_xor8_serialize",
    [rffi.VOIDP, rffi.CCHARP, rffi.LONGLONG],
    rffi.LONGLONG,
    compilation_info=eci,
)

_xor8_deserialize = rffi.llexternal(
    "gnitz_xor8_deserialize",
    [rffi.CCHARP, rffi.LONGLONG],
    rffi.VOIDP,
    compilation_info=eci,
)

_xor8_free = rffi.llexternal(
    "gnitz_xor8_free",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Bloom
# ---------------------------------------------------------------------------

_bloom_create = rffi.llexternal(
    "gnitz_bloom_create",
    [rffi.UINT],
    rffi.VOIDP,
    compilation_info=eci,
)

_bloom_add = rffi.llexternal(
    "gnitz_bloom_add",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    lltype.Void,
    compilation_info=eci,
)

_bloom_may_contain = rffi.llexternal(
    "gnitz_bloom_may_contain",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_bloom_reset = rffi.llexternal(
    "gnitz_bloom_reset",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

_bloom_free = rffi.llexternal(
    "gnitz_bloom_free",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# XXH3 checksum
# ---------------------------------------------------------------------------

_xxh3_checksum = rffi.llexternal(
    "gnitz_xxh3_checksum",
    [rffi.CCHARP, rffi.LONGLONG],
    rffi.ULONGLONG,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# WAL encode/decode
# ---------------------------------------------------------------------------

_wal_encode = rffi.llexternal(
    "gnitz_wal_encode",
    [rffi.CCHARP, rffi.LONGLONG, rffi.LONGLONG,
     rffi.ULONGLONG, rffi.UINT, rffi.UINT,
     rffi.VOIDPP, rffi.UINTP,
     rffi.UINT, rffi.ULONGLONG],
    rffi.LONGLONG,
    compilation_info=eci,
)

_wal_validate_and_parse = rffi.llexternal(
    "gnitz_wal_validate_and_parse",
    [rffi.CCHARP, rffi.LONGLONG,
     rffi.ULONGLONGP, rffi.UINTP, rffi.UINTP,
     rffi.UINTP, rffi.ULONGLONGP,
     rffi.UINTP, rffi.UINTP,
     rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)
