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
        # xxh3 checksum + hash
        "uint64_t gnitz_xxh3_checksum(const uint8_t *data, int64_t len);",
        "uint64_t gnitz_xxh3_hash_u128(uint64_t lo, uint64_t hi, uint64_t seed_lo, uint64_t seed_hi);",
        # shard reader
        "void *gnitz_shard_open(const char *path, const void *schema_desc, int32_t validate_checksums);",
        "void *gnitz_shard_open_from_buffers("
        "  uint8_t *pk_lo, uint8_t *pk_hi,"
        "  uint8_t *weight, uint8_t *null_bm,"
        "  void **col_ptrs, uint64_t *col_sizes,"
        "  uint32_t num_payload_cols,"
        "  uint8_t *blob_ptr, uint64_t blob_len,"
        "  uint32_t count, void *schema_desc);",
        "void gnitz_shard_close(void *handle);",
        "int32_t gnitz_shard_row_count(const void *handle);",
        "int32_t gnitz_shard_has_xor8(const void *handle);",
        "int32_t gnitz_shard_xor8_may_contain(const void *handle, uint64_t key_lo, uint64_t key_hi);",
        "int32_t gnitz_shard_find_row(const void *handle, uint64_t key_lo, uint64_t key_hi);",
        "int32_t gnitz_shard_lower_bound(const void *handle, uint64_t key_lo, uint64_t key_hi);",
        "uint64_t gnitz_shard_get_pk_lo(const void *handle, int32_t row);",
        "uint64_t gnitz_shard_get_pk_hi(const void *handle, int32_t row);",
        "int64_t gnitz_shard_get_weight(const void *handle, int32_t row);",
        "uint64_t gnitz_shard_get_null_word(const void *handle, int32_t row);",
        "const uint8_t *gnitz_shard_col_ptr(const void *handle, int32_t row, int32_t col_idx, int32_t col_size);",
        "const uint8_t *gnitz_shard_blob_ptr(const void *handle);",
        "int64_t gnitz_shard_blob_len(const void *handle);",
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
        # manifest
        "int64_t gnitz_manifest_serialize("
        "  uint8_t *out_buf, int64_t out_capacity,"
        "  const uint8_t *entries, uint32_t count,"
        "  uint64_t global_max_lsn);",
        "int32_t gnitz_manifest_parse("
        "  const uint8_t *buf, int64_t buf_len,"
        "  uint8_t *out_entries, uint32_t max_entries,"
        "  uint64_t *out_global_max_lsn);",
        # compaction
        "int32_t gnitz_compact_shards("
        "  char **input_files, uint32_t num_inputs,"
        "  char *output_file,"
        "  void *schema_desc, uint32_t table_id);",
        "int32_t gnitz_merge_and_route("
        "  char **input_files, uint32_t num_inputs,"
        "  char *output_dir,"
        "  uint64_t *guard_keys, uint32_t num_guards,"
        "  void *schema_desc,"
        "  uint32_t table_id, uint32_t level_num, uint64_t lsn_tag,"
        "  void *out_results, uint32_t max_results);",
        # shard file write
        "int32_t gnitz_write_shard("
        "  int32_t dirfd, const char *filename,"
        "  uint32_t table_id, uint32_t row_count,"
        "  void **region_ptrs, uint64_t *region_sizes,"
        "  uint32_t num_regions,"
        "  uint64_t *pk_lo_ptr, uint64_t *pk_hi_ptr,"
        "  int32_t durable);",
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

_xxh3_hash_u128 = rffi.llexternal(
    "gnitz_xxh3_hash_u128",
    [rffi.ULONGLONG, rffi.ULONGLONG, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.ULONGLONG,
    compilation_info=eci,
    _nowrapper=True,
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

# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

_manifest_serialize = rffi.llexternal(
    "gnitz_manifest_serialize",
    [rffi.CCHARP, rffi.LONGLONG,
     rffi.CCHARP, rffi.UINT,
     rffi.ULONGLONG],
    rffi.LONGLONG,
    compilation_info=eci,
)

_manifest_parse = rffi.llexternal(
    "gnitz_manifest_parse",
    [rffi.CCHARP, rffi.LONGLONG,
     rffi.CCHARP, rffi.UINT,
     rffi.ULONGLONGP],
    rffi.INT,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Schema packing (shared between shard reader and compaction)
# ---------------------------------------------------------------------------

def pack_schema(schema):
    """Pack a TableSchema into a flat C buffer matching Rust SchemaDescriptor."""
    buf = lltype.malloc(rffi.CCHARP.TO, SCHEMA_DESC_SIZE, flavor="raw")
    i = 0
    while i < SCHEMA_DESC_SIZE:
        buf[i] = '\x00'
        i += 1
    num_cols = len(schema.columns)
    rffi.cast(rffi.UINTP, buf)[0] = rffi.cast(rffi.UINT, num_cols)
    rffi.cast(rffi.UINTP, rffi.ptradd(buf, 4))[0] = rffi.cast(rffi.UINT, schema.pk_index)
    ci = 0
    while ci < num_cols:
        col = schema.columns[ci]
        base = 8 + ci * 4
        buf[base] = chr(col.field_type.code)
        buf[base + 1] = chr(col.field_type.size)
        buf[base + 2] = chr(1 if col.is_nullable else 0)
        ci += 1
    return buf


# ---------------------------------------------------------------------------
# Shard reader
# ---------------------------------------------------------------------------

_shard_open = rffi.llexternal(
    "gnitz_shard_open",
    [rffi.CCHARP, rffi.VOIDP, rffi.INT],
    rffi.VOIDP,
    compilation_info=eci,
)

_shard_open_from_buffers = rffi.llexternal(
    "gnitz_shard_open_from_buffers",
    [rffi.CCHARP, rffi.CCHARP,
     rffi.CCHARP, rffi.CCHARP,
     rffi.VOIDPP, rffi.ULONGLONGP,
     rffi.UINT,
     rffi.CCHARP, rffi.ULONGLONG,
     rffi.UINT, rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_shard_close = rffi.llexternal(
    "gnitz_shard_close",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

_shard_row_count = rffi.llexternal(
    "gnitz_shard_row_count",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_shard_has_xor8 = rffi.llexternal(
    "gnitz_shard_has_xor8",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_shard_xor8_may_contain = rffi.llexternal(
    "gnitz_shard_xor8_may_contain",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_shard_find_row = rffi.llexternal(
    "gnitz_shard_find_row",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_shard_lower_bound = rffi.llexternal(
    "gnitz_shard_lower_bound",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_shard_get_pk_lo = rffi.llexternal(
    "gnitz_shard_get_pk_lo",
    [rffi.VOIDP, rffi.INT],
    rffi.ULONGLONG,
    compilation_info=eci,
)

_shard_get_pk_hi = rffi.llexternal(
    "gnitz_shard_get_pk_hi",
    [rffi.VOIDP, rffi.INT],
    rffi.ULONGLONG,
    compilation_info=eci,
)

_shard_get_weight = rffi.llexternal(
    "gnitz_shard_get_weight",
    [rffi.VOIDP, rffi.INT],
    rffi.LONGLONG,
    compilation_info=eci,
)

_shard_get_null_word = rffi.llexternal(
    "gnitz_shard_get_null_word",
    [rffi.VOIDP, rffi.INT],
    rffi.ULONGLONG,
    compilation_info=eci,
)

_shard_col_ptr = rffi.llexternal(
    "gnitz_shard_col_ptr",
    [rffi.VOIDP, rffi.INT, rffi.INT, rffi.INT],
    rffi.CCHARP,
    compilation_info=eci,
)

_shard_blob_ptr = rffi.llexternal(
    "gnitz_shard_blob_ptr",
    [rffi.VOIDP],
    rffi.CCHARP,
    compilation_info=eci,
)

_shard_blob_len = rffi.llexternal(
    "gnitz_shard_blob_len",
    [rffi.VOIDP],
    rffi.LONGLONG,
    compilation_info=eci,
)


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------

# SchemaDescriptor layout: 4 (num_columns) + 4 (pk_index) + 64*4 (columns) = 264 bytes
SCHEMA_DESC_SIZE = 264

_compact_shards = rffi.llexternal(
    "gnitz_compact_shards",
    [rffi.CCHARPP, rffi.UINT, rffi.CCHARP, rffi.VOIDP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

# GuardResult: 8 (lo) + 8 (hi) + 256 (filename) = 272 bytes
GUARD_RESULT_SIZE = 272

_merge_and_route = rffi.llexternal(
    "gnitz_merge_and_route",
    [rffi.CCHARPP, rffi.UINT, rffi.CCHARP,
     rffi.ULONGLONGP, rffi.UINT,
     rffi.VOIDP,
     rffi.UINT, rffi.UINT, rffi.ULONGLONG,
     rffi.VOIDP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Shard file write
# ---------------------------------------------------------------------------

_write_shard = rffi.llexternal(
    "gnitz_write_shard",
    [rffi.INT, rffi.CCHARP,
     rffi.UINT, rffi.UINT,
     rffi.VOIDPP, rffi.ULONGLONGP,
     rffi.UINT,
     rffi.ULONGLONGP, rffi.ULONGLONGP,
     rffi.INT],
    rffi.INT,
    compilation_info=eci,
)
