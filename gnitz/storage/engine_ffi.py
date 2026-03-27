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
        # WAL writer lifecycle
        "void *gnitz_wal_writer_open(const char *path, int32_t *out_error);",
        "int32_t gnitz_wal_writer_append("
        "  void *handle, uint64_t lsn, uint32_t table_id, uint32_t count,"
        "  void **region_ptrs, uint32_t *region_sizes,"
        "  uint32_t num_regions, uint64_t blob_size);",
        "int32_t gnitz_wal_writer_truncate(void *handle);",
        "void gnitz_wal_writer_close(void *handle);",
        # WAL reader lifecycle
        "void *gnitz_wal_reader_open("
        "  const char *path, char **out_base_ptr, int64_t *out_file_size);",
        "int32_t gnitz_wal_reader_next("
        "  void *handle,"
        "  uint64_t *out_lsn, uint32_t *out_tid, uint32_t *out_count,"
        "  uint32_t *out_num_regions, uint64_t *out_blob_size,"
        "  uint32_t *out_offsets, uint32_t *out_sizes,"
        "  uint32_t max_regions);",
        "void gnitz_wal_reader_close(void *handle);",
        # shard file writing
        "int32_t gnitz_write_shard("
        "  int32_t dirfd, const char *basename,"
        "  uint32_t table_id, uint32_t row_count,"
        "  void **region_ptrs, uint32_t *region_sizes,"
        "  uint32_t num_regions, int32_t durable);",
        # manifest file I/O
        "int32_t gnitz_manifest_read_file("
        "  const char *path,"
        "  uint8_t *out_entries, uint32_t max_entries,"
        "  uint64_t *out_global_max_lsn);",
        "int32_t gnitz_manifest_write_file("
        "  const char *path,"
        "  const uint8_t *entries_buf, uint32_t count,"
        "  uint64_t global_max_lsn);",
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
        # batch merge (memtable consolidation)
        "int32_t gnitz_merge_batches("
        "  void **in_region_ptrs, uint32_t *in_region_sizes,"
        "  uint32_t *in_row_counts, uint32_t num_batches,"
        "  uint32_t regions_per_batch,"
        "  void *schema_desc,"
        "  void **out_region_ptrs, uint32_t *out_region_sizes,"
        "  uint32_t *out_row_count);",
        # single-batch sort + consolidation
        "int32_t gnitz_consolidate_batch("
        "  void **in_region_ptrs, uint32_t *in_region_sizes,"
        "  uint32_t row_count, uint32_t regions_per_batch,"
        "  void *schema_desc,"
        "  void **out_region_ptrs, uint32_t *out_region_sizes,"
        "  uint32_t *out_row_count);",
        # single-batch sort (no consolidation)
        "int32_t gnitz_sort_batch("
        "  void **in_region_ptrs, uint32_t *in_region_sizes,"
        "  uint32_t row_count, uint32_t regions_per_batch,"
        "  void *schema_desc,"
        "  void **out_region_ptrs, uint32_t *out_region_sizes,"
        "  uint32_t *out_row_count);",
        # scatter-copy (indexed row subset)
        "int32_t gnitz_scatter_copy("
        "  void **in_region_ptrs, uint32_t *in_region_sizes,"
        "  uint32_t in_row_count, uint32_t regions_per_batch,"
        "  uint32_t *indices, uint32_t num_indices,"
        "  int64_t *weights,"
        "  void *schema_desc,"
        "  void **out_region_ptrs, uint32_t *out_region_sizes,"
        "  uint32_t *out_row_count);",
        # shard index (opaque FLSM lifecycle handle)
        "void *gnitz_shard_index_create(uint32_t table_id, char *output_dir, void *schema_desc);",
        "void gnitz_shard_index_close(void *handle);",
        "int32_t gnitz_shard_index_load_manifest(void *handle, char *manifest_path);",
        "int32_t gnitz_shard_index_add_shard(void *handle, char *shard_path, uint64_t min_lsn, uint64_t max_lsn);",
        "int32_t gnitz_shard_index_compact(void *handle);",
        "int32_t gnitz_shard_index_publish_manifest(void *handle, char *manifest_path);",
        "int32_t gnitz_shard_index_try_cleanup(void *handle);",
        "int32_t gnitz_shard_index_needs_compaction(void *handle);",
        "uint64_t gnitz_shard_index_max_lsn(void *handle);",
        "int32_t gnitz_shard_index_all_shard_ptrs(void *handle, void **out_ptrs, uint32_t max_ptrs);",
        "int32_t gnitz_shard_index_find_pk("
        "  void *handle, uint64_t key_lo, uint64_t key_hi,"
        "  void **out_shard_ptrs, int32_t *out_row_indices, uint32_t max_results);",
        # read cursor (opaque N-way merge cursor)
        "void *gnitz_read_cursor_create("
        "  void **batch_region_ptrs, uint32_t *batch_region_sizes,"
        "  uint32_t *batch_row_counts, uint32_t num_batches,"
        "  uint32_t regions_per_batch,"
        "  void **shard_handles, uint32_t num_shards,"
        "  void *schema_desc);",
        "void gnitz_read_cursor_seek("
        "  void *handle, uint64_t key_lo, uint64_t key_hi,"
        "  int32_t *out_valid, uint64_t *out_key_lo, uint64_t *out_key_hi,"
        "  int64_t *out_weight, uint64_t *out_null_word);",
        "void gnitz_read_cursor_next("
        "  void *handle,"
        "  int32_t *out_valid, uint64_t *out_key_lo, uint64_t *out_key_hi,"
        "  int64_t *out_weight, uint64_t *out_null_word);",
        "const uint8_t *gnitz_read_cursor_col_ptr("
        "  const void *handle, int32_t col_idx, int32_t col_size);",
        "const uint8_t *gnitz_read_cursor_blob_ptr(const void *handle);",
        "void gnitz_read_cursor_close(void *handle);",
        "void *gnitz_read_cursor_create_from_snapshots("
        "  void **snap_handles, uint32_t num_snapshots,"
        "  void **shard_handles, uint32_t num_shards,"
        "  void *schema_desc);",
        # memtable (opaque handle)
        "void *gnitz_memtable_create(void *schema_desc, uint64_t max_bytes);",
        "void gnitz_memtable_close(void *handle);",
        "int32_t gnitz_memtable_upsert_batch("
        "  void *handle, void **in_ptrs, uint32_t *in_sizes,"
        "  uint32_t row_count, uint32_t regions_per_batch);",
        "void gnitz_memtable_bloom_add(void *handle, uint64_t key_lo, uint64_t key_hi);",
        "int32_t gnitz_memtable_may_contain_pk(void *handle, uint64_t key_lo, uint64_t key_hi);",
        "int32_t gnitz_memtable_should_flush(void *handle);",
        "int32_t gnitz_memtable_is_empty(void *handle);",
        "int32_t gnitz_memtable_total_row_count(void *handle);",
        "void *gnitz_memtable_get_snapshot(void *handle);",
        "uint32_t gnitz_memtable_snapshot_count(void *snap);",
        "void gnitz_memtable_snapshot_free(void *snap);",
        "int64_t gnitz_memtable_lookup_pk("
        "  void *handle, uint64_t key_lo, uint64_t key_hi,"
        "  int32_t *out_found);",
        "uint64_t gnitz_memtable_found_null_word(void *handle);",
        "const uint8_t *gnitz_memtable_found_col_ptr(void *handle, int32_t payload_col, int32_t col_size);",
        "const uint8_t *gnitz_memtable_found_blob_ptr(void *handle);",
        "int64_t gnitz_memtable_find_weight_for_row("
        "  void *handle, uint64_t key_lo, uint64_t key_hi,"
        "  void **ref_ptrs, uint32_t *ref_sizes,"
        "  uint32_t ref_count, uint32_t regions_per_batch);",
        "int32_t gnitz_memtable_flush("
        "  void *handle, int32_t dirfd, const char *basename,"
        "  uint32_t table_id, int32_t durable);",
        "void gnitz_memtable_reset(void *handle);",
        # table (unified opaque handle)
        "void *gnitz_table_create(const char *dir, const char *name,"
        "  const void *schema_desc, uint32_t table_id,"
        "  uint64_t arena_size, int32_t durable);",
        "void gnitz_table_close(void *handle);",
        "int32_t gnitz_table_ingest_batch("
        "  void *handle, void **ptrs, uint32_t *sizes,"
        "  uint32_t count, uint32_t rpb);",
        "int32_t gnitz_table_ingest_batch_memonly("
        "  void *handle, void **ptrs, uint32_t *sizes,"
        "  uint32_t count, uint32_t rpb);",
        "void *gnitz_table_create_cursor(void *handle);",
        "int32_t gnitz_table_has_pk(void *handle, uint64_t key_lo, uint64_t key_hi);",
        "int64_t gnitz_table_retract_pk(void *handle, uint64_t key_lo, uint64_t key_hi,"
        "  int32_t *out_found);",
        "int64_t gnitz_table_get_weight(void *handle, uint64_t key_lo, uint64_t key_hi,"
        "  void **ref_ptrs, uint32_t *ref_sizes, uint32_t ref_count, uint32_t rpb);",
        "int32_t gnitz_table_flush(void *handle);",
        "int32_t gnitz_table_compact_if_needed(void *handle);",
        "void gnitz_table_set_has_wal(void *handle, int32_t flag);",
        "uint64_t gnitz_table_current_lsn(void *handle);",
        "void gnitz_table_bloom_add(void *handle, uint64_t key_lo, uint64_t key_hi);",
        "int32_t gnitz_table_memtable_upsert_batch("
        "  void *handle, void **ptrs, uint32_t *sizes,"
        "  uint32_t count, uint32_t rpb);",
        "int32_t gnitz_table_memtable_should_flush(void *handle);",
        "int32_t gnitz_table_memtable_is_empty(void *handle);",
        "uint64_t gnitz_table_found_null_word(void *handle);",
        "const uint8_t *gnitz_table_found_col_ptr(void *handle, int32_t payload_col, int32_t col_size);",
        "const uint8_t *gnitz_table_found_blob_ptr(void *handle);",
        "void *gnitz_table_get_snapshot(void *handle);",
        "int32_t gnitz_table_all_shard_ptrs(void *handle, void **out_ptrs, uint32_t max_ptrs);",
        "void *gnitz_table_create_child(void *handle, const char *name, const void *schema_desc);",
        # partitioned table (hash-routed N-way handle)
        "void *gnitz_ptable_create(const char *dir, const char *name,"
        "  const void *schema_desc, uint32_t table_id, uint32_t num_partitions,"
        "  int32_t durable, uint32_t part_start, uint32_t part_end, uint64_t arena_size);",
        "void gnitz_ptable_close(void *handle);",
        "int32_t gnitz_ptable_ingest_batch(void *handle, void **ptrs, uint32_t *sizes,"
        "  uint32_t count, uint32_t rpb);",
        "int32_t gnitz_ptable_ingest_batch_memonly(void *handle, void **ptrs, uint32_t *sizes,"
        "  uint32_t count, uint32_t rpb);",
        "void *gnitz_ptable_create_cursor(void *handle);",
        "int32_t gnitz_ptable_has_pk(void *handle, uint64_t key_lo, uint64_t key_hi);",
        "int64_t gnitz_ptable_retract_pk(void *handle, uint64_t key_lo, uint64_t key_hi,"
        "  int32_t *out_found);",
        "int64_t gnitz_ptable_get_weight(void *handle, uint64_t key_lo, uint64_t key_hi,"
        "  void **ref_ptrs, uint32_t *ref_sizes, uint32_t ref_count, uint32_t rpb);",
        "int32_t gnitz_ptable_flush(void *handle);",
        "int32_t gnitz_ptable_compact_if_needed(void *handle);",
        "void gnitz_ptable_set_has_wal(void *handle, int32_t flag);",
        "uint64_t gnitz_ptable_current_lsn(void *handle);",
        "void gnitz_ptable_close_partitions_outside(void *handle, uint32_t start, uint32_t end);",
        "void gnitz_ptable_close_all_partitions(void *handle);",
        "uint64_t gnitz_ptable_found_null_word(void *handle);",
        "const uint8_t *gnitz_ptable_found_col_ptr(void *handle, int32_t payload_col, int32_t col_size);",
        "const uint8_t *gnitz_ptable_found_blob_ptr(void *handle);",
        "void *gnitz_ptable_create_child(void *handle, const char *name, const void *schema_desc);",
        "void gnitz_ptable_bloom_add(void *handle, uint64_t key_lo, uint64_t key_hi);",
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
# WAL writer lifecycle
# ---------------------------------------------------------------------------

_wal_writer_open = rffi.llexternal(
    "gnitz_wal_writer_open",
    [rffi.CCHARP, rffi.INTP],
    rffi.VOIDP,
    compilation_info=eci,
)

_wal_writer_append = rffi.llexternal(
    "gnitz_wal_writer_append",
    [rffi.VOIDP,
     rffi.ULONGLONG, rffi.UINT, rffi.UINT,
     rffi.VOIDPP, rffi.UINTP,
     rffi.UINT, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_wal_writer_truncate = rffi.llexternal(
    "gnitz_wal_writer_truncate",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_wal_writer_close = rffi.llexternal(
    "gnitz_wal_writer_close",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# WAL reader lifecycle
# ---------------------------------------------------------------------------

_wal_reader_open = rffi.llexternal(
    "gnitz_wal_reader_open",
    [rffi.CCHARP, rffi.CCHARPP, rffi.LONGLONGP],
    rffi.VOIDP,
    compilation_info=eci,
)

_wal_reader_next = rffi.llexternal(
    "gnitz_wal_reader_next",
    [rffi.VOIDP,
     rffi.ULONGLONGP, rffi.UINTP, rffi.UINTP,
     rffi.UINTP, rffi.ULONGLONGP,
     rffi.UINTP, rffi.UINTP,
     rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_wal_reader_close = rffi.llexternal(
    "gnitz_wal_reader_close",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Manifest file I/O
# ---------------------------------------------------------------------------

_manifest_read_file = rffi.llexternal(
    "gnitz_manifest_read_file",
    [rffi.CCHARP,
     rffi.CCHARP, rffi.UINT,
     rffi.ULONGLONGP],
    rffi.INT,
    compilation_info=eci,
)

_manifest_write_file = rffi.llexternal(
    "gnitz_manifest_write_file",
    [rffi.CCHARP,
     rffi.CCHARP, rffi.UINT,
     rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Shard file writing
# ---------------------------------------------------------------------------

_write_shard = rffi.llexternal(
    "gnitz_write_shard",
    [rffi.INT, rffi.CCHARP,
     rffi.UINT, rffi.UINT,
     rffi.VOIDPP, rffi.UINTP,
     rffi.UINT, rffi.INT],
    rffi.INT,
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
# Batch merge (memtable consolidation)
# ---------------------------------------------------------------------------

_merge_batches = rffi.llexternal(
    "gnitz_merge_batches",
    [rffi.VOIDPP, rffi.UINTP,
     rffi.UINTP, rffi.UINT,
     rffi.UINT,
     rffi.VOIDP,
     rffi.VOIDPP, rffi.UINTP,
     rffi.UINTP],
    rffi.INT,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Single-batch sort + consolidation
# ---------------------------------------------------------------------------

_consolidate_batch = rffi.llexternal(
    "gnitz_consolidate_batch",
    [rffi.VOIDPP, rffi.UINTP,
     rffi.UINT, rffi.UINT,
     rffi.VOIDP,
     rffi.VOIDPP, rffi.UINTP,
     rffi.UINTP],
    rffi.INT,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Single-batch sort (no consolidation)
# ---------------------------------------------------------------------------

_sort_batch = rffi.llexternal(
    "gnitz_sort_batch",
    [rffi.VOIDPP, rffi.UINTP,
     rffi.UINT, rffi.UINT,
     rffi.VOIDP,
     rffi.VOIDPP, rffi.UINTP,
     rffi.UINTP],
    rffi.INT,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Scatter-copy (indexed row subset)
# ---------------------------------------------------------------------------

_scatter_copy = rffi.llexternal(
    "gnitz_scatter_copy",
    [rffi.VOIDPP, rffi.UINTP,
     rffi.UINT, rffi.UINT,
     rffi.UINTP, rffi.UINT,
     rffi.LONGLONGP,
     rffi.VOIDP,
     rffi.VOIDPP, rffi.UINTP,
     rffi.UINTP],
    rffi.INT,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Shard Index (opaque FLSM lifecycle handle)
# ---------------------------------------------------------------------------

_shard_index_create = rffi.llexternal(
    "gnitz_shard_index_create",
    [rffi.UINT, rffi.CCHARP, rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_shard_index_close = rffi.llexternal(
    "gnitz_shard_index_close",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

_shard_index_load_manifest = rffi.llexternal(
    "gnitz_shard_index_load_manifest",
    [rffi.VOIDP, rffi.CCHARP],
    rffi.INT,
    compilation_info=eci,
)

_shard_index_add_shard = rffi.llexternal(
    "gnitz_shard_index_add_shard",
    [rffi.VOIDP, rffi.CCHARP, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_shard_index_compact = rffi.llexternal(
    "gnitz_shard_index_compact",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_shard_index_publish_manifest = rffi.llexternal(
    "gnitz_shard_index_publish_manifest",
    [rffi.VOIDP, rffi.CCHARP],
    rffi.INT,
    compilation_info=eci,
)

_shard_index_try_cleanup = rffi.llexternal(
    "gnitz_shard_index_try_cleanup",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_shard_index_needs_compaction = rffi.llexternal(
    "gnitz_shard_index_needs_compaction",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_shard_index_max_lsn = rffi.llexternal(
    "gnitz_shard_index_max_lsn",
    [rffi.VOIDP],
    rffi.ULONGLONG,
    compilation_info=eci,
)

_shard_index_all_shard_ptrs = rffi.llexternal(
    "gnitz_shard_index_all_shard_ptrs",
    [rffi.VOIDP, rffi.VOIDPP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_shard_index_find_pk = rffi.llexternal(
    "gnitz_shard_index_find_pk",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG,
     rffi.VOIDPP, rffi.INTP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Read cursor (opaque N-way merge cursor)
# ---------------------------------------------------------------------------

_read_cursor_create = rffi.llexternal(
    "gnitz_read_cursor_create",
    [rffi.VOIDPP, rffi.UINTP,
     rffi.UINTP, rffi.UINT,
     rffi.UINT,
     rffi.VOIDPP, rffi.UINT,
     rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_read_cursor_seek = rffi.llexternal(
    "gnitz_read_cursor_seek",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG,
     rffi.INTP, rffi.ULONGLONGP, rffi.ULONGLONGP,
     rffi.LONGLONGP, rffi.ULONGLONGP],
    lltype.Void,
    compilation_info=eci,
)

_read_cursor_next = rffi.llexternal(
    "gnitz_read_cursor_next",
    [rffi.VOIDP,
     rffi.INTP, rffi.ULONGLONGP, rffi.ULONGLONGP,
     rffi.LONGLONGP, rffi.ULONGLONGP],
    lltype.Void,
    compilation_info=eci,
)

_read_cursor_col_ptr = rffi.llexternal(
    "gnitz_read_cursor_col_ptr",
    [rffi.VOIDP, rffi.INT, rffi.INT],
    rffi.CCHARP,
    compilation_info=eci,
)

_read_cursor_blob_ptr = rffi.llexternal(
    "gnitz_read_cursor_blob_ptr",
    [rffi.VOIDP],
    rffi.CCHARP,
    compilation_info=eci,
)

_read_cursor_close = rffi.llexternal(
    "gnitz_read_cursor_close",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

_read_cursor_create_from_snapshots = rffi.llexternal(
    "gnitz_read_cursor_create_from_snapshots",
    [rffi.VOIDPP, rffi.UINT,
     rffi.VOIDPP, rffi.UINT,
     rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# MemTable (opaque handle)
# ---------------------------------------------------------------------------

_memtable_create = rffi.llexternal(
    "gnitz_memtable_create",
    [rffi.VOIDP, rffi.ULONGLONG],
    rffi.VOIDP,
    compilation_info=eci,
)

_memtable_close = rffi.llexternal(
    "gnitz_memtable_close",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

_memtable_upsert_batch = rffi.llexternal(
    "gnitz_memtable_upsert_batch",
    [rffi.VOIDP, rffi.VOIDPP, rffi.UINTP, rffi.UINT, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_memtable_bloom_add = rffi.llexternal(
    "gnitz_memtable_bloom_add",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    lltype.Void,
    compilation_info=eci,
)

_memtable_may_contain_pk = rffi.llexternal(
    "gnitz_memtable_may_contain_pk",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_memtable_should_flush = rffi.llexternal(
    "gnitz_memtable_should_flush",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_memtable_is_empty = rffi.llexternal(
    "gnitz_memtable_is_empty",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_memtable_total_row_count = rffi.llexternal(
    "gnitz_memtable_total_row_count",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_memtable_get_snapshot = rffi.llexternal(
    "gnitz_memtable_get_snapshot",
    [rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_memtable_snapshot_count = rffi.llexternal(
    "gnitz_memtable_snapshot_count",
    [rffi.VOIDP],
    rffi.UINT,
    compilation_info=eci,
)

_memtable_snapshot_free = rffi.llexternal(
    "gnitz_memtable_snapshot_free",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

_memtable_lookup_pk = rffi.llexternal(
    "gnitz_memtable_lookup_pk",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG, rffi.INTP],
    rffi.LONGLONG,
    compilation_info=eci,
)

_memtable_found_null_word = rffi.llexternal(
    "gnitz_memtable_found_null_word",
    [rffi.VOIDP],
    rffi.ULONGLONG,
    compilation_info=eci,
)

_memtable_found_col_ptr = rffi.llexternal(
    "gnitz_memtable_found_col_ptr",
    [rffi.VOIDP, rffi.INT, rffi.INT],
    rffi.CCHARP,
    compilation_info=eci,
)

_memtable_found_blob_ptr = rffi.llexternal(
    "gnitz_memtable_found_blob_ptr",
    [rffi.VOIDP],
    rffi.CCHARP,
    compilation_info=eci,
)

_memtable_find_weight_for_row = rffi.llexternal(
    "gnitz_memtable_find_weight_for_row",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG,
     rffi.VOIDPP, rffi.UINTP, rffi.UINT, rffi.UINT],
    rffi.LONGLONG,
    compilation_info=eci,
)

_memtable_flush = rffi.llexternal(
    "gnitz_memtable_flush",
    [rffi.VOIDP, rffi.INT, rffi.CCHARP, rffi.UINT, rffi.INT],
    rffi.INT,
    compilation_info=eci,
)

_memtable_reset = rffi.llexternal(
    "gnitz_memtable_reset",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Table (unified opaque handle)
# ---------------------------------------------------------------------------

_table_create = rffi.llexternal(
    "gnitz_table_create",
    [rffi.CCHARP, rffi.CCHARP, rffi.VOIDP, rffi.UINT, rffi.ULONGLONG, rffi.INT],
    rffi.VOIDP,
    compilation_info=eci,
)

_table_close = rffi.llexternal(
    "gnitz_table_close",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

_table_ingest_batch = rffi.llexternal(
    "gnitz_table_ingest_batch",
    [rffi.VOIDP, rffi.VOIDPP, rffi.UINTP, rffi.UINT, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_table_ingest_batch_memonly = rffi.llexternal(
    "gnitz_table_ingest_batch_memonly",
    [rffi.VOIDP, rffi.VOIDPP, rffi.UINTP, rffi.UINT, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_table_create_cursor = rffi.llexternal(
    "gnitz_table_create_cursor",
    [rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_table_has_pk = rffi.llexternal(
    "gnitz_table_has_pk",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_table_retract_pk = rffi.llexternal(
    "gnitz_table_retract_pk",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG, rffi.INTP],
    rffi.LONGLONG,
    compilation_info=eci,
)

_table_get_weight = rffi.llexternal(
    "gnitz_table_get_weight",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG,
     rffi.VOIDPP, rffi.UINTP, rffi.UINT, rffi.UINT],
    rffi.LONGLONG,
    compilation_info=eci,
)

_table_flush = rffi.llexternal(
    "gnitz_table_flush",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_table_compact_if_needed = rffi.llexternal(
    "gnitz_table_compact_if_needed",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_table_set_has_wal = rffi.llexternal(
    "gnitz_table_set_has_wal",
    [rffi.VOIDP, rffi.INT],
    lltype.Void,
    compilation_info=eci,
)

_table_current_lsn = rffi.llexternal(
    "gnitz_table_current_lsn",
    [rffi.VOIDP],
    rffi.ULONGLONG,
    compilation_info=eci,
)

_table_bloom_add = rffi.llexternal(
    "gnitz_table_bloom_add",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    lltype.Void,
    compilation_info=eci,
)

_table_memtable_upsert_batch = rffi.llexternal(
    "gnitz_table_memtable_upsert_batch",
    [rffi.VOIDP, rffi.VOIDPP, rffi.UINTP, rffi.UINT, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_table_memtable_should_flush = rffi.llexternal(
    "gnitz_table_memtable_should_flush",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_table_memtable_is_empty = rffi.llexternal(
    "gnitz_table_memtable_is_empty",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_table_found_null_word = rffi.llexternal(
    "gnitz_table_found_null_word",
    [rffi.VOIDP],
    rffi.ULONGLONG,
    compilation_info=eci,
)

_table_found_col_ptr = rffi.llexternal(
    "gnitz_table_found_col_ptr",
    [rffi.VOIDP, rffi.INT, rffi.INT],
    rffi.CCHARP,
    compilation_info=eci,
)

_table_found_blob_ptr = rffi.llexternal(
    "gnitz_table_found_blob_ptr",
    [rffi.VOIDP],
    rffi.CCHARP,
    compilation_info=eci,
)

_table_get_snapshot = rffi.llexternal(
    "gnitz_table_get_snapshot",
    [rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_table_all_shard_ptrs = rffi.llexternal(
    "gnitz_table_all_shard_ptrs",
    [rffi.VOIDP, rffi.VOIDPP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_table_create_child = rffi.llexternal(
    "gnitz_table_create_child",
    [rffi.VOIDP, rffi.CCHARP, rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# PartitionedTable (hash-routed N-way handle)
# ---------------------------------------------------------------------------

_ptable_create = rffi.llexternal(
    "gnitz_ptable_create",
    [rffi.CCHARP, rffi.CCHARP, rffi.VOIDP, rffi.UINT, rffi.UINT,
     rffi.INT, rffi.UINT, rffi.UINT, rffi.ULONGLONG],
    rffi.VOIDP, compilation_info=eci,
)
_ptable_close = rffi.llexternal(
    "gnitz_ptable_close", [rffi.VOIDP], lltype.Void, compilation_info=eci,
)
_ptable_ingest_batch = rffi.llexternal(
    "gnitz_ptable_ingest_batch",
    [rffi.VOIDP, rffi.VOIDPP, rffi.UINTP, rffi.UINT, rffi.UINT],
    rffi.INT, compilation_info=eci,
)
_ptable_ingest_batch_memonly = rffi.llexternal(
    "gnitz_ptable_ingest_batch_memonly",
    [rffi.VOIDP, rffi.VOIDPP, rffi.UINTP, rffi.UINT, rffi.UINT],
    rffi.INT, compilation_info=eci,
)
_ptable_create_cursor = rffi.llexternal(
    "gnitz_ptable_create_cursor", [rffi.VOIDP], rffi.VOIDP, compilation_info=eci,
)
_ptable_has_pk = rffi.llexternal(
    "gnitz_ptable_has_pk",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.INT, compilation_info=eci,
)
_ptable_retract_pk = rffi.llexternal(
    "gnitz_ptable_retract_pk",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG, rffi.INTP],
    rffi.LONGLONG, compilation_info=eci,
)
_ptable_get_weight = rffi.llexternal(
    "gnitz_ptable_get_weight",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG,
     rffi.VOIDPP, rffi.UINTP, rffi.UINT, rffi.UINT],
    rffi.LONGLONG, compilation_info=eci,
)
_ptable_flush = rffi.llexternal(
    "gnitz_ptable_flush", [rffi.VOIDP], rffi.INT, compilation_info=eci,
)
_ptable_compact_if_needed = rffi.llexternal(
    "gnitz_ptable_compact_if_needed", [rffi.VOIDP], rffi.INT, compilation_info=eci,
)
_ptable_set_has_wal = rffi.llexternal(
    "gnitz_ptable_set_has_wal", [rffi.VOIDP, rffi.INT], lltype.Void, compilation_info=eci,
)
_ptable_current_lsn = rffi.llexternal(
    "gnitz_ptable_current_lsn", [rffi.VOIDP], rffi.ULONGLONG, compilation_info=eci,
)
_ptable_close_partitions_outside = rffi.llexternal(
    "gnitz_ptable_close_partitions_outside",
    [rffi.VOIDP, rffi.UINT, rffi.UINT], lltype.Void, compilation_info=eci,
)
_ptable_close_all_partitions = rffi.llexternal(
    "gnitz_ptable_close_all_partitions", [rffi.VOIDP], lltype.Void, compilation_info=eci,
)
_ptable_found_null_word = rffi.llexternal(
    "gnitz_ptable_found_null_word", [rffi.VOIDP], rffi.ULONGLONG, compilation_info=eci,
)
_ptable_found_col_ptr = rffi.llexternal(
    "gnitz_ptable_found_col_ptr", [rffi.VOIDP, rffi.INT, rffi.INT],
    rffi.CCHARP, compilation_info=eci,
)
_ptable_found_blob_ptr = rffi.llexternal(
    "gnitz_ptable_found_blob_ptr", [rffi.VOIDP], rffi.CCHARP, compilation_info=eci,
)
_ptable_create_child = rffi.llexternal(
    "gnitz_ptable_create_child", [rffi.VOIDP, rffi.CCHARP, rffi.VOIDP],
    rffi.VOIDP, compilation_info=eci,
)
_ptable_bloom_add = rffi.llexternal(
    "gnitz_ptable_bloom_add",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    lltype.Void, compilation_info=eci,
)
