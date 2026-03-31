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
        # logging
        "void gnitz_log_init(uint32_t level, const uint8_t *tag, uint32_t tag_len);",
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
        # DBSP operators (single-call, Rust-side)
        "int32_t gnitz_op_distinct("
        "  const void *delta, void *cursor, const void *schema,"
        "  void **out_result, void **out_consolidated);",
        "int32_t gnitz_op_anti_join_dt("
        "  const void *delta, void *cursor, const void *schema,"
        "  void **out_result);",
        "int32_t gnitz_op_semi_join_dt("
        "  const void *delta, void *cursor, const void *schema,"
        "  void **out_result);",
        "int32_t gnitz_op_join_dt("
        "  const void *delta, void *cursor,"
        "  const void *left_schema, const void *right_schema,"
        "  void **out_result);",
        "int32_t gnitz_op_join_dt_outer("
        "  const void *delta, void *cursor,"
        "  const void *left_schema, const void *right_schema,"
        "  void **out_result);",
        "int32_t gnitz_op_anti_join_dd("
        "  const void *a, const void *b, const void *schema,"
        "  void **out_result);",
        "int32_t gnitz_op_semi_join_dd("
        "  const void *a, const void *b, const void *schema,"
        "  void **out_result);",
        "int32_t gnitz_op_join_dd("
        "  const void *a, const void *b,"
        "  const void *left_schema, const void *right_schema,"
        "  void **out_result);",
        # expression programs and scalar functions
        "void *gnitz_expr_program_create("
        "  const int64_t *code, uint32_t code_len,"
        "  uint32_t num_regs, uint32_t result_reg,"
        "  const uint8_t *const_string_data,"
        "  const uint32_t *const_string_offsets,"
        "  const uint32_t *const_string_lengths,"
        "  uint32_t num_const_strings);",
        "void gnitz_expr_program_free(void *handle);",
        "void *gnitz_scalar_func_create_expr_predicate(void *program);",
        "void *gnitz_scalar_func_create_expr_map(void *program);",
        "void *gnitz_scalar_func_create_universal_predicate("
        "  uint32_t col_idx, uint8_t op, uint64_t val_bits, int32_t is_float);",
        "void *gnitz_scalar_func_create_universal_projection("
        "  const uint32_t *src_indices, const uint8_t *src_types, uint32_t count);",
        "void gnitz_scalar_func_free(void *handle);",
        # linear operators
        "int32_t gnitz_op_filter("
        "  const void *batch, const void *func, const void *schema,"
        "  void **out_result);",
        "int32_t gnitz_op_map("
        "  const void *batch, const void *func,"
        "  const void *in_schema, const void *out_schema,"
        "  int32_t reindex_col, void **out_result);",
        "int32_t gnitz_op_negate(const void *batch, void **out_result);",
        "int32_t gnitz_op_union("
        "  const void *batch_a, const void *batch_b,"
        "  const void *schema, void **out_result);",
        # reduce operators
        "int32_t gnitz_op_reduce("
        "  const void *delta, void *trace_in_cursor, void *trace_out_cursor,"
        "  const void *input_schema, const void *output_schema,"
        "  const uint32_t *group_by_cols, uint32_t num_group_by_cols,"
        "  const void *agg_descs, uint32_t num_aggs,"
        "  void *avi_cursor, int32_t avi_for_max, uint8_t avi_agg_col_type_code,"
        "  const uint32_t *avi_group_by_cols, uint32_t avi_num_group_by_cols,"
        "  const void *avi_input_schema,"
        "  void *gi_cursor, uint32_t gi_col_idx, uint8_t gi_col_type_code,"
        "  const void *finalize_prog, const void *finalize_out_schema,"
        "  void **out_result, void **out_finalized);",
        "int32_t gnitz_op_gather_reduce("
        "  const void *partial, void *trace_out_cursor,"
        "  const void *partial_schema,"
        "  const void *agg_descs, uint32_t num_aggs,"
        "  void **out_result);",
        # VM program lifecycle
        "void *gnitz_vm_program_create("
        "  const uint8_t *instr_data, uint32_t instr_data_len,"
        "  const void *reg_schemas, const uint8_t *reg_kinds,"
        "  uint32_t num_registers,"
        "  void **func_handles, uint32_t num_funcs,"
        "  void **table_handles, uint32_t num_tables,"
        "  void **expr_handles, uint32_t num_exprs,"
        "  const void *agg_descs_buf, uint32_t num_agg_descs,"
        "  const uint32_t *group_cols_buf, uint32_t num_group_cols,"
        "  const void *extra_schemas, uint32_t num_extra_schemas);",
        "int32_t gnitz_vm_execute_epoch("
        "  void *handle, void *input_batch,"
        "  uint16_t input_reg_idx, uint16_t output_reg_idx,"
        "  void **cursor_handles, uint32_t num_cursors,"
        "  void **out_result);",
        "void gnitz_vm_program_free(void *handle);",
        # scan trace
        "int32_t gnitz_op_scan_trace("
        "  void *cursor, const void *schema, int32_t chunk_limit,"
        "  void **out_result);",
        # integrate with indexes
        "int32_t gnitz_op_integrate_with_indexes("
        "  const void *batch, void *target_table, const void *input_schema,"
        "  void *gi_table, uint32_t gi_col_idx, uint8_t gi_col_type_code,"
        "  void *avi_table, int32_t avi_for_max, uint8_t avi_agg_col_type_code,"
        "  const uint32_t *avi_group_by_cols, uint32_t avi_num_group_by_cols,"
        "  const void *avi_input_schema, uint32_t avi_agg_col_idx);",
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
        "uint64_t gnitz_read_cursor_blob_len(const void *handle);",
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
        # batch (OwnedBatch as first-class FFI handle)
        "void *gnitz_batch_create(const void *schema_desc, uint32_t initial_capacity);",
        "void gnitz_batch_free(void *handle);",
        "void *gnitz_batch_clone(const void *handle);",
        "void gnitz_batch_clear(void *handle);",
        "uint32_t gnitz_batch_length(const void *handle);",
        "int32_t gnitz_batch_is_sorted(const void *handle);",
        "int32_t gnitz_batch_is_consolidated(const void *handle);",
        "void gnitz_batch_set_sorted(void *handle, int32_t val);",
        "void gnitz_batch_set_consolidated(void *handle, int32_t val);",
        "uint64_t gnitz_batch_get_pk_lo(const void *handle, uint32_t row);",
        "uint64_t gnitz_batch_get_pk_hi(const void *handle, uint32_t row);",
        "int64_t gnitz_batch_get_weight(const void *handle, uint32_t row);",
        "uint64_t gnitz_batch_get_null_word(const void *handle, uint32_t row);",
        "const uint8_t *gnitz_batch_col_ptr(const void *handle, uint32_t row, uint32_t payload_col, uint32_t col_size);",
        "const uint8_t *gnitz_batch_blob_ptr(const void *handle);",
        "int32_t gnitz_batch_append_row("
        "  void *handle, uint64_t pk_lo, uint64_t pk_hi, int64_t weight, uint64_t null_word,"
        "  void **col_ptrs, uint32_t *col_sizes, uint32_t num_cols,"
        "  const uint8_t *blob_src, uint32_t blob_len);",
        "int32_t gnitz_batch_append_batch(void *handle, const void *src, uint32_t start, uint32_t end);",
        "int32_t gnitz_batch_append_batch_negated(void *handle, const void *src, uint32_t start, uint32_t end);",
        "void *gnitz_batch_to_sorted(const void *handle, const void *schema_desc);",
        "void *gnitz_batch_to_consolidated(const void *handle, const void *schema_desc);",
        "void *gnitz_batch_scatter_copy(const void *src, const uint32_t *indices, uint32_t num_indices, const void *schema_desc);",
        "int32_t gnitz_batch_append_row_from_batch(void *handle, const void *src, uint32_t row, int64_t weight);",
        "void *gnitz_batch_scatter_copy_weighted(const void *src, const uint32_t *indices, const int64_t *weights, uint32_t num_indices, const void *schema_desc);",
        "const uint8_t *gnitz_batch_region_ptr(const void *handle, uint32_t idx);",
        "uint32_t gnitz_batch_region_size(const void *handle, uint32_t idx);",
        "uint32_t gnitz_batch_num_regions(const void *handle);",
        "void *gnitz_batch_from_regions(const void *schema_desc, void **ptrs, uint32_t *sizes, uint32_t count, uint32_t rpb);",
        "void gnitz_batch_alloc_system(void *handle, uint32_t n);",
        "uint8_t *gnitz_batch_col_extend(void *handle, uint32_t payload_col, uint32_t n_bytes);",
        "uint64_t gnitz_batch_blob_extend(void *handle, uint32_t n_bytes);",
        "uint64_t gnitz_batch_blob_len(const void *handle);",
        "void gnitz_batch_set_count(void *handle, uint32_t count);",
    ],
    link_files=[_lib_path] if _lib_path else [],
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_log_init = rffi.llexternal(
    "gnitz_log_init",
    [rffi.UINT, rffi.CCHARP, rffi.UINT],
    lltype.Void,
    compilation_info=eci,
)


def log_init(level, tag):
    """Initialize Rust-side logging. Call after log.init() in main.py."""
    n = len(tag)
    tag_buf = lltype.malloc(rffi.CCHARP.TO, max(n, 1), flavor="raw")
    for i in range(n):
        tag_buf[i] = tag[i]
    _log_init(rffi.cast(rffi.UINT, level), tag_buf, rffi.cast(rffi.UINT, n))
    lltype.free(tag_buf, flavor="raw")


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

# ---------------------------------------------------------------------------
# DBSP operators (single-call, Rust-side)
# ---------------------------------------------------------------------------

_op_distinct = rffi.llexternal(
    "gnitz_op_distinct",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDPP, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_op_anti_join_dt = rffi.llexternal(
    "gnitz_op_anti_join_dt",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_op_semi_join_dt = rffi.llexternal(
    "gnitz_op_semi_join_dt",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_op_join_dt = rffi.llexternal(
    "gnitz_op_join_dt",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_op_join_dt_outer = rffi.llexternal(
    "gnitz_op_join_dt_outer",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_op_anti_join_dd = rffi.llexternal(
    "gnitz_op_anti_join_dd",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_op_semi_join_dd = rffi.llexternal(
    "gnitz_op_semi_join_dd",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_op_join_dd = rffi.llexternal(
    "gnitz_op_join_dd",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_vm_program_create = rffi.llexternal(
    "gnitz_vm_program_create",
    [rffi.CCHARP, rffi.UINT,             # instr_data, instr_data_len
     rffi.VOIDP, rffi.CCHARP,            # reg_schemas, reg_kinds
     rffi.UINT,                           # num_registers
     rffi.VOIDPP, rffi.UINT,             # func_handles, num_funcs
     rffi.VOIDPP, rffi.UINT,             # table_handles, num_tables
     rffi.VOIDPP, rffi.UINT,             # expr_handles, num_exprs
     rffi.VOIDP, rffi.UINT,              # agg_descs_buf, num_agg_descs
     rffi.UINTP, rffi.UINT,              # group_cols_buf, num_group_cols
     rffi.VOIDP, rffi.UINT],             # extra_schemas, num_extra_schemas
    rffi.VOIDP,
    compilation_info=eci,
)

_vm_execute_epoch = rffi.llexternal(
    "gnitz_vm_execute_epoch",
    [rffi.VOIDP, rffi.VOIDP,             # handle, input_batch
     rffi.USHORT, rffi.USHORT,           # input_reg_idx, output_reg_idx
     rffi.VOIDPP, rffi.UINT,             # cursor_handles, num_cursors
     rffi.VOIDPP],                        # out_result
    rffi.INT,
    compilation_info=eci,
)

_vm_program_free = rffi.llexternal(
    "gnitz_vm_program_free",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

_op_scan_trace = rffi.llexternal(
    "gnitz_op_scan_trace",
    [rffi.VOIDP, rffi.VOIDP, rffi.INT, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_op_integrate_with_indexes = rffi.llexternal(
    "gnitz_op_integrate_with_indexes",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP,
     rffi.VOIDP, rffi.UINT, rffi.UCHAR,
     rffi.VOIDP, rffi.INT, rffi.UCHAR,
     rffi.UINTP, rffi.UINT,
     rffi.VOIDP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Expression programs and scalar functions
# ---------------------------------------------------------------------------

_expr_program_create = rffi.llexternal(
    "gnitz_expr_program_create",
    [rffi.LONGLONGP, rffi.UINT, rffi.UINT, rffi.UINT,
     rffi.CCHARP, rffi.UINTP, rffi.UINTP, rffi.UINT],
    rffi.VOIDP,
    compilation_info=eci,
)

_expr_program_free = rffi.llexternal(
    "gnitz_expr_program_free",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

_scalar_func_create_expr_predicate = rffi.llexternal(
    "gnitz_scalar_func_create_expr_predicate",
    [rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_scalar_func_create_expr_map = rffi.llexternal(
    "gnitz_scalar_func_create_expr_map",
    [rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_scalar_func_create_universal_predicate = rffi.llexternal(
    "gnitz_scalar_func_create_universal_predicate",
    [rffi.UINT, rffi.UCHAR, rffi.ULONGLONG, rffi.INT],
    rffi.VOIDP,
    compilation_info=eci,
)

_scalar_func_create_universal_projection = rffi.llexternal(
    "gnitz_scalar_func_create_universal_projection",
    [rffi.UINTP, rffi.CCHARP, rffi.UINT],
    rffi.VOIDP,
    compilation_info=eci,
)

_scalar_func_free = rffi.llexternal(
    "gnitz_scalar_func_free",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Linear operators (single-call, Rust-side)
# ---------------------------------------------------------------------------

_op_filter = rffi.llexternal(
    "gnitz_op_filter",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_op_map = rffi.llexternal(
    "gnitz_op_map",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.INT, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_op_negate = rffi.llexternal(
    "gnitz_op_negate",
    [rffi.VOIDP, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_op_union = rffi.llexternal(
    "gnitz_op_union",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_op_reduce = rffi.llexternal(
    "gnitz_op_reduce",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP,           # delta, trace_in, trace_out
     rffi.VOIDP, rffi.VOIDP,                         # input_schema, output_schema
     rffi.UINTP, rffi.UINT,                           # group_by_cols, num_gcols
     rffi.VOIDP, rffi.UINT,                           # agg_descs, num_aggs
     rffi.VOIDP, rffi.INT, rffi.UCHAR,               # avi_cursor, for_max, agg_type
     rffi.UINTP, rffi.UINT, rffi.VOIDP,              # avi_gcols, num, avi_schema
     rffi.VOIDP, rffi.UINT, rffi.UCHAR,              # gi_cursor, col_idx, col_type
     rffi.VOIDP, rffi.VOIDP,                         # finalize_prog, fin_schema
     rffi.VOIDPP, rffi.VOIDPP],                       # out_result, out_finalized
    rffi.INT,
    compilation_info=eci,
)

_op_gather_reduce = rffi.llexternal(
    "gnitz_op_gather_reduce",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP,
     rffi.VOIDP, rffi.UINT, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

AGG_DESC_SIZE = 8  # sizeof(AggDescriptor) = 8 bytes

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

_read_cursor_blob_len = rffi.llexternal(
    "gnitz_read_cursor_blob_len",
    [rffi.VOIDP],
    rffi.ULONGLONG,
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

# ---------------------------------------------------------------------------
# Batch (OwnedBatch as first-class FFI handle)
# ---------------------------------------------------------------------------

_batch_create = rffi.llexternal(
    "gnitz_batch_create", [rffi.VOIDP, rffi.UINT], rffi.VOIDP, compilation_info=eci,
)
_batch_free = rffi.llexternal(
    "gnitz_batch_free", [rffi.VOIDP], lltype.Void, compilation_info=eci,
)
_batch_clone = rffi.llexternal(
    "gnitz_batch_clone", [rffi.VOIDP], rffi.VOIDP, compilation_info=eci,
)
_batch_clear = rffi.llexternal(
    "gnitz_batch_clear", [rffi.VOIDP], lltype.Void, compilation_info=eci,
)
_batch_length = rffi.llexternal(
    "gnitz_batch_length", [rffi.VOIDP], rffi.UINT, compilation_info=eci,
)
_batch_is_sorted = rffi.llexternal(
    "gnitz_batch_is_sorted", [rffi.VOIDP], rffi.INT, compilation_info=eci,
)
_batch_is_consolidated = rffi.llexternal(
    "gnitz_batch_is_consolidated", [rffi.VOIDP], rffi.INT, compilation_info=eci,
)
_batch_set_sorted = rffi.llexternal(
    "gnitz_batch_set_sorted", [rffi.VOIDP, rffi.INT], lltype.Void, compilation_info=eci,
)
_batch_set_consolidated = rffi.llexternal(
    "gnitz_batch_set_consolidated", [rffi.VOIDP, rffi.INT], lltype.Void, compilation_info=eci,
)
_batch_get_pk_lo = rffi.llexternal(
    "gnitz_batch_get_pk_lo", [rffi.VOIDP, rffi.UINT], rffi.ULONGLONG, compilation_info=eci,
)
_batch_get_pk_hi = rffi.llexternal(
    "gnitz_batch_get_pk_hi", [rffi.VOIDP, rffi.UINT], rffi.ULONGLONG, compilation_info=eci,
)
_batch_get_weight = rffi.llexternal(
    "gnitz_batch_get_weight", [rffi.VOIDP, rffi.UINT], rffi.LONGLONG, compilation_info=eci,
)
_batch_get_null_word = rffi.llexternal(
    "gnitz_batch_get_null_word", [rffi.VOIDP, rffi.UINT], rffi.ULONGLONG, compilation_info=eci,
)
_batch_col_ptr = rffi.llexternal(
    "gnitz_batch_col_ptr", [rffi.VOIDP, rffi.UINT, rffi.UINT, rffi.UINT],
    rffi.CCHARP, compilation_info=eci,
)
_batch_blob_ptr = rffi.llexternal(
    "gnitz_batch_blob_ptr", [rffi.VOIDP], rffi.CCHARP, compilation_info=eci,
)
_batch_append_row = rffi.llexternal(
    "gnitz_batch_append_row",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG, rffi.LONGLONG, rffi.ULONGLONG,
     rffi.VOIDPP, rffi.UINTP, rffi.UINT, rffi.CCHARP, rffi.UINT],
    rffi.INT, compilation_info=eci,
)
_batch_append_batch = rffi.llexternal(
    "gnitz_batch_append_batch", [rffi.VOIDP, rffi.VOIDP, rffi.UINT, rffi.UINT],
    rffi.INT, compilation_info=eci,
)
_batch_append_batch_negated = rffi.llexternal(
    "gnitz_batch_append_batch_negated", [rffi.VOIDP, rffi.VOIDP, rffi.UINT, rffi.UINT],
    rffi.INT, compilation_info=eci,
)
_batch_to_sorted = rffi.llexternal(
    "gnitz_batch_to_sorted", [rffi.VOIDP, rffi.VOIDP], rffi.VOIDP, compilation_info=eci,
)
_batch_to_consolidated = rffi.llexternal(
    "gnitz_batch_to_consolidated", [rffi.VOIDP, rffi.VOIDP], rffi.VOIDP, compilation_info=eci,
)
_batch_scatter_copy = rffi.llexternal(
    "gnitz_batch_scatter_copy", [rffi.VOIDP, rffi.UINTP, rffi.UINT, rffi.VOIDP],
    rffi.VOIDP, compilation_info=eci,
)
_batch_append_row_from_batch = rffi.llexternal(
    "gnitz_batch_append_row_from_batch",
    [rffi.VOIDP, rffi.VOIDP, rffi.UINT, rffi.LONGLONG],
    rffi.INT, compilation_info=eci,
)
_batch_scatter_copy_weighted = rffi.llexternal(
    "gnitz_batch_scatter_copy_weighted",
    [rffi.VOIDP, rffi.UINTP, rffi.LONGLONGP, rffi.UINT, rffi.VOIDP],
    rffi.VOIDP, compilation_info=eci,
)
_batch_region_ptr = rffi.llexternal(
    "gnitz_batch_region_ptr", [rffi.VOIDP, rffi.UINT], rffi.CCHARP, compilation_info=eci,
)
_batch_region_size = rffi.llexternal(
    "gnitz_batch_region_size", [rffi.VOIDP, rffi.UINT], rffi.UINT, compilation_info=eci,
)
_batch_num_regions = rffi.llexternal(
    "gnitz_batch_num_regions", [rffi.VOIDP], rffi.UINT, compilation_info=eci,
)
_batch_from_regions = rffi.llexternal(
    "gnitz_batch_from_regions", [rffi.VOIDP, rffi.VOIDPP, rffi.UINTP, rffi.UINT, rffi.UINT],
    rffi.VOIDP, compilation_info=eci,
)
_batch_alloc_system = rffi.llexternal(
    "gnitz_batch_alloc_system", [rffi.VOIDP, rffi.UINT], lltype.Void, compilation_info=eci,
)
_batch_col_extend = rffi.llexternal(
    "gnitz_batch_col_extend", [rffi.VOIDP, rffi.UINT, rffi.UINT],
    rffi.CCHARP, compilation_info=eci,
)
_batch_blob_extend = rffi.llexternal(
    "gnitz_batch_blob_extend", [rffi.VOIDP, rffi.UINT], rffi.ULONGLONG, compilation_info=eci,
)
_batch_blob_len = rffi.llexternal(
    "gnitz_batch_blob_len", [rffi.VOIDP], rffi.ULONGLONG, compilation_info=eci,
)
_batch_set_count = rffi.llexternal(
    "gnitz_batch_set_count", [rffi.VOIDP, rffi.UINT], lltype.Void, compilation_info=eci,
)
