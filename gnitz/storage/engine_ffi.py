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
        # linear operators
        "int32_t gnitz_op_union("
        "  const void *batch_a, const void *batch_b,"
        "  const void *schema, void **out_result);",
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
        "int32_t gnitz_ptable_get_child_dir(void *handle, char *buf, uint32_t buf_len);",
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
        "int32_t gnitz_batch_append_row_simple("
        "  void *handle, uint64_t pk_lo, uint64_t pk_hi, int64_t weight, uint64_t null_word,"
        "  const int64_t *lo_values, const uint64_t *hi_values,"
        "  char **str_ptrs, const uint32_t *str_lens,"
        "  uint32_t n_payload);",
        "int32_t gnitz_batch_append_batch(void *handle, const void *src, uint32_t start, uint32_t end);",
        "int32_t gnitz_batch_append_batch_negated(void *handle, const void *src, uint32_t start, uint32_t end);",
        "void *gnitz_batch_to_sorted(const void *handle, const void *schema_desc);",
        "void *gnitz_batch_to_consolidated(const void *handle, const void *schema_desc);",
        "void *gnitz_batch_scatter_copy(const void *src, const uint32_t *indices, uint32_t num_indices, const void *schema_desc);",
        "int32_t gnitz_batch_append_row_from_batch(void *handle, const void *src, uint32_t row, int64_t weight);",
        "int32_t gnitz_batch_append_row_from_cursor("
        "  void *handle, const void *cursor_handle, int64_t weight);",
        "int32_t gnitz_batch_append_row_from_table_found("
        "  void *handle, const void *table_handle,"
        "  uint64_t pk_lo, uint64_t pk_hi, int64_t weight);",
        "int32_t gnitz_batch_append_row_from_ptable_found("
        "  void *handle, const void *ptable_handle,"
        "  uint64_t pk_lo, uint64_t pk_hi, int64_t weight);",
        "void *gnitz_batch_scatter_copy_weighted(const void *src, const uint32_t *indices, const int64_t *weights, uint32_t num_indices, const void *schema_desc);",
        "const uint8_t *gnitz_batch_region_ptr(const void *handle, uint32_t idx);",
        "uint32_t gnitz_batch_region_size(const void *handle, uint32_t idx);",
        "uint32_t gnitz_batch_num_regions(const void *handle);",
        "void *gnitz_batch_from_regions(const void *schema_desc, void **ptrs, uint32_t *sizes, uint32_t count, uint32_t rpb);",
        "void *gnitz_batch_project_index("
        "  const void *src, uint32_t source_col_idx,"
        "  const void *index_schema_desc);",
        # partition routing
        "uint32_t gnitz_partition_for_key(uint64_t pk_lo, uint64_t pk_hi);",
        "uint32_t gnitz_worker_for_pk(uint64_t pk_lo, uint64_t pk_hi, uint32_t num_workers);",
        "void *gnitz_partition_router_create(void);",
        "void gnitz_partition_router_free(void *handle);",
        "int32_t gnitz_partition_router_worker_for_index_key("
        "  const void *handle, uint32_t tid, uint32_t col_idx, uint64_t key_lo);",
        "void gnitz_partition_router_record_routing("
        "  void *handle, const void *batch, const void *schema,"
        "  uint32_t tid, uint32_t col_idx, uint32_t worker);",
        # exchange batch repartitioning
        "int32_t gnitz_repartition_batch("
        "  const void *src_batch,"
        "  const uint32_t *col_indices, uint32_t num_col_indices,"
        "  const void *schema_desc, uint32_t num_workers,"
        "  void **out_handles);",
        "int32_t gnitz_relay_scatter("
        "  void *const *source_handles, uint32_t num_sources,"
        "  const uint32_t *col_indices, uint32_t num_col_indices,"
        "  const void *schema_desc, uint32_t num_workers,"
        "  void **out_handles);",
        "int32_t gnitz_multi_scatter("
        "  const void *src_batch,"
        "  const uint32_t *col_specs_flat, const uint32_t *spec_lengths,"
        "  uint32_t num_specs, const void *schema_desc, uint32_t num_workers,"
        "  void **out_handles);",
        # IPC syscall wrappers (ipc_sys.rs)
        "int32_t gnitz_eventfd_create(void);",
        "int32_t gnitz_eventfd_signal(int32_t efd);",
        "int32_t gnitz_eventfd_wait(int32_t efd, int32_t timeout_ms);",
        "int32_t gnitz_eventfd_wait_any(const int32_t *efds, int32_t n, int32_t timeout_ms);",
        "int32_t gnitz_fallocate(int32_t fd, int64_t length);",
        "int32_t gnitz_try_set_nocow(int32_t fd);",
        "int32_t gnitz_fdatasync(int32_t fd);",
        # IPC atomics + SAL/W2M transport (ipc.rs)
        # Note: use char*/char** instead of uint8_t* for RPython CCHARP compatibility
        "uint64_t gnitz_atomic_load_u64(const char *ptr);",
        "void gnitz_atomic_store_u64(char *ptr, uint64_t val);",
        # SAL write: returns struct {int32_t status; uint64_t new_cursor;}
        # We call it and read the two fields via a raw buffer.
        "int32_t gnitz_sal_write_group("
        "  char *sal_ptr, uint64_t write_cursor,"
        "  uint32_t num_workers, uint32_t target_id,"
        "  uint64_t lsn, uint32_t flags, uint32_t epoch,"
        "  uint64_t mmap_size,"
        "  char **worker_ptrs, const uint32_t *worker_sizes);",
        # SAL read: we use the FFI struct via raw pointer access
        "int32_t gnitz_sal_read_group_header("
        "  const char *sal_ptr, uint64_t read_cursor, uint32_t worker_id);",
        # W2M
        "int64_t gnitz_w2m_write("
        "  char *region_ptr, const char *data_ptr,"
        "  uint32_t data_size, uint64_t region_size);",
        "uint64_t gnitz_w2m_read("
        "  const char *region_ptr, uint64_t read_cursor,"
        "  char **out_data_ptr, uint32_t *out_data_size);",
        # IPC wire protocol (ipc.rs)
        # Note: use char* instead of uint8_t* for RPython CCHARP compatibility
        "int32_t gnitz_ipc_encode_wire("
        "  uint64_t target_id, uint64_t client_id, uint64_t flags,"
        "  uint64_t seek_pk_lo, uint64_t seek_pk_hi, uint64_t seek_col_idx,"
        "  uint32_t status,"
        "  const char *error_msg_ptr, uint32_t error_msg_len,"
        "  const void *schema_desc,"
        "  char **col_names_ptr, const uint32_t *col_names_len,"
        "  uint32_t num_col_names,"
        "  const void *batch_handle,"
        "  char **out_ptr, uint32_t *out_len);",
        "void gnitz_ipc_wire_free(char *ptr, uint32_t len);",
        "void *gnitz_ipc_decode_wire("
        "  const char *data_ptr, uint32_t data_size,"
        "  uint32_t *out_status, uint64_t *out_client_id,"
        "  uint64_t *out_target_id, uint64_t *out_flags,"
        "  uint64_t *out_seek_pk_lo, uint64_t *out_seek_pk_hi,"
        "  uint64_t *out_seek_col_idx,"
        "  char **out_error_msg_ptr, uint32_t *out_error_msg_len,"
        "  void *out_schema_desc, int32_t *out_has_schema,"
        "  void **out_batch_handle, uint32_t *out_num_col_names);",
        "int32_t gnitz_ipc_decode_result_col_name("
        "  const void *handle, uint32_t col_idx,"
        "  char **out_ptr, uint32_t *out_len);",
        "void *gnitz_ipc_decode_result_take_batch(void *handle);",
        "void gnitz_ipc_decode_result_free(void *handle);",
        # CatalogEngine lifecycle
        "void *gnitz_catalog_open(const uint8_t *base_dir, uint32_t base_dir_len);",
        "void gnitz_catalog_close(void *handle);",
        "const uint8_t *gnitz_catalog_last_error(uint32_t *out_len);",
        # CatalogEngine DDL
        "int32_t gnitz_catalog_create_schema(void *handle, const uint8_t *name, uint32_t name_len);",
        "int64_t gnitz_catalog_create_table("
        "  void *handle, const uint8_t *name, uint32_t name_len,"
        "  const uint8_t *col_defs, uint32_t col_defs_len,"
        "  uint32_t num_cols, uint32_t pk_col_idx, int32_t unique_pk);",
        "int64_t gnitz_catalog_create_view("
        "  void *handle, const uint8_t *name, uint32_t name_len,"
        "  const uint8_t *sql_def, uint32_t sql_def_len,"
        "  const int32_t *nodes, uint32_t nodes_count,"
        "  const int32_t *edges, uint32_t edges_count,"
        "  const int64_t *sources, uint32_t sources_count,"
        "  const int64_t *params, uint32_t params_count,"
        "  const int32_t *group_cols, uint32_t group_cols_count,"
        "  const uint8_t *output_col_defs, uint32_t output_col_defs_len, uint32_t output_col_defs_count,"
        "  const int64_t *deps, uint32_t deps_count);",
        "int64_t gnitz_catalog_create_index("
        "  void *handle, const uint8_t *owner, uint32_t owner_len,"
        "  const uint8_t *col_name, uint32_t col_name_len, int32_t is_unique);",
        "int32_t gnitz_catalog_drop_schema(void *handle, const uint8_t *name, uint32_t name_len);",
        "int32_t gnitz_catalog_drop_table(void *handle, const uint8_t *name, uint32_t name_len);",
        "int32_t gnitz_catalog_drop_view(void *handle, const uint8_t *name, uint32_t name_len);",
        "int32_t gnitz_catalog_drop_index(void *handle, const uint8_t *name, uint32_t name_len);",
        # CatalogEngine ID allocation
        "int64_t gnitz_catalog_allocate_schema_id(void *handle);",
        "int64_t gnitz_catalog_allocate_table_id(void *handle);",
        "int64_t gnitz_catalog_allocate_index_id(void *handle);",
        "int32_t gnitz_catalog_advance_sequence("
        "  void *handle, int64_t seq_id, int64_t old_val, int64_t new_val);",
        # CatalogEngine queries
        "int32_t gnitz_catalog_has_id(const void *handle, int64_t table_id);",
        "int32_t gnitz_catalog_get_schema_desc("
        "  const void *handle, int64_t table_id, void *out_schema);",
        "int32_t gnitz_catalog_get_depth(const void *handle, int64_t table_id);",
        "int32_t gnitz_catalog_is_unique_pk(const void *handle, int64_t table_id);",
        # CatalogEngine ingestion/scan/seek/flush
        "int32_t gnitz_catalog_ingest(void *handle, int64_t table_id, void *batch);",
        "void *gnitz_catalog_ingest_effective("
        "  void *handle, int64_t table_id, void *batch);",
        "int32_t gnitz_catalog_push_and_evaluate("
        "  void *handle, int64_t table_id, void *batch);",
        "void *gnitz_catalog_scan(void *handle, int64_t table_id);",
        "void *gnitz_catalog_seek("
        "  void *handle, int64_t table_id, uint64_t pk_lo, uint64_t pk_hi);",
        "void *gnitz_catalog_seek_by_index("
        "  void *handle, int64_t table_id, uint32_t col_idx,"
        "  uint64_t key_lo, uint64_t key_hi);",
        "int32_t gnitz_catalog_flush(void *handle, int64_t table_id);",
        "int32_t gnitz_catalog_validate_unique_indices("
        "  void *handle, int64_t table_id, const void *batch);",
        "int32_t gnitz_catalog_validate_fk_inline("
        "  void *handle, int64_t table_id, const void *batch);",
        # CatalogEngine worker support
        "int32_t gnitz_catalog_ddl_sync(void *handle, int64_t table_id, void *batch);",
        "int32_t gnitz_catalog_raw_store_ingest(void *handle, int64_t table_id, void *batch);",
        # CatalogEngine partition management
        "void gnitz_catalog_set_active_partitions(void *handle, uint32_t start, uint32_t end);",
        "void gnitz_catalog_close_user_table_partitions(void *handle);",
        "void gnitz_catalog_trim_worker_partitions(void *handle, uint32_t start, uint32_t end);",
        "void gnitz_catalog_disable_user_table_wal(void *handle);",
        "void gnitz_catalog_invalidate_all_plans(void *handle);",
        # CatalogEngine FK/index metadata
        "uint32_t gnitz_catalog_get_fk_count(const void *handle, int64_t table_id);",
        "int32_t gnitz_catalog_get_fk_constraint("
        "  const void *handle, int64_t table_id, uint32_t idx,"
        "  uint32_t *out_col_idx, int64_t *out_target_table_id, uint8_t *out_col_type);",
        "uint32_t gnitz_catalog_get_index_circuit_count(const void *handle, int64_t table_id);",
        "int32_t gnitz_catalog_get_index_circuit_info("
        "  const void *handle, int64_t table_id, uint32_t idx,"
        "  uint32_t *out_col_idx, int32_t *out_is_unique, uint8_t *out_type_code);",
        "void *gnitz_catalog_get_index_store(const void *handle, int64_t table_id, uint32_t col_idx);",
        # CatalogEngine handle accessors
        "void *gnitz_catalog_get_ptable_handle(const void *handle, int64_t table_id);",
        "void *gnitz_catalog_get_dag_handle(void *handle);",
        "uint32_t gnitz_catalog_iter_user_table_ids("
        "  const void *handle, int64_t *out_ids, uint32_t max_ids);",
        "int32_t gnitz_catalog_get_col_name("
        "  void *handle, int64_t table_id, uint32_t col_idx,"
        "  uint8_t *out_buf, uint32_t max_len);",
        "uint64_t gnitz_catalog_get_max_flushed_lsn(const void *handle, int64_t table_id);",
        # DagEngine (lifecycle/registry removed — use CatalogEngine)
        # DagEngine cache
        "void gnitz_dag_invalidate(void *handle, int64_t view_id);",
        "void gnitz_dag_invalidate_all(void *handle);",
        "void gnitz_dag_invalidate_dep_map(void *handle);",
        # DagEngine evaluate + execute
        "int32_t gnitz_dag_evaluate(void *handle, int64_t source_id, void *delta);",
        "void *gnitz_dag_execute_epoch("
        "  void *handle, int64_t view_id, void *input, int64_t source_id);",
        "int32_t gnitz_dag_ingest(void *handle, int64_t table_id, void *batch);",
        "int32_t gnitz_dag_flush(void *handle, int64_t table_id);",
        # DagEngine metadata queries
        "int32_t gnitz_dag_get_dep_map(void *handle, int64_t *out_pairs, uint32_t max_pairs);",
        "int32_t gnitz_dag_get_shard_cols(void *handle, int64_t view_id, int32_t *out_cols, uint32_t max);",
        "int32_t gnitz_dag_get_exchange_info("
        "  void *handle, int64_t view_id, int32_t *out_cols, uint32_t max,"
        "  int32_t *out_trivial, int32_t *out_copart);",
        "int32_t gnitz_dag_get_source_ids(void *handle, int64_t view_id, int64_t *out_ids, uint32_t max);",
        "int32_t gnitz_dag_get_preloadable_views("
        "  void *handle, int64_t src_tid, int64_t *out_vids, int32_t *out_cols, uint32_t max);",
        "int32_t gnitz_dag_get_join_shard_cols("
        "  void *handle, int64_t view_id, int64_t source_id,"
        "  int32_t *out_cols, uint32_t max);",
        "int32_t gnitz_dag_get_exchange_schema("
        "  void *handle, int64_t view_id, void *out_schema);",
        "int32_t gnitz_dag_ensure_compiled(void *handle, int64_t view_id);",
        "int32_t gnitz_dag_view_needs_exchange(void *handle, int64_t view_id);",
        "void *gnitz_dag_execute_post_epoch("
        "  void *handle, int64_t view_id, void *input);",
        "int32_t gnitz_dag_plan_source_co_partitioned("
        "  void *handle, int64_t view_id, int64_t source_id);",
        "int32_t gnitz_dag_validate_graph("
        "  void *handle,"
        "  const int32_t *nodes_ptr, uint32_t nodes_count,"
        "  const int32_t *edges_ptr, uint32_t edges_count,"
        "  const int64_t *sources_ptr, uint32_t sources_count);",
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


def unpack_schema(buf):
    """Unpack a C SchemaDescriptor buffer into a TableSchema."""
    from gnitz.core.types import TableSchema, ColumnDefinition, FieldType
    from rpython.rlib.rarithmetic import intmask
    num_cols = intmask(rffi.cast(rffi.UINTP, buf)[0])
    pk_index = intmask(rffi.cast(rffi.UINTP, rffi.ptradd(buf, 4))[0])
    cols = []
    for ci in range(num_cols):
        base = 8 + ci * 4
        type_code = ord(buf[base])
        size = ord(buf[base + 1])
        nullable = ord(buf[base + 2]) != 0
        ft = FieldType(type_code, size, size)
        cols.append(ColumnDefinition(ft, is_nullable=nullable, name="c%d" % ci))
    return TableSchema(cols, pk_index=pk_index)


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

_op_union = rffi.llexternal(
    "gnitz_op_union",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.VOIDPP],
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
_ptable_get_child_dir = rffi.llexternal(
    "gnitz_ptable_get_child_dir", [rffi.VOIDP, rffi.CCHARP, rffi.UINT],
    rffi.INT, compilation_info=eci,
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
_batch_append_row_simple = rffi.llexternal(
    "gnitz_batch_append_row_simple",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG, rffi.LONGLONG, rffi.ULONGLONG,
     rffi.LONGLONGP, rffi.ULONGLONGP, rffi.CCHARPP, rffi.UINTP, rffi.UINT],
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
_batch_append_row_from_cursor = rffi.llexternal(
    "gnitz_batch_append_row_from_cursor",
    [rffi.VOIDP, rffi.VOIDP, rffi.LONGLONG],
    rffi.INT, compilation_info=eci,
)
_batch_append_row_from_table_found = rffi.llexternal(
    "gnitz_batch_append_row_from_table_found",
    [rffi.VOIDP, rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG, rffi.LONGLONG],
    rffi.INT, compilation_info=eci,
)
_batch_append_row_from_ptable_found = rffi.llexternal(
    "gnitz_batch_append_row_from_ptable_found",
    [rffi.VOIDP, rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG, rffi.LONGLONG],
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
_batch_project_index = rffi.llexternal(
    "gnitz_batch_project_index",
    [rffi.VOIDP, rffi.UINT, rffi.VOIDP],
    rffi.VOIDP, compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Partition routing
# ---------------------------------------------------------------------------

_partition_for_key = rffi.llexternal(
    "gnitz_partition_for_key",
    [rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.UINT,
    compilation_info=eci,
)

_worker_for_pk = rffi.llexternal(
    "gnitz_worker_for_pk",
    [rffi.ULONGLONG, rffi.ULONGLONG, rffi.UINT],
    rffi.UINT,
    compilation_info=eci,
)

_partition_router_create = rffi.llexternal(
    "gnitz_partition_router_create",
    [],
    rffi.VOIDP,
    compilation_info=eci,
)

_partition_router_free = rffi.llexternal(
    "gnitz_partition_router_free",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

_partition_router_worker_for_index_key = rffi.llexternal(
    "gnitz_partition_router_worker_for_index_key",
    [rffi.VOIDP, rffi.UINT, rffi.UINT, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_partition_router_record_routing = rffi.llexternal(
    "gnitz_partition_router_record_routing",
    [rffi.VOIDP, rffi.VOIDP, rffi.VOIDP, rffi.UINT, rffi.UINT, rffi.UINT],
    lltype.Void,
    compilation_info=eci,
)

# Exchange repartitioning
# ---------------------------------------------------------------------------

_repartition_batch = rffi.llexternal(
    "gnitz_repartition_batch",
    [rffi.VOIDP, rffi.UINTP, rffi.UINT, rffi.VOIDP, rffi.UINT, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_relay_scatter = rffi.llexternal(
    "gnitz_relay_scatter",
    [rffi.VOIDPP, rffi.UINT, rffi.UINTP, rffi.UINT, rffi.VOIDP, rffi.UINT, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

_multi_scatter = rffi.llexternal(
    "gnitz_multi_scatter",
    [rffi.VOIDP, rffi.UINTP, rffi.UINTP, rffi.UINT, rffi.VOIDP, rffi.UINT, rffi.VOIDPP],
    rffi.INT,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# IPC syscall wrappers (ipc_sys.rs)
# ---------------------------------------------------------------------------

_eventfd_create = rffi.llexternal(
    "gnitz_eventfd_create", [], rffi.INT,
    compilation_info=eci,
)

_eventfd_signal = rffi.llexternal(
    "gnitz_eventfd_signal", [rffi.INT], rffi.INT,
    compilation_info=eci,
)

_eventfd_wait = rffi.llexternal(
    "gnitz_eventfd_wait", [rffi.INT, rffi.INT], rffi.INT,
    compilation_info=eci,
)

_eventfd_wait_any = rffi.llexternal(
    "gnitz_eventfd_wait_any", [rffi.INTP, rffi.INT, rffi.INT], rffi.INT,
    compilation_info=eci,
)

_fallocate = rffi.llexternal(
    "gnitz_fallocate", [rffi.INT, rffi.LONGLONG], rffi.INT,
    compilation_info=eci,
)

_try_set_nocow = rffi.llexternal(
    "gnitz_try_set_nocow", [rffi.INT], rffi.INT,
    compilation_info=eci,
)

_fdatasync = rffi.llexternal(
    "gnitz_fdatasync", [rffi.INT], rffi.INT,
    compilation_info=eci,
)


def eventfd_create():
    """Create a non-blocking, close-on-exec eventfd. Returns fd (int)."""
    from rpython.rlib.rarithmetic import intmask
    from gnitz.core.errors import StorageError
    fd = _eventfd_create()
    if rffi.cast(lltype.Signed, fd) < 0:
        raise StorageError("eventfd_create failed")
    return intmask(fd)


def eventfd_signal(efd):
    """Signal an eventfd (increment counter by 1). Fire-and-forget."""
    _eventfd_signal(rffi.cast(rffi.INT, efd))


def eventfd_wait(efd, timeout_ms):
    """Wait for an eventfd. Returns >0 if ready, 0 on timeout, <0 on error."""
    from rpython.rlib.rarithmetic import intmask
    return intmask(_eventfd_wait(
        rffi.cast(rffi.INT, efd), rffi.cast(rffi.INT, timeout_ms)))


def eventfd_wait_any(efd_list, timeout_ms):
    """Wait for any of the eventfds. All ready fds are drained."""
    from rpython.rlib.rarithmetic import intmask
    count = len(efd_list)
    if count == 0:
        return 0
    c_fds = lltype.malloc(rffi.INTP.TO, count, flavor='raw')
    try:
        for i in range(count):
            c_fds[i] = rffi.cast(rffi.INT, efd_list[i])
        result = _eventfd_wait_any(
            c_fds, rffi.cast(rffi.INT, count),
            rffi.cast(rffi.INT, timeout_ms))
    finally:
        lltype.free(c_fds, flavor='raw')
    return intmask(result)


def try_set_nocow(fd):
    """Set FS_NOCOW_FL on fd. Silently ignored on non-btrfs."""
    _try_set_nocow(rffi.cast(rffi.INT, fd))


def fallocate_c(fd, length):
    """Pre-allocate blocks for fd. Raises StorageError on failure."""
    from gnitz.core.errors import StorageError
    res = _fallocate(rffi.cast(rffi.INT, fd), rffi.cast(rffi.LONGLONG, length))
    if rffi.cast(lltype.Signed, res) < 0:
        raise StorageError("fallocate failed")


# ---------------------------------------------------------------------------
# IPC atomics + SAL/W2M transport (ipc.rs)
# ---------------------------------------------------------------------------

_atomic_load_u64 = rffi.llexternal(
    "gnitz_atomic_load_u64",
    [rffi.CCHARP],
    rffi.ULONGLONG,
    compilation_info=eci,
    _nowrapper=True,
)

_atomic_store_u64 = rffi.llexternal(
    "gnitz_atomic_store_u64",
    [rffi.CCHARP, rffi.ULONGLONG],
    lltype.Void,
    compilation_info=eci,
    _nowrapper=True,
)

_w2m_write = rffi.llexternal(
    "gnitz_w2m_write",
    [rffi.CCHARP, rffi.CCHARP, rffi.UINT, rffi.ULONGLONG],
    rffi.LONGLONG,
    compilation_info=eci,
)

_w2m_read = rffi.llexternal(
    "gnitz_w2m_read",
    [rffi.CCHARP, rffi.ULONGLONG, rffi.CCHARPP, rffi.UINTP],
    rffi.ULONGLONG,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# IPC wire protocol (ipc.rs)
# ---------------------------------------------------------------------------

_ipc_encode_wire = rffi.llexternal(
    "gnitz_ipc_encode_wire",
    [rffi.ULONGLONG, rffi.ULONGLONG, rffi.ULONGLONG,   # target, client, flags
     rffi.ULONGLONG, rffi.ULONGLONG, rffi.ULONGLONG,   # seek_pk_lo/hi, seek_col
     rffi.UINT,                                         # status
     rffi.CCHARP, rffi.UINT,                            # error_msg ptr/len
     rffi.VOIDP,                                        # schema_desc
     rffi.CCHARPP, rffi.UINTP,                          # col_names ptr/len arrays
     rffi.UINT,                                         # num_col_names
     rffi.VOIDP,                                        # batch handle
     rffi.CCHARPP, rffi.UINTP],                         # out_ptr, out_len
    rffi.INT,
    compilation_info=eci,
)

_ipc_wire_free = rffi.llexternal(
    "gnitz_ipc_wire_free",
    [rffi.CCHARP, rffi.UINT],
    lltype.Void,
    compilation_info=eci,
)

_ipc_decode_wire = rffi.llexternal(
    "gnitz_ipc_decode_wire",
    [rffi.CCHARP, rffi.UINT,                           # data_ptr, data_size
     rffi.UINTP, rffi.ULONGLONGP,                      # out_status, out_client_id
     rffi.ULONGLONGP, rffi.ULONGLONGP,                 # out_target_id, out_flags
     rffi.ULONGLONGP, rffi.ULONGLONGP,                 # out_seek_pk_lo/hi
     rffi.ULONGLONGP,                                   # out_seek_col_idx
     rffi.CCHARPP, rffi.UINTP,                          # out_error_msg ptr/len
     rffi.VOIDP, rffi.INTP,                             # out_schema_desc, out_has_schema
     rffi.VOIDPP, rffi.UINTP],                          # out_batch_handle, out_num_col_names
    rffi.VOIDP,
    compilation_info=eci,
)

_ipc_decode_result_col_name = rffi.llexternal(
    "gnitz_ipc_decode_result_col_name",
    [rffi.VOIDP, rffi.UINT, rffi.CCHARPP, rffi.UINTP],
    rffi.INT,
    compilation_info=eci,
)

_ipc_decode_result_take_batch = rffi.llexternal(
    "gnitz_ipc_decode_result_take_batch",
    [rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_ipc_decode_result_free = rffi.llexternal(
    "gnitz_ipc_decode_result_free",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# CatalogEngine FFI
# ---------------------------------------------------------------------------

_catalog_open = rffi.llexternal(
    "gnitz_catalog_open",
    [rffi.VOIDP, rffi.UINT],
    rffi.VOIDP,
    compilation_info=eci,
)

_catalog_close = rffi.llexternal(
    "gnitz_catalog_close", [rffi.VOIDP], lltype.Void,
    compilation_info=eci,
)

_catalog_last_error = rffi.llexternal(
    "gnitz_catalog_last_error",
    [rffi.UINTP],
    rffi.VOIDP,
    compilation_info=eci,
)

_catalog_create_schema = rffi.llexternal(
    "gnitz_catalog_create_schema",
    [rffi.VOIDP, rffi.VOIDP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_catalog_create_table = rffi.llexternal(
    "gnitz_catalog_create_table",
    [rffi.VOIDP, rffi.VOIDP, rffi.UINT,
     rffi.VOIDP, rffi.UINT, rffi.UINT, rffi.UINT, rffi.INT],
    rffi.LONGLONG,
    compilation_info=eci,
)

_catalog_create_view = rffi.llexternal(
    "gnitz_catalog_create_view",
    [rffi.VOIDP,
     rffi.VOIDP, rffi.UINT,    # name
     rffi.VOIDP, rffi.UINT,    # sql_def
     rffi.INTP, rffi.UINT,     # nodes
     rffi.INTP, rffi.UINT,     # edges
     rffi.LONGLONGP, rffi.UINT,  # sources
     rffi.LONGLONGP, rffi.UINT,  # params
     rffi.INTP, rffi.UINT,     # group_cols
     rffi.VOIDP, rffi.UINT, rffi.UINT,  # output_col_defs (ptr, byte_len, count)
     rffi.LONGLONGP, rffi.UINT],  # deps
    rffi.LONGLONG,
    compilation_info=eci,
)

_catalog_create_index = rffi.llexternal(
    "gnitz_catalog_create_index",
    [rffi.VOIDP, rffi.VOIDP, rffi.UINT, rffi.VOIDP, rffi.UINT, rffi.INT],
    rffi.LONGLONG,
    compilation_info=eci,
)

_catalog_drop_schema = rffi.llexternal(
    "gnitz_catalog_drop_schema",
    [rffi.VOIDP, rffi.VOIDP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_catalog_drop_table = rffi.llexternal(
    "gnitz_catalog_drop_table",
    [rffi.VOIDP, rffi.VOIDP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_catalog_drop_view = rffi.llexternal(
    "gnitz_catalog_drop_view",
    [rffi.VOIDP, rffi.VOIDP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_catalog_drop_index = rffi.llexternal(
    "gnitz_catalog_drop_index",
    [rffi.VOIDP, rffi.VOIDP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_catalog_allocate_schema_id = rffi.llexternal(
    "gnitz_catalog_allocate_schema_id",
    [rffi.VOIDP], rffi.LONGLONG,
    compilation_info=eci,
)

_catalog_allocate_table_id = rffi.llexternal(
    "gnitz_catalog_allocate_table_id",
    [rffi.VOIDP], rffi.LONGLONG,
    compilation_info=eci,
)

_catalog_allocate_index_id = rffi.llexternal(
    "gnitz_catalog_allocate_index_id",
    [rffi.VOIDP], rffi.LONGLONG,
    compilation_info=eci,
)

_catalog_advance_sequence = rffi.llexternal(
    "gnitz_catalog_advance_sequence",
    [rffi.VOIDP, rffi.LONGLONG, rffi.LONGLONG, rffi.LONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_catalog_has_id = rffi.llexternal(
    "gnitz_catalog_has_id",
    [rffi.VOIDP, rffi.LONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_catalog_get_schema_desc = rffi.llexternal(
    "gnitz_catalog_get_schema_desc",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_catalog_get_depth = rffi.llexternal(
    "gnitz_catalog_get_depth",
    [rffi.VOIDP, rffi.LONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_catalog_is_unique_pk = rffi.llexternal(
    "gnitz_catalog_is_unique_pk",
    [rffi.VOIDP, rffi.LONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_catalog_ingest = rffi.llexternal(
    "gnitz_catalog_ingest",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_catalog_ingest_effective = rffi.llexternal(
    "gnitz_catalog_ingest_effective",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_catalog_push_and_evaluate = rffi.llexternal(
    "gnitz_catalog_push_and_evaluate",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_catalog_scan = rffi.llexternal(
    "gnitz_catalog_scan",
    [rffi.VOIDP, rffi.LONGLONG],
    rffi.VOIDP,
    compilation_info=eci,
)

_catalog_seek = rffi.llexternal(
    "gnitz_catalog_seek",
    [rffi.VOIDP, rffi.LONGLONG, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.VOIDP,
    compilation_info=eci,
)

_catalog_seek_by_index = rffi.llexternal(
    "gnitz_catalog_seek_by_index",
    [rffi.VOIDP, rffi.LONGLONG, rffi.UINT, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.VOIDP,
    compilation_info=eci,
)

_catalog_flush = rffi.llexternal(
    "gnitz_catalog_flush",
    [rffi.VOIDP, rffi.LONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_catalog_validate_unique_indices = rffi.llexternal(
    "gnitz_catalog_validate_unique_indices",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_catalog_validate_fk_inline = rffi.llexternal(
    "gnitz_catalog_validate_fk_inline",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_catalog_ddl_sync = rffi.llexternal(
    "gnitz_catalog_ddl_sync",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_catalog_raw_store_ingest = rffi.llexternal(
    "gnitz_catalog_raw_store_ingest",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_catalog_set_active_partitions = rffi.llexternal(
    "gnitz_catalog_set_active_partitions",
    [rffi.VOIDP, rffi.UINT, rffi.UINT],
    lltype.Void,
    compilation_info=eci,
)

_catalog_close_user_table_partitions = rffi.llexternal(
    "gnitz_catalog_close_user_table_partitions",
    [rffi.VOIDP], lltype.Void,
    compilation_info=eci,
)

_catalog_trim_worker_partitions = rffi.llexternal(
    "gnitz_catalog_trim_worker_partitions",
    [rffi.VOIDP, rffi.UINT, rffi.UINT],
    lltype.Void,
    compilation_info=eci,
)

_catalog_disable_user_table_wal = rffi.llexternal(
    "gnitz_catalog_disable_user_table_wal",
    [rffi.VOIDP], lltype.Void,
    compilation_info=eci,
)

_catalog_invalidate_all_plans = rffi.llexternal(
    "gnitz_catalog_invalidate_all_plans",
    [rffi.VOIDP], lltype.Void,
    compilation_info=eci,
)

_catalog_get_fk_count = rffi.llexternal(
    "gnitz_catalog_get_fk_count",
    [rffi.VOIDP, rffi.LONGLONG],
    rffi.UINT,
    compilation_info=eci,
)

_catalog_get_fk_constraint = rffi.llexternal(
    "gnitz_catalog_get_fk_constraint",
    [rffi.VOIDP, rffi.LONGLONG, rffi.UINT,
     rffi.UINTP, rffi.LONGLONGP, rffi.UCHARP],
    rffi.INT,
    compilation_info=eci,
)

_catalog_get_index_circuit_count = rffi.llexternal(
    "gnitz_catalog_get_index_circuit_count",
    [rffi.VOIDP, rffi.LONGLONG],
    rffi.UINT,
    compilation_info=eci,
)

_catalog_get_index_circuit_info = rffi.llexternal(
    "gnitz_catalog_get_index_circuit_info",
    [rffi.VOIDP, rffi.LONGLONG, rffi.UINT,
     rffi.UINTP, rffi.INTP, rffi.UCHARP],
    rffi.INT,
    compilation_info=eci,
)

_catalog_get_index_store = rffi.llexternal(
    "gnitz_catalog_get_index_store",
    [rffi.VOIDP, rffi.LONGLONG, rffi.UINT],
    rffi.VOIDP,
    compilation_info=eci,
)

_catalog_get_ptable_handle = rffi.llexternal(
    "gnitz_catalog_get_ptable_handle",
    [rffi.VOIDP, rffi.LONGLONG],
    rffi.VOIDP,
    compilation_info=eci,
)

_catalog_get_dag_handle = rffi.llexternal(
    "gnitz_catalog_get_dag_handle",
    [rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_catalog_iter_user_table_ids = rffi.llexternal(
    "gnitz_catalog_iter_user_table_ids",
    [rffi.VOIDP, rffi.LONGLONGP, rffi.UINT],
    rffi.UINT,
    compilation_info=eci,
)

_catalog_get_col_name = rffi.llexternal(
    "gnitz_catalog_get_col_name",
    [rffi.VOIDP, rffi.LONGLONG, rffi.UINT, rffi.VOIDP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_catalog_get_max_flushed_lsn = rffi.llexternal(
    "gnitz_catalog_get_max_flushed_lsn",
    [rffi.VOIDP, rffi.LONGLONG],
    rffi.ULONGLONG,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# DagEngine FFI
# ---------------------------------------------------------------------------

# Removed: _dag_create, _dag_destroy, _dag_set_sys_tables,
# _dag_register_table, _dag_unregister_table, _dag_set_depth,
# _dag_add_index_circuit, _dag_remove_index_circuit
# — CatalogEngine owns DagEngine lifecycle.

_dag_invalidate = rffi.llexternal(
    "gnitz_dag_invalidate",
    [rffi.VOIDP, rffi.LONGLONG],
    lltype.Void,
    compilation_info=eci,
)

_dag_invalidate_all = rffi.llexternal(
    "gnitz_dag_invalidate_all", [rffi.VOIDP], lltype.Void,
    compilation_info=eci,
)

_dag_invalidate_dep_map = rffi.llexternal(
    "gnitz_dag_invalidate_dep_map", [rffi.VOIDP], lltype.Void,
    compilation_info=eci,
)

_dag_evaluate = rffi.llexternal(
    "gnitz_dag_evaluate",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_dag_execute_epoch = rffi.llexternal(
    "gnitz_dag_execute_epoch",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP, rffi.LONGLONG],
    rffi.VOIDP,
    compilation_info=eci,
)

_dag_ingest = rffi.llexternal(
    "gnitz_dag_ingest",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_dag_flush = rffi.llexternal(
    "gnitz_dag_flush",
    [rffi.VOIDP, rffi.LONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_dag_get_dep_map = rffi.llexternal(
    "gnitz_dag_get_dep_map",
    [rffi.VOIDP, rffi.LONGLONGP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_dag_get_shard_cols = rffi.llexternal(
    "gnitz_dag_get_shard_cols",
    [rffi.VOIDP, rffi.LONGLONG, rffi.INTP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_dag_get_exchange_info = rffi.llexternal(
    "gnitz_dag_get_exchange_info",
    [rffi.VOIDP, rffi.LONGLONG, rffi.INTP, rffi.UINT, rffi.INTP, rffi.INTP],
    rffi.INT,
    compilation_info=eci,
)

_dag_get_source_ids = rffi.llexternal(
    "gnitz_dag_get_source_ids",
    [rffi.VOIDP, rffi.LONGLONG, rffi.LONGLONGP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_dag_get_preloadable_views = rffi.llexternal(
    "gnitz_dag_get_preloadable_views",
    [rffi.VOIDP, rffi.LONGLONG, rffi.LONGLONGP, rffi.INTP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_dag_get_join_shard_cols = rffi.llexternal(
    "gnitz_dag_get_join_shard_cols",
    [rffi.VOIDP, rffi.LONGLONG, rffi.LONGLONG, rffi.INTP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_dag_get_exchange_schema = rffi.llexternal(
    "gnitz_dag_get_exchange_schema",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_dag_ensure_compiled = rffi.llexternal(
    "gnitz_dag_ensure_compiled",
    [rffi.VOIDP, rffi.LONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_dag_view_needs_exchange = rffi.llexternal(
    "gnitz_dag_view_needs_exchange",
    [rffi.VOIDP, rffi.LONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_dag_execute_post_epoch = rffi.llexternal(
    "gnitz_dag_execute_post_epoch",
    [rffi.VOIDP, rffi.LONGLONG, rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_dag_plan_source_co_partitioned = rffi.llexternal(
    "gnitz_dag_plan_source_co_partitioned",
    [rffi.VOIDP, rffi.LONGLONG, rffi.LONGLONG],
    rffi.INT,
    compilation_info=eci,
)
