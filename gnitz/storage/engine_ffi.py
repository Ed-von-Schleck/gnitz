# gnitz/storage/engine_ffi.py
#
# RPython FFI bindings for libgnitz_engine (Rust staticlib).
# Provides WAL encode/decode, xxh3 checksum, cursor, table, and batch packing.

import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo

_lib_dir = os.environ.get("GNITZ_ENGINE_LIB", "")
_lib_path = os.path.join(_lib_dir, "libgnitz_engine.a") if _lib_dir else ""

eci = ExternalCompilationInfo(
    pre_include_bits=[
        "#include <stdint.h>",
        # xxh3 checksum + hash
        "uint64_t gnitz_xxh3_checksum(const uint8_t *data, int64_t len);",
        "uint64_t gnitz_xxh3_hash_u128(uint64_t lo, uint64_t hi, uint64_t seed_lo, uint64_t seed_hi);",
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
        # batch descriptor struct (matches Rust GnitzBatchDesc)
        "typedef struct {"
        "  uint8_t *pk_lo; uint8_t *pk_hi;"
        "  uint8_t *weight; uint8_t *null_bm;"
        "  void **col_ptrs; uint64_t *col_sizes;"
        "  uint32_t num_payload_cols;"
        "  uint8_t *blob_ptr; uint64_t blob_len;"
        "  uint32_t count;"
        "} GnitzBatchDesc;",
        # cursor
        "void *gnitz_cursor_create("
        "  void **borrowed_handles, uint32_t num_borrowed,"
        "  GnitzBatchDesc *batch_descs, uint32_t num_batches,"
        "  void *schema_desc);",
        "void gnitz_cursor_seek(void *cursor, uint64_t key_lo, uint64_t key_hi);",
        "int32_t gnitz_cursor_advance(void *cursor);",
        "int32_t gnitz_cursor_is_valid(void *cursor);",
        "uint64_t gnitz_cursor_key_lo(void *cursor);",
        "uint64_t gnitz_cursor_key_hi(void *cursor);",
        "int64_t gnitz_cursor_weight(void *cursor);",
        "uint64_t gnitz_cursor_null_word(void *cursor);",
        "uint8_t *gnitz_cursor_col_ptr(void *cursor, uint32_t col_idx, uint32_t col_size);",
        "uint8_t *gnitz_cursor_blob_ptr(void *cursor);",
        "int64_t gnitz_cursor_blob_len(void *cursor);",
        "void gnitz_cursor_close(void *cursor);",
        # table
        "void *gnitz_table_create_ephemeral("
        "  char *directory, char *name, void *schema_desc,"
        "  uint32_t table_id, uint64_t memtable_arena);",
        "void *gnitz_table_create_persistent("
        "  char *directory, char *name, void *schema_desc,"
        "  uint32_t table_id, uint64_t memtable_arena);",
        "void gnitz_table_close(void *handle);",
        "int32_t gnitz_table_ingest_batch(void *handle, GnitzBatchDesc *desc, uint64_t lsn);",
        "int32_t gnitz_table_ingest_batch_memonly(void *handle, GnitzBatchDesc *desc);",
        "int32_t gnitz_table_ingest_one("
        "  void *handle, uint64_t key_lo, uint64_t key_hi, int64_t weight,"
        "  uint64_t null_word, void **col_ptrs, uint32_t num_payload_cols,"
        "  uint8_t *source_blob, uint64_t source_blob_len);",
        "int32_t gnitz_table_flush(void *handle);",
        "void *gnitz_table_create_cursor(void *handle);",
        "int32_t gnitz_table_has_pk(void *handle, uint64_t key_lo, uint64_t key_hi);",
        "int64_t gnitz_table_get_weight("
        "  void *handle, uint64_t key_lo, uint64_t key_hi,"
        "  uint64_t null_word, void **col_ptrs, uint64_t *col_sizes,"
        "  uint32_t num_payload_cols, uint8_t *blob_ptr, uint64_t blob_len);",
        "int32_t gnitz_table_retract_pk_row("
        "  const void *handle, uint64_t key_lo, uint64_t key_hi,"
        "  const uint8_t **out_col_ptrs, uint32_t n_payload,"
        "  uint64_t *out_null_word, const uint8_t **out_blob_ptr,"
        "  uint64_t *out_blob_len, int64_t *out_weight,"
        "  void **out_cleanup);",
        "void gnitz_retract_row_free(void *cleanup);",
        "void *gnitz_table_create_child(void *handle, char *name, void *schema_desc);",
        "int32_t gnitz_table_should_flush(void *handle);",
        "int32_t gnitz_table_is_empty(void *handle);",
        "int32_t gnitz_table_compact_if_needed(void *handle);",
        "uint64_t gnitz_table_current_lsn(void *handle);",
        "void gnitz_table_set_lsn(void *handle, uint64_t lsn);",
    ],
    link_files=[_lib_path] if _lib_path else [],
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
# Schema packing (shared between table, cursor, and batch desc)
# ---------------------------------------------------------------------------

# SchemaDescriptor layout: 4 (num_columns) + 4 (pk_index) + 64*4 (columns) = 264 bytes
SCHEMA_DESC_SIZE = 264

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
# Cursor
# ---------------------------------------------------------------------------

_cursor_create = rffi.llexternal(
    "gnitz_cursor_create",
    [rffi.VOIDPP, rffi.UINT,
     rffi.VOIDP, rffi.UINT,
     rffi.VOIDP],
    rffi.VOIDP,
    compilation_info=eci,
)

_cursor_seek = rffi.llexternal(
    "gnitz_cursor_seek",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    lltype.Void,
    compilation_info=eci,
)

_cursor_advance = rffi.llexternal(
    "gnitz_cursor_advance",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_cursor_is_valid = rffi.llexternal(
    "gnitz_cursor_is_valid",
    [rffi.VOIDP],
    rffi.INT,
    compilation_info=eci,
)

_cursor_key_lo = rffi.llexternal(
    "gnitz_cursor_key_lo",
    [rffi.VOIDP],
    rffi.ULONGLONG,
    compilation_info=eci,
)

_cursor_key_hi = rffi.llexternal(
    "gnitz_cursor_key_hi",
    [rffi.VOIDP],
    rffi.ULONGLONG,
    compilation_info=eci,
)

_cursor_weight = rffi.llexternal(
    "gnitz_cursor_weight",
    [rffi.VOIDP],
    rffi.LONGLONG,
    compilation_info=eci,
)

_cursor_null_word = rffi.llexternal(
    "gnitz_cursor_null_word",
    [rffi.VOIDP],
    rffi.ULONGLONG,
    compilation_info=eci,
)

_cursor_col_ptr = rffi.llexternal(
    "gnitz_cursor_col_ptr",
    [rffi.VOIDP, rffi.UINT, rffi.UINT],
    rffi.CCHARP,
    compilation_info=eci,
)

_cursor_blob_ptr = rffi.llexternal(
    "gnitz_cursor_blob_ptr",
    [rffi.VOIDP],
    rffi.CCHARP,
    compilation_info=eci,
)

_cursor_blob_len = rffi.llexternal(
    "gnitz_cursor_blob_len",
    [rffi.VOIDP],
    rffi.LONGLONG,
    compilation_info=eci,
)

_cursor_close = rffi.llexternal(
    "gnitz_cursor_close",
    [rffi.VOIDP],
    lltype.Void,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Batch descriptor
# ---------------------------------------------------------------------------

# GnitzBatchDesc: 80 bytes on 64-bit (must match Rust #[repr(C)])
BATCH_DESC_SIZE = 80

# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------

_table_create_ephemeral = rffi.llexternal(
    "gnitz_table_create_ephemeral",
    [rffi.CCHARP, rffi.CCHARP, rffi.VOIDP, rffi.UINT, rffi.ULONGLONG],
    rffi.VOIDP, compilation_info=eci,
)

_table_create_persistent = rffi.llexternal(
    "gnitz_table_create_persistent",
    [rffi.CCHARP, rffi.CCHARP, rffi.VOIDP, rffi.UINT, rffi.ULONGLONG],
    rffi.VOIDP, compilation_info=eci,
)

_table_close = rffi.llexternal(
    "gnitz_table_close", [rffi.VOIDP], lltype.Void, compilation_info=eci,
)

_table_ingest_batch = rffi.llexternal(
    "gnitz_table_ingest_batch",
    [rffi.VOIDP, rffi.VOIDP, rffi.ULONGLONG],
    rffi.INT, compilation_info=eci,
)

_table_ingest_batch_memonly = rffi.llexternal(
    "gnitz_table_ingest_batch_memonly",
    [rffi.VOIDP, rffi.VOIDP],
    rffi.INT, compilation_info=eci,
)

_table_ingest_one = rffi.llexternal(
    "gnitz_table_ingest_one",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG, rffi.LONGLONG,
     rffi.ULONGLONG, rffi.VOIDPP, rffi.UINT,
     rffi.CCHARP, rffi.ULONGLONG],
    rffi.INT, compilation_info=eci,
)

_table_flush = rffi.llexternal(
    "gnitz_table_flush", [rffi.VOIDP], rffi.INT, compilation_info=eci,
)

_table_create_cursor = rffi.llexternal(
    "gnitz_table_create_cursor", [rffi.VOIDP], rffi.VOIDP, compilation_info=eci,
)

_table_has_pk = rffi.llexternal(
    "gnitz_table_has_pk",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.INT, compilation_info=eci,
)

_table_get_weight = rffi.llexternal(
    "gnitz_table_get_weight",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG,
     rffi.ULONGLONG, rffi.VOIDPP, rffi.ULONGLONGP, rffi.UINT,
     rffi.CCHARP, rffi.ULONGLONG],
    rffi.LONGLONG, compilation_info=eci,
)

_table_retract_pk_row = rffi.llexternal(
    "gnitz_table_retract_pk_row",
    [rffi.VOIDP, rffi.ULONGLONG, rffi.ULONGLONG,
     rffi.VOIDPP, rffi.UINT,
     rffi.ULONGLONGP, rffi.VOIDPP,
     rffi.ULONGLONGP, rffi.LONGLONGP,
     rffi.VOIDPP],
    rffi.INT, compilation_info=eci,
)

_retract_row_free = rffi.llexternal(
    "gnitz_retract_row_free",
    [rffi.VOIDP],
    lltype.Void, compilation_info=eci,
)

_table_create_child = rffi.llexternal(
    "gnitz_table_create_child",
    [rffi.VOIDP, rffi.CCHARP, rffi.VOIDP],
    rffi.VOIDP, compilation_info=eci,
)

_table_should_flush = rffi.llexternal(
    "gnitz_table_should_flush", [rffi.VOIDP], rffi.INT, compilation_info=eci,
)

_table_is_empty = rffi.llexternal(
    "gnitz_table_is_empty", [rffi.VOIDP], rffi.INT, compilation_info=eci,
)

_table_compact_if_needed = rffi.llexternal(
    "gnitz_table_compact_if_needed", [rffi.VOIDP], rffi.INT, compilation_info=eci,
)

_table_current_lsn = rffi.llexternal(
    "gnitz_table_current_lsn", [rffi.VOIDP], rffi.ULONGLONG, compilation_info=eci,
)

_table_set_lsn = rffi.llexternal(
    "gnitz_table_set_lsn", [rffi.VOIDP, rffi.ULONGLONG], lltype.Void, compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Batch descriptor packing (shared between table and ephemeral_table)
# ---------------------------------------------------------------------------

def pack_batch_desc(base, batch, schema, num_payload, col_ptrs, col_sizes):
    """Pack an ArenaZSetBatch into a GnitzBatchDesc C struct."""
    pi = 0
    pk_index = schema.pk_index
    num_cols = len(schema.columns)
    ci = 0
    while ci < num_cols:
        if ci != pk_index:
            col_ptrs[pi] = rffi.cast(rffi.VOIDP, batch.col_bufs[ci].base_ptr)
            col_sizes[pi] = rffi.cast(rffi.ULONGLONG, batch.col_bufs[ci].offset)
            pi += 1
        ci += 1

    off = 0
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, batch.pk_lo_buf.base_ptr)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, batch.pk_hi_buf.base_ptr)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, batch.weight_buf.base_ptr)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, batch.null_buf.base_ptr)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, col_ptrs)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, col_sizes)
    off += 8
    rffi.cast(rffi.UINTP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.UINT, num_payload)
    off += 4
    off += 4  # padding
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, batch.blob_arena.base_ptr)
    off += 8
    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.ULONGLONG, batch.blob_arena.offset)
    off += 8
    rffi.cast(rffi.UINTP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.UINT, batch.length())
