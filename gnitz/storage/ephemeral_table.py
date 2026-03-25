# gnitz/storage/ephemeral_table.py
#
# Thin FFI wrapper around Rust RustTable (ephemeral mode).

import os
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, errors
from gnitz.core.store import ZSetStore
from gnitz.core.batch import pk_eq
from gnitz.storage import (
    engine_ffi,
    comparator as storage_comparator,
    cursor,
)

NULL_HANDLE = lltype.nullptr(rffi.VOIDP.TO)


class EphemeralTable(ZSetStore):

    _immutable_fields_ = ["schema", "table_id", "directory", "name"]

    def __init__(self, directory, name, schema, table_id=1,
                 memtable_arena_size=1 * 1024 * 1024, validate_checksums=False):
        self.schema = schema
        self.table_id = table_id
        self.directory = directory
        self.name = name
        self.validate_checksums = validate_checksums
        self.is_closed = False
        self.current_lsn = r_uint64(0)
        self._handle = NULL_HANDLE
        self._retract_accessor = storage_comparator.RetractRowAccessor(schema)

        if len(directory) == 0:
            return

        schema_buf = engine_ffi.pack_schema(schema)
        try:
            with rffi.scoped_str2charp(directory) as dir_p:
                with rffi.scoped_str2charp(name) as name_p:
                    handle = engine_ffi._table_create_ephemeral(
                        dir_p, name_p,
                        rffi.cast(rffi.VOIDP, schema_buf),
                        rffi.cast(rffi.UINT, table_id),
                        rffi.cast(rffi.ULONGLONG, memtable_arena_size),
                    )
            if not handle:
                raise errors.StorageError("table_create_ephemeral failed")
            self._handle = handle
        finally:
            lltype.free(schema_buf, flavor="raw")

    def get_schema(self):
        return self.schema

    def _build_cursor(self):
        handle = engine_ffi._table_create_cursor(self._handle)
        if not handle:
            raise errors.StorageError("table_create_cursor failed")
        return cursor.RustUnifiedCursorFromHandle(self.schema, handle)

    def compact_if_needed(self):
        engine_ffi._table_compact_if_needed(self._handle)

    def create_cursor(self):
        self.compact_if_needed()
        return self._build_cursor()

    def create_child(self, name, child_schema):
        schema_buf = engine_ffi.pack_schema(child_schema)
        try:
            with rffi.scoped_str2charp(name) as name_p:
                child_handle = engine_ffi._table_create_child(
                    self._handle, name_p,
                    rffi.cast(rffi.VOIDP, schema_buf),
                )
            if not child_handle:
                raise errors.StorageError("table_create_child failed")
        finally:
            lltype.free(schema_buf, flavor="raw")

        child = EphemeralTable("", name, child_schema, self.table_id)
        child.directory = self.directory + "/scratch_" + name
        child._handle = child_handle
        return child

    def has_pk(self, key_lo, key_hi):
        return bool(intmask(engine_ffi._table_has_pk(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )))

    def retract_pk(self, key_lo, key_hi, out_batch):
        num_payload = len(self.schema.columns) - 1
        out_col_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
        out_null = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        out_blob_ptr = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
        out_blob_len = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        out_weight = lltype.malloc(rffi.LONGLONGP.TO, 1, flavor="raw")
        out_cleanup = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
        try:
            out_cleanup[0] = NULL_HANDLE
            found = engine_ffi._table_retract_pk_row(
                self._handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                out_col_ptrs,
                rffi.cast(rffi.UINT, num_payload),
                out_null, out_blob_ptr, out_blob_len,
                out_weight, out_cleanup,
            )
            if intmask(found) == 0:
                return False
            null_word = r_uint64(out_null[0])
            blob_ptr = rffi.cast(rffi.CCHARP, out_blob_ptr[0])
            self._retract_accessor.set_ptrs(out_col_ptrs, null_word, blob_ptr)
            out_batch.append_from_accessor(
                key_lo, key_hi, r_int64(-1), self._retract_accessor)
        finally:
            cleanup = out_cleanup[0]
            if cleanup:
                engine_ffi._retract_row_free(cleanup)
            lltype.free(out_col_ptrs, flavor="raw")
            lltype.free(out_null, flavor="raw")
            lltype.free(out_blob_ptr, flavor="raw")
            lltype.free(out_blob_len, flavor="raw")
            lltype.free(out_weight, flavor="raw")
            lltype.free(out_cleanup, flavor="raw")
        return True

    def get_weight(self, key_lo, key_hi, accessor):
        num_payload = len(self.schema.columns) - 1
        col_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
        col_sizes = lltype.malloc(rffi.ULONGLONGP.TO, num_payload, flavor="raw")
        try:
            pi = 0
            pk_index = self.schema.pk_index
            ci = 0
            num_cols = len(self.schema.columns)
            while ci < num_cols:
                if ci != pk_index:
                    col_ptrs[pi] = rffi.cast(rffi.VOIDP, accessor.get_col_ptr(ci))
                    col_sizes[pi] = rffi.cast(
                        rffi.ULONGLONG,
                        self.schema.columns[ci].field_type.size,
                    )
                    pi += 1
                ci += 1

            null_word = r_uint64(0)
            for i in range(num_cols):
                if i == pk_index:
                    continue
                if accessor.is_null(i):
                    payload_idx = i if i < pk_index else i - 1
                    null_word |= r_uint64(1) << payload_idx

            blob_ptr = lltype.nullptr(rffi.CCHARP.TO)
            blob_len = 0
            from gnitz.core.batch import ColumnarBatchAccessor
            if isinstance(accessor, ColumnarBatchAccessor):
                if accessor._batch is not None:
                    blob_ptr = accessor._batch.blob_arena.base_ptr
                    blob_len = accessor._batch.blob_arena.offset

            w = engine_ffi._table_get_weight(
                self._handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                rffi.cast(rffi.ULONGLONG, null_word),
                col_ptrs, col_sizes,
                rffi.cast(rffi.UINT, num_payload),
                rffi.cast(rffi.CCHARP, blob_ptr),
                rffi.cast(rffi.ULONGLONG, blob_len),
            )
            return rffi.cast(lltype.Signed, w)
        finally:
            lltype.free(col_ptrs, flavor="raw")
            lltype.free(col_sizes, flavor="raw")

    def ingest_batch(self, batch):
        if self.is_closed:
            raise errors.StorageError("Table is closed")
        if batch.length() == 0:
            return
        num_payload = len(self.schema.columns) - 1
        desc_buf = lltype.malloc(
            rffi.CCHARP.TO, engine_ffi.BATCH_DESC_SIZE, flavor="raw"
        )
        col_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
        col_sizes = lltype.malloc(rffi.ULONGLONGP.TO, num_payload, flavor="raw")
        try:
            engine_ffi.pack_batch_desc(desc_buf, batch, self.schema, num_payload, col_ptrs, col_sizes)
            rc = engine_ffi._table_ingest_batch_memonly(
                self._handle, rffi.cast(rffi.VOIDP, desc_buf),
            )
            if intmask(rc) < 0:
                raise errors.StorageError("table_ingest_batch failed (%d)" % intmask(rc))
        finally:
            lltype.free(col_ptrs, flavor="raw")
            lltype.free(col_sizes, flavor="raw")
            lltype.free(desc_buf, flavor="raw")

    def ingest_one(self, key_lo, key_hi, weight, accessor):
        if self.is_closed:
            raise errors.StorageError("Table is closed")
        num_payload = len(self.schema.columns) - 1
        col_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
        try:
            pi = 0
            pk_index = self.schema.pk_index
            ci = 0
            num_cols = len(self.schema.columns)
            while ci < num_cols:
                if ci != pk_index:
                    col_ptrs[pi] = rffi.cast(rffi.VOIDP, accessor.get_col_ptr(ci))
                    pi += 1
                ci += 1

            null_word = r_uint64(0)
            for i in range(num_cols):
                if i == pk_index:
                    continue
                if accessor.is_null(i):
                    payload_idx = i if i < pk_index else i - 1
                    null_word |= r_uint64(1) << payload_idx

            blob_ptr = lltype.nullptr(rffi.CCHARP.TO)
            blob_len = 0
            from gnitz.core.batch import ColumnarBatchAccessor
            if isinstance(accessor, ColumnarBatchAccessor):
                if accessor._batch is not None:
                    blob_ptr = accessor._batch.blob_arena.base_ptr
                    blob_len = accessor._batch.blob_arena.offset

            rc = engine_ffi._table_ingest_one(
                self._handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                rffi.cast(rffi.LONGLONG, weight),
                rffi.cast(rffi.ULONGLONG, null_word),
                col_ptrs,
                rffi.cast(rffi.UINT, num_payload),
                rffi.cast(rffi.CCHARP, blob_ptr),
                rffi.cast(rffi.ULONGLONG, blob_len),
            )
            if intmask(rc) < 0:
                raise errors.StorageError("table_ingest_one failed (%d)" % intmask(rc))
        finally:
            lltype.free(col_ptrs, flavor="raw")

    def ingest_projection(self, source_batch, source_col_idx, source_col_type,
                          index_payload_accessor, is_unique):
        """Project a source batch column into this index table.

        For each row in source_batch with weight != 0 and non-null indexed
        column, extracts (index_key -> source_pk) and ingests into this table.
        """
        from gnitz.core.keys import promote_to_index_key

        n = source_batch.length()
        if n == 0:
            return
        for ri in range(n):
            weight = source_batch.get_weight(ri)
            if weight == r_int64(0):
                continue
            acc = source_batch.get_accessor(ri)
            if acc.is_null(source_col_idx):
                continue

            source_pk_lo = source_batch.get_pk_lo(ri)
            source_pk_hi = source_batch.get_pk_hi(ri)
            index_key = promote_to_index_key(acc, source_col_idx, source_col_type)
            index_key_lo = r_uint64(intmask(index_key))
            index_key_hi = r_uint64(intmask(index_key >> 64))

            if is_unique and weight > 0 and self.has_pk(index_key_lo, index_key_hi):
                raise errors.LayoutError(
                    "Unique index violation during projection")

            index_payload_accessor.set_pk(source_pk_lo, source_pk_hi)
            self.ingest_one(index_key_lo, index_key_hi, weight, index_payload_accessor)

    def flush(self):
        if self.is_closed:
            raise errors.StorageError("Table is closed")
        rc = engine_ffi._table_flush(self._handle)
        rc_int = intmask(rc)
        if rc_int < 0:
            raise errors.StorageError("table_flush failed (%d)" % rc_int)
        return "" if rc_int == 0 else self.directory

    def close(self):
        if self.is_closed:
            return
        self.is_closed = True
        if self._handle:
            engine_ffi._table_close(self._handle)
            self._handle = NULL_HANDLE
