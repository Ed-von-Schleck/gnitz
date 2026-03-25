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
    shard_table,
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
        self._retract_soa = storage_comparator.SoAAccessor(schema)

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
        out_shard = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
        out_row = lltype.malloc(rffi.INTP.TO, 1, flavor="raw")
        out_weight = lltype.malloc(rffi.LONGLONGP.TO, 1, flavor="raw")
        try:
            out_shard[0] = NULL_HANDLE
            out_row[0] = rffi.cast(rffi.INT, -1)
            out_weight[0] = rffi.cast(rffi.LONGLONG, 0)
            found = engine_ffi._table_retract_pk(
                self._handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                rffi.cast(rffi.VOIDPP, out_shard),
                out_row, out_weight,
            )
            if intmask(found) == 0:
                return False
            shard_handle = out_shard[0]
            row_idx = intmask(out_row[0])
        finally:
            lltype.free(out_shard, flavor="raw")
            lltype.free(out_row, flavor="raw")
            lltype.free(out_weight, flavor="raw")

        if shard_handle:
            view = shard_table.TableShardView("", self.schema)
            view._handle = shard_handle
            view._blob_ptr = engine_ffi._shard_blob_ptr(shard_handle)
            view.count = intmask(engine_ffi._shard_row_count(shard_handle))
            self._retract_soa.set_row(view, row_idx)
            out_batch.append_from_accessor(key_lo, key_hi, r_int64(-1), self._retract_soa)
            engine_ffi._shard_close(shard_handle)
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
            from gnitz.storage.memtable import _pack_batch_desc
            _pack_batch_desc(desc_buf, batch, self.schema, num_payload, col_ptrs, col_sizes)
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
