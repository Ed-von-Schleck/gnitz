# gnitz/storage/owned_batch.py
#
# ArenaZSetBatch + ColumnarBatchAccessor — thin wrappers over Rust OwnedBatch.

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.core import comparator as core_comparator


# ---------------------------------------------------------------------------
# Columnar Batch Accessor
# ---------------------------------------------------------------------------


class ColumnarBatchAccessor(core_comparator.RowAccessor):
    """
    Reads data from a columnar ArenaZSetBatch by row index.
    Implements the full RowAccessor interface.
    """

    _immutable_fields_ = ["_schema", "_has_nullable"]

    def __init__(self, schema):
        self._schema = schema
        self._has_nullable = schema.has_nullable
        self._batch = None
        self._row_idx = 0

    def bind(self, batch, row_idx):
        self._batch = batch
        self._row_idx = row_idx

    def _payload_idx(self, col_idx):
        if col_idx == self._schema.pk_index:
            return -1
        if col_idx < self._schema.pk_index:
            return col_idx
        return col_idx - 1

    def is_null(self, col_idx):
        if col_idx == self._schema.pk_index:
            return False
        if not self._has_nullable:
            return False
        batch = self._batch
        assert batch is not None
        payload_idx = self._payload_idx(col_idx)
        null_word = batch._read_null_word(self._row_idx)
        return bool(null_word & (r_uint64(1) << payload_idx))

    def get_int(self, col_idx):
        batch = self._batch
        assert batch is not None
        if col_idx == self._schema.pk_index:
            return batch._read_pk_lo(self._row_idx)
        return batch._read_col_int(self._row_idx, col_idx)

    def get_int_signed(self, col_idx):
        return rffi.cast(rffi.LONGLONG, self.get_int(col_idx))

    def get_float(self, col_idx):
        batch = self._batch
        assert batch is not None
        return batch._read_col_float(self._row_idx, col_idx)

    def get_u128_lo(self, col_idx):
        batch = self._batch
        assert batch is not None
        ptr = batch._col_ptr(self._row_idx, col_idx)
        return rffi.cast(rffi.ULONGLONGP, ptr)[0]

    def get_u128_hi(self, col_idx):
        batch = self._batch
        assert batch is not None
        ptr = batch._col_ptr(self._row_idx, col_idx)
        return rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]

    def get_str_struct(self, col_idx):
        batch = self._batch
        assert batch is not None
        return batch._read_col_str_struct(self._row_idx, col_idx)

    def get_col_ptr(self, col_idx):
        batch = self._batch
        assert batch is not None
        return batch._read_col_ptr(self._row_idx, col_idx)


# ---------------------------------------------------------------------------
# ArenaZSetBatch — Thin wrapper over Rust OwnedBatch handle
# ---------------------------------------------------------------------------


class ArenaZSetBatch(object):
    _immutable_fields_ = ["_schema"]

    def __init__(self, schema, initial_capacity=1024):
        from gnitz.storage import engine_ffi

        self._schema = schema
        schema_buf = engine_ffi.pack_schema(schema)
        self._handle = engine_ffi._batch_create(
            rffi.cast(rffi.VOIDP, schema_buf),
            rffi.cast(rffi.UINT, initial_capacity),
        )
        lltype.free(schema_buf, flavor="raw")
        self._raw_accessor = ColumnarBatchAccessor(schema)
        self._sorted = True
        self._consolidated = True
        self._freed = False
        self._ref_count = 1

    @staticmethod
    def _wrap_handle(schema, handle, is_sorted, is_consolidated):
        from gnitz.storage import engine_ffi

        batch = ArenaZSetBatch(schema, initial_capacity=0)
        engine_ffi._batch_free(batch._handle)
        batch._handle = handle
        batch._sorted = is_sorted
        batch._consolidated = is_consolidated
        return batch

    @staticmethod
    def from_regions(schema, region_ptrs, region_sizes, count, num_regions, is_sorted=False):
        from gnitz.storage import engine_ffi

        schema_buf = engine_ffi.pack_schema(schema)
        handle = engine_ffi._batch_from_regions(
            rffi.cast(rffi.VOIDP, schema_buf),
            region_ptrs, region_sizes,
            rffi.cast(rffi.UINT, count),
            rffi.cast(rffi.UINT, num_regions),
        )
        lltype.free(schema_buf, flavor="raw")
        return ArenaZSetBatch._wrap_handle(schema, handle, is_sorted, False)

    def acquire(self):
        self._ref_count += 1
        return self

    def release(self):
        self._ref_count -= 1
        if self._ref_count <= 0:
            self.free()

    def length(self):
        from gnitz.storage import engine_ffi

        return intmask(engine_ffi._batch_length(self._handle))

    def is_sorted(self):
        return self._sorted

    def _invalidate_cache(self):
        self._sorted = False
        self._consolidated = False

    def mark_sorted(self, value):
        from gnitz.storage import engine_ffi
        self._sorted = value
        engine_ffi._batch_set_sorted(
            self._handle, rffi.cast(rffi.INT, 1 if value else 0))

    def mark_consolidated(self, value):
        from gnitz.storage import engine_ffi
        self._consolidated = value
        if value:
            self._sorted = True
        engine_ffi._batch_set_sorted(
            self._handle, rffi.cast(rffi.INT, 1 if self._sorted else 0))
        engine_ffi._batch_set_consolidated(
            self._handle, rffi.cast(rffi.INT, 1 if value else 0))

    def is_empty(self):
        return self.length() == 0

    def clear(self):
        from gnitz.storage import engine_ffi

        engine_ffi._batch_clear(self._handle)
        self._sorted = True
        self._consolidated = True

    def free(self):
        from gnitz.storage import engine_ffi

        if self._freed:
            return
        engine_ffi._batch_free(self._handle)
        self._freed = True

    # -------------------------------------------------------------------
    # Internal column read helpers
    # -------------------------------------------------------------------

    def _read_pk_lo(self, i):
        from gnitz.storage import engine_ffi

        return engine_ffi._batch_get_pk_lo(self._handle, rffi.cast(rffi.UINT, i))

    def _read_pk_hi(self, i):
        from gnitz.storage import engine_ffi

        return engine_ffi._batch_get_pk_hi(self._handle, rffi.cast(rffi.UINT, i))

    def _read_weight(self, i):
        from gnitz.storage import engine_ffi

        return engine_ffi._batch_get_weight(self._handle, rffi.cast(rffi.UINT, i))

    def _read_null_word(self, i):
        from gnitz.storage import engine_ffi

        return engine_ffi._batch_get_null_word(self._handle, rffi.cast(rffi.UINT, i))

    def _col_ptr(self, i, col_idx):
        from gnitz.storage import engine_ffi

        schema = self._schema
        payload_col = col_idx if col_idx < schema.pk_index else col_idx - 1
        col_size = schema.columns[col_idx].field_type.size
        return engine_ffi._batch_col_ptr(
            self._handle,
            rffi.cast(rffi.UINT, i),
            rffi.cast(rffi.UINT, payload_col),
            rffi.cast(rffi.UINT, col_size),
        )

    def _read_col_int(self, i, col_idx):
        ptr = self._col_ptr(i, col_idx)
        sz = self._schema.columns[col_idx].field_type.size
        if sz == 8:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.LONGLONGP, ptr)[0])
        elif sz == 4:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.UINTP, ptr)[0])
        elif sz == 2:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.USHORTP, ptr)[0])
        elif sz == 1:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.UCHARP, ptr)[0])
        return r_uint64(0)

    def _read_col_float(self, i, col_idx):
        ptr = self._col_ptr(i, col_idx)
        sz = self._schema.columns[col_idx].field_type.size
        if sz == 4:
            return float(rffi.cast(rffi.FLOATP, ptr)[0])
        return float(rffi.cast(rffi.DOUBLEP, ptr)[0])

    def _read_col_u128(self, i, col_idx):
        ptr = self._col_ptr(i, col_idx)
        lo = rffi.cast(rffi.ULONGLONGP, ptr)[0]
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
        return (r_uint128(hi) << 64) | r_uint128(lo)

    def _read_col_str_struct(self, i, col_idx):
        from gnitz.storage import engine_ffi

        ptr = self._col_ptr(i, col_idx)
        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr[0])
        prefix = rffi.cast(lltype.Signed, u32_ptr[1])
        blob_base = engine_ffi._batch_blob_ptr(self._handle)
        s = None
        if False:
            s = ""
        return (length, prefix, ptr, blob_base, s)

    def _read_col_ptr(self, i, col_idx):
        return self._col_ptr(i, col_idx)

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    def get_pk(self, i):
        lo = self._read_pk_lo(i)
        hi = self._read_pk_hi(i)
        return (r_uint128(hi) << 64) | r_uint128(lo)

    def get_pk_lo(self, i):
        return self._read_pk_lo(i)

    def get_pk_hi(self, i):
        return self._read_pk_hi(i)

    def get_weight(self, i):
        return self._read_weight(i)

    def get_accessor(self, i):
        self.bind_accessor(i, self._raw_accessor)
        return self._raw_accessor

    def bind_accessor(self, i, out_accessor):
        out_accessor.bind(self, i)

    @jit.unroll_safe
    def append_from_accessor(self, pk_lo, pk_hi, weight, accessor):
        from gnitz.storage import engine_ffi

        assert not self._freed

        if isinstance(accessor, ColumnarBatchAccessor):
            src_batch = accessor._batch
            if src_batch is not None:
                rc = engine_ffi._batch_append_row_from_batch(
                    self._handle, src_batch._handle,
                    rffi.cast(rffi.UINT, accessor._row_idx),
                    rffi.cast(rffi.LONGLONG, weight),
                )
                if intmask(rc) < 0:
                    raise errors.StorageError("append_row_from_batch failed")
                self._invalidate_cache()
                return

        from gnitz.catalog.system_tables import RowBuilder
        if isinstance(accessor, RowBuilder):
            n = accessor._n
            lo_arr = lltype.malloc(rffi.LONGLONGP.TO, max(n, 1), flavor="raw")
            hi_arr = lltype.malloc(rffi.ULONGLONGP.TO, max(n, 1), flavor="raw")
            str_ptrs_arr = lltype.malloc(rffi.CCHARPP.TO, max(n, 1), flavor="raw")
            str_lens_arr = lltype.malloc(rffi.UINTP.TO, max(n, 1), flavor="raw")
            str_bufs = lltype.malloc(rffi.CCHARPP.TO, max(n, 1), flavor="raw")
            for pi in range(max(n, 1)):
                str_bufs[pi] = lltype.nullptr(rffi.CCHARP.TO)
            try:
                for pi in range(n):
                    lo_arr[pi] = accessor._lo[pi]
                    hi_arr[pi] = rffi.cast(rffi.ULONGLONG,
                        accessor._hi[pi] if accessor._hi is not None else r_uint64(0))
                    if accessor._strs is not None:
                        s = accessor._strs[pi]
                        slen = len(s)
                        if slen > 0:
                            buf = rffi.str2charp(s)
                            str_bufs[pi] = buf
                            str_ptrs_arr[pi] = buf
                            str_lens_arr[pi] = rffi.cast(rffi.UINT, slen)
                        else:
                            str_ptrs_arr[pi] = lltype.nullptr(rffi.CCHARP.TO)
                            str_lens_arr[pi] = rffi.cast(rffi.UINT, 0)
                    else:
                        str_ptrs_arr[pi] = lltype.nullptr(rffi.CCHARP.TO)
                        str_lens_arr[pi] = rffi.cast(rffi.UINT, 0)
                rc = engine_ffi._batch_append_row_simple(
                    self._handle,
                    rffi.cast(rffi.ULONGLONG, pk_lo),
                    rffi.cast(rffi.ULONGLONG, pk_hi),
                    rffi.cast(rffi.LONGLONG, weight),
                    rffi.cast(rffi.ULONGLONG, accessor._null_word),
                    lo_arr, hi_arr, str_ptrs_arr, str_lens_arr,
                    rffi.cast(rffi.UINT, n),
                )
                if intmask(rc) < 0:
                    raise errors.StorageError("batch_append_row_simple failed")
            finally:
                lltype.free(lo_arr, flavor="raw")
                lltype.free(hi_arr, flavor="raw")
                for pi in range(n):
                    if str_bufs[pi]:
                        rffi.free_charp(str_bufs[pi])
                lltype.free(str_bufs, flavor="raw")
                lltype.free(str_ptrs_arr, flavor="raw")
                lltype.free(str_lens_arr, flavor="raw")
            self._invalidate_cache()
            return

        from gnitz.storage.cursor import RustCursorAccessor
        from gnitz.storage.ephemeral_table import TableFoundAccessor
        from gnitz.storage.partitioned_table import PTableFoundAccessor

        if isinstance(accessor, RustCursorAccessor):
            rc = engine_ffi._batch_append_row_from_cursor(
                self._handle, accessor._handle,
                rffi.cast(rffi.LONGLONG, weight),
            )
            if intmask(rc) < 0:
                raise errors.StorageError("append_row_from_cursor failed")
            self._invalidate_cache()
            return

        if isinstance(accessor, TableFoundAccessor):
            rc = engine_ffi._batch_append_row_from_table_found(
                self._handle, accessor._handle,
                rffi.cast(rffi.ULONGLONG, pk_lo),
                rffi.cast(rffi.ULONGLONG, pk_hi),
                rffi.cast(rffi.LONGLONG, weight),
            )
            if intmask(rc) < 0:
                raise errors.StorageError("append_row_from_table_found failed")
            self._invalidate_cache()
            return

        if isinstance(accessor, PTableFoundAccessor):
            rc = engine_ffi._batch_append_row_from_ptable_found(
                self._handle, accessor._handle,
                rffi.cast(rffi.ULONGLONG, pk_lo),
                rffi.cast(rffi.ULONGLONG, pk_hi),
                rffi.cast(rffi.LONGLONG, weight),
            )
            if intmask(rc) < 0:
                raise errors.StorageError("append_row_from_ptable_found failed")
            self._invalidate_cache()
            return

        raise errors.StorageError("append_from_accessor: unknown accessor type")

    def clone(self):
        from gnitz.storage import engine_ffi

        new_handle = engine_ffi._batch_clone(self._handle)
        return ArenaZSetBatch._wrap_handle(
            self._schema, new_handle, self._sorted, self._consolidated
        )

    def append_batch(self, other, start=0, end=-1):
        from gnitz.storage import engine_ffi

        assert not self._freed
        if end == -1:
            end = other.length()
        if end <= start:
            return
        rc = engine_ffi._batch_append_batch(
            self._handle, other._handle,
            rffi.cast(rffi.UINT, start), rffi.cast(rffi.UINT, end),
        )
        if intmask(rc) < 0:
            raise errors.StorageError("append_batch failed")
        self._invalidate_cache()

    def append_batch_negated(self, other, start=0, end=-1):
        from gnitz.storage import engine_ffi

        assert not self._freed
        if end == -1:
            end = other.length()
        if end <= start:
            return
        rc = engine_ffi._batch_append_batch_negated(
            self._handle, other._handle,
            rffi.cast(rffi.UINT, start), rffi.cast(rffi.UINT, end),
        )
        if intmask(rc) < 0:
            raise errors.StorageError("append_batch_negated failed")
        self._invalidate_cache()

    def _direct_append_row(self, src, src_idx, weight_override):
        from gnitz.storage import engine_ffi

        assert not self._freed
        rc = engine_ffi._batch_append_row_from_batch(
            self._handle, src._handle,
            rffi.cast(rffi.UINT, src_idx),
            rffi.cast(rffi.LONGLONG, weight_override),
        )
        if intmask(rc) < 0:
            raise errors.StorageError("append_row_from_batch failed")
        self._invalidate_cache()

    def _copy_rows_indexed(self, src, indices, weights):
        from gnitz.storage import engine_ffi

        n = len(indices)
        if n == 0:
            return
        idx_arr = lltype.malloc(rffi.UINTP.TO, n, flavor="raw")
        for i in range(n):
            idx_arr[i] = rffi.cast(rffi.UINT, indices[i])
        schema_buf = engine_ffi.pack_schema(self._schema)
        try:
            if weights is not None:
                w_arr = lltype.malloc(rffi.LONGLONGP.TO, n, flavor="raw")
                for i in range(n):
                    w_arr[i] = rffi.cast(rffi.LONGLONG, weights[i])
                try:
                    new_handle = engine_ffi._batch_scatter_copy_weighted(
                        src._handle, idx_arr, w_arr,
                        rffi.cast(rffi.UINT, n),
                        rffi.cast(rffi.VOIDP, schema_buf),
                    )
                finally:
                    lltype.free(w_arr, flavor="raw")
            else:
                new_handle = engine_ffi._batch_scatter_copy(
                    src._handle, idx_arr,
                    rffi.cast(rffi.UINT, n),
                    rffi.cast(rffi.VOIDP, schema_buf),
                )
        finally:
            lltype.free(idx_arr, flavor="raw")
            lltype.free(schema_buf, flavor="raw")
        temp = ArenaZSetBatch._wrap_handle(self._schema, new_handle, False, False)
        self.append_batch(temp)
        temp.free()

    def _copy_rows_indexed_src_weights(self, src, indices):
        self._copy_rows_indexed(src, indices, None)

    def to_sorted(self):
        from gnitz.storage import engine_ffi

        if self.length() <= 1 or self._sorted:
            return self
        schema_buf = engine_ffi.pack_schema(self._schema)
        new_handle = engine_ffi._batch_to_sorted(
            self._handle, rffi.cast(rffi.VOIDP, schema_buf)
        )
        lltype.free(schema_buf, flavor="raw")
        if new_handle == self._handle:
            self._sorted = True
            return self
        return ArenaZSetBatch._wrap_handle(self._schema, new_handle, True, False)

    def to_consolidated(self):
        from gnitz.storage import engine_ffi

        if self.length() == 0 or self._consolidated:
            return self
        schema_buf = engine_ffi.pack_schema(self._schema)
        new_handle = engine_ffi._batch_to_consolidated(
            self._handle, rffi.cast(rffi.VOIDP, schema_buf)
        )
        lltype.free(schema_buf, flavor="raw")
        if new_handle == self._handle:
            self._sorted = True
            self._consolidated = True
            return self
        return ArenaZSetBatch._wrap_handle(self._schema, new_handle, True, True)
