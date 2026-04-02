# gnitz/core/batch.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import strings as string_logic, errors, types
from gnitz.core import comparator as core_comparator


def pk_lt(a_lo, a_hi, b_lo, b_hi):
    """Compare two 128-bit keys (hi/lo pairs) for less-than. JIT-friendly."""
    if a_hi < b_hi:
        return True
    if a_hi > b_hi:
        return False
    return a_lo < b_lo


def pk_eq(a_lo, a_hi, b_lo, b_hi):
    """Compare two 128-bit keys (hi/lo pairs) for equality. JIT-friendly."""
    return a_lo == b_lo and a_hi == b_hi


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
# RowBuilder — direct columnar row construction
# ---------------------------------------------------------------------------


class RowBuilder(core_comparator.RowAccessor):
    _immutable_fields_ = ["_schema", "_pk_index", "_n", "_has_nullable"]

    def __init__(self, schema, target):
        self._schema = schema
        self._target = target
        self._pk_index = schema.pk_index
        n = schema.n_payload
        self._n = n
        self._has_nullable = schema.has_nullable
        self._lo = [r_int64(0)] * n
        self._hi = [r_uint64(0)] * n if schema.has_u128 else None
        self._strs = [""] * n if schema.has_string else None
        self._null_word = r_uint64(0)
        self._pk_lo = r_uint64(0)
        self._pk_hi = r_uint64(0)
        self._weight = r_int64(0)
        self._curr = 0

    def begin(self, pk_lo, pk_hi, weight):
        self._pk_lo = rffi.cast(rffi.ULONGLONG, pk_lo)
        self._pk_hi = rffi.cast(rffi.ULONGLONG, pk_hi)
        self._weight = weight
        self._curr = 0
        self._null_word = r_uint64(0)

    def put_int(self, val_i64):
        self._lo[self._curr] = val_i64
        self._curr += 1

    def put_float(self, val_f64):
        self._lo[self._curr] = float2longlong(val_f64)
        self._curr += 1

    def put_string(self, val_str):
        self._lo[self._curr] = r_int64(0)
        if self._strs is not None:
            self._strs[self._curr] = val_str
        self._curr += 1

    def put_u128(self, lo_u64, hi_u64):
        self._lo[self._curr] = r_int64(intmask(lo_u64))
        if self._hi is not None:
            self._hi[self._curr] = hi_u64
        self._curr += 1

    def put_null(self):
        self._lo[self._curr] = r_int64(0)
        self._null_word = self._null_word | (r_uint64(1) << self._curr)
        self._curr += 1

    def commit(self):
        self._target.append_from_accessor(self._pk_lo, self._pk_hi, self._weight, self)

    def _check_overflow(self):
        if self._curr >= self._n:
            raise errors.LayoutError(
                "Map function attempted to write too many columns "
                "(Schema expects %d non-PK columns)" % self._n
            )

    def append_int(self, val):
        self._check_overflow()
        self._lo[self._curr] = val
        self._curr += 1

    def append_float(self, val_f64):
        self._check_overflow()
        self._lo[self._curr] = float2longlong(val_f64)
        self._curr += 1

    def append_string(self, val_str):
        self._check_overflow()
        self._lo[self._curr] = r_int64(0)
        if self._strs is not None:
            self._strs[self._curr] = val_str
        self._curr += 1

    def append_u128(self, lo_u64, hi_u64):
        self._check_overflow()
        self._lo[self._curr] = r_int64(intmask(lo_u64))
        if self._hi is not None:
            self._hi[self._curr] = hi_u64
        self._curr += 1

    def append_null(self, payload_col_idx):
        self._check_overflow()
        if payload_col_idx != self._curr:
            raise errors.LayoutError(
                "Out-of-order column append detected: expected payload col %d, got %d"
                % (self._curr, payload_col_idx)
            )
        self._lo[self._curr] = r_int64(0)
        self._null_word |= r_uint64(1) << payload_col_idx
        self._curr += 1

    # RowAccessor read interface

    def _payload_idx(self, col_idx):
        if col_idx == self._pk_index:
            return -1
        if col_idx < self._pk_index:
            return col_idx
        return col_idx - 1

    def is_null(self, col_idx):
        if col_idx == self._pk_index:
            return False
        if not self._has_nullable:
            return False
        return bool(self._null_word & (r_uint64(1) << self._payload_idx(col_idx)))

    def get_int(self, col_idx):
        if col_idx == self._pk_index:
            return self._pk_lo
        return r_uint64(self._lo[self._payload_idx(col_idx)])

    def get_int_signed(self, col_idx):
        return rffi.cast(rffi.LONGLONG, r_uint64(self._lo[self._payload_idx(col_idx)]))

    def get_float(self, col_idx):
        return longlong2float(self._lo[self._payload_idx(col_idx)])

    def get_u128_lo(self, col_idx):
        p_idx = self._payload_idx(col_idx)
        return r_uint64(self._lo[p_idx])

    def get_u128_hi(self, col_idx):
        p_idx = self._payload_idx(col_idx)
        return self._hi[p_idx] if self._hi is not None else r_uint64(0)

    def get_str_struct(self, col_idx):
        s = self._strs[self._payload_idx(col_idx)] if self._strs is not None else ""
        prefix = rffi.cast(lltype.Signed, string_logic.compute_prefix(s))
        return (
            len(s),
            prefix,
            lltype.nullptr(rffi.CCHARP.TO),
            lltype.nullptr(rffi.CCHARP.TO),
            s,
        )

    def get_col_ptr(self, col_idx):
        return lltype.nullptr(rffi.CCHARP.TO)


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

        if isinstance(accessor, RowBuilder):
            from gnitz.storage import engine_ffi
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

        # Generic path: only IndexPayloadAccessor reaches here
        schema = self._schema
        num_payload = schema.n_payload
        col_ptrs = lltype.malloc(rffi.VOIDPP.TO, max(num_payload, 1), flavor="raw")
        col_sizes = lltype.malloc(rffi.UINTP.TO, max(num_payload, 1), flavor="raw")
        val_bufs = lltype.malloc(rffi.CCHARP.TO, max(num_payload * 16, 1), flavor="raw")

        null_word = r_uint64(0)
        for i in range(len(schema.columns)):
            if i == schema.pk_index:
                continue
            if accessor.is_null(i):
                payload_idx = i if i < schema.pk_index else i - 1
                null_word |= r_uint64(1) << payload_idx

        try:
            pi = 0
            for ci in range(len(schema.columns)):
                if ci == schema.pk_index:
                    continue
                col_type = schema.columns[ci].field_type
                stride = col_type.size
                col_sizes[pi] = rffi.cast(rffi.UINT, stride)
                vbuf = rffi.ptradd(val_bufs, pi * 16)
                col_ptrs[pi] = rffi.cast(rffi.VOIDP, vbuf)

                if null_word & (r_uint64(1) << pi):
                    for b in range(stride):
                        vbuf[b] = "\x00"
                    pi += 1
                    continue

                code = col_type.code
                if code == types.TYPE_STRING.code:
                    length, prefix, src_struct_ptr, src_heap_ptr, py_string = (
                        accessor.get_str_struct(ci)
                    )
                    rffi.cast(rffi.UINTP, vbuf)[0] = rffi.cast(rffi.UINT, length)
                    rffi.cast(rffi.UINTP, rffi.ptradd(vbuf, 4))[0] = rffi.cast(
                        rffi.UINT, prefix
                    )
                    if length <= string_logic.SHORT_STRING_THRESHOLD:
                        sfx = length - 4 if length > 4 else 0
                        if sfx > 0 and src_struct_ptr != lltype.nullptr(rffi.CCHARP.TO):
                            for k in range(sfx):
                                vbuf[8 + k] = src_struct_ptr[8 + k]
                        for k in range(sfx, 8):
                            vbuf[8 + k] = "\x00"
                    else:
                        if src_struct_ptr != lltype.nullptr(rffi.CCHARP.TO):
                            for k in range(8):
                                vbuf[8 + k] = src_struct_ptr[8 + k]
                        else:
                            for k in range(8):
                                vbuf[8 + k] = "\x00"
                elif code == types.TYPE_F64.code:
                    rffi.cast(rffi.DOUBLEP, vbuf)[0] = rffi.cast(
                        rffi.DOUBLE, accessor.get_float(ci)
                    )
                elif code == types.TYPE_F32.code:
                    rffi.cast(rffi.FLOATP, vbuf)[0] = rffi.cast(
                        rffi.FLOAT, accessor.get_float(ci)
                    )
                elif code == types.TYPE_U128.code:
                    rffi.cast(rffi.ULONGLONGP, vbuf)[0] = rffi.cast(
                        rffi.ULONGLONG, accessor.get_u128_lo(ci)
                    )
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(vbuf, 8))[0] = rffi.cast(
                        rffi.ULONGLONG, accessor.get_u128_hi(ci)
                    )
                else:
                    rffi.cast(rffi.LONGLONGP, vbuf)[0] = rffi.cast(
                        rffi.LONGLONG, accessor.get_int(ci)
                    )
                pi += 1

            actual_blob_src, actual_blob_len = accessor.get_blob_source()

            rc = engine_ffi._batch_append_row(
                self._handle,
                rffi.cast(rffi.ULONGLONG, pk_lo),
                rffi.cast(rffi.ULONGLONG, pk_hi),
                rffi.cast(rffi.LONGLONG, weight),
                rffi.cast(rffi.ULONGLONG, null_word),
                col_ptrs, col_sizes,
                rffi.cast(rffi.UINT, num_payload),
                actual_blob_src,
                rffi.cast(rffi.UINT, actual_blob_len),
            )
            if intmask(rc) < 0:
                raise errors.StorageError("batch_append_row failed")
        finally:
            lltype.free(col_ptrs, flavor="raw")
            lltype.free(col_sizes, flavor="raw")
            lltype.free(val_bufs, flavor="raw")
        self._invalidate_cache()

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


# ---------------------------------------------------------------------------
# Output Capability Security
# ---------------------------------------------------------------------------


class BatchWriter(object):
    _immutable_fields_ = []

    def __init__(self, batch):
        if batch.length() != 0:
            raise errors.StorageError(
                "FATAL: Operator output register is not empty. "
                "The VM must clear destination registers before evaluation."
            )
        self._batch = batch

    def get_schema(self):
        return self._batch._schema

    @jit.unroll_safe
    def append_from_accessor(self, pk_lo, pk_hi, weight, accessor):
        self._batch.append_from_accessor(pk_lo, pk_hi, weight, accessor)

    def append_batch(self, other, start=0, end=-1):
        self._batch.append_batch(other, start, end)

    def append_batch_negated(self, other, start=0, end=-1):
        self._batch.append_batch_negated(other, start, end)

    def direct_append_row(self, src_batch, src_idx, weight):
        self._batch._direct_append_row(src_batch, src_idx, weight)

    def copy_rows_indexed(self, src, indices, weights):
        self._batch._copy_rows_indexed(src, indices, weights)

    def copy_rows_indexed_src_weights(self, src, indices):
        self._batch._copy_rows_indexed_src_weights(src, indices)

    def mark_sorted(self, value):
        self._batch.mark_sorted(value)

    def mark_consolidated(self, value):
        self._batch.mark_consolidated(value)

    def consolidate(self):
        old = self._batch
        if old.length() == 0 or old._consolidated:
            return
        new = old.to_consolidated()
        if new is old:
            old.mark_consolidated(True)
            return
        old.clear()
        old.append_batch(new)
        old.mark_consolidated(True)
        new.free()


# ---------------------------------------------------------------------------
# Scope Management (RAII-style for RPython)
# ---------------------------------------------------------------------------


class BatchScope(object):
    def __init__(self, batch):
        self.input = batch
        self.output = None

    def __enter__(self):
        self.output = self.input
        return self.output

    def __exit__(self, etype, evalue, etb):
        if self.output is not None and self.output is not self.input:
            self.output.free()


class SortedScope(BatchScope):
    def __enter__(self):
        self.output = self.input.to_sorted()
        return self.output


class ConsolidatedScope(BatchScope):
    def __enter__(self):
        self.output = self.input.to_consolidated()
        return self.output
