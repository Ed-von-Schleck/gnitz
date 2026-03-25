# gnitz/storage/cursor.py

from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128, r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core.store import AbstractCursor
from gnitz.core.comparator import RowAccessor
from gnitz.core.batch import ColumnarBatchAccessor, pk_lt
from gnitz.storage import engine_ffi
from gnitz.storage.buffer import c_memmove

MAX_U64 = r_uint64(0xFFFFFFFFFFFFFFFF)

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)
NULL_HANDLE = lltype.nullptr(rffi.VOIDP.TO)


# ---------------------------------------------------------------------------
# BaseCursor
# ---------------------------------------------------------------------------


class BaseCursor(AbstractCursor):
    def __init__(self):
        pass

    def seek(self, key_lo, key_hi):
        raise NotImplementedError

    def advance(self):
        raise NotImplementedError

    def key_lo(self):
        raise NotImplementedError

    def key_hi(self):
        raise NotImplementedError

    def weight(self):
        raise NotImplementedError

    def is_valid(self):
        raise NotImplementedError

    def get_accessor(self):
        raise NotImplementedError

    def close(self):
        pass


# ---------------------------------------------------------------------------
# SortedBatchCursor (used by DBSP anti_join + join operators)
# ---------------------------------------------------------------------------


class SortedBatchCursor(BaseCursor):

    _immutable_fields_ = ["_batch", "accessor"]

    def __init__(self, batch):
        BaseCursor.__init__(self)
        self._batch = batch
        self._pos = 0
        self.accessor = ColumnarBatchAccessor(batch._schema)

    def seek(self, key_lo, key_hi):
        lo = 0
        hi = self._batch.length()
        while lo < hi:
            mid = (lo + hi) >> 1
            if pk_lt(self._batch.get_pk_lo(mid), self._batch.get_pk_hi(mid), key_lo, key_hi):
                lo = mid + 1
            else:
                hi = mid
        self._pos = lo

    def advance(self):
        self._pos += 1

    def is_valid(self):
        return self._pos < self._batch.length()

    def key_lo(self):
        if self._pos >= self._batch.length():
            return MAX_U64
        return self._batch.get_pk_lo(self._pos)

    def key_hi(self):
        if self._pos >= self._batch.length():
            return MAX_U64
        return self._batch.get_pk_hi(self._pos)

    def peek_key_lo(self):
        if self._pos < self._batch.length():
            return self._batch.get_pk_lo(self._pos)
        return r_uint64(0)

    def peek_key_hi(self):
        if self._pos < self._batch.length():
            return self._batch.get_pk_hi(self._pos)
        return r_uint64(0)

    def weight(self):
        if self._pos >= self._batch.length():
            return r_int64(0)
        return self._batch.get_weight(self._pos)

    def get_accessor(self):
        self.accessor.bind(self._batch, self._pos)
        return self.accessor

    def is_exhausted(self):
        return self._pos >= self._batch.length()

    def estimated_length(self):
        return self._batch.length()

    def bind_to(self, acc):
        acc.bind(self._batch, self._pos)

    def close(self):
        pass


# ---------------------------------------------------------------------------
# RustCursorAccessor
# ---------------------------------------------------------------------------


class RustCursorAccessor(RowAccessor):
    """Reads column data from a Rust UnifiedCursor's current position via FFI."""

    _immutable_fields_ = ["schema"]

    def __init__(self, schema, cursor_handle):
        self.schema = schema
        self._handle = cursor_handle

    def _get_ptr(self, col_idx):
        return engine_ffi._cursor_col_ptr(
            self._handle,
            rffi.cast(rffi.UINT, col_idx),
            rffi.cast(rffi.UINT, self.schema.columns[col_idx].field_type.size),
        )

    def is_null(self, col_idx):
        schema = self.schema
        if col_idx == schema.pk_index:
            return False
        if not schema.has_nullable:
            return False
        null_word = r_uint64(engine_ffi._cursor_null_word(self._handle))
        payload_idx = col_idx if col_idx < schema.pk_index else col_idx - 1
        return bool(null_word & (r_uint64(1) << payload_idx))

    def get_int(self, col_idx):
        ptr = self._get_ptr(col_idx)
        sz = self.schema.columns[col_idx].field_type.size
        if sz == 8:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.LONGLONGP, ptr)[0])
        elif sz == 4:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.UINTP, ptr)[0])
        elif sz == 2:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.USHORTP, ptr)[0])
        elif sz == 1:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.UCHARP, ptr)[0])
        return r_uint64(0)

    def get_int_signed(self, col_idx):
        ptr = self._get_ptr(col_idx)
        sz = self.schema.columns[col_idx].field_type.size
        if sz == 8:
            return rffi.cast(rffi.LONGLONG, rffi.cast(rffi.LONGLONGP, ptr)[0])
        elif sz == 4:
            return rffi.cast(rffi.LONGLONG, rffi.cast(rffi.INTP, ptr)[0])
        elif sz == 2:
            return rffi.cast(rffi.LONGLONG, rffi.cast(rffi.SHORTP, ptr)[0])
        elif sz == 1:
            return rffi.cast(rffi.LONGLONG, rffi.cast(rffi.SIGNEDCHARP, ptr)[0])
        return r_int64(0)

    def get_float(self, col_idx):
        ptr = self._get_ptr(col_idx)
        sz = self.schema.columns[col_idx].field_type.size
        if sz == 4:
            return float(rffi.cast(rffi.FLOATP, ptr)[0])
        return float(rffi.cast(rffi.DOUBLEP, ptr)[0])

    def get_u128(self, col_idx):
        ptr = self._get_ptr(col_idx)
        lo = rffi.cast(rffi.ULONGLONGP, ptr)
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))
        return (r_uint128(hi[0]) << 64) | r_uint128(lo[0])

    def get_u128_lo(self, col_idx):
        ptr = self._get_ptr(col_idx)
        return r_uint64(rffi.cast(rffi.ULONGLONGP, ptr)[0])

    def get_u128_hi(self, col_idx):
        ptr = self._get_ptr(col_idx)
        return r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0])

    def get_str_struct(self, col_idx):
        ptr = self._get_ptr(col_idx)
        blob_ptr = engine_ffi._cursor_blob_ptr(self._handle)

        if not ptr:
            return (0, 0, NULL_PTR, blob_ptr, None)

        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr[0])
        prefix = rffi.cast(lltype.Signed, u32_ptr[1])

        s = None
        if False:
            s = ""
        return (length, prefix, ptr, blob_ptr, s)

    def get_col_ptr(self, col_idx):
        return self._get_ptr(col_idx)


# ---------------------------------------------------------------------------
# RustUnifiedCursor
# ---------------------------------------------------------------------------


class RustUnifiedCursor(AbstractCursor):
    """N-way merge cursor backed by Rust's UnifiedCursor via FFI."""

    def __init__(self, schema, borrowed_handles, memtable_snapshot):
        self.schema = schema
        self._handle = NULL_HANDLE
        self._snapshot = memtable_snapshot

        num_borrowed = len(borrowed_handles)
        has_batch = memtable_snapshot is not None and memtable_snapshot.length() > 0
        num_batches = 1 if has_batch else 0
        num_payload = len(schema.columns) - 1

        # Pack borrowed shard handles
        if num_borrowed > 0:
            c_handles = lltype.malloc(rffi.VOIDPP.TO, num_borrowed, flavor="raw")
            i = 0
            while i < num_borrowed:
                c_handles[i] = borrowed_handles[i]
                i += 1
        else:
            c_handles = lltype.nullptr(rffi.VOIDPP.TO)

        # Pack batch descriptor for memtable snapshot
        if has_batch:
            desc_buf = lltype.malloc(
                rffi.CCHARP.TO, engine_ffi.BATCH_DESC_SIZE, flavor="raw"
            )
            col_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
            col_sizes = lltype.malloc(rffi.ULONGLONGP.TO, num_payload, flavor="raw")
            pi = 0
            ci = 0
            while ci < len(schema.columns):
                if ci != schema.pk_index:
                    col_ptrs[pi] = rffi.cast(
                        rffi.VOIDP, memtable_snapshot.col_bufs[ci].base_ptr
                    )
                    col_sizes[pi] = rffi.cast(
                        rffi.ULONGLONG, memtable_snapshot.col_bufs[ci].offset
                    )
                    pi += 1
                ci += 1
            _pack_batch_desc(
                desc_buf, memtable_snapshot, schema, num_payload, col_ptrs, col_sizes
            )
        else:
            desc_buf = lltype.nullptr(rffi.CCHARP.TO)
            col_ptrs = lltype.nullptr(rffi.VOIDPP.TO)
            col_sizes = lltype.nullptr(rffi.ULONGLONGP.TO)

        schema_buf = engine_ffi.pack_schema(schema)

        try:
            handle = engine_ffi._cursor_create(
                c_handles,
                rffi.cast(rffi.UINT, num_borrowed),
                rffi.cast(rffi.VOIDP, desc_buf),
                rffi.cast(rffi.UINT, num_batches),
                rffi.cast(rffi.VOIDP, schema_buf),
            )
            if not handle:
                raise Exception("gnitz_cursor_create failed")
            self._handle = handle
        finally:
            lltype.free(schema_buf, flavor="raw")
            if col_ptrs:
                lltype.free(col_ptrs, flavor="raw")
            if col_sizes:
                lltype.free(col_sizes, flavor="raw")
            if desc_buf:
                lltype.free(desc_buf, flavor="raw")
            if c_handles:
                lltype.free(c_handles, flavor="raw")

        self._accessor = RustCursorAccessor(schema, self._handle)

    def seek(self, key_lo, key_hi):
        engine_ffi._cursor_seek(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )

    def advance(self):
        engine_ffi._cursor_advance(self._handle)

    def is_valid(self):
        return bool(intmask(engine_ffi._cursor_is_valid(self._handle)))

    def key_lo(self):
        return r_uint64(engine_ffi._cursor_key_lo(self._handle))

    def key_hi(self):
        return r_uint64(engine_ffi._cursor_key_hi(self._handle))

    def weight(self):
        return intmask(engine_ffi._cursor_weight(self._handle))

    def get_accessor(self):
        return self._accessor

    def estimated_length(self):
        return 0

    def close(self):
        if self._handle:
            engine_ffi._cursor_close(self._handle)
            self._handle = NULL_HANDLE
        if self._snapshot is not None:
            self._snapshot.release()
            self._snapshot = None


class RustUnifiedCursorFromHandle(AbstractCursor):
    """Wraps a pre-created Rust cursor handle (from gnitz_table_create_cursor)."""

    def __init__(self, schema, handle):
        self.schema = schema
        self._handle = handle
        self._accessor = RustCursorAccessor(schema, handle)

    def seek(self, key_lo, key_hi):
        engine_ffi._cursor_seek(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )

    def advance(self):
        engine_ffi._cursor_advance(self._handle)

    def is_valid(self):
        return bool(intmask(engine_ffi._cursor_is_valid(self._handle)))

    def key_lo(self):
        return r_uint64(engine_ffi._cursor_key_lo(self._handle))

    def key_hi(self):
        return r_uint64(engine_ffi._cursor_key_hi(self._handle))

    def weight(self):
        return intmask(engine_ffi._cursor_weight(self._handle))

    def get_accessor(self):
        return self._accessor

    def estimated_length(self):
        return 0

    def close(self):
        if self._handle:
            engine_ffi._cursor_close(self._handle)
            self._handle = NULL_HANDLE


def _pack_batch_desc(base, batch, schema, num_payload, col_ptrs, col_sizes):
    """Write a GnitzBatchDesc at base for one ArenaZSetBatch."""
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
