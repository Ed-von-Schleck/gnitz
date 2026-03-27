# gnitz/storage/cursor.py

from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128, r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core.store import AbstractCursor
from gnitz.core import comparator as core_comparator
from gnitz.core.batch import ColumnarBatchAccessor, pk_lt

MAX_U64 = r_uint64(0xFFFFFFFFFFFFFFFF)

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


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
# SortedBatchCursor
# ---------------------------------------------------------------------------


class SortedBatchCursor(BaseCursor):
    """Sequential iterator over a sorted ArenaZSetBatch."""

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
        """Bind an external ColumnarBatchAccessor to this cursor's current position."""
        acc.bind(self._batch, self._pos)

    def close(self):
        pass





# ---------------------------------------------------------------------------
# RustCursorAccessor — reads column data from the Rust cursor handle
# ---------------------------------------------------------------------------


class RustCursorAccessor(core_comparator.RowAccessor):
    """
    Reads column data from the current row of a Rust-side ReadCursor.
    Calls gnitz_read_cursor_col_ptr / blob_ptr per access.
    Valid only until the next advance/seek on the owning cursor.
    """

    _immutable_fields_ = ["_schema", "_has_nullable"]

    def __init__(self, schema, handle, cursor):
        self._schema = schema
        self._has_nullable = schema.has_nullable
        self._handle = handle
        self._cursor = cursor

    def _col_ptr(self, col_idx):
        from gnitz.storage import engine_ffi
        stride = self._schema.columns[col_idx].field_type.size
        return engine_ffi._read_cursor_col_ptr(
            self._handle,
            rffi.cast(rffi.INT, col_idx),
            rffi.cast(rffi.INT, stride),
        )

    def is_null(self, col_idx):
        if col_idx == self._schema.pk_index:
            return False
        if not self._has_nullable:
            return False
        payload_idx = col_idx if col_idx < self._schema.pk_index else col_idx - 1
        return bool(self._cursor._current_null_word & (r_uint64(1) << payload_idx))

    def get_int(self, col_idx):
        ptr = self._col_ptr(col_idx)
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

    def get_int_signed(self, col_idx):
        ptr = self._col_ptr(col_idx)
        sz = self._schema.columns[col_idx].field_type.size
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
        ptr = self._col_ptr(col_idx)
        sz = self._schema.columns[col_idx].field_type.size
        if sz == 4:
            return float(rffi.cast(rffi.FLOATP, ptr)[0])
        return float(rffi.cast(rffi.DOUBLEP, ptr)[0])

    def get_u128(self, col_idx):
        ptr = self._col_ptr(col_idx)
        lo = rffi.cast(rffi.ULONGLONGP, ptr)[0]
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
        return (r_uint128(hi) << 64) | r_uint128(lo)

    def get_u128_lo(self, col_idx):
        ptr = self._col_ptr(col_idx)
        return rffi.cast(rffi.ULONGLONGP, ptr)[0]

    def get_u128_hi(self, col_idx):
        ptr = self._col_ptr(col_idx)
        return rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]

    def get_str_struct(self, col_idx):
        from gnitz.storage import engine_ffi
        ptr = self._col_ptr(col_idx)
        blob_ptr = engine_ffi._read_cursor_blob_ptr(self._handle)

        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr[0])
        prefix = rffi.cast(lltype.Signed, u32_ptr[1])

        s = None
        if False:
            s = ""
        return (length, prefix, ptr, blob_ptr, s)

    def get_col_ptr(self, col_idx):
        return self._col_ptr(col_idx)


# ---------------------------------------------------------------------------
# RustUnifiedCursor — opaque Rust cursor via FFI
# ---------------------------------------------------------------------------


class RustUnifiedCursor(AbstractCursor):
    """
    N-way merge cursor backed by a Rust-side ReadCursor handle.
    Replaces the RPython UnifiedCursor + TournamentTree + sub-cursors.
    """

    _immutable_fields_ = ["schema", "_accessor"]

    def __init__(self, schema, shard_views, snapshots):
        """Create a Rust-backed N-way merge cursor.

        snapshots: list of ArenaZSetBatch (consolidated memtable snapshots).
                   Each must remain alive until close(). Can be empty.
        shard_views: list of TableShardView (NOT owned by cursor).
        """
        from gnitz.storage import engine_ffi
        from rpython.rlib.rarithmetic import intmask

        self.schema = schema
        self._snapshots = snapshots
        self._snapshot_handles = []  # list of void* (Rust MemTableSnapshot handles)
        if False:
            self._snapshot_handles.append(lltype.nullptr(rffi.VOIDP.TO))
        self._shard_views = shard_views

        num_cols = len(schema.columns)
        pk_index = schema.pk_index
        num_payload_cols = num_cols - 1
        regions_per_batch = 4 + num_payload_cols + 1

        # Count non-empty snapshots
        num_batches = 0
        for snap in snapshots:
            if snap is not None and snap.length() > 0:
                num_batches += 1

        total_batch_regions = num_batches * regions_per_batch
        batch_ptrs = lltype.malloc(
            rffi.VOIDPP.TO, max(total_batch_regions, 1), flavor="raw"
        )
        batch_sizes = lltype.malloc(
            rffi.UINTP.TO, max(total_batch_regions, 1), flavor="raw"
        )
        batch_counts = lltype.malloc(
            rffi.UINTP.TO, max(num_batches, 1), flavor="raw"
        )

        idx = 0
        bi = 0
        for snap in snapshots:
            if snap is None or snap.length() == 0:
                continue
            count = snap.length()
            batch_counts[bi] = rffi.cast(rffi.UINT, count)
            bi += 1
            batch_ptrs[idx] = rffi.cast(rffi.VOIDP, snap.pk_lo_buf.base_ptr)
            batch_sizes[idx] = rffi.cast(rffi.UINT, count * 8)
            idx += 1
            batch_ptrs[idx] = rffi.cast(rffi.VOIDP, snap.pk_hi_buf.base_ptr)
            batch_sizes[idx] = rffi.cast(rffi.UINT, count * 8)
            idx += 1
            batch_ptrs[idx] = rffi.cast(rffi.VOIDP, snap.weight_buf.base_ptr)
            batch_sizes[idx] = rffi.cast(rffi.UINT, count * 8)
            idx += 1
            batch_ptrs[idx] = rffi.cast(rffi.VOIDP, snap.null_buf.base_ptr)
            batch_sizes[idx] = rffi.cast(rffi.UINT, count * 8)
            idx += 1
            for ci in range(num_cols):
                if ci == pk_index:
                    continue
                col_sz = count * snap.col_strides[ci]
                batch_ptrs[idx] = rffi.cast(rffi.VOIDP, snap.col_bufs[ci].base_ptr)
                batch_sizes[idx] = rffi.cast(rffi.UINT, col_sz)
                idx += 1
            batch_ptrs[idx] = rffi.cast(rffi.VOIDP, snap.blob_arena.base_ptr)
            batch_sizes[idx] = rffi.cast(rffi.UINT, snap.blob_arena.offset)
            idx += 1

        # Pack shard handles
        num_shards = len(shard_views)
        shard_handle_arr = lltype.malloc(
            rffi.VOIDPP.TO, max(num_shards, 1), flavor="raw"
        )
        for si in range(num_shards):
            shard_handle_arr[si] = shard_views[si]._handle

        schema_buf = engine_ffi.pack_schema(schema)

        try:
            handle = engine_ffi._read_cursor_create(
                batch_ptrs,
                batch_sizes,
                batch_counts,
                rffi.cast(rffi.UINT, num_batches),
                rffi.cast(rffi.UINT, regions_per_batch),
                shard_handle_arr,
                rffi.cast(rffi.UINT, num_shards),
                rffi.cast(rffi.VOIDP, schema_buf),
            )
            self._handle = handle
        finally:
            lltype.free(batch_ptrs, flavor="raw")
            lltype.free(batch_sizes, flavor="raw")
            lltype.free(batch_counts, flavor="raw")
            lltype.free(shard_handle_arr, flavor="raw")
            lltype.free(schema_buf, flavor="raw")

        self._accessor = RustCursorAccessor(schema, self._handle, self)

        # Compute estimated_length for adaptive path selection in operators
        est = 0
        for snap in snapshots:
            if snap is not None:
                est += snap.length()
        for sv in shard_views:
            est += sv.count
        self._estimated_len = est

        # Read initial state
        self._current_null_word = r_uint64(0)
        out_valid = lltype.malloc(rffi.INTP.TO, 1, flavor="raw")
        out_key_lo = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        out_key_hi = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        out_weight = lltype.malloc(rffi.LONGLONGP.TO, 1, flavor="raw")
        out_null = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        try:
            engine_ffi._read_cursor_seek(
                self._handle,
                rffi.cast(rffi.ULONGLONG, r_uint64(0)),
                rffi.cast(rffi.ULONGLONG, r_uint64(0)),
                out_valid,
                out_key_lo,
                out_key_hi,
                out_weight,
                out_null,
            )
            self._update_state(out_valid, out_key_lo, out_key_hi, out_weight, out_null)
        finally:
            lltype.free(out_valid, flavor="raw")
            lltype.free(out_key_lo, flavor="raw")
            lltype.free(out_key_hi, flavor="raw")
            lltype.free(out_weight, flavor="raw")
            lltype.free(out_null, flavor="raw")

    @staticmethod
    def from_handle(schema, cursor_handle):
        """Wrap a CursorHandle from gnitz_table_create_cursor.

        The cursor_handle is a void* directly from the Rust Table.
        No snapshot/shard marshaling needed.
        """
        from gnitz.storage import engine_ffi

        cur = RustUnifiedCursor(schema, [], [])
        engine_ffi._read_cursor_close(cur._handle)
        cur._handle = cursor_handle
        cur._accessor = RustCursorAccessor(schema, cursor_handle, cur)
        cur._estimated_len = 1000000  # conservative: forces merge-walk over swap

        out_valid = lltype.malloc(rffi.INTP.TO, 1, flavor="raw")
        out_key_lo = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        out_key_hi = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        out_weight = lltype.malloc(rffi.LONGLONGP.TO, 1, flavor="raw")
        out_null = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        try:
            engine_ffi._read_cursor_seek(
                cursor_handle,
                rffi.cast(rffi.ULONGLONG, r_uint64(0)),
                rffi.cast(rffi.ULONGLONG, r_uint64(0)),
                out_valid, out_key_lo, out_key_hi, out_weight, out_null,
            )
            cur._update_state(out_valid, out_key_lo, out_key_hi, out_weight, out_null)
        finally:
            lltype.free(out_valid, flavor="raw")
            lltype.free(out_key_lo, flavor="raw")
            lltype.free(out_key_hi, flavor="raw")
            lltype.free(out_weight, flavor="raw")
            lltype.free(out_null, flavor="raw")

        return cur

    def _update_state(self, out_valid, out_key_lo, out_key_hi, out_weight, out_null_word):
        self._valid = bool(intmask(out_valid[0]))
        if self._valid:
            self._current_key_lo = r_uint64(out_key_lo[0])
            self._current_key_hi = r_uint64(out_key_hi[0])
            self._current_weight = r_int64(out_weight[0])
            self._current_null_word = r_uint64(out_null_word[0])

    def seek(self, key_lo, key_hi):
        from gnitz.storage import engine_ffi
        out_valid = lltype.malloc(rffi.INTP.TO, 1, flavor="raw")
        out_key_lo = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        out_key_hi = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        out_weight = lltype.malloc(rffi.LONGLONGP.TO, 1, flavor="raw")
        out_null = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        try:
            engine_ffi._read_cursor_seek(
                self._handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                out_valid,
                out_key_lo,
                out_key_hi,
                out_weight,
                out_null,
            )
            self._update_state(out_valid, out_key_lo, out_key_hi, out_weight, out_null)
        finally:
            lltype.free(out_valid, flavor="raw")
            lltype.free(out_key_lo, flavor="raw")
            lltype.free(out_key_hi, flavor="raw")
            lltype.free(out_weight, flavor="raw")
            lltype.free(out_null, flavor="raw")

    def advance(self):
        from gnitz.storage import engine_ffi
        if not self._valid:
            return
        out_valid = lltype.malloc(rffi.INTP.TO, 1, flavor="raw")
        out_key_lo = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        out_key_hi = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        out_weight = lltype.malloc(rffi.LONGLONGP.TO, 1, flavor="raw")
        out_null = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        try:
            engine_ffi._read_cursor_next(
                self._handle,
                out_valid,
                out_key_lo,
                out_key_hi,
                out_weight,
                out_null,
            )
            self._update_state(out_valid, out_key_lo, out_key_hi, out_weight, out_null)
        finally:
            lltype.free(out_valid, flavor="raw")
            lltype.free(out_key_lo, flavor="raw")
            lltype.free(out_key_hi, flavor="raw")
            lltype.free(out_weight, flavor="raw")
            lltype.free(out_null, flavor="raw")

    def key_lo(self):
        return self._current_key_lo

    def key_hi(self):
        return self._current_key_hi

    def weight(self):
        return self._current_weight

    def is_valid(self):
        return self._valid

    def get_accessor(self):
        return self._accessor

    def estimated_length(self):
        return self._estimated_len

    def close(self):
        from gnitz.storage import engine_ffi
        if self._handle:
            engine_ffi._read_cursor_close(self._handle)
            self._handle = lltype.nullptr(rffi.VOIDP.TO)
        for snap in self._snapshots:
            if snap is not None:
                snap.release()
        self._snapshots = []
        for sh in self._snapshot_handles:
            engine_ffi._memtable_snapshot_free(sh)
        self._snapshot_handles = []
