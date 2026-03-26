# gnitz/storage/cursor.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128, r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import types
from gnitz.core.store import AbstractCursor
from gnitz.storage import tournament_tree, comparator
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
# MemTableCursor
# ---------------------------------------------------------------------------


class MemTableCursor(BaseCursor):
    """
    Cursor over a consolidated snapshot of a MemTable's sorted runs.
    Creates an independent snapshot to avoid use-after-free when flush()
    frees runs while the cursor is live.
    """

    _immutable_fields_ = ["schema", "_snapshot", "_accessor"]

    def __init__(self, memtable):
        BaseCursor.__init__(self)
        self.schema = memtable.schema
        self._snapshot = memtable.get_consolidated_snapshot()
        self._pos = 0
        self._accessor = ColumnarBatchAccessor(memtable.schema)

    def seek(self, key_lo, key_hi):
        lo = 0
        hi = self._snapshot.length()
        while lo < hi:
            mid = (lo + hi) >> 1
            if pk_lt(self._snapshot.get_pk_lo(mid), self._snapshot.get_pk_hi(mid), key_lo, key_hi):
                lo = mid + 1
            else:
                hi = mid
        self._pos = lo

    def advance(self):
        self._pos += 1

    def is_valid(self):
        return self._pos < self._snapshot.length()

    def key_lo(self):
        if self._pos >= self._snapshot.length():
            return MAX_U64
        return self._snapshot.get_pk_lo(self._pos)

    def key_hi(self):
        if self._pos >= self._snapshot.length():
            return MAX_U64
        return self._snapshot.get_pk_hi(self._pos)

    def peek_key_lo(self):
        if self._pos < self._snapshot.length():
            return self._snapshot.get_pk_lo(self._pos)
        return r_uint64(0)

    def peek_key_hi(self):
        if self._pos < self._snapshot.length():
            return self._snapshot.get_pk_hi(self._pos)
        return r_uint64(0)

    def weight(self):
        if self._pos >= self._snapshot.length():
            return r_int64(0)
        return self._snapshot.get_weight(self._pos)

    def get_accessor(self):
        self._accessor.bind(self._snapshot, self._pos)
        return self._accessor

    def estimated_length(self):
        return self._snapshot.length()

    def close(self):
        if self._snapshot is not None:
            self._snapshot.release()
            self._snapshot = None


# ---------------------------------------------------------------------------
# ShardCursor
# ---------------------------------------------------------------------------


class ShardCursor(BaseCursor):
    _immutable_fields_ = ["view", "schema", "is_u128", "accessor"]

    def __init__(self, shard_view):
        BaseCursor.__init__(self)
        self.view = shard_view
        self.schema = shard_view.schema
        self.is_u128 = self.schema.get_pk_column().field_type.code == types.TYPE_U128.code
        self.position = 0
        self.accessor = comparator.SoAAccessor(self.schema)
        self._skip_ghosts()

    def get_accessor(self):
        return self.accessor

    def _skip_ghosts(self):
        while self.position < self.view.count:
            if self.view.get_weight(self.position) != 0:
                self.accessor.set_row(self.view, self.position)
                return
            self.position += 1

    def seek(self, key_lo, key_hi):
        self.position = self.view.find_lower_bound(key_lo, key_hi)
        self._skip_ghosts()

    def advance(self):
        if not self.is_valid():
            return
        self.position += 1
        self._skip_ghosts()

    def key_lo(self):
        if not self.is_valid():
            return MAX_U64
        return self.view.get_pk_lo(self.position)

    def key_hi(self):
        if not self.is_valid():
            return MAX_U64
        return self.view.get_pk_hi(self.position)

    def peek_key_lo(self):
        if not self.is_valid():
            return r_uint64(0)
        return self.view.get_pk_lo(self.position)

    def peek_key_hi(self):
        if not self.is_valid():
            return r_uint64(0)
        return self.view.get_pk_hi(self.position)

    def weight(self):
        if not self.is_valid():
            return r_int64(0)
        return self.view.get_weight(self.position)

    def is_valid(self):
        return self.position < self.view.count

    def estimated_length(self):
        return self.view.count


def _copy_cursors(cursors):
    res = newlist_hint(len(cursors))
    for c in cursors:
        res.append(c)
    return res


# ---------------------------------------------------------------------------
# UnifiedCursor
# ---------------------------------------------------------------------------


class UnifiedCursor(AbstractCursor):
    """
    N-way merge cursor over one or more sub-cursors (MemTable + shards).
    """

    _immutable_fields_ = ["schema", "is_single_source", "tree"]

    def __init__(self, schema, cursors):
        self.schema = schema
        self.cursors = cursors
        self.num_cursors = len(cursors)
        self.is_single_source = self.num_cursors == 1

        if not self.is_single_source:
            self.tree = tournament_tree.TournamentTree(_copy_cursors(self.cursors), schema)
        else:
            self.tree = None

        # Appendix A: Split u128 into lo/hi components for alignment safety.
        self._current_key_lo = r_uint64(0)
        self._current_key_hi = r_uint64(0)
        self._current_weight = r_int64(0)

        self._current_accessor = None
        self._valid = False
        self._find_next_non_ghost()

    def _find_next_non_ghost(self):
        if self.is_single_source:
            cursor = self.cursors[0]
            if cursor.is_valid():
                self._current_key_lo = cursor.key_lo()
                self._current_key_hi = cursor.key_hi()
                self._current_weight = cursor.weight()
                self._current_accessor = cursor.get_accessor()
                self._valid = True
            else:
                self._valid = False
            return

        while not self.tree.is_exhausted():
            min_key_lo = self.tree.get_min_key_lo()
            min_key_hi = self.tree.get_min_key_hi()

            if min_key_lo == MAX_U64 and min_key_hi == MAX_U64:
                break

            num_candidates = self.tree.get_all_indices_at_min()

            net_weight = r_int64(0)
            idx = 0
            while idx < num_candidates:
                c_idx = self.tree._min_indices[idx]
                net_weight += self.cursors[c_idx].weight()
                idx += 1

            if net_weight != r_int64(0):
                self._current_key_lo = min_key_lo
                self._current_key_hi = min_key_hi
                self._current_weight = net_weight
                self._current_accessor = self.cursors[self.tree._min_indices[0]].get_accessor()
                self._valid = True
                return
            else:
                idx = 0
                while idx < num_candidates:
                    self.tree.advance_cursor_by_index(self.tree._min_indices[idx])
                    idx += 1

        self._valid = False

    def seek(self, key_lo, key_hi):
        for c in self.cursors:
            c.seek(key_lo, key_hi)
        if not self.is_single_source:
            self.tree.rebuild()
        self._find_next_non_ghost()

    def advance(self):
        if not self._valid:
            return
        if self.is_single_source:
            self.cursors[0].advance()
            self._find_next_non_ghost()
            return

        # Reuse cached indices from the last _find_next_non_ghost() call
        # instead of traversing the heap again.
        count = self.tree._min_count
        idx = 0
        while idx < count:
            self.tree.advance_cursor_by_index(self.tree._min_indices[idx])
            idx += 1

        self._find_next_non_ghost()

    def key_lo(self):
        return self._current_key_lo

    def key_hi(self):
        return self._current_key_hi

    def weight(self):
        return self._current_weight

    def is_valid(self):
        return self._valid

    def get_accessor(self):
        return self._current_accessor

    def estimated_length(self):
        total = 0
        for c in self.cursors:
            total += c.estimated_length()
        return total

    def close(self):
        if self.tree is not None:
            self.tree.close()
        for c in self.cursors:
            c.close()


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

    def __init__(self, schema, shard_views, batch_snapshot):
        from gnitz.storage import engine_ffi
        from rpython.rlib.rarithmetic import intmask

        self.schema = schema
        self._snapshot = batch_snapshot
        self._shard_views = shard_views

        num_cols = len(schema.columns)
        pk_index = schema.pk_index
        num_payload_cols = num_cols - 1
        regions_per_batch = 4 + num_payload_cols + 1

        # Pack batch snapshot regions (1 batch: the consolidated snapshot)
        num_batches = 0
        count = 0
        if batch_snapshot is not None:
            count = batch_snapshot.length()
        if count > 0:
            num_batches = 1

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

        if num_batches == 1:
            batch_counts[0] = rffi.cast(rffi.UINT, count)
            idx = 0
            batch_ptrs[idx] = rffi.cast(
                rffi.VOIDP, batch_snapshot.pk_lo_buf.base_ptr
            )
            batch_sizes[idx] = rffi.cast(rffi.UINT, count * 8)
            idx += 1
            batch_ptrs[idx] = rffi.cast(
                rffi.VOIDP, batch_snapshot.pk_hi_buf.base_ptr
            )
            batch_sizes[idx] = rffi.cast(rffi.UINT, count * 8)
            idx += 1
            batch_ptrs[idx] = rffi.cast(
                rffi.VOIDP, batch_snapshot.weight_buf.base_ptr
            )
            batch_sizes[idx] = rffi.cast(rffi.UINT, count * 8)
            idx += 1
            batch_ptrs[idx] = rffi.cast(
                rffi.VOIDP, batch_snapshot.null_buf.base_ptr
            )
            batch_sizes[idx] = rffi.cast(rffi.UINT, count * 8)
            idx += 1
            for ci in range(num_cols):
                if ci == pk_index:
                    continue
                col_sz = count * batch_snapshot.col_strides[ci]
                batch_ptrs[idx] = rffi.cast(
                    rffi.VOIDP, batch_snapshot.col_bufs[ci].base_ptr
                )
                batch_sizes[idx] = rffi.cast(rffi.UINT, col_sz)
                idx += 1
            batch_ptrs[idx] = rffi.cast(
                rffi.VOIDP, batch_snapshot.blob_arena.base_ptr
            )
            batch_sizes[idx] = rffi.cast(
                rffi.UINT, batch_snapshot.blob_arena.offset
            )

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
        est = count  # batch snapshot rows
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
        if self._snapshot is not None:
            self._snapshot.release()
            self._snapshot = None
