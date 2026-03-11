# gnitz/core/batch.py

from rpython.rlib import jit
from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import serialize, strings as string_logic, errors, types
from gnitz.core import comparator as core_comparator
from gnitz.storage import buffer

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


class BatchBlobAllocator(string_logic.BlobAllocator):
    """Strategy for writing variable-length data into the batch's blob arena."""

    def __init__(self, arena):
        self.arena = arena

    def allocate(self, string_data):
        length = len(string_data)
        dest = self.arena.alloc(length, alignment=1)
        for i in range(length):
            dest[i] = string_data[i]
        return r_uint64(
            rffi.cast(lltype.Signed, dest)
            - rffi.cast(lltype.Signed, self.arena.base_ptr)
        )

    def allocate_from_ptr(self, src_ptr, length):
        dest = self.arena.alloc(length, alignment=1)
        buffer.c_memmove(
            rffi.cast(rffi.VOIDP, dest),
            rffi.cast(rffi.VOIDP, src_ptr),
            rffi.cast(rffi.SIZE_T, length),
        )
        return r_uint64(
            rffi.cast(lltype.Signed, dest)
            - rffi.cast(lltype.Signed, self.arena.base_ptr)
        )


# ---------------------------------------------------------------------------
# Columnar Batch Accessor
# ---------------------------------------------------------------------------


class ColumnarBatchAccessor(core_comparator.RowAccessor):
    """
    Reads data from a columnar ArenaZSetBatch by row index.
    Implements the full RowAccessor interface.
    """

    _immutable_fields_ = ["_schema"]

    def __init__(self, schema):
        self._schema = schema
        self._batch = None
        self._row_idx = 0

    def bind(self, batch, row_idx):
        self._batch = batch
        self._row_idx = row_idx

    def _payload_idx(self, col_idx):
        if col_idx == self._schema.pk_index:
            return -1  # PK is not in the payload
        if col_idx < self._schema.pk_index:
            return col_idx
        return col_idx - 1

    def is_null(self, col_idx):
        if col_idx == self._schema.pk_index:
            return False  # PKs are never null
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

    def get_u128(self, col_idx):
        batch = self._batch
        assert batch is not None
        return batch._read_col_u128(self._row_idx, col_idx)

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
    """
    Reusable builder for appending rows directly into an ArenaZSetBatch.
    Eliminates intermediate row object allocations.
    Uses fixed-size arrays (index assignment, not .append()) for zero-alloc reuse.
    """

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

    def begin(self, pk, weight):
        pk_u128 = r_uint128(pk)
        self._pk_lo = r_uint64(pk_u128)
        self._pk_hi = r_uint64(pk_u128 >> 64)
        self._weight = weight
        self._curr = 0
        self._null_word = r_uint64(0)

    def put_int(self, val_i64):
        self._lo[self._curr] = val_i64
        self._curr += 1

    def put_float(self, val_f64):
        from rpython.rlib.longlong2float import float2longlong
        self._lo[self._curr] = float2longlong(val_f64)
        self._curr += 1

    def put_string(self, val_str):
        self._lo[self._curr] = r_int64(0)
        if self._strs is not None:
            self._strs[self._curr] = val_str
        self._curr += 1

    def put_u128(self, lo_u64, hi_u64):
        from rpython.rlib.rarithmetic import intmask
        self._lo[self._curr] = r_int64(intmask(lo_u64))
        if self._hi is not None:
            self._hi[self._curr] = hi_u64
        self._curr += 1

    def put_null(self):
        self._lo[self._curr] = r_int64(0)
        self._null_word = self._null_word | (r_uint64(1) << self._curr)
        self._curr += 1

    def commit(self):
        pk = (r_uint128(self._pk_hi) << 64) | r_uint128(self._pk_lo)
        self._target.append_from_accessor(pk, self._weight, self)

    # -- append_* API (used by op_map via ScalarFunction.evaluate_map) --

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
        from rpython.rlib.rarithmetic import intmask
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

    def commit_row(self, pk, weight):
        if self._curr != self._n:
            raise errors.LayoutError(
                "Map function failed to write all columns: "
                "expected %d, wrote %d" % (self._n, self._curr)
            )
        self._target.append_from_accessor(pk, weight, self)
        self._curr = 0
        self._null_word = r_uint64(0)

    # RowAccessor read interface

    def _payload_idx(self, col_idx):
        if col_idx == self._pk_index:
            return -1  # PK is not in the payload
        if col_idx < self._pk_index:
            return col_idx
        return col_idx - 1

    def is_null(self, col_idx):
        if col_idx == self._pk_index:
            return False  # PKs are never null
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

    def get_u128(self, col_idx):
        p_idx = self._payload_idx(col_idx)
        lo = r_uint64(self._lo[p_idx])
        hi = self._hi[p_idx] if self._hi is not None else r_uint64(0)
        return (r_uint128(hi) << 64) | r_uint128(lo)

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
# Mergesort Support (Raw Indices)
# ---------------------------------------------------------------------------


def _mergesort_indices(indices, batch, lo, hi, scratch):
    if hi - lo <= 1:
        return
    mid = (lo + hi) >> 1
    _mergesort_indices(indices, batch, lo, mid, scratch)
    _mergesort_indices(indices, batch, mid, hi, scratch)
    _merge_indices(indices, batch, lo, mid, hi, scratch)


def _merge_indices(indices, batch, lo, mid, hi, scratch):
    for k in range(lo, mid):
        scratch[k] = indices[k]

    i = lo
    j = mid
    k = lo

    while i < mid and j < hi:
        if batch.compare_indices(scratch[i], indices[j]) <= 0:
            indices[k] = scratch[i]
            i += 1
        else:
            indices[k] = indices[j]
            j += 1
        k += 1

    while i < mid:
        indices[k] = scratch[i]
        i += 1
        k += 1


# ---------------------------------------------------------------------------
# ArenaZSetBatch — Columnar (SoA) Layout
# ---------------------------------------------------------------------------


class ArenaZSetBatch(object):
    """
    A columnar Z-Set batch stored in per-column raw memory buffers.
    Operations like to_sorted() and to_consolidated() are functional: they
    return a new batch if work is required, leaving the original untouched.
    """

    _immutable_fields_ = ["_schema"]

    def __init__(self, schema, initial_capacity=1024):
        self._schema = schema

        self.pk_lo_buf = buffer.Buffer(initial_capacity * 8)
        self.pk_hi_buf = buffer.Buffer(initial_capacity * 8)
        self.weight_buf = buffer.Buffer(initial_capacity * 8)
        self.null_buf = buffer.Buffer(initial_capacity * 8)
        self.blob_arena = buffer.Buffer(initial_capacity * 64)

        num_cols = len(schema.columns)
        col_bufs = newlist_hint(num_cols)
        col_strides = newlist_hint(num_cols)
        for i in range(num_cols):
            if i == schema.pk_index:
                col_bufs.append(buffer.Buffer(0))
                col_strides.append(0)
            else:
                sz = schema.columns[i].field_type.size
                col_bufs.append(buffer.Buffer(initial_capacity * sz))
                col_strides.append(sz)
        self.col_bufs = col_bufs
        self.col_strides = col_strides

        self.allocator = BatchBlobAllocator(self.blob_arena)

        self._raw_accessor = ColumnarBatchAccessor(schema)
        self._cmp_acc_a = ColumnarBatchAccessor(schema)
        self._cmp_acc_b = ColumnarBatchAccessor(schema)

        self._count = 0
        self._sorted = False
        self._freed = False

    @staticmethod
    def from_buffers(
        schema, pk_lo_buf, pk_hi_buf, weight_buf, null_buf,
        col_bufs, col_strides, blob_buf, count, is_sorted=True
    ):
        """Zero-copy factory for IPC transport."""
        batch = ArenaZSetBatch(schema, initial_capacity=0)
        batch.pk_lo_buf.free()
        batch.pk_hi_buf.free()
        batch.weight_buf.free()
        batch.null_buf.free()
        batch.blob_arena.free()
        for cb in batch.col_bufs:
            cb.free()

        batch.pk_lo_buf = pk_lo_buf
        batch.pk_hi_buf = pk_hi_buf
        batch.weight_buf = weight_buf
        batch.null_buf = null_buf
        batch.col_bufs = col_bufs
        batch.col_strides = col_strides
        batch.blob_arena = blob_buf
        batch._count = count
        batch._sorted = is_sorted
        batch.allocator = BatchBlobAllocator(batch.blob_arena)
        return batch

    def length(self):
        return self._count

    def is_sorted(self):
        return self._sorted

    def is_empty(self):
        return self._count == 0

    def clear(self):
        self.pk_lo_buf.offset = 0
        self.pk_hi_buf.offset = 0
        self.weight_buf.offset = 0
        self.null_buf.offset = 0
        self.blob_arena.offset = 0
        for i in range(len(self.col_bufs)):
            self.col_bufs[i].offset = 0
        self._count = 0
        self._sorted = False

    def free(self):
        if self._freed:
            return
        self.pk_lo_buf.free()
        self.pk_hi_buf.free()
        self.weight_buf.free()
        self.null_buf.free()
        self.blob_arena.free()
        for i in range(len(self.col_bufs)):
            self.col_bufs[i].free()
        self._freed = True

    # -------------------------------------------------------------------
    # Internal column read helpers
    # -------------------------------------------------------------------

    def _read_pk_lo(self, i):
        ptr = rffi.ptradd(self.pk_lo_buf.base_ptr, i * 8)
        return rffi.cast(rffi.ULONGLONGP, ptr)[0]

    def _read_pk_hi(self, i):
        ptr = rffi.ptradd(self.pk_hi_buf.base_ptr, i * 8)
        return rffi.cast(rffi.ULONGLONGP, ptr)[0]

    def _read_weight(self, i):
        ptr = rffi.ptradd(self.weight_buf.base_ptr, i * 8)
        return rffi.cast(rffi.LONGLONGP, ptr)[0]

    def _read_null_word(self, i):
        ptr = rffi.ptradd(self.null_buf.base_ptr, i * 8)
        return rffi.cast(rffi.ULONGLONGP, ptr)[0]

    def _col_ptr(self, i, col_idx):
        stride = self.col_strides[col_idx]
        return rffi.ptradd(self.col_bufs[col_idx].base_ptr, i * stride)

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
        ptr = self._col_ptr(i, col_idx)

        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr[0])
        prefix = rffi.cast(lltype.Signed, u32_ptr[1])

        # RPython annotation: force str-or-None union type for relocate_string
        s = None
        if False:
            s = ""
        return (length, prefix, ptr, self.blob_arena.base_ptr, s)

    def _read_col_ptr(self, i, col_idx):
        return self._col_ptr(i, col_idx)

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    def get_pk(self, i):
        assert 0 <= i < self._count
        lo = self._read_pk_lo(i)
        hi = self._read_pk_hi(i)
        return (r_uint128(hi) << 64) | r_uint128(lo)

    def get_weight(self, i):
        assert 0 <= i < self._count
        return self._read_weight(i)

    def get_accessor(self, i):
        self.bind_accessor(i, self._raw_accessor)
        return self._raw_accessor

    def bind_accessor(self, i, out_accessor):
        assert 0 <= i < self._count
        out_accessor.bind(self, i)

    @jit.unroll_safe
    def append_from_accessor(self, pk, weight, accessor):
        assert not self._freed
        pk_u128 = r_uint128(pk)

        self.pk_lo_buf.put_u64(rffi.cast(rffi.ULONGLONG, r_uint64(pk_u128)))
        self.pk_hi_buf.put_u64(
            rffi.cast(rffi.ULONGLONG, r_uint64(pk_u128 >> 64))
        )
        self.weight_buf.put_i64(weight)

        # Compute null word
        null_word = r_uint64(0)
        if isinstance(accessor, ColumnarBatchAccessor):
            src_batch = accessor._batch
            if src_batch is not None:
                null_word = src_batch._read_null_word(accessor._row_idx)
        elif isinstance(accessor, RowBuilder):
            null_word = accessor._null_word
        else:
            for i in range(len(self._schema.columns)):
                if i == self._schema.pk_index:
                    continue
                if accessor.is_null(i):
                    payload_idx = i if i < self._schema.pk_index else i - 1
                    null_word |= r_uint64(1) << payload_idx

        self.null_buf.put_u64(null_word)

        # Write each column
        schema = self._schema
        num_cols = len(schema.columns)
        for ci in range(num_cols):
            if ci == schema.pk_index:
                continue

            col_type = schema.columns[ci].field_type
            stride = col_type.size
            dest = self.col_bufs[ci].alloc(stride, alignment=col_type.alignment)

            payload_idx = ci if ci < schema.pk_index else ci - 1
            if null_word & (r_uint64(1) << payload_idx):
                for b in range(stride):
                    dest[b] = "\x00"
                continue

            code = col_type.code
            if code == types.TYPE_STRING.code:
                length, prefix, src_struct_ptr, src_heap_ptr, py_string = (
                    accessor.get_str_struct(ci)
                )
                string_logic.relocate_string(
                    dest,
                    length,
                    prefix,
                    src_struct_ptr,
                    src_heap_ptr,
                    py_string,
                    self.allocator,
                )
            elif code == types.TYPE_F64.code:
                rffi.cast(rffi.DOUBLEP, dest)[0] = rffi.cast(
                    rffi.DOUBLE, accessor.get_float(ci)
                )
            elif code == types.TYPE_F32.code:
                rffi.cast(rffi.FLOATP, dest)[0] = rffi.cast(
                    rffi.FLOAT, accessor.get_float(ci)
                )
            elif code == types.TYPE_U128.code:
                v = accessor.get_u128(ci)
                rffi.cast(rffi.ULONGLONGP, dest)[0] = rffi.cast(
                    rffi.ULONGLONG, r_uint64(v)
                )
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(dest, 8))[0] = rffi.cast(
                    rffi.ULONGLONG, r_uint64(v >> 64)
                )
            elif code == types.TYPE_U64.code:
                rffi.cast(rffi.ULONGLONGP, dest)[0] = rffi.cast(
                    rffi.ULONGLONG, accessor.get_int(ci)
                )
            elif code == types.TYPE_I64.code:
                rffi.cast(rffi.LONGLONGP, dest)[0] = rffi.cast(
                    rffi.LONGLONG, accessor.get_int(ci)
                )
            elif code == types.TYPE_U32.code:
                rffi.cast(rffi.UINTP, dest)[0] = rffi.cast(
                    rffi.UINT, accessor.get_int(ci) & 0xFFFFFFFF
                )
            elif code == types.TYPE_I32.code:
                rffi.cast(rffi.UINTP, dest)[0] = rffi.cast(
                    rffi.UINT, accessor.get_int(ci) & 0xFFFFFFFF
                )
            elif code == types.TYPE_U16.code or code == types.TYPE_I16.code:
                from rpython.rlib.rarithmetic import intmask

                v16 = intmask(accessor.get_int(ci))
                dest[0] = chr(v16 & 0xFF)
                dest[1] = chr((v16 >> 8) & 0xFF)
            elif code == types.TYPE_U8.code or code == types.TYPE_I8.code:
                from rpython.rlib.rarithmetic import intmask

                dest[0] = chr(intmask(accessor.get_int(ci)) & 0xFF)
            else:
                rffi.cast(rffi.LONGLONGP, dest)[0] = rffi.cast(
                    rffi.LONGLONG, accessor.get_int(ci)
                )

        self._count += 1
        self._sorted = False

    def compare_indices(self, idx_a, idx_b):
        ahi = self._read_pk_hi(idx_a)
        bhi = self._read_pk_hi(idx_b)
        if ahi < bhi:
            return -1
        if ahi > bhi:
            return 1

        alo = self._read_pk_lo(idx_a)
        blo = self._read_pk_lo(idx_b)
        if alo < blo:
            return -1
        if alo > blo:
            return 1

        self._cmp_acc_a.bind(self, idx_a)
        self._cmp_acc_b.bind(self, idx_b)

        return core_comparator.compare_rows(
            self._schema, self._cmp_acc_a, self._cmp_acc_b
        )

    def clone(self):
        """Creates a deep copy of the batch."""
        cap = self._count if self._count > 8 else 8
        new_batch = ArenaZSetBatch(self._schema, initial_capacity=cap)
        new_batch.append_batch(self)
        new_batch._sorted = self._sorted
        return new_batch

    def append_batch(self, other, start=0, end=-1):
        """
        Bulk-append rows [start, end) from another same-schema batch.
        Uses per-column memcpy for fixed-width columns, falling back to
        per-row string relocation for string columns.
        """
        assert not self._freed
        if end == -1:
            end = other._count
        n = end - start
        if n <= 0:
            return

        # Structural columns: pk_lo, pk_hi, weight, null (all 8-byte stride)
        self.pk_lo_buf.append_from_buffer(other.pk_lo_buf, start * 8, n * 8)
        self.pk_hi_buf.append_from_buffer(other.pk_hi_buf, start * 8, n * 8)
        self.weight_buf.append_from_buffer(other.weight_buf, start * 8, n * 8)
        self.null_buf.append_from_buffer(other.null_buf, start * 8, n * 8)

        # Payload columns
        schema = self._schema
        has_varlen = schema.has_varlen
        num_cols = len(schema.columns)
        for ci in range(num_cols):
            if ci == schema.pk_index:
                continue
            col_type = schema.columns[ci].field_type
            stride = col_type.size

            if not has_varlen or col_type.code != types.TYPE_STRING.code:
                # Fixed-width: single memcpy
                self.col_bufs[ci].append_from_buffer(
                    other.col_bufs[ci], start * stride, n * stride
                )
            else:
                # Strings: per-row relocation (blob offsets differ between arenas)
                for row in range(start, end):
                    src_ptr = other._col_ptr(row, ci)
                    dest = self.col_bufs[ci].alloc(stride, alignment=col_type.alignment)
                    u32_ptr = rffi.cast(rffi.UINTP, src_ptr)
                    length = rffi.cast(lltype.Signed, u32_ptr[0])
                    prefix = rffi.cast(lltype.Signed, u32_ptr[1])
                    # RPython annotation: force str-or-None union type for relocate_string
                    s = None
                    if False:
                        s = ""
                    string_logic.relocate_string(
                        dest, length, prefix,
                        src_ptr, other.blob_arena.base_ptr,
                        s, self.allocator,
                    )

        self._count += n
        self._sorted = False

    def append_batch_negated(self, other, start=0, end=-1):
        """
        Bulk-append rows [start, end) with all weights negated.
        Uses per-column memcpy for payload, writes negated weights.
        """
        assert not self._freed
        if end == -1:
            end = other._count
        n = end - start
        if n <= 0:
            return

        # PK, null — bulk copy
        self.pk_lo_buf.append_from_buffer(other.pk_lo_buf, start * 8, n * 8)
        self.pk_hi_buf.append_from_buffer(other.pk_hi_buf, start * 8, n * 8)
        self.null_buf.append_from_buffer(other.null_buf, start * 8, n * 8)

        # Weight — negate during copy
        for row in range(start, end):
            w = other._read_weight(row)
            self.weight_buf.put_i64(r_int64(-intmask(w)))

        # Payload columns — same as append_batch
        schema = self._schema
        has_varlen = schema.has_varlen
        num_cols = len(schema.columns)
        for ci in range(num_cols):
            if ci == schema.pk_index:
                continue
            col_type = schema.columns[ci].field_type
            stride = col_type.size

            if not has_varlen or col_type.code != types.TYPE_STRING.code:
                self.col_bufs[ci].append_from_buffer(
                    other.col_bufs[ci], start * stride, n * stride
                )
            else:
                for row in range(start, end):
                    src_ptr = other._col_ptr(row, ci)
                    dest = self.col_bufs[ci].alloc(stride, alignment=col_type.alignment)
                    u32_ptr = rffi.cast(rffi.UINTP, src_ptr)
                    length = rffi.cast(lltype.Signed, u32_ptr[0])
                    prefix = rffi.cast(lltype.Signed, u32_ptr[1])
                    # RPython annotation: force str-or-None union type for relocate_string
                    s = None
                    if False:
                        s = ""
                    string_logic.relocate_string(
                        dest, length, prefix,
                        src_ptr, other.blob_arena.base_ptr,
                        s, self.allocator,
                    )

        self._count += n
        self._sorted = False

    def _direct_append_row(self, src, src_idx, weight_override):
        """
        Append a single row from src batch using direct column copy (no accessor).
        Uses weight_override instead of the source weight.
        """
        assert not self._freed
        self.pk_lo_buf.put_u64(src._read_pk_lo(src_idx))
        self.pk_hi_buf.put_u64(src._read_pk_hi(src_idx))
        self.weight_buf.put_i64(weight_override)
        self.null_buf.put_u64(src._read_null_word(src_idx))

        schema = self._schema
        has_varlen = schema.has_varlen
        num_cols = len(schema.columns)
        for ci in range(num_cols):
            if ci == schema.pk_index:
                continue
            stride = self.col_strides[ci]
            src_ptr = src._col_ptr(src_idx, ci)
            col_type = schema.columns[ci].field_type

            if not has_varlen or col_type.code != types.TYPE_STRING.code:
                dest = self.col_bufs[ci].alloc(
                    stride, alignment=col_type.alignment
                )
                buffer.c_memmove(
                    rffi.cast(rffi.VOIDP, dest),
                    rffi.cast(rffi.VOIDP, src_ptr),
                    rffi.cast(rffi.SIZE_T, stride),
                )
            else:
                dest = self.col_bufs[ci].alloc(
                    stride, alignment=col_type.alignment
                )
                u32_ptr = rffi.cast(rffi.UINTP, src_ptr)
                length = rffi.cast(lltype.Signed, u32_ptr[0])
                prefix = rffi.cast(lltype.Signed, u32_ptr[1])
                # RPython annotation: force str-or-None union type for relocate_string
                s = None
                if False:
                    s = ""
                string_logic.relocate_string(
                    dest, length, prefix,
                    src_ptr, src.blob_arena.base_ptr,
                    s, self.allocator,
                )

        self._count += 1
        self._sorted = False

    def to_sorted(self):
        """
        Functional sort. Returns a new batch if sorting is needed,
        otherwise returns self.
        """
        if self._count <= 1 or self._sorted:
            self._sorted = True
            return self

        indices = newlist_hint(self._count)
        scratch = newlist_hint(self._count)
        for i in range(self._count):
            indices.append(i)
            scratch.append(0)

        _mergesort_indices(indices, self, 0, self._count, scratch)

        new_batch = ArenaZSetBatch(self._schema, initial_capacity=self._count)
        has_varlen = self._schema.has_varlen

        for i in range(self._count):
            old_idx = indices[i]

            # Copy pk_lo
            new_batch.pk_lo_buf.put_u64(self._read_pk_lo(old_idx))
            # Copy pk_hi
            new_batch.pk_hi_buf.put_u64(self._read_pk_hi(old_idx))
            # Copy weight
            new_batch.weight_buf.put_i64(self._read_weight(old_idx))
            # Copy null word
            null_w = self._read_null_word(old_idx)
            new_batch.null_buf.put_u64(null_w)

            # Copy each column
            schema = self._schema
            num_cols = len(schema.columns)
            for ci in range(num_cols):
                if ci == schema.pk_index:
                    continue
                stride = self.col_strides[ci]
                src_ptr = self._col_ptr(old_idx, ci)
                col_type = schema.columns[ci].field_type

                if not has_varlen or col_type.code != types.TYPE_STRING.code:
                    dest = new_batch.col_bufs[ci].alloc(
                        stride, alignment=col_type.alignment
                    )
                    buffer.c_memmove(
                        rffi.cast(rffi.VOIDP, dest),
                        rffi.cast(rffi.VOIDP, src_ptr),
                        rffi.cast(rffi.SIZE_T, stride),
                    )
                else:
                    dest = new_batch.col_bufs[ci].alloc(
                        stride, alignment=col_type.alignment
                    )
                    u32_ptr = rffi.cast(rffi.UINTP, src_ptr)
                    length = rffi.cast(lltype.Signed, u32_ptr[0])
                    prefix = rffi.cast(lltype.Signed, u32_ptr[1])
                    # RPython annotation: force str-or-None union type for relocate_string
                    s = None
                    if False:
                        s = ""
                    string_logic.relocate_string(
                        dest,
                        length,
                        prefix,
                        src_ptr,
                        self.blob_arena.base_ptr,
                        s,
                        new_batch.allocator,
                    )

            new_batch._count += 1

        new_batch._sorted = True
        return new_batch

    def to_consolidated(self):
        """
        Functional consolidation. Returns a new consolidated batch.
        Uses direct column copy instead of accessor dispatch.
        """
        if self._count == 0:
            return self

        sorted_view = self.to_sorted()

        res = ArenaZSetBatch(self._schema, initial_capacity=sorted_view._count)

        acc_a = ColumnarBatchAccessor(self._schema)
        acc_b = ColumnarBatchAccessor(self._schema)

        i = 0
        while i < sorted_view._count:
            pk_i_lo = sorted_view._read_pk_lo(i)
            pk_i_hi = sorted_view._read_pk_hi(i)
            weight_acc = sorted_view._read_weight(i)

            acc_a.bind(sorted_view, i)

            j = i + 1
            while j < sorted_view._count:
                if (
                    sorted_view._read_pk_lo(j) != pk_i_lo
                    or sorted_view._read_pk_hi(j) != pk_i_hi
                ):
                    break

                acc_b.bind(sorted_view, j)

                if core_comparator.compare_rows(self._schema, acc_a, acc_b) != 0:
                    break

                weight_acc += sorted_view._read_weight(j)
                j += 1

            if weight_acc != 0:
                res._direct_append_row(sorted_view, i, weight_acc)

            i = j

        if sorted_view is not self:
            sorted_view.free()

        res._sorted = True
        return res


# ---------------------------------------------------------------------------
# Output Capability Security
# ---------------------------------------------------------------------------


class BatchWriter(object):
    """
    A strictly write-only facade for an output register.
    Guarantees the destination is empty upon creation and restricts the API
    to prevent operators from accidentally reading or mutating existing data.
    """

    _immutable_fields_ = ["_batch"]

    def __init__(self, batch):
        if batch.length() != 0:
            raise errors.StorageError(
                "FATAL: Operator output register is not empty. "
                "The VM must clear destination registers before evaluation."
            )
        self._batch = batch

    def get_schema(self):
        """Returns the schema expected by this destination batch."""
        return self._batch._schema

    @jit.unroll_safe
    def append_from_accessor(self, pk, weight, accessor):
        self._batch.append_from_accessor(pk, weight, accessor)

    def append_batch(self, other, start=0, end=-1):
        """Bulk column copy from another same-schema batch."""
        self._batch.append_batch(other, start, end)

    def append_batch_negated(self, other, start=0, end=-1):
        """Bulk column copy with negated weights."""
        self._batch.append_batch_negated(other, start, end)

    def direct_append_row(self, src_batch, src_idx, weight):
        """Single-row direct column copy (no accessor dispatch)."""
        self._batch._direct_append_row(src_batch, src_idx, weight)


# ---------------------------------------------------------------------------
# Scope Management (RAII-style for RPython)
# ---------------------------------------------------------------------------


class BatchScope(object):
    """
    Context manager base that ensures functional batch copies are
    properly freed at the end of an operator's execution scope.
    """

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ZSetBatch = ArenaZSetBatch
