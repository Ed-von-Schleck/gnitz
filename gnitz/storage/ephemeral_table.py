# gnitz/storage/ephemeral_table.py
#
# Thin RPython wrapper over the Rust Table opaque handle.
# The Rust side owns MemTable + ShardIndex + optional WAL writer.
# RPython keeps a small accumulator for single-row appends (upsert_single).

import os
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, errors
from gnitz.core.comparator import RowAccessor
from gnitz.core.store import ZSetStore
from gnitz.core.keys import promote_to_index_key
from gnitz.core.batch import ArenaZSetBatch
from gnitz.storage import engine_ffi, cursor
from gnitz.storage.memtable import _pack_batch_regions, ACCUMULATOR_THRESHOLD


# ---------------------------------------------------------------------------
# TableFoundAccessor — reads from the last-found row of a Rust Table
# ---------------------------------------------------------------------------


class TableFoundAccessor(RowAccessor):
    """Reads column data from the last-found row of a Rust Table handle.

    Valid only after retract_pk returned found=True and before the next mutation.
    """

    _immutable_fields_ = ["_schema", "_has_nullable"]

    def __init__(self, schema, handle):
        self._schema = schema
        self._has_nullable = schema.has_nullable
        self._handle = handle
        self._null_word = r_uint64(0)

    def refresh_null_word(self):
        self._null_word = r_uint64(
            engine_ffi._table_found_null_word(self._handle)
        )

    def _col_ptr(self, col_idx):
        pk_index = self._schema.pk_index
        payload_col = col_idx if col_idx < pk_index else col_idx - 1
        stride = self._schema.columns[col_idx].field_type.size
        return engine_ffi._table_found_col_ptr(
            self._handle,
            rffi.cast(rffi.INT, payload_col),
            rffi.cast(rffi.INT, stride),
        )

    def is_null(self, col_idx):
        if col_idx == self._schema.pk_index:
            return False
        if not self._has_nullable:
            return False
        payload_idx = col_idx if col_idx < self._schema.pk_index else col_idx - 1
        return bool(self._null_word & (r_uint64(1) << payload_idx))

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

    def get_u128_lo(self, col_idx):
        ptr = self._col_ptr(col_idx)
        return rffi.cast(rffi.ULONGLONGP, ptr)[0]

    def get_u128_hi(self, col_idx):
        ptr = self._col_ptr(col_idx)
        return rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]

    def get_str_struct(self, col_idx):
        ptr = self._col_ptr(col_idx)
        blob_ptr = engine_ffi._table_found_blob_ptr(self._handle)
        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr[0])
        prefix = rffi.cast(lltype.Signed, u32_ptr[1])
        s = None
        if False:
            s = ""
        return (length, prefix, ptr, blob_ptr, s)


# ---------------------------------------------------------------------------
# EphemeralTable — thin wrapper over Rust Table handle
# ---------------------------------------------------------------------------


class EphemeralTable(ZSetStore):
    """Thin RPython wrapper over the Rust Table opaque handle.

    The Rust handle owns MemTable + ShardIndex + optional WAL.
    RPython keeps only a small accumulator for single-row appends.
    """

    _immutable_fields_ = ["schema", "table_id"]

    def __init__(
        self,
        directory,
        name,
        schema,
        table_id=0,
        memtable_arena_size=1 * 1024 * 1024,
        validate_checksums=False,
        durable=False,
    ):
        self.schema = schema
        self.table_id = table_id
        self.directory = directory
        self.name = name

        schema_buf = engine_ffi.pack_schema(schema)
        try:
            with rffi.scoped_str2charp(directory) as dir_c:
                with rffi.scoped_str2charp(name) as name_c:
                    self._handle = engine_ffi._table_create(
                        dir_c, name_c,
                        rffi.cast(rffi.VOIDP, schema_buf),
                        rffi.cast(rffi.UINT, table_id),
                        rffi.cast(rffi.ULONGLONG, memtable_arena_size),
                        rffi.cast(rffi.INT, 1 if durable else 0),
                    )
        finally:
            lltype.free(schema_buf, flavor="raw")

        initial_capacity = memtable_arena_size // (schema.memtable_stride + 32)
        if initial_capacity < 16:
            initial_capacity = 16
        self._accumulator = ArenaZSetBatch(schema, initial_capacity=initial_capacity)
        self._retract_acc = TableFoundAccessor(schema, self._handle)
        self._lookup_found_buf = lltype.malloc(rffi.INTP.TO, 1, flavor="raw")
        self.is_closed = False
        self._has_wal = durable

    @staticmethod
    def from_handle(handle, schema, table_id):
        """Wrap an existing Rust Table handle (e.g., from create_child).

        Creates a thin wrapper without constructing a new Rust Table.
        We use a regular __init__ call with a dummy directory, then replace
        the handle.  The dummy __init__ creates a Rust Table in /tmp that
        is immediately closed and replaced.
        """
        import os
        dummy_dir = "/tmp/_gnitz_dummy_%d" % os.getpid()
        obj = EphemeralTable(dummy_dir, "dummy", schema, table_id=table_id)
        engine_ffi._table_close(obj._handle)
        obj._handle = handle
        obj._retract_acc = TableFoundAccessor(schema, handle)
        return obj

    def get_schema(self):
        return self.schema

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def ingest_batch(self, batch):
        if batch.length() == 0:
            return
        sorted_batch = batch.to_sorted()
        ptrs, sizes, count, rpb = _pack_batch_regions(sorted_batch, self.schema)
        try:
            rc = intmask(engine_ffi._table_ingest_batch(
                self._handle, ptrs, sizes,
                rffi.cast(rffi.UINT, count),
                rffi.cast(rffi.UINT, rpb),
            ))
        finally:
            lltype.free(ptrs, flavor="raw")
            lltype.free(sizes, flavor="raw")
            if sorted_batch is not batch:
                sorted_batch.free()
        if rc == -2:
            raise errors.MemTableFullError()

    def ingest_batch_memonly(self, batch):
        if batch.length() == 0:
            return
        sorted_batch = batch.to_sorted()
        ptrs, sizes, count, rpb = _pack_batch_regions(sorted_batch, self.schema)
        try:
            rc = intmask(engine_ffi._table_ingest_batch_memonly(
                self._handle, ptrs, sizes,
                rffi.cast(rffi.UINT, count),
                rffi.cast(rffi.UINT, rpb),
            ))
        finally:
            lltype.free(ptrs, flavor="raw")
            lltype.free(sizes, flavor="raw")
            if sorted_batch is not batch:
                sorted_batch.free()
        if rc == -2:
            raise errors.MemTableFullError()

    def upsert_single(self, key_lo, key_hi, weight, accessor):
        """Buffer a single row in the RPython accumulator."""
        self._accumulator.append_from_accessor(key_lo, key_hi, weight, accessor)
        engine_ffi._table_bloom_add(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )
        if self._accumulator.length() >= ACCUMULATOR_THRESHOLD:
            self._flush_accumulator()

    def _flush_accumulator(self):
        if self._accumulator.length() == 0:
            return
        sorted_acc = self._accumulator.to_sorted()
        ptrs, sizes, count, rpb = _pack_batch_regions(sorted_acc, self.schema)
        try:
            engine_ffi._table_memtable_upsert_batch(
                self._handle, ptrs, sizes,
                rffi.cast(rffi.UINT, count),
                rffi.cast(rffi.UINT, rpb),
            )
        finally:
            lltype.free(ptrs, flavor="raw")
            lltype.free(sizes, flavor="raw")
        if sorted_acc is not self._accumulator:
            sorted_acc.free()
        self._accumulator.free()
        self._accumulator = ArenaZSetBatch(self.schema)

    def ingest_projection(
        self, source_batch, source_col_idx, source_col_type, payload_accessor, is_unique
    ):
        n = source_batch.length()
        if n == 0:
            return
        acc = source_batch.get_accessor(0)
        for i in range(n):
            source_batch.bind_accessor(i, acc)
            if acc.is_null(source_col_idx):
                continue
            weight = source_batch.get_weight(i)
            if weight == r_int64(0):
                continue
            index_key = promote_to_index_key(acc, source_col_idx, source_col_type)
            index_key_lo = r_uint64(intmask(index_key))
            index_key_hi = r_uint64(intmask(index_key >> 64))
            if is_unique and weight > r_int64(0):
                self._flush_accumulator()
                if self.has_pk(index_key_lo, index_key_hi):
                    raise errors.LayoutError(
                        "Unique index violation on column index %d" % source_col_idx
                    )
            source_pk = source_batch.get_pk(i)
            payload_accessor.pk_lo = r_uint64(intmask(source_pk))
            payload_accessor.pk_hi = r_uint64(intmask(source_pk >> 64))
            try:
                self.upsert_single(index_key_lo, index_key_hi, weight, payload_accessor)
            except errors.MemTableFullError:
                self.flush()
                self.upsert_single(index_key_lo, index_key_hi, weight, payload_accessor)

    def ingest_one(self, key_lo, key_hi, weight, accessor):
        try:
            self.upsert_single(key_lo, key_hi, weight, accessor)
        except errors.MemTableFullError:
            self.flush()
            self.upsert_single(key_lo, key_hi, weight, accessor)

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def create_cursor(self):
        self._flush_accumulator()
        handle = engine_ffi._table_create_cursor(self._handle)
        return cursor.RustUnifiedCursor.from_handle(self.schema, handle)

    def has_pk(self, key_lo, key_hi):
        self._flush_accumulator()
        return intmask(engine_ffi._table_has_pk(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )) != 0

    def retract_pk(self, key_lo, key_hi, out_batch):
        self._flush_accumulator()
        weight = engine_ffi._table_retract_pk(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
            self._lookup_found_buf,
        )
        found = intmask(self._lookup_found_buf[0]) != 0
        if not found:
            return False
        self._retract_acc.refresh_null_word()
        out_batch.append_from_accessor(key_lo, key_hi, r_int64(-1), self._retract_acc)
        return True

    def get_weight(self, key_lo, key_hi, accessor):
        self._flush_accumulator()
        ref_batch = ArenaZSetBatch(self.schema, initial_capacity=1)
        ref_batch.append_from_accessor(key_lo, key_hi, r_int64(1), accessor)
        ptrs, sizes, count, rpb = _pack_batch_regions(ref_batch, self.schema)
        try:
            w = engine_ffi._table_get_weight(
                self._handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                ptrs, sizes,
                rffi.cast(rffi.UINT, count),
                rffi.cast(rffi.UINT, rpb),
            )
        finally:
            lltype.free(ptrs, flavor="raw")
            lltype.free(sizes, flavor="raw")
            ref_batch.free()
        return r_int64(w)

    # ------------------------------------------------------------------
    # Flush / compact / lifecycle
    # ------------------------------------------------------------------

    def flush(self):
        self._flush_accumulator()
        engine_ffi._table_flush(self._handle)
        return ""

    def compact_if_needed(self):
        engine_ffi._table_compact_if_needed(self._handle)

    def create_child(self, name, schema):
        child_dir = os.path.join(self.directory, "scratch_" + name)
        return EphemeralTable(
            child_dir, name, schema,
            table_id=self.table_id,
            memtable_arena_size=256 * 1024,
        )

    def is_empty(self):
        return (
            self._accumulator.length() == 0
            and intmask(engine_ffi._table_memtable_is_empty(self._handle)) != 0
        )

    def close(self):
        if self.is_closed:
            return
        self.is_closed = True
        self._accumulator.free()
        lltype.free(self._lookup_found_buf, flavor="raw")
        engine_ffi._table_close(self._handle)

    @property
    def current_lsn(self):
        return r_uint64(engine_ffi._table_current_lsn(self._handle))

    def set_has_wal(self, flag):
        self._has_wal = flag
        engine_ffi._table_set_has_wal(
            self._handle,
            rffi.cast(rffi.INT, 1 if flag else 0),
        )
