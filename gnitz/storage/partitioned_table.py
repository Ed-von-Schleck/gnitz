# gnitz/storage/partitioned_table.py
#
# Thin RPython wrapper over the Rust PartitionedTable opaque handle.
# Hash routing, batch splitting, and multi-partition cursor assembly
# all happen in Rust.

from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.core.store import ZSetStore
from gnitz.core.batch import ArenaZSetBatch
from gnitz.storage import engine_ffi, cursor
from gnitz.storage.memtable import _pack_batch_regions
from gnitz.core.comparator import RowAccessor
from gnitz.storage.ephemeral_table import EphemeralTable
from gnitz.catalog.system_tables import FIRST_USER_TABLE_ID

NUM_PARTITIONS = 256


def get_num_partitions(table_id):
    if table_id < FIRST_USER_TABLE_ID:
        return 1
    return NUM_PARTITIONS


class PTableFoundAccessor(RowAccessor):
    """Reads from last-found row of a Rust PartitionedTable handle."""

    _immutable_fields_ = ["_schema", "_has_nullable"]

    def __init__(self, schema, handle):
        self._schema = schema
        self._has_nullable = schema.has_nullable
        self._handle = handle
        self._null_word = r_uint64(0)

    def refresh_null_word(self):
        self._null_word = r_uint64(
            engine_ffi._ptable_found_null_word(self._handle)
        )

    def _col_ptr(self, col_idx):
        pk_index = self._schema.pk_index
        payload_col = col_idx if col_idx < pk_index else col_idx - 1
        stride = self._schema.columns[col_idx].field_type.size
        return engine_ffi._ptable_found_col_ptr(
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
        blob_ptr = engine_ffi._ptable_found_blob_ptr(self._handle)
        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr[0])
        prefix = rffi.cast(lltype.Signed, u32_ptr[1])
        s = None
        if False:
            s = ""
        return (length, prefix, ptr, blob_ptr, s)


class PartitionedTable(ZSetStore):
    _immutable_fields_ = ["schema", "num_partitions"]

    def __init__(self, handle, schema, table_id, num_partitions):
        self._handle = handle
        self.schema = schema
        self.table_id = table_id
        self.num_partitions = num_partitions
        self._retract_acc = PTableFoundAccessor(schema, handle)
        self._lookup_found_buf = lltype.malloc(rffi.INTP.TO, 1, flavor="raw")
        self.is_closed = False

    def get_schema(self):
        return self.schema

    def ingest_batch(self, batch):
        if batch.length() == 0:
            return
        sorted_batch = batch.to_sorted()
        ptrs, sizes, count, rpb = _pack_batch_regions(sorted_batch, self.schema)
        try:
            rc = intmask(engine_ffi._ptable_ingest_batch(
                self._handle, ptrs, sizes,
                rffi.cast(rffi.UINT, count),
                rffi.cast(rffi.UINT, rpb),
            ))
        finally:
            lltype.free(ptrs, flavor="raw")
            lltype.free(sizes, flavor="raw")
            if sorted_batch is not batch:
                sorted_batch.free()
        if rc < 0:
            raise errors.StorageError("ptable ingest failed (%d)" % rc)

    def ingest_batch_memonly(self, batch):
        if batch.length() == 0:
            return
        sorted_batch = batch.to_sorted()
        ptrs, sizes, count, rpb = _pack_batch_regions(sorted_batch, self.schema)
        try:
            rc = intmask(engine_ffi._ptable_ingest_batch_memonly(
                self._handle, ptrs, sizes,
                rffi.cast(rffi.UINT, count),
                rffi.cast(rffi.UINT, rpb),
            ))
        finally:
            lltype.free(ptrs, flavor="raw")
            lltype.free(sizes, flavor="raw")
            if sorted_batch is not batch:
                sorted_batch.free()
        if rc < 0:
            raise errors.StorageError("ptable ingest_memonly failed (%d)" % rc)

    def create_cursor(self):
        handle = engine_ffi._ptable_create_cursor(self._handle)
        return cursor.RustUnifiedCursor.from_handle(self.schema, handle)

    def has_pk(self, key_lo, key_hi):
        return intmask(engine_ffi._ptable_has_pk(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )) != 0

    def retract_pk(self, key_lo, key_hi, out_batch):
        weight = engine_ffi._ptable_retract_pk(
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
        ref_batch = ArenaZSetBatch(self.schema, initial_capacity=1)
        ref_batch.append_from_accessor(key_lo, key_hi, r_int64(1), accessor)
        ptrs, sizes, count, rpb = _pack_batch_regions(ref_batch, self.schema)
        try:
            w = engine_ffi._ptable_get_weight(
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

    def flush(self):
        rc = intmask(engine_ffi._ptable_flush(self._handle))
        if rc < 0:
            raise errors.StorageError("ptable flush failed (%d)" % rc)
        return ""

    def compact_if_needed(self):
        rc = intmask(engine_ffi._ptable_compact_if_needed(self._handle))
        if rc < 0:
            raise errors.StorageError("ptable compact_if_needed failed (%d)" % rc)

    def set_has_wal(self, flag):
        engine_ffi._ptable_set_has_wal(
            self._handle,
            rffi.cast(rffi.INT, 1 if flag else 0),
        )

    @property
    def current_lsn(self):
        return r_uint64(engine_ffi._ptable_current_lsn(self._handle))

    def get_max_flushed_lsn(self):
        return self.current_lsn

    def close_partitions_outside(self, start, end):
        engine_ffi._ptable_close_partitions_outside(
            self._handle,
            rffi.cast(rffi.UINT, start),
            rffi.cast(rffi.UINT, end),
        )

    def close_all_partitions(self):
        engine_ffi._ptable_close_all_partitions(self._handle)

    def get_child_base_dir(self):
        """Return the directory under which child tables should be created.
        Uses partition 0's directory as the base."""
        buf = lltype.malloc(rffi.CCHARP.TO, 4096, flavor="raw")
        n = intmask(engine_ffi._ptable_get_child_dir(self._handle, buf, rffi.cast(rffi.UINT, 4096)))
        if n <= 0:
            lltype.free(buf, flavor="raw")
            return ""
        result = rffi.charpsize2str(buf, n)
        lltype.free(buf, flavor="raw")
        return result

    def create_child(self, name, schema):
        schema_buf = engine_ffi.pack_schema(schema)
        try:
            with rffi.scoped_str2charp(name) as name_c:
                child_handle = engine_ffi._ptable_create_child(
                    self._handle, name_c,
                    rffi.cast(rffi.VOIDP, schema_buf),
                )
        finally:
            lltype.free(schema_buf, flavor="raw")
        return EphemeralTable.from_handle(child_handle, schema, self.table_id)

    def close(self):
        if self.is_closed:
            return
        self.is_closed = True
        lltype.free(self._lookup_found_buf, flavor="raw")
        engine_ffi._ptable_close(self._handle)

    def is_empty(self):
        return False  # conservative; partitioned tables are never "empty" in a meaningful sense


def _partition_arena_size(num_partitions):
    if num_partitions <= 1:
        return 1 * 1024 * 1024
    return 256 * 1024


def make_partitioned_persistent(
    directory, name, schema, table_id, num_partitions,
    part_start=0, part_end=-1,
):
    if part_end == -1:
        part_end = num_partitions
    arena_size = _partition_arena_size(num_partitions)
    schema_buf = engine_ffi.pack_schema(schema)
    try:
        with rffi.scoped_str2charp(directory) as dir_c:
            with rffi.scoped_str2charp(name) as name_c:
                handle = engine_ffi._ptable_create(
                    dir_c, name_c,
                    rffi.cast(rffi.VOIDP, schema_buf),
                    rffi.cast(rffi.UINT, table_id),
                    rffi.cast(rffi.UINT, num_partitions),
                    rffi.cast(rffi.INT, 1),
                    rffi.cast(rffi.UINT, part_start),
                    rffi.cast(rffi.UINT, part_end),
                    rffi.cast(rffi.ULONGLONG, arena_size),
                )
    finally:
        lltype.free(schema_buf, flavor="raw")
    return PartitionedTable(handle, schema, table_id, num_partitions)


def make_partitioned_ephemeral(
    directory, name, schema, table_id, num_partitions,
    part_start=0, part_end=-1,
):
    if part_end == -1:
        part_end = num_partitions
    arena_size = _partition_arena_size(num_partitions)
    schema_buf = engine_ffi.pack_schema(schema)
    try:
        with rffi.scoped_str2charp(directory) as dir_c:
            with rffi.scoped_str2charp(name) as name_c:
                handle = engine_ffi._ptable_create(
                    dir_c, name_c,
                    rffi.cast(rffi.VOIDP, schema_buf),
                    rffi.cast(rffi.UINT, table_id),
                    rffi.cast(rffi.UINT, num_partitions),
                    rffi.cast(rffi.INT, 0),
                    rffi.cast(rffi.UINT, part_start),
                    rffi.cast(rffi.UINT, part_end),
                    rffi.cast(rffi.ULONGLONG, arena_size),
                )
    finally:
        lltype.free(schema_buf, flavor="raw")
    return PartitionedTable(handle, schema, table_id, num_partitions)


def _partition_for_key(pk_lo, pk_hi):
    """Compute partition index. Kept for test_partition_basics."""
    from gnitz.core import xxh
    h = xxh.hash_u128_inline(pk_lo, pk_hi)
    return intmask(r_uint64(h) & r_uint64(0xFF))
