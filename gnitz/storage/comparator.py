# gnitz/storage/comparator.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, strings as string_logic, values as db_values
from gnitz.core.comparator import RowAccessor, ValueAccessor
from gnitz.storage.memtable_node import node_get_payload_ptr

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


class PackedNodeAccessor(RowAccessor):
    """
    Accesses data from a row-oriented (AoS) MemTable node in raw memory.
    """

    def __init__(self, schema, blob_base_ptr):
        self.schema = schema
        self.blob_base_ptr = blob_base_ptr
        self.key_size = schema.get_pk_column().field_type.size
        self.payload_ptr = NULL_PTR

    def set_row(self, base_ptr, node_off):
        """MemTable specific: uses base ptr + arena offset."""
        self.payload_ptr = node_get_payload_ptr(base_ptr, node_off, self.key_size)

    def _get_ptr(self, col_idx):
        off = self.schema.get_column_offset(col_idx)
        return rffi.ptradd(self.payload_ptr, off)

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

    def get_str_struct(self, col_idx):
        ptr = self._get_ptr(col_idx)
        if not ptr:
            return (0, 0, NULL_PTR, self.blob_base_ptr, None)

        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr[0])
        prefix = rffi.cast(lltype.Signed, u32_ptr[1])

        # Annotator hint: ensure the 5th element is Optional to match
        # the VM's BatchAccessor.
        s = None
        if False:
            s = ""
        return (length, prefix, ptr, self.blob_base_ptr, s)

    def get_col_ptr(self, col_idx):
        return self._get_ptr(col_idx)


class SoAAccessor(RowAccessor):
    """
    Accesses data from a column-oriented (SoA) Table Shard View in raw memory.
    """

    def __init__(self, schema):
        self.schema = schema
        self.view = None
        self.row_idx = 0

    def set_row(self, view, row_idx):
        """Shard specific: uses ShardView + row index."""
        self.view = view
        self.row_idx = row_idx

    def _get_ptr(self, col_idx):
        return self.view.get_col_ptr(self.row_idx, col_idx)

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

    def get_str_struct(self, col_idx):
        ptr = self._get_ptr(col_idx)
        blob_ptr = NULL_PTR
        if self.view.blob_buf:
            blob_ptr = self.view.blob_buf.ptr

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


class RawWALAccessor(RowAccessor):
    """
    Implementation that reads directly from a RawWALRecord's raw pointers.
    Enables zero-copy WAL recovery by mapping schema offsets onto the WAL record.
    Also used by TableShardWriter for zero-copy MemTable flushing.
    """

    def __init__(self, schema):
        self.schema = schema
        self.pk_index = schema.pk_index
        self.null_word = r_uint64(0)
        self.payload_ptr = NULL_PTR
        self.heap_ptr = NULL_PTR

    def _payload_idx(self, col_idx):
        # Maps absolute schema column index to 0-based payload column index (skipping PK)
        if col_idx < self.pk_index:
            return col_idx
        return col_idx - 1

    def set_record(self, raw_record):
        """Sets state from a RawWALRecord object."""
        self.set_pointers(raw_record.payload_ptr, raw_record.heap_ptr, raw_record.null_word)

    def set_pointers(self, payload_ptr, heap_ptr, null_word=r_uint64(0)):
        """Sets state directly from raw pointers (e.g. MemTable flush)."""
        self.null_word = null_word
        self.payload_ptr = payload_ptr
        self.heap_ptr = heap_ptr

    def is_null(self, col_idx):
        payload_idx = self._payload_idx(col_idx)
        return bool(self.null_word & (r_uint64(1) << payload_idx))

    def _get_ptr(self, col_idx):
        off = self.schema.get_column_offset(col_idx)
        return rffi.ptradd(self.payload_ptr, off)

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

    def get_str_struct(self, col_idx):
        ptr = self._get_ptr(col_idx)
        if not ptr:
            return (0, 0, NULL_PTR, self.heap_ptr, None)

        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr[0])
        prefix = rffi.cast(lltype.Signed, u32_ptr[1])

        s = None
        if False:
            s = ""
        return (length, prefix, ptr, self.heap_ptr, s)

    def get_col_ptr(self, col_idx):
        return self._get_ptr(col_idx)
