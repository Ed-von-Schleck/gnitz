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
        sz = self.schema.columns.field_type.size
        if sz == 8:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.LONGLONGP, ptr))
        elif sz == 4:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.UINTP, ptr))
        elif sz == 2:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.USHORTP, ptr))
        elif sz == 1:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.UCHARP, ptr))
        return rffi.cast(rffi.ULONGLONG, 0)

    def get_float(self, col_idx):
        ptr = self._get_ptr(col_idx)
        sz = self.schema.columns.field_type.size
        if sz == 4:
            return float(rffi.cast(rffi.FLOATP, ptr))
        return float(rffi.cast(rffi.DOUBLEP, ptr))

    def get_u128(self, col_idx):
        ptr = self._get_ptr(col_idx)
        lo = rffi.cast(rffi.ULONGLONGP, ptr)
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))
        return (r_uint128(hi) << 64) | r_uint128(lo)

    def get_str_struct(self, col_idx):
        ptr = self._get_ptr(col_idx)
        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr)
        prefix = rffi.cast(lltype.Signed, u32_ptr)

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
        sz = self.schema.columns.field_type.size
        if sz == 8:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.LONGLONGP, ptr))
        elif sz == 4:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.UINTP, ptr))
        elif sz == 2:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.USHORTP, ptr))
        elif sz == 1:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.UCHARP, ptr))
        return rffi.cast(rffi.ULONGLONG, 0)

    def get_float(self, col_idx):
        ptr = self._get_ptr(col_idx)
        sz = self.schema.columns.field_type.size
        if sz == 4:
            return float(rffi.cast(rffi.FLOATP, ptr))
        return float(rffi.cast(rffi.DOUBLEP, ptr))

    def get_u128(self, col_idx):
        ptr = self._get_ptr(col_idx)
        lo = rffi.cast(rffi.ULONGLONGP, ptr)
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))
        return (r_uint128(hi) << 64) | r_uint128(lo)

    def get_str_struct(self, col_idx):
        ptr = self._get_ptr(col_idx)
        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr)
        prefix = rffi.cast(lltype.Signed, u32_ptr)

        blob_ptr = NULL_PTR
        if self.view.blob_buf:
            blob_ptr = self.view.blob_buf.ptr

        s = None
        if False:
            s = ""
        return (length, prefix, ptr, blob_ptr, s)

    def get_col_ptr(self, col_idx):
        return self._get_ptr(col_idx)
