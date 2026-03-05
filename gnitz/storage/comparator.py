# gnitz/storage/comparator.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, strings as string_logic
from gnitz.core.comparator import RowAccessor

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


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
