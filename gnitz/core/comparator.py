# gnitz/core/comparator.py

from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import strings as string_logic, types


class RowAccessor(object):
    """
    Abstract base for accessing row data across different physical formats.
    """

    def get_int(self, col_idx):
        # Returns r_uint64 (unsigned bit pattern)
        raise NotImplementedError

    def get_float(self, col_idx):
        raise NotImplementedError

    def get_u128(self, col_idx):
        raise NotImplementedError

    def get_str_struct(self, col_idx):
        # Returns (length, prefix, struct_ptr, heap_ptr, py_string)
        raise NotImplementedError

    def get_col_ptr(self, col_idx):
        raise NotImplementedError

    def is_null(self, col_idx):
        """Returns True if the column value is logically null."""
        return False


class PayloadRowAccessor(RowAccessor):
    """Accesses data from an in-memory PayloadRow object."""

    _immutable_fields_ = ["schema", "_row"]

    def __init__(self, schema):
        self.schema = schema
        self._row = None
        self.pk_index = schema.pk_index

    def set_row(self, row):
        self._row = row

    def _payload_idx(self, col_idx):
        if col_idx < self.pk_index:
            return col_idx
        return col_idx - 1

    def is_null(self, col_idx):
        if self._row is None:
            return True
        return self._row.is_null(self._payload_idx(col_idx))

    def get_int(self, col_idx):
        return self._row.get_int(self._payload_idx(col_idx))

    def get_float(self, col_idx):
        return self._row.get_float(self._payload_idx(col_idx))

    def get_u128(self, col_idx):
        return self._row.get_u128(self._payload_idx(col_idx))

    def get_str_struct(self, col_idx):
        s = self._row.get_str(self._payload_idx(col_idx))

        prefix = rffi.cast(lltype.Signed, string_logic.compute_prefix(s))
        return (
            len(s),
            prefix,
            lltype.nullptr(rffi.CCHARP.TO),
            lltype.nullptr(rffi.CCHARP.TO),
            s,
        )

    def get_col_ptr(self, col_idx):
        # Not supported for Python objects
        return lltype.nullptr(rffi.CCHARP.TO)


class ValueAccessor(PayloadRowAccessor):
    """Alias for PayloadRowAccessor used in comparisons."""

    pass


@jit.unroll_safe
def compare_rows(schema, acc1, acc2):
    """
    Generic row comparator using RowAccessors.
    Handles all Gnitz types, including German String content checks and
    correct signed-integer comparison.
    """
    num_cols = len(schema.columns)
    signed_ints = (types.TYPE_I64, types.TYPE_I32, types.TYPE_I16, types.TYPE_I8)

    for i in range(num_cols):
        if i == schema.pk_index:
            continue

        # 1. Null Handling (Null < Not Null)
        n1 = acc1.is_null(i)
        n2 = acc2.is_null(i)
        if n1 and n2:
            continue
        if n1:
            return -1
        if n2:
            return 1

        # 2. Type-specific comparison
        col_type = schema.columns[i].field_type
        if col_type == types.TYPE_STRING:
            l1, p1, ptr1, h1, s1 = acc1.get_str_struct(i)
            l2, p2, ptr2, h2, s2 = acc2.get_str_struct(i)
            res = string_logic.compare_structures(
                l1, p1, ptr1, h1, s1, l2, p2, ptr2, h2, s2
            )
            if res != 0:
                return res
        elif col_type == types.TYPE_U128:
            v1 = acc1.get_u128(i)
            v2 = acc2.get_u128(i)
            if v1 < v2:
                return -1
            if v1 > v2:
                return 1
        elif col_type in (types.TYPE_F64, types.TYPE_F32):
            v1 = acc1.get_float(i)
            v2 = acc2.get_float(i)
            if v1 < v2:
                return -1
            if v1 > v2:
                return 1
        elif col_type in signed_ints:
            # Reinterpret unsigned bit pattern as signed for correct comparison
            v1 = rffi.cast(rffi.LONGLONG, acc1.get_int(i))
            v2 = rffi.cast(rffi.LONGLONG, acc2.get_int(i))
            if v1 < v2:
                return -1
            if v1 > v2:
                return 1
        else:
            # Unsigned integer comparison
            v1 = acc1.get_int(i)
            v2 = acc2.get_int(i)
            if v1 < v2:
                return -1
            if v1 > v2:
                return 1

    return 0
