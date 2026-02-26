# gnitz/core/comparator.py

from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import strings as string_logic, types


class RowAccessor(object):
    def get_int(self, col_idx):
        raise NotImplementedError

    def get_int_signed(self, col_idx):
        return rffi.cast(rffi.LONGLONG, self.get_int(col_idx))

    def get_float(self, col_idx):
        raise NotImplementedError

    def get_u128(self, col_idx):
        raise NotImplementedError

    def get_str_struct(self, col_idx):
        raise NotImplementedError

    def get_col_ptr(self, col_idx):
        raise NotImplementedError

    def is_null(self, col_idx):
        return False


class PayloadRowAccessor(RowAccessor):
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

    def get_int_signed(self, col_idx):
        return self._row.get_int_signed(self._payload_idx(col_idx))

    def get_float(self, col_idx):
        return self._row.get_float(self._payload_idx(col_idx))

    def get_u128(self, col_idx):
        return self._row.get_u128(self._payload_idx(col_idx))

    def get_str_struct(self, col_idx):
        s = self._row.get_str(self._payload_idx(col_idx))
        # compute_prefix returns LONGLONG, so the tuple is consistent
        prefix = string_logic.compute_prefix(s)
        return (
            len(s),
            prefix,
            lltype.nullptr(rffi.CCHARP.TO),
            lltype.nullptr(rffi.CCHARP.TO),
            s,
        )

    def get_col_ptr(self, col_idx):
        return lltype.nullptr(rffi.CCHARP.TO)


@jit.unroll_safe
def compare_rows(schema, acc1, acc2):
    num_cols = len(schema.columns)

    for i in range(num_cols):
        if i == schema.pk_index:
            continue

        n1 = acc1.is_null(i)
        n2 = acc2.is_null(i)
        if n1 and n2:
            continue
        if n1:
            return -1
        if n2:
            return 1

        col_type = schema.columns[i].field_type
        if col_type.code == types.TYPE_STRING.code:
            # Both acc1 and acc2 now return identical types in the tuple
            l1, p1, ptr1, h1, s1 = acc1.get_str_struct(i)
            l2, p2, ptr2, h2, s2 = acc2.get_str_struct(i)
            
            res = string_logic.compare_structures(
                l1, p1, ptr1, h1, s1,
                l2, p2, ptr2, h2, s2
            )
            if res != 0:
                return res
        elif col_type.code == types.TYPE_U128.code:
            v1 = acc1.get_u128(i)
            v2 = acc2.get_u128(i)
            if v1 < v2:
                return -1
            if v1 > v2:
                return 1
        elif col_type.code == types.TYPE_F64.code or col_type.code == types.TYPE_F32.code:
            v1 = acc1.get_float(i)
            v2 = acc2.get_float(i)
            if v1 < v2:
                return -1
            if v1 > v2:
                return 1
        else:
            v1 = acc1.get_int_signed(i)
            v2 = acc2.get_int_signed(i)
            if v1 < v2:
                return -1
            if v1 > v2:
                return 1

    return 0
