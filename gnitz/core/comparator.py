# gnitz/core/comparator.py

from rpython.rtyper.lltypesystem import rffi


class RowAccessor(object):
    def get_int(self, col_idx):
        raise NotImplementedError

    def get_int_signed(self, col_idx):
        return rffi.cast(rffi.LONGLONG, self.get_int(col_idx))

    def get_float(self, col_idx):
        raise NotImplementedError

    def get_u128_lo(self, col_idx):
        raise NotImplementedError

    def get_u128_hi(self, col_idx):
        raise NotImplementedError

    def get_str_struct(self, col_idx):
        raise NotImplementedError

    def get_col_ptr(self, col_idx):
        raise NotImplementedError

    def is_null(self, col_idx):
        return False


