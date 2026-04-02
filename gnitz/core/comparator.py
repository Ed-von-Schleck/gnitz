# gnitz/core/comparator.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from gnitz.core.strings import NULL_PTR


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

    def get_blob_source(self):
        """Return (blob_ptr, blob_len) for the backing blob arena.

        Default returns (NULL, 0).  Subclasses backed by a Rust batch or
        cursor override this so that append_from_accessor can pass the
        blob to gnitz_batch_append_row for German-string relocation.
        """
        return NULL_PTR, 0


