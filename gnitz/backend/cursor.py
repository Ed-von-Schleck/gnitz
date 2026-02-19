# gnitz/backend/cursor.py
from rpython.rlib.rarithmetic import r_int64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.core.comparator import RowAccessor

class AbstractCursor(object):
    """
    The complete read interface the VM requires from any persistent Z-Set.
    Implement this to create a new backend.
    """
    def seek(self, key):
        # key: r_uint128
        raise NotImplementedError

    def advance(self):
        raise NotImplementedError

    def is_valid(self):
        # → bool
        raise NotImplementedError

    def key(self):
        # → r_uint128
        raise NotImplementedError

    def weight(self):
        # → r_int64
        raise NotImplementedError

    def get_accessor(self):
        # → RowAccessor
        raise NotImplementedError

    def close(self):
        raise NotImplementedError
