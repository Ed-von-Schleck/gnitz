# gnitz/core/values.py

from rpython.rlib.rarithmetic import r_int64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128 
from rpython.rlib.jit import specialize

# Constants for value types
TAG_INT = 0
TAG_FLOAT = 1
TAG_STRING = 2
TAG_NULL = 3

@specialize.argtype(0)
def _make_int_impl(val):
    # intmask() truncates and forces the result to a signed machine word.
    # r_int64() ensures the storage in TaggedValue is a consistent 64-bit signed int.
    return TaggedValue(TAG_INT, r_int64(intmask(val)), 0.0, "")

class TaggedValue(object):
    """
    A tagged union representing a database value.
    Designed for zero-allocation ingestion and JIT virtualization.
    """
    _immutable_fields_ = ['tag', 'i64', 'f64', 'str_val']

    def __init__(self, tag, i, f, s):
        self.tag = tag
        self.i64 = i     # Must be r_int64
        self.f64 = f     # Must be float
        self.str_val = s # Must be string

    @staticmethod
    def make_int(val):
        return _make_int_impl(val)

    @staticmethod
    def make_float(val):
        return TaggedValue(TAG_FLOAT, r_int64(0), val, "")

    @staticmethod
    def make_string(val):
        return TaggedValue(TAG_STRING, r_int64(0), 0.0, val)

    @staticmethod
    def make_null():
        return TaggedValue(TAG_NULL, r_int64(0), 0.0, "")

    def is_null(self):
        return self.tag == TAG_NULL

    def to_string(self):
        if self.tag == TAG_INT:
            return str(self.i64)
        elif self.tag == TAG_FLOAT:
            return str(self.f64)
        elif self.tag == TAG_STRING:
            return self.str_val
        else:
            return "NULL"
