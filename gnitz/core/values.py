# gnitz/core/values.py

from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.jit import specialize

# Constants for value types
TAG_INT = 0
TAG_FLOAT = 1
TAG_STRING = 2
TAG_NULL = 3
TAG_U128 = 4


@specialize.argtype(0)
def _make_int_impl(val):
    # intmask() truncates and forces the result to a signed machine word.
    # r_int64() ensures the storage in TaggedValue is a consistent 64-bit signed int.
    return TaggedValue(TAG_INT, r_int64(intmask(val)), r_uint64(0), 0.0, "")


class TaggedValue(object):
    """
    A tagged union representing a database value.

    Tags and their active fields:
        TAG_INT    — i64 holds the value bitcast to r_int64 (covers all types
                     from TYPE_I8 through TYPE_U64). Callers needing unsigned
                     semantics cast back with r_uint64(val.i64).
        TAG_FLOAT  — f64 holds the value.
        TAG_STRING — str_val holds the value.
        TAG_NULL   — no fields are meaningful.
        TAG_U128   — i64 holds the low 64 bits (bitcast r_uint64 → r_int64);
                     u128_hi holds the high 64 bits as r_uint64.
                     Reconstruction: (r_uint128(u128_hi) << 64) | r_uint128(r_uint64(i64))

    TYPE_U128 Primary Keys do not pass through TaggedValue; they are handled
    as native r_uint128 throughout the ingestion and cursor layers.
    """

    _immutable_fields_ = ["tag", "i64", "u128_hi", "f64", "str_val"]

    def __init__(self, tag, i, hi, f, s):
        self.tag = tag
        self.i64 = i        # r_int64  — low word for TAG_U128, full value for TAG_INT
        self.u128_hi = hi   # r_uint64 — high word for TAG_U128, zero for all other tags
        self.f64 = f        # float
        self.str_val = s    # str

    @staticmethod
    def make_int(val):
        return _make_int_impl(val)

    @staticmethod
    def make_float(val):
        return TaggedValue(TAG_FLOAT, r_int64(0), r_uint64(0), val, "")

    @staticmethod
    def make_string(val):
        return TaggedValue(TAG_STRING, r_int64(0), r_uint64(0), 0.0, val)

    @staticmethod
    def make_null():
        return TaggedValue(TAG_NULL, r_int64(0), r_uint64(0), 0.0, "")

    @staticmethod
    def make_u128(lo, hi):
        """
        Constructs a TAG_U128 value from two r_uint64 halves.

        Args:
            lo: r_uint64 — low 64 bits of the 128-bit value.
            hi: r_uint64 — high 64 bits of the 128-bit value.

        The low word is stored as r_int64 via a bitcast (no value loss).
        Reconstruction: (r_uint128(hi) << 64) | r_uint128(r_uint64(i64))
        """
        # FIX: Use intmask(lo) to explicitly authorize the unsigned -> signed bitcast
        return TaggedValue(TAG_U128, r_int64(intmask(lo)), r_uint64(hi), 0.0, "")

    def is_null(self):
        return self.tag == TAG_NULL

    def to_string(self):
        if self.tag == TAG_INT:
            return str(self.i64)
        elif self.tag == TAG_FLOAT:
            return str(self.f64)
        elif self.tag == TAG_STRING:
            return self.str_val
        elif self.tag == TAG_U128:
            return str(self.u128_hi) + ":" + str(r_uint64(self.i64))
        else:
            return "NULL"
