from rpython.rlib.rarithmetic import r_uint, r_uint64
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long 

class DBValue(object):
    pass

class IntValue(DBValue):
    def __init__(self, v):
        self.v = r_uint64(v)
    def get_int(self):
        return int(self.v)

class FloatValue(DBValue):
    def __init__(self, v):
        self.v = float(v)
    def get_float(self):
        return self.v

class StringValue(DBValue):
    def __init__(self, v):
        self.v = str(v)
    def get_string(self):
        return self.v

class U128Value(DBValue):
    def __init__(self, v):
        self.v = r_uint128(v)
    def get_u128(self):
        return self.v

def wrap(val):
    if isinstance(val, float):
        return FloatValue(val)
    if isinstance(val, (int, long)):
        return IntValue(val)
    if isinstance(val, str):
        return StringValue(val)
    if isinstance(val, r_uint128):
        return U128Value(val)
    if isinstance(val, DBValue):
        return val
    raise TypeError("Unsupported type for DBValue: %s" % type(val))
