from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128 

class DBValue(object):
    """
    Base class for all GnitzDB types.
    Provides a polymorphic interface for RPython translation.
    """
    def get_int(self): 
        return r_uint64(0)
    
    def get_float(self): 
        return 0.0
    
    def get_string(self): 
        return ""
    
    def get_u128(self): 
        # R_uint128 is treated as a distinct type in RPython
        return r_uint128(0)

class IntValue(DBValue):
    def __init__(self, v):
        self.v = r_uint64(v)
    
    def get_int(self):
        return self.v

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
    """
    Wraps a Python primitive into a DBValue.
    Used primarily for ingestion and testing.
    """
    if isinstance(val, float):
        return FloatValue(val)
    if isinstance(val, (int, long)):
        # Check if it might be 128-bit.
        # RPython: comparing a Python long against a machine constant works.
        if val > r_uint64(-1):
            return U128Value(val)
        return IntValue(val)
    if isinstance(val, str):
        return StringValue(val)
    if isinstance(val, r_uint128):
        return U128Value(val)
    if isinstance(val, DBValue):
        return val
    raise TypeError("Unsupported type for DBValue")
