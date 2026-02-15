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
        return r_uint128(0)

class IntValue(DBValue):
    _immutable_fields_ = ['v']
    def __init__(self, v):
        # Explicit cast to fixed-width to avoid RPython annotation mixing
        self.v = r_uint64(v)
    
    def get_int(self):
        return self.v

class FloatValue(DBValue):
    _immutable_fields_ = ['v']
    def __init__(self, v):
        self.v = float(v)
    
    def get_float(self):
        return self.v

class StringValue(DBValue):
    _immutable_fields_ = ['v']
    def __init__(self, v):
        # RPython: self.v is a non-nullable string
        self.v = v
    
    def get_string(self):
        return self.v

class U128Value(DBValue):
    _immutable_fields_ = ['v_lo', 'v_hi']
    def __init__(self, v):
        # v is a stack-local r_uint128; splitting prevents GC-heap alignment issues
        self.v_lo = r_uint64(v)
        self.v_hi = r_uint64(v >> 64)
    
    def get_u128(self):
        return (r_uint128(self.v_hi) << 64) | r_uint128(self.v_lo)

def wrap(val):
    """
    Wraps a Python primitive into a DBValue.
    """
    if isinstance(val, float):
        return FloatValue(val)
    if isinstance(val, str):
        return StringValue(val)
    if isinstance(val, r_uint128):
        return U128Value(val)
    # RPython: int is a machine-word. 
    # Large literals passed in tests are handled here.
    if isinstance(val, int):
        return IntValue(r_uint64(val))
    if isinstance(val, DBValue):
        return val
        
    # Fallback to avoid FrozenDesc/Annotation errors
    raise TypeError("Unsupported type for DBValue wrap")
