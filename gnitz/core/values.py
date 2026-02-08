"""
gnitz/core/values.py
Wrapper classes for Z-Set values to ensure strict typing in RPython.
"""

class DBValue(object):
    """Base class for database values."""
    pass

class IntValue(DBValue):
    """Wraps a 64-bit integer."""
    def __init__(self, v):
        self.v = int(v)

class StringValue(DBValue):
    """Wraps a string."""
    def __init__(self, v):
        self.v = str(v)

def wrap(val):
    """Helper to wrap raw python types."""
    if isinstance(val, int):
        return IntValue(val)
    if isinstance(val, str):
        return StringValue(val)
    if isinstance(val, DBValue):
        return val
    raise TypeError("Unsupported type for DBValue")
