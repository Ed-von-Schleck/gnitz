# gnitz/storage/metadata.py

"""
Canonical shard metadata representation.

This module contains the authoritative data structure for describing a shard's
physical location and logical content (Key/LSN ranges). It serves as a neutral
leaf node to prevent circular dependencies between index.py, manifest.py,
engine.py, and compactor.py.
"""

from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128


class ManifestEntry(object):
    """Authoritative shard metadata record."""
    _immutable_fields_ = [
        'table_id', 'shard_filename', 'pk_min_lo', 'pk_min_hi', 
        'pk_max_lo', 'pk_max_hi', 'min_lsn', 'max_lsn'
    ]
    
    def __init__(self, table_id, shard_filename, min_key, max_key, min_lsn, max_lsn):
        self.table_id = table_id
        self.shard_filename = shard_filename
        mk = r_uint128(min_key)
        xk = r_uint128(max_key)
        self.pk_min_lo = r_uint64(mk)
        self.pk_min_hi = r_uint64(mk >> 64)
        self.pk_max_lo = r_uint64(xk)
        self.pk_max_hi = r_uint64(xk >> 64)
        self.min_lsn = r_uint64(min_lsn)
        self.max_lsn = r_uint64(max_lsn)

    def get_min_key(self):
        return (r_uint128(self.pk_min_hi) << 64) | r_uint128(self.pk_min_lo)
    
    def get_max_key(self):
        return (r_uint128(self.pk_max_hi) << 64) | r_uint128(self.pk_max_lo)
