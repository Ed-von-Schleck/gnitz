# gnitz/storage/cursor.py

from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128
from gnitz.core import types

class ShardCursor(object):
    """
    Unified Seekable Cursor for Columnar Shards.
    Supports linear traversal and binary-search seeking.
    """
    _immutable_fields_ = ['view', 'schema', 'is_u128']

    def __init__(self, shard_view):
        self.view = shard_view
        self.schema = shard_view.schema
        self.is_u128 = self.schema.get_pk_column().field_type == types.TYPE_U128
        self.position = 0
        self.exhausted = False
        # Initialize at first non-ghost
        self._skip_ghosts()

    def _skip_ghosts(self):
        """Advances the cursor until it hits a non-zero weight or end of shard."""
        while self.position < self.view.count:
            if self.view.get_weight(self.position) != 0:
                self.exhausted = False
                return
            self.position += 1
        self.exhausted = True

    def seek(self, target_key):
        """
        Jumps to the first record where Key >= target_key.
        Maintains the Ghost Property by skipping annihilated records.
        """
        self.position = self.view.find_lower_bound(target_key)
        self._skip_ghosts()

    def advance(self):
        """Moves to the next record."""
        if self.exhausted:
            return
        self.position += 1
        self._skip_ghosts()

    def key(self):
        """Returns the current Primary Key."""
        if self.exhausted:
            return r_uint128(-1) # Max possible key as sentinel
        if self.is_u128:
            return self.view.get_pk_u128(self.position)
        return r_uint128(self.view.get_pk_u64(self.position))

    def weight(self):
        """Returns the current net weight."""
        if self.exhausted:
            return r_int64(0)
        return self.view.get_weight(self.position)

    def is_valid(self):
        return not self.exhausted

    # Compatibility for TournamentTree
    def is_exhausted(self):
        return self.exhausted

    def peek_key(self):
        return self.key()
